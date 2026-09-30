#!/usr/bin/env python3
"""Detect before collection; fail only after recovery/persistence is attempted.

A green run proves a fresh committed sample, not reliable scheduled cadence.
Raw/CSV timestamps and the daily coverage report remain the sampling record.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import math
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone

from rebuild_history import FIELDS, rows_from

MAX_AGE_MINUTES = 60.0


def assess(snapshot, now=None, max_age=MAX_AGE_MINUTES):
    """Classify a snapshot without failing a workflow before it can recover."""
    if not math.isfinite(max_age) or max_age < 0:
        raise ValueError('max_age must be finite and non-negative')
    now = now or datetime.now(timezone.utc)
    result = {'status': 'invalid', 'generated': None, 'age_minutes': None}
    try:
        generated = snapshot['generated']
        ts = datetime.fromisoformat(generated)
        if ts.tzinfo is None:
            raise ValueError('latest poll timestamp has no timezone')
        age = (now - ts).total_seconds() / 60
        result.update(generated=generated, age_minutes=round(age, 2))
        # Match should_poll: even small future skew must be corrected, not
        # treated as a healthy baseline that a real sample must advance past.
        if age < 0:
            raise ValueError('latest poll timestamp is in the future')
        rows = snapshot['endpoints']
        if not isinstance(rows, list) or not rows:
            raise ValueError('latest poll has no endpoint observations')
        if snapshot['endpoint_count'] != len(rows) or snapshot['models_polled'] <= 0:
            raise ValueError('latest poll counts are inconsistent or empty')
        known = sum(row['state'] in {'up', 'degraded', 'down', 'idle'} for row in rows)
        if not known:
            raise ValueError('all endpoint observations are unknown; collection failed')
        result.update(endpoint_count=len(rows), unknown_endpoints=len(rows) - known)
        result.update(status='stale' if age > max_age else 'healthy',
                      reason=f'latest poll is {age:.1f} min old (limit {max_age:g} min)')
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        result['reason'] = str(exc)
    return result


def read_snapshot(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError) as exc:
        return {'read_error': str(exc)}


def git_bytes(ref, path):
    # The caller fetches origin immediately before validation. Read immutable
    # objects from that fetched commit, never locally unpushed poll artifacts.
    return subprocess.check_output(['git', 'show', f'{ref}:{path}'])


def verify_persisted(ref, max_age=MAX_AGE_MINUTES, now=None):
    commit = subprocess.check_output(['git', 'rev-parse', f'{ref}^{{commit}}'], text=True).strip()
    snapshot = json.loads(git_bytes(commit, 'status/latest.json'))
    result = assess(snapshot, now=now, max_age=max_age)
    result['commit'] = commit
    if result['status'] != 'healthy':
        return result
    ts = datetime.fromisoformat(snapshot['generated']).astimezone(timezone.utc)
    raw_path = f'raw/{ts:%Y-%m-%d}/{ts:%H%M%S}.json.gz'
    csv_path = f'derived/{ts:%Y-%m-%d}.csv'
    archive = json.loads(gzip.decompress(git_bytes(commit, raw_path)))
    if archive['generated'] != snapshot['generated']:
        raise ValueError('persisted raw archive timestamp does not match latest.json')
    expected = rows_from(archive)
    if len(archive['endpoints']) != snapshot['models_polled']:
        raise ValueError('persisted raw model count does not match latest.json')
    reader = csv.DictReader(io.StringIO(git_bytes(commit, csv_path).decode()))
    if reader.fieldnames != FIELDS:
        raise ValueError('persisted CSV schema is incomplete')
    actual = [row for row in reader if row['ts'] == snapshot['generated']]
    def index(rows):
        indexed = {(row['model'], row['endpoint_id']): row for row in rows}
        if len(indexed) != len(rows):
            raise ValueError('persisted sample has duplicate endpoint identities')
        return indexed
    exp, act, latest = index(expected), index(actual), index(snapshot['endpoints'])
    if set(exp) != set(act) or set(exp) != set(latest):
        raise ValueError('persisted raw/CSV/latest endpoint identities do not match')
    def normalized(value):
        return '' if value is None else str(value)
    for key, row in exp.items():
        if any(normalized(row[field]) != act[key][field] for field in FIELDS):
            raise ValueError(f'persisted CSV does not match raw observations for {key}')
        for field in ['provider', 'endpoint_tag', 'endpoint_id', 'identity_ambiguous', 'state', 'up5m', 'up30m']:
            if row[field] != latest[key][field]:
                raise ValueError(f'persisted latest {field} does not match raw observations for {key}')
    result.update(raw_path=raw_path, csv_path=csv_path, matching_rows=len(actual))
    return result


def output(name, value):
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as fh:
            fh.write(f'{name}={value}\n')


def inspect(args):
    result = assess(read_snapshot(args.latest))
    Path(args.report).write_text(json.dumps(result))
    recovery = result['status'] != 'healthy'
    output('recovery_needed', str(recovery).lower())
    output('age_minutes', result['age_minutes'] if result['age_minutes'] is not None else '')
    print(('::warning::' if recovery else '') + result['reason'])
    if recovery:
        print('Recovery will run before deciding workflow success. Past gaps cannot be backfilled.')
    return 0


def verify(args):
    try:
        result = verify_persisted(args.ref, max_age=args.max_age)
        if getattr(args, 'before', None) and result['status'] == 'healthy':
            before = read_snapshot(args.before)
            if before.get('status') in {'healthy', 'stale'}:
                if datetime.fromisoformat(result['generated']) <= datetime.fromisoformat(before['generated']):
                    result.update(status='invalid', reason='due poll did not advance the persisted sample')
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.CalledProcessError) as exc:
        result = {'status': 'invalid', 'reason': f'persisted sample validation failed: {exc}'}
    Path(args.report).write_text(json.dumps(result))
    print(json.dumps(result, indent=2))
    if result['status'] != 'healthy':
        annotation = 'warning' if getattr(args, 'candidate', False) else 'error'
        print(f'::{annotation}::Recovery did not establish a fresh, valid persisted sample')
        return 1
    return 0


def summarize(args):
    before, after = read_snapshot(args.before), read_snapshot(args.after)
    before_status, after_status = before.get('status', 'unavailable'), after.get('status', 'unavailable')
    successful = after_status == 'healthy' and os.environ.get('JOB_STATUS') == 'success'
    outcome = ('recovered' if before_status != 'healthy' else 'healthy') if successful else 'failed or incomplete'
    lines = ['### Collection health', '', f'- Outcome: **{outcome}**',
             f'- Before: {before_status}; {before.get("reason", "check did not complete")}',
             f'- After: {after_status}; {after.get("reason", "validation did not complete")}',
             f'- Poll due: `{os.environ.get("POLL_DUE", "unknown")}`',
             f'- Poll / commit / validation: {os.environ.get("POLL_RESULT", "unknown")} / '
             f'{os.environ.get("COMMIT_RESULT", "unknown")} / {os.environ.get("VERIFY_RESULT", "unknown")}']
    if after.get('commit'):
        lines.append(f'- Verified persisted commit: `{after["commit"]}`')
    if before.get('generated') and after.get('generated'):
        first = datetime.fromisoformat(before['generated'])
        last = datetime.fromisoformat(after['generated'])
        gap = (last - first).total_seconds() / 60
        if gap > 0:
            lines.append(f'- Actual interval between samples: **{gap:.2f} minutes** '
                         f'({before["generated"]} → {after["generated"]})')
    lines += ['', 'A recovered run does not fill historical sampling gaps. Raw/CSV timestamps '
              'and the daily coverage report retain those gaps. GitHub schedules remain best-effort.']
    text = '\n'.join(lines) + '\n'
    print(text)
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as fh:
        fh.write(text)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    check = commands.add_parser('inspect')
    check.add_argument('--latest', default='status/latest.json')
    check.add_argument('--report', required=True)
    check.set_defaults(func=inspect)
    persisted = commands.add_parser('verify')
    persisted.add_argument('--ref', required=True)
    persisted.add_argument('--max-age', type=float, default=MAX_AGE_MINUTES)
    persisted.add_argument('--report', required=True)
    persisted.add_argument('--before', help='Require advancement over this valid pre-poll report')
    persisted.add_argument('--candidate', action='store_true', help='Check a competing writer before discarding our sample')
    persisted.set_defaults(func=verify)
    summary = commands.add_parser('summary')
    summary.add_argument('--before', required=True)
    summary.add_argument('--after', required=True)
    summary.set_defaults(func=summarize)
    args = parser.parse_args()
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
