#!/usr/bin/env python3
"""Archive public upstream data, never execute its code or modify the poll branch."""
import argparse
import base64
import concurrent.futures
import datetime as dt
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.request

UPSTREAM = 'dthinkr/openrouter-uptime'
BRANCH = 'upstream-data'
# Fixed user-approved UTC boundary; never a rolling 'today' window.
START = '2026-10-19'
MAX_BLOB = 95 * 1024 * 1024
BATCH_BYTES = 128 * 1024 * 1024
RAW = re.compile(r'raw/(\d{4}-\d{2}-\d{2})/([0-2]\d[0-5]\d[0-5]\d)\.json\.gz\Z')
DERIVED = re.compile(r'derived/(\d{4}-\d{2}-\d{2})\.csv\Z')
# Current snapshots only. Historical incidents/change logs and cumulative
# coverage/audit/last_seen are intentionally excluded from this scope.
STATUS = {'status/latest.json', 'status/models.json', 'status/providers.json'}
SCOPE = {'start_date': START, 'status_paths': sorted(STATUS)}
SHA = re.compile(r'[0-9a-f]{40}\Z')


def encode(obj):
    return (json.dumps(obj, sort_keys=True, separators=(',', ':')) + '\n').encode()


def blob_sha(data):
    return hashlib.sha1(f'blob {len(data)}\0'.encode() + data).hexdigest()


def allowed(path):
    if path in STATUS or path == 'LICENSE':
        return True
    match = RAW.fullmatch(path) or DERIVED.fullmatch(path)
    if not match:
        return False
    day = dt.date.fromisoformat(match[1])
    if RAW.fullmatch(path):
        dt.datetime.strptime(match[2], '%H%M%S')
    return day >= dt.date.fromisoformat(START)


def select_tree(tree):
    if tree.get('truncated') is not False:
        raise ValueError('Incomplete upstream tree; refusing to infer deletions')
    result = {}
    for item in tree['tree']:
        path = item['path']
        if allowed(path):
            if item['type'] != 'blob' or item['mode'] != '100644':
                raise ValueError(f'Unexpected data object mode: {path}')
            if not SHA.fullmatch(item['sha']) or not 0 <= item['size'] < MAX_BLOB:
                raise ValueError(f'Invalid hash or oversized source blob: {path}')
            if path in result:
                raise ValueError(f'Duplicate path: {path}')
            result[path] = {k: item[k] for k in ('sha', 'size')}
    if 'LICENSE' not in result or not any(RAW.fullmatch(p) for p in result):
        raise ValueError('Missing upstream license or raw data')
    return result


def request(url, limit, token=None):
    # No token is sent to raw.githubusercontent.com; no upstream code is run.
    headers = {'User-Agent': 'openrouter-uptime-archive', 'Accept-Encoding': 'identity'}
    if token:
        if not url.startswith('https://api.github.com/'):
            raise ValueError('Token only allowed on GitHub API')
        headers['Authorization'] = 'Bearer ' + token
    for attempt in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=90) as response:
                data = response.read(limit + 1)
            if len(data) > limit:
                raise ValueError(f'Response exceeds size bound: {url}')
            return data
        except urllib.error.HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or attempt == 4:
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt == 4:
                raise
        time.sleep(2 ** attempt)


def api(path):
    suffix = '/' + path if path else ''
    return json.loads(request('https://api.github.com/repos/' + UPSTREAM + suffix,
                              32 * 1024 * 1024, os.getenv('GITHUB_TOKEN')))


def snapshot():
    # Read default branch name, resolve it once, then use immutable SHA everywhere.
    branch = api('')['default_branch']
    from urllib.parse import quote
    commit = api('commits/' + quote(branch, safe=''))['sha']
    if not SHA.fullmatch(commit):
        raise ValueError('Invalid upstream commit')
    tree = api('git/trees/' + commit + '?recursive=1')
    return {'scope': SCOPE, 'commit': commit, 'files': select_tree(tree)}


def validate_snapshot(pending):
    if pending.get('scope') != SCOPE or not SHA.fullmatch(pending.get('commit', '')):
        raise ValueError('Snapshot scope or commit mismatch; refusing old-history resume')
    for path, item in pending['files'].items():
        if not allowed(path) or not SHA.fullmatch(item['sha']) or not 0 <= item['size'] < MAX_BLOB:
            raise ValueError('Snapshot contains a path outside the approved scope')


def download(item, commit):
    path, entry = item
    data = request(f'https://raw.githubusercontent.com/{UPSTREAM}/{commit}/{path}', entry['size'])
    if len(data) != entry['size'] or blob_sha(data) != entry['sha']:
        raise ValueError(f'Upstream byte verification failed: {path}')
    stored = data if path.endswith('.gz') else gzip.compress(data, compresslevel=6, mtime=0)
    if len(stored) >= MAX_BLOB:
        raise ValueError(f'Oversized archive object: {path}')
    meta = dict(entry, sha256=hashlib.sha256(data).hexdigest(),
                object=f"objects/{entry['sha'][:2]}/{entry['sha']}.{'identity' if path.endswith('.gz') else 'gzip'}.gz",
                encoding='identity' if path.endswith('.gz') else 'gzip',
                stored_size=len(stored), stored_git_sha=blob_sha(stored),
                stored_sha256=hashlib.sha256(stored).hexdigest(),
                source_commit=commit)
    return path, meta, stored


def batches(items):
    batch, size = [], 0
    for item in items:
        if batch and (size + item[1]['size'] > BATCH_BYTES or len(batch) >= 512):
            yield batch
            batch, size = [], 0
        batch.append(item)
        size += item[1]['size']
    if batch:
        yield batch


def coverage(files, manifest):
    days = {}
    for path, item in files.items():
        match = RAW.fullmatch(path)
        if match:
            day = days.setdefault(match[1], {'upstream_samples': 0, 'archived_samples': 0, 'seconds': []})
            day['upstream_samples'] += 1
            value = match[2]
            day['seconds'].append(int(value[:2]) * 3600 + int(value[2:4]) * 60 + int(value[4:]))
            if manifest['files'].get(path, {}).get('sha') == item['sha']:
                day['archived_samples'] += 1
    # Include absent dates so missing days are visible, including the first day.
    if days:
        date, end = dt.date.fromisoformat(START), dt.date.fromisoformat(max(days))
        while date <= end:
            day = days.setdefault(date.isoformat(), {'upstream_samples': 0, 'archived_samples': 0, 'seconds': []})
            seconds = sorted(day.pop('seconds'))
            boundaries = [0] + seconds + [86400]
            day['largest_gap_seconds_including_day_edges'] = max(b - a for a, b in zip(boundaries, boundaries[1:]))
            day['first_sample_utc'] = None if not seconds else seconds[0]
            day['last_sample_utc'] = None if not seconds else seconds[-1]
            day['missing_vs_nominal_96'] = max(0, 96 - day['upstream_samples'])
            day['all_existing_raw_archived'] = day['archived_samples'] == day['upstream_samples']
            date += dt.timedelta(days=1)
    return {'source': UPSTREAM, 'scope': SCOPE, 'start': START, 'days': days,
            'note': '96/day is a comparison baseline, not a historical SLA. Day closure does not imply complete observations. Filename timestamps only; no inferred samples.'}


class Store:
    """No data checkout: index/tree operations preserve remote objects lazily."""
    def __init__(self, directory, remote):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.env = os.environ.copy()
        token = self.env.get('GITHUB_TOKEN')
        if token:
            self.env.update(GIT_CONFIG_COUNT='1', GIT_CONFIG_KEY_0='http.https://github.com/.extraheader',
                GIT_CONFIG_VALUE_0='AUTHORIZATION: basic ' + base64.b64encode(('x-access-token:' + token).encode()).decode())
        self.git('init', '-q')
        self.git('config', 'user.name', 'upstream-archive-bot')
        self.git('config', 'user.email', 'actions@github.com')
        self.git('config', 'pack.window', '0')
        self.git('remote', 'add', 'origin', remote)
        refs = self.git('ls-remote', '--heads', 'origin', 'refs/heads/' + BRANCH).decode().split()
        self.head = None
        if refs:
            self.git('fetch', '--quiet', '--depth=1', '--filter=blob:none', 'origin', 'refs/heads/' + BRANCH)
            self.head = self.git('rev-parse', 'FETCH_HEAD').decode().strip()
            self.git('read-tree', self.head)
        self.manifest = self.read('manifest.json') if self.head else {
            'schema': 2, 'source': UPSTREAM, 'scope': SCOPE, 'start': START,
            'complete_commit': None, 'files': {}}
        if (self.manifest.get('source') != UPSTREAM or self.manifest.get('schema') != 2
                or self.manifest.get('scope') != SCOPE or self.manifest.get('start') != START):
            raise ValueError('Archive manifest scope mismatch; explicit migration required')
        self.pending = self.read('pending.json') if self.head else None
        if self.pending is not None:
            validate_snapshot(self.pending)
        if self.head:
            self.verify_objects()

    def git(self, *args, data=None):
        result = subprocess.run(['git', '-C', str(self.directory), *args], input=data,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env)
        if result.returncode:
            raise RuntimeError(f'git {args[0]} failed: ' + result.stderr.decode(errors='replace'))
        if b'filtering not recognized' in result.stderr:
            raise RuntimeError('Remote lacks partial-clone support; refusing a full-data fallback')
        return result.stdout

    def verify_objects(self):
        # Tree-only verification does not hydrate historical blobs. The stored
        # Git hash binds both bytes and length; never use ls-tree -l here.
        objects = {}
        for record in self.git('ls-tree', '-r', '-z', self.head).split(b'\0'):
            if record:
                header, path = record.split(b'\t', 1)
                mode, kind, sha = header.decode().split()
                if mode == '100644' and kind == 'blob':
                    objects[path.decode()] = sha
        for path, current in self.manifest['files'].items():
            if not allowed(path):
                raise ValueError('Unexpected manifest source path')
            for version in [current] + current.get('previous_versions', []):
                sha = version.get('stored_git_sha')
                if not sha or objects.get(version['object']) != sha:
                    raise ValueError(f'Archive object missing or inconsistent: {path}')
        license_entry = self.manifest['files'].get('LICENSE')
        if license_entry and objects.get('LICENSE.upstream') != license_entry['sha']:
            raise ValueError('Readable upstream license inconsistent with manifest')

    def read(self, path):
        return json.loads(self.git('show', self.head + ':' + path))

    def put(self, path, data):
        if len(data) >= MAX_BLOB:
            raise ValueError('Archive metadata/object exceeds safe GitHub blob size')
        sha = self.git('hash-object', '-w', '--stdin', data=data).decode().strip()
        self.git('update-index', '--add', '--cacheinfo', '100644', sha, path)

    def save(self, pending, complete=False):
        self.pending = None if complete else pending
        if complete:
            self.manifest['complete_commit'] = pending['commit']
            for path, entry in self.manifest['files'].items():
                entry['present_in_upstream'] = path in pending['files']
        self.put('manifest.json', encode(self.manifest))
        self.put('pending.json', encode(self.pending))
        report = coverage(pending['files'], self.manifest)
        report.update(snapshot_commit=pending['commit'], snapshot_complete=complete,
            expected_files=len(pending['files']), verified_files=sum(
                self.manifest['files'].get(p, {}).get('sha') == e['sha'] for p, e in pending['files'].items()),
            expected_source_bytes=sum(e['size'] for e in pending['files'].values()),
            verified_source_bytes=sum(e['size'] for p, e in pending['files'].items()
                if self.manifest['files'].get(p, {}).get('sha') == e['sha']))
        self.put('coverage.json', encode(report))
        # Prior remote objects are verified by tree SHA and intentionally absent
        # locally. Default write-tree would hydrate every promised blob.
        tree = self.git('write-tree', '--missing-ok').decode().strip()
        if self.head and tree == self.git('rev-parse', self.head + '^{tree}').decode().strip():
            return report
        parents = ['-p', self.head] if self.head else []
        commit = self.git('commit-tree', tree, *parents, '-m',
            f"archive {pending['commit']} {'complete' if complete else 'checkpoint'}").decode().strip()
        # Non-forced push is an atomic branch update. A collision fails closed;
        # rerun reads the winner's manifest and safely fills remaining objects.
        self.git('push', '--quiet', 'origin', commit + ':refs/heads/' + BRANCH)
        actual = self.git('ls-remote', '--heads', 'origin', 'refs/heads/' + BRANCH).decode().split()[0]
        if actual != commit:
            raise RuntimeError('Archive branch changed during push verification')
        self.head = commit
        return report


def sync(store, pending, workers=8):
    validate_snapshot(pending)
    commit, files = pending['commit'], pending['files']
    todo = [(p, e) for p, e in sorted(files.items())
            if store.manifest['files'].get(p, {}).get('sha') != e['sha']]
    # License is archived and readable before any data checkpoints are published.
    todo.sort(key=lambda item: item[0] != 'LICENSE')
    for batch in batches(todo):
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            for path, meta, data in executor.map(lambda item: download(item, commit), batch):
                old = store.manifest['files'].get(path)
                versions = [] if old is None else old.get('previous_versions', []) + [
                    {k: v for k, v in old.items() if k not in ('previous_versions', 'present_in_upstream')}]
                store.put(meta['object'], data)
                if path == 'LICENSE':
                    store.put('LICENSE.upstream', gzip.decompress(data))
                store.manifest['files'][path] = dict(meta, previous_versions=versions, present_in_upstream=True)
        store.save(pending)
        print(f"Checkpoint: {len(store.manifest['files'])} paths archived", flush=True)
    return store.save(pending, complete=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--store', required=True, help='Fresh temporary directory; never the poll checkout')
    args = parser.parse_args()
    repo = os.environ['GITHUB_REPOSITORY']
    if repo != 'lyll23/openrouter-uptime-f':
        raise ValueError('This workflow only writes its configured archive repository')
    store = Store(args.store, f'https://github.com/{repo}.git')
    # Pin a new target once; resume any interrupted target first. At most two
    # snapshots/run prevents chasing a continuously advancing upstream forever.
    target = snapshot()
    if store.pending:
        sync(store, store.pending)
    report = sync(store, target)
    print(json.dumps(report, indent=2))
    summary = os.getenv('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a') as out:
            out.write(f"## Upstream archive\nSource commit: `{report['snapshot_commit']}`\n\n"
                f"Verified {report['verified_files']}/{report['expected_files']} existing files. "
                f"Complete snapshot: {report['snapshot_complete']}.\n\n"
                f"[Coverage and gaps](https://github.com/{repo}/blob/{BRANCH}/coverage.json). "
                "Existing-source completeness is not uninterrupted sampling.\n")


if __name__ == '__main__':
    main()
