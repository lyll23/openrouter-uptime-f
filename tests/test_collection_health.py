"""Recovery is green only once valid fresh data exists on the remote."""
import csv
import contextlib
import gzip
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import shutil
import tempfile
import sys
import textwrap
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
spec = importlib.util.spec_from_file_location('collection_health', ROOT / 'scripts/collection_health.py')
health = importlib.util.module_from_spec(spec)
spec.loader.exec_module(health)
NOW = datetime(2026, 9, 30, 0, 5, tzinfo=timezone.utc)


def sample(age=0, state='up', now=NOW):
    return {'generated': (now - timedelta(minutes=age)).isoformat(),
            'models_polled': 1, 'endpoint_count': 1,
            'endpoints': [{'model': 'test/model', 'provider': 'Test', 'endpoint_tag': 'test',
                           'endpoint_id':'test', 'identity_ambiguous':False, 'state': state,
                           'up5m':100, 'up30m':100}]}


def step_shell(name):
    workflow = (ROOT / '.github/workflows/poll.yml').read_text()
    step = workflow.split(f'      - name: {name}\n', 1)[1].split('\n      - name:', 1)[0]
    shell = step.split('        run: |\n', 1)[1]
    return textwrap.dedent(shell)


class HealthTest(unittest.TestCase):
    def test_sixty_minute_boundary_is_unchanged(self):
        self.assertEqual(health.assess(sample(60), now=NOW)['status'], 'healthy')
        self.assertEqual(health.assess(sample(60.01), now=NOW)['status'], 'stale')

    def test_twelve_minute_validation_boundary(self):
        self.assertEqual(health.assess(sample(12), now=NOW, max_age=12)['status'], 'healthy')
        self.assertEqual(health.assess(sample(12.01), now=NOW, max_age=12)['status'], 'stale')

    def test_malformed_naive_future_empty_and_all_unknown_cannot_pass(self):
        broken = [None, [], {}, {'generated': 'bad'}, {'generated': '2026-09-30T00:00:00'},
                  sample(-1), sample(-6), sample(state='unknown'), dict(sample(), endpoints=[]),
                  dict(sample(), endpoint_count=3), dict(sample(), models_polled=0)]
        for value in broken:
            with self.subTest(value=value):
                self.assertEqual(health.assess(value, now=NOW)['status'], 'invalid')

    def test_partial_endpoint_failure_is_visible_without_discarding_good_observation(self):
        value = sample()
        value['endpoints'].append({'state': 'unknown'})
        value['endpoint_count'] = 2
        result = health.assess(value, now=NOW)
        self.assertEqual(result['status'], 'healthy')
        self.assertEqual(result['unknown_endpoints'], 1)

    def test_invalid_limit_fails_closed(self):
        for limit in [-1, float('nan'), float('inf')]:
            with self.assertRaises(ValueError):
                health.assess(sample(), now=NOW, max_age=limit)

    def test_stale_detect_is_warning_and_exit_zero_before_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            latest, report, output = [root / name for name in ['latest', 'before', 'output']]
            latest.write_text(json.dumps(sample(170, now=datetime.now(timezone.utc))))
            log = io.StringIO()
            with mock.patch.dict(os.environ, {'GITHUB_OUTPUT': str(output)}), contextlib.redirect_stdout(log):
                result = health.inspect(SimpleNamespace(latest=latest, report=report))
            self.assertEqual(result, 0)
            self.assertIn('::warning::', log.getvalue())
            self.assertNotIn('::error::', log.getvalue())
            self.assertIn('recovery_needed=true', output.read_text())
            self.assertEqual(json.loads(report.read_text())['status'], 'stale')

    def test_missing_snapshot_requests_recovery(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            root = Path(tmp)
            self.assertEqual(health.inspect(SimpleNamespace(latest=root/'missing', report=root/'before')), 0)
            self.assertEqual(json.loads((root/'before').read_text())['status'], 'invalid')

    def test_summary_keeps_real_gap_and_does_not_hide_failed_step(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            before=health.assess(sample(170), now=NOW)
            after=health.assess(sample(), now=NOW)
            (root/'before').write_text(json.dumps(before)); (root/'after').write_text(json.dumps(after))
            args=SimpleNamespace(before=root/'before', after=root/'after')
            with mock.patch.dict(os.environ, {'GITHUB_STEP_SUMMARY': str(root/'summary'), 'JOB_STATUS':'success'}), contextlib.redirect_stdout(io.StringIO()):
                health.summarize(args)
            self.assertIn('**recovered**', (root/'summary').read_text())
            self.assertIn('170.00 minutes', (root/'summary').read_text())
            with mock.patch.dict(os.environ, {'GITHUB_STEP_SUMMARY': str(root/'failed'), 'JOB_STATUS':'failure'}), contextlib.redirect_stdout(io.StringIO()):
                health.summarize(args)
            self.assertIn('failed or incomplete', (root/'failed').read_text())


class PersistedTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.repo = Path(self.tmp.name)
        self.old_cwd = Path.cwd(); os.chdir(self.repo); self.addCleanup(os.chdir, self.old_cwd)
        self.git('init', '-q', '-b', 'main')
        self.git('config', 'user.name', 'Test'); self.git('config', 'user.email', 'test@example.invalid')

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.repo, text=True).strip()

    def write_sample(self, value, include_raw=True, count=1):
        ts=datetime.fromisoformat(value['generated'])
        for folder in ['status', 'derived', f'raw/{ts:%Y-%m-%d}']:
            (self.repo/folder).mkdir(parents=True, exist_ok=True)
        (self.repo/'status/latest.json').write_text(json.dumps(value))
        if include_raw:
            (self.repo/f'raw/{ts:%Y-%m-%d}/{ts:%H%M%S}.json.gz').write_bytes(gzip.compress(json.dumps({'generated':value['generated'], 'endpoints':{'test/model':{'data':{'endpoints':[{'tag':'test','provider_name':'Test','status':0,'uptime_last_5m':100,'uptime_last_30m':100,'uptime_last_1d':100}]}}}}).encode()))
        with (self.repo/f'derived/{ts:%Y-%m-%d}.csv').open('w') as fh:
            writer=csv.writer(fh); writer.writerow(health.FIELDS)
            for _ in range(count): writer.writerow([value['generated'],'test/model','Test','test','test',False,'up',0,100,100,100])
    def commit_sample(self, value, include_raw=True, count=1):
        self.write_sample(value, include_raw=include_raw, count=count)
        self.git('add', '-A'); self.git('commit', '-qm', 'sample')
        self.git('update-ref', 'refs/remotes/origin/main', 'HEAD')
        return self.git('rev-parse', 'HEAD')

    def test_valid_persisted_sample_across_midnight(self):
        sha = self.commit_sample(sample(10))
        result = health.verify_persisted('origin/main', now=NOW)
        self.assertEqual(result['status'], 'healthy'); self.assertEqual(result['commit'], sha)
        self.assertEqual(result['raw_path'], 'raw/2026-09-29/235500.json.gz')
        self.assertEqual(result['matching_rows'], 1)

    def test_unpushed_local_sample_cannot_mask_stale_remote(self):
        self.commit_sample(sample(170))
        (self.repo/'status/latest.json').write_text(json.dumps(sample()))
        result=health.verify_persisted('origin/main', now=NOW)
        self.assertEqual(result['status'], 'stale')

    def test_missing_raw_fails(self):
        self.commit_sample(sample(), include_raw=False)
        with self.assertRaises(subprocess.CalledProcessError):
            health.verify_persisted('origin/main', now=NOW)

    def test_incomplete_csv_fails(self):
        self.commit_sample(sample(), count=0)
        with self.assertRaisesRegex(ValueError, 'endpoint identities'):
            health.verify_persisted('origin/main', now=NOW)

    def test_raw_timestamp_must_match(self):
        self.commit_sample(sample())
        p=self.repo/'raw/2026-09-30/000500.json.gz'
        p.write_bytes(gzip.compress(json.dumps({'generated':'wrong'}).encode()))
        self.git('add', '-A'); self.git('commit', '-qm', 'bad raw')
        self.git('update-ref', 'refs/remotes/origin/main', 'HEAD')
        with self.assertRaisesRegex(ValueError, 'raw archive timestamp'):
            health.verify_persisted('origin/main', now=NOW)

    def test_forced_noop_does_not_claim_new_sample(self):
        before = health.assess(sample(1), now=NOW)
        path = self.repo/'before'; path.write_text(json.dumps(before))
        args = SimpleNamespace(ref='origin/main', max_age=12, report=self.repo/'after', before=path)
        with mock.patch.object(health, 'verify_persisted', return_value=before.copy()), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(health.verify(args), 1)
        self.assertIn('did not advance', json.loads(args.report.read_text())['reason'])

    def test_recovery_can_correct_a_small_future_timestamp(self):
        before=health.assess(sample(-1), now=NOW)
        self.assertEqual(before['status'],'invalid')
        path=self.repo/'before'; path.write_text(json.dumps(before))
        args=SimpleNamespace(ref='origin/main', max_age=12, report=self.repo/'after', before=path)
        with mock.patch.object(health, 'verify_persisted', return_value=health.assess(sample(), now=NOW)), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(health.verify(args),0)

    def test_csv_state_drift_fails(self):
        self.commit_sample(sample())
        path=self.repo/'derived/2026-09-30.csv'
        path.write_text(path.read_text().replace(',up,', ',down,'))
        self.git('add', '-A'); self.git('commit', '-qm', 'drift')
        self.git('update-ref', 'refs/remotes/origin/main', 'HEAD')
        with self.assertRaisesRegex(ValueError, 'CSV does not match raw'):
            health.verify_persisted('origin/main', now=NOW)

    def test_failed_remote_fetch_or_decode_is_a_nonzero_result(self):
        args=SimpleNamespace(ref='origin/main', max_age=60, report=self.repo/'after')
        for error in [subprocess.CalledProcessError(1,['git']), ValueError('invalid JSON')]:
            with mock.patch.object(health, 'verify_persisted', side_effect=error), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(health.verify(args), 1)
            self.assertEqual(json.loads(args.report.read_text())['status'], 'invalid')


class WorkflowGitTest(PersistedTest):
    """Execute the actual workflow commit/verification shell against local git."""
    def prepare_remote(self, age):
        self.commit_sample(sample(age, now=datetime.now(timezone.utc)))
        (self.repo/'scripts').mkdir()
        for name in ['collection_health.py', 'should_poll.py', 'rebuild_history.py']:
            shutil.copy(ROOT/'scripts'/name, self.repo/'scripts'/name)
        (self.repo/'.gitignore').write_text('__pycache__/\n')
        self.git('add', '-A'); self.git('commit', '-qm', 'scripts')
        other=tempfile.TemporaryDirectory(); self.addCleanup(other.cleanup)
        remote=Path(other.name)/'remote.git'
        subprocess.run(['git','init','-q','--bare',str(remote)], check=True)
        self.git('remote','add','origin',str(remote)); self.git('push','-q','origin','main')
        runtime=Path(other.name)/'runtime'; runtime.mkdir()
        bindir=Path(other.name)/'bin'; bindir.mkdir()
        sleep=bindir/'sleep'; sleep.write_text('#!/bin/sh\nexit 0\n'); sleep.chmod(0o755)
        self.env=dict(os.environ, DEFAULT_BRANCH='main', RUNNER_TEMP=str(runtime),
                      MIN_INTERVAL_MIN='12', POLL_DUE='true', PATH=str(bindir)+os.pathsep+os.environ['PATH'])
        before=runtime/'collection-before.json'
        subprocess.run(['python3','scripts/collection_health.py','inspect','--report',str(before)],
                       env=self.env, check=True, capture_output=True)
        return remote

    def shell(self, name):
        return subprocess.run(['bash','-euo','pipefail','-c',step_shell(name)],
                              cwd=self.repo, env=self.env, capture_output=True, text=True)

    def test_stale_recovers_and_verifies_actual_pushed_data(self):
        self.prepare_remote(170)
        self.write_sample(sample(now=datetime.now(timezone.utc)))
        commit=self.shell('Commit'); self.assertEqual(commit.returncode,0,commit.stdout+commit.stderr)
        result=self.shell('Validate fresh sample persisted on the default branch')
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        after=json.loads((Path(self.env['RUNNER_TEMP'])/'collection-after.json').read_text())
        self.assertEqual(after['status'],'healthy')
        self.assertEqual(after['commit'],self.git('rev-parse','origin/main'))

    def test_skipped_poll_still_validates_persisted_data(self):
        self.prepare_remote(5); self.env['POLL_DUE']='false'
        result=self.shell('Validate fresh sample persisted on the default branch')
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)

    def test_rejected_persistence_stays_failure(self):
        remote=self.prepare_remote(170)
        hook=remote/'hooks/pre-receive'; hook.write_text('#!/bin/sh\nexit 1\n'); hook.chmod(0o755)
        self.write_sample(sample(now=datetime.now(timezone.utc)))
        commit=self.shell('Commit'); self.assertNotEqual(commit.returncode,0)
        self.assertIn('push failed after 5 attempts', commit.stdout)
        result=self.shell('Validate fresh sample persisted on the default branch')
        self.assertNotEqual(result.returncode,0)

    def test_competing_valid_sample_covers_push_race(self):
        remote=self.prepare_remote(170)
        # Simulate another writer committing a new sample after our checkout.
        self.write_sample(sample(now=datetime.now(timezone.utc)))
        self.git('add','-A'); self.git('commit','-qm','competing sample')
        self.git('push','-q','origin','main')
        self.git('reset','--hard','HEAD~1')
        self.write_sample(sample(now=datetime.now(timezone.utc)))
        commit=self.shell('Commit')
        self.assertEqual(commit.returncode,0,commit.stdout+commit.stderr)
        self.assertIn('other collector covered this window',commit.stdout)
        result=self.shell('Validate fresh sample persisted on the default branch')
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)

    def test_competing_invalid_sample_is_not_accepted(self):
        self.prepare_remote(170)
        self.write_sample(sample(now=datetime.now(timezone.utc)), count=0)
        self.git('add','-A'); self.git('commit','-qm','invalid competitor')
        self.git('push','-q','origin','main'); self.git('reset','--hard','HEAD~1')
        self.write_sample(sample(now=datetime.now(timezone.utc)))
        commit=self.shell('Commit')
        self.assertNotIn('other collector covered this window',commit.stdout)
        # Rebase may either preserve our good data or fail on a real conflict;
        # it must never claim successful recovery on invalid remote artifacts.
        result=self.shell('Validate fresh sample persisted on the default branch')
        if commit.returncode == 0:
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        else:
            self.assertNotEqual(result.returncode,0)


class WorkflowTest(unittest.TestCase):
    def test_no_new_permission_or_recovery_trigger_loop(self):
        workflow=(ROOT/'.github/workflows/poll.yml').read_text()
        self.assertNotIn('\n  workflow_run:', workflow)
        self.assertFalse((ROOT/'.github/workflows/liveness.yml').exists())
        self.assertIn('permissions:\n  contents: write\n', workflow)
        self.assertIn('group: poll\n  cancel-in-progress: false', workflow)
        self.assertIn('- cron: "23 * * * *"', workflow)
        self.assertIn('- cron: "7,22,37,52 * * * *"', workflow)
        self.assertIn('if: github.ref_name == github.event.repository.default_branch', workflow)
        self.assertEqual(workflow.count('continue-on-error: true'), 1)
        self.assertIn('if: ${{ !cancelled() }}', workflow)
        self.assertIn('JOB_STATUS: ${{ job.status }}', workflow)

    def run_guard(self, age, hourly, recovery=False, force=False):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); latest=root/'latest'; output=root/'output'
            latest.write_text(json.dumps(sample(age, now=datetime.now(timezone.utc))))
            env=dict(os.environ, GITHUB_OUTPUT=str(output), LATEST_OVERRIDE=str(latest),
                     MIN_INTERVAL_MIN='12', HOURLY_CHECK=str(hourly).lower(),
                     RECOVERY_NEEDED=str(recovery).lower(), FORCE_POLL=str(force or recovery).lower())
            subprocess.run(['bash','-euo','pipefail','-c',step_shell('Is a poll due?')], cwd=ROOT, env=env, check=True, capture_output=True)
            return output.read_text()

    def test_regular_due_poll_is_not_skipped_because_health_is_fresh(self):
        self.assertIn('poll=true', self.run_guard(20, hourly=False))

    def test_hourly_check_collects_if_prior_scheduled_sample_was_missed(self):
        self.assertIn('poll=true', self.run_guard(20, hourly=True))
        self.assertIn('poll=false', self.run_guard(1, hourly=True))

    def test_near_stale_boundary_attempts_collection_before_validation(self):
        self.assertIn('poll=true', self.run_guard(59.99, hourly=True))

    def test_regular_fresh_duplicate_skips(self):
        self.assertIn('poll=false', self.run_guard(5, hourly=False))

    def test_hourly_stale_recovers_and_force_still_works(self):
        self.assertIn('poll=true', self.run_guard(170, hourly=True, recovery=True))
        self.assertIn('poll=true', self.run_guard(1, hourly=False, force=True))

    def test_invalid_but_recent_sample_forces_recovery(self):
        self.assertIn('poll=true', self.run_guard(1, hourly=True, recovery=True))


if __name__ == '__main__':
    unittest.main()
