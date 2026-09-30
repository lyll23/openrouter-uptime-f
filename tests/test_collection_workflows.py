"""Regression checks for fork liveness and lossless sparse collection."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]


def liveness_code():
    workflow = (ROOT / '.github/workflows/liveness.yml').read_text()
    code = workflow.split("python3 - <<'PY'\n", 1)[1].split('\n          PY', 1)[0]
    return compile(textwrap.dedent(code), 'liveness.yml', 'exec')


class LivenessTest(unittest.TestCase):
    def check(self, generated, limit='60', branch='main', repository='lyll23/openrouter-uptime-f'):
        env = {'GITHUB_REPOSITORY': repository, 'DEFAULT_BRANCH': branch,
               'MAX_AGE_MINUTES': limit}
        payload = io.BytesIO(json.dumps({'generated': generated}).encode())
        output = io.StringIO()
        with mock.patch.dict(os.environ, env), contextlib.redirect_stdout(output), \
             mock.patch('urllib.request.urlopen', return_value=payload) as fetch:
            error = None
            try:
                exec(liveness_code(), {})
            except SystemExit as exc:
                error = str(exc)
        return fetch, output.getvalue(), error

    def test_checks_fork_instead_of_upstream(self):
        now = datetime.now(timezone.utc).isoformat()
        fetch, output, error = self.check(now)
        self.assertIsNone(error)
        fetch.assert_called_once_with(
            'https://raw.githubusercontent.com/lyll23/openrouter-uptime-f/main/status/latest.json',
            timeout=30)
        self.assertIn('lyll23/openrouter-uptime-f@main', output)

    def test_default_branch_is_not_assumed_to_be_main(self):
        fetch, _, error = self.check(datetime.now(timezone.utc).isoformat(), branch='stable')
        self.assertIsNone(error)
        self.assertIn('/stable/status/latest.json', fetch.call_args.args[0])

    def test_stale_poll_fails(self):
        old = datetime.now(timezone.utc) - timedelta(minutes=61)
        _, _, error = self.check(old.isoformat())
        self.assertIn('collector is silent', error)

    def test_zero_limit_can_prove_failure(self):
        old = datetime.now(timezone.utc) - timedelta(seconds=1)
        _, _, error = self.check(old.isoformat(), limit='0')
        self.assertIn('collector is silent', error)

    def test_future_timestamp_cannot_mask_outage(self):
        future = datetime.now(timezone.utc) + timedelta(minutes=10)
        _, _, error = self.check(future.isoformat())
        self.assertIn('in the future', error)

    def test_naive_timestamp_is_not_silently_accepted(self):
        _, _, error = self.check('2026-09-30T00:00:00')
        self.assertIn('no timezone', error)

    def test_nonfinite_and_negative_limits_fail(self):
        for limit in ['nan', 'inf', '-1']:
            with self.subTest(limit=limit):
                fetch, _, error = self.check(datetime.now(timezone.utc).isoformat(), limit=limit)
                self.assertIn('finite and non-negative', error)
                fetch.assert_not_called()


class PollGuardTest(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('should_poll', ROOT / 'scripts/should_poll.py')
        self.guard = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.guard)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.guard.LATEST = Path(self.tmp.name) / 'latest.json'

    def decision(self, payload, force=''):
        self.guard.LATEST.write_text(json.dumps(payload))
        output = io.StringIO()
        with mock.patch.dict(os.environ, {'MIN_INTERVAL_MIN': '12', 'FORCE_POLL': force}, clear=True), \
             contextlib.redirect_stdout(output):
            self.guard.main()
        return output.getvalue()

    def test_recent_poll_is_not_duplicated(self):
        now = datetime.now(timezone.utc) - timedelta(minutes=11)
        self.assertTrue(self.decision({'generated': now.isoformat()}).startswith('poll=false'))

    def test_due_poll_is_not_suppressed(self):
        old = datetime.now(timezone.utc) - timedelta(minutes=15)
        self.assertTrue(self.decision({'generated': old.isoformat()}).startswith('poll=true'))

    def test_force_bypasses_guard(self):
        self.assertTrue(self.decision({'generated': datetime.now(timezone.utc).isoformat()},
                                      force='true').startswith('poll=true'))

    def test_malformed_snapshot_fails_open(self):
        for payload in [{}, [], None, {'generated': None}, {'generated': 12}, {'generated': 'bad'}]:
            with self.subTest(payload=payload):
                self.assertTrue(self.decision(payload).startswith('poll=true'))

    def test_future_snapshot_does_not_suppress_collection(self):
        future = datetime.now(timezone.utc) + timedelta(minutes=1)
        self.assertTrue(self.decision({'generated': future.isoformat()}).startswith('poll=true'))


class SparseCollectionTest(unittest.TestCase):
    def test_appends_current_and_next_day_without_deleting_history(self):
        # Exercise actual git sparse semantics, not a mock. Every historical
        # file must survive git add -A, and an already-existing next-day CSV
        # must be present if a delayed run crosses midnight.
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / 'source'
            repo.mkdir()
            def git(*args, cwd=repo):
                return subprocess.run(['git', *args], cwd=cwd, check=True,
                                      text=True, capture_output=True).stdout
            git('init', '-q', '-b', 'main')
            git('config', 'user.name', 'Test')
            git('config', 'user.email', 'test@example.invalid')
            dates = [datetime.now(timezone.utc) + timedelta(days=i) for i in (-1, 0, 1)]
            yesterday, today, tomorrow = [d.strftime('%Y-%m-%d') for d in dates]
            files = {
                f'derived/{yesterday}.csv': 'historical\n',
                f'derived/{today}.csv': 'old today\n',
                f'derived/{tomorrow}.csv': 'old tomorrow\n',
                f'raw/{yesterday}/000000.json.gz': 'old archive',
                'status/latest.json': '{}', 'scripts/should_poll.py': '# test\n',
                'README.md': 'readme\n', '.gitignore': '__pycache__/\n',
            }
            for name, content in files.items():
                path = repo / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content)
            git('add', '-A')
            git('commit', '-qm', 'seed')
            work = Path(tmp) / 'collector'
            git('clone', '-q', '--no-checkout', str(repo), str(work))
            git('config', 'user.name', 'Test', cwd=work)
            git('config', 'user.email', 'test@example.invalid', cwd=work)
            workflow = (ROOT / '.github/workflows/poll.yml').read_text()
            patterns = textwrap.dedent(workflow.split('          sparse-checkout: |\n', 1)[1]
                                      .split('\n\n', 1)[0]).splitlines()
            git('sparse-checkout', 'set', '--no-cone', *patterns, cwd=work)
            git('checkout', '-q', 'main', cwd=work)
            # Run the workflow's actual pre-guard shell, including date logic.
            block = workflow.split('          # Include both dates', 1)[1]
            block = '          # Include both dates' + block.split('          python3 scripts/should_poll.py', 1)[0]
            env = dict(os.environ, GITHUB_REF_NAME='main')
            subprocess.run(['bash', '-euc', textwrap.dedent(block)], cwd=work, env=env,
                           check=True, capture_output=True, text=True)
            self.assertFalse((work / f'derived/{yesterday}.csv').exists())
            self.assertFalse((work / f'raw/{yesterday}/000000.json.gz').exists())
            for day in [today, tomorrow]:
                path = work / f'derived/{day}.csv'
                with path.open('a') as fh:
                    fh.write('new row\n')
                raw = work / f'raw/{day}/123456.json.gz'
                raw.parent.mkdir(parents=True, exist_ok=True)
                raw.write_text('new archive')
            git('add', '-A', cwd=work)
            changed = git('diff', '--cached', '--name-status', cwd=work)
            self.assertNotIn('D\t', changed)
            git('commit', '-qm', 'poll', cwd=work)
            for day, prefix in [(today, 'old today'), (tomorrow, 'old tomorrow')]:
                self.assertEqual(git('show', f'HEAD:derived/{day}.csv', cwd=work),
                                 f'{prefix}\nnew row\n')
            self.assertEqual(git('show', f'HEAD:raw/{yesterday}/000000.json.gz', cwd=work),
                             'old archive')
            self.assertEqual(git('show', f'HEAD:derived/{yesterday}.csv', cwd=work), 'historical\n')


if __name__ == '__main__':
    unittest.main()
