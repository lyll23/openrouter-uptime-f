import copy
import gzip
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('sync_upstream', Path(__file__).parents[1] / 'scripts/sync_upstream.py')
s = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s)


def entry(data):
    return {'sha': s.blob_sha(data), 'size': len(data)}


def tree_entry(path, data):
    return dict(path=path, mode='100644', type='blob', **entry(data))


class SyncTests(unittest.TestCase):
    def test_allowlist(self):
        for path in ['raw/2026-07-04/000001.json.gz', 'derived/2026-10-18.csv', 'status/latest.json', 'LICENSE']:
            self.assertTrue(s.allowed(path))
        for path in ['raw/2026-07-03/000001.json.gz', '.github/workflows/evil.yml',
                     'scripts/poll.py', 'status/evil.json', 'raw/../../evil', 'derived/2026-07-04.csv/evil']:
            self.assertFalse(s.allowed(path))
        for path in ['raw/2026-07-04/290001.json.gz', 'derived/2026-02-31.csv']:
            with self.assertRaises(ValueError):
                s.allowed(path)

    def test_tree_validation(self):
        good = {'truncated': False, 'tree': [tree_entry('LICENSE', b'MIT'), tree_entry('raw/2026-07-04/000000.json.gz', b'raw')]}
        self.assertEqual(len(s.select_tree(good)), 2)
        for change in ['truncated', 'symlink', 'size', 'sha', 'duplicate', 'no_license']:
            value = copy.deepcopy(good)
            if change == 'truncated': value['truncated'] = True
            elif change == 'symlink': value['tree'][0]['mode'] = '120000'
            elif change == 'size': value['tree'][0]['size'] = s.MAX_BLOB
            elif change == 'sha': value['tree'][0]['sha'] = '../bad'
            elif change == 'duplicate': value['tree'].append(value['tree'][0])
            else: value['tree'].pop(0)
            with self.assertRaises(ValueError, msg=change): s.select_tree(value)

    def test_download_verifies_and_reversibly_compresses(self):
        data = b'a,b\n1,2\n'
        with patch.object(s, 'request', return_value=data) as req:
            path, meta, stored = s.download(('derived/2026-07-04.csv', entry(data)), 'a' * 40)
            self.assertEqual(gzip.decompress(stored), data)
            self.assertIn('/' + 'a' * 40 + '/', req.call_args.args[0])
            self.assertEqual(meta['sha256'], s.hashlib.sha256(data).hexdigest())
        with patch.object(s, 'request', return_value=b'bad'):
            with self.assertRaises(ValueError): s.download(('derived/2026-07-04.csv', entry(data)), 'a' * 40)
        raw = gzip.compress(data)
        with patch.object(s, 'request', return_value=raw):
            _, meta, stored = s.download(('raw/2026-07-04/000000.json.gz', entry(raw)), 'a' * 40)
            self.assertEqual(stored, raw)
            self.assertEqual(meta['encoding'], 'identity')

    def test_equal_source_bytes_different_encodings_do_not_alias(self):
        data = b'same source bytes'
        with patch.object(s, 'request', return_value=data):
            raw = s.download(('raw/2026-07-04/000000.json.gz', entry(data)), 'a' * 40)
            csv = s.download(('derived/2026-07-04.csv', entry(data)), 'a' * 40)
        self.assertNotEqual(raw[1]['object'], csv[1]['object'])
        self.assertNotEqual(raw[1]['stored_git_sha'], csv[1]['stored_git_sha'])

    def test_bounded_batches(self):
        with patch.object(s, 'BATCH_BYTES', 10):
            values = [(str(i), {'size': 6}) for i in range(3)]
            self.assertEqual([len(b) for b in s.batches(values)], [1, 1, 1])

    def test_coverage_missing_dates_and_incomplete_import(self):
        files = {'raw/2026-07-04/120000.json.gz': entry(b'a'), 'raw/2026-07-06/000000.json.gz': entry(b'b')}
        result = s.coverage(files, {'files': {}})['days']
        self.assertEqual(result['2026-07-05']['upstream_samples'], 0)
        self.assertEqual(result['2026-07-05']['largest_gap_seconds_including_day_edges'], 86400)
        self.assertFalse(result['2026-07-04']['all_existing_raw_archived'])
        self.assertEqual(result['2026-07-04']['missing_vs_nominal_96'], 95)

    def test_download_failure_resumes_published_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remote = root / 'remote.git'
            subprocess.run(['git', 'init', '--bare', '-q', str(remote)], check=True)
            subprocess.run(['git', '-C', str(remote), 'config', 'uploadpack.allowFilter', 'true'], check=True)
            data = {'LICENSE': b'MIT', 'derived/2026-07-04.csv': b'rows',
                    'raw/2026-07-04/000000.json.gz': gzip.compress(b'{}')}
            snap = {'commit': 'd' * 40, 'files': {p: entry(v) for p, v in data.items()}}
            store = s.Store(root / 'first', str(remote))
            real_download = s.download
            seen = []
            def download(item, commit):
                seen.append(item[0])
                with patch.object(s, 'request', return_value=data[item[0]]):
                    return real_download(item, commit)
            def fail_after_license(item, commit):
                if item[0] != 'LICENSE': raise OSError('simulated interrupted download')
                return download(item, commit)
            with patch.object(s, 'BATCH_BYTES', 1), patch.object(s, 'download', side_effect=fail_after_license):
                with self.assertRaises(OSError): s.sync(store, snap, workers=1)
            resumed = s.Store(root / 'second', 'file://' + str(remote))
            self.assertEqual(resumed.pending['commit'], 'd' * 40)
            self.assertIsNone(resumed.manifest['complete_commit'])
            seen.clear()
            with patch.object(s, 'download', side_effect=download):
                report = s.sync(resumed, resumed.pending, workers=1)
            self.assertNotIn('LICENSE', seen)
            self.assertEqual(report['verified_files'], 3)
            self.assertTrue(report['snapshot_complete'])

    def test_git_checkpoints_resume_revisions_and_deletion_retention(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remote = root / 'remote.git'
            subprocess.run(['git', 'init', '--bare', '-q', str(remote)], check=True)
            subprocess.run(['git', '-C', str(remote), 'config', 'uploadpack.allowFilter', 'true'], check=True)
            store = s.Store(root / 'first', str(remote))
            data = {'LICENSE': b'MIT test', 'raw/2026-07-04/120000.json.gz': gzip.compress(b'{}'),
                    'derived/2026-07-04.csv': b'a,b\n1,2\n'}
            snap = {'commit': 'a' * 40, 'files': {p: entry(v) for p, v in data.items()}}
            real_download = s.download
            def download(item, commit):
                with patch.object(s, 'request', return_value=data[item[0]]):
                    return real_download(item, commit)
            with patch.object(s, 'download', side_effect=download):
                report = s.sync(store, snap, workers=1)
            self.assertTrue(report['snapshot_complete'])
            self.assertEqual(report['verified_files'], 3)
            old_object = store.manifest['files']['derived/2026-07-04.csv']['object']
            resumed = s.Store(root / 'second', 'file://' + str(remote))
            self.assertIsNone(resumed.pending)
            with patch.object(s, 'download', side_effect=AssertionError('No-op downloaded')):
                s.sync(resumed, snap, workers=1)
            data['derived/2026-07-04.csv'] = b'new corrected rows'
            revised = {'commit': 'b' * 40, 'files': {p: entry(v) for p, v in data.items() if p != 'raw/2026-07-04/120000.json.gz'}}
            with patch.object(s, 'download', side_effect=download): s.sync(resumed, revised, workers=1)
            self.assertFalse(resumed.manifest['files']['raw/2026-07-04/120000.json.gz']['present_in_upstream'])
            self.assertEqual(len(resumed.manifest['files']['derived/2026-07-04.csv']['previous_versions']), 1)
            self.assertTrue(resumed.git('show', resumed.head + ':' + old_object))
            self.assertFalse((root / 'second' / 'objects').exists())
            # Neither write-tree nor push may hydrate unchanged historical raw
            # objects. Inspect pack indexes directly (cat-file would fetch).
            raw_sha = entry(data['raw/2026-07-04/120000.json.gz'])['sha']
            packs = (root / 'second' / '.git' / 'objects' / 'pack').glob('*.idx')
            pack_listing = b''.join(subprocess.check_output(['git', 'verify-pack', '-v', str(p)]) for p in packs)
            self.assertNotIn(raw_sha.encode(), pack_listing)
            self.assertFalse((root / 'second' / '.git' / 'objects' / raw_sha[:2] / raw_sha[2:]).exists())
            self.assertEqual(resumed.git('show', resumed.head + ':LICENSE.upstream'), b'MIT test')
            # A committed pending checkpoint survives restart without falsely
            # advancing complete_commit; resumed work is tied to original SHA.
            interrupted = {'commit': 'c' * 40, 'files': revised['files']}
            resumed.save(interrupted)
            third = s.Store(root / 'third', 'file://' + str(remote))
            self.assertEqual(third.pending['commit'], 'c' * 40)
            self.assertEqual(third.manifest['complete_commit'], 'b' * 40)
            with patch.object(s, 'download', side_effect=AssertionError('Unexpected download')):
                s.sync(third, third.pending, workers=1)
            self.assertEqual(third.manifest['complete_commit'], 'c' * 40)
            # A manifest cannot certify an object that someone removed from
            # the archive tree; detection uses only tree hashes, not blob reads.
            third.git('update-index', '--force-remove', old_object)
            bad_tree = third.git('write-tree').decode().strip()
            bad = third.git('commit-tree', bad_tree, '-p', third.head, '-m', 'simulate accidental deletion').decode().strip()
            third.git('push', '--quiet', 'origin', bad + ':refs/heads/' + s.BRANCH)
            with self.assertRaisesRegex(ValueError, 'Archive object missing'):
                s.Store(root / 'fourth', 'file://' + str(remote))


if __name__ == '__main__':
    unittest.main()
