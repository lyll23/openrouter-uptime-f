# Daily upstream history archive

The `sync-upstream` workflow copies the **currently available** public data in
[dthinkr/openrouter-uptime](https://github.com/dthinkr/openrouter-uptime), starting
2026-07-04, to this repository's independent [`upstream-data` branch](https://github.com/lyll23/openrouter-uptime-f/tree/upstream-data).
It runs daily at 02:17 UTC (GitHub scheduling is best-effort), on manual dispatch,
and once when its workflow/script changes on `main`. The first successful run
backfills the entire available history. No OpenRouter key or additional secret
is needed: the job uses the existing ephemeral `GITHUB_TOKEN` with only
`contents: write`, matching the existing collector's content-write scope.

## Separation and provenance

The archive is a separate orphan branch, never merged into `main`. This workflow
never modifies, deletes, reconciles, or reclassifies this fork's own `raw/`,
`derived/`, `status/`, poll schedules, or Railway service. Upstream observations
and local observations are distinct sources. The upstream Git history is not
mirrored, and code/actions from upstream are never checked out or executed.
Upstream MIT attribution is preserved in the readable `LICENSE.upstream` file,
and the original license bytes are also archived and hashed.

Every run resolves upstream's default branch to one immutable commit. The full
recursive tree is inspected against a strict allowlist: `raw/YYYY-MM-DD/HHMMSS.json.gz`,
`derived/YYYY-MM-DD.csv`, nine explicitly named JSON/JSONL status files, and
`LICENSE`. Dates before 2026-07-04 are excluded. Unexpected data modes, malformed
paths, truncated trees, missing license/raw data, invalid hashes, and individual
source/archive blobs of 95 MiB or more fail closed. Such an oversized file needs
a reviewed sharding change; it is never silently omitted.

Every downloaded file must match both its tree byte count and Git blob SHA-1.
The manifest additionally records SHA-256 of original and stored bytes. Raw
`.json.gz` files retain their exact bytes. Other files are gzip-compressed with a
zero timestamp; `encoding: gzip` means decompress once to recover original bytes.
`encoding: identity` means stored bytes already are the original `.json.gz` file.

## Layout and reading data

- `manifest.json`: source repository, last fully completed source commit, and
  per-source-path current archived version, sizes, SHA-1, SHA-256, encoding,
  content-addressed object path, source commit, previous versions and presence.
- `objects/`: immutable `.gz` objects named by their original Git blob SHA.
- `pending.json`: pinned source commit and full target tree for an incomplete
  import; `null` means that target completed.
- `coverage.json`: counts of expected and verified files, snapshot completeness,
  per-day source/archived raw sample counts, absent dates and maximum observation
  gaps including day edges. `first_sample_utc` / `last_sample_utc` are seconds
  since UTC midnight. Timestamp measurements come from raw filenames.
- `LICENSE.upstream`: original upstream MIT license notice.

For a source path, look up its `object` in `manifest.json` and download that object
from `upstream-data`. Decompress only if its manifest `encoding` is `gzip`.
Check original byte size, SHA-256 and Git blob SHA before analysis. For example,
in an archive checkout (a full checkout downloads all archived objects):

```python
import gzip, hashlib, json
from pathlib import Path
manifest = json.loads(Path('manifest.json').read_text())
entry = manifest['files']['derived/2026-07-04.csv']
stored = Path(entry['object']).read_bytes()
assert hashlib.sha256(stored).hexdigest() == entry['stored_sha256']
data = gzip.decompress(stored) if entry['encoding'] == 'gzip' else stored
assert len(data) == entry['size']
assert hashlib.sha256(data).hexdigest() == entry['sha256']
assert hashlib.sha1(f'blob {len(data)}\0'.encode() + data).hexdigest() == entry['sha']
Path('2026-07-04.csv').write_bytes(data)
```

Do not interpret `all_existing_raw_archived` as uninterrupted sampling. It only
means that all raw files present in the pinned upstream tree are archived. A
missing day has zero observations, even though zero of zero existing files are
copied. `missing_vs_nominal_96` is a comparison baseline, **not a historical SLA**:
early upstream collection was hourly at best. Today's day edges are incomplete
by definition. A calendar date being closed does not make its observations complete.

## Incremental behavior, revisions and interruptions

Every run compares all allowlisted paths and blob SHAs, not commit ancestry or a
moving date window. Historical CSV rebuilds, upstream history squash/rebase,
late-arriving old files and missed runs are therefore discovered. When a path
changes, its previous object stays in `objects/` and its metadata stays in
`previous_versions`. When upstream deletes a path, the last local copy remains;
`present_in_upstream` becomes false after a complete scan. No archived object is
garbage-collected or force-pushed by this workflow. A returned file is marked
present again at completion. This cannot recover content upstream deleted before
our first archive captured it.

The archive uses partial-clone (`blob:none`) fetch plus Git index/tree operations;
it does not check out old data. Ordinary daily runs transfer metadata and changed
files, not the entire historical dataset. Binary archives are content-addressed;
text datasets are losslessly compressed to reduce repository growth. Git delta
search is disabled to avoid hydrating old objects during push. Bootstrap processes
at most 512 files / 128 MiB of source bytes in each checkpoint, far below GitHub's
single-push limit, rather than pushing the entire ~2.56 GB uncompressed source
snapshot at once. Each stored blob is below 95 MiB. Initial raw archives still
consume significant repository storage; revisions are deliberately retained.

Each checkpoint publishes objects, manifest, coverage and pending target in a
single non-forced Git commit/push, then verifies the remote head. Only the final
checkpoint advances `complete_commit`. During partial imports, per-file
`source_commit` and `pending.json` distinguish mixed-version progress from a
completed snapshot. If download, validation or push fails, the run fails visibly;
the next manual/daily run starts from the last successfully published checkpoint.
It completes that pinned target first, then a newly pinned current target. At most
two targets are processed per run, so a moving upstream cannot keep it running
forever. The branch has its own concurrency group and never races the poll writer.
An external conflicting write causes a non-fast-forward failure; rerun after
inspection rather than force-push. A no-change complete target is a Git no-op.

If an unfinished source commit becomes unavailable after an upstream history
rewrite, recovery stops rather than claiming success. An operator should inspect
`pending.json` and the last `complete_commit`, then explicitly decide how to
abandon that target; do not delete retained objects. If GitHub rejects a blob or
repository growth, pause and review a sharded/archive-storage design before
changing destinations. No external storage account or new repository is used.

## Verification and operation

Run `python3 -m unittest discover -s tests -v` and
`python3 -m py_compile scripts/*.py`. Tests use a temporary local bare repository
for real commit/push/restart/no-checkout behavior and mocked upstream HTTP bytes;
they do not execute an upstream collector or write a production branch.

After merge, inspect the `sync-upstream` Actions run and its summary. Success
requires `pending.json = null`, `coverage.json.snapshot_complete = true`, and
`verified_files == expected_files`; compare `snapshot_commit` with the manifest's
`complete_commit`. Read per-day gaps separately. A created PR, code test pass,
or one successful checkpoint is **not** evidence that the historical import
finished. GitHub failure notifications apply; no workflow dispatches another
workflow, creates credentials, or expands account permissions.
