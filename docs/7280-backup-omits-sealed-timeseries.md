# #7280 — a full backup silently omits every sealed TimeSeries segment

## Problem

`FullBackupFormat` archives the two configuration files plus every **page** file, taken either from
`PageSnapshot.getFiles()` (`backupFromSnapshot`) or from `FileManager.getFiles()` (`backupFromFrozenFiles`).
`TimeSeriesSealedStore` opens `<base>.ts.sealed` with plain `RandomAccessFile`/`FileChannel` I/O and never
registers it as a `ComponentFile`, so it appears in neither enumeration. Every compacted historical sample is
absent from the archive, the restore reports success, and the loss is visible only to a later range query.

The same gap was found and fixed on the HA snapshot-ship path under #4382 (`SnapshotHttpHandler`); the backup
format never got the equivalent treatment.

## Root cause

Two independent defects, not one:

1. **Presence.** Neither backup path enumerates `.ts.sealed`.
2. **Consistency.** A `.ts.sealed` file is replaced as a whole file, outside the page snapshot's point-in-time
   window and outside the flush suspension. Simply adding a directory listing (the `SnapshotHttpHandler`
   copy-paste) pairs a sealed image read at time `T` with a page image fixed at `t0 <= T`, which can duplicate
   samples. See the interleaving analysis below.

## Interleaving analysis — why a directory listing alone is not enough

Compaction (`TimeSeriesShard.compactInternal`) is crash-safe in three committed steps:

- **Phase 0** (under `compactionLock.writeLock`): commits `compactionInProgress = true` and
  `compactionWatermark = <sealed block count before compaction>`.
- **Phases 1-3**: lock-free, writes `.ts.sealed.tmp`.
- **Phase 4c** (under `compactionLock.writeLock`): `Files.move(tmp -> .ts.sealed, ATOMIC_MOVE)`, then
  `mutableBucket.clearDataPages()` and `compactionInProgress = false`, **committed in one transaction**.

Recovery on open (`TimeSeriesShard` constructor, lines 193-203) truncates the sealed store back to the
watermark whenever `compactionInProgress` is still true.

Let `P` be the page image the backup captures (t0 for the snapshot path, the freeze point for the frozen path)
and `Z` the `.ts.sealed` image, read at `T >= P`.

| Compaction position | Page image `P` | Sealed image `Z` | Restored result |
|---|---|---|---|
| No compaction in `[P, T]` | consistent | consistent | correct |
| Phase 0 committed **before** `P`, Phase 4c in `(P, T]` | flag `true`, watermark set, samples still in the mutable bucket | post-swap, extra blocks | **correct** — open-time recovery truncates `Z` back to the watermark |
| Phase 0 **and** Phase 4c both inside `(P, T]` | flag `false`, samples still in the mutable bucket | post-swap, extra blocks | **duplicated samples** — nothing tells recovery to truncate |

The third row is the one a plain directory listing ships. Compaction runs on the maintenance scheduler every
60s per type; a backup of a real database is longer than that.

The mirror-image ordering (read `Z` before establishing `P`) is strictly worse: it turns duplication into
**loss**, because a Phase 4c landing in between clears the mutable pages the archived sealed image does not yet
carry.

## The invariant

> A full backup archive carries, for every TimeSeries shard, a `.ts.sealed` image that pairs with the page
> image in the same archive either identically or as a torn compaction the restored database's own open-time
> recovery repairs — so the restore reproduces the complete sample set with neither loss nor duplication.

The second clause is what makes the fix cheap: the pause only has to exclude a **whole** compaction from the
window, not every compaction, because a compaction that started before `P` is already covered by the
`compactionInProgress` watermark recovery.

## Fix

1. `TimeSeriesSealedStore.FILE_EXTENSION` + `TimeSeriesSealedStore.listSealedFiles(File)` — one definition of
   "which files in a database directory are sealed stores", replacing the two hand-rolled listings in
   `SnapshotHttpHandler`. `endsWith(".ts.sealed")` deliberately excludes `.ts.sealed.tmp` (compaction scratch)
   and `.ts.sealed.incoming` (HA install staging).
2. `TimeSeriesCompactionPause` (new, in `com.arcadedb.engine.timeseries` because `getCompactionLock()` is
   package-private) — holds every shard's `compactionLock.readLock()` of every TimeSeries type in the database.
   Blocks Phase 0 / 4a / 4c, all of which need the write lock; leaves appends running (they take the read lock).
3. `FullBackupFormat` archives the sealed stores on both paths, with the pause held across
   `[page image established, sealed images fully read]`.
   - The pause is acquired at the top of each backup attempt, **outside `executeInReadLock` and outside the
     flush suspension**, for two independent reasons documented on `pauseCompaction()`:
     - **Outside the database read lock**, because compaction's own order is compaction-write-lock first,
       database read lock second (`LocalDatabase.commit()` runs under the read lock). Taking them the other way
       round closes a cycle with any waiting DDL: the database lock is a `ReentrantReadWriteLock`, a queued
       writer stops new readers from barging, so the compaction could not commit, the backup could not take the
       compaction lock, and the DDL could not take the write lock the backup's read lock holds.
     - **Outside the flush suspension**, because a compaction in Phase 4c holds the compaction write lock while
       it commits, and a commit throttled by a suspension this thread already took would never release it.
   - Snapshot path: released **immediately after** the sealed files are archived — the page bytes are streamed
     afterwards from the already-fixed t0 window, so compaction is held for the sealed-store copy only and not
     for the whole backup. This preserves what #6075 bought. Safe because `ParallelZipArchiveWriter.addFile`
     and `addEntry` read their input to the end before returning (the worker threads only compress), so no
     sealed file is still being read at that point.
   - Frozen path: held for the whole callback; that path already freezes everything for its duration.

## Completeness

### Invariant

Stated above.

### Enumeration (commands run)

`grep -rn "getFileManager().getFiles()" --include='*.java' . | grep -v /test/`

```
ha-raft/.../SnapshotHttpHandler.java:537          ha-raft/.../SnapshotHttpHandler.java:648
ha-raft/.../RaftReplicatedDatabase.java:3112      ha-raft/.../PostVerifyDatabaseHandler.java:178
integration/.../FullBackupFormat.java:211         server/.../ServerSecurityDatabaseUser.java:280
engine/.../LocalSchema.java:283,428               engine/.../SortedIndexBuildRecoveryMarker.java:70,102,155
engine/.../PaginatedSparseVectorEngine.java:1621,1826,1913,1974
engine/.../LSMVectorIndex.java:1365,1447          engine/.../PageManager.java:825
engine/.../UnreferencedFiles.java:234
```

`grep -rn "snapshot.getFiles()" --include='*.java' . | grep -v /test/`

```
ha-raft/.../SnapshotHttpHandler.java:534,645      ha-raft/.../SnapshotManager.java:182
ha-raft/.../PostVerifyDatabaseHandler.java:155    integration/.../FullBackupFormat.java:190
```

`grep -rn '\.ts\.sealed' --include='*.java' . | grep /main/java/ | grep -v engine/timeseries/`

```
ha-raft/.../SnapshotHttpHandler.java:543,548,652   ha-raft/.../ArcadeStateMachine.java:2501,2703
ha-raft/.../RaftReplicatedDatabase.java:2323       ha-raft/.../SnapshotManager.java:152
engine/.../LocalTimeSeriesType.java:141            engine/.../LocalSchema.java:2121
engine/.../DatabaseChecker.java:237,668
```

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `FullBackupFormat.backupFromSnapshot` (`PAGE_SNAPSHOT_ENABLED=true`) | yes | yes — `sealedSegmentsSurviveABackupRoundTrip[1]`, `theArchiveCarriesOneEntryPerSealedStoreAndNoScratchFile[1]` |
| `FullBackupFormat.backupFromFrozenFiles` (`PAGE_SNAPSHOT_ENABLED=false`) | yes | yes — the same two, `[2]` |
| Compaction committing **inside** the backup window (both paths) | yes | yes — `aCompactionRacingTheBackupNeitherLosesNorDuplicatesSamples[1,2]`, plus `Issue7280CompactionPauseTest.aHeldPauseBlocksCompactionUntilItIsReleased` for the pause itself |
| `FullRestoreFormat` extraction of a `.ts.sealed` entry | no change needed — extracts every entry by name into the database directory, and `FileUtils.checkValidName` accepts `<type>_shard_<n>.ts.sealed` (rejects only empty / separators / `.` / `..`) | yes — the round-trip tests are the evidence |
| `SnapshotHttpHandler.serveSnapshotZip` (HA snapshot ship) | presence already fixed under #4382; **consistency not fixed** — filed as #7337 | pre-existing HA coverage |
| `SnapshotHttpHandler.estimateUncompressedBytes` | already counts sealed files (line 652); switched to the shared helper | pre-existing |
| `SnapshotManager.computeFileChecksums` (`/checksums`) | already covers sealed — reads every non-transient file in the directory | pre-existing |
| `PostVerifyDatabaseHandler` checksums | **not covered** — it iterates the file list, so `.ts.sealed` is never CRC'd on either of its paths. Pre-existing HA-verify gap, unrelated to backup — filed as #7338 | no |
| `Exporter` (logical export) | argued: reads samples through the query engine, which merges sealed + mutable, so it never sees the physical file layer | n/a |
| Retention / downsampling rewriting a sealed store inside the window | argued: they rewrite the sealed store only and never touch the mutable bucket, so the archived pair `pages(P) + sealed(post-retention)` is exactly the state the live database is in a moment later — a valid state, not a torn one. Reads are safe against the rewrite because it is `tmp`-then-`ATOMIC_MOVE`, so an opened stream keeps a complete old inode | no |

### Reachability

`FullBackupFormat` is constructed by `Backup.backupDatabase()` (`integration/.../backup/Backup.java`), which is
what `BACKUP DATABASE`, the server's backup HTTP handler and the console all reach. Both paths are selected at
runtime by `PAGE_SNAPSHOT_ENABLED`, and the tests drive both. `TimeSeriesCompactionPause.acquire` walks
`database.getSchema().getTypes()`, so on a database with no TimeSeries type it acquires nothing and costs one
type-list iteration.

### Adversarial pass

The isolated `general-purpose` subagent this step calls for is not available in this environment (no `Task`
tool), so the pass was run by hand against the two claims the patch most depends on. Both found something:

1. **"The pause is safe to release after the sealed copy."** Only true if the archive writer has finished
   reading by then. Verified: `ParallelZipArchiveWriter.addFile` opens the file and drives `addEntry`, which
   reads the stream to exhaustion on the CALLING thread before returning - the pool threads only deflate chunks
   handed to them ("THE SINGLE THREAD THAT CALLS addFile ... DO NOT START TOUCHING THEM FROM A WORKER").
   `ZipStreamArchiveWriter` is single-threaded outright. Claim holds; the reasoning is now in the code comment
   rather than left implicit.
2. **"Acquiring the pause inside the database read lock is fine."** It was NOT. Compaction takes the
   compaction write lock and then commits, and `LocalDatabase.commit()` runs under the database READ lock, so
   compaction's order is compaction-then-database. The first draft took them database-then-compaction, which
   closes a three-way cycle with a waiting DDL writer (a `ReentrantReadWriteLock` queued writer blocks new
   readers from barging). The 60s budget would have turned it into a failed backup rather than a hang, but a
   backup that fails whenever a `CREATE TYPE` overlaps it is its own bug report. **Fixed in this branch**: the
   pause is now acquired at the top of the attempt, outside `executeInReadLock`, so the backup and compaction
   take the two locks in the same order and there is no cycle.

### Residual risk

- A backup taken **on an HA follower** can still tear: the follower installs a leader's sealed blob through
  `TimeSeriesSealedStore.installSealedFile`, which takes the store's own `directoryLock`, not the shard's
  `compactionLock`, so the pause does not exclude it. Filed as **#7337**.
- `PostVerifyDatabaseHandler` never checksums `.ts.sealed`, so a sealed-store divergence between HA peers is
  invisible to `/verify`. Pre-existing, filed as **#7338**.
- The pause is bounded at 60s. If a shard's compaction write lock cannot be taken inside that budget the backup
  **fails loudly** rather than writing an archive that may restore with duplicated samples.

## Verification

| Command | Result |
|---|---|
| `mvn -o -pl engine test -Dtest='com.arcadedb.engine.timeseries.*Test'` | 415 tests, 0 failures |
| `mvn -o -pl integration test` | 318 tests, 0 failures, 9 skipped |
| `mvn -o -pl integration -Pintegration verify -Dit.test='*Backup*IT'` | 39 tests, 0 failures |
| `mvn -o -pl ha-raft test -Dtest='*Snapshot*Test'` | 185 tests, 0 failures |

### Proof the new tests can fail

Both halves of the fix were reverted in turn and the suite re-run, so neither test is passing for a reason
other than the one it names:

- **Sealed archiving removed, pause kept** — all 6 `Issue7280SealedTimeSeriesBackupIT` cases fail, with the
  message each is for: "every sample, sealed and mutable alike, must come back" (round trip, both paths), the
  missing `Reading_shard_*.ts.sealed` archive entries, and "the archive must be a point in time between the two
  samplings" (the race test, failing as loss).
- **Pause removed, sealed archiving kept** — `aCompactionRacingTheBackupNeitherLosesNorDuplicatesSamples` fails
  on "a repeated timestamp means a post-compaction sealed image was paired with a pre-compaction page image",
  listing ~4,000 duplicated timestamps. Three consecutive runs: 2, 1 and 2 of the two parameterisations red.
  This is the third row of the interleaving table, reproduced.

## Impact

- A full backup of a database with TimeSeries types now restores the compacted history instead of silently
  dropping it. Archives taken before this fix are still incomplete and cannot be repaired after the fact.
- The archive gains one entry per shard. On the snapshot path TimeSeries compaction is held only for the time it
  takes to read the sealed stores, not for the whole backup; on the frozen path it is held for the window that
  path already freezes.
- `SnapshotHttpHandler`'s two hand-rolled `.ts.sealed` listings are now the one shared helper, so the suffix has
  a single definition.
