# #7456 - HA snapshot ship still reads the configuration files off the filesystem under the database read lock

Follow-up to #6114, filed from its completeness sweep.

## Root cause

`SnapshotHttpHandler.handleSnapshot` wraps the ENTIRE snapshot transfer in `db.executeInReadLock(...)`, on
both the point-in-time window path and the frozen-files fallback. The only thing that lock protects on the
window path is the pair of configuration files, which `serveSnapshotZip` reads off the filesystem
(`LocalDatabase.getConfigurationFile()` / `LocalSchema.getConfigurationFile()`) AFTER t0, and which
`estimateUncompressedBytes` sizes the same way. A follower resyncing from a multi-GB leader therefore blocks
that leader's DDL (`CREATE TYPE`, `DROP TYPE`, `CREATE INDEX`) for the whole transfer - the longest such
window in the product, and exactly the cost #6114 removed for the full backup.

Two defects, one cause:

1. **Correctness.** The archived `configuration.json` / `schema.json` are read after t0, so they describe a
   moment later than the pages in the same archive. The read lock hid that by excluding DDL for the whole
   transfer.
2. **Availability.** That exclusion is the cost.

## The invariant

> When the HA snapshot ship serves a database through an open `PageSnapshot` window, every byte it archives -
> the pages, the configuration files and the announced uncompressed size - comes from that window's t0, and no
> database read lock is held for the duration of the transfer.

The frozen-files fallback is deliberately outside the invariant: there the page image is the live on-disk one,
held still by the flush suspension rather than by a point in time, so the configuration still has to be pinned
by the lock. That is the same asymmetry `FullBackupFormat` carries since #6114.

## Completeness

### Greps run (in this worktree, on `origin/main` @ 76079418a2)

`grep -rn "getConfigurationFile()" . --include="*.java" | grep -v /target/ | grep -v src/test`

```
ha-raft/.../SnapshotHttpHandler.java:568:      final File configFile = ((LocalDatabase) db.getEmbedded()).getConfigurationFile();
ha-raft/.../SnapshotHttpHandler.java:572:      final File schemaFile = ((LocalSchema) db.getSchema()).getConfigurationFile();
ha-raft/.../SnapshotHttpHandler.java:687:    final File configFile = ((LocalDatabase) db.getEmbedded()).getConfigurationFile();
ha-raft/.../SnapshotHttpHandler.java:691:    final File schemaFile = ((LocalSchema) db.getSchema()).getConfigurationFile();
integration/.../FullBackupFormat.java:247:    origSize += compressFile(archive, ((LocalDatabase) database.getEmbedded()).getConfigurationFile());
integration/.../FullBackupFormat.java:248:    origSize += compressFile(archive, ((LocalSchema) database.getSchema()).getConfigurationFile());
engine/.../LocalDatabase.java:2388:  public File getConfigurationFile() {          <- the declaration
engine/.../LocalSchema.java:2865:  public File getConfigurationFile() {           <- the declaration
```

Four call sites outside the declarations, all reached only from a frozen-files path after this change: the two
in `FullBackupFormat.backupFromFrozenFiles` (line 247/248, the fallback #6114 deliberately left alone) and the
two in `SnapshotHttpHandler` that this change moves under the `snapshot == null` branch.

`grep -rn "openSnapshot(" . --include="*.java" | grep -v /target/ | grep -v src/test` - every consumer of a window:

```
ha-raft/.../SnapshotHttpHandler.java:341        (snapshot ship)          <- fixed here
ha-raft/.../SnapshotHttpHandler.java:427        (/checksums)             <- argued below
ha-raft/.../PostVerifyDatabaseHandler.java:619  (HA verify)              <- filed as #7634
integration/.../FullBackupFormat.java:217       (full backup)            <- fixed by #6114
engine/.../PageManager.java:614                 (the declaration)
```

### Entry-point coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `GET /api/v1/ha/snapshot/{db}` -> `serveSnapshotZip`, window path: the two configuration ZIP entries | yes | yes - `theWindowPathArchivesTheConfigurationCapturedAtT0` |
| `GET /api/v1/ha/snapshot/{db}` -> `estimateUncompressedBytes`, window path: the `X-ArcadeDB-Uncompressed-Bytes` header | yes | yes - `theAnnouncedSizeCountsTheWindowsConfigurationBytes` |
| `GET /api/v1/ha/snapshot/{db}` -> `handleSnapshot`, window path: `executeInReadLock` around the whole transfer | yes | yes - `theWindowPathHoldsNoDatabaseReadLock` |
| `GET /api/v1/ha/snapshot/{db}` -> frozen-files fallback (`suspendFlushAndExecute`) | no, BY DESIGN: keeps the lock and keeps reading the files | yes - `theFallbackPathStillReadsTheConfigurationOffTheFilesystem` and `theFallbackPathStillHoldsTheDatabaseReadLock` |
| `GET /api/v1/ha/snapshot/{db}/checksums` -> `computeChecksums` | no - **argued** | n/a |
| `POST /api/v1/ha/verify/{db}` -> `PostVerifyDatabaseHandler.computeLocalChecksums`, window path | no - **filed as #7634** | n/a |
| full backup -> `FullBackupFormat.backupFromSnapshot` | already fixed by #6114 | existing `Issue6114LockFreeBackupIT` |

**Argued: `/checksums`.** `computeChecksums` builds its answer from `SnapshotManager.computeFileChecksums(dbDir,
snapshot)`, which walks the database DIRECTORY rather than the window's file list, so it CRCs
`configuration.json` and `schema.json` as ordinary directory entries. The read lock still has something to
protect there - a DDL rewriting `schema.json` mid-CRC would make the answer disagree with a peer's for a file
neither window covers - so removing it would be a correctness regression, not a win. This is the same reason
the issue itself gives for leaving it out.

**Filed: the HA verify (#7634).** `PostVerifyDatabaseHandler.computeLocalChecksums` also wraps its window path
in `db.executeInReadLock(...)`, and on that path it reads only `snapshot.getFiles()` plus the sealed stores -
nothing the lock protects. The same removal applies, but a verify reads bytes rather than shipping them and
has its own peer-comparison contract, so it is a separate change with its own test. Out of scope here per the
issue's own framing; filed so it is not left for the reporter.

### Residual risk

- The fallback path still blocks DDL for the whole transfer. That is deliberate and unchanged: with no t0 to
  capture the configuration at, the lock is the only thing keeping it in step with the frozen pages.
- **The microsecond-wide barrier gap becomes reachable during a ship, where it was not before.**
  `PageManager.captureConfigurationFiles` documents it: the t0 barrier holds the FILE-SET monitor, which a DDL
  releases between registering a file and saving the schema, so a window landing in that gap can observe a DDL
  half-applied. Stating this precisely, because the obvious phrasing is wrong: the read lock removed here DID
  keep a not-yet-started DDL out for the whole transfer, so during a ship the gap was previously unreachable
  except by a DDL already past the write lock. Removing the lock makes it reachable - but only during the
  barrier itself, which is one bounded drain rather than the whole transfer, and it is exactly the same
  exposure every full backup has carried since #6114. #7457 (now CLOSED) narrowed it by moving
  `LocalSchema.recordFileChanges`'s save inside the write-locked DDL callback; what is left is the file-set
  monitor's own release, which no reader lock ever covered.
- **A symlinked `configuration.json` / `schema.json` is now shipped rather than silently dropped.**
  `addFileToZip`'s symlink refusal applies to the fallback branch only; the window holds bytes read with
  `Files.readAllBytes`, which follows the link. Argued in the javadoc on `addConfigurationToZip`: the entry name
  is one of two fixed ones and the bytes are the leader's own live configuration, so the refusal's threat model
  (an entry whose content came from an attacker-chosen path) does not apply, and dropping the entry ships a
  follower a database with no schema - the worse outcome. `FullBackupFormat` has behaved this way since #6114;
  this aligns the ship with it. No test pinned the old behaviour: `SnapshotSymlinkTest.zipBuilderSkipsSymlinks`
  reimplements the loop locally and never calls the handler.
- **A database closing while a ship starts now logs one spurious WARNING.** `openSnapshot` is no longer inside
  `executeInReadLock`, so a `PageSnapshotException(CLOSING)` is caught and logged as "falling back to suspending
  the page flush" before the fallback's `executeInReadLock` throws `DatabaseIsClosedException`. The observable
  outcome for the follower is unchanged - the request still fails the same way - so this is noise, not a defect,
  and it is the same shape `FullBackupFormat` carries. Not worth a behaviour change here.
- Nothing else in the product reads these two files off the filesystem while holding a window open: the grep
  above is the evidence.

## Implementation

Three changes in `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java`:

1. `streamThroughPointInTimeImage(db, databaseName, pause, streamer)` - new, package-private and static. The
   branch that used to live inside `handleSnapshot`'s `db.executeInReadLock(...)`, with the lock moved ONTO the
   fallback arm instead of around both. `handleSnapshot` now reads as one call.
2. `addConfigurationToZip(zipOut, db, snapshot, manifest)` - new, package-private and static. Archives
   `snapshot.getConfigurationFiles()` when a window is open, and the two live files otherwise.
3. `estimateUncompressedBytes` sizes the configuration the same way the entries are produced, so the
   `X-ArcadeDB-Uncompressed-Bytes` header describes the archive actually sent.

`addFileToZip` / `addStreamToZip` / `addBytesToZip` became `static` so the extracted helper can call them; the
one `LogManager.instance().log(this, ...)` in `addFileToZip` became `log(SnapshotHttpHandler.class, ...)`. No
other behaviour changed.

## Adversarial pass

The orchestrator's Phase 1.5 calls for an isolated `general-purpose` subagent. **No `Task` tool is available in
this environment**, so the pass was run in-session against the diff rather than by an agent that had not been
persuaded. That is a weaker pass and is recorded as such. Four candidate findings, all resolved before the PR:

| # | Finding | Disposition |
|---|---|---|
| 1 | The window path loses `addFileToZip`'s symlink refusal for the two configuration files | REAL, in scope. Argued and documented in the javadoc and in Residual risk above; behaviour deliberately matches `FullBackupFormat` since #6114 |
| 2 | `estimateUncompressedBytes` had two consecutive `if (snapshot != null)` blocks after the change | REAL, fixed here: one if/else, window arm and filesystem arm each complete |
| 3 | Removing the read lock makes the `captureConfigurationFiles` barrier gap reachable during a ship | REAL, in scope. The first draft of this document repeated the engine comment's phrasing, which understates it; corrected in Residual risk above |
| 4 | A closing database now logs a spurious "falling back" WARNING | REAL, out of scope and not worth a change. Observable outcome unchanged; recorded in Residual risk |

## Verification

| Run | Result |
|---|---|
| `mvnw -o -pl ha-raft test -Dtest=Issue7456SnapshotShipConfigurationFromWindowTest` | 5/5 pass |
| The same 5 tests against the behaviour reverted in place (seams kept, `addConfigurationToZip` forced to the filesystem, `estimateUncompressedBytes` forced to the filesystem, `streamThroughPointInTimeImage` put back under `executeInReadLock`) | **3 failures, 2 passes** - exactly the three window-path tests fail and the two fallback regression guards stay green, so each test fails for its own reason and none passes vacuously |
| `mvnw -o -pl ha-raft test -DexcludedGroups=benchmark,slow,vector` (whole module) | 1497 run, 2 failures - both `ArcadeStateMachinePerDatabaseHaltTest`, **pre-existing**: reproduced on the untouched main checkout with `-Dtest=ArcadeStateMachinePerDatabaseHaltTest` (2 run, 2 failures) |
| `mvnw -o -pl ha-raft verify -DskipITs=false -Dit.test=RaftFullSnapshotResyncIT` | 1/1 pass - the end-to-end follower resync through the changed handler |
| `mvnw -o -pl ha-raft verify -DskipITs=false -Dit.test=Issue5277...,SnapshotAcquireNewDatabaseIT,SnapshotInstallerIntegrationIT,Issue4749...,RaftPeriodicSnapshotCompactionIT` | 7/7 pass |

### Reachability

`handleSnapshot` is the live `GET /api/v1/ha/snapshot/{database}` route a follower hits when it falls behind the
compacted Raft log; the window arm is taken whenever `arcadedb.pageSnapshotEnabled` is on, which is the default.
`RaftFullSnapshotResyncIT` drives the whole path over HTTP with a real cluster, so the changed code is not
reached only by the new unit tests.


## PR

https://github.com/ArcadeData/arcadedb/pull/7636

## Review cycles

### Cycle 1 - `cae3035f`

`claude` reviewed and found no functional bug and nothing blocking ("Solid, well-tested change with honest
documentation of its residual risk surface"). CodeRabbit posted its "review in progress" placeholder and had
still not produced a review 10 minutes later; it re-reviews on every push, so cycle 2 picks it up.

Applied:

- a dedicated regression test for the symlink behaviour change the reviewer said was argued but unguarded -
  `theWindowPathShipsASymlinkedSchemaThatTheFallbackStillRefuses`, which drives the fallback's refusal and the
  window's shipping in one test. It failed on its first run and caught a real fixture bug (a relative symlink
  target resolves against the LINK's directory, so the link was broken and the window saw an absent file);
- the `java.util.function.BiConsumer` import moved back into alphabetical order.

Skipped or deferred, with rationale: `docs/review-deferred-cae3035f.md`. In short - the t0-barrier gap has no
synchronisation point a test could hold it open at (the same reason `Issue6114LockFreeBackupIT` gives for the
same gap), the spurious-WARNING item has nothing in this repository to change, and the reviewer explicitly
asked a maintainer, not the loop, to sign off on the two deliberate behaviour changes.

The entry-point coverage table above is unchanged by this cycle: no new entry point was found, and the symlink
test guards behaviour on two rows that were already covered.
