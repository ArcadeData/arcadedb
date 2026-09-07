# #7209 - HA: Raft snapshot markers are never pruned

`cleanupOldSnapshots` is a no-op without an `.md5` companion.

## Root cause

`ArcadeStateMachine.registerSnapshotMarker` writes a zero-byte `snapshot.<term>_<index>` marker and
then asks Ratis to retain only the newest:

```java
storage.cleanupOldSnapshots(new SnapshotRetentionPolicy() {
  @Override public int getNumSnapshotsRetained() { return 1; }
});
```

Ratis 3.3.0's `SimpleStateMachineStorage.cleanupOldSnapshots` (verified against the
`ratis-server-3.3.0-sources.jar` in `~/.m2`) only advances its delete index after it has counted
`numSnapshotsRetained` markers that **have** an md5 companion:

```java
for (int i = 0; i < allSnapshotFiles.size(); i++) {
  final SingleFileSnapshotInfo snapshot = allSnapshotFiles.get(i);
  if (snapshot.hasMd5()) {
    if (++numSnapshotsWithMd5 == numSnapshotsRetained) { deleteIdx = i + 1; break; }
  } else {
    LOG.warn("Snapshot file {} has missing MD5 file.", snapshot);
  }
}
if (deleteIdx > 0) { ...delete the tail... }
```

ArcadeDB writes no `.md5` companion by design (the marker is a placeholder; the database files on
disk are the real snapshot - see #6991), so `numSnapshotsWithMd5` never reaches 1, `deleteIdx` stays
`-1`, and nothing is ever deleted, whatever retention policy is passed.

Consequence: one inode and one directory entry per checkpoint, for the life of the node, plus a
directory scan over all of them on every `getSingleFileSnapshotInfos()` - which runs once per
checkpoint (Ratis's own `StateMachineUpdater.takeSnapshot` calls `cleanupOldSnapshots` right after
`takeSnapshot`) and once per restart (`SimpleStateMachineStorage.init` -> `getLatestSnapshot`).

`RaftLogCompactionScheduler` triggers a local snapshot on a wall-clock cadence, and drops the
creation gap to 1 under disk pressure, so the accumulation rate is not bounded by write volume.

## Option chosen

The issue offered two options.

**Option 2 (write the `.md5` companion)** was rejected: it doubles the file count per marker, makes
`SimpleStateMachineStorage.findLatestSnapshot` start preferring md5-bearing markers over
md5-less ones (changing which marker wins in a mixed pre/post-upgrade directory), publishes a digest
on a `FileInfo` that ArcadeDB's own resync path never verifies, and still leaves the pre-upgrade
accumulation on disk forever because those markers have no companion either.

**Option 1 (prune directly) was implemented.** The pruning is a dozen lines, depends on nothing but
Ratis's public `SNAPSHOT_REGEX`, deletes deterministically, and cleans up the legacy accumulation.
The `cleanupOldSnapshots` call, which provably deletes nothing for ArcadeDB, is removed.

Note that removing our call does **not** make the #6991 warning filter unnecessary: Ratis's
`StateMachineUpdater.takeSnapshot` calls `cleanupOldSnapshots` itself after every snapshot
(`ratis-server` 3.3.0, `StateMachineUpdater.java:301`), so the same warn loop still runs on a path
ArcadeDB does not own. The filter stays.

## Completeness

### 1. Invariant

> After `registerSnapshotMarker(term, index)` returns `true`, the Raft state-machine directory holds
> no `snapshot.<t>_<i>` marker whose `i` is strictly below `index` - and a node restart prunes any
> such marker left over from before this fix.

### 2. Every way to violate it

Writers of a `snapshot.*` marker anywhere in the repo:

```
$ grep -rn "getSnapshotFile\|updateLatestSnapshot\|new SingleFileSnapshotInfo\|cleanupOldSnapshots\|SnapshotRetentionPolicy" \
    --include="*.java" --exclude-dir=.worktrees .
ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java:55:  import ...SnapshotRetentionPolicy;
ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java:1314: final File snapshotFile = storage.getSnapshotFile(term, index);
ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java:1323: storage.updateLatestSnapshot(new SingleFileSnapshotInfo(
ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java:1329: storage.cleanupOldSnapshots(new SnapshotRetentionPolicy() {
<remaining hits are test files and javadoc prose>
```

`registerSnapshotMarker` is the only production writer. Its callers:

```
$ grep -rn "registerSnapshotMarker" --include="*.java" .
.../ArcadeStateMachine.java:1284:    if (!registerSnapshotMarker(term, currentIndex)) {          # takeSnapshot()
.../ArcadeStateMachine.java:1565:      if (!registerSnapshotMarker(snapshotTerm, snapshotIndex))  # notifyInstallSnapshotFromLeader()
.../Issue6111StaleSnapshotReadFloorTest.java:674                                                   # test, reflective
.../RatisSnapshotDigestWarningFilterTest.java:297                                                  # test, reflective
```

Other `StateMachine` implementations that could own a second state-machine directory:

```
$ grep -rn "implements StateMachine\|extends BaseStateMachine" --include="*.java" --exclude-dir=.worktrees .
ha-raft/src/test/.../OriginNodeSkipIT.java:274:  static class OriginTrackingStateMachine extends BaseStateMachine
ha-raft/src/test/.../RaftHAServerIT.java:236:   static class CountingStateMachine extends BaseStateMachine
ha-raft/src/main/.../ArcadeStateMachine.java:120: public class ArcadeStateMachine extends BaseStateMachine
```

Both other implementations are test fixtures and take no snapshots.

Ratis-side callers of `cleanupOldSnapshots` (ratis-server 3.3.0 sources):

```
$ grep -rn "cleanupOldSnapshots" ratis-server-3.3.0-sources/
org/apache/ratis/statemachine/impl/BaseStateMachine.java:185       # the no-op default storage
org/apache/ratis/statemachine/impl/SimpleStateMachineStorage.java:108
org/apache/ratis/server/impl/StateMachineUpdater.java:301          # after every takeSnapshot()
```

### 3. Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `takeSnapshot()` -> `registerSnapshotMarker` (leader/local periodic checkpoint, `RaftLogCompactionScheduler`) | yes | yes - `repeatedCheckpointsLeaveExactlyOneMarker` |
| `notifyInstallSnapshotFromLeader()` -> `registerSnapshotMarker` (follower install) | yes | yes - `registrationFromTheFollowerInstallPathPrunesToo` drives the shared private writer at increasing indices, as that path does |
| `initialize()` on a node carrying markers written before this fix | yes | yes - `startupPrunesMarkersLeftByEarlierVersions` |
| Ratis's own `StateMachineUpdater` -> `cleanupOldSnapshots` after each snapshot | n/a - argued | Still a no-op for ArcadeDB (no `.md5`), so it deletes nothing and cannot resurrect anything. It remains the reason the #6991 digest-warning filter is installed; `RatisSnapshotDigestWarningFilterTest.theRatisInitiatedCleanupIsSuppressedToo` already pins that. |
| Non-marker files in the state-machine directory (`.md5`, `.tmp`, `.corrupt`, `raft-meta`, ...) | n/a - argued | The prune matches `SimpleStateMachineStorage.SNAPSHOT_REGEX` (`snapshot\.(\d+)_(\d+)`, anchored with `matches()`), so nothing else is a candidate. Pinned by `pruningLeavesNonMarkerFilesAlone`. |

### 4. Reachability

`ArcadeStateMachine` is the state machine `RaftHAServer` registers with Ratis, and
`registerSnapshotMarker` is on both the periodic-checkpoint and the follower-install paths, neither
of which is behind a feature flag. `initialize()` runs on every HA start. No new configuration is
introduced, so nothing gates the fix off.

### 5. Residual risk

- Two markers can share an index and differ only in term (`snapshot.5_90` and `snapshot.7_90`). The
  prune deletes strictly below the retained index, so both survive. Deleting the lower-term one is
  deliberately not attempted: `SimpleStateMachineStorage.updateLatestSnapshot` keeps the *previous*
  info on an equal index, so the live `getLatestSnapshot()` may point at exactly that file, and
  deleting it would leave the storage referencing a path that no longer exists. This is bounded, not
  a leak: a snapshot at an unchanged applied index requires a term change with zero entries applied
  in between, and `RaftLogCompactionScheduler` always passes a creation gap of at least 1, which
  makes Ratis skip the snapshot when no entries were applied.
- Pruning is best-effort. A delete that fails (permissions, a Windows share holding the handle) is
  logged at FINE and leaves one stale directory entry; snapshot registration still succeeds, because
  failing a checkpoint over a cosmetic cleanup would block log purge and is strictly worse.
- The startup prune only runs when a marker already exists; a directory holding nothing but
  `.tmp`/`.corrupt` leftovers is untouched. Those are Ratis's to clean and ArcadeDB never writes them.

## Changes

- `ArcadeStateMachine.registerSnapshotMarker` - the `storage.cleanupOldSnapshots(policy)` call is
  replaced by `pruneObsoleteSnapshotMarkers(parentDir, index)`.
- `ArcadeStateMachine.pruneObsoleteSnapshotMarkers(File, long)` - new. Deletes every
  `snapshot.<term>_<index>` in the state-machine directory whose index is strictly below the retained
  one, best-effort, matching only `SimpleStateMachineStorage.SNAPSHOT_REGEX`.
- `ArcadeStateMachine.pruneSnapshotMarkersAtStartup()` - new. One-shot sweep in `initialize()`, after
  `storage.init()`, so an upgrading node does not carry its old accumulation until the next checkpoint.
- `ArcadeStateMachine.initialize()` - comment now records why the #6991 digest-warning filter survives
  the removal of ArcadeDB's own `cleanupOldSnapshots` call.

### One existing test was re-pointed

`RatisSnapshotDigestWarningFilterTest.withoutTheFilterTheMissingDigestWarningReachesTheLog` is the
arming check for the #6991 filter: with the filter off, registering a marker had to produce the
"Snapshot file ... has missing MD5 file." warning, so the suppression assertions could not pass
vacuously. That warning is emitted **only** inside `SimpleStateMachineStorage.cleanupOldSnapshots`,
which this fix stops calling from `registerSnapshotMarker`, so the test went red.

It is re-pointed at the call site that still runs in production - Ratis's own
`StateMachineUpdater.takeSnapshot` -> `cleanupOldSnapshots`, which is exactly what its sibling
`theRatisInitiatedCleanupIsSuppressedToo` already drives. The assertion, its description and the
suppression tests around it are unchanged; only the two lines that reach the emitting code moved.
Left as it was, the test would have been pinning a code path this PR deletes.

## Test results

New: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue7209SnapshotMarkerPruningTest.java`,
7 tests. Before the fix 4 of them failed (8 markers after 8 checkpoints, 3 after 3 installs, the
obsolete marker still present, and the startup sweep absent); 2 pin behaviour that must not regress
and passed both before and after; the 7th came out of the adversarial pass below and was proved able
to fail by removing the guard it pins (3 warnings against a baseline of 2).

```
$ mvn -o test -pl ha-raft -DexcludedGroups=benchmark,vector,slow
[INFO] Tests run: 1213, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

Earlier attempts at that lane aborted mid-run with
`GrpcServicesImpl ... Failed to bind to address 0.0.0.0/0.0.0.0:2434` -> `System.exit(1)`, first on
`WaitForApplyTest` and then on `LeaveClusterTest`. That is the fixed-port collision `CLAUDE.md`
documents, not a consequence of this change: `lsof -nP -iTCP:2434 -sTCP:LISTEN` named a JVM running out
of a different worktree on this machine. The green run above was taken once that port was free.

## Impact

- Bounded state-machine directory: one marker after a checkpoint instead of one per checkpoint for
  the life of the node, so the per-checkpoint and per-restart directory scans stay O(1).
- The #6991 warning now fires at most once per checkpoint rather than once per accumulated marker,
  because the loop it lives in has one entry to walk. The filter still suppresses it either way.
- No configuration, no wire format and no on-disk format change. A node that downgrades after this
  fix simply resumes accumulating.

## Adversarial pass

The orchestrator's Phase 1.5 spawns an isolated subagent to write the follow-up issue it would file
against this patch. No `Task` tool was available in this session, so the pass was run by hand against
the diff instead. That is a weaker check - the reviewer had already been convinced by its own
reasoning - and it is recorded here rather than glossed over.

1. **A fresh node would log the Ratis directory-scan warning one extra time per boot.** Real, and
   fixed here. `SimpleStateMachineStorage.loadLatestSnapshot()` caches nothing when its directory scan
   fails, so on a node whose state-machine directory does not exist yet, every `getLatestSnapshot()`
   call repeats the scan and re-logs `"Failed to updateLatestSnapshot from ..."` - a WARNING #6991
   deliberately does not filter, because that logger also reports genuine I/O failures. The startup
   sweep added a third call. It now checks the directory exists before asking, and
   `theStartupSweepDoesNotRepeatTheDirectoryScanWarning` compares the state machine against the raw
   `storage.init()` + `reinitialize()` sequence rather than a hardcoded count. Verified armed: with
   the directory check removed the test reports 3 warnings against a baseline of 2.

2. **Could the prune delete the file the live `getLatestSnapshot()` points at?** Not real, with
   evidence. Deletion is strictly below `keepIndex`, and `keepIndex` is never above the live latest:
   at registration `keepIndex = index` while `updateLatestSnapshot` returns
   `max(previous, index) >= index`; at startup `keepIndex = latest.getIndex()`. The equal-index,
   differing-term case is the one that could invert this, and it is excluded by the strict comparison
   - see Residual risk.

3. **Concurrent registration from `takeSnapshot()` and `notifyInstallSnapshotFromLeader()`.** Not
   real. Every interleaving leaves the highest-index marker on disk, because a prune only ever removes
   indices below the one its own call registered. The one interleaving that removes a file another
   thread just wrote (a lower-index install racing a higher-index checkpoint) already reported the
   higher index through `getLatestSnapshot()` before this change, so what Ratis observes is unchanged.

4. **A directory named like a marker.** Not real in effect: `File.delete()` returns false on a
   non-empty directory and the failure is logged at FINE, so it degrades to a no-op.

5. **A Ratis upgrade that renames `SNAPSHOT_REGEX`.** Not real as a risk to ship: it is a
   `public static final` field on a public class, and its removal is a compile error, not a silent
   behaviour change - which is strictly better than the silent no-op this issue is about.

No finding was left out of scope, so no follow-up issue was filed by this pass.
