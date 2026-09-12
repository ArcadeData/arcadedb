# #7449 - Startup snapshot-swap recovery replaces a database directory without taking the maintenance slot

Issue: https://github.com/ArcadeData/arcadedb/issues/7449
Follow-up to #7444 (`SnapshotInstaller.install` takes the per-database maintenance slot) and #7384
(`BackupCoordinator` admits one whole-database maintenance operation at a time).

## Root cause

`SnapshotInstaller.recoverPendingSnapshotSwaps(Path)` walks `databases/`, finds every directory carrying a
`.snapshot-pending` marker, and finishes or rolls back the interrupted swap through `recoverSingleDatabase` -
which calls `atomicSwap`, `restoreBackup`, `clearLiveDatabaseFiles` and `deleteDirectoryIfExists` against the
*live* database directory. It took no maintenance slot, and could not take one as written: the signature is
`recoverPendingSnapshotSwaps(Path databasesDir)`, with no `ArcadeDBServer` to reach a `BackupCoordinator`
through. `install` was given exactly that slot by #7444; this driver of the same file movement was left out.

## Expected vs actual

- Expected: while this node repairs a database directory, a backup of that database on this node is refused,
  the same way it is refused while an install replaces the directory.
- Actual: `BackupTask.run` takes `Operation.BACKUP` unopposed and goes on to read a directory whose files are
  being moved in and out underneath it.

## Completeness

### 1. The invariant

> Every routine that moves files into or out of a **live** database directory on this node holds that
> database's `RESTORE` maintenance slot for the duration, or has waited a bounded time for it and given up
> loudly.

### 2. Enumerating the ways to violate it

```
$ grep -rn "recoverPendingSnapshotSwaps(" --include='*.java' */src/main
ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java:539:        SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDirectory);
ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java:83: * On startup, {@link #recoverPendingSnapshotSwaps(Path)} detects incomplete swaps
ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java:793:  public static void recoverPendingSnapshotSwaps(final Path databasesDir) {
```

Exactly one production caller. Next, every caller of the per-database routine it drives:

```
$ grep -rn "recoverSingleDatabase(" --include='*.java' .
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java:719:        recoverSingleDatabase(dbPath);
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java:828:        recoverSingleDatabase(dbDir);
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java:836:  private static void recoverSingleDatabase(final Path dbDir) {

$ grep -rn "reconcileRetainedBackup(" --include='*.java' .
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java:300:      reconcileRetainedBackup(databaseName, dbPath, snapshotBackup, pendingMarker, server);
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java:701:  private static void reconcileRetainedBackup(...)
```

Line 719 is inside `reconcileRetainedBackup`, whose only caller (line 300) is inside
`installHoldingMaintenanceSlot` - reached only from `install`, which took the slot at line 247.

Siblings: every other routine in `ha-raft/src/main` that moves files under a database directory.

```
$ grep -rn "restoreBackup(\|atomicSwap(\|clearLiveDatabaseFiles(\|deleteDirectoryIfExists(" --include='*.java' ha-raft/src/main
(25 hits, all in SnapshotInstaller.java: 304,305,333,392,437,494,505,511,531,541,554,564,755,756,809,849,850,857,863,865,885,1471,1472,1484,1498)
```

They partition into: the `install` body (slot held since #7444), `acquireNewDatabase`'s staging lifecycle
(494-564), the `recoverSingleDatabase` body (849-885) and the shared `atomicSwap`/`rollback` helpers
(1471-1498) which are reached only from those.

```
$ grep -rn "sweepOrphanedStagingDirectories" --include='*.java' */src/main
ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java:542
ha-raft/src/main/java/com/arcadedb/server/ha/raft/DeferredDatabaseDeleter.java:130
```

And the two guards that decide whether a backup of a given name can exist at all on this server:

```
$ grep -n "existsDatabase\|getDatabaseNames" server/src/main/java/com/arcadedb/server/backup/BackupTask.java server/src/main/java/com/arcadedb/server/backup/AutoBackupSchedulerPlugin.java
BackupTask.java:84:    if (!server.existsDatabase(databaseName)) {
AutoBackupSchedulerPlugin.java:136:    final Set<String> databaseNames = server.getDatabaseNames();
AutoBackupSchedulerPlugin.java:244:      if (server.existsDatabase(databaseName))
AutoBackupSchedulerPlugin.java:336:    final Set<String> databaseNames = server.getDatabaseNames();
```

### 3. Coverage table

| Entry point (moves files in/out of a database directory) | Covered by fix? | Covered by a test? |
|---|---|---|
| `ArcadeStateMachine.initialize()` -> `recoverPendingSnapshotSwaps` -> `recoverSingleDatabase` | **yes - this PR** | yes (`Issue7449StartupRecoveryTakesMaintenanceSlotTest`) |
| `install` -> `installHoldingMaintenanceSlot` (every install driver) | pre-existing (#7444, line 247) | yes (`Issue7444SnapshotInstallExcludesBackupTest`) |
| `install` -> `reconcileRetainedBackup` -> `recoverSingleDatabase` | argued - runs under the slot `install` already holds | covered transitively by #7444's tests |
| `recoverPendingSnapshotSwaps` acquire-staging arm (`databases/.acquire-<name>` deletion, line 809) | argued - not a live database directory | yes (the per-database-slot test proves the pass is not serialised on it) |
| `DeferredDatabaseDeleter.sweepOrphanedStagingDirectories` (`databases/<staging-prefix>*`) | argued - the database was already dropped and its directory renamed aside | no |
| `acquireNewDatabase` publish/delete of `dbPath` (lines 541/554) | argued - both non-delegating arms run only when `!server.existsDatabase(name)`; the arm that finds it registered delegates to `install`, which takes the slot | no |
| Queries served from a database that is open *while* the recovery pass repairs its directory | **no - filed as #7530** | no |
| `ServerControlPlane` restore / import / triggerBackup, SQL `BACKUP`/`IMPORT` | pre-existing (#7384, #7443) | pre-existing tests |

**Argued rows, with the evidence:**

- *`reconcileRetainedBackup`*: its single call site is line 300, inside `installHoldingMaintenanceSlot`; the
  only caller of that method is `install`, which takes `RESTORE` at line 247 before calling it. The slot is
  not reentrant, so taking it a second time here would deadlock against the caller's own reservation.
- *the acquire-staging arm*: it deletes `databases/.acquire-<name>`, a `.`-prefixed reserved directory
  (`ArcadeDBServer.isReservedDatabaseName`) that `loadDatabases` never opens and `getDatabaseNames` never
  reports. There is no registered database by that name, so no backup entry point can be reading it - and
  there is no database name to key a reservation with either.
- *`sweepOrphanedStagingDirectories`*: it lists only `STAGING_PREFIX + "*"` directories, which
  `DeferredDatabaseDeleter.rename` produced by moving an already-dropped database directory aside. The
  database is gone from the registry before the rename, so `BackupTask.run`'s `existsDatabase` guard (line 84)
  and `AutoBackupSchedulerPlugin`'s `getDatabaseNames` enumeration both exclude it.
- *`acquireNewDatabase`*: the delete at 541 and the publish at 546 are reached only after two
  `server.existsDatabase(databaseName)` re-checks have both answered false (lines 477 and 509); the arm that
  finds the database registered delegates to `install`, which takes the slot. A database that is not in the
  registry has no scheduled `BackupTask` (`AutoBackupSchedulerPlugin` enumerates `getDatabaseNames`) and any
  tick for it returns at `BackupTask.run` line 84.

### 5. Reachability

The issue's own timing rationale does not hold as written, and the fix is reachable for a different reason -
worth writing down so the next reader does not re-derive it:

- `AutoBackupSchedulerPlugin.getInstallationPriority()` returns `AFTER_DATABASES_OPEN`
  (`AutoBackupSchedulerPlugin.java:275`), and `ArcadeDBServer.start()` runs that phase at line 445, *after*
  `startPlugins(AFTER_HTTP_ON)` at line 421 which starts `RaftHAPlugin`
  (`RaftHAPlugin.java:87`). The `configuredPlugins = "AutoBackupSchedulerPlugin," + ...` prepend at
  `ArcadeDBServer.java:1768` orders the plugin *list*, not the phase. So on the cold-start path the scheduler
  does not exist yet while `initialize()` recovers - and a database carrying `.snapshot-pending` is deferred by
  `loadDatabases` (line 1516) anyway, so it is not registered and `BackupTask.run`'s line-84 guard would skip it.
- What makes the fix reachable is `RaftHAServer.restartRatis` (line 1565): the `HealthMonitor` background
  thread builds a **new** state machine (`this.stateMachine = createStateMachine()`, line 1611) and calls
  `raftServer.start()` (line 1647) while the ArcadeDB server is `ONLINE`. Ratis then calls
  `ArcadeStateMachine.initialize()` again, which runs `recoverPendingSnapshotSwaps` again - now with the
  auto-backup scheduler started, the databases registered, and a scheduled tick able to be in flight.
- That last link is asserted from the pinned Ratis release's bytecode, not from memory:

```
$ grep -n 'ratis.version' ha-raft/pom.xml
37:        <ratis.version>3.3.0</ratis.version>

$ cd "$(mktemp -d)" && unzip -oq ~/.m2/repository/org/apache/ratis/ratis-server/3.3.0/ratis-server-3.3.0.jar \
    'org/apache/ratis/server/impl/RaftServerImpl.class' 'org/apache/ratis/server/impl/ServerState.class'

$ javap -p -c org/apache/ratis/server/impl/RaftServerImpl.class   # inside boolean start()
  10: invokevirtual #136  // Method org/apache/ratis/util/LifeCycle.compareAndTransition:(...)Z   <- start()'s NEW->STARTING
  26: invokevirtual #137  // Method org/apache/ratis/server/impl/ServerState.initialize:(Lorg/apache/ratis/statemachine/StateMachine;)V

$ javap -p -c org/apache/ratis/server/impl/ServerState.class | grep 'StateMachine.initialize'
  43: invokeinterface #59,  4  // InterfaceMethod org/apache/ratis/statemachine/StateMachine.initialize:(Lorg/apache/ratis/server/RaftServer;Lorg/apache/ratis/protocol/RaftGroupId;Lorg/apache/ratis/server/storage/RaftStorage;)V
```

  The `invokevirtual` at offset 26 is inside `RaftServerImpl.start()` - the `compareAndTransition` at offset 10
  is that method's own `NEW -> STARTING` entry check - so starting the new server re-runs `initialize` and
  therefore this pass. (First written against 3.2.2, which is also in the local repository; CodeRabbit pointed
  out on the PR that `ha-raft/pom.xml` pins 3.3.0, and the call chain is identical in the version this module
  actually builds against.)

- Nothing opens the directory mid-repair while the database is **closed**: `atomicSwap` skips every
  `.snapshot*`-prefixed entry, so the `.snapshot-pending` marker stays in `dbDir` for the whole repair and is
  deleted only on the last line of `recoverSingleDatabase` - and `ArcadeDBServer.getDatabase` refuses a
  directory carrying it (line 1425). The case where the database is **open** is #7530, below.

So: the slot is taken on a path that really does run concurrently with a scheduled backup, and on the
cold-start path it is a cheap no-op reservation.

### 7. Residual risk

- The wait is bounded by `arcadedb.ha.snapshotInstallBackupWaitMs` and, when it expires, recovery proceeds
  anyway with a WARNING. That is deliberate and matches `install`: a directory left half-swapped is worse than
  a backup that reads a torn one, and the database is deferred by `loadDatabases` until the marker clears.
- `acquireNewDatabase`'s publish window is argued, not fixed. The argument rests on the two `existsDatabase`
  re-checks and on Ratis applying the log single-threaded; `ServerControlPlane.triggerBackup` takes the slot
  before checking that the database exists, so a manual trigger for a name being acquired could in principle
  hold `BACKUP` while the acquisition publishes. It fails immediately afterwards in `executeImmediateBackup`
  and never reads the directory, which is why this is argued rather than filed.
- **#7530**: this PR excludes the *backup*, not the *reader*. The pass still moves files without closing the
  database, without the registry lock and without the 503 window, unlike `reconcileRetainedBackup` three
  methods above it - and two `install` failure arms deliberately leave the `.snapshot-pending` marker in place
  while calling `reopenQuietly`, so "registered, open and marked" is a state the installer produces on purpose.
  Filed rather than fixed here: closing and reopening a database is a different change from admitting a
  maintenance operation, it has to stay a no-op at cold start where `loadDatabases` deliberately defers the
  marked directory, and `setSnapshotInstallInProgress` is node-wide so where to hold it is a design decision.
- The per-database waits are serial, so a node with several interrupted swaps *and* a conflicting operation on
  each could spend up to N x `snapshotInstallBackupWaitMs` in `initialize()`. Bounded and configurable, and it
  needs every one of those databases to be open-and-marked (see #7530) for a backup to be running on them at
  all, so it is documented rather than defended against.
- What that stall actually delays, since `initialize()` can run on the `HealthMonitor` thread during a
  `restartRatis` (asked on the PR review, answered by reading rather than by assurance): the monitor is a
  `newSingleThreadScheduledExecutor` driven by `scheduleWithFixedDelay(this::tickSafely, intervalMs * 2,
  intervalMs, ...)` (`HealthMonitor.java:247,253`). Fixed *delay*, not fixed rate, so the next tick is measured
  from the end of this one - a slow restart postpones the following health check by the stall and cannot pile
  ticks up behind it. A second restart is excluded independently: `restartRatis` holds `recoveryLock` for the
  whole restart (`RaftHAServer.java:1566`). So the knock-on is bounded to "the next health check happens later",
  which is the same exposure a slow Ratis `start()` already carries.
- Nothing else in `ha-raft/src/main` moves files under a database directory: the grep in section 2 is
  exhaustive over that module.

## Changes

### `ha-raft/.../SnapshotInstaller.java`

- New overload `recoverPendingSnapshotSwaps(Path databasesDir, ArcadeDBServer server)`. The existing
  one-argument form delegates to it with a `null` server, so the seven existing call sites in the recovery
  tests keep working unchanged and an embedded caller with no server still gets the old behaviour.
- New `recoverSingleDatabaseHoldingMaintenanceSlot(String databaseName, Path dbDir, ArcadeDBServer server)`,
  which wraps `recoverSingleDatabase` in `BackupCoordinator.begin(name, RESTORE, waitMs)` /
  `end(name, RESTORE)`. The wait reuses `arcadedb.ha.snapshotInstallBackupWaitMs`, the setting `install`
  already uses - no new configuration key. Expiry logs a WARNING naming the operation in the way and the
  setting to raise, and proceeds; the reservation is released only when it was actually taken.
- The database name is the directory name, which is the key `ArcadeDBServer` resolves `databases/<name>`
  from and the key every backup entry point passes to the coordinator.
- The `.acquire-*` arm is untouched: it deletes a reserved staging directory, not a live database.
- New test-only `recoveryBarrierForTesting`, fired at the head of `recoverSingleDatabase` - the same shape
  as the existing `swapBarrierForTesting`, `null` in production.

### `ha-raft/.../ArcadeStateMachine.java`

- `initialize()` passes `server` to `recoverPendingSnapshotSwaps`.

### `ha-raft/src/test/.../Issue7449StartupRecoveryTakesMaintenanceSlotTest.java` (new)

Five tests against a real `ArcadeDBServer`'s coordinator and synthetic pending-swap state.

## Test results

```
$ mvn -o -pl ha-raft test -Dtest=Issue7449StartupRecoveryTakesMaintenanceSlotTest
Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
```

Proof the tests can fail - with `recoverSingleDatabaseHoldingMaintenanceSlot`'s coordinator lookup replaced by
`null` (i.e. the pre-fix behaviour), the two that reproduce the bug go red and the three property guards stay
green, which is what they are for:

```
[ERROR] Tests run: 5, Failures: 2
[ERROR]   ...aBackupIsRefusedWhileRecoveryIsRepairingTheDatabaseDirectory:110
          [the recovery holds this node's maintenance slot while it repairs the directory]
[ERROR]   ...recoveryWaitsForABackupThatIsAlreadyRunning:144
          [the recovery waited instead of moving files under a running backup]
```

Regression sweep:

```
$ mvn -o -pl ha-raft test -DexcludedGroups=benchmark,slow,vector
Tests run: 449, Failures: 0, Errors: 0, Skipped: 0
```

```
$ mvn -o -pl server test -Dtest='BackupCoordinatorTest,Issue7444MaintenanceSlotWaitTest,Issue7384*Test,BackupTask*Test,AutoBackup*Test'
Tests run: 36, Failures: 0, Errors: 0, Skipped: 0
```

Two pre-existing failures seen along the way, neither caused by this change:

- `ArcadeStateMachinePerDatabaseHaltTest` (2 failures) reproduces identically with both changed files
  restored to their `HEAD` content (`git show HEAD:<path> > <path>`, test class removed), so it is red on
  `main`. It never calls `initialize()` - it constructs the state machine and calls `applyTransaction` with a
  null server. It passed in the full-module run and failed only under a `-Dtest` selection, which is
  order-dependent behaviour of that test rather than anything this PR touches.
- `LeaveClusterTest` crashed its forked VM ("terminated without properly saying goodbye"), with
  `Tests run: 0` - the fork died before running anything. It stands up a real 3-node cluster on the fixed
  ports, and `lsof -nP -iTCP -sTCP:LISTEN` showed 2480, 2481, 2482, 2434 and 2435 all held by another agent's
  server at the time. That is the documented port-conflict symptom (Ratis calls `System.exit` from `start()`,
  issue #5418).

## Impact

- HA only, and only on a node that finds a `.snapshot-pending` marker. On a node with no interrupted swap the
  pass does not reach the reservation at all: the loop `continue`s before it.
- Cost when it does reach it: one `ConcurrentHashMap` compute per repaired database, plus - only when a
  conflicting operation is running - a bounded wait on the coordinator's single monitor.
- No new configuration, no new dependency, no change to the recovery decision tree itself.

## Finding ledger

- [x] 1. `recoverPendingSnapshotSwaps` replaces a live database directory without taking the maintenance slot
  - **fixed**, per database, with the bounded wait `install` uses.

## Adversarial pass

The `Task` tool is disabled in this session, so the Phase 1.5 subagent could not be spawned and the pass was
run by hand against the staged diff. That is weaker by construction - the reviewer had already been convinced -
so it was run as a list of specific claims to disprove rather than as a general read.

| Finding | Disposition |
|---|---|
| The pass moves files under a database that can be **open and registered**, with no close, no registry lock and no 503 window - while `reconcileRetainedBackup` does all three around the identical call | **Real, out of scope - filed as [#7530](https://github.com/ArcadeData/arcadedb/issues/7530)** with the verified evidence: the two `install` arms that keep the marker and call `reopenQuietly`, and `getDatabase`'s open-database short-circuit ahead of the marker check |
| The marker could be moved aside by `atomicSwap`, opening a window where a closed database can be reopened mid-repair | **Not real.** `atomicSwap` skips every `.snapshot*`-prefixed entry in both phases, so `.snapshot-pending` never leaves `dbDir`; it is removed on the last line of `recoverSingleDatabase` |
| Taking the slot in the recovery pass could deadlock against `install`, which holds it while calling `reconcileRetainedBackup` -> `recoverSingleDatabase` | **Not real.** `reconcileRetainedBackup` calls `recoverSingleDatabase` directly (line 719), not the new slot-taking wrapper, so the non-reentrant reservation is never taken twice on one thread. Both acquisitions are bounded regardless |
| "`ArcadeDBServer`'s field is final and initialised inline", repeated from #7444's comment into the new one | **Verified, not merely inherited.** `grep -n "backupCoordinator" ArcadeDBServer.java` -> `185: private final BackupCoordinator backupCoordinator = new BackupCoordinator();` |
| "Ratis calls `initialize()` again on a `restartRatis`", the claim the whole reachability story rests on | **Verified** against ratis-server 3.3.0 bytecode - the version `ha-raft/pom.xml` pins (see Reachability above) - not assumed from the framework's documented behaviour |
| The per-database wait could stack across databases and delay a HealthMonitor Ratis restart | **Real but not a defect**: bounded, configurable, and it needs the #7530 state to occur at all. Recorded in residual risk |

## Review cycles

### Cycle 1 - d8cde9c

| Reviewer | Finding | Disposition |
|---|---|---|
| `claude` | No blocking issues. Minor: `initialize()` can now block up to `snapshotInstallBackupWaitMs` per database on the thread driving `RaftServerImpl.start()`, which can be the `HealthMonitor` thread - "worth someone confirming stalling `HealthMonitor` for that duration has no knock-on effects" | **Answered with evidence** in Residual risk: `scheduleWithFixedDelay` on a single-thread scheduler means the next tick is measured from the end of this one (no pile-up), and `recoveryLock` already excludes a concurrent restart. No code change |
| CodeRabbit (inline, `docs/...:138`) | The javap proof cites ratis-server 3.2.2 but `ha-raft/pom.xml` pins `ratis.version` 3.3.0, so it does not establish the behaviour of the dependency this module builds against | **Valid - fixed.** Re-ran the extraction against `ratis-server-3.3.0.jar`; the call chain is identical, and the doc now shows the 3.3.0 output plus the `grep` that establishes the pinned version |
| Codacy | 1 new Info issue: PMD `FieldDeclarationsShouldBeAtStartOfClass` on `recoveryBarrierForTesting` (line 192) | **Valid - fixed.** The whole four-field cluster already sat after `maxZipEntryUncompressedBytes()`; only the new field showed up because Codacy reports the delta. Moved all four above the first method rather than splitting the new barrier away from `swapBarrierForTesting`, which clears three pre-existing violations of the same rule as well. Pure relocation - the fields are independent initialisers with no ordering relationship |

### Cycle 2 - 6b41055

| Reviewer | Finding | Disposition |
|---|---|---|
| `claude` | No correctness issues. Nit: committing a 301-line companion analysis doc under `docs/` is "unusual", and "repo has no existing convention of per-issue docs under `docs/`" | **Declined - the premise is wrong.** `git ls-files docs/` lists 124 tracked files, of which the per-issue analysis docs are an established series: `docs/6965-ha-shared-page-lost-update.md`, `docs/6989-ha-schema-entry-delta.md`, `docs/7122-query-duration-language-tag-unbounded.md`, `docs/7225-unlearn-removed-peer-hosts.md`, `docs/7250-revoke-established-raft-transport.md`, `docs/7259-raft-ha-cluster-bootstrap-settle-gate.md`, `docs/7264-graphimporter-tx-leak-zero-count.md` and more. This file follows that naming and placement exactly |
| `claude` | Nit: `recoveryBarrierForTesting` is a `static volatile` test hook on a production class, accumulating test-only surface | **Acknowledged, no change.** It mirrors the pre-existing `swapBarrierForTesting` in the same class field-for-field; introducing a different mechanism for the sibling hook would cost more than the one reference read per recovered database it replaces. Flagged, not a defect |
| `claude` | Nit: worst case is `N x snapshotInstallBackupWaitMs` for a node with several interrupted swaps and a conflicting operation on each | **Already recorded** in Residual risk, same disposition as the cycle-1 HealthMonitor answer: bounded, configurable, per database rather than per pass by design |
| CodeRabbit | Re-reviewed the cycle-1 correction and resolved the inline thread itself ("the updated `javap` output now matches the `ratis.version` pinned by `ha-raft/pom.xml`"). No new findings | **Nothing to do.** Stale `3.2.2` reference in the adversarial-pass table above corrected to 3.3.0 in this cycle for consistency with the regenerated proof |
| Codacy / Codecov | 0 new issues, 100% diff coverage, all modified lines covered | **Clean** |

CI on 6b41055: the `unit-tests` and `ha-integration-tests` red was re-confirmed as pre-existing on the base commit 99b66927 and unrelated to this change (`Issue7089NaNTransparentSumAvgTest`, `MultiColumnAggregationResultTest`, both `ArcadeStateMachinePerDatabaseHaltTest` methods, and `Issue5569SlotMergeDeleteRaftIT`). Locally, `mvn -o -pl ha-raft test` over the snapshot-recovery classes is green: 45 tests, 0 failures.
