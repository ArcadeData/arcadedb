# #7530 - Snapshot-swap recovery moves files under an open, registered database

Follow-up to #7449. That issue gave `SnapshotInstaller.recoverPendingSnapshotSwaps` the per-database
maintenance slot, so a scheduled *backup* cannot read a directory the repair is rebuilding. It said
nothing about *queries*: the repair still moved files in and out of a live database directory without
closing the database, without the registry lock and without the 503 window.

## Invariant

> Every `SnapshotInstaller` path that moves files inside a **live database directory**
> (`databases/<name>/`) runs with that database closed and deregistered, under
> `ArcadeDBServer.getDatabasesLock()`, inside the `setSnapshotInstallInProgress` 503 window - and
> leaves the registry exactly as it found it.

The trailing clause is the half the issue asked to get right rather than copy blindly: the pass also
runs at cold start, where `loadDatabases(false)` has deliberately **deferred** a marked directory.
Reopening it there would register a database the second `loadDatabases(true)` pass is supposed to pick
up.

## Analysis

### Why "open" is reachable

Confirmed by reading the tree, not from the issue text:

- `ArcadeDBServer.getDatabase(name, ..., underSnapshotRecovery=false)` serves a registered, open entry
  from the lock-free fast path (`server/src/main/java/com/arcadedb/server/ArcadeDBServer.java:1424`)
  *before* the `isAwaitingSnapshotRecovery` refusal, which sits inside the `db == null || !db.isOpen()`
  branch below it (`:1438`). A registered, open, marked database is therefore served normally.
- `SnapshotInstaller.swapAndReopen` produces that state on purpose on two failure arms: the `atomicSwap`
  failure arm (`:443`, "Leave the pending marker in place") and the failed-open arm (`:471`, "The pending
  marker is intentionally NOT cleared here"). Both call `reopenQuietly`, which reopens through
  `reopenDatabaseUnderSnapshotRecovery` - the entry point that looks past the marker.
- The pass is not startup-only: `ArcadeStateMachine.initialize()` calls it (`:557`) and
  `RaftHAServer.restartRatis` rebuilds the state machine while the node is ONLINE.

### Lock order

The repair takes the `BackupCoordinator` slot first and the registry lock second. That is the same order
`install` uses (slot at `SnapshotInstaller.java:305`, `databasesLock` inside `swapAndReopen` at `:424`),
so the new nesting introduces no inversion. Verified that nothing takes `databasesLock` and then *waits*
on a slot - the only blocking `begin(db, op, waitMs)` call sites are the two in `SnapshotInstaller`, both
outside the lock:

```
$ grep -rn "\.begin(" --include="*.java" server/src/main/java ha-raft/src/main/java | grep -i coordinator
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:1017:    final Operation running = coordinator.begin(databaseName, Operation.BACKUP);
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:1038:    final Operation running = server.getBackupCoordinator().begin(databaseName, operation);
server/src/main/java/com/arcadedb/server/backup/BackupTask.java:116:    final BackupCoordinator.Operation running = coordinator.begin(databaseName, BackupCoordinator.Operation.BACKUP);
ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java:305:          : coordinator.begin(databaseName, BackupCoordinator.Operation.RESTORE,
ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java:970:    final BackupCoordinator.Operation refusedBy = coordinator.begin(databaseName, BackupCoordinator.Operation.RESTORE,
```

None of the three non-`SnapshotInstaller` call sites is inside a `getDatabasesLock()` section
(`ServerControlPlane:1017`/`:1038` and `BackupTask:116` are all at the head of their methods; the
`getDatabasesLock()` sections in that file are at `:1478` and `:1779`).

### The node-wide flag is not nesting-safe

`setSnapshotInstallInProgress` writes an `AtomicBoolean`. Taking it per database in a loop - which is
what the issue asks for, and what keeps the 503 window narrow - means the pass flips it N times. With a
plain boolean, the `finally` of the repair of database A clears the window an `install` of database B is
relying on, because the flag is node-wide and the two run on different threads (the apply thread and the
health-monitor-driven Ratis restart). That race is latent on `main` already: two concurrent
`install`s of different databases have the same shape. Both setter call sites pair `true` with `false` in
a `finally`:

```
$ grep -rn "setSnapshotInstallInProgress" --include="*.java" . | grep -v /target/ | grep -v test
ha-raft/.../SnapshotInstaller.java:395:    server.setSnapshotInstallInProgress(true);
ha-raft/.../SnapshotInstaller.java:399:      server.setSnapshotInstallInProgress(false);
ha-raft/.../SnapshotInstaller.java:761:    server.setSnapshotInstallInProgress(true);
ha-raft/.../SnapshotInstaller.java:769:      server.setSnapshotInstallInProgress(false);
server/.../ArcadeDBServer.java:288:  public void setSnapshotInstallInProgress(final boolean inProgress) {
```

so a depth counter behind the unchanged boolean API is safe for every existing caller.

## Completeness

### 2. Every way to violate the invariant

Every `SnapshotInstaller` routine that renames or deletes entries inside `databases/<name>/`, and its
callers:

```
$ grep -n "recoverSingleDatabase(\|atomicSwap(\|restoreBackup(\|clearLiveDatabaseFiles(\|rollbackToBackup(" \
    ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java
438:        atomicSwap(dbPath, snapshotNew, snapshotBackup);          # swapAndReopen
470:        rollbackToBackup(dbPath, snapshotBackup);                 # swapAndReopen
765:        recoverSingleDatabase(dbPath);                            # reconcileRetainedBackup
794:  private static void rollbackToBackup(...)
801:      clearLiveDatabaseFiles(dbPath);                             # rollbackToBackup
802:      restoreBackup(dbPath, snapshotBackup);                      # rollbackToBackup
966:      recoverSingleDatabase(dbDir);                               # recoverSingleDatabaseHoldingMaintenanceSlot (no coordinator)
982:      recoverSingleDatabase(dbDir);                               # recoverSingleDatabaseHoldingMaintenanceSlot (slot held)
991:  private static void recoverSingleDatabase(final Path dbDir)
1008:        atomicSwap(dbDir, snapshotNew, snapshotBackup);          # recoverSingleDatabase
1024:        restoreBackup(dbDir, snapshotBackup);                    # recoverSingleDatabase
1626:  private static void atomicSwap(...)
1666:          clearLiveDatabaseFiles(dbDir);                         # atomicSwap's own restore
1667:        restoreBackup(dbDir, backupDir);                         # atomicSwap's own restore
1682:  private static void restoreBackup(...)
```

Production callers of the pass itself - one, and it passes a server:

```
$ grep -rn "recoverPendingSnapshotSwaps" --include="*.java" . | grep -v /target/ | grep -v src/test
ha-raft/.../ArcadeStateMachine.java:131:  (javadoc)
ha-raft/.../ArcadeStateMachine.java:136:  (javadoc)
ha-raft/.../ArcadeStateMachine.java:557:        SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDirectory, server);
```

### 3. Entry-point coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `install` -> `swapAndReopen` -> `atomicSwap` / `rollbackToBackup` | already held the lock, the close and the 503 window before this change | yes - `SnapshotInstallSwapLockTest` (#4832), untouched |
| `install` -> `reconcileRetainedBackup` -> `recoverSingleDatabase` | already held all four before this change | yes - `Issue7139RetainedBackupNotDestroyedTest`, untouched |
| `ArcadeStateMachine.initialize` -> `recoverPendingSnapshotSwaps` -> `recoverSingleDatabaseHoldingMaintenanceSlot` -> `recoverSingleDatabase` | **yes - this change** | yes - `Issue7530RecoveryClosesOpenDatabaseTest`, 5 tests |
| Cold start: same path, database deferred by `loadDatabases(false)` and therefore unregistered | **yes** - reopen is conditional on having closed a registered instance | yes - `aDatabaseTheBootScanDeferredIsNotRegisteredByTheRepair` |
| Concurrent `install` of *another* database while the pass flips the node-wide 503 flag | **yes** - the flag became a depth counter | yes - `theInstallWindowSurvivesAPassThatOpensAndClosesItAlongside` |
| `recoverPendingSnapshotSwaps` -> `.acquire-*` staging-dir deletion | **argued** | - |
| `acquireNewDatabase` -> `deleteDirectoryIfExists(dbPath)` / `publishStaging` | **argued** | - |
| Registered but *unresolvable* entry (registered + closed + marked) on the repaired path | **yes** - the lookup failure is caught, the pass continues | yes - `aRegisteredButUnresolvableDatabaseDoesNotAbandonTheWholePass` |
| An unchecked failure repairing one database ending the whole scan / failing Ratis init | **yes** - per-database guard in the scan loop | yes - `anUncheckedFailureRepairingOneDatabaseDoesNotEndTheScan` |
| `recoverPendingSnapshotSwaps(Path)` one-argument overload | **argued** | yes - `recoveryWithoutAServerStillReconcilesTheDirectory` (#7449, untouched) |

**Argued rows, with evidence:**

- `.acquire-*` staging-dir deletion (`SnapshotInstaller.java:895`) operates on
  `databases/.acquire-<name>/`. That name is reserved: `loadDatabases` skips `isReservedDatabaseName`
  directories, and `ArcadeDBServer` resolves a database to `databases/<name>` with no prefix, so no
  `.acquire-*` directory is ever registered or opened. There is no registered instance to close and no
  reader to deflect.
- `acquireNewDatabase` (`:511`) is the never-seen-database path. It refuses to touch `databases/<name>/`
  while the server has it registered: `existsDatabase` is checked at `:522` (delegating to `install`
  instead) and re-checked at `:555` immediately before the delete-and-rename at `:586`/`:592`. The
  directory it deletes is by construction unregistered, so the invariant - which is about a *registered,
  open* database - cannot be violated there. The residual non-atomicity between that re-check and the
  rename is a different property (a create racing an acquire), argued in the code from Ratis's
  single-threaded apply and unchanged by this PR.
- The one-argument `recoverPendingSnapshotSwaps(Path)` overload has no server, therefore no registry to
  lock and no registered database to close. Its only callers are tests (grep above), and the contract is
  documented on the two-argument overload.

### 5. Reachability

The changed code is on the single production call path
`ArcadeStateMachine.initialize()` -> `recoverPendingSnapshotSwaps(dir, server)` (grep above), which Ratis
calls from `RaftServer.start()` - at cold start and again on every `RaftHAServer.restartRatis`. No flag
gates it. `setSnapshotInstallInProgress` is read by `AbstractServerHttpHandler:289` on every HTTP
request, so the 503 window is live. `getDatabasesLock()` is the same monitor `getDatabase`,
`createDatabase` and `swapAndReopen` contend on.

### 7. Residual risk

- The 503 window is node-wide by construction, so while any one database is being repaired, HTTP
  requests for *every* database on the node get a 503. Taking it per database rather than once for the
  pass keeps each window as short as the repair of one directory, which is the narrower of the two
  options the issue named.
- Non-HTTP callers (gRPC, Postgres, Bolt, Mongo wire protocols) do not consult
  `isSnapshotInstallInProgress`. They are excluded by the registry lock and the close, not by the 503 -
  which is the stronger of the two guarantees - but an in-flight request that already holds a reference
  to the `Database` object is not interrupted. That is the same exposure `swapAndReopen` has had since
  #4832 and is not narrowed here.
- A database registered but already **closed** when the pass reaches it is not reopened by the repair,
  because the repair reopens only what it closed. `loadDatabases(true)` and the next `getDatabase` pick
  it up once the marker is gone. See the log line in `closeRegisteredDatabaseForRepair`.


## Adversarial pass

The skill spawns a `general-purpose` subagent for this. **No `Task` tool is available in this
environment**, so the pass was run by the author against the committed diff rather than by a reader who
had not been convinced. That is weaker and is recorded as such. Findings:

1. **The catch around the close was too wide.** *Real, fixed here.* The first cut wrapped the whole of
   `closeLocalDatabaseIfOpen` in a `catch (Exception)`. The justification written next to it - "only an
   entry that is already closed can fail" - is true of the *lookup* and false of the *close*: a
   `getEmbedded().close()` that threw would have been swallowed, leaving the database registered and
   open, and the repair would have gone on to rename files underneath it. That is the exact defect this
   change exists to remove, reintroduced by its own error handling. Narrowed to the lookup
   (`closeRegisteredDatabaseForRepair`); a failing close now propagates out of the pass, loudly, with
   the marker left on disk. `aRegisteredButUnresolvableDatabaseDoesNotAbandonTheWholePass` covers the
   arm that is still caught, and the surefire output confirms it reaches that branch rather than passing
   for another reason:

   ```
   WARNI [SnapshotInstaller] Database 'recov7530' is registered but did not resolve before repairing its
   interrupted snapshot swap: ... an interrupted HA snapshot install left '.snapshot-pending' ...
   ```

2. **"The lookup only refuses a closed entry" was an exhaustive claim with no proof.** *Real, fixed
   here.* Now argued from the code in the javadoc: an open, registered entry is returned by the
   lock-free fast path when ONLINE, and otherwise by the locked path, whose `db == null || !db.isOpen()`
   guard it fails - neither consults the marker. `db == null` cannot occur because `existsDatabase`
   was true and `databasesLock` is held across both calls.

3. **`closeLocalDatabaseIfOpen` was changed to return a boolean nothing read.** *Real, fixed here.* Both
   install call sites ignore it. It is back to `void`, and the mutating half both paths share is
   `closeAndDeregister`.

4. **`.acquire-*` cleanup runs with no lock.** *Not real.* `ACQUIRE_STAGING_PREFIX` is `".acquire-"`
   and `ArcadeDBServer.RESERVED_DATABASE_PREFIX` is `"."`, so `isReservedDatabaseName` is true for
   every such directory and `loadDatabases` skips it; no server resolves a database to that path.
   Nothing registered, nothing to close.

5. **The lock is held across an unbounded file move.** *Not real as a defect.* `swapAndReopen` and
   `reconcileRetainedBackup` have held `databasesLock` across the identical move since #4832. Matching
   them is the point.

6. **`server != null` with a null `BackupCoordinator` now takes the registry lock where it previously
   did not.** *Real, accepted.* `ArcadeDBServer.backupCoordinator` is `final` and initialised inline, so
   this is a test-only shape; the whole `ha-raft` suite (1490 tests) is green, so no test passes a stub
   that would NPE on `getDatabasesLock()`.

## Test results

```
$ mvn -o -pl ha-raft test -Dtest=Issue7530RecoveryClosesOpenDatabaseTest
Tests run: 6, Failures: 0, Errors: 0, Skipped: 0

$ mvn -o -pl ha-raft test -DexcludedGroups=benchmark,slow,vector
Tests run: 1490, Failures: 2, Errors: 0, Skipped: 0
```

The two failures are `ArcadeStateMachinePerDatabaseHaltTest`, **red on `main` before this branch** -
reproduced on a clean `origin/main` worktree at `6c23ce74e3` and filed as **#7630**. Nothing in this
diff touches the apply path they exercise.

Before the fix, 3 of the 6 new tests failed:

```
Issue7530RecoveryClosesOpenDatabaseTest.theRepairClosesAndDeregistersARegisteredOpenDatabaseAndReopensItAfterwards:110
  [the database was closed and deregistered before its files were moved] expected false but was true
Issue7530RecoveryClosesOpenDatabaseTest.theRepairDeflectsHttpClientsWhileItMovesFiles:185
  [HTTP clients are deflected with a 503 while the files are being moved] expected true but was false
Issue7530RecoveryClosesOpenDatabaseTest.theRepairHoldsTheRegistryLockWhileItMovesFiles:155
  [a concurrent registry operation is blocked while the repair moves files] expected false but was true
```

The other two are guards against getting the fix's two subtle choices wrong rather than against the
original bug, so they pass on unmodified code. Both were proved able to fail by temporarily sabotaging
the choice they guard - making the reopen unconditional, and reverting the holder count to a boolean -
which turned both red with their own messages, then reverted.

## Impact

- HA nodes only. Standalone servers never construct `ArcadeStateMachine`, so the repaired path never
  runs for them. The `ArcadeDBServer` change is behaviour-neutral for a single holder.
- The repair of one database now blocks `getDatabase`/`createDatabase` for *every* database on the node
  for the duration of that one directory's repair, and answers HTTP with 503 for the same window. That
  is the cost of the exclusion, and the per-database (rather than per-pass) granularity is what keeps it
  to one directory at a time.

## Recommendations

- The pre-existing red `ArcadeStateMachinePerDatabaseHaltTest` (#7630) means the default `test` lane is
  not running that class on `main`. Worth finding out why before it hides a real regression.
- `isSnapshotInstallInProgress` is consulted by HTTP only. If the 503 window is meant to be a wire-level
  contract rather than an HTTP one, the other wire protocols need the same check - out of scope here,
  and noted under residual risk above rather than filed, because it is a design question about what the
  window is for rather than a defect.


## Review cycles

### Cycle 1 - `f05d893a9b`

`claude` found one real correctness issue and one minor one. Both applied.

**Blast radius of a close failure.** Verified before agreeing, and the reviewer is right on all three
links of the chain:

- `recoverPendingSnapshotSwaps`'s `for (final Path dbDir : stream)` loop has no per-database
  try/catch - only the `DirectoryStream` is wrapped, in `catch (IOException e)`.
- `LocalDatabase.close()` (`engine/.../LocalDatabase.java:387`) declares no checked exception, so a
  close failure is unchecked.
- `ArcadeStateMachine.initialize()` calls the pass at `:557` with no try/catch either.

Before this PR the loop could not be ended by one bad directory: `recoverSingleDatabase` catches its own
`IOException`s and every helper it reaches declares only `IOException`. Closing the database first put an
unchecked failure on the path for the first time - and a close that throws is most likely in exactly the
disk-pressure conditions that leave a marker behind. So one database's close failure could have abandoned
every other pending marker in the pass and then failed the Ratis start for the node. Now caught per
database at SEVERE, the marker left on disk, the scan continuing. `Error` is deliberately not caught.

**`catch (final Exception e)` on the lookup was broader than the reasoning behind it.** Narrowed to
`DatabaseNotAvailableException`, the one the argument is actually built on. Anything else from
`getDatabase` is a bug worth seeing, and is now survivable precisely because of the per-database guard
above.

`anUncheckedFailureRepairingOneDatabaseDoesNotEndTheScan` pins the behaviour: two marked directories,
the first repair throws, and the pass must not throw, must repair the other directory, and must leave
neither the 503 window nor the maintenance slot held. Driven through `recoveryBarrierForTesting` rather
than through a failing `close()`, because that is the seam available without mocking a database; it
throws from inside `recoverSingleDatabase` and unwinds through the same registry lock, reopen, 503 window
and slot a failing close does, so it pins the guard rather than one origin of the failure. Proved able to
fail by replacing the guard with a bare call, which turned it red with the escaping exception:

```
[ERROR] anUncheckedFailureRepairingOneDatabaseDoesNotEndTheScan
        IllegalState simulated unchecked failure while repairing this database
```

Full suite after the change: `Tests run: 1491, Failures: 2` - the same two pre-existing
`ArcadeStateMachinePerDatabaseHaltTest` failures (#7630).

No deferred items.
