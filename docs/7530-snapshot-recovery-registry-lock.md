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
