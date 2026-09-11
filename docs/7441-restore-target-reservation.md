# #7441 - restore's "target already exists" check is not atomic with the swap that replaces it

## Problem

`ServerControlPlane.restoreDatabase` and `restoreBackup` sample the target name up front:

```java
final String dbPath = databaseDirectory(databaseName);
if (databaseNameIsTaken(databaseName, dbPath))      // <- registry + filesystem, no lock held
  throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

performRestore(databaseName, dbPath, url, "restore database", listener);
```

and act on it minutes later, in `swapRestoredDatabase`:

```java
if (server.existsDatabase(databaseName))
  dropDatabaseForRestore(databaseName);

synchronized (server.getDatabasesLock()) {
  if (finalDir.exists())
    FileUtils.deleteRecursively(finalDir);
  Files.move(tempDir.toPath(), finalDir.toPath(), ...);
}
```

`ArcadeDBServer.createDatabase` takes `databasesLock` for its own check-then-act, but the restore
check never takes it, and the window between check and swap is the whole duration of the download.
A `create database X` that lands inside that window is dropped and overwritten without a word - by
the one command whose contract is "I will never replace an existing database".

Issue #7384 already closed the restore-vs-restore, restore-vs-backup and restore-vs-import halves
of this with the per-database `BackupCoordinator` slot. What it did not close is create-vs-restore:
`createDatabase` asks the coordinator nothing.

## Invariant

**Once a restore has passed its "target already exists" check, no database can be created on this
server under that name until the restore has finished - and if one appears anyway, the restore
fails instead of destroying it.**

Two clauses, because they are enforced by two different mechanisms and one of them cannot be
exhaustive: a name reservation binds every creator that goes through `ArcadeDBServer`, and a
re-check immediately before the swap catches anything that did not.

## Completeness

### Every creator of a database on a server

```
$ grep -rn "\.createDatabase(" --include='*.java' . | grep -v /target/ | grep -v /src/test/ \
    | grep -v "DatabaseFactory\|createDatabases\|createDatabaseInReplicas"
ha-raft/.../ArcadeStateMachine.java:2986:    server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
grpc-client/.../RemoteGrpcServer.java:478:          .createDatabase(CreateDatabaseRequest...)      # client side, not a server creator
server/.../ServerControlPlane.java:298:    final ServerDatabase database = server.createDatabase(databaseName, ...);
server/.../ServerControlPlane.java:1399:      final ServerDatabase createdDb = server.createDatabase(databaseName, ...);
server/.../http/handler/PostServerCommandHandler.java:394:    controlPlane.createDatabase(databaseName);
grpcw/.../ArcadeDbGrpcAdminService.java:1219:    return controlPlane.createDatabase(name);

$ grep -rn "getDatabase([^)]*, *true" --include='*.java' . | grep -v /target/ | grep -v /src/test/
gremlin/.../GremlinServerPlugin.java:245:          server.getDatabase(dbName, true, true);
server/.../ArcadeDBServer.java:964:    return getDatabase(databaseName, false, true);
server/.../ArcadeDBServer.java:968:    return getDatabase(databaseName, true, true);

$ grep -rn "factory.create()" server/src/main/java/com/arcadedb/server/ArcadeDBServer.java
1016:      DatabaseInternal embeddedDatabase = (DatabaseInternal) factory.create();     # createDatabase
1288:          embDatabase = ... (factory.exists() ? factory.open(...) : factory.create());  # getDatabase

$ grep -rn "\.registerDatabase(" --include='*.java' . | grep -v /target/ \
    | grep -v Profiler | grep -v Retention | grep -v AutoBackup
ha-raft/src/test/java/...Issue7011BootstrapSourceRaceTest.java:75
ha-raft/src/test/java/...Issue7221ForceSnapshotGuardMissingDatabaseTest.java:74
ha-raft/src/test/java/...ArcadeStateMachineAppliedIndexPerDatabaseTest.java:103,132
ha-raft/src/test/java/...Issue7143ForceSnapshotReplayGuardTest.java:69
ha-raft/src/test/java/...ArcadeStateMachineBootstrapBaselinePersistenceTest.java:73
ha-raft/src/test/java/...ArcadeStateMachineBootstrapDivergenceTest.java:76
ha-raft/src/test/java/...ArcadeStateMachineDeferredDropTest.java:79
# no src/main caller
```

A server-side database therefore comes into existence at exactly two places, both inside
`ArcadeDBServer` and both already under `databasesLock`: `createDatabase` (line 1016) and the
`createIfNotExists` arm of `getDatabase` (line 1288). That is where the reservation is checked.

### Every restore that swaps a directory into place

```
$ grep -rn "performRestore\|restoreDatabase(\|restoreBackup(" --include='*.java' . \
    | grep -v /target/ | grep -v /src/test/
server/.../ServerControlPlane.java:1289 restoreDatabase   -> performRestore:1304
server/.../ServerControlPlane.java:1328 restoreBackup     -> performRestore:1348
server/.../http/handler/PostServerCommandHandler.java:424,458 -> the two control-plane methods
grpcw/.../ArcadeDbGrpcAdminService.java:803,827                -> the two control-plane methods
ha-raft/.../SnapshotInstaller.java:686,795,1402,1417  -> a PRIVATE restoreBackup(Path,Path) of its own
integration/.../restore/Restore.java, format/*                -> the CLI/library restore, no server registry
grpc-client/.../RemoteGrpcServer.java:861,879                  -> client side
```

Both transports funnel into the same two control-plane methods, which is where the reservation is
taken and released. `SnapshotInstaller` is the HA snapshot path: it is a different method with the
same name that already holds `databasesLock` across its whole close->swap->reopen (issue #4832),
so it is not the window this issue describes.

### Coverage table

| # | Entry point | Reaches the window? | Outcome |
|---|---|---|---|
| 1 | `create database` over HTTP (`PostServerCommandHandler` -> `ServerControlPlane.createDatabase` -> `ArcadeDBServer.createDatabase`) | yes | **fixed here**, test `aCreateDatabaseOfTheRestoreTargetIsRefusedWhileTheRestoreRuns`, `theHttpCreateDatabaseCommandAnswers409WhileTheNameIsReserved` |
| 2 | `create database` over gRPC (`ArcadeDbGrpcAdminService` -> the same control-plane method) | yes | **fixed here** - same chokepoint; driven in test through `ServerControlPlane.createDatabase`, which is the whole of what the gRPC RPC calls |
| 3 | `ArcadeDBServer.getDatabase(name, createIfNotExists=true, ...)` -> `factory.create()` (`GremlinServerPlugin`, `getOrCreateDatabase`) | yes | **fixed here**, test `getOrCreateDatabaseIsRefusedWhileTheNameIsReservedForARestore` |
| 4 | HA Raft apply `ArcadeStateMachine.applyInstallDatabaseEntry` -> `server.createDatabase` | yes | **fixed here** - same chokepoint. It already has to handle the two `IllegalArgumentException`s `createDatabase` throws for an existing name; this adds a third condition to an existing failure mode rather than a new one. In HA a restore only runs on the leader, and on the leader the create that would have produced the entry is refused before the entry is submitted |
| 5 | `import database` (`ServerControlPlane.importDatabase` -> `server.createDatabase`) | no | **argued**: IMPORT conflicts with RESTORE in `BackupCoordinator.Operation.conflictsWith`, so the import is refused before it reaches `createDatabase` (issue #7384, test `everyHttpRestoreAndImportCommandAnswers409WhileARestoreOfTheTargetRuns`) |
| 6 | a second `restore database` / `restore backup` of the same target | no | **argued**: RESTORE conflicts with everything, same slot, same #7384 test |
| 7 | `ArcadeDBServer.registerDatabase` | n/a | **argued**: no `src/main` caller - the grep above finds only test classes in `ha-raft` |
| 8 | a database appearing out of band: an embedded `DatabaseFactory` in the same JVM, an operator's `mkdir`, a half-finished operation | yes, and not reservable | **fixed here** by the second clause: the swap's own lock section re-checks and turns the silent destruction into a failed restore. Tests `aDatabaseDirectoryThatAppearsDuringARestoreFailsTheSwapInsteadOfBeingDestroyed` (directory) and `aDatabaseRegisteredDuringARestoreIsNotDroppedByTheSwap` (registered database) |
| 9 | another server process sharing the same database directory | yes | **not covered** - see Residual risk. Nothing in this JVM can see it, the same limit `BackupCoordinator` documents for backups |
| 10 | `ArcadeDbGrpcService.getDatabase(String, DatabaseCredentials)` -> its own `DatabaseFactory.create()` and private `databasePool`, bypassing `ArcadeDBServer` entirely | no | **argued** - see below. Raised in review on PR #7452; the sweep above missed it because it greps `ArcadeDBServer`'s creators and this path has its own factory |

### Row 10: the gRPC service's own factory

`ArcadeDbGrpcService` keeps a `databasePool` of its own and, on a miss, opens or creates through a bare
`DatabaseFactory`. That call is unreachable on a server:

```
$ grep -rn "new ArcadeDbGrpcService(" --include='*.java' . | grep -v /target/ | grep -v /src/test/
grpcw/.../GrpcServerPlugin.java:262:      this.grpcService = new ArcadeDbGrpcService(databasePath, arcadeServer, ...)
```

The single production construction site passes the `ArcadeDBServer` the plugin was configured with, so
`arcadeServer` is non-null, and the branch that runs first is:

```java
if (arcadeServer != null) {
  Database db = arcadeServer.getDatabase(databaseName);   // (name, createIfNotExists=false, allowLoad=true)
  ...
  if (db != null) { ...; return db; }
}
```

`ArcadeDBServer.getDatabase` has no `return null` for a missing database - it opens from disk, and
`LocalDatabase.open()` raises `DatabaseOperationException("Database '...' does not exist")`
(`engine/.../LocalDatabase.java:291`). So `db != null` never falls through on a server, and the raw
`DatabaseFactory.create()` below it runs only when `arcadeServer == null`, which is a standalone or
embedded use of the class with no server registry to reserve a name in. A comment at that line now says
so, so the next person auditing creators does not have to re-derive it.

### Reachability

`ArcadeDBServer.createDatabase` is called on every `create database` this server serves (HTTP line
394, gRPC line 1219, both through `ServerControlPlane.createDatabase`); the reservation is taken
by `restoreDatabase`/`restoreBackup`, which are the only two methods either transport calls to
restore. No flag gates either. The pre-swap re-check runs on every `performRestore` that does not
carry `overwrite`. The tests drive the control plane and the HTTP endpoint directly, not a mock.

## Fix

1. `ArcadeDBServer` grows a set of database names reserved for an in-flight restore, guarded by
   the same `databasesLock` that already guards the registry's check-then-act. `createDatabase`
   and the `createIfNotExists` arm of `getDatabase` refuse a reserved name with
   `OperationInProgressException`, which HTTP already answers with 409 and gRPC with `ABORTED` -
   the request is well formed, and retrying once the restore finishes is the fix.
2. `restoreDatabase` and `restoreBackup` take the reservation **in the same `databasesLock`
   section as the existence check** and release it in a `finally` around the whole restore, so the
   check and the swap are two ends of one reservation rather than two independent samples.
3. `swapRestoredDatabase` re-asks `databaseNameIsTaken` **inside the same `databasesLock` section
   that deletes and moves**, unless the caller passed `overwrite`. A name that appeared anyway
   fails the restore instead of being destroyed.
4. The HA-aware `dropDatabaseForRestore` is gated on the same flag. It has to run outside
   `databasesLock` (an HA drop round-trips through Raft and the apply thread takes that lock -
   issue #4832), so a database registered during the restore would have been dropped there,
   before the guard in (3) ever ran. A restore that promised not to replace anything has nothing
   to drop by definition: the target did not exist when the command was accepted.

A reservation is not a lock a creator waits on: `createDatabase` is refused immediately rather
than parked behind a multi-minute download.

## Residual risk

- Row 9: a second process (another server instance, the CLI `restore`) writing the same database
  directory cannot be seen from this JVM. `BackupCoordinator`'s class javadoc already states this
  limit for backups; the reservation inherits it. Out of scope here - closing it needs an on-disk
  lock file, not an in-memory set.
- The reservation is per `ArcadeDBServer` instance, deliberately: an HA test and a co-located pair
  of nodes run several servers with the same database names in one process.
- The pre-swap re-check is a tripwire, not a second claim. It is atomic with the delete-and-move
  it guards - both are in one `databasesLock` section - but it only answers the question at that
  instant, and for anything outside this JVM the answer can be stale the moment it is read. For
  every creator that goes through `ArcadeDBServer` it is the reservation, not the re-check, that
  carries the invariant.

## Changes

- `server/src/main/java/com/arcadedb/server/ArcadeDBServer.java`
  - `restoringDatabaseNames`, a `Set<String>` guarded by `databasesLock`, plus
    `reserveDatabaseNameForRestore` / `releaseDatabaseNameReservedForRestore` /
    `isDatabaseNameReservedForRestore` and the private
    `checkDatabaseNameIsNotBeingRestored`.
  - `createDatabase` and the `createIfNotExists` arm of `getDatabase` - the only two callers of
    `DatabaseFactory.create()` in the class - ask it before creating anything. The refusal is
    `ServerControlPlane.OperationInProgressException`, which HTTP already answers with 409 and
    gRPC with `ABORTED`.
  - The `factory.exists()` arm of `getDatabase` is deliberately untouched: opening a directory
    that is already there is what `restore backup ... overwrite` means to replace, and refusing it
    would take a live database away from its readers for the length of the restore.
- `server/src/main/java/com/arcadedb/server/ServerControlPlane.java`
  - `reserveRestoreTarget` runs the existence check and the claim inside one
    `synchronized (server.getDatabasesLock())` block; `restoreDatabase` and `restoreBackup` call
    it and release the claim from a `finally` around the whole restore.
  - `performRestore` and `swapRestoredDatabase` take a `replaceExisting` flag. When it is false,
    the swap re-asks `databaseNameIsTaken` under `databasesLock` before dropping anything and
    fails the restore if the answer has changed.
- `server/src/test/java/com/arcadedb/server/backup/Issue7441RestoreNameReservationIT.java` - new.

## Test results

```
$ mvn -o -pl server test -Dtest=Issue7441RestoreNameReservationIT
Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
```

The tests can fail. With the two `checkDatabaseNameIsNotBeingRestored` calls and the pre-swap
re-check disabled - the fix's three load-bearing lines, everything else left in place - the same
run is:

```
Tests run: 6, Failures: 4, Errors: 0
  aCreateDatabaseOfTheRestoreTargetIsRefusedWhileTheRestoreRuns
  theHttpCreateDatabaseCommandAnswers409WhileTheNameIsReservedForARestore   expected: 409
  getOrCreateDatabaseIsRefusedWhileTheNameIsReservedForARestore
  aDatabaseDirectoryThatAppearsDuringARestoreFailsTheSwapInsteadOfBeingDestroyed
```

The two that stay green are the ones that assert the claim is released, which it is either way.

Regression runs:

```
$ mvn -o -pl server test -Dtest='Issue7441...,Issue7384ConcurrentRestoreIT,ServerRestoreDatabaseIT,
    ServerBackupDatabaseIT,ServerImportDatabaseIT,ServerControlPlane*Test,BackupCoordinatorTest,
    ReservedInternalDatabaseTest,ServerDefaultDatabasesIT,Issue6778DatabaseNotAvailableExceptionTest'
Tests run: 33, Failures: 0, Errors: 0, Skipped: 0

$ mvn -o -pl server test -DexcludedGroups=benchmark,vector,slow
Tests run: 1052, Failures: 0, Errors: 0, Skipped: 0

$ mvn -o -pl ha-raft test -Dtest='ArcadeStateMachine*Test,Issue7011*,Issue7143*,Issue7221*'
Tests run: 108, Failures: 0, Errors: 0, Skipped: 0

$ mvn -o install -DskipTests
BUILD SUCCESS   (full reactor)
```

An earlier pass of that same suite reported 7 failures, all in `PostClusterAuthSessionHandlerTest`,
and they were environmental rather than a regression: that class hardcodes `http://localhost:2480`,
and a locally installed ArcadeDB 26.9.1 (`/opt/homebrew/Cellar/arcadedb/26.9.1`) is holding it, so
which server that client reaches is not the test's to decide - both runs log `HTTP Port 2480 not
available` dozens of times. The tell is `missingTokenAndUnknownActionAnswer400` getting a **404**
from `/api/v1/cluster/auth-session`, a route 26.9.1 does not have, which puts those requests
outside the test JVM. The class is green in the run above with the same 1052 tests and nothing else
changed, and it touches neither restore nor database creation.

The suite count is 1052 in both runs because Surefire's default includes do not match `*IT`: the
new class runs under Failsafe (and under the explicit `-Dtest=` runs above), as
`Issue7384ConcurrentRestoreIT` does.

## Adversarial pass

No `Task` tool is available in this session - this agent is itself a subagent and cannot spawn
one - so the pass was run by re-reading the diff cold against the issue rather than by an
uncontaminated reviewer. That is a weaker pass than the skill asks for and is recorded as such.

| Finding | Disposition |
|---|---|
| The re-check was placed before `dropDatabaseForRestore`, which runs outside `databasesLock` and takes an HA Raft round-trip. A database registered during the restore was therefore still dropped - by the drop, before the guard could refuse. The guard only caught the *directory* case, which is the weaker one. | **Real, fixed here.** The re-check moved inside the swap's own lock section and the drop was gated on `replaceExisting`. New test `aDatabaseRegisteredDuringARestoreIsNotDroppedByTheSwap`; with only the drop gate reverted it fails at line 316 (`existsDatabase` false - the database had been dropped). |
| `restore backup ... overwrite` skips the re-check entirely, so a directory appearing out of band during an overwrite restore is still destroyed. | **Not a defect.** `overwrite` is the caller stating that the target is replaced; refusing it would break the command's only purpose. Recorded rather than argued away silently. |
| gRPC `CreateDatabase` could create on a replica while the leader restores, putting the create outside the leader's claim. | **Not real.** `ArcadeDbGrpcAdminService` line 1175 gates the RPC on `ha.isLeader()`, and `PostServerCommandHandler` forwards `create database` to the leader (`forwardToLeaderIfReplica`) - the same "everything lands on the leader" argument issue #7384 relies on. |
| The claim could leak if something threw between `reserveRestoreTarget` returning and the `try` being entered. | **Not real.** There is no statement between them in either caller; `reserveRestoreTarget(...)` is immediately followed by `try {`. Also covered by `aFailedRestoreReleasesTheNameReservation`. |
| A new lock-ordering hazard: `reserveRestoreTarget` holds `databasesLock` across the existence check. | **Not real.** Under the lock it does a `ConcurrentHashMap.containsKey` and one `File.exists()`, and calls nothing that can take another lock. `createDatabase` already holds the same monitor across a whole database creation. |

No follow-up issue was filed: every row of the coverage table is fixed here or argued, and the one
uncovered row (9, a second process) is the limit `BackupCoordinator` already documents for backups.

## Review cycles

### Cycle 1 - 8c1203a

`claude` reviewed and found no bugs: it independently re-traced the reservation's atomicity, the
release-in-finally on both restore entry points, the HA leader-only argument for row 4, the
409/`ABORTED` mapping and the `replaceExisting` gating, and confirmed each.

One non-blocking observation, and it was a real hole in the *sweep* even though it is not a hole in
the fix: `ArcadeDbGrpcService.getDatabase(String, DatabaseCredentials)` creates through a
`DatabaseFactory` of its own, which the "every creator" grep did not reach because that grep is
scoped to `ArcadeDBServer`. Verified rather than taken on trust - `GrpcServerPlugin` is the only
production construction site and always passes a non-null server, and `ArcadeDBServer.getDatabase`
throws rather than returning null for a missing database - and recorded as row 10 with the
evidence, plus a comment at the call site itself.

Nothing else in the review asked for a change; the style and test notes were confirmations.

### Cycle 2 - fb5b3f1

`claude` reviewed again, re-derived the core mechanism, the HA row-4 argument, the
`restoringDatabaseNames` synchronisation and the `replaceExisting` drop gate, and found no bugs.
Two optional items, both answered by writing the decision down where the code is rather than by
changing behaviour:

1. *The exception type reaches up into `ServerControlPlane` from `ArcadeDBServer`.* Fair reading,
   and the tidier arrangement would be to hoist `OperationInProgressException` somewhere neutral.
   Not done here: it is public API that `BackupInProgressException` extends and that handlers and
   tests in `server`, `grpcw` and `ha-raft` already catch under that name, so moving it is its own
   change rather than something to ride along with a bug fix. The reason is now in the javadoc of
   `checkDatabaseNameIsNotBeingRestored` instead of only in this file.
2. *`databaseNameIsTaken`'s `File.exists()` now runs under `databasesLock`.* True, and deliberate -
   sampling the name outside the lock is the bug. It is one stat, once per restore, on a monitor
   `createDatabase` already holds across the creation of a whole database. Recorded in the javadoc
   of `reserveDatabaseNameForRestore` so the next reader does not have to relitigate it.

The review's remaining sections (correctness, tests, security) were confirmations with nothing to act on.
