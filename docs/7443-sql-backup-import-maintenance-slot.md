# #7443 - SQL BACKUP DATABASE and IMPORT DATABASE bypass the per-database maintenance slot

## The defect

`BACKUP DATABASE` and `IMPORT DATABASE` are SQL statements executed by `arcadedb-engine`. The admission
policy that serialises whole-database maintenance - `BackupCoordinator`, one instance per `ArcadeDBServer`,
extended by #6753 and #7384 - lives in `arcadedb-server`, and the engine does not depend on the server. So
the two statements reached no policy at all.

A client sending `BACKUP DATABASE` as SQL over HTTP, Postgres, gRPC, Bolt or the console therefore ran a
full backup of a live database that

* a concurrent `restore database` / `restore backup` of the same database did not see, and could drop the
  directory out from under, and
* did not refuse a second SQL backup of the same database - two archives resolving their default name from
  one timestamp, writing into one file, which is #6753 still open on this path.

`IMPORT DATABASE` has the same shape: nothing stopped a restore from dropping and replacing the very
directory an import was loading into.

## The invariant

> A `BACKUP DATABASE` or `IMPORT DATABASE` statement executed against a database hosted by an ArcadeDB
> server holds that server's per-database maintenance slot for the whole of its execution, and is refused -
> with the same message and the same retryable status every other entry point uses - when a conflicting
> operation already holds it.

Its two halves are "the server binds a coordinator to every database it opens" and "the statements take and
release the slot on every path out". Both are tested separately, because either alone is a no-op.

## The design

The issue named two candidate shapes. This takes the first: an interface for the policy moves down into the
engine, and the server registers its instance.

1. **`com.arcadedb.engine.MaintenanceCoordinator`** (new, engine). Carries the `Operation` enum -
   `BACKUP` / `RESTORE` / `IMPORT`, with `verb()`, `phrase()` and `conflictsWith()` - the `begin`/`end`
   contract, the shared `refusal(...)` wording, a `boundTo(Database)` lookup and a `reserve(...)` helper
   returning a `Reservation` built for try-with-resources.

   The enum **moved** here from `BackupCoordinator`; it was not copied. `BackupCoordinator` now implements
   the interface, so `BackupCoordinator.Operation` still resolves for every existing usage - Java inherits
   member types into an implementing class's scope. A *single-type import* of it must name the canonical
   type, though, so three files that imported `BackupCoordinator.Operation` now import
   `MaintenanceCoordinator.Operation`. That is the only change in those files, and no assertion moved.

2. **`com.arcadedb.exception.DatabaseOperationInProgressException`** (new, engine). The refusal type both
   sides raise. `ServerControlPlane.OperationInProgressException` now extends it, so the existing HTTP 409
   arm and the existing gRPC `ABORTED` arm cover the SQL path by matching the base type - one arm each,
   not two.

3. **`ServerDatabase`'s constructor** binds `server.getBackupCoordinator()` to the wrapped database under
   `MaintenanceCoordinator.WRAPPER_NAME`. That constructor is the single funnel for all four of
   `ArcadeDBServer`'s open paths, and `setWrapper` delegates down to the embedded instance, which is the
   same map the statement reads through whichever wrapper layer it holds.

   Binding per database instance - rather than a JVM-wide singleton like `OperationProgressRegistry` - is
   deliberate: the admission is per server, and an HA test (or a co-located pair of nodes) runs several
   servers with the same database names in one process.

4. **The statements** wrap their body in
   `try (Reservation slot = MaintenanceCoordinator.reserve(context.getDatabase(), Operation.X))`, after
   their existing permission check, so an unauthorized caller can neither take nor hold a slot.

A database with no coordinator bound - an embedded process with no server in it - gets `Reservation.NONE`
and behaves exactly as it did before.

## Completeness

### Every way to violate the invariant

Reflective use of the integration module from the engine - i.e. every SQL statement that runs a
whole-database maintenance operation:

```
$ grep -rn "com.arcadedb.integration" engine/src/main/java
engine/.../BackupDatabaseStatement.java:97:  Class.forName("com.arcadedb.integration.backup.Backup");
engine/.../ImportDatabaseStatement.java:70:  Class.forName("com.arcadedb.integration.importer.Importer");
engine/.../ExportDatabaseStatement.java:80:  Class.forName("com.arcadedb.integration.exporter.Exporter");
```

Every construction site of `Backup` / `Importer` outside the integration module itself and outside tests:

```
$ grep -rn "integration.backup.Backup\|integration.importer.Importer" --include='*.java' . \
    | grep -v /test/ | grep -v /integration/src/
server/.../ServerControlPlane.java:1015:  Class.forName("com.arcadedb.integration.backup.Backup");
server/.../ServerControlPlane.java:1432:  Class.forName("com.arcadedb.integration.importer.Importer");
server/.../backup/BackupTask.java:289:    Class.forName("com.arcadedb.integration.backup.Backup");
engine/.../BackupDatabaseStatement.java:97
engine/.../ImportDatabaseStatement.java:70
```

Every place a `ServerDatabase` is built, i.e. every path on which the binding must happen:

```
$ grep -rn "new ServerDatabase(" --include='*.java' . | grep -v /test/
server/.../ArcadeDBServer.java:1026   (createDatabase)
server/.../ArcadeDBServer.java:1078   (registerDatabase)
server/.../ArcadeDBServer.java:1187   (rewrapDatabases, HA)
server/.../ArcadeDBServer.java:1312   (getDatabase, open-from-disk)
```

All four go through the one constructor, which is where the binding is.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| SQL `BACKUP DATABASE` -> `BackupDatabaseStatement` (any transport, any client) | yes | yes - `Issue7443SqlMaintenanceSlotTest`, `Issue7443SqlMaintenanceSlotIT` |
| SQL `IMPORT DATABASE` -> `ImportDatabaseStatement` | yes | yes - same two classes |
| Server binds its coordinator on `createDatabase` / `registerDatabase` / `rewrapDatabases` / open-from-disk | yes (one constructor) | yes - `Issue7443SqlMaintenanceSlotIT#theServerBindsItsCoordinatorToTheDatabasesItOpens` |
| A SQL statement's slot seen by the server's own entry points (the converse direction) | yes | yes - `Issue7443SqlMaintenanceSlotIT#aSlotHeldTheWayTheSqlStatementHoldsItRefusesTheServersOwnBackup` |
| Refusal reaches an HTTP client as 409 | yes (base-type arm) | yes - two IT tests assert the status and the wording |
| Refusal reaches a gRPC `ExecuteCommand` client as `ABORTED` | yes (`GrpcErrorMapper` arm) | yes - `Issue7443GrpcMaintenanceSlotStatusTest` |
| `ServerControlPlane.triggerBackup` / `restoreDatabase` / `restoreBackup` / `importDatabase` | already, #6753 + #7384 | existing `Issue7384ConcurrentRestoreIT`, `Issue6753ConcurrentBackupIT` - re-run green |
| `BackupTask` (auto-backup schedule) | already, #6753 | existing `Issue7384ConcurrentRestoreIT#theScheduledBackupTaskSkips...` - re-run green |
| Embedded process, no server: both statements | argued - no coordinator bound, `Reservation.NONE`, behaviour unchanged by design | yes - `Issue7443SqlMaintenanceSlotTest#withNoCoordinatorBoundBothStatementsRunExactlyAsBefore` |
| SQL `EXPORT DATABASE` | **no - filed as #7450** | no |
| CLI, and a second process writing into a shared backup directory | argued - outside any in-JVM admission policy by construction; `FullBackupFormat` creating its target atomically is what keeps those from corrupting an archive, and it is unchanged (the issue says so too) | n/a |
| Postgres / Bolt / MongoDB / Redis wire protocols | argued - the invariant HOLDS there (the slot is taken by the statement, not by the transport). What those transports do not have is a dedicated status for the refusal: `ErrorCategory` has no conflict category, so they report it as a server error carrying the message verbatim, exactly as they report every other `CommandExecutionException` today. Adding a category is a change to every protocol's mapping and is not attempted here | n/a |

### Reachability

The binding is in `ServerDatabase`'s constructor, which every server-hosted database passes through
(four call sites, listed above); `Issue7443SqlMaintenanceSlotIT#theServerBindsItsCoordinatorToTheDatabasesItOpens`
asserts it on a live server, through both the wrapper the request path holds and the embedded instance the
SQL engine executes against. No flag gates it. `Issue7443SqlMaintenanceSlotIT` drives the refusal through a
real HTTP request to `/api/v1/command/<db>`, so the changed code runs on the live path and not only under a
unit test.

Both new test classes were run against a deliberately neutered fix to prove they can fail:
disabling `MaintenanceCoordinator.reserve` turns 5 of the engine module's 7 red; disabling the
`ServerDatabase` binding turns 5 of the IT's 7 red. The two that stay green in each case are the ones that
assert unchanged behaviour, which is what they are for.

### Residual risk

* `EXPORT DATABASE` still takes no slot. Filed as **#7450**, with the reason it could not be folded in here:
  admitting `EXPORT` breaks the "every kind conflicts with itself" property that lets `BackupCoordinator`
  hold an `EnumSet`, since two exports of one database to two files are legitimate.
* The refusal is a first-class status only on HTTP (409) and gRPC (`ABORTED`). On the other wire protocols
  it is a server error carrying the message - see the table.
* Admission remains per server instance. Two servers sharing a filesystem, and the CLI, are out of scope
  here exactly as they were in #7384.

## Test results

| Suite | Result |
|---|---|
| `engine` `Issue7443SqlMaintenanceSlotTest` | 7/7 green (5 red with the fix neutered) |
| `server` `Issue7443SqlMaintenanceSlotIT` | 7/7 green (5 red with the binding neutered) |
| `grpcw` `Issue7443GrpcMaintenanceSlotStatusTest` | 2/2 green |
| `server` module unit suite | 1054/1054 green |
| `server` ITs `Issue7384ConcurrentRestoreIT`, `Issue6753ConcurrentBackupIT`, `ServerBackupDatabaseIT`, `ServerImportDatabaseIT`, `ServerRestoreDatabaseIT` | 14/14 green |
| `engine` module unit suite (`-DexcludedGroups=benchmark,vector,slow`) | 14815 run, 1 failure: `MultiColumnAggregationResultTest.emptySumAndCountStayZeroNotNaN`. **Pre-existing** - reproduced identically on the base commit `6a9ced547e` in a clean worktree, and no timeseries file is in this diff |
| `grpcw` `GrpcErrorMapperTest`, `Issue6183LeaderRedirectMapperTest` | 18/18 green |
| `integration` importer/restore regression classes | 61/61 green |
| Full reactor `install -DskipTests` and `test-compile` | green |

## Adversarial pass

The orchestrator's Phase 1.5 spawns a subagent that has not read any of the above and asks it to write the
follow-up issue it would file against this patch. **No `Task` tool was available in this session**, so the
pass was run by hand instead, against the diff, hunting specifically for the shapes that pass a green suite.
Recording that, because a pass run by the author is worth less than one run by someone unpersuaded.

| Finding | Verified how | Disposition |
|---|---|---|
| `EXPORT DATABASE` is the third statement of this shape and still takes nothing | `grep -rn "com.arcadedb.integration" engine/src/main/java` returns three hits, not two | **Filed as #7450.** Out of scope here: admitting `EXPORT` breaks the "every kind conflicts with itself" property that lets `BackupCoordinator` hold an `EnumSet` |
| A server path that HOLDS the slot and then runs the SQL statement would refuse itself - an outage dressed as a fix | `grep -rniE '"(sql)?[^"]*\b(backup\|import) database' server/src/main grpcw/src/main ha-raft/src/main` - the only hit that executes SQL is `ArcadeDBServer:1431`, the startup `SERVER_DEFAULT_DATABASES` `{import:...}` command, which holds nothing. `ServerControlPlane.importDatabase` and `triggerBackup` drive the `Importer`/`Backup` classes directly, under their own slot | **Not real**, and the startup path is covered by the existing `ServerImportDatabaseIT`, which drives a real `{import:classpath://...}` at boot and stays green |
| `PostCommandHandler` documents `BACKUP DATABASE` as "mutates no record and takes no lock" - which this patch makes false | Read at `PostCommandHandler:497`, next to the streaming refusal that reads it | **Fixed here.** A comment asserting an invariant the code no longer holds is the next bug report |
| The map the lookup reads, `LocalDatabase.wrappers`, is a plain `HashMap` - and this patch adds a writer that runs while the database is live (HA `rewrapDatabases` wraps one already serving requests) plus a reader on every maintenance statement | `grep -n "wrappers" engine/.../LocalDatabase.java` -> `new HashMap<>()`. The exposure pre-dates the patch (the query-engine factories write here from request threads on first use of a language) but the patch widens it | **Fixed here**: `ConcurrentHashMap`. A put concurrent with a get on a `HashMap` is not merely a lost write, it can corrupt the table; the read is no slower and takes no lock |
| The gRPC claim was wrong in the first draft: `ExecuteCommand` - where a SQL statement surfaces - is on `ArcadeDbGrpcService` and maps errors through `GrpcErrorMapper`, **not** through the admin service's `toStatusException` that #7384 widened | Read `ArcadeDbGrpcService:682` -> `GrpcErrorMapper.toStatusRuntimeException`, whose ladder had no arm for this type and ended at `INTERNAL` | **Fixed here**: an `ABORTED` arm in `GrpcErrorMapper`, the overclaiming comment corrected, and `Issue7443GrpcMaintenanceSlotStatusTest` pinning both surfaces |
| The reservation could be taken before the permission check, letting an unauthorized caller hold a slot | Read both statements: `checkPermissionsOnDatabase(UPDATE_SECURITY)` is the first thing each does, and `reserve` follows it | **Not real** |
| A refused statement could leave a progress entry published for an operation that never ran | `Issue7443SqlMaintenanceSlotTest` asserts `OperationProgressRegistry.instance().getOperations(...)` is empty after every refusal | **Not real** - the reservation is taken before the progress entry is registered |

## Final re-run after the adversarial fixes

| Suite | Result |
|---|---|
| `engine` unit suite | 14815 run, same single pre-existing `MultiColumnAggregationResultTest` failure, nothing else |
| `server` unit suite | 1054/1054 green |
| `server` ITs (`Issue7443SqlMaintenanceSlotIT`, `Issue7384ConcurrentRestoreIT`, `Issue6753ConcurrentBackupIT`, `ServerBackupDatabaseIT`, `ServerImportDatabaseIT`, `ServerRestoreDatabaseIT`, `ServerDefaultDatabasesIT`, `Issue7385RestoreProgressIT`) | 26/26 green |
| `grpcw` unit suite | 240/240 green |
| `gremlin` `ArcadeGraph*Test` (the other writers of the wrappers map) | green |

## Review cycles

### Cycle 1 - `8ad93e5858`

The `claude` review found no correctness problem and confirmed the wiring end to end, including two
things worth recording because they were assertions about this tree rather than opinions:

* `ServerDatabase`'s `if (server != null)` guard is exercised, not defensive decoration -
  `ha-raft`'s `ArcadeStateMachineBootstrapMismatchTest` constructs `new ServerDatabase(null, localDb)`
  at two call sites. Verified by grep.
* Nothing is left resolving the removed nested `BackupCoordinator.Operation`. Verified by the full
  reactor `test-compile`.

Applied:

* **Import order in `ServerControlPlane`.** The new imports had landed between `JSONObject` and the
  `server.backup` block. Sorted back into place. The other five touched files were checked too and were
  already in order.

Filed rather than fixed here:

* **#7461** - `ImportDatabaseStatement` sets `result = FAIL` for an importer `IllegalArgumentException`
  and then unconditionally overwrites it with `OK` two lines later, so the one failure the statement
  means to report in-band is invisible to every client. Pre-existing and untouched by this PR; the fix
  is a contract decision (in-band `FAIL` with a reason, or throw like every other failure on that path),
  not a moved assignment.

Not acted on, with the reason:

* **The two gRPC `ABORTED` arms.** The review names the duplication and agrees it is inherent: the admin
  RPCs and `ExecuteCommand` are different services with different mappers. Collapsing them means giving
  `ArcadeDbGrpcAdminService` a dependency on `GrpcErrorMapper`'s ladder, which would change the status of
  every other exception it maps. Both arms are pinned by `Issue7443GrpcMaintenanceSlotStatusTest`.
