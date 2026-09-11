# #7395 - `restore database` and `restore backup` disagree on what "the target already exists" means

Issue: https://github.com/ArcadeData/arcadedb/issues/7395
Type: bug (labels `bug`, `server`, `backup-restore`)
Branch: `fix/7395-restore-existence-check-parity`

## Finding ledger

- [x] 1. `ServerControlPlane.restoreDatabase` asks only the filesystem; `ServerControlPlane.restoreBackup` asks the
  registry *and* the filesystem. Unify on the stricter predicate, keeping `restore database`'s unconditional refusal
  and `restore backup`'s `overwrite` gate.

## Analysis

`server/src/main/java/com/arcadedb/server/ServerControlPlane.java`, as of `main`:

```java
// restoreDatabase (line 1227)
final String dbPath = databaseDirectory(databaseName);
if (new File(dbPath).exists())
  throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

// restoreBackup (line 1259)
final String dbPath = databaseDirectory(targetDatabase);
if ((server.existsDatabase(targetDatabase) || new File(dbPath).exists()) && !overwrite)
  throw new IllegalArgumentException("Database '" + targetDatabase + "' already exists. ...");
```

The divergence is only observable in one state: **registered in `ArcadeDBServer.databases` but no directory on
disk**. That state is reachable out of band (the directory removed under a running server) and through the server's
own API - `LocalDatabase.drop()` closes the database and deletes the directory but does **not** unregister it;
`ServerControlPlane.dropQuietly` has to call `server.removeDatabase(name)` as a separate second step, which is the
proof that the two halves are independent:

```java
private void dropQuietly(final ServerDatabase database, final String databaseName) {
  try {
    database.getEmbedded().drop();
    server.removeDatabase(databaseName);   // <- separate step
```

In that state `restore database` proceeds, restores into a temp directory and reaches `swapRestoredDatabase`, which
drops the registered database and moves the new directory into place. `restore backup` refuses unless `overwrite`.

### Which predicate is "intended"

`ArcadeDBServer.createDatabase` - the third place on the server that asks the same question - already asks the
stricter one, registry first and then the files:

```java
synchronized (databasesLock) {
  serverDatabase = databases.get(databaseName);
  if (serverDatabase != null)
    throw new IllegalArgumentException("Database '" + databaseName + "' already exists");
  ...
  if (factory.exists())
    throw new IllegalArgumentException("Database '" + databaseName + "' already exists");
```

So the stricter predicate is the server-wide norm, and `restoreDatabase` was the outlier. That settles the question
the issue leaves open: `restore database` refuses a registered-but-absent database rather than silently replacing it.

## Completeness

### 1. Invariant

> `restore database` and `restore backup` answer "does the target already exist?" with one and the same predicate -
> the name is registered in the server's database registry **or** a directory of that name is present under
> `SERVER_DATABASE_DIRECTORY` - with `restore database` refusing unconditionally and `restore backup` refusing
> unless `overwrite` is set.

### 2. Ways to violate it

```
$ grep -rn "existsDatabase(" --include="*.java" server/src/main/java grpcw/src/main/java
server/.../ServerControlPlane.java:318:    if (!server.existsDatabase(databaseName))          # dropDatabase guard, not a restore target check
server/.../ServerControlPlane.java:1260:   if ((server.existsDatabase(targetDatabase) || new File(dbPath).exists()) && !overwrite)
server/.../ServerControlPlane.java:1450:   if (server.existsDatabase(databaseName))           # swapRestoredDatabase, post-restore
server/.../ArcadeDBServer.java:983:       public boolean existsDatabase(...)                 # the accessor itself
server/.../ArcadeDBServer.java:1369, SchemaInfo:130, AbstractServerHttpHandler:1147/1235,
  BackupTask:84, AutoBackupSchedulerPlugin:244, grpcw/ArcadeDbGrpcAdminService:140/147/166/199/225
                                                                                     # unrelated existence lookups
```

```
$ grep -rn "databaseDirectory(\|new File(dbPath)" --include="*.java" server/src/main/java
server/.../ServerControlPlane.java:1227:  final String dbPath = databaseDirectory(databaseName);
server/.../ServerControlPlane.java:1228:  if (new File(dbPath).exists())                       # <- the outlier
server/.../ServerControlPlane.java:1259:  final String dbPath = databaseDirectory(targetDatabase);
server/.../ServerControlPlane.java:1260:  if ((server.existsDatabase(targetDatabase) || ...
server/.../ServerControlPlane.java:1407:  final File finalDir = new File(dbPath);              # performRestore, post-check
server/.../ServerControlPlane.java:1510:  private String databaseDirectory(...)                # the accessor itself
```

```
$ grep -rn "controlPlane.restoreDatabase(\|controlPlane.restoreBackup(" --include="*.java" .
server/.../PostServerCommandHandler.java:423:  controlPlane.restoreDatabase(databaseName, url, listener);
server/.../PostServerCommandHandler.java:457:  controlPlane.restoreBackup(databaseName, fileName, targetDatabase, overwrite, listener);
grpcw/.../ArcadeDbGrpcAdminService.java:803:  controlPlane.restoreBackup(req.getDatabase(), req.getFileName(), req.getTargetDatabase(), req.getOverwrite(), listener);
grpcw/.../ArcadeDbGrpcAdminService.java:827:  controlPlane.restoreDatabase(req.getDatabase(), req.getUrl(), listener);
```

```
$ grep -rni "RESTORE_DATABASE\|RESTORE_BACKUP\|\"restore database" --include="*.java" */src/main/java
# the only command dispatchers are PostServerCommandHandler:77/79/122/163-168 and the two gRPC RPCs above.
# console/src/main/java has no restore command; there is no RESTORE SQL statement.
# ha-raft SnapshotInstaller.restoreBackup is a private static Path->Path file copy, unrelated to this predicate.
```

Four transports, two shared implementations. No transport repeats the check for itself.

### 3. Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| HTTP `POST /api/v1/server` `restore database <n> <url>` -> `ServerControlPlane.restoreDatabase` | yes | yes - `Issue7395RestoreTargetExistsIT` |
| HTTP `POST /api/v1/server` `restore backup <db> <f> as <t>` -> `ServerControlPlane.restoreBackup` | yes (predicate now shared, behaviour unchanged) | yes - `Issue7395RestoreTargetExistsIT` |
| gRPC `RestoreDatabase` -> `ServerControlPlane.restoreDatabase` | yes | yes - `Issue7395GrpcRestoreTargetExistsIT` |
| gRPC `RestoreBackup` -> `ServerControlPlane.restoreBackup` | yes (unchanged) | yes - `Issue7395GrpcRestoreTargetExistsIT` |
| `ServerControlPlane.importDatabase` -> `ArcadeDBServer.createDatabase` | n/a - argued | argued: `createDatabase` already applies registry-then-`factory.exists()`, the stricter predicate (`ArcadeDBServer.java:1002-1014`). It is the norm this fix aligns restore with, not a violator of it. |
| `ha-raft` `SnapshotInstaller.restoreBackup(Path, Path)` | n/a - argued | argued: `private static void restoreBackup(final Path dbDir, final Path backupDir)` - a file-copy helper taking two paths, no database name, no registry. Same method name, different question. |

### 5. Reachability

`ServerControlPlane` is constructed by `ArcadeDBServer` and reached on every restore command; the two methods are
the only implementation behind all four transports (grep C above). Nothing gates the changed lines behind a flag.
The new tests drive the changed lines over real HTTP and real gRPC against a live server.

## Residual risk

`restore database` now refuses, rather than silently replacing, a database that is registered but whose directory is
gone. An operator who was relying on `restore database` to repair that state has to `drop database <name>` first, or
use `restore backup ... as <name>` with `overwrite`. That is the behaviour change the issue asks for, and the error
message names the database so the required step is obvious.

Nothing else is left uncovered: the four transports in the table are the complete set of restore entry points per
grep C, and both remaining rows are argued with evidence rather than left blank.


## Changes

`server/src/main/java/com/arcadedb/server/ServerControlPlane.java`

- New private `databaseNameIsTaken(databaseName, dbPath)` - `server.existsDatabase(name) || new File(dbPath).exists()` -
  with the javadoc that says why the two halves are independent and which command used to ask only one of them.
- `restoreDatabase` now calls it in place of its filesystem-only check. Its refusal stays unconditional, and its
  javadoc now says so and names the two ways out (`restore backup ... as <name>` with `overwrite`, or `drop database`).
- `restoreBackup` now calls it in place of its inline copy of the same expression. Behaviour unchanged: the
  `overwrite` gate still sits over the predicate.

Tests:

- `server/src/test/java/com/arcadedb/server/backup/Issue7395RestoreTargetExistsIT` (HTTP, 7 tests)
- `grpcw/src/test/java/com/arcadedb/server/grpc/Issue7395GrpcRestoreTargetExistsIT` (gRPC, 6 tests)

Both build the divergent state through the server's own API - `createDatabase`, then `getEmbedded().drop()`, which
deletes the directory and leaves the registry entry - rather than by deleting files under an open database.

## Test results

Both new classes were run against the unfixed tree first. In each, the one test that asserts the new behaviour failed
and the other five passed, which is what pins the existing behaviour rather than re-asserting it:

```
# before the fix, server module
[ERROR] Tests run: 6, Failures: 1 -- Issue7395RestoreTargetExistsIT
[ERROR]   restoreDatabaseRefusesATargetRegisteredOnTheServerWhoseDirectoryIsGone:165
          [{"error":"Cannot execute command", ... "detail":"Error restoring database -> ...
            The backup file '...nonexistent-7395.zip' does not exist ..."}]

# before the fix, grpcw module (predicate temporarily reverted to `new File(dbPath).exists()`)
[ERROR] Tests run: 6, Failures: 1 -- Issue7395GrpcRestoreTargetExistsIT
[ERROR]   restoreDatabaseRefusesATargetRegisteredOnTheServerWhoseDirectoryIsGone:172
```

After the fix, the new classes and every connected suite:

```
$ mvn -o verify -DskipITs=false -pl server \
    -Dit.test='BackupRestoreDeleteApiIT,BackupApiCommandsIT,Issue7308RestoreTargetNameIT,\
               RestoreImportSecurityDurabilityIT,Issue7392OnDemandBackupWithoutAutoBackupIT,\
               Issue7395RestoreTargetExistsIT,PostServerCommandHandlerIT' \
    -Dtest='ServerControlPlaneProgressAndSessionsTest'
Tests run: 4,  Failures: 0  (ServerControlPlaneProgressAndSessionsTest)
Tests run: 50, Failures: 0  (the seven ITs above)
BUILD SUCCESS

$ mvn -o verify -DskipTests -DskipITs=false -pl grpcw \
    -Dit.test='Issue7308GrpcRestoreImportIT,Issue7308GrpcRestoreImportUrlGuardIT,\
               Issue7308GrpcRestoreImportAuthorizationIT,Issue7395GrpcRestoreTargetExistsIT,\
               Issue7304GrpcControlPlaneIT'
Tests run: 53, Failures: 0
BUILD SUCCESS
```

## Impact

Four transports, one behaviour change, in one state only: `restore database` against a name the server still has
registered but whose directory is gone now answers `400 Database '<name>' already exists` (gRPC `INVALID_ARGUMENT`)
instead of restoring over it. Every other input to either command behaves exactly as before - the five pre-existing
assertions in each new class, and the 103 tests of the connected suites, are the evidence.

## Adversarial pass

The orchestrator's Phase 1.5 asks for a `general-purpose` subagent that has not seen the author's
reasoning. **No subagent-spawning tool is available in this session** (`ListAgents`/`SendMessage` exist,
`Task` does not), so the pass was run against the diff directly instead of by a fresh reader. That is a
weaker version of the same check and is recorded as such rather than claimed as the real thing.

| Finding | Disposition |
|---|---|
| The new `restoreDatabase` javadoc tells the operator to `drop database` or `restore backup ... overwrite`. Does that escape hatch hold? `restore database` now refuses a state it used to repair, so a wrong hatch would be a comment the code does not honour. | **Real, fixed here.** Verified rather than assumed: `aTargetRefusedBecauseItIsOnlyRegisteredCanStillBeDroppedSoTheOperatorCanRetry` builds the state the way it is actually reached - the directory removed from under a database that is still open - then asserts the refusal *and* that `drop database` clears it (200, no longer registered). Green. |
| `drop database` throws `DatabaseIsClosedException` in the other variant of the state, where the database was dropped through `getEmbedded().drop()` without `removeDatabase` (what this fixture's helper does, and what `Issue7308GrpcRestoreImportIT`'s teardown does). | **Not real** as a product defect. `LocalDatabase.drop()` -> `closeForDrop()` -> `checkDatabaseIsOpen` does throw on an already-closed database, but no server code reaches that state: `dropQuietly` and `dropDatabaseClusterWide` are the only two callers of `getEmbedded().drop()` in `server/src/main/java`, and both pair it with `server.removeDatabase`. Only test teardown leaves the pair half-done. The realistic route into the state is the out-of-band directory removal, and the test above proves that one recovers. |
| The existence check is not atomic with the swap that acts on it: nothing holds `databasesLock` between them, and the window is the whole duration of the restore, so a `create database` that lands inside it is silently dropped by `swapRestoredDatabase`. | **Real, out of scope - filed as [#7441](https://github.com/ArcadeData/arcadedb/issues/7441).** Predates this fix and is untouched by it (both commands had the window before and after); closing it means reserving the name for the restore's duration, which is a different change from agreeing on the predicate. |
| The registry is a case-sensitive `Map`, `new File(dbPath).exists()` is case-insensitive on macOS and Windows, so `restore database GRAPH` and `restore database graph` can disagree about which half answers. | **Not real** as a new gap: both halves were already in `restoreBackup`'s predicate before this fix, so the behaviour is unchanged on every input. Recorded under residual risk rather than filed - it is a property of `ArcadeDBServer`'s registry, not of restore. |

## Residual risk (updated)

- `restore database` now refuses, rather than silently replacing, a database that is registered but whose
  directory is gone. The way out is `drop database <name>` - asserted, not assumed - or
  `restore backup ... as <name>` with `overwrite`.
- The check is not atomic with the swap: [#7441](https://github.com/ArcadeData/arcadedb/issues/7441).
- Case-sensitivity between the two halves of the predicate is whatever `ArcadeDBServer`'s registry and the
  host filesystem already made it; this fix neither widens nor narrows it.
