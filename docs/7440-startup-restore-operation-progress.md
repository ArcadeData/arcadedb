# #7440 - the startup `restore:` command of `defaultDatabases` publishes no OperationProgress

Follow-up to #7385. Type: enhancement (`enhancement`, `server`, `observability`, `backup-restore`,
milestone 26.10.1).

## Goal

#7385 made every restore that reaches `ServerControlPlane.performRestore` publish an
`OperationProgress`, so `GET /api/v1/progress/{database}` reports it while it runs: HTTP
`restore database`, HTTP `restore backup`, gRPC `RestoreDatabase` and gRPC `RestoreBackup`.

The `restore:` startup command of `arcadedb.server.defaultDatabases` does not go through the
control plane and stayed silent. `ArcadeDBServer.start()` calls `httpServer.startService()`
(line 385) before `loadDefaultDatabases()` (line 408), so the HTTP listener is already accepting
requests while a startup restore is extracting, and `GetProgressHandler` reads only the lock-free
registry snapshot - no database access. A container booting with
`arcadedb.server.defaultDatabases=mydb[root]{restore:https://.../backup.zip}` against a large
archive therefore answered "nothing running" on the very database it was building.

## Analysis

`server/src/main/java/com/arcadedb/server/ArcadeDBServer.java`, `loadDefaultDatabases()`,
`case "restore":` constructs `com.arcadedb.integration.restore.Restore` reflectively (the
`arcadedb-integration` module is optional on the server classpath), calls `restoreDatabase()`,
then opens the restored database with `getDatabase(dbName)`. Nothing registers with
`OperationProgressRegistry`, and nothing installs the `ProgressCallback` #7385 added to
`Restore.setProgressCallback`.

The path also had no seam a test could drive: it is a `switch` arm inside a private method of the
server's startup sequence.

## Completeness

### Invariant

Every restore the **server** performs publishes an `OperationProgress` under its target database
name for as long as it runs, and retires it in a `finally` on success and on failure alike -
including the `restore:` startup command of `arcadedb.server.defaultDatabases`.

### Enumeration

```
$ grep -rn 'com\.arcadedb\.integration\.restore\.Restore' --include='*.java' . | grep -v '/target/' | grep src/main
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:1542:  Class.forName("com.arcadedb.integration.restore.Restore");
server/src/main/java/com/arcadedb/server/ArcadeDBServer.java:1407:      Class.forName("com.arcadedb.integration.restore.Restore");
```

Two reflective construction sites in production code. Everything else is `src/test`.

```
$ grep -rn 'new Restore(' --include='*.java' . | grep -v '/target/' | grep src/main
integration/src/main/java/com/arcadedb/integration/restore/Restore.java:53:    new Restore(args).restoreDatabase();
```

`Restore.main` - the standalone CLI, which is not a server.

```
$ grep -rn 'restoreDatabase\b' --include='*.java' server/src/main grpcw/src/main | grep -v '/target/'
ServerControlPlane.java:1289  public void restoreDatabase(...)            <- HTTP restore database + gRPC RestoreDatabase
PostServerCommandHandler.java:424  controlPlane.restoreDatabase(...)
ArcadeDbGrpcAdminService.java:827  controlPlane.restoreDatabase(...)
ArcadeDBServer.java:1411      clazz.getMethod("restoreDatabase").invoke  <- THIS ISSUE
```

Sibling of the same shape - the other long-running startup command, `import:`:

```
$ sed -n '60,72p' engine/src/main/java/com/arcadedb/query/sql/parser/ImportDatabaseStatement.java
    final OperationProgress progress = OperationProgressRegistry.instance()
        .register(context.getDatabase().getName(), "import database");
```

`loadDefaultDatabases()`'s `import:` arm runs `database.command("sql", "import database ...")`,
which lands in `ImportDatabaseStatement` - already publishing since #5376. No gap there.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| HTTP `restore database` -> `ServerControlPlane.performRestore` | already (#7385) | `Issue7385RestoreProgressIT` |
| HTTP `restore backup` -> `performRestore` | already (#7385) | `Issue7385RestoreProgressIT` |
| gRPC `RestoreDatabase` / `RestoreBackup` -> `performRestore` | already (#7385) | `Issue7385RestoreProgressIT` |
| startup `restore:` of `defaultDatabases` -> `ArcadeDBServer.loadDefaultDatabases` | **yes** | `Issue7440StartupRestoreProgressIT` (3 tests) |
| startup `import:` of `defaultDatabases` -> `ImportDatabaseStatement` | argued - already publishes since #5376, evidence above | `-` |
| `Restore.main` (CLI) | argued - a standalone JVM with no registry reader: no HTTP progress endpoint, no console poller, nothing can observe the registry, and the CLI already prints its own progress to stdout | `-` |

### Reachability

`restoreDatabaseFromStartupCommand` is called from the `case "restore":` arm of
`loadDefaultDatabases()`, which `ArcadeDBServer.start()` calls unconditionally on every boot at
line 408. No feature flag gates it. `Issue7440StartupRestoreProgressIT.theProgressEndpointReportsAStartupRestoreWhileItRuns`
drives a real server boot with `SERVER_DEFAULT_DATABASES=...{restore:...}` and polls the real HTTP
endpoint, so the wiring - not only the extracted method - is what the test exercises.

### Residual risk

- The startup restore still resolves `SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS` from the **static**
  global rather than from the server's own `ContextConfiguration`, unlike
  `ServerControlPlane.performRestore` which calls `setAllowLocalUrls` explicitly. That is a
  pre-existing security-policy-resolution difference, untouched here, and unrelated to progress
  publication. Not a regression introduced by this PR.
- Progress for a startup restore is **process-local and boot-local**: a poll that arrives before
  `httpServer.startService()` (line 385) still sees nothing, because there is no listener yet.
  Unavoidable without moving the listener earlier.

## Change

`server/src/main/java/com/arcadedb/server/ArcadeDBServer.java`

- The `case "restore":` arm of `loadDefaultDatabases()` is now three lines: drop the previous database if
  any, then call the new `restoreDatabaseFromStartupCommand(dbName, url, databasePath)`. The
  commented-out `// new Restore(commandParams, dbPath).restoreDatabase();` line went with it.
- `restoreDatabaseFromStartupCommand` (package-private, so a test can drive it with a real archive)
  registers an `OperationProgress` named `restore database` - the same label the HTTP/gRPC verb uses,
  because it is the same operation over a different transport - seeds it with step 1 of 2
  (`Restoring files`), installs the `ProgressCallback` on the restorer, marks step 2 of 2
  (`Activating database`) around the `getDatabase()` that opens the result, and unregisters in a
  `finally`.
- `installStartupRestoreProgressCallback` renumbers the format's own 1-of-1 step into 1-of-2, and
  swallows a `ReflectiveOperationException` the way `ServerControlPlane` does: an `arcadedb-integration`
  build without the setter reports no counters rather than failing the boot.

Two steps, not `performRestore`'s three: this command restores straight into the final directory (no
temp directory to swap in) and forces no cluster snapshot.

## Tests

`server/src/test/java/com/arcadedb/server/Issue7440StartupRestoreProgressIT.java` - three tests, on
its own free port and bound/dialled on the IPv4 loopback literal at both ends.

| Test | What it pins |
|---|---|
| `theProgressEndpointReportsAStartupRestoreWhileItRuns` | A real server boot with `SERVER_DEFAULT_DATABASES=graph[...]{restore:http://.../backup-7440.zip}`, the archive trickled out in 30 slices 50 ms apart. A sampler started in `onBeforeStarting` polls `GET /api/v1/progress/graph` and the registry while the server boots, and the test asserts both reported the restore, that `done` ADVANCED past 0 (a callback installed but never fed would not show that), and that the operation is retired afterwards |
| `aStartupRestorePublishesTheArchiveCounters` | A local archive takes the parallel extractor, which knows the entry count up front, so a real DENOMINATOR reaches the registry. The only assertion that catches `installStartupRestoreProgressCallback` failing to find the setter: that failure is swallowed by design, so the two coarse step markers would still be published and everything else would still pass |
| `aFailedStartupRestoreRetiresTheOperation` | A restore that throws must not leave a phantom operation for the life of the process |

All three were run RED first: two failed against the same patch with the registration removed
(`nothing was published while the restore ran`, `the startup restore published nothing to the
operation progress registry`), and the third was separately shown to fail when the `finally` is
replaced by a plain trailing `unregister` (`a failed startup restore must retire its operation too`).

### Results

- `Issue7440StartupRestoreProgressIT` - 3/3 green, and green on three consecutive runs of the mixed
  batch below (this test was flaky before it was moved off the shared 2480-2489 range).
- `ServerRestoreDatabaseIT`, `ServerImportDatabaseIT`, `ServerDefaultDatabasesIT`,
  `ServerReadOnlyDatabasesIT`, `Issue7385RestoreProgressIT`, `Issue7440StartupRestoreProgressIT` -
  11/11 green, three times in a row.
- Full `server` module unit suite - `Tests run: 1054, Failures: 0, Errors: 0, Skipped: 0`.

A note for whoever runs this locally: an ArcadeDB server listening on `*:2480` (a Homebrew install,
an IDE, a previous run) does NOT stop a test server from binding `127.0.0.1:2480` as well, but it
DOES answer a client that dials the name `localhost` and resolves `::1` first - with its own HTTP
status, which reads as an authorization failure rather than as a port conflict. That is what the
`LOOPBACK` constant and the dedicated free port in this test are for.

## Adversarial pass

The `Task` tool was not available in this session, so the pass could not be run by a subagent kept
ignorant of the author's reasoning. It was run by the author against the diff instead, which is
weaker, and is recorded as such.

1. **The startup restore takes no per-database operation slot** - real, verified, out of scope, filed
   as **#7454**. #7384 gave `restore database` / `restore backup` / `import database` / `trigger
   backup` an exclusive per-database slot through `ServerControlPlane.beginExclusive`
   (`ServerControlPlane.java:1298`, `:1341`, `:1397`, `:930`). The startup command does not go through
   the control plane and takes none. The HTTP listener is up while the startup restore runs
   (`startService()` at `:386`, `loadDefaultDatabases()` at `:408`) and no handler gates on server
   status - `grep -rn 'STATUS.ONLINE\|getStatus() !=' server/src/main/java/com/arcadedb/server/http/`
   returns nothing - so a client restore, backup or import of the same database is admitted during the
   boot window. #7440 makes that window observable; it deliberately does not change admission, which is
   #7384's invariant rather than this one's.
2. **A failed startup restore destroys the previous database** - real, pre-existing, deliberate, NOT
   filed. The `case "restore":` arm drops the target before restoring
   (`// DROP THE DATABASE BECAUSE THE RESTORE OPERATION WILL TAKE CARE OF CREATING A NEW DATABASE`),
   where `performRestore` restores into a temp directory and swaps on success (#5027). Untouched here
   and unrelated to progress publication; noted so it is not mistaken for something this PR introduced.
3. **Moving `getDatabase()` inside the `try` changes which exceptions are caught** - not real. The
   five clauses catch `ClassNotFoundException`, `NoSuchMethodException`, `IllegalAccessException`,
   `InstantiationException` and `InvocationTargetException`, all checked; `getDatabase` declares no
   checked exception, so nothing it throws can be captured by them. Its behaviour on failure is
   unchanged apart from the operation now being retired first.
4. **The progress could outlive a restore that fails inside the reflective block** - not real, and
   pinned by `aFailedStartupRestoreRetiresTheOperation`, which was shown to go red without the
   `finally`.
5. **The `import:` startup command is silent too** - not real. It runs
   `database.command("sql", "import database ...")`, which lands in `ImportDatabaseStatement`, publishing
   since #5376 (`ImportDatabaseStatement.java:66`).

## Ledger

- [x] Publish an `OperationProgress` for the startup `restore:` command - fixed, retired in a `finally`
- [x] Feed it the `ProgressCallback` #7385 added to `Restore.setProgressCallback` - fixed, and pinned by
      the denominator assertion
- [x] Give the path a seam a test can drive - fixed, `restoreDatabaseFromStartupCommand` is
      package-private and two of the three tests call it directly
- [x] Adversarial follow-up: no operation slot at boot - filed as #7454

## Pull request

https://github.com/ArcadeData/arcadedb/pull/7455

## Review cycles

| Cycle | Head SHA | Changes | Bot outcome |
|---|---|---|---|
| 1 | `6932946` | none - nothing actionable arrived | **timeout**. The gating `claude` bot posted nothing on any of the three surfaces (formal review, inline review comment, PR issue comment) within the 15-minute window. Its workflow run is not stalled: run 34599178805 / job 103262038152 finished `completed`/`success` at 12:31:25Z, two minutes after the push, with `"subtype": "success"`, `"is_error": false`, 29 turns and `permission_denials_count: 0` - it read the diff and then never ran the `gh pr comment` its prompt asks for. The known "review bot can time out before posting" failure mode, an infrastructure problem rather than a verdict on this PR |

Other reviewers on the same SHA did report, and neither asked for a change:

- **CodeRabbit** - "No actionable comments were generated in the recent review." Merge risk: minimal. Its one failed pre-merge check is `Docstring Coverage` at 42.86%, counting the test class's private helpers (`freePort`, `port`, `stop`, `typeName`, `databaseDirectory`) as undocumented functions. Not acted on: these are four-line test helpers whose names say what they do, and the methods that carry real decisions - `restoreDatabaseFromStartupCommand`, `installStartupRestoreProgressCallback`, every test method, and the non-obvious fields - all have Javadoc.
- **Codacy** - pass.

No deferred-items notes file was produced: no review comment arrived to defer.

## CI on the PR head (`6932946`)

`integration-tests` - the lane that actually runs `Issue7440StartupRestoreProgressIT` - **passed**,
as did `build-and-package`, `lint`, `builder-tests`, `slow-unit-tests`, `vector-unit-tests`,
`studio-e2e-tests`, `opencypher-tck-tests`, every language e2e lane, CodeQL, Codacy and Meterian.

Two lanes are red, and both are red on `main` independently of this branch:

- **`unit-tests`** - `Issue7089NaNTransparentSumAvgTest.oneNaNSampleNoLongerPoisonsTheBucketOnAnyPath:298`
  and `MultiColumnAggregationResultTest.emptySumAndCountStayZeroNotNaN:86`, both in
  `com.arcadedb.engine.timeseries`. The `main` run of this PR's own merge base (run 34598973837,
  commit `6533998b`) fails with the same two tests, the same line numbers and the same
  `Tests run: 14806, Failures: 2, Errors: 0, Skipped: 22` total. Nothing in this PR touches `engine`.
- **`ha-integration-tests`** - `GetClusterHandlerIT.everyNodePublishesItsOwnResyncStateAndPosition:166`,
  `Issue5569SlotMergeDeleteRaftIT.mergedDeletesReplicateIntact:121` and
  `RaftPriorityRejoinIT.leaderRestartThenReplicaRestartConverges:143`, the long-known flaky Raft lane
  (#5668). Red on three of the last four `main` runs (34599296509, 34589620366, 34589587148). The
  changed code additionally cannot run there: it is the `case "restore":` arm of
  `loadDefaultDatabases()`, and `grep -rn 'SERVER_DEFAULT_DATABASES\|defaultDatabases' ha-raft/src`
  returns nothing, so no test in that module configures a startup restore at all.

## Final state

`timeout` - one cycle, no changes applied, PR open and awaiting the developer. Merge is the
developer's call; this workflow does not merge.
