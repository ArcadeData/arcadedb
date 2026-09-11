# #7385 - A running restore publishes no OperationProgress

Follow-up to #7308. `GET /api/v1/progress/{database}` reports the long-running operations of a
database; the console and Studio render it. Six operations publish there today. A **restore**
publishes nothing, so for the minutes a `restore backup` or `restore database` runs, a third party
looking at the database sees an idle database that is in fact being replaced.

## Finding ledger

- [x] 1. No restore registers an `OperationProgress`, on any transport - **fixed** on all four control-plane
      entry points, with real entry counters; the startup path is filed as #7440.

## Analysis

### Who publishes today

```
$ grep -rn "OperationProgressRegistry.instance().register" --include="*.java" engine/src/main server/src/main integration/src/main grpcw/src/main
engine/.../RebuildIndexStatement.java:196     "rebuild index"
engine/.../CompactIndexStatement.java:73      "compact index"
server/.../ServerControlPlane.java:1301       "import database"     # added by #7308

$ grep -rn -A2 "OperationProgressRegistry.instance()$" --include="*.java" engine/src/main server/src/main | grep register
engine/.../CheckDatabaseStatement.java-139    "check database" / "check database fix"
engine/.../ImportDatabaseStatement.java-67    "import database"
engine/.../BackupDatabaseStatement.java-94    "backup database"
```

Six producers; no restore among them. The issue body lists three because it was written against an
older tree - the extra three do not change the finding.

### The restore has real progress to report

Unlike the import - whose record total is unknown up front, so `ImportDatabaseStatement` and
`runImport` both publish only a coarse `(1, 1, 0, -1)` marker - the restore's unit of work is an
archive entry, and the parallel extractor (#6086) reads the whole entry list out of the ZIP central
directory **before** any thread starts writing:

```
integration/.../ParallelZipExtractor.java:140   final List<PlannedEntry> plan = new ArrayList<>(entries.size());
integration/.../ParallelZipExtractor.java:178   return new ExtractStats(plan.size(), databaseOrigSize);
```

So the parallel path can report `done/total` with a real denominator, and the sequential walk - the
fallback for an http(s) or an encrypted archive, which only learns an entry when it reaches it - can
report `done` against `total = -1`, which is exactly what `OperationProgress.getPercentage()`
already renders as "unknown".

### The three phases of a server-side restore

`ServerControlPlane.performRestore` is not only the extraction. It is:

1. extract the archive into a temporary sibling directory (`Restore.restoreDatabase()`),
2. `swapRestoredDatabase` - drop the previous target, if any, and move the temp directory into place,
3. `replicateRestoredDatabase` - in HA, submit an install-database Raft entry with
   `forceSnapshot=true` so every replica pulls the restored files.

Step 3 is a no-op outside HA and can take minutes inside it, so the operation is published as three
steps rather than one.

## Completeness

### The invariant

> Every restore this server's control plane runs publishes an `OperationProgress` for its target
> database, from before the first byte is extracted until after the database is activated and
> replicated - and retires it in a `finally`, on success and on failure alike.

### Entry points

```
$ grep -rn 'Class.forName("com.arcadedb.integration.restore.Restore")\|new Restore(' --include="*.java" engine/src/main server/src/main integration/src/main grpcw/src/main console/src/main
server/.../ServerControlPlane.java:1402
server/.../ArcadeDBServer.java:1407          # startup SERVER_DEFAULT_DATABASES 'restore:' command
integration/.../Restore.java:51              # CLI main()

$ grep -rn "controlPlane.restoreDatabase(\|controlPlane.restoreBackup(" --include="*.java" server/src/main grpcw/src/main
server/.../PostServerCommandHandler.java:423   restore database  (HTTP)
server/.../PostServerCommandHandler.java:457   restore backup    (HTTP)
grpcw/.../ArcadeDbGrpcAdminService.java:797    RestoreBackup     (gRPC)
grpcw/.../ArcadeDbGrpcAdminService.java:821    RestoreDatabase   (gRPC)
```

There is no `RESTORE DATABASE` SQL statement - the engine's parser has `BackupDatabaseStatement` but
no restore counterpart (`ls engine/src/main/java/com/arcadedb/query/sql/parser/ | grep -i restore`
returns only the per-record `RestoreDocument/Edge/Vertex` statements), so the SQL transport is not an
entry point at all.

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| HTTP `restore database <name> <url>` -> `ServerControlPlane.restoreDatabase` -> `performRestore` | yes | yes - `Issue7385RestoreProgressIT.httpRestoreDatabasePublishesProgress` |
| HTTP `restore backup <db> <file> as <target>` -> `ServerControlPlane.restoreBackup` -> `performRestore` | yes | yes - `Issue7385RestoreProgressIT.httpRestoreBackupPublishesProgress` |
| gRPC `RestoreDatabase` -> `ServerControlPlane.restoreDatabase` -> `performRestore` | yes | yes - `Issue7385RestoreProgressIT.restoreDatabasePublishesProgressAtTheControlPlane` (the shared layer both transports call) |
| gRPC `RestoreBackup` -> `ServerControlPlane.restoreBackup` -> `performRestore` | yes | yes - `Issue7385RestoreProgressIT.restoreBackupPublishesProgressAtTheControlPlane` |
| failing restore (any of the four) must still retire the operation | yes | yes - `Issue7385RestoreProgressIT.aFailedRestoreRetiresTheOperation` |
| `Restore.setProgressCallback` on the parallel extractor | yes | yes - `Issue7385RestoreProgressCallbackTest.parallelRestoreReportsEntryCountsAgainstAKnownTotal` |
| `Restore.setProgressCallback` on the sequential walk | yes | yes - `Issue7385RestoreProgressCallbackTest.sequentialRestoreReportsEntryCountsWithAnUnknownTotal` |
| startup `SERVER_DEFAULT_DATABASES` `{restore:<url>}` -> `ArcadeDBServer.loadDefaultDatabases` | **no** - filed | n/a |
| `Restore.main()` CLI | **no** - argued | n/a |

### Argued rows

- **`Restore.main()` CLI.** `OperationProgressRegistry` is process-local, and it has exactly two
  readers:

  ```
  $ grep -rn "OperationProgressRegistry.instance().getOperations" --include="*.java" . | grep -v /src/test/
  server/.../ServerControlPlane.java:255      # HTTP /progress/{db} and gRPC GetProgress
  console/.../Console.java:1165               # the console's own poller, EMBEDDED databases only
  ```

  `Restore.main()` calls `System.exit(0)` when it returns and runs neither a server nor a console, so
  a registration there has no reader by construction. The new `setProgressCallback` is available to
  any embedder that wants the same counters without the registry.

### Filed rows

- **Startup `restore:` command** - #7440. `httpServer.startService()` runs at
  `ArcadeDBServer.java:386`, before `loadDefaultDatabases()` at `:408`, so the endpoint really is
  listening while a startup restore runs and a `root` poll would be authorized
  (`checkAuthorizationOnDatabase` only calls `canAccessToDatabase`, which the wildcard grant answers
  for a database that does not exist yet). That path does not go through the control plane, has no
  `ProgressListener`, and cannot be driven from a test deterministically, so it is filed rather than
  fixed blind.

### Residual risk

- A restore started before this build, or by the startup path, still shows nothing (#7440).
- The `done/total` counters are per **archive entry**, not per byte: a database that is one 4 GB page
  file and six tiny ones reports `6/7` for almost the whole restore. That is the granularity #6086
  chose for the parallelism itself, and finer progress would need per-entry byte accounting the
  extractor does not keep.
- Progress is process-local: in HA, polling a replica shows nothing while the leader restores. That is
  the registry's documented design, not a gap this change introduces.

## Changes

**`integration`** - the restore learns to report counters.

- `Restore.setProgressCallback(ProgressCallback)`: a new opt-in sink, installed on the format before it runs.
  `ProgressCallback` lives in the engine (`com.arcadedb.utility`), which the server already has on its
  classpath, so the server installs it with a plain reflective `getMethod` and no proxy - unlike the
  `ConsoleLogger$LogListener` it has to proxy.
- `AbstractRestoreFormat`: holds the callback, owns the `RESTORE_STEP_NAME` constant and the null-safe
  `reportRestoreProgress`.
- `FullRestoreFormat`: the sequential walk reports `(entriesDone, -1)` per entry and hands the callback to the
  parallel extractor.
- `ParallelZipExtractor`: a new 3-arg constructor takes the callback (the 2-arg one still exists and still
  means "no progress"); reports `(0, n)` before the pool is created, `(done, n)` per entry from the worker, and a
  closing `(n, n)` from the coordinating thread once every worker has been collected - because the per-entry
  reports come from several threads and can publish `n` then `n-1`, which would otherwise leave a finished
  extraction showing one entry short for the rest of the operation. The per-entry report was also moved
  **before** its log line, so a caller watching both never sees a line saying an entry is done while the counter
  still says it is not.

**`server`** - `ServerControlPlane.performRestore` registers the operation.

- Registered before the first byte is extracted, retired in a `finally` that now spans the extract, the swap and
  the replicate - the three phases are published as steps 1, 2 and 3 of `RESTORE_STEPS`, so the endpoint keeps
  saying something after the archive is unpacked (the swap drops the database being replaced; in HA the
  replication makes every replica pull the restored files).
- The operation is labelled with the command the operator typed - `restore database` or `restore backup` - rather
  than one name standing for both, so `performRestore` takes it as a parameter.
- `installRestoreProgressCallback` renumbers the format's own step (always 1 of 1, because the integration module
  cannot see the swap and replicate phases) into step 1 of three. Best-effort like `importerContext`: a build of
  `arcadedb-integration` without the setter reports no counters rather than failing the restore.

**`engine`** - one javadoc line: `OperationProgressRegistry` now lists restores among its producers.

### Why no Studio change

Studio's two restore actions (`importWithSSE` from the dataset modal, and `doRestoreBackup` from the backup
panel) already ask for `text/event-stream` and render the restore's own log lines, so they were never the blind
half. `startCommandProgressMonitor` is the query editor's poller and matches SQL commands only; there is no
`RESTORE DATABASE` SQL statement, so adding restore to its regex would be dead code. What was blind is exactly
what the issue's scope note says: a third party looking at the database - another Studio session, the console,
an operator with curl - polling `/api/v1/progress/{database}`. That is what this change fixes.

## Test results

```
$ mvn -o -pl integration -Pintegration -DexcludedGroups=benchmark verify
Tests run: 129, Failures: 0, Errors: 0, Skipped: 0      # ITs, incl. Issue6086ParallelRestoreIT (19)
   + Issue7385RestoreProgressCallbackTest               3/3

$ mvn -o -pl server -Pintegration -Dit.test='com.arcadedb.server.backup.*IT' verify
Tests run: 1042, Failures: 0, Errors: 0, Skipped: 0     # server unit suite
Tests run: 32,   Failures: 0, Errors: 0, Skipped: 0     # backup/restore ITs
   + Issue7385RestoreProgressIT                         4/4

$ mvn -o -pl grpcw -Pintegration -Dit.test='Issue7308Grpc*IT,Issue7310Grpc*IT' verify
Tests run: 37, Failures: 0, Errors: 0, Skipped: 0
```

### The tests were proved able to fail

Three canary runs, each reverted:

| Canary | Result |
|---|---|
| `Restore.restoreDatabase()` stops installing the callback on the format | `Issue7385RestoreProgressCallbackTest` 2/3 fail, "Expecting actual not to be empty" |
| `performRestore` registers under `databaseName + "-CANARY"` | `Issue7385RestoreProgressIT` 4/4 fail |
| `installRestoreProgressCallback` looks up `setProgressCallbackCANARY` | `Issue7385RestoreProgressIT` 1/4 fails, "the archive entry count never reached the registry" |

The third canary is the one that justifies the `total > 0` assertion existing at all: the reflective lookup
swallows its own failure by design, so without that assertion a build that silently stopped installing the
callback would still publish the coarse step markers and every other assertion would still pass.

The same assertion caught a real defect in the test itself on its first run: `getOperations` hands back the
**live** `OperationProgress` objects the producer is still writing to, so collecting the references meant every
"sample" read alike at the end - whatever the last write left behind. The sampler takes an `op.toJSON()` snapshot
instead, which is also exactly what the progress endpoint serializes.

## Reachability

`performRestore` is reached by all four transports on live paths
(`PostServerCommandHandler.java:423`/`:457`, `ArcadeDbGrpcAdminService.java:797`/`:821`), and
`theProgressEndpointReportsARunningRestore` drives the whole stack end to end: a real HTTP `restore database`
against a deliberately slow local archive server, with `GET /api/v1/progress/{database}` polled from outside
until it reports the running restore. No feature flag gates any of it; `SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS`
gates only which URLs the test may use, not the publication.

## Adversarial pass

The orchestrator's Phase 1.5 calls for one subagent that has not been persuaded by the author's reasoning. **No
subagent-spawning tool was available in this session** (`ToolSearch` for a `Task`/general-purpose agent returned
nothing; the only peers `ListAgents` reports are other people's unrelated sessions). Per the skill's error table
that is not a hard gate, so the pass was run by the author against the diff, and what it found is recorded here
with the same disposition rules.

| Finding | Disposition |
|---|---|
| `performRestore`'s `finally` now spans the swap and the replicate, but only an **extraction** failure is tested. A failure in `swapRestoredDatabase` is not. | **Not real as a coverage gap.** One `finally` covers all three phases and the extraction-failure test proves it runs on the exception path; a second test would exercise the same `finally` through a harder-to-provoke throw. Recorded rather than argued away silently. |
| Moving the reflective block into an inner `try` could change which exceptions the `FileUtils.deleteRecursively(tempDir)` handlers see. | **Checked, not real.** `swapRestoredDatabase` and `replicateRestoredDatabase` sit *after* the inner `try/catch`, exactly where they sat before relative to the old outer one, so no temp-dir cleanup now fires for a failure that used to escape it. |
| `RESTORE_STEP_EXTRACT` in the server duplicates the string literal of `AbstractRestoreFormat.RESTORE_STEP_NAME`; a change to one silently desynchronises the other. | **Real, fixed here.** Unavoidable - `arcadedb-integration` is optional and reached only reflectively, so the constant cannot be referenced - so the duplication now carries a comment saying why it exists and what a drift actually costs (a changed label mid-step, nothing more). |
| The per-entry reports come from several worker threads, so `OperationProgress.done` can go backwards and a finished extraction could be left reading one entry short. | **Real, fixed here** before the pass, by the closing report the coordinating thread makes once every worker has been collected. Asserted by `parallelRestoreReportsEntryCountsAgainstAKnownTotal`. |
| `done`/`total` reset to `0`/`-1` at steps 2 and 3, so a reader watching a percentage sees 100% and then "unknown". | **Not real.** `ProgressCallback`'s contract is that `done`/`total` are the units of the *current step*, and steps 2 and 3 have no countable units. `getPercentage()` returns -1, which the console and Studio already render as "no bar". |
| A non-root user scoped to specific databases cannot poll the progress of a restore into a brand-new database, because `checkAuthorizationOnDatabase` asks `canAccessToDatabase` about a name that does not exist yet. | **Real, out of scope and pre-existing.** It is the progress endpoint's authorization rule, not something this change introduces, and it applies equally to `import database` since #7308. Not filed: refusing to report on a database the caller may not access is the endpoint behaving as designed. |

## Pull request

https://github.com/ArcadeData/arcadedb/pull/7446

### Review cycles

| Cycle | Head SHA | Changes | Bot outcome |
|---|---|---|---|
| 1 | `30cc4ae9` | the initial implementation as described above | `claude` reviewed by manual code tracing (it reported that `mvn` was blocked by its sandbox, so it did not run the suite). Independently re-derived the two claims worth checking rather than taking the PR body's word for them: that the swap/replicate placement relative to the inner `try/catch` is unchanged from before the PR, and that the startup path at `ArcadeDBServer.java:1393-1416` really does still lack the wiring, so the #7440 gap claim is accurate. It flagged the `RESTORE_STEP_EXTRACT` / `RESTORE_STEP_NAME` literal duplication as a future-maintenance fragility and agreed the optional-dependency constraint leaves no compile-time way to share it - which is what the comment added before the PR opened already says. **No blocking issues, no actionable items.** |

No changes were applied in response to the review, so no follow-up commit was needed and no deferred-items notes
file was produced.

### Final state

`clean-approval` on cycle 1 of a maximum of 4.
