# #7450 - SQL `EXPORT DATABASE` bypasses the per-database maintenance slot

Issue: https://github.com/ArcadeData/arcadedb/issues/7450
PR: https://github.com/ArcadeData/arcadedb/pull/7647
Follow-up to #7443 (SQL `BACKUP`/`IMPORT` took the slot) and #7384 (the slot admitted restores and imports).

## Root cause

`ExportDatabaseStatement.executeSimple` reads the whole database off disk and writes an archive - the
same shape of work `BACKUP DATABASE` does - and reserved nothing. A concurrent `restore database` of the
same database drops and replaces the directory the export is reading, which is exactly the hazard #7384
closed for backups and #7443 closed for SQL backups.

It was left out of #7443 because `EXPORT` cannot be added to the existing vocabulary unchanged: every
constant of `MaintenanceCoordinator.Operation` conflicts with itself, and `BackupCoordinator` relies on
that property by storing the running operations of a database in an `EnumSet` (at most one of each kind,
so a set needs no multiplicity). Two exports of one database to two different files are legitimate, so
admitting `EXPORT` means `conflictsWith` stops being "self plus RESTORE" and the set has to become a
multiset, so that each of two concurrent exports releases only its own reservation.

## Invariant

> While a `RESTORE` holds a database's maintenance slot no `EXPORT DATABASE` of it may start, and while
> any `EXPORT DATABASE` of a database is running no `RESTORE` of it may start - and every other pair
> involving an export (export/export, export/backup, export/import) is admitted, each holder releasing
> exactly its own reservation.

## Completeness

### Commands run (Step 4.5 sweep)

```
$ grep -rn "integration.exporter.Exporter\|new Exporter(" --include="*.java" . | grep -v "/test/"
gremlin/.../GraphMLExporterFormat.java:24:  import com.arcadedb.integration.exporter.ExporterContext;    (format SPI, not an entry point)
gremlin/.../GraphSONExporterFormat.java:24: import com.arcadedb.integration.exporter.ExporterContext;    (format SPI, not an entry point)
integration/.../format/AbstractExporterFormat.java:22                                                     (format SPI, not an entry point)
integration/.../format/JsonlExporterFormat.java:32                                                        (format SPI, not an entry point)
integration/src/main/java/com/arcadedb/integration/exporter/Exporter.java:53: new Exporter(args).exportDatabase();   (CLI main)
engine/src/main/java/com/arcadedb/query/sql/parser/ExportDatabaseStatement.java:80                        (THE SQL entry point)
```

```
$ grep -rn "EXPORT DATABASE\|exportDatabase\|\"export\"" --include="*.java" \
      server grpcw ha-raft mongodbw postgresw redisw bolt console studio graphql | grep -v "/test/"
console/src/main/java/com/arcadedb/console/Console.java:161: "connect", ..., "export", "import", ...
```

The single console hit is the tab-completion word list; the console's `export` is executed as the SQL
statement. There is no HTTP, gRPC, Postgres, Bolt, Mongo or Redis handler that exports a database:

```
$ grep -rln "Export" --include="*.java" server/src/main/java grpcw/src/main/java
(no output)
```

```
$ grep -rn "Operation.values()" --include="*.java" .
server/src/test/.../Issue7443SqlMaintenanceSlotIT.java:86           @AfterEach end() sweep - unaffected
server/src/test/.../Issue7384ConcurrentRestoreIT.java:125,130       @AfterEach end() sweep - unaffected
server/src/test/.../Issue7384OperationAdmissionTest.java:49,62      loops still hold with EXPORT
server/src/test/.../Issue7384OperationAdmissionTest.java:98         ASSERTS THE PROPERTY #7450 CHANGES
server/src/test/.../Issue7384OperationAdmissionTest.java:169        enumerates the admissible co-holder sets
server/src/test/.../Issue7441RestoreNameReservationIT.java:123,130  @AfterEach end() sweep - unaffected
grpcw/src/test/.../Issue7384GrpcRestoreSerialisationIT.java:134,139 @AfterEach end() sweep - unaffected
```

```
$ grep -rn "EnumSet<Operation>" --include="*.java" .
server/src/main/java/com/arcadedb/server/backup/BackupCoordinator.java:108 (javadoc), 111, 158, 233
```

The `EnumSet` is confined to `BackupCoordinator`; nothing outside it holds or is handed one, so turning
it into a counter array changes no signature.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| SQL `EXPORT DATABASE` -> `ExportDatabaseStatement.executeSimple` (reached over HTTP, Postgres, gRPC, Bolt, the console and embedded-with-a-server alike) | yes | yes - `Issue7450SqlExportMaintenanceSlotTest` (engine, reserve/release/refusal/no-coordinator) and `Issue7450SqlExportMaintenanceSlotIT` (server, real `BackupCoordinator` bound, HTTP 409) |
| `MaintenanceCoordinator.Operation.conflictsWith` policy for `EXPORT` | yes | yes - `Issue7450ExportAdmissionTest` |
| `BackupCoordinator` admission structure (set -> per-kind counter) | yes | yes - `Issue7450ExportAdmissionTest` (two exports coexist and release independently, third-party kinds unaffected) |
| HA snapshot install (`begin(name, RESTORE, timeoutMs)`) vs a running export | yes - `RESTORE` conflicts with `EXPORT` in both directions, and the bounded wait re-checks through the same `begin` | yes - `Issue7450ExportAdmissionTest#aSnapshotInstallWaitsForARunningExport` |
| CLI `Exporter.main` (`integration/.../Exporter.java:53`) | argued - see below | n/a |
| A default-named `EXPORT DATABASE` resolving the SAME file name on every run (cached statement mutated its own `url`) | yes - found by the adversarial pass, fixed here because the admission policy's stated premise depends on it | yes - `Issue7450SqlExportMaintenanceSlotTest#executingADefaultNamedExportDoesNotMutateTheCachedStatement` and `Issue7450SqlExportMaintenanceSlotIT#twoSuccessiveDefaultNamedExportsProduceTwoDifferentArchives` |
| Two concurrent exports of one database racing onto the SAME target file (same explicit URL, or two default names in one millisecond) | no | filed as a follow-up (see Residual risk) |
| Overlapping exports starving a waiting `RESTORE` | no | filed as a follow-up (see Residual risk) |
| `EXPORT DATABASE` publishes no `OperationProgress` entry (its sibling `BACKUP DATABASE` does) | no | filed as a follow-up (see Residual risk) |

**Argued - CLI `Exporter.main`.** The CLI runs in its own JVM with no `ArcadeDBServer` in it, so no
`MaintenanceCoordinator` is bound to the database it opens and `MaintenanceCoordinator.reserve` returns
`Reservation.NONE`. That is not a gap this issue introduces: the CLI backup and the CLI import are in
exactly the same position after #7384 and #7443, and the admission is documented as per server instance.
A CLI export of a directory a server is restoring is out of reach of any in-process policy; what protects
the archive there is the exporter refusing to overwrite an existing file.

## Residual risk

Two things this fix deliberately does not cover, both filed before the PR opened:

1. **Two concurrent exports to the same EXPLICIT target file.** Admitting `EXPORT` concurrently with itself is the
   point of this issue, and the justification is that two exports resolve to two different files. They do
   not have to: `EXPORT DATABASE "same.jsonl.tgz" WITH overwrite = true` twice, or two default-named
   exports starting in the same millisecond, resolve to one path. The exporter formats check
   `file.exists() && !settings.overwriteFile` and then create the file, which is check-then-create and not
   atomic. This is not a regression - nothing coordinated two SQL exports before this change either - but
   the change is what makes it worth naming. Filed as **#7644**. Two of its three cases are closed in this
   PR: the cached statement reusing the FIRST run's name (adversarial pass, below) and the same-millisecond
   default-name collision (review cycle 1, below). What #7644 still tracks is the explicit same-URL case -
   `EXPORT DATABASE "same.tgz"` twice - and the non-atomic `file.exists()`-then-create in the formats, which
   no naming change can close.
2. **No live progress for `EXPORT DATABASE`.** `BackupDatabaseStatement` registers an `OperationProgress`
   so the operation is visible in the progress endpoint, the console and Studio while it runs;
   `ExportDatabaseStatement` never has. Now that an export holds a slot that can refuse a restore, an
   operator who sees a refusal has no way to see what is holding it. Filed as **#7645**.

3. **Overlapping exports can starve a restore.** The slot used to admit at most one operation of each kind,
   so a waiter - the HA snapshot install's bounded wait from #7444 - was always waiting for a bounded amount
   of work. `EXPORT`'s count is unbounded by design, so a stream of overlapping exports of one database can
   refuse every `restore database` and expire every snapshot-install wait. It needs a deliberate operator
   loop, since `EXPORT DATABASE` is admin-only and no schedule issues it, but the liveness property it
   removes was previously free. Filed as **#7646**.

Nothing else in the coverage table is blank.

## Adversarial pass

The `Task` tool for spawning an isolated subagent is not available in this environment, so the pass was run
by re-reading the tree against the issue text rather than by an agent that had not been persuaded. Three
findings, each verified by running something rather than by reading:

### 1. A cached `EXPORT DATABASE` reused the first run's file name - REAL, fixed here

The premise this whole change rests on, written into `Operation`'s javadoc, is that two exports of one
database write two different files. A probe against a live server said otherwise:

```
PROBE-FIRST : 200 {"operation":"export database","toUrl":"graph-export-20260915-230306764.jsonl.tgz", ... "result":"OK"}
PROBE-SECOND: 500 {"detail":"Error on exporting database -> The export file
                   'exports/graph-export-20260915-230306764.jsonl.tgz' already exist and '-o' setting is false"}
```

Two `EXPORT DATABASE` 50 ms apart resolved to ONE name. `StatementCache.get` hands the same parsed
`Statement` instance to every execution of the same text:

```
$ sed -n '59,75p' engine/src/main/java/com/arcadedb/query/sql/parser/StatementCache.java
  public Statement get(final String statement) {
    ... parsedStatement = cache.remove(statement); if (parsedStatement != null) cache.put(statement, parsedStatement);
    ... if (parsedStatement == null) { parsedStatement = parse(statement); ... }
    return parsedStatement;
```

and `executeSimple` wrote its resolved default back into `this.url`, freezing the first run's timestamp for
the life of the database - and racing on that one field when two executions overlap, which is precisely the
collision the admission policy assumes away.

Fixed in scope, because the alternative was shipping a javadoc claim the code contradicts: the default name
is resolved into a local and `this.url` is never assigned. Regression tests
`Issue7450SqlExportMaintenanceSlotTest#executingADefaultNamedExportDoesNotMutateTheCachedStatement` and
`Issue7450SqlExportMaintenanceSlotIT#twoSuccessiveDefaultNamedExportsProduceTwoDifferentArchives`, both
proved to fail against the old field assignment (one failure each, re-verified by reinstating it).

The change also makes the statement's identity stable across execution - `getIdentityElements()` returns
`{url, settings}`, and executing one used to mutate the first of them. `BackupDatabaseStatement` never had
this shape: it reads `this.url` and never assigns it.

### 2. Overlapping exports can starve a restore - REAL, out of scope, filed as #7646

See Residual risk 3.

### 3. A new enum constant leaking into a wire protocol or an exhaustive switch - NOT REAL

`Operation` is engine-internal and is not mapped to any proto, HTTP payload or persisted form, and nothing
switches over it:

```
$ grep -rn "Operation\." --include="*.java" grpcw/src/main/java server/src/main/java/com/arcadedb/server/http \
    | grep -i "maintenance\|BACKUP\|RESTORE\|IMPORT"
(no output)

$ grep -rn "switch" --include="*.java" server/src/main/java engine/src/main/java | grep -i operation
engine/src/main/java/com/arcadedb/query/sql/parser/UpdateOperations.java:56:    switch (type) {   (unrelated: UPDATE clause kinds)
```

Every other consumer is a test `@AfterEach` loop over `Operation.values()` calling `end()`, which a new
constant only makes more thorough. `mvn -o -q -pl grpcw test-compile` exits 0.

## Changes

| File | Change |
|---|---|
| `engine/.../engine/MaintenanceCoordinator.java` | `Operation.EXPORT("export", "an export")`. `conflictsWith` becomes "a restore excludes everything, and everything else excludes a second of its own kind except `EXPORT`". Javadoc on the interface, the enum, `begin` and `end` says what an operation that does not exclude itself means for the contract. |
| `server/.../backup/BackupCoordinator.java` | `Map<String, EnumSet<Operation>>` becomes `Map<String, int[]>` - a reservation COUNT per `Operation.ordinal()`, copy-on-write so a reader outside the map's per-entry lock is never handed a mutating value. `begin` walks a cached `Operation[]` and increments; `end` decrements one claim and drops the entry only when every count is zero, and refuses to go below zero. |
| `engine/.../sql/parser/ExportDatabaseStatement.java` | Wraps the export in `try (Reservation slot = MaintenanceCoordinator.reserve(db, Operation.EXPORT))`, placed AFTER the target validation so a statement its own validation rejects never holds the slot. |
| `engine/.../exception/DatabaseOperationInProgressException.java` | Javadoc names `EXPORT DATABASE` as a raiser. |

Two existing tests in `Issue7384OperationAdmissionTest` asserted the property this issue deliberately
changes, and were adjusted rather than left red - no assertion was removed:

* `anOperationAlwaysExcludesASecondOneOfItsOwnKind` skips `EXPORT` with a comment naming #7450 and the test
  that covers it instead. The three kinds that do exclude themselves keep every assertion they had.
* `concurrentCallersNeverBothHoldConflictingOperations` enumerated the co-holder sets that may survive a
  race; the four sets `EXPORT` adds are appended. Its real assertion - the pairwise `conflictsWith` loop -
  is untouched.

## Verification

```
mvn -o -pl engine test -Dtest='Issue7450SqlExportMaintenanceSlotTest,Issue7443SqlMaintenanceSlotTest,
    ExportDatabaseStatementTestParserTest,Issue6409FollowupsTest,Issue6401NodeIdentityTest,
    BackupDatabaseStatementTestParserTest'
  -> Tests run: 38, Failures: 0, Errors: 0

mvn -o -pl server verify -Dtest='BackupCoordinatorTest,Issue7384OperationAdmissionTest,
    Issue7444MaintenanceSlotWaitTest,Issue7450ExportAdmissionTest'
  -Dit.test='Issue7450SqlExportMaintenanceSlotIT,Issue7443SqlMaintenanceSlotIT,Issue7384ConcurrentRestoreIT,
    Issue7441RestoreNameReservationIT,ExportBackupAuthorizationIT'
  -> Tests run: 31, Failures: 0 (unit) and Tests run: 31, Failures: 0 (IT)

mvn -o -pl ha-raft test -Dtest='Issue7449StartupRecoveryTakesMaintenanceSlotTest,
    Issue7444SnapshotInstallExcludesBackupTest,Issue7530RecoveryClosesOpenDatabaseTest'
  -> Tests run: 16, Failures: 0, Errors: 0

mvn -o -q -pl grpcw test-compile  -> exit 0 (the module reads Operation.values() in its IT fixtures)
```

### The new tests were proved able to fail

Reverting only `ExportDatabaseStatement.java` to its `HEAD` content and re-running the engine test:

```
Tests run: 6, Failures: 4  -- Issue7450SqlExportMaintenanceSlotTest
  aSqlExportReservesTheSlotAndReleasesItEvenWhenTheExportFails      FAILURE
  aSqlExportIsRefusedWhileARestoreHoldsTheSlotAndOwesNoRelease      FAILURE
  theSlotIsAskedForTheExportsOwnOperation                           FAILURE
  anExportWithAnExplicitTargetReservesTheSameSlot                   FAILURE
```

The two that still pass are the ones that must: `withNoCoordinatorBoundTheExportRunsExactlyAsBefore` and
`anExportRejectedByItsOwnValidationReservesNothing` assert that behaviour was PRESERVED, so passing before
the fix is what they are for.

Reverting only `BackupCoordinator.java` to its `HEAD` content - the `EnumSet` version, which still compiles
against the new `EXPORT` constant - isolates the multiset half:

```
Tests run: 9, Failures: 3  -- Issue7450ExportAdmissionTest
  endingOneOfTwoExportsLeavesTheOtherHoldingTheDatabase             FAILURE
  manyExportsAndOneBackupEachReleaseOnlyTheirOwnReservation         FAILURE
  aSnapshotInstallWaitsForEveryRunningExport                        FAILURE
```

That is the defect the issue predicted: with a set of kinds, the first of two exports to finish frees the
database and lets a restore in under the second one.

## Reachability

The changed code runs on a live path, not only under the new tests:

```
$ grep -rn "new BackupCoordinator()\|WRAPPER_NAME" --include="*.java" server/src/main/java engine/src/main/java
server/src/main/java/com/arcadedb/server/ArcadeDBServer.java:185: private final BackupCoordinator backupCoordinator = new BackupCoordinator();
server/src/main/java/com/arcadedb/server/ServerDatabase.java:98:  wrapped.setWrapper(MaintenanceCoordinator.WRAPPER_NAME, server.getBackupCoordinator());

$ grep -rn "MaintenanceCoordinator.reserve(" --include="*.java" . | grep -v /test/
engine/.../ImportDatabaseStatement.java:82  Operation.IMPORT
engine/.../ExportDatabaseStatement.java:94  Operation.EXPORT   <- new
engine/.../BackupDatabaseStatement.java:102 Operation.BACKUP
```

The coordinator is an unconditional field of `ArcadeDBServer`, bound by `ServerDatabase` to every database
the server opens with no flag in the way, and the reservation in the statement is unconditional too.
`Issue7450SqlExportMaintenanceSlotIT` closes the loop end to end over HTTP against a real server: it asserts
the statement is looking at the very `BackupCoordinator` the server's own entry points use, gets a 409 on
`EXPORT DATABASE` while a restore holds the slot, and gets a 409 on `restore database` while an export holds
it.

## Ledger

- [x] `ExportDatabaseStatement` takes the slot for a new `Operation.EXPORT` that excludes only `RESTORE` - done
- [x] `BackupCoordinator` counts reservations per kind rather than holding a set of kinds - done
- [x] A test that two exports of one database coexist - `Issue7450ExportAdmissionTest#twoExportsOfOneDatabaseRunTogether`, and `Issue7450SqlExportMaintenanceSlotIT#aSecondSqlExportOfTheSameDatabaseIsAdmitted` through the statement
- [x] A test that a restore in flight refuses an export and vice versa - `Issue7450ExportAdmissionTest#anExportAndARestoreOfOneDatabaseExcludeEachOther`, and both directions over HTTP in the IT

## Review cycles

### Cycle 1 - `26c891e3`

`claude` (PR issue comment, the gating surface on this org's repos): no blocking findings. It verified
`conflictsWith`'s new asymmetric relation against the pairwise cases, the `int[]` multiset's copy-on-write
property, the reservation being taken after validation and permission checks, and confirmed the
`StatementCache` reasoning behind the `this.url` fix independently. One non-blocking observation - the
`@AfterEach` drain bound in the IT is a magic number - explicitly marked "not a request for change"; the
bound was nonetheless raised from 16 to 64 and its comment now names the worst case (six, in the new
concurrency test) rather than saying "a couple".

`coderabbitai` (inline thread on `ExportDatabaseStatement.java:72`, Major): **make default export names
collision-safe**. Two default targets can still match when both executions start within the same millisecond,
and the formats check `file.exists()` and then create, so a shared name means two writers of one archive
rather than one clean refusal.

Accepted. It is the same gap as residual risk 1, and it undercuts the admission policy's own premise, so
closing the half a naming change CAN close belongs here rather than in the follow-up. Verified first that
nothing reads an export archive name back:

```
$ grep -rn -- "-export-" --include="*.java" . | grep -v /test/
engine/src/main/java/com/arcadedb/query/sql/parser/ExportDatabaseStatement.java:70   (the only producer)
```

(unlike a BACKUP archive, whose timestamp `BackupCoordinator.parseArchiveTimestamp` parses for retention and
listing - a suffix there WOULD have broken a reader).

Applied as the suggested `UUID.randomUUID()` component, with the timestamp kept first so the archives of one
database still sort chronologically, and with the name generation extracted to a package-private
`ExportDatabaseStatement.defaultTargetName(databaseName, format)`. The extraction is what makes the review's
requested test deterministic rather than a race the test hopes to lose: `defaultTargetName` is called a
thousand times in a tight loop, the run asserts at least one repeated TIMESTAMP - proving the
same-millisecond case was genuinely exercised - and asserts all thousand names are distinct. Reverting the
random component to a constant fails exactly that test and the convention test, and nothing else.

`Issue7450SqlExportMaintenanceSlotIT#concurrentDefaultNamedExportsProduceDistinctArchives` adds the
end-to-end half the review asked for: six concurrent `EXPORT DATABASE` requests, all admitted, six distinct
targets, six archives on disk, slot free afterwards.

Re-verified: engine 56 tests / 0 failures, server 31 unit + 33 IT / 0 failures.

No deferred items. Nothing was skipped as a disagreement.

### Cycle 2 - `e5aadf50`

`claude`: no correctness bug found. It traced all 4x4 `conflictsWith` pairs by hand against the stated
invariant, confirmed the `int[]` copy-on-write swap never publishes a partially-updated array to
`isInProgress`, confirmed the reservation ordering matches `BackupDatabaseStatement`, and confirmed the
`this.url` removal is a real fix rather than a style change. Two non-blocking notes, both explicitly "no
action needed": the `@AfterEach` drain bound is still a magic number (flagged only in case a heavier
concurrency test is added later), and #7646 / #7644 are correctly scoped out and tracked.

`coderabbitai`: re-reviewed the push, posted no actionable comments, and resolved its own thread from cycle 1
after re-verifying the fix.

Working tree clean, nothing applied, no deferred items.

## Deferred items

None. No `review-deferred-*.md` was produced by this run - the six such files in `docs/` are committed
artifacts of earlier PRs (#7210, #7442, #7556, #7585), not of this one.

## Final state

`clean-approval` after 2 review cycles.

Follow-ups opened by this work, none of which block the merge:

| Issue | What it tracks |
|---|---|
| #7644 | Two exports naming the SAME explicit URL, and the non-atomic `file.exists()`-then-create in the exporter formats. Its other two cases - the cached statement reusing a name, and the same-millisecond default-name collision - were closed in this PR. |
| #7645 | `EXPORT DATABASE` publishes no `OperationProgress`, so an operator who reads "an export of it is already in progress" has no surface showing that export. |
| #7646 | `EXPORT`'s reservation count is unbounded by design, so overlapping exports can refuse every restore and expire the HA snapshot install's bounded wait. |

Merge is the developer's.

## CI on the final commit

`unit-tests` is red on `5a69666`, and none of it is this PR. Three tests fail, all in code this diff does
not touch (`git diff --name-only origin/main...HEAD` lists only `MaintenanceCoordinator`,
`DatabaseOperationInProgressException`, `ExportDatabaseStatement`, `BackupCoordinator` and their tests):

| Failing test | Module |
|---|---|
| `MultiColumnAggregationResultTest#emptySumAndCountStayZeroNotNaN` | engine / timeseries |
| `Issue7089NaNTransparentSumAvgTest#oneNaNSampleNoLongerPoisonsTheBucketOnAnyPath` | engine / timeseries |
| `ArcadeStateMachinePerDatabaseHaltTest` (both methods) | ha-raft |

Checked rather than asserted - a second worktree was cut at pristine `origin/main` and the same classes run
there:

```
origin/main (detached, no changes from this branch):
  MultiColumnAggregationResultTest.emptySumAndCountStayZeroNotNaN            FAILURE
  ArcadeStateMachinePerDatabaseHaltTest.perDatabaseApplyErrorDoesNotTripNodeWideHalt   FAILURE
  ArcadeStateMachinePerDatabaseHaltTest.otherDatabasesKeepApplyingAfterOneDatabaseFails FAILURE
```

Identical failures, so `main` is already red on them. (`Issue7089NaNTransparentSumAvgTest` passes locally on
both trees; its CI failure is in the same timeseries NaN area and equally untouched by this diff.)

Every test class this PR adds or changes passed in that same CI run:

```
com.arcadedb.query.sql.parser.Issue7450SqlExportMaintenanceSlotTest: tests=9 failures=0 errors=0
com.arcadedb.server.backup.Issue7450ExportAdmissionTest:             tests=9 failures=0 errors=0
com.arcadedb.server.backup.Issue7384OperationAdmissionTest:          tests=8 failures=0 errors=0
com.arcadedb.query.sql.parser.Issue7443SqlMaintenanceSlotTest:       tests=7 failures=0 errors=0
com.arcadedb.server.backup.Issue7444MaintenanceSlotWaitTest:         tests=6 failures=0 errors=0
com.arcadedb.server.backup.BackupCoordinatorTest:                    tests=8 failures=0 errors=0
```

and `integration-tests`, `vector-unit-tests`, `build-and-package`, `lint`, `claude-review`, CodeQL, Codacy,
Meterian and every e2e lane are green.
