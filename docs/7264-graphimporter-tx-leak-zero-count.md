# #7264 - A failure inside GraphImporter's row loop leaks a nested transaction and reports 0 vertices

Issue: https://github.com/ArcadeData/arcadedb/issues/7264
Branch: `fix/7264-graphimporter-tx-leak-zero-count`
Type: bug (labels `bug`, `importer`; milestone 26.10.1)

## Finding ledger

- [x] 1. A throw inside `processVertexSource`'s row loop skips `database.commit()` and leaves the
      transaction the method pushed on the caller's stack - fixed here, 2 regression tests
- [x] 2. The same throw skips `ts.count` / `totalVertices`, so `getVertexCount()` reports 0 for a
      source whose intermediate commits made rows durable - fixed here, 1 regression test
- [x] 3. Same shape of defect in four other importer entry points - filed as
      [#7272](https://github.com/ArcadeData/arcadedb/issues/7272)

## Analysis

`GraphImporter.processVertexSource` (`integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java`)
opened a transaction before the row loop, committed every 50,000 rows inside it, committed once
after it, and then assigned the per-type counters. No `try`, no `finally`, and `run()` has no
`catch` either.

Two consequences, both verified by a test that failed before the fix:

1. **Nested transaction left behind.** `LocalDatabase.begin()` pushes a *nested*
   `TransactionContext` when one is already active
   (`engine/src/main/java/com/arcadedb/database/LocalDatabase.java:627-644`); `commit()` pops it
   through `popIfNotLastTransaction()` (`:655-674`). A caller that held its own transaction got it
   shadowed: the caller's next `commit()` popped and committed the importer's abandoned rows and
   left the caller's own work uncommitted and still open.
   Checked and ruled out: `GraphBatch.close()`, which runs on the way out of the failed
   try-with-resources in `run()`, does **not** resolve the leak. Its `flush()` returns immediately
   (`edgeCount == 0` during the vertex pass) and `batchUpdateVertexHeadChunks()` is skipped because
   `preAllocateEdgeChunks` writes the head chunk straight onto the vertex
   (`GraphBatch#getOrCreateOutEdgeChunk`) instead of populating `deferredOutHead`.

2. **Zero reported for a partial import.** `ts.count` and `totalVertices` were assigned after the
   final commit, so a failure left them at 0 while the intermediate commits had already made
   whole multiples of 50,000 rows durable. `getVertexCount()` is public, so an operator or a caller
   that catches the exception reads 0 and concludes nothing was written.

One correction to the fix sketch in the issue body:

- the sketch assigns `ts.count = count[0]` in the `finally`. `count[0]` is the number of rows
  **read**, which after a rollback overstates what is on the disk by up to 50,000. The fix tracks
  the count at the last successful commit separately and reports that.
- the sketch catches `RuntimeException`. `RecordSource.forEach` and `RecordVisitor.visit` are both
  declared `throws Exception`, and the issue itself lists "an `IOException` from the reader" as a
  reachable throw site, so the catch has to be `Exception`.
- a bare `database.rollback()` is not safe on its own: it pops whatever transaction is on top of
  the stack, so firing it after this method's own transaction has already been committed and popped
  would roll back the **caller's**. The fix tracks whether the pushed transaction is still current.

## Invariant

`processVertexSource` never returns or throws with the transaction it pushed still on the
transaction stack, and after a failure `getVertexCount()` reports the number of vertices durably
committed rather than zero.

## Completeness

### Ways to violate the invariant (commands and output)

Transaction sites inside `GraphImporter`:

```
$ grep -n "database\.begin()\|database\.commit()\|database\.rollback()" \
    integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java
976:    database.begin();
1042:        database.commit();
1043:        database.begin();
1046:    database.commit();
```

All four are inside `processVertexSource`. The class's three other units of work:

```
$ grep -n "public void run()\|private void process\|private void flushEdgeType" \
    integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java
780:  public void run() throws Exception {
941:  private void processVertexSource(final GraphBatch batch, final VertexSourceDef vsd) throws Exception {
1160:  private void processEdgeSource(final EdgeSourceDef esd, final int sourceIndex) throws Exception {
1231:  private void flushEdgeType(final EdgeCollector ec) {
```

Readers of the count the fix repairs:

```
$ grep -n "totalVertices" .../graph/GraphImporter.java
105:  private long totalVertices;
809:  ... "  Topology: %,d vertices, %,d edge refs", totalVertices, ...
820:        totalVertices, totalEdges, ...
911:    return totalVertices;          # getVertexCount()
1051:    totalVertices += ts.count;
```

Same-shape siblings across the importer package (`database.begin()` before a row loop with periodic
commits):

```
$ grep -rn "database\.begin()\|database\.commit()\|database\.rollback()" \
    integration/src/main/java/com/arcadedb/integration/importer/
... format/JsonlImporterFormat.java:140,171,172,187,188,203,212,909,910,923,924
... format/JSONImporterFormat.java:152,160,190,227,230,245,248
... format/CSVImporterFormat.java:196,198,209,216,283,301,302,305,449,451,462,468,590,602,603,613
... format/RDFImporterFormat.java:51,81,82,86
... Neo4jImporter.java:341,385,386,403,642,669
... OrientDBImporter.java:491,506,507,510
... AbstractImporter.java:112,139,148,156,161
```

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `processVertexSource` row loop throws, no caller transaction | yes | yes - `aFailedVertexSourceLeavesNoTransactionActive` |
| `processVertexSource` row loop throws, caller holds a transaction | yes | yes - `aFailedVertexSourceGivesTheCallerItsOwnTransactionBack` |
| `processVertexSource` throws after an intermediate commit (report) | yes | yes - `aFailureAfterAnIntermediateCommitReportsTheRowsThatCommitted` |
| `processVertexSource`'s own `database.commit()` throws | yes | **argued** - `LocalDatabase.commit()` pops the transaction in a `finally` (`:655-674`), so it is off the stack whether it succeeds or throws. The fix clears the "still mine" flag *before* the call, so the catch does not then roll back the caller's. Driving a commit failure needs fault injection the importer has no hook for |
| `processEdgeSource` | **argued** - opens no transaction; the grep above shows every `begin()` in the class is inside `processVertexSource`, and the method only fills in-memory buffers | n/a |
| `flushEdgeType` | **argued** - delegates to `GraphBatch`, which owns its transactions and is closed by try-with-resources | n/a |
| `run()`'s `GraphBatch` close after a failed vertex source | yes, by consequence - the transaction is rolled back before `close()` runs | exercised by all three tests (the batch is closed on the exceptional path) |
| `RDFImporterFormat.parse` | **filed** - [#7272](https://github.com/ArcadeData/arcadedb/issues/7272) | no |
| `CSVImporterFormat.loadEdges` | **filed** - [#7272](https://github.com/ArcadeData/arcadedb/issues/7272) | no |
| `OrientDBImporter.updateDocumentLinks` | **filed** - [#7272](https://github.com/ArcadeData/arcadedb/issues/7272) | no |
| `Neo4jImporter.readFile` | **filed** - [#7272](https://github.com/ArcadeData/arcadedb/issues/7272) | no |
| `JsonlImporterFormat` | **argued** - the mirror case (touching a transaction it does not own), already tracked by #6561 | n/a |
| `CSVImporterFormat.loadDocuments` / `loadVertices`, `JSONImporterFormat` | **argued** - both already roll back on the failure path (`rollbackIfOwned`, and `JSONImporterFormat:160/190/230`) | pre-existing tests |

### Reachability

`GraphImporter.processVertexSource` runs on every `GraphImporter.run()`, which is the only way to
use the class - `run()` is called by `GraphImporter.main`, by the JSON-config path
(`GraphImporter.fromJSON`) and by the existing tests. `getVertexCount()` is public and is the value
`main` prints. No flag gates the changed lines: the `try`/`catch`/`finally` wraps the loop
unconditionally, and the intermediate-commit branch is the same one that was already there, now
reading the extracted `COMMIT_EVERY_ROWS` constant.

### Residual risk

- A failure past an intermediate commit still leaves a **partial** graph. That is inherent to
  committing periodically; the fix makes the size of the partial import visible (`getVertexCount()`
  plus a `WARNING` naming the committed rows and the rolled-back ones) instead of hiding it behind
  a zero. It does not resume, truncate or otherwise repair the import.
- `ts.buckets` / `ts.positions` are trimmed and kept at their full read length while `ts.count`
  reports only the committed prefix. Nothing reads them after a failure - the exception propagates
  out of `run()` before pass 2, and `close()` clears `typeStates` - so the two are never compared;
  truncating them would cost a copy on the success path for no reader.
- The four sibling entry points listed above are untouched and tracked by #7272.
- A `rollback()` that fails is reported at `SEVERE` and swallowed, so a repeated failure there is
  visible only by reading the log. Giving the importer metrics would be the place to surface it
  (review cycle 3, observation 3); out of scope here.

## Changes

`integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java`

- extracted the literal `50_000` commit cadence into `COMMIT_EVERY_ROWS`, documented as the reason a
  failed import is partial rather than atomic;
- `processVertexSource` wraps the row loop and the final commit in `try` / `catch (Exception)` /
  `finally`:
  - `committed[0]` tracks the row count at the last successful commit;
  - `txOpen[0]` tracks whether the transaction this method pushed is still current, cleared *before*
    each `commit()` because `LocalDatabase.commit()` pops in a `finally`;
  - the catch logs a `WARNING` naming the committed and the rolled-back row counts when anything had
    been committed, and rethrows the original exception unwrapped (existing tests assert on those
    messages and types);
  - the `finally` rolls back when the pushed transaction is still current - in the `finally` rather
    than the `catch` so an `Error` resolves it too - and assigns `ts.buckets`, `ts.positions`,
    `ts.count = committed[0]` and `totalVertices` on every exit.

`integration/src/test/java/com/arcadedb/integration/importer/GraphImporterFailedSourceTest.java` (new)

- three tests, one per fixed row of the coverage table, driven through the public `RecordSource`
  extension point so the failing row can be placed exactly (an `intProperty` fed non-numeric text,
  which throws from `readProperty` - one of the throw sites the issue names).

## Adversarial pass

No `Task` tool was available in this session, so the Phase 1.5 subagent could not be spawned. The
pass was run inline instead, reading the diff against the issue body as the reporter would. What it
turned up:

- **Real, fixed here.** The rollback sat in `catch (Exception)`, so an `Error` - and
  `OutOfMemoryError` is the one a 200k-row import can realistically raise - would have run the
  counter `finally` and still left the transaction on the stack, which is the exact defect the issue
  reports. Moved into the `finally`, guarded by the same `txOpen[0]` flag. The three tests still
  pass; none of them can reach an `Error`, so this one row of the table rests on the guard being the
  same guard the tested paths use rather than on a test of its own.
- **Real, out of scope, filed.** Four other importer row loops have the same shape -
  [#7272](https://github.com/ArcadeData/arcadedb/issues/7272).
- **Considered, not real.** `totalVertices += ts.count` in a `finally` double-counting a type
  imported by two sources: `validateEdgeTargets` refuses a type declared by more than one vertex
  source (`GraphImporter.java:850-863`), and each call builds its own `TypeState`.
- **Considered, not real.** A `database.begin()` that itself throws (either the one before the loop
  or the one after an intermediate commit) skipping the rollback: `txOpen[0]` is false at both
  points, and nothing this method opened is on the stack, so there is nothing to roll back.
- **Considered, argued at first, then fixed in review cycle 2.** The `finally` masking the original
  exception if `rollback()` throws. `LocalDatabase.rollback()` already swallows
  `TransactionException` (`:677-690`), which is what made this look survivable - but a throw there
  would also have skipped the counter assignment after it, which is this issue's own "reports 0
  vertices" symptom re-entering through the code that fixes it. The rollback now reports a failure
  at `SEVERE` and swallows it.

## Test results

All three tests fail on the unfixed tree and pass on the fixed one.

Before the fix:

```
Tests run: 3, Failures: 3, Errors: 0, Skipped: 0
  aFailedVertexSourceLeavesNoTransactionActive:116
    [the transaction processVertexSource opened must be resolved before the failure propagates]
    Expecting value to be false but was true
  aFailedVertexSourceGivesTheCallerItsOwnTransactionBack:151
    [one commit for the one transaction the caller opened must leave nothing active]
    Expecting value to be false but was true
  aFailureAfterAnIntermediateCommitReportsTheRowsThatCommitted:181
    [the report must name the rows the intermediate commit made durable, not zero]
    expected: 50000L but was: 0L
```

After the fix:

- `mvn -o -pl integration test -Dtest=GraphImporterFailedSourceTest` - 3/3 pass (2.0 s; the 50,010
  row case runs in ~1.1 s, so no `@Tag("slow")`)
- `mvn -o -pl integration test` - `Tests run: 240, Failures: 0, Errors: 0, Skipped: 9`
- `mvn -o -pl integration verify -DskipITs=false -DskipTests=true` -
  `Tests run: 123, Failures: 0, Errors: 0, Skipped: 0` (includes `CSVImporterIT`, `Neo4jImporterIT`,
  `JsonLImporterIT`, `OrientDBImporterIT`)

## Impact

Behaviour changes only on the failure path of a vertex source:

- the transaction the importer pushed is rolled back instead of left active, so a caller keeps its
  own transaction and the rows the importer abandoned are no longer committed by the caller's next
  `commit()`;
- `getVertexCount()` and the per-type `TypeState` report the durable prefix instead of zero;
- a new `WARNING` line is logged when the failed source had already committed rows.

The success path is unchanged: `committed[0] == count[0]` after the final commit, so `ts.count` and
`totalVertices` carry the same values as before.

## PR and review cycles

PR: https://github.com/ArcadeData/arcadedb/pull/7273

- **cycle 1** - `0f4c9a30` - the fix, the three tests and this doc. Review: no blocking findings; one
  request for a one-line confirmation that skipping the `WARNING` when nothing had been committed is
  deliberate, and a note that `ts.buckets`/`ts.positions` outlive the committed count on the failure
  path. Both answered with comments at the two lines in question (`dcbe91ee`).
- **cycle 2** - `dcbe91ee` - review found a real off-by-one: the `WARNING` described
  `count[0] - committed[0]` as "rows read since the last commit", but `count[0]` is incremented only
  after a row becomes a vertex, so the failing row was not in it. The message now speaks in the terms
  the counters actually hold. The same review flagged, as non-blocking residual risk, that the
  rollback in the `finally` could replace the propagating exception; acted on rather than deferred,
  because a throw there would also skip the counter assignment and re-create this issue's own
  symptom. Both fixed in `31515737`.
- **cycle 3** - `31515737` - clean. Its two observations (a comment on the `committed[0] > 0` guard,
  a comment at `ts.buckets = bk.trim()`) were already in the tree from cycle 1; the review appears to
  have read them only in the cycle-1 commit message. No changes applied, no deferred items.

Final state: **clean-approval** after 3 of the 4 allowed cycles.

Merge is the developer's; this branch was never merged or closed by the workflow.

## Recommendations

- Resolve #7272 so the other four importer row loops behave the same way.
- A resumable import (skip the first N rows of a source) would turn the partial state this fix now
  reports into something an operator can act on directly. Out of scope here.
