# Issue #6367: Small self-loop CREATE/MERGE query returns DeadlockDetected

## Verification against the reported version

The literal query from the issue was checked out at three points:

- **ArcadeDB `26.8.1`** (the version reported): reproduces exactly as described. An end-to-end Bolt
  test (real neo4j Java driver, `session.run(query).consume()`) against a build of that tag fails
  with `org.neo4j.driver.exceptions.TransientException` / `Neo.TransientError.Transaction.DeadlockDetected`.
- **This branch (based on `main`, 26.9.1-SNAPSHOT)**: the same test, byte-for-byte, passes,
  consistently across 6 repeated runs. An OpenCypher planner/executor rewrite landed between
  26.8.1 and `main` (a new cost-based optimizer, `AnchorSelector`, rewritten `MatchNodeStep`/
  `MatchRelationshipStep`, etc, 146 files and roughly 6800 lines changed in
  `engine/.../query/opencypher/` alone) and already changed how this self-loop pattern is planned
  and committed, so the exact reported symptom does not reproduce on `main`.
- The reduced, single-clause version tried directly against the engine (bypassing Bolt entirely)
  also does not reproduce on `main`.

Per project practice ("verify an issue is unfixed before resolving it"), the literal repro is not
something to chase further, but the investigation surfaced a real, still-live defect in the same
neighborhood, described below.

## Root cause analysis

An autocommit Cypher query containing a `CREATE` clause followed by a `MERGE` clause runs each
write clause as its own auto-commit mini-transaction when no outer transaction is already active
(see the Javadoc on `OpenCypherQueryEngine.executionDatabase`).

- `CreateStep.createPatterns()` / `createPatternsBatch()` wrap their work in
  `database.transaction(..., true)`. That both joins an already-active transaction (HTTP's
  auto-commit wrapper, or an explicit `BEGIN`) and, when it owns the transaction itself, retries
  automatically on `NeedRetryException`/`ConcurrentModificationException` up to
  `GlobalConfiguration.TX_RETRIES` attempts.
- `MergeStep.executeMerge()` instead called `context.getDatabase().begin()` / `.commit()`
  directly, with no retry. Any MVCC conflict raised while it owned its own mini-transaction
  propagated immediately. The same raw-begin/commit-with-no-retry pattern is also present in
  `SetStep`, `DeleteStep`, `RemoveStep` and `ForeachStep`, so the same gap exists there; only
  `MergeStep` is touched by this PR since it is the one directly implicated by #6367, and fixing
  it is enough to demonstrate and regression-test the mechanism. The sibling steps are a natural,
  narrowly-scoped follow-up.

Over HTTP, `DatabaseAbstractHandler.executeInTransaction()` wraps the entire command dispatch in
one outer `database.transaction(..., false, retries)` call. `CreateStep` and `MergeStep` both then
join that single outer transaction (`wasInTransaction`/`joinCurrentTx` is true), so neither commits
on its own. One atomic commit covers the whole query, and if the outer transaction conflicts, the
whole HTTP request (the whole query, replanned from scratch) is retried up to `TX_RETRIES` times.
This is what masks the missing retry inside `MergeStep` when the request arrives over HTTP.

Bolt's `BoltNetworkExecutor.handleRun()` calls `database.command(...)` directly for an autocommit
`RUN` message, with no outer wrapper. So `CREATE` commits (and retries) as its own mini-transaction,
then `MergeStep` begins a second, unretried mini-transaction. Any MVCC conflict raised while that
second mini-transaction commits (self-inflicted, from the same query, or from real concurrent
activity) has nothing to retry it: the `NeedRetryException` propagates straight out of `handleRun`,
gets mapped by `BoltErrorCodes.TRANSIENT_CONFLICT_ERROR`, and reaches the driver as
`Neo.TransientError.Transaction.DeadlockDetected`, exactly the symptom #6367 reports, and exactly
what reproduced on 26.8.1.

This gap is real and demonstrable independent of the exact self-loop shape in the issue: an
engine-level regression test (`Issue6367MergeStepAutoRetryTest`) drives ordinary concurrent
`MERGE` find-or-create traffic against `main`, unwrapped, and reliably hits it.

## Fix

Bring `MergeStep` in line with `CreateStep`: wrap `executeMerge()`'s body in
`database.transaction(..., true)` instead of raw `begin()`/`commit()`. This gives `MergeStep`:

- The same join-vs-own-transaction semantics as `CreateStep` (no behavior change when an outer
  transaction, an explicit `BEGIN` or HTTP's auto-commit wrapper, is already active).
- Automatic retry, up to `TX_RETRIES`, when `MergeStep` owns its own mini-transaction (Bolt
  autocommit, or any other unwrapped caller).

Query statistics are restored to the pre-attempt snapshot on each retry (mirroring `CreateStep`),
so a retried attempt does not double-count created/matched nodes, relationships or properties.

## Testing

- `engine/src/test/java/com/arcadedb/query/opencypher/Issue6367MergeStepAutoRetryTest.java`,
  engine-level regression test. Drives 8 threads x 25 iterations of an unwrapped, autocommit
  `database.command("opencypher", "MERGE (a:Cnt6367 {id:1}) RETURN a")` find-or-create against the
  same uniquely-indexed node, exactly the shape `BoltNetworkExecutor.handleRun` uses. Confirmed
  RED before the fix (`MergeStep`'s raw `begin()`/`commit()` surfaces `ConcurrentModificationException`
  under contention) and GREEN after: no conflict escapes, every call succeeds, and MERGE's
  match-or-create semantics still hold (exactly one vertex, never a duplicate).
- `bolt/src/test/java/com/arcadedb/bolt/Bolt6367SelfLoopMergeIT.java`, end-to-end regression test
  using a real neo4j Bolt driver session against the literal query from the issue. Locks in that
  the reported query keeps completing successfully (it already does on `main`, verified not to on
  `26.8.1`).
- Full `com.arcadedb.query.opencypher.**` suite (11492 tests) run clean with the fix in place.
- Full `arcadedb-bolt` unit suite (319 tests) plus `Bolt4908TransientConflictIT` (a genuine,
  deliberately-conflicting explicit-transaction pair across two Bolt sessions, unaffected by this
  change) run clean with the fix in place.
