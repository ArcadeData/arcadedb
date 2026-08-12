# Issue #6073: Write CypherProcedures aren't auto-committed like SET/CREATE/DELETE

## Root cause

`CallStep.executeProcedure` (`engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/CallStep.java`)
was the only write path in the openCypher engine that did not auto-commit when no transaction was already
open. Per `OpenCypherQueryEngine`'s class Javadoc, `SetStep`, `DeleteStep`, `MergeStep`, `RemoveStep`,
`ForeachStep`, and a writing `CALL` subquery all `begin()`/`commit()` on `context.getDatabase()` when no
transaction is active. `CallStep.executeProcedure` had no such wrapping, so a top-level `CALL` to a write
`CypherProcedure` (`isWriteProcedure() == true` - `merge.node`, `merge.relationship`,
`apoc.refactor.mergeNodes`, `apoc.refactor.cloneNodesWithRelationships`, `apoc.do.when`) with no caller-managed
transaction failed with `TransactionException: Transaction not active` on the first `ResultSet.next()` pull.

Confirmed empirically (before the fix) that all four affected procedures throw
`TransactionException: Transaction not begun` / `Transaction not active` under the exact reproduction from the
issue body - see "Review cycles" below for the specific stack traces captured while the regression test ran
against the pre-fix code.

## Affected components

- `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/CallStep.java` - the only site that needed
  a change; every `CypherProcedure` implementation (`MergeNode`, `MergeRelationship`, `RefactorMergeNodes`,
  `RefactorCloneNodesWithRelationships`, `DoWhen`) already performs its mutation synchronously inside
  `execute()`, before returning its `Stream<Result>`, so wrapping the single call site was sufficient.

## Expected vs actual behavior

- **Before:** `CALL merge.node(...)` (or any write procedure) as a standalone top-level statement with no
  explicit `database.begin()`/`commit()` around it threw `TransactionException`.
- **After:** the same call auto-commits, matching `SET`/`CREATE`/`DELETE`/`MERGE`/`REMOVE`/`FOREACH`. Calling a
  write procedure inside an explicit caller-managed transaction (the documented workaround, and what
  `RefactorMergeNodesTest` already exercises) is unaffected - auto-commit only fires when no transaction is
  already open.

## Fix

`CallStep.executeProcedure` now computes `autoCommit = procedure.isWriteProcedure() &&
!context.getDatabase().isTransactionActive()` and wraps the `procedure.execute(...)` call with
`begin()`/`commit()` when `autoCommit` is true, with `rollback()` on both the `CommandParsingException` (client
error) and general exception paths - mirroring `SetStep.applySetOperations`'s `wasInTransaction` pattern exactly
(the reference precedent named in the issue's suggested fix).

## Tests

New test class: `engine/src/test/java/com/arcadedb/query/opencypher/executor/steps/CallStepWriteProcedureAutoCommitTest.java`
(7 tests):

- `mergeNodeAutoCommitsWithNoExplicitTransaction`
- `mergeRelationshipAutoCommitsWithNoExplicitTransaction`
- `refactorMergeNodesAutoCommitsWithNoExplicitTransaction` (the issue's exact reproduction)
- `writeProcedureInsideExplicitTransactionIsUnaffected` (regression guard for the documented workaround)
- `chainedCallAutoCommitsEachRowWithNoExplicitTransaction` (UNWIND + per-row CALL, no explicit transaction)
- `failedWriteProcedureCallDoesNotLeaveTransactionOpen` (rollback path leaves no dangling transaction)
- `unknownPropertiesPolicyStillThrowsWithNoExplicitTransaction`

Verified TDD-style: ran the new test class against the pre-fix `CallStep.java` (via `git stash`) first - 4 of 7
failed with the exact `TransactionException` from the issue body, proving the tests catch the bug. Restored the
fix; all 7 passed.

## Test results

- New test class: 7/7 pass.
- `engine` module, `com.arcadedb.query.opencypher.procedures.**` (all procedure tests, read-only and write):
  589/589 pass, 0 failures/errors.
- `engine` module, `com.arcadedb.query.opencypher.executor.**` (full executor-step package, includes CallStep,
  SetStep, MergeStep, DeleteStep and all other Cypher execution steps): 718/718 pass, 0 failures/errors.
- `mvn -pl engine -am compile` passes cleanly.

## Impact analysis

Behavior change is scoped to the 5 procedures where `isWriteProcedure()` returns `true`. Read-only procedures
(all `algo.*`, `path.*`, `meta.*`, `db.*`) take `autoCommit = false` unconditionally and are unaffected. A
chained CALL (e.g. `UNWIND ... CALL merge.node(...)`) auto-commits once per input row when no explicit
transaction wraps the whole statement, matching `SetStep`'s existing per-row auto-commit behavior for bulk
`UNWIND ... SET` - this is a deliberate consistency choice, not a new pattern.

## Recommendations for monitoring / future improvements

None identified beyond what's already tracked: the wider APOC procedure surface (`apoc.periodic.*`,
`apoc.trigger.*`, etc.) is out of scope for this fix and remains tracked separately as future work, unrelated
to the auto-commit gap fixed here.
