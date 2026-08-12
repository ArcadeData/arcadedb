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

Test class: `engine/src/test/java/com/arcadedb/query/opencypher/executor/steps/CallStepWriteProcedureAutoCommitTest.java`
(10 tests):

- `mergeNodeAutoCommitsWithNoExplicitTransaction`
- `mergeRelationshipAutoCommitsWithNoExplicitTransaction`
- `refactorMergeNodesAutoCommitsWithNoExplicitTransaction` (the issue's exact reproduction)
- `writeProcedureInsideExplicitTransactionIsUnaffected` (regression guard for the documented workaround)
- `chainedCallAutoCommitsEachRowWithNoExplicitTransaction` (UNWIND + per-row CALL, no explicit transaction)
- `refactorCloneNodesWithRelationshipsAutoCommitsWithNoExplicitTransaction`
- `doWhenAutoCommitsWriteSubQueryWithNoExplicitTransaction` (do.when's nested `database.command()` dispatch,
  a materially different code path from the other 4 direct-mutation procedures)
- `failedWriteProcedureCallDoesNotLeaveTransactionOpen` (rollback path leaves no dangling transaction)
- `optionalCallToFailingWriteProcedureSuppressesErrorAndLeavesNoDanglingTransaction` (OPTIONAL CALL combines
  rollback with error suppression rather than rethrow; per OPTIONAL semantics the failed call still yields one
  row with every YIELD field null, not zero rows - confirmed empirically, not assumed)
- `unknownPropertiesPolicyStillThrowsWithNoExplicitTransaction`

Verified TDD-style: ran the new test class against the pre-fix `CallStep.java` (via `git stash`) first - 4 of 7
tests existing at that point failed with the exact `TransactionException` from the issue body, proving the
tests catch the bug. Restored the fix; all passed.

## Test results (final, after all review cycles)

- New test class: 10/10 pass.
- `engine` module, `com.arcadedb.query.opencypher.procedures.**` (all procedure tests, read-only and write):
  589/589 pass, 0 failures/errors.
- `engine` module, `com.arcadedb.query.opencypher.executor.**` (full executor-step package, includes CallStep,
  SetStep, MergeStep, DeleteStep and all other Cypher execution steps): 720/720 pass, 0 failures/errors.
- `mvn -pl engine -am compile` passes cleanly.

## Impact analysis

Behavior change is scoped to the 5 procedures where `isWriteProcedure()` returns `true`. Read-only procedures
(all `algo.*`, `path.*`, `meta.*`, `db.*`) take `autoCommit = false` unconditionally and are unaffected.

**Bulk writes via `UNWIND ... CALL <write-procedure>`:** a chained CALL auto-commits once per input row when no
explicit transaction wraps the whole statement, matching `SetStep`'s existing per-row auto-commit behavior for
bulk `UNWIND ... SET` - a deliberate consistency choice, not a new pattern. This means N rows pay N
begin/commit (WAL flush) cycles rather than one batched commit. Callers doing a large bulk merge/write via a
write procedure should wrap the whole `UNWIND` in an explicit `database.begin()`/`commit()` to get one commit
for the batch, exactly as they already should for bulk `UNWIND ... SET`.

**`apoc.do.when` always pays for a transaction, even on a pure-read branch:** `DoWhen.isWriteProcedure()`
returns `true` unconditionally (it can't know at registration time whether the caller-supplied `ifQuery`/
`elseQuery` will actually write), so every top-level `apoc.do.when(...)` call now begins+commits even when the
selected branch never mutates anything. Not a correctness issue, just an overhead every `do.when` call now pays
that it didn't before this fix (previously nothing wrapped a top-level `do.when` CALL at all). Consciously not
addressed here - see the HA caveat below, which this compounds with.

## HA caveat - see issue #6094

A bare top-level `CALL` to a write procedure (no CREATE/SET/MERGE/DELETE/REMOVE/FOREACH clause anywhere in the
statement) is misclassified `readOnly = true` by `SimpleCypherStatement` - pre-existing, but harmless before
this fix (a write procedure with no transaction always threw before anything could commit). This fix's
auto-commit is the first thing that actually calls `begin()`/`commit()` for a `CALL`, which makes the
misclassification live: on an HA leader, `OpenCypherQueryEngine.executionDatabase()` hands that auto-commit the
raw (non-Raft-wrapped) database instance instead of the wrapped one, so the write can commit locally without
being proposed to Raft; on a follower, the write executes entirely locally instead of forwarding to the leader.
A silent replication gap, not a loud failure - worse for HA deployments than the `TransactionException` this PR
fixes.

A mechanical classification fix was attempted (`SimpleCypherStatement.anyWriteProcedureCall`, checking
`CypherProcedureRegistry.get(name).isWriteProcedure()` the same way `anyWriteSubquery` already does for
`CALL { ... }` blocks) and reverted: it breaks 10 of `DoWhenTest`'s 11 tests, because `do.when`'s unconditional
`isWriteProcedure()==true` means every `CALL apoc.do.when(...)` gets reclassified as non-idempotent, including
existing tests that correctly-looking call it via `database.query()` for branches that happen to be read-only
- those tests were unknowingly relying on today's misclassification bug for `query()`'s idempotency gate to
pass. Per review feedback, the HA blast radius is broader than the 5 write procedures alone: it also covers
every `apoc.do.when` call, including pure-read branches, since the procedure is unconditionally classified as a
potential write.

Filed as [#6094](https://github.com/ArcadeData/arcadedb/issues/6094) with full analysis (leader/follower
failure modes, the `DoWhenTest` tension, suggested resolution paths, required new HA-routing test coverage).
**Recommend #6094 lands before or alongside this PR on any branch/release used in an HA deployment**; on a
non-HA (single-node) deployment this fix is safe to merge as-is. Flagged prominently in the PR description per
review feedback, rather than left as a footnote.

## Review cycles (PR #6093)

- **Cycle 1** (`77013dbb6`): flagged (1) missing test coverage for `apoc.refactor.cloneNodesWithRelationships`
  and `apoc.do.when`, (2) a latent correctness risk - `commit()` ran immediately after building the result
  iterator, not after draining it, which only works because every current write procedure happens to
  materialize its `Stream` eagerly; a future lazily-streaming write procedure could silently regress. Both
  applied: added the 2 missing tests, forced eager materialization on the auto-commit path plus documented the
  "must materialize eagerly" contract on `Procedure#execute`. Two nitpicks explicitly marked non-blocking by
  the reviewer (do.when's unconditional write cost, per-row auto-commit) were skipped with rationale.
- **Cycle 2** (`66bc3a7d8`): raised the HA-routing misclassification bug as a Must-fix (see above), and noted
  the review-cycle notes file didn't belong in the committed tree. Attempted the classification fix, reverted
  it after confirming it breaks 10/11 `DoWhenTest` tests; filed #6094 with the full analysis instead. Removed
  the notes file per feedback.
- **Cycle 3** (`50e0a0a8c`): re-raised the same HA finding with a stronger recommendation to surface it in the
  PR description rather than only in the linked issue, plus 3 minor polish items (javadoc accuracy,
  try-with-resources consistency, PR description test count). All applied; PR description updated with a
  prominent HA caveat section.
- **Cycle 4** (`581c74ade`, final - `max-cycles=4` reached): reiterated that the HA caveat should gate merge
  sequencing rather than be a footnote (a process/maintainer-sign-off recommendation, not a code change - see
  Final state below), clarified the HA blast radius also covers `do.when`'s read branches, flagged one untested
  branch combination (`OPTIONAL CALL` to a failing write procedure), and reconfirmed the eager-materialization
  fix and rollback guards as correct. Added the missing `OPTIONAL CALL` test (discovered and fixed a wrong
  assumption in the process: OPTIONAL yields one null-field row, not zero rows) and broadened this doc's HA
  caveat section to cover `do.when`'s read branches.

## Final state: max-cycles-reached

All 4 review cycles produced actionable feedback that was either applied or, for the HA-routing bug, verified
and deferred to #6094 with a documented reason it can't be safely resolved within this loop (breaks pre-existing
tests this loop is not permitted to modify; needs a `do.when` classification design decision). Every code/test
review comment across all 4 cycles was applied. The one item still open is not a code change this loop can
make: **whether to sequence this PR's merge relative to #6094 on branches used for HA deployments** is a
maintainer decision, explicitly requested by the reviewer in cycle 4. Flagged prominently in the PR description
and this doc for the developer.

## Recommendations for monitoring / future improvements

- Resolve #6094 (HA-routing misclassification) before or alongside merging this PR on any HA-deployed branch.
- The wider APOC procedure surface (`apoc.periodic.*`, `apoc.trigger.*`, etc.) is out of scope for this fix and
  remains tracked separately as future work, unrelated to the auto-commit gap fixed here.
