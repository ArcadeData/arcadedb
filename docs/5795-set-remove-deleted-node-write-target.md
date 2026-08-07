# Issue #5795: SET/REMOVE on a node deleted earlier in the same query silently succeed

## Root cause

`DeleteStep` replaces a deleted node/relationship's value in the result row with a
`DeletedEntityMarker` (see `com.arcadedb.query.opencypher.executor.DeletedEntityMarker`).
`PropertyAccessExpression.evaluate()` calls `DeletedEntityMarker.checkNotDeleted(variable)`
before dereferencing, so any later property read (`RETURN t.v`, `SET t.v = t.v + 1`) correctly
throws `CommandExecutionException("DeletedEntityAccess: ...")` and the whole statement rolls
back.

The write-target paths never had that check:

- `SetStep.resolveLatestDoc()` (used by `SET t.v = value`, `SET t = {...}`, `SET t += {...}`)
  only checked `instanceof Document`; a `DeletedEntityMarker` fails that check and the method
  returns `null`, so the caller (`applyPropertySet` / `applyReplaceMap` / `applyMergeMap`) treats
  it as "variable not found" and no-ops instead of failing.
- `SetStep.applyLabels()` (used by `SET t:Label`) only checked `instanceof Vertex`, same no-op.
- `RemoveStep.removeProperty()` (used by `REMOVE t.v`) only checked `obj == null` /
  `instanceof Document`.
- `RemoveStep.removeLabels()` (used by `REMOVE t:Label`) only checked `instanceof Vertex`.

Because each of `DeleteStep`/`SetStep`/`RemoveStep` only begins/commits its own mini-transaction
when it is not already inside one (see `wasInTransaction` in each step, and
`DatabaseAbstractHandler` wrapping the whole command in one transaction for the HTTP command
API), a silent no-op in the SET/REMOVE step let the whole statement report success - committing
the DELETE - while the write itself vanished. A thrown exception, by contrast, propagates out of
the step and rolls back the entire statement's transaction, exactly like the property-read path.

## Fix

Added a `DeletedEntityMarker.checkNotDeleted(...)` call to every write-target resolution path
that was missing it, matching the check already used by property reads:

- `SetStep.resolveLatestDoc()` - covers `SET t.v = value`, `SET t = {...}`, `SET t += {...}`
  (both the pre-clause reload in phase 1 and the phase-2 apply methods share this helper).
- `SetStep.applyLabels()` - covers `SET t:Label`.
- `RemoveStep.removeProperty()` - covers `REMOVE t.v`.
- `RemoveStep.removeLabels()` - covers `REMOVE t:Label`.
- `SetStep.applyPropertySet()` expression-target branch - covers
  `SET (CASE WHEN ... THEN t END).prop = value` and non-variable bracket-syntax bases. This
  branch resolves its target via a separate `evaluator.evaluate(item.getTargetExpression(), ...)`
  call that never goes through `resolveLatestDoc()`, so it needed its own check. **Found in code
  review** (PR #5913, cycle 1) - not part of the original fix.

No behavior changed for live (non-deleted) targets: `checkNotDeleted` is a no-op unless the value
is actually a `DeletedEntityMarker`.

## Files changed

- `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/SetStep.java`
- `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/RemoveStep.java`
- `engine/src/test/java/com/arcadedb/query/opencypher/CypherDeletedNodeWriteTargetIssue5795Test.java` (new)

## Tests

New regression test `CypherDeletedNodeWriteTargetIssue5795Test` (11 cases): covers all six affected
forms from the issue (`SET t.v=99`, `SET t={...}`, `SET t+={...}`, `SET t:Label`, `REMOVE t.v`,
`REMOVE t:Label`), the no-`WITH`-boundary variant (`DELETE t SET t.v=99`), the CASE-subclause
expression-target form (`SET (CASE WHEN ... THEN t END).v = 99`, added during code review), a
deleted-relationship SET target (added during code review), plus two control cases that must
remain unchanged: property-read-after-delete (already failed, must keep failing) and SET on a
live node (must keep succeeding).

**TDD verification**: ran the original 9-case suite with the fix stashed out (`git stash`) - 7
failed with `AssertionError: Expecting code to raise a throwable` (the 2 control cases still
passed, as expected). Separately, ran the CASE-target test with just that one `checkNotDeleted`
call removed - it failed the same way. Restored both fixes and reran the full 11-case suite - all
pass.

Regression scope run after the fix (all green, no failures):

- `CypherDeletedNodeWriteTargetIssue5795Test` - 11/11
- `OpenCypherDeleteTest` - 9/9
- `OpenCypherSetTest` - 22/22
- `OpenCypherRemoveTest` - 9/9
- `OpenCypherRemoveRelationshipTest` - 2/2
- `CypherSequentialDeleteSetIssue5097Test` - 3/3
- `CypherSetSnapshotIssue5190Test` - 3/3

`engine` module compiles cleanly (`mvn -pl engine -am compile`).

## Review cycles

- **Cycle 1** (head `d0341a6d5`): Claude review found one gap (`SetStep.applyPropertySet()`
  expression-target branch bypassed the check) and one coverage gap (no deleted-relationship SET
  test); both applied. See `docs/review-deferred-d0341a6d5.md` for the full categorization,
  including the one item deliberately left out of scope (`MergeStep`, already flagged in this
  doc's Recommendations section below).

## Impact analysis

Behavioral change only for the invalid use-after-delete case: statements like
`DELETE t WITH t SET t.v = 99` now raise `CommandExecutionException` and roll back (including the
DELETE) instead of silently committing the deletion and discarding the write. This matches the
already-existing behavior of the property-read and RHS-evaluation paths, removing the asymmetry
reported in the issue. No change to any statement that does not reference a node deleted earlier
in the same query.

## Recommendations

- Consider auditing `MergeStep` for the same class of "write target resolved from result row"
  pattern if a similar deleted-node-reuse report surfaces for `MERGE`; it was not in scope for
  this issue (the issue's repro cases are all DELETE + SET/REMOVE) and was left untouched.
