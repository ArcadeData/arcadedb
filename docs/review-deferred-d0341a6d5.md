# Review notes for PR #5913, cycle 1 (head d0341a6d5)

Claude review: https://github.com/ArcadeData/arcadedb/pull/5913#issuecomment-5217229722

## Applied

- **Correctness (blocking): `SetStep.applyPropertySet()` expression-target branch bypassed the
  `DeletedEntityMarker` check.** Confirmed by static inspection (`resolveLatestDoc()` is only
  called from the plain-variable branch; the `item.getTargetExpression() != null` branch used for
  `SET (CASE WHEN ... THEN t END).prop = value` and non-variable bracket-syntax bases evaluates
  the target directly and never routes through the new check). Added
  `DeletedEntityMarker.checkNotDeleted(obj)` right after evaluating the target expression, added
  regression test `setCaseExpressionTargetOnDeletedNodeRaisesErrorAndRollsBackTheDelete`. Verified
  the new test fails without the fix (`Expecting code to raise a throwable`) and passes with it
  restored.
- **Minor: no test coverage for a deleted relationship as a SET target.** Added
  `setPropertyOnDeletedRelationshipRaisesErrorAndRollsBackTheDelete`, which deletes a relationship
  and then targets it with `SET r.v = 99`, confirming the same `DeletedEntityMarker` check applies
  to the shared `Document` supertype regardless of vertex vs relationship origin.

## Skipped (with justification)

- **Minor: `MergeStep` (`ON MATCH SET` / `ON CREATE SET`) has the same `instanceof Document/Vertex`
  pattern without a `DeletedEntityMarker` guard.** The reviewer confirmed this is accurate but
  explicitly flagged it as non-blocking, and it was already called out as an intentional
  follow-up in this PR's own "Recommendations" section (`docs/5795-...md`) and PR description,
  since it is not part of issue #5795's reported repro (all repro cases are DELETE + SET/REMOVE,
  not MERGE). Leaving it for a separate issue/PR to keep this change minimal and focused, per the
  project's TDD/minimal-fix guidance.
- **Style/docs notes**: no action needed - reviewer found no style or convention issues.
