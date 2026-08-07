# Deferred/skipped review items - PR #5893 (issue #5800), cycle 2 review of `5f4d382`

The Claude review on commit `5f4d382` (issue comment id `5216026925`) was overall positive
("a targeted, well-tested fix with clear root-cause analysis... all non-blocking
suggestions/follow-ups rather than blockers"). One item was applied directly (see the commit on
top of this note: the stale `PropertyAccessExpression.convertFromStorage()` Javadoc reference in
`TemporalUtil.toCoreJavaType`, which this PR's own refactor made stale).

The following two items were left for the developer to decide on, per the skill's "skip
nitpick/optional, record rationale" rule - neither blocks this PR:

## 1. Duplicate `convertFromStorage` logic in `OrderByStep`

> `executor/steps/OrderByStep.java` has its own private `convertFromStorage` (lines ~259+) that
> looks like a near-duplicate of the logic you just consolidated. Not introduced by this PR and
> out of scope, but worth a follow-up to point it at `TemporalUtil.convertFromStorage` too while
> you're in the neighborhood.

Rationale for not addressing here: the reviewer explicitly calls this pre-existing and out of
scope for this bug fix. Consolidating it would touch a third, unrelated file/execution path
(`ORDER BY` sorting) for no behavioral fix to issue #5800, and risks scope creep in a PR that is
otherwise a minimal, well-isolated change. Worth a small follow-up cleanup PR/issue.

## 2. `rid.asVertex()` unfriendly error on a deleted/non-vertex RID target

> If the persisted RID no longer resolves to a `Vertex` (record deleted, or the LINK actually
> points to a plain `Document`/`Edge`), `RID.asVertex()` throws `RecordNotFoundException` rather
> than the friendly `TypeError: Cannot access property ...`... might be worth filing that issue
> now rather than just noting it in the tracking doc, since HTTP callers would still see an
> unfriendly 500-style error for that case.

Rationale for not addressing here: this is explicitly called out as pre-existing behavior
inherited unchanged from `PropertyAccessExpression`'s identical, already-shipped pattern - not a
regression introduced by this PR. It is now reachable from a second code path
(`ChainedPropertyAccessExpression`) as a side effect of this fix, but fixing the underlying
"unfriendly error on dereferencing a broken RID" behavior is a separate, broader concern that
also affects the pre-existing single-level path and deserves its own design/triage rather than
a reactive patch bundled into this bug fix. Filing a tracking issue is left to the developer's
judgment (this automated run does not open new GitHub issues on the developer's behalf).

Both items are also recorded in `docs/5800-cypher-node-property-dereference.md` under
"Recommendations".
