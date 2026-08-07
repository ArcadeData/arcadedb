# Issue #5800: Node-valued property is dereferenceable in the writing query but becomes a non-dereferenceable `DatabaseRID` after commit

## Summary

`SET holder.ref = target` (where `target` is a node) succeeds, and the assigned value is still
the live `Vertex` during the writing query, so `holder.ref.id` correctly resolves against it.
ArcadeDB persists the assigned node as a LINK/RID (`DatabaseRID`), which is the intended,
documented storage representation for a NODE-valued Cypher property. In a subsequent
transaction, `MATCH (holder:T) RETURN holder.ref.id` raised:

```
TypeError: Cannot access property 'id' on DatabaseRID value
```

surfaced as an HTTP 500 by the HTTP command API, breaking the transaction-boundary
independence of the same property-access expression.

## Root cause

The OpenCypher expression parser builds a chained property-access AST node,
`CypherExpressionBuilder.ChainedPropertyAccessExpression`, whenever a `.` postfix is applied
to something other than a plain variable (e.g. the outer `.id` in `holder.ref.id`, chained onto
the inner `holder.ref` property access). Its `evaluate()` method handled `Document`, `Map`,
`Result`, and several temporal types as the base value, but not `RID` (or its `DatabaseRID`
subclass).

The sibling AST node used for single-level, variable-bound property access,
`com.arcadedb.query.opencypher.ast.PropertyAccessExpression`, already dereferences a `RID` base
value via `rid.asVertex().get(propertyName)` (used today for lazy vertex resolution in
algorithm procedures). `ChainedPropertyAccessExpression` never got the equivalent branch, so
once `holder.ref` resolved (during evaluation of the inner `PropertyAccessExpression`) to a
persisted `DatabaseRID` instead of a live `Vertex`, the outer chained `.id` access fell through
to the generic "not a property-bearing type" `CommandExecutionException`.

## Fix

Added the missing `RID` branch to `ChainedPropertyAccessExpression.evaluate()` in
`engine/src/main/java/com/arcadedb/query/opencypher/parser/CypherExpressionBuilder.java`,
mirroring the existing behavior in `PropertyAccessExpression`: a `RID` base value is
transparently dereferenced via `rid.asVertex().get(propertyName)`. This keeps ArcadeDB's
intentional NODE-to-LINK/RID conversion on write, while making the persisted value
dereferenceable through the same expression shape across the transaction boundary - matching
the second option the issue proposed ("preserve stable dereference semantics across
transaction boundaries").

Following review feedback, the dereferenced value also goes through the same temporal-type
restoration (`Duration`/`LocalTime`/`Time` strings back to their `Cypher*` wrapper types) that
`PropertyAccessExpression` already applied - otherwise a temporal property on the dereferenced
target would come back as a raw ISO-8601 `String` on the chained path while still resolving to
a proper `CypherDuration`/etc. on the single-level path. That conversion logic
(`convertFromStorage`) was extracted out of `PropertyAccessExpression` into a new
`TemporalUtil.convertFromStorage()` shared utility (both AST nodes live in different packages,
and the method was `private`), so both access paths call the same code instead of duplicating
~80 lines of string-sniffing logic a third time.

No storage format or write-path change was needed or made; this is a read-path fix only.

## Tests

New regression test:
`engine/src/test/java/com/arcadedb/query/opencypher/CypherNodeValuedPropertyDereferenceIssue5800Test.java`

- `chainedPropertyAccessOnPersistedLinkDereferencesAfterCommit` - reproduces the exact issue
  scenario: writes `holder.ref = target` and confirms `holder.ref.id` still works in the writing
  query, then opens a new transaction and confirms `holder.ref.id` resolves to `42` instead of
  throwing.
- `chainedPropertyAccessOnPersistedLinkRestoresTemporalType` - added during review: confirms a
  `Duration`-typed property on the dereferenced target restores to a `CypherDuration` (so
  `.seconds` resolves) rather than staying a raw `String`, pinning the `TemporalUtil` extraction
  above. Verified this test fails with the exact predicted `TypeError: Cannot access property
  'seconds' on String value` when the `convertFromStorage` call is temporarily removed, and
  passes with it restored.
- `directPropertyAccessOnPersistedLinkStillDereferences` - control test pinning down that the
  single-level, variable-bound property access path (`WITH holder.ref AS r RETURN r.id`), which
  already handled `RID` before this fix, keeps working.

All new tests failed before their respective fix landed (the first with the exact reported
`TypeError`, matching the stack trace through `ChainedPropertyAccessExpression.evaluate`; the
temporal one with the predicted `TypeError` on the raw `String`) and pass after it.

## Verification

- Reproduced the bug manually against the pre-fix `engine` jar with a standalone driver script
  replaying the issue's exact Cypher statements; observed the same `TypeError` reported in the
  issue.
- `mvn -pl engine test -Dtest=CypherNodeValuedPropertyDereferenceIssue5800Test`: failed before
  the fix (2 errors, matching the exact reported `TypeError` and stack trace through
  `ChainedPropertyAccessExpression.evaluate`), passed after it (2/2).
- Re-ran the standalone driver script against the fixed `engine` jar: `holder.ref` still reads
  back as a `DatabaseRID` (`#1:1`), and `holder.ref.id` now resolves to `42` instead of raising
  `TypeError`.
- Targeted regression sweep across property/SET-related OpenCypher unit tests (inline property
  filters/normalization/pattern binding, dynamic property mutation, `SET`, subquery parsing,
  edge mandatory property, parameterized property maps, existing property-access type-error
  test #5285, etc.): all passed.
- Broad regression sweep across `com.arcadedb.query.opencypher.{ast,executor,parser,procedures,
  rewriter,optimizer,functions}` plus all top-level `com.arcadedb.query.opencypher` test classes
  (excluding the `benchmark`/`slow`-tagged and TCK-suite/benchmark tests, per repository
  convention): 7629 tests, 0 failures, 0 errors - both before and after the `TemporalUtil`
  extraction added during review.

## Impact analysis

This is a narrowly-scoped read-path fix inside one AST node's `evaluate()` method. It only
changes behavior for the specific case that previously threw an exception (`RID` base value
reaching `ChainedPropertyAccessExpression`); all previously-handled base types (`Document`,
`Map`, `Result`, temporal types) are unaffected. `rid.asVertex()` follows the same lazy-loading
pattern already used by `PropertyAccessExpression`, so there is no new performance concern.

## Recommendations

- If ArcadeDB ever wants Neo4j-identical semantics (rejecting a NODE-valued property assignment
  outright, rather than converting it to a LINK/RID), that would be a separate, larger behavior
  change requiring its own design discussion - out of scope for this fix, which preserves
  ArcadeDB's existing intentional LINK-property behavior and only fixes its read-path
  inconsistency.
- Dereferencing a RID whose target no longer resolves to a live `Vertex` (record deleted, or a
  LINK pointing at a `Document`/`Edge`) raises the raw `RecordNotFoundException`/
  `ClassCastException` from `asVertex()` rather than a Cypher-flavored `TypeError`. This is
  pre-existing behavior inherited unchanged from `PropertyAccessExpression`, but this fix makes
  it reachable from the far more common chained-access path too. Tracked as
  [#5898](https://github.com/ArcadeData/arcadedb/issues/5898).
- `OrderByStep.java` has its own separate, near-duplicate `convertFromStorage` implementation
  that was not consolidated into `TemporalUtil.convertFromStorage()` here, since it's pre-existing
  and out of scope for this bug fix - worth a small follow-up cleanup.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5893

## Review cycles

Automated review loop via `resolve-issue-with-review`, gated on the `claude` GitHub Actions
reviewer, up to 4 cycles.

1. **`ba99a6e`** (initial fix + test) - Claude review requested one change before merge: the new
   `RID` branch didn't apply `convertFromStorage`'s temporal-type restoration, unlike the sibling
   `PropertyAccessExpression` branch it claimed to mirror; a temporal-typed dereferenced property
   would silently come back as a raw `String`. Applied in the next commit.
2. **`5f4d382`** (extract `TemporalUtil.convertFromStorage`, add temporal-parity test) - Claude
   review: positive, with three non-blocking observations (a stale Javadoc reference the refactor
   left behind; a separate pre-existing duplicate in `OrderByStep`; the pre-existing
   `asVertex()` unfriendly-error gap, now reachable via a second path). Applied the Javadoc fix;
   recorded the other two as deferred/skip items with rationale.
3. **`6fad3df9`** (Javadoc fix + deferred-items note) - Claude review: still positive, but
   reiterated the `asVertex()` unfriendly-error gap more strongly ("the one thing I'd actually
   want before/after merge is a tracked follow-up issue") and separately noted the
   `review-deferred-*.md` file reads as PR-discussion audit trail rather than root-cause
   documentation and would go stale after merge. Filed
   [#5898](https://github.com/ArcadeData/arcadedb/issues/5898) to track the `asVertex()` gap,
   posted the two deferred observations as a PR comment instead, and removed the
   `docs/review-deferred-*.md` file.
4. **`9712c69`** (file #5898, move review notes to a PR comment, update this doc) - Claude
   review: clean approval - "Nothing missing that should block merge... No blocking issues
   found."

## Deferred items

None outstanding as unattended-decision items. Two review observations were explicitly deferred
to the developer's judgment rather than actioned in this PR (see "Recommendations" above and the
cycle-2/3 history): the `OrderByStep` duplicate-logic cleanup, and #5898 (filed, not fixed, per
the reviewer's own framing of it as a separate concern). The `docs/review-deferred-5f4d382.md`
audit-trail file that existed between cycles 2 and 3 was removed in cycle 3; its content is
preserved in a PR comment instead.

## Final state

`clean-approval` after 4 review cycles.
