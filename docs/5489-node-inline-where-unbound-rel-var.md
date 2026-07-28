# Issue #5489 - node inline `WHERE` in a pattern comprehension sees the same hop's relationship variable as unbound

- Issue: https://github.com/ArcadeData/arcadedb/issues/5489
- PR: https://github.com/ArcadeData/arcadedb/pull/5491
- Branch: `fix/5489-node-inline-where-unbound-rel-var`
- Base: `main` at 74f776db3
- Final state: `timeout` (see "Review cycles")

## Problem

A node inline `WHERE` predicate may reference the relationship variable bound by the same hop:

```cypher
MATCH (a:A {v:1}) RETURN [(a)-[r:E]->(x:A WHERE x.v > r.w) | x.v] AS vs
```

In a pattern comprehension this returned `[]`. The equivalent `MATCH` spelling returned the correct
row, so the two spellings disagreed.

## Root cause

`PatternComprehensionExpression` evaluates the end node's inline predicate against a row built by
`inlineWhereRow(endNodePattern, currentResult)`, which copies the bindings visible *before* the hop.
The hop's edge is only bound afterwards, when the hop result is assembled. So `r` resolved to null,
the comparison was false for every candidate, and the comprehension collected nothing.

Two traversal paths were affected:

- `traverseEdges` - the eval row was copied at the top of the expansion, while the relationship
  variable was bound further down, after `matchesEndPattern` had already run.
- `traverseVariableLength` - same ordering.

The `MATCH` spelling is unaffected because it hoists the inline predicate into the clause `WHERE`,
which runs once the whole pattern, `r` included, is bound.

This is the residual of the same family as #5460 (relationship inline `WHERE`) and #5480 (node
inline `WHERE` dropped by the comprehension parser). Here the predicate was applied, but one of its
inputs was silently unbound.

## Fix

`matchesEndPattern` now takes the hop's `RelationshipPattern` and `Edge` and publishes the
relationship binding onto the eval row before the predicate runs. The binding is written only when
the node pattern actually declares a predicate, so the allocation-free path for predicate-free
patterns is untouched, and the row is the one already hoisted out of the candidate loop, so no extra
copying is introduced.

A zero-length variable-length hop passes a null edge, leaving the relationship variable unbound,
which is what that pattern implies. For a variable-length hop the current edge is bound, matching
what `buildHopResult` already binds for the same pattern, so the predicate and the emitted row agree
on what `r` means.

The leading node's predicate is deliberately unchanged: at that position the relationship is bound
later in the pattern, so referencing it is out of scope by Cypher's left-to-right scoping.

## Files changed

- `engine/src/main/java/com/arcadedb/query/opencypher/ast/PatternComprehensionExpression.java`
- `engine/src/test/java/com/arcadedb/query/opencypher/Issue5489NodeInlineWhereRelVariableTest.java` (new)

## Verification

`Issue5489NodeInlineWhereRelVariableTest`, 11 methods (10 at the first push, plus the
anonymous-relationship edge case added in review): 5 failed before the fix and all pass after.
Coverage includes the reported shape, a predicate referencing only the relationship variable, a
predicate that must reject every candidate, the combined relationship-plus-node inline form,
explicit parity against the `MATCH` spelling, a variable-length hop, an earlier-hop relationship
reference (which already worked and is pinned as a control), an anonymous relationship, and
predicate-free controls.

Regression run over the whole `com.arcadedb.query.opencypher.**` package: 7618 tests, 0 failures.
The 3 errors are `OpenCypherCustomFunctionTest` GraalVM polyglot `NoClassDefFoundError`s; they were
reproduced identically on a clean tree at 74f776db3 with the change stashed, so they are
environmental and pre-existing.

## Review cycles

### Cycle 1 - head 697984f7

`claude[bot]`: reviewed, no blocking items, verdict "looks good to merge". Two non-blocking nits,
both applied in 1e582454:

1. **Anonymous relationship plus a predicate that names a relationship variable** was not exercised
   by a test. Confirmed by running it before writing the assertion: the reference resolves to null
   and the comprehension yields an empty list, which is the engine's pre-existing behavior for any
   undefined variable, so this is documentation of an edge case rather than a regression. Pinned by
   `anonymousRelationshipLeavesAPredicateVariableUndefined`, paired with the named-relationship
   spelling so the contrast is explicit.
2. **`@author` tag** carried over from the package template. Applied, with a caveat: the reviewer
   guessed it was a copy-paste slip, but the tag is in fact the dominant convention in this package
   (12+ `Issue5*Test` files, including those merged with #5480 and #5481). It was removed anyway
   because it attributes the test to someone who did not write it, and four files in the same
   package already carry no tag, so removal has in-package precedent.

`gemini-code-assist`: did not respond within the 15-minute polling window on head 697984f7, so the
both-reviewers gate could not be satisfied and the loop exited with a `timeout` state rather than
`clean-approval`. This matches the reviewer's known inconsistent re-review behavior on this repo and
is not a signal about the change.

No deferred items: both raised points were actionable and were addressed, so no
`review-deferred-*.md` notes file was produced.

## Known gaps, not addressed here

- Relationship inline `WHERE` is ignored outright on variable-length patterns (`WHERE false` still
  returns every row). Tracked separately as #5490.
- For a multi-hop variable-length pattern, `r` binds to a single edge rather than the list of
  traversed edges. That predates this change and is left as is; the fix only makes the predicate
  agree with the row the traversal already emits.
