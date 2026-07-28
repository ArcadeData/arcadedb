# Issue #5489 - node inline `WHERE` in a pattern comprehension sees the same hop's relationship variable as unbound

- Issue: https://github.com/ArcadeData/arcadedb/issues/5489
- Branch: `fix/5489-node-inline-where-unbound-rel-var`
- Base: `main` at 74f776db3

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

`Issue5489NodeInlineWhereRelVariableTest`, 10 methods: 5 failed before the fix and all 10 pass
after. Coverage includes the reported shape, a predicate referencing only the relationship variable,
a predicate that must reject every candidate, the combined relationship-plus-node inline form,
explicit parity against the `MATCH` spelling, a variable-length hop, an earlier-hop relationship
reference (which already worked and is pinned as a control), and predicate-free controls.

Regression run over the whole `com.arcadedb.query.opencypher.**` package: 7618 tests, 0 failures.
The 3 errors are `OpenCypherCustomFunctionTest` GraalVM polyglot `NoClassDefFoundError`s; they were
reproduced identically on a clean tree at 74f776db3 with the change stashed, so they are
environmental and pre-existing.

## Known gaps, not addressed here

- Relationship inline `WHERE` is ignored outright on variable-length patterns (`WHERE false` still
  returns every row). Tracked separately as #5490.
- For a multi-hop variable-length pattern, `r` binds to a single edge rather than the list of
  traversed edges. That predates this change and is left as is; the fix only makes the predicate
  agree with the row the traversal already emits.
