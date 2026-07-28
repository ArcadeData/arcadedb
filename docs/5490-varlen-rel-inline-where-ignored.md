# Issue #5490 - relationship inline `WHERE` is ignored entirely on variable-length patterns

- Issue: https://github.com/ArcadeData/arcadedb/issues/5490
- Branch: `fix/5490-varlen-rel-inline-where-ignored`
- Base: `main` at 4aaa90c56

## Problem

```cypher
MATCH (a:A {v:1})-[r:E*1..1 WHERE r.tag = 'ok']->(x:A) RETURN count(*)
```

returned 2 instead of 1. `WHERE false` also returned 2, which is conclusive: the predicate never
reached the traversal, rather than being evaluated incorrectly.

## Scope, established by probing before fixing

The issue reported the `MATCH` shape. Probing every surface that parses a variable-length
relationship showed the defect was wider, and that two of the suspected surfaces were already
correct:

| Surface | Before | Verdict |
|---|---|---|
| `MATCH`, fixed length | correct | not affected |
| `MATCH`, variable length (`*1..1`, `*1..2`, `*`) | predicate ignored | **broken** |
| `MATCH`, variable length, property map | correct | not affected |
| `EXISTS {}`, variable length | predicate ignored | **broken** |
| `EXISTS {}`, variable length, property map | correct | not affected |
| Pattern comprehension, variable length | correct | not affected |

Pattern comprehensions were already correct because `PatternComprehensionExpression`
`traverseVariableLength` enforces the predicate itself rather than delegating to the shared
traverser.

## Root cause

The predicate was parsed onto the AST but never handed to the traversal engine. Two producers built
a `VariableLengthPathTraverser` from the relationship pattern and passed only the types, the
property map, and the hop bounds:

- `ExpandPathStep.createTraverser()` - the `MATCH` executor.
- `PatternPredicateExpression.evaluateVLPPattern()` - the `EXISTS {}` evaluator, which additionally
  passed `null` for the property filters.

`GraphTraverser` and its BFS/DFS subclasses had no notion of a per-relationship predicate at all, so
there was nothing for the producers to pass it to.

## Fix

Threaded a per-relationship predicate through the traversal package, applied at the same per-edge
choke point as the existing property filter:

- `GraphTraverser` gains an optional `Predicate<Edge> edgePredicate`, a `withEdgePredicate` setter
  and a `matchesEdgePredicate` check. Null means unconstrained, so patterns without a predicate do
  no per-edge evaluation.
- `BreadthFirstTraverser` and `DepthFirstTraverser` apply it immediately after
  `matchesPropertyFilter`, the single place each already rejects an edge.
- `VariableLengthPathTraverser` builds its BFS or DFS strategy internally, so it now forwards the
  predicate to that delegate. Both `traverse` and `traversePaths` route through one `delegate()`
  helper, which is what keeps the two entry points from drifting.
- Both producers build the predicate from the pattern's `WHERE`, copying the enclosing bindings once
  per source row and rebinding only the relationship variable per candidate edge. Copying once
  rather than per edge matters because a traversal evaluates this for every relationship it walks,
  and it is what lets the predicate reference outer-scope variables.

Pruning during traversal rather than filtering completed paths afterwards is deliberate: under BFS
shortest-path semantics a post-filter would discard a target whose shortest path fails the
predicate, instead of finding the longer path that satisfies it.

### Semantics

Every relationship the path traverses must satisfy the predicate, matching the inline property map
and the clause-level `all(e IN r WHERE ...)` spelling. A test asserts the inline and clause-level
spellings agree.

## Files changed

- `engine/src/main/java/com/arcadedb/query/opencypher/traversal/GraphTraverser.java`
- `engine/src/main/java/com/arcadedb/query/opencypher/traversal/BreadthFirstTraverser.java`
- `engine/src/main/java/com/arcadedb/query/opencypher/traversal/DepthFirstTraverser.java`
- `engine/src/main/java/com/arcadedb/query/opencypher/traversal/VariableLengthPathTraverser.java`
- `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/ExpandPathStep.java`
- `engine/src/main/java/com/arcadedb/query/opencypher/ast/PatternPredicateExpression.java`
- `engine/src/test/java/com/arcadedb/query/opencypher/Issue5490VarLengthRelInlineWhereTest.java` (new)

## Verification

`Issue5490VarLengthRelInlineWhereTest`, 13 methods: 7 failed before the fix and all 13 pass after.
Coverage includes the reported shape, always-false and always-true predicates, the multi-hop
all-relationships-must-satisfy rule, an unbounded `*` pattern, a predicate referencing an outer
binding, parity against the clause-level `all(...)` spelling, both `EXISTS {}` shapes, and controls
for fixed length, property map, pattern comprehension and predicate-free patterns.

Regression run over `com.arcadedb.query.opencypher.**`: 7632 tests, 0 failures. The 3 errors are
`OpenCypherCustomFunctionTest` GraalVM polyglot `NoClassDefFoundError`s, reproduced identically on a
clean tree at 4aaa90c56 with the change stashed, so they are environmental and pre-existing.

Every consumer of the traversal classes was checked: `ExpandPathStep`, `PatternPredicateExpression`
and `ShortestPathStep` are the only ones, all inside the OpenCypher engine and all covered by that
run. `ShortestPathStep` uses only the static `matchesPropertyFilter` helper, which is untouched.

## Known gaps, not addressed here

- For a multi-hop variable-length pattern the relationship variable binds to a single edge rather
  than the list of traversed edges when a predicate reads it. That predates this change; the
  predicate is evaluated per traversed relationship, which is the semantics the fix implements.
