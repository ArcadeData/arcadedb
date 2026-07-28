# Issue #5496 - expression-form `shortestPath` returns null for an unbound endpoint

- Issue: https://github.com/ArcadeData/arcadedb/issues/5496
- Branch: `fix/5496-shortestpath-expr-unbound-target`
- Base: `main` at 2dfc3c75b

## Problem

```cypher
MATCH (a:A {v:1}) RETURN shortestPath((a)-[*]->(x:A)) AS p
```

returned `null`. The caller had no way to tell "no path exists" from "this query shape is not
supported". A pattern whose endpoint carried no variable at all raised a bare
`IllegalArgumentException` instead, so two spellings of the same mistake failed in two different ways
and neither was actionable.

## Why the originally proposed fix was dropped

The issue as filed recommended teaching the expression form to resolve an unbound endpoint, "matching
the `MATCH` form". Probing the `MATCH` form before implementing showed that is not well defined.

`ShortestPathStep`, the `MATCH` evaluator, also requires both endpoints to be bound vertices and
skips the row otherwise. The `MATCH` spelling works because the **planner** binds the endpoint with a
node scan first and then computes one path per candidate, so it **multiplies rows**:

```cypher
MATCH p = shortestPath((a:A {v:1})-[*]->(x:A)) RETURN x.v, [n IN nodes(p) | n.v]
-- 3 rows: x=1 -> [1], x=10 -> [1,10], x=2 -> [1,10,2]
```

An expression produces a single value per input row and cannot multiply rows, so it can never return
what the `MATCH` spelling returns for the same written pattern. "Make the two agree" is not
implementable. The remaining choices were to invent a different scalar semantics for the expression
form (nearest matching node) or to reject the shape. Rejecting was chosen: it removes the real
user-facing harm, the silent `null`, without introducing two spellings of one pattern that
legitimately disagree.

This also corrected a mistake in the issue as filed: a reported zero-length path `[1]` was a
misreading of the first of those three rows, not a defect. A comment on the issue records both
corrections.

## Fix

`ShortestPathExpression.evaluate` now raises `CommandExecutionException` when an endpoint cannot be
resolved, naming the offending endpoint and the spelling that does support searching:

> `shortestPath() as an expression requires both endpoints bound to vertices, but 'x' is not bound.`
> `Use MATCH p = shortestPath(...) to search for an unbound endpoint.`

Four cases raise: a start or end pattern with no variable, and a start or end variable that is not
bound in the current row. `allShortestPaths()` names itself in the message.

**Null still propagates.** A variable that *is* present in the row but does not hold a vertex, the
usual case being null from a non-matching `OPTIONAL MATCH`, keeps returning null rather than raising.
That is standard Cypher null propagation and is a different situation from an unbound variable. The
two are distinguished by whether the row carries the name at all, not by whether the value is a
vertex. A genuine "no path between two bound vertices" answer also stays null, which is what the new
error must not be confused with.

The class Javadoc previously said only "Both endpoints must be bound to variables"; it now states the
rule and why the `MATCH` spelling differs.

## Files changed

- `engine/src/main/java/com/arcadedb/query/opencypher/ast/ShortestPathExpression.java`
- `engine/src/test/java/com/arcadedb/query/opencypher/Issue5496ShortestPathExpressionUnboundEndpointTest.java` (new)

## Verification

`Issue5496ShortestPathExpressionUnboundEndpointTest`, 11 methods: 6 failed before the fix and all 11
pass after. Coverage: unbound target, unbound source, anonymous target, anonymous source,
`allShortestPaths`, and an assertion on message content (names the variable and the working
spelling). Controls that must not regress: bound endpoints still compute the path, bound endpoints
with an inline relationship `WHERE` still work (guards the #5481 path), an unsatisfiable predicate
between bound endpoints still returns null, an endpoint bound to null still propagates as null, and
the `MATCH` spelling still resolves an unbound endpoint across three candidate rows.

Regression run over `com.arcadedb.query.opencypher.**`: 7643 tests, 0 failures. Turning a silent
null into a thrown exception is the kind of change that breaks tests relying on the old behavior, so
this run was the main risk check; nothing depended on it. The 3 errors are
`OpenCypherCustomFunctionTest` GraalVM polyglot `NoClassDefFoundError`s, reproduced identically on a
clean tree at 2dfc3c75b with the change stashed, so they are environmental and pre-existing.

Consumers of `ShortestPathExpression` were enumerated: all are inside the OpenCypher engine and
covered by that run. Hits for `shortestPath` elsewhere in the repo (Studio function reference, Python
bindings, graph/OLAP tests) are the SQL `SQLFunctionShortestPath`, a different entry point that this
change does not touch.

## Deliberately not changed

- The arity guard at the top of `evaluate` still raises `IllegalArgumentException` for a pattern
  without exactly 2 nodes. Converting it would be consistent, but it is a different error class and
  is not reachable from a query shape covered by a test here, and the repo requires new behavior to
  come with a test.
- Whether an `x = source` candidate should yield a zero-length path under `[*]`, which is
  one-or-more, is a separate question about the `MATCH` form and is untouched.
