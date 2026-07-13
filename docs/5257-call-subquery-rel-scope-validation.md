# #5257 - CREATE/MERGE relationship patterns bypass CALL subquery scope validation

## Symptom

Inside a `CALL { ... }` subquery, `CREATE`/`MERGE` relationship patterns referencing outer variables
that were never imported are silently accepted:

```cypher
MATCH (a:A {id: 1}), (b:B {id: 2})
CALL {
  CREATE (a)-[:R]->(b)
  RETURN 1 AS ok
}
RETURN ok
```

The query succeeds, reports `relationshipsCreated: 1`, but `MATCH (:A)-[r:R]->(:B) RETURN count(r)`
returns 0. `SET`, `DELETE` and label updates correctly raise `UndefinedVariable` in the same position.

## Root cause

`CypherSemanticValidator.validateVariableScope` seeds a `CALL { ... }` body with an empty scope when
nothing is imported (`validateSubqueryBranchScope`). `case SET`/`case DELETE` check their variables
against that scope, but `case CREATE`/`case MERGE` never check pattern variables - they call
`addBoundVarsFromPattern` unconditionally, which *declares* `a` and `b` as fresh variables.

At runtime `CreateStep.createPath` / `MergeStep.createNewPath` look the variable up in the seed row,
find nothing, and fall back to `createVertex(...)`, minting brand-new anonymous `Vertex`-typed nodes.
The edge is then created between those orphans and `stats.incRelationshipsCreated()` fires. Hence:
success + counter + no relationship between the intended endpoints (and two orphan vertices).

Neo4j and Memgraph both reject these queries.

## Fix

Thread a `shadowed` set through `validateVariableScope`: names visible in an enclosing scope that were
*not* imported into the current `CALL` subquery. `CREATE` and `MERGE` now reject any pattern variable
(node, relationship or path) that is absent from the current scope but present in `shadowed`.

`shadowed` is computed at subquery entry as `(outerScope + inheritedShadowed) - importedNames`, where
`importedNames` is:
- the explicit scope list for `CALL (a, b) { ... }`,
- the whole outer scope for `CALL (*) { ... }`,
- the outer names that survive a leading importing `WITH` (so `CALL { WITH a CREATE (a)-[:R]->(b) }`
  still flags `b`),
- empty for a bare `CALL { ... }`.

This also closes the same hole for nested subqueries, since `shadowed` is inherited downwards.

## Tests

`engine/src/test/java/com/arcadedb/query/opencypher/Issue5257CallSubqueryRelationshipScopeTest.java`

## Impact

Queries that previously "succeeded" while writing orphan vertices now fail fast with
`UndefinedVariable`. That is the intended, spec-aligned behaviour and matches Neo4j/Memgraph.

## Known related gap (not addressed)

A `WITH` *inside* a subquery body that drops an imported variable, followed by a `CREATE` re-using that
name, is still not flagged (Neo4j errors). Tracked separately - the reported bug is the un-imported case.
