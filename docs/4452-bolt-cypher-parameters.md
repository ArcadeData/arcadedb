# Issue #4452 - Bolt Cypher Parameterized Queries Return Empty Results

## Problem

Cypher queries executed via the Neo4j Bolt protocol with parameters (`$param`) return empty results, while the same queries with literal values work correctly.

Affected cases from the issue report:
- `MATCH (n:Tag { name: $nameParam }) RETURN n` - parameterized node property filter
- `MATCH (n) WHERE ID(n) = $parentId RETURN n` - parameterized ID filter
- `MATCH (from)-[:rel*0..]->(x:Tag { name: $nameParam }) WHERE ID(from) = $parentId` - VLP with both

## Root Cause

`ExpandPathStep.matchesTargetProperties` (before commit `2aa5534ce`) did not resolve `ParameterReference` objects stored in the target node pattern's property map. The method directly compared `vertex.get(key)` to the raw `ParameterReference` object:

```java
// BEFORE (broken):
final Object expected = entry.getValue();  // ParameterReference("nameParam")
if (actual == null || !actual.equals(expected))  // "tag2".equals(ParameterReference) = false
    return false;
```

So for VLP queries like `MATCH (from)-[:rel*0..]->(x:Tag { name: $nameParam })`, the target node property filter always rejected every candidate node when the value was a parameter, producing empty results.

## Fix

Already applied in commit `2aa5534ce` (issue #4271, 2026-05-20):
- Added `ParameterReference` resolution from `context.getInputParameters()` in `matchesTargetProperties`
- Added numeric type-safe comparison (Integer vs Long coercion)
- Added support for `currentResult` context for expression-based values

Users on v26.5.1 need to upgrade to v26.6.1+ to get this fix.

## Verification

Five regression tests added to `BoltProtocolIT`:
1. `matchWithParameterPropertyFilter` - basic `MATCH (n:T {prop: $param})`
2. `matchWithWhereParameterStringFilter` - `WHERE n.prop = $param`
3. `matchByIdParameter` - `WHERE ID(n) = $id`
4. `vlpMatchWithParameters` - full VLP pattern with both parameters
5. `vlpMatchWithParametersInTransaction` - same within an explicit transaction

All 76 Bolt tests pass (28s run).

Additionally, four end-to-end tests were added to `e2e-js/src/js-bolt-e2e.test.js` (Testcontainers +
the real `neo4j-driver`), under a `variable-length path with parameters (issue #4452)` describe block:
they build the agent/tag graph from the issue report and assert the `(from)-[:agentTags*0..]->(x:Tag
{name: $nameParam}) WHERE ID(from) = $parentId` query resolves both parameters. Verified end-to-end:
all 10 tests pass against `arcadedata/arcadedb:latest` (26.6.1-SNAPSHOT, contains the fix), and the
two VLP `$nameParam` tests fail (return empty) against `arcadedata/arcadedb:26.5.1` (pre-fix),
confirming they reproduce the reported bug.

## PR

https://github.com/ArcadeData/arcadedb/pull/4459

## Review cycles

- **Cycle 1** - HEAD `f1a5a86`. Gemini: 3 inline comments suggesting `try-finally` + `DETACH DELETE`
  cleanup. Claude: "Up to standards", 0 blocking, 4 advisory items.
  - Applied (claude): explicit `assertThat(allNodes.hasNext()).isFalse()` in `matchByIdParameter`;
    stored parent-id `Result` + `hasNext()` check before `.next()` in both VLP tests; comment on the
    clean-per-test-database dependency; comment clarifying `*0..` is intentional (mirrors the issue's
    query, zero-hop filtered by the target label).
  - Skipped (gemini cleanup nits): `BaseGraphServerTest` recreates the database per test method
    (`@BeforeEach` deletes + recreates, `@AfterEach` drops) and each test uses unique type names, so
    there is no cross-test pollution; manual cleanup would also break consistency with every existing
    test in `BoltProtocolIT`. Claude independently confirmed this. Replied on all three threads.
- **Cycle 2** - HEAD `51a711e18`. Gemini: same 3 cleanup nits carried forward (no fresh review).
  Claude: "Approve with minor suggestions", 0 blocking, 3 advisory items.
  - Applied (claude): wrapped `vlpMatchWithParameters` setup in a single transaction for consistency
    with `vlpMatchWithParametersInTransaction`; comment noting `ORDER BY n.score` is load-bearing.
  - Skipped (claude): consolidating setup into a comma-separated CREATE - purely cosmetic, claude
    labeled it "not a blocker"; the per-statement form is clearer.
- **Cycle 3** - HEAD `1e8a27b8`. Gemini: same 3 cleanup nits carried forward. Claude: one blocking
  item - remove the committed `docs/review-deferred-*.md` workflow-metadata files (established project
  decision, see `docs/4397-matchescondition-regex-hashcode-collision.md`).
  - Applied: removed `docs/review-deferred-f1a5a86.md` and `docs/review-deferred-51a711e.md`; their
    rationale is captured in this section instead.

## Final state

clean-approval - all actionable bot feedback addressed. The only recurring open items are gemini's
cleanup nits, which are non-applicable for this test framework (justified-skip, replies on threads,
corroborated by claude across all cycles).
