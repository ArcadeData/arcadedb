# #5463 / #5462 - `count(*)` returns 0 for a relationship inline `WHERE` predicate

Two issues, one defect, one reproducer. Both are closed by this change.

- #5463 - "`count(*)` returns 0 while `count(r)` returns 1 for a relationship inline `WHERE` predicate"
- #5462 - "`count(*)` returns 0 for relationship inline `WHERE` while `count(r)` returns the correct result"

## Reported behavior

Graph: two `(:A {v:1})-[:E]->(:A {v:2})` edges, tagged `ok` and `bad`.

```cypher
MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN count(*) AS c;  -- 0, expected 1
MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN count(r) AS c;  -- 1, correct
MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN r.tag AS t;     -- "ok", correct
```

Changing only the aggregate expression changed the result of the preceding `MATCH`. #5462 adds a
sharper control: keeping `count(*)` and merely adding `collect(r.tag)` to the same projection
flipped the count from 0 to 1.

## Root cause

The Cypher variable-usage analysis that decides which pattern bindings the plan has to materialize
did not look inside inline pattern predicates. When nothing *outside* the pattern referenced `r`,
the edge binding was elided from the plan, so the inline predicate `r.tag = 'ok'` was evaluated
against an unbound variable, yielded false and rejected every row - hence 0.

Any downstream mention of `r` (`count(r)`, `r.tag`, or a `collect(r.tag)` sitting next to the
`count(*)`) kept the binding alive and masked the defect. That is exactly the
"downstream-usage-dependent correctness" asymmetry both reporters observed, and it explains why a
literal predicate such as `WHERE 1=1` behaved correctly: it references no variable at all.

## Status on `main`

The behavioral fix is already on `main`. It landed in `475582108`
("fix(cypher) #5464: honor inline WHERE predicates in node and relationship patterns"), whose third
listed defect is this one, tracked there as #5466:

> A relationship inline predicate lost its edge binding whenever nothing else in the query
> referenced the relationship variable, so it evaluated against an unbound variable and filtered out
> every row - `count(*)` returned 0 while `count(r)` returned 1 (issue #5466). The variable-usage
> analysis now looks inside inline pattern predicates.

Both reporters ran build `1134185d5494f5ab0583da4a508d846449f8e04c`, which predates that commit, so
#5463 and #5462 were filed against a build that could not yet contain the fix. The pattern
comprehension leg of the same defect class was closed separately by `043309f61` (PR #5471, #5460).

This was verified rather than assumed. The regression test below was run at `475582108^`
(`116278160`, the reporters' era) and at current `main`:

| Commit | Result |
|---|---|
| `116278160` (pre-fix) | 8 of 10 test methods FAIL, `count(*)` answers 0 exactly as reported |
| `043309f61` (current `main`) | 10 of 10 PASS |

## Change in this PR

Regression coverage only - no production code change is needed, and none is made. The two reported
shapes had no dedicated test pinning them, so the behavior could silently regress again.

`engine/src/test/java/com/arcadedb/query/opencypher/Issue5463CountStarRelInlineWhereTest.java`
(10 test methods) covers:

- `count(*)` agreeing with `count(r)` and with the materialized `r.tag` rows;
- the inline predicate honored for every tag value, including the empty and the match-all case;
- #5462's control - a second projection item (`collect(r.tag)`) must not change `count(*)`;
- the literal-predicate and clause-level-`WHERE` controls from both issue reports;
- anonymous source, anonymous target and both-ends-anonymous endpoint shapes;
- right-to-left, undirected and untyped relationship variants;
- `count(b)`, `count(DISTINCT b)`, `sum(b.v)` and `count(*)` routed through `WITH`;
- `count(*)` under a grouping key;
- `OPTIONAL MATCH`, hit and miss;
- both pattern-parsing paths that accept a relationship inline predicate - the `MATCH` path and the
  pattern-comprehension / `COUNT {}` / `EXISTS {}` path.

## Verification

```
mvn -o test -Dtest='Issue5463CountStarRelInlineWhereTest,CypherInlinePatternWhereTest,\
Issue5464ExistsRelInlineWhereTest,OpenCypherPatternComprehensionInlineWhereTest,\
CypherInlinePropertyFilterTest,CypherInlinePropertyNormalizationTest,OpenCypherPatternPredicateTest,\
CypherPatternPredicateTest,CypherExistsTest,CypherCountSubqueryTest,CypherCountSubqueryCorrelatedTest,\
CypherCollectSubqueryTest,CountEdgesOptimizationTest,CypherCountNonExistingLabelTest,GAVEligibilityTest'
```

`Tests run: 170, Failures: 0, Errors: 0, Skipped: 0` - `BUILD SUCCESS`.

## Adjacent gap found, deliberately not fixed here

While probing the surrounding defect class on current `main`, the relationship inline `WHERE` is
still ignored outright inside a **variable-length** relationship pattern:

```cypher
MATCH (a:A {v:1})-[r:E*1..1 WHERE r.tag = 'ok']->(b:A)  RETURN count(*) AS c;  -- 2
MATCH (a:A {v:1})-[r:E*1..1 WHERE r.tag = 'zzz']->(b:A) RETURN count(*) AS c;  -- 2
```

The predicate changes nothing, so it is dropped rather than misevaluated - a different failure mode
from the one #5463/#5462 report (which is 0, not 2, and uses a single-hop pattern). It is left
alone on purpose:

- it is out of scope for these two issues;
- the `shortestPath` half of it is already tracked by #5481, which is being worked in its own
  worktree, and the node-pattern half by #5480;
- the plain variable-length case has no open issue yet and should be triaged separately.

## Recommendation

After merge, confirm #5463 and #5462 against a released build rather than `latest`, since both
reporters hit a snapshot image that predated `475582108`.
