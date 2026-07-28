# #5463 / #5462 - `count(*)` returns 0 for a relationship inline `WHERE` predicate

Two issues, one defect, one reproducer. Both are closed by this change.

- #5463 - "`count(*)` returns 0 while `count(r)` returns 1 for a relationship inline `WHERE` predicate"
- #5462 - "`count(*)` returns 0 for relationship inline `WHERE` while `count(r)` returns the correct result"

## Problem

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

## Fix

None required. The behavioral fix is already on `main`, in `475582108`
("fix(cypher) #5464: honor inline WHERE predicates in node and relationship patterns"), whose third
listed defect is this one, tracked there as #5466:

> A relationship inline predicate lost its edge binding whenever nothing else in the query
> referenced the relationship variable, so it evaluated against an unbound variable and filtered out
> every row - `count(*)` returned 0 while `count(r)` returned 1 (issue #5466). The variable-usage
> analysis now looks inside inline pattern predicates.

Both reporters ran build `1134185d5494f5ab0583da4a508d846449f8e04c`, which predates that commit, so
#5463 and #5462 were filed against a build that could not yet contain the fix. The pattern
comprehension leg of the same defect class was closed separately by `043309f61` (PR #5471, #5460).

This PR therefore changes no production code and carries regression coverage only. What was
genuinely missing is the test: neither reported shape had anything pinning it, so the behavior could
silently regress again.

## Tests

`engine/src/test/java/com/arcadedb/query/opencypher/Issue5463CountStarRelInlineWhereTest.java`,
10 methods:

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

The test was not assumed to guard the defect, it was proven to. It was run at `475582108^`
(`116278160`, the reporters' era) and at current `main`:

| Commit | Result |
|---|---|
| `116278160` (pre-fix) | **8 of 10 methods FAIL**, `count(*)` answers 0 exactly as reported |
| `043309f61` (current `main`) | **10 of 10 PASS** |

Surrounding suites, run together:

```
mvn -o test -Dtest='Issue5463CountStarRelInlineWhereTest,CypherInlinePatternWhereTest,\
Issue5464ExistsRelInlineWhereTest,OpenCypherPatternComprehensionInlineWhereTest,\
CypherInlinePropertyFilterTest,CypherInlinePropertyNormalizationTest,OpenCypherPatternPredicateTest,\
CypherPatternPredicateTest,CypherExistsTest,CypherCountSubqueryTest,CypherCountSubqueryCorrelatedTest,\
CypherCollectSubqueryTest,CountEdgesOptimizationTest,CypherCountNonExistingLabelTest,GAVEligibilityTest'
```

`Tests run: 170, Failures: 0, Errors: 0, Skipped: 0` - `BUILD SUCCESS`.

## Impact and notes

No production code is touched, so behavior-change risk is nil. The value is purely in pinning a
fix that landed without dedicated coverage for either reported shape.

When closing the issues, confirm them against a released build rather than `latest`: both reporters
hit a snapshot image that predated `475582108`, so an unchanged `latest` tag can still reproduce the
old behavior.

## PR

https://github.com/ArcadeData/arcadedb/pull/5485

## Review cycles

**Cycle 1** - `cace1273` - claude reviewed: **LGTM**, "clean, well-scoped, regression-coverage-only
PR", no blocking concerns. gemini-code-assist posted nothing within the 15-minute window, and
nothing in a further 8 minutes of polling, consistent with its ongoing sunset.

| Note | Decision |
|---|---|
| Preserve the "fails 8/10 at `475582108^`" provenance, since a regression test that never failed is worthless | **Already present.** It is in the commit message, the PR body and the Verification section above. No change. |
| The tracking doc overlaps the PR body, so it is a second place to keep in sync | **Kept.** The reviewer scoped this as consistent with repo convention. The most recent tracking docs on `main` (`docs/5460-*`, `docs/5454-*`) carry the same overlap, so trimming it would depart from convention rather than follow it. |
| Add a `// see #...` breadcrumb or a `@Disabled` placeholder test for the plain variable-length gap, which has no issue yet | **Skipped, with the gap recorded below instead.** A `@Disabled` test pointing at no tracking issue is dead weight that goes stale, and a breadcrumb comment in a test class about a defect that class does not exercise is misplaced. Filing the issue is the right home for it, but filing issues is outside the scope authorized for this PR, so it is documented under Known gaps for the maintainer. |

No code changes resulted from this cycle.

## Final state

`timeout` - claude approved on the only cycle with no blocking items; gemini-code-assist never
responded, so the loop's both-bots gate could not be satisfied. The PR is review-clean as far as the
one responding reviewer is concerned.

## Known gaps

Found while probing the surrounding defect class on current `main`, **not** fixed here.

The relationship inline `WHERE` is still ignored outright inside a **variable-length** relationship
pattern:

```cypher
MATCH (a:A {v:1})-[r:E*1..1 WHERE r.tag = 'ok']->(b:A)  RETURN count(*) AS c;  -- 2
MATCH (a:A {v:1})-[r:E*1..1 WHERE r.tag = 'zzz']->(b:A) RETURN count(*) AS c;  -- 2
```

The predicate changes nothing, so it is dropped rather than misevaluated - a different failure mode
from the 0 that #5463/#5462 report on a single-hop pattern. Left alone deliberately: it is out of
scope for these two issues, its `shortestPath` half is already tracked by #5481 and the node-pattern
half by #5480 (both being worked separately), and the plain variable-length case has no open issue
yet and warrants its own triage.
