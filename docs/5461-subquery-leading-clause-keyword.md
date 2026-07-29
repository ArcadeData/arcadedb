# #5461 - UNWIND as the first clause yields zero rows in COUNT {} and EXISTS {} subqueries

## Symptom

A subquery body whose first clause is `UNWIND` produces no rows:

```cypher
RETURN COUNT { UNWIND [1, 2] AS y RETURN y } AS c;
-- returns 0, expected 2
```

Adding an otherwise inert `WITH 1 AS dummy` before the `UNWIND` restores the correct answer, which is
what makes the report look like a subquery initialization problem. `EXISTS { }` is affected the same
way (silently `false`), and so is any body opening with `CALL`, `OPTIONAL MATCH` or `FOREACH`.

The `CALL { }` half of the report did **not** reproduce on `main`: the returning-CALL repro from the
issue already yields the expected 6 rows. `SubqueryStep` seeds its inner plan with a one-row seed, so
`UnwindStep` gets its input. That case is covered by a test here as a regression guard only.

## Root cause

Two incomplete keyword lists, both in the text-rewriting layer that `COUNT { }`, `EXISTS { }` and
`COLLECT { }` use. Those three expressions do not run inside the outer plan - they re-execute their
body as a **standalone query string**, once per outer row - so every decision about the body is made
by string inspection.

**1. The parse-time pattern wrapper.** `CypherExpressionBuilder.parseCountExpression` and
`parseExistsExpression` have to tell a bare pattern (`COUNT { (a)-[:KNOWS]->(b) }`, which needs a
synthesized `MATCH`) from a full subquery body (which does not). The test was:

```java
if (!upperTrimmed.startsWith("MATCH") && !upperTrimmed.startsWith("WITH") && !upperTrimmed.startsWith("RETURN"))
  subquery = "MATCH " + subquery + " RETURN 1";
```

`UNWIND [1, 2] AS y RETURN y` matches none of the three, so it was read as a pattern and became
`MATCH UNWIND [1, 2] AS y RETURN y RETURN 1`, which does not parse. Each of the three expressions
absorbs a body that cannot run into its neutral value, so the parse failure surfaced as `COUNT` = 0
and `EXISTS` = false rather than as an error. `COLLECT` has no such wrapper, which is why it was the
one of the three that already worked - the asymmetry is the tell.

The leading `WITH` workaround works simply because `WITH` is one of the three listed keywords.

**2. The correlation injection point.** Once the body is correlated to an outer row,
`CorrelatedSubqueryRewriter.injectWhereConditions` inserts `WHERE id(n) = $param` before the first
clause keyword that closes the MATCH pattern. Its list was `WITH, RETURN, ORDER, SKIP, LIMIT, UNION` -
also missing `UNWIND`. For `MATCH (n) UNWIND n.tags AS t RETURN t` it therefore skipped past the
`UNWIND` and produced `MATCH (n) UNWIND n.tags AS t WHERE id(n) = $p RETURN t`. `WHERE` cannot follow
`UNWIND`, so this too failed to parse and was absorbed as 0.

Only the second list matters once an outer graph variable is in play, which is why a correlated
`MATCH (n:T) RETURN COUNT { UNWIND n.tags AS t RETURN t }` stayed broken after the first list was
fixed. Both had to be corrected.

A third copy of the same list already existed and was already correct: `ExistsExpression`'s private
`wrapNonMatchBody` knew about `UNWIND`, `CALL` and `OPTIONAL`. Three copies, three different contents.

## Fix

`CorrelatedSubqueryRewriter` gains the single authoritative list and one public predicate:

- `CLAUSE_KEYWORDS` - every keyword that can open a Cypher subquery body (`MATCH`, `OPTIONAL`, `WITH`,
  `RETURN`, `UNWIND`, `CALL`, `FOREACH`, `LOAD`, `USE`, `FINISH`, and the mutating clauses).
- `startsWithClauseKeyword(body)` - case-insensitive, with a word boundary so a pattern bound to a
  variable such as `matches` is not read as `MATCH`.
- `CLAUSE_BOUNDARY_KEYWORDS` - the same list plus `ORDER`, `SKIP`, `LIMIT`, `UNION`, used by
  `injectWhereConditions` to find where the MATCH pattern ends. `WHERE` stays out of it: it is the one
  keyword the injection merges into instead of stopping at.

The three drifted call sites now route through it:

- `CypherExpressionBuilder.parseCountExpression` / `parseExistsExpression` - wrap into `MATCH` only
  when the body is genuinely not a clause.
- `ExistsExpression.wrapNonMatchBody` - its private keyword list deleted in favor of the shared one.
- `CorrelatedSubqueryRewriter.injectWhereConditions` - scans for any boundary keyword.

One further correlated-path defect had to go with it. `CountExpression`'s non-MATCH wrapper built
`"MATCH " + patterns + ", " + body + " RETURN 1"`, comma-splicing the injected pattern into whatever
clause the body opened with: `MATCH (n), UNWIND n.tags AS t RETURN t`. It now mirrors `EXISTS` and
prepends the patterns as their own MATCH clause when the body starts with a clause keyword.

## Verification

New regression test `engine/src/test/java/com/arcadedb/query/opencypher/Issue5461SubqueryLeadingClauseTest.java`,
10 tests. Every one of them was confirmed failing before the fix: 5 failures on the first run, then 1
remaining correlated failure that isolated the second defect, and the no-`RETURN` case checked
separately by restoring the old three-keyword whitelist (0 instead of 2).

Coverage: `COUNT`/`EXISTS`/`COLLECT` with an `UNWIND`-first body across list sizes 0-3; the issue's
per-outer-row case; bodies opening with `CALL` and `OPTIONAL MATCH`; an `UNWIND`-first body with no
`RETURN` of its own; the correlated `MATCH (n:T) ... COUNT { UNWIND n.tags AS t ... }` form; the
`WITH 1 AS dummy` and `RETURN 1` controls from the issue; the returning-`CALL {}` repro; and a guard
that bare-pattern bodies are still wrapped into a `MATCH`.

Regression run: the full `com.arcadedb.query.opencypher.**` suite, 7693 tests, 0 failures, plus the
full `engine` module suite.

## Impact

Any `COUNT { }` or `EXISTS { }` whose body opens with `UNWIND`, `CALL`, `OPTIONAL MATCH` or `FOREACH`
silently returned the neutral value. The silence is the dangerous part: a de-duplicating
`WHERE NOT EXISTS { ... }` guard built on such a body degrades into an unconditional write. The
shared keyword list closes the class of defect rather than the single reported instance.
