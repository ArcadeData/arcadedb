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
11 tests. Every one of them was confirmed failing before the fix: 5 failures on the first run, then 1
remaining correlated failure that isolated the second defect, the no-`RETURN` case checked separately
by restoring the old three-keyword whitelist (0 instead of 2), and the classifier unit test checked
by removing the bounds check (`StringIndexOutOfBoundsException`).

A first attempt at that last test asserted `COUNT { }` did not throw. It passed with and without the
bounds check - `COUNT { }` never reaches the classifier - so it was replaced with a direct call to
the public method rather than kept as coverage it did not provide.

Coverage: `COUNT`/`EXISTS`/`COLLECT` with an `UNWIND`-first body across list sizes 0-3; the issue's
per-outer-row case; bodies opening with `CALL` and `OPTIONAL MATCH`; an `UNWIND`-first body with no
`RETURN` of its own; the correlated `MATCH (n:T) ... COUNT { UNWIND n.tags AS t ... }` form; the
`WITH 1 AS dummy` and `RETURN 1` controls from the issue; the returning-`CALL {}` repro; a guard that
bare-pattern bodies are still wrapped into a `MATCH`; and the classifier itself against blank bodies
and patterns bound to keyword-prefixed variables (`matches = (a)-->(b)`).

Regression run: the full `com.arcadedb.query.opencypher.**` suite, 7695 tests, 0 failures, and the
full `engine` module suite, 10150 tests, 0 failures. The opencypher count reconciles against a
measured `main` baseline of 7684 plus this branch's 11 new tests.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5540

### Review cycles

**Cycle 1** - `a5cc23d` - claude[bot]: no blocking findings. It independently traced and confirmed
the three correctness properties the fix depends on: a `WHERE`-before-`UNWIND` corruption is not
reachable (a `whereCondition` is only ever produced alongside a `matchPattern`, so
`injectMatchPatterns` always runs first), adding `MATCH`/`OPTIONAL` to the boundary list handles
multi-`MATCH` bodies correctly, and `COLLECT` needs no wrapper change because its `WITH *` shape
already tolerates any leading clause. Three minor non-blocking notes, dispositioned below.

**Cycle 2** - `0a8ed53` - claude[bot]: LGTM, no blocking findings. Four minor notes, dispositioned
below. One of them (`INSERT` is not a standard openCypher clause) was checked against the grammar and
is incorrect: `Cypher25Parser.g4` defines `insertClause : INSERT insertPatternList` and lists it among
the clause alternatives, so `INSERT` belongs in the list and was kept.

**Cycle 3** - `a550352` - claude[bot]: no blocking issues. It confirmed the first-char pre-filter is
sound for both callers (derived from the superset array, and non-ASCII first chars fall through to
the full scan), and that the bounds check closes the empty-body case. Three minor notes, all
declined with reasons below.

### CI note

`Meterian client scan` fails on this PR with a security score of 0. It is **not caused by this
change**: the same check fails identically on PRs #5539, #5536 and #5534, and this branch touches no
dependency manifest (four Java files, one test, one doc). Every other gate - Codacy, CodeQL,
`claude-review`, setup - passes.

## Follow-ups

- **Applied in cycle 1.** The bare-pattern branch of `CountExpression.wrapNonMatchBody` is
  unreachable, because a bare pattern is normalized into `MATCH ... RETURN 1` at build time before
  `correlate` runs. Verified (single construction site) and documented with a comment rather than
  deleted, so the branch stays correct if that normalization ever moves.
- **Investigated, not a defect.** Both review cycles flagged the
  `subquery.toUpperCase().contains("RETURN ")` test as a naive substring match that would
  false-positive on a literal such as `WHERE x = 'RETURN y'`. Probed directly: it does not produce a
  wrong answer. `COUNT { MATCH (n:T) WHERE n.name = 'RETURN y' }` returns the true count (2 of 3
  rows), because a `MATCH`-only body executes fine without the synthesized `RETURN`. The substring
  match is imprecise but currently harmless, so nothing was filed for it.
- **No action, informational.** The mutating keywords in `CLAUSE_KEYWORDS` are inert on the
  `COUNT`/`EXISTS` parse-time path, since update clauses are rejected before it. They are carried for
  completeness of the shared list and for boundary detection.
- **Applied in cycle 2.** The boundary scan walked all 21 keywords at every unnested character
  position. It now rejects a position on a single `boolean[]` read keyed by the first character. The
  table is derived from `CLAUSE_BOUNDARY_KEYWORDS` at class init, so it cannot drift from the list
  the way the three original copies did, and because that array is a superset of `CLAUSE_KEYWORDS` it
  is a sound pre-filter for both callers. Indexing the first character also made a blank body throw,
  so `matchesAnyKeywordAt` now bounds-checks and a unit test covers it - verified non-vacuous by
  removing the check and watching the test fail with `StringIndexOutOfBoundsException`.
- **Declined in cycle 3, with a correction.** `startsWithClauseKeyword` allocates a
  `trim().toUpperCase()` copy per call. The review placed this off the row-scan hot path; that is not
  quite right - `CountExpression`/`ExistsExpression` call it through `wrapNonMatchBody`, which runs
  once per outer row for a correlated subquery. It is still not worth changing: each of those outer
  rows already pays for a complete standalone parse and execution of the subquery
  (`database.query(...)`), which exceeds two short-string allocations by orders of magnitude. Removing
  the allocation would also mean either duplicating the word-boundary logic or adding a
  case-insensitive variant of it, and duplicated keyword logic is precisely what caused this bug.
- **Filed as #5541.** The update-clause guards in all three parse methods
  (`upper.contains("SET ")`, `"CREATE "`, ...) are naive substring scans with the exact blind spot
  fixed here. Confirmed by direct probe: `COUNT { MATCH (n:T) WHERE n.name = 'SET x' RETURN n }`
  throws `InvalidClauseComposition: COUNT subquery cannot contain update clauses` for a read-only
  body, and `EXISTS` and `COLLECT` fail identically. Left out of this PR to keep it scoped; unlike
  #5461 it throws rather than returning a silent neutral value, so it is visible rather than
  data-corrupting.

## Impact

Any `COUNT { }` or `EXISTS { }` whose body opens with `UNWIND`, `CALL`, `OPTIONAL MATCH` or `FOREACH`
silently returned the neutral value. The silence is the dangerous part: a de-duplicating
`WHERE NOT EXISTS { ... }` guard built on such a body degrades into an unconditional write. The
shared keyword list closes the class of defect rather than the single reported instance.
