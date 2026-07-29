# #5541 - COUNT/EXISTS/COLLECT reject a read-only body when a string literal contains an update keyword

## Symptom

A read-only subquery whose body merely *mentions* an update keyword fails to parse:

```cypher
CREATE (:T {name:'SET x'});

RETURN COUNT { MATCH (n:T) WHERE n.name = 'SET x' RETURN n } AS v;
-- CommandParsingException: InvalidClauseComposition: COUNT subquery cannot contain update clauses
```

`EXISTS { }` and `COLLECT { }` fail identically, with their own message. The keyword does not have to
open the literal: `WHERE n.name = 'a SET b'` is rejected too.

## Root cause

`COUNT { }`, `EXISTS { }` and `COLLECT { }` must be read-only, so each of the three parse methods in
`CypherExpressionBuilder` guards its body against update clauses. All three did it by substring scan
over the raw text:

```java
final String upper = subquery.toUpperCase();
if (upper.contains("SET ") || upper.contains("CREATE ") || upper.contains("DELETE ")
    || upper.contains("MERGE ") || upper.contains("REMOVE "))
  throw new CommandParsingException("InvalidClauseComposition: ...");
```

`contains` cannot tell a clause from ordinary text. A string literal is ordinary text, so any user
data containing one of the five keywords made a legal read-only filter unparseable.

This is the same class of defect as #5461 (fixed in #5540): a decision about a subquery body taken by
naive substring matching. #5540 consolidated the *clause-keyword* lists into
`CorrelatedSubqueryRewriter`, which already scans with word boundaries and skips string literals;
these update-clause guards were the remaining naive scans in the same three methods.

The failure mode differs from #5461 in a useful way. #5461 was silent - the parse failure was
absorbed into the expression's neutral value, so `COUNT` returned 0. This one throws, so it is
visible rather than data-corrupting.

## Fix

`CorrelatedSubqueryRewriter` gains the update-clause list and one public predicate, next to the
clause-keyword list it already owns:

- `UPDATE_CLAUSE_KEYWORDS` - `CREATE`, `MERGE`, `SET`, `DELETE`, `REMOVE`, `INSERT`. `DELETE` also
  covers `DETACH DELETE`; on `INSERT` see the review-cycle note below.
- `containsUpdateClause(body)` - walks the body once, skipping string literals and backtick-quoted
  identifiers (including backslash escapes), and matches only on a word boundary through the existing
  `matchesKeywordAt`.

The three guards in `CypherExpressionBuilder` (`parseExistsExpression`, `parseCollectExpression`,
`parseCountExpression`) now call it. Their messages are unchanged.

Two deliberate choices:

- **Nesting is not tracked.** Unlike `injectWhereConditions`, the scan does not skip bracketed
  constructs. A write nested inside one - `CALL { ... CREATE ... }` - is still a write and must still
  be rejected. Only literals are exempt, which is exactly the reported false positive.
- **Word boundaries make the guard slightly stronger, not weaker.** The old scan required a trailing
  space, so `CREATE(n)` slipped through; the boundary check catches it. Conversely `n.createdAt`,
  `n.set`, `[r:SET_BY]` and `{create: 1}` are correctly *not* clauses, via the existing rejection of
  `.`, `:` and `$` prefixes and of identifier characters on either side.

The first-character pre-filter table introduced in #5540 is now built by a shared `firstCharTable`
helper and passed to `matchesAnyKeywordAt` explicitly, so the update-keyword scan gets its own
correctly derived table rather than borrowing the boundary one. Still derived, never hand-maintained.

## Verification

New regression test
`engine/src/test/java/com/arcadedb/query/opencypher/Issue5541SubqueryUpdateClauseGuardTest.java`,
12 tests.

Proven to fail before the fix: with the helper added but the three guards still naive, 6 of the 9
failed, each with the exact reported `InvalidClauseComposition` exception - the `COUNT`, `EXISTS`,
`COLLECT`, substring-of-literal, quoted-forms and correlated cases. The remaining 3 are the guard
that genuine update clauses are *still* rejected, the guard that a rejected body writes nothing, and
the direct unit test of the new predicate; those must pass in both states.

Coverage: literals in single quotes, double quotes and backticks, and an escaped quote inside a
literal; a keyword as a mid-literal substring; the correlated form; every one of the five update
clauses plus `DETACH DELETE` still rejected across all three expressions; a rejected body leaving the
data untouched; and the predicate itself against property access, labels/types, map keys and
longer identifiers that merely start with a keyword.

The openCypher TCK scenario that requires rejection - `ExistentialSubquery2.feature` scenario [3],
"Full existential subquery with update clause should fail" - continues to pass.

Regression run: the full `com.arcadedb.query.opencypher.**` suite, 7714 tests, 0 failures, and the
full `engine` module suite, 10169 tests, 0 failures. Both suites were re-measured after each review
cycle changed the predicate, rather than carried forward.

Both totals reconcile exactly against the last measured run of the previous branch (7695 openCypher /
10150 engine): +7 for `OpenCypherStDevEmptyInputTest`, which reached `main` after that branch point,
and +12 for the new class here. Compared per class, no pre-existing test class changed its count, so
the fix moved nothing it should not have. That puts the current `main` baseline at 7702 openCypher /
10157 engine.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5542

### Review cycles

**Cycle 1** - `e173e070` - claude[bot], no blocking findings but one substantive challenge: that
leaving `INSERT` out let a real write execute inside a read-only subquery. The code half of the claim
checked out - `ClauseDispatcher.handleInsert` routes to `visitInsertClause`, which builds a
`CreateClause`, so `INSERT` genuinely is `CREATE` in this engine.

The consequence half did not. Probed directly against the branch:

```
COUNT   { INSERT (m:T {name:'leaked-count'})   RETURN m }  ->  0
EXISTS  { INSERT (m:T {name:'leaked-exists'})  RETURN m }  ->  false
COLLECT { INSERT (m:T {name:'leaked-collect'}) RETURN m }  ->  []
total :T nodes afterwards = 2, unchanged; leaked nodes = 0
```

No write reaches storage. What happens instead is the #5461 failure mode: the body fails downstream
and each expression absorbs it into its neutral value, so the query silently answers `0` instead of
raising. So this was not the data-integrity gap it was reported to be - but `INSERT` was still added,
for the better reason that it converts a silently wrong answer into a proper error and stops
`CLAUSE_KEYWORDS` and `UPDATE_CLAUSE_KEYWORDS` disagreeing about whether `INSERT` writes. The earlier
"it parses on `main` today" objection was weak once it was clear those queries answer `0` today.

Also applied from cycle 1: backslash escaping no longer applies inside backtick-quoted identifiers,
where Cypher escapes a literal backtick by doubling it rather than with a backslash. The review
called this cosmetic and suggested only correcting the comment; the one-character fix is strictly
more correct than the comment, so the behavior was fixed instead. Covered by a test in which the
backslash form would otherwise swallow the closing backtick and hide a genuine trailing `SET`.

Declined, with the reason recorded in code: comments are not skipped by the scan, so an update
keyword inside a `//` or `/* */` comment still trips the guard. That blind spot predates this change
(the old `contains("SET ")` had it too) and is not widened here; fixing it means teaching the scanner
comment syntax, which is beyond this bug.

**Cycle 2** - `de4a9b78` - claude[bot], no blocking findings, two actionable points.

The first was that the PR description still argued for leaving `INSERT` out while the code and the
doc had it in. Correct, and the description was rewritten to present the rejection as the intentional
behavior change it is rather than deny it.

The second was a suspected false positive on a map key separated from its colon by whitespace, e.g.
`{set : 1}`. `matchesKeywordAt` rejects a keyword glued to its colon, but the reviewer noted the
check looks only at the immediately following character. Probed, and it is real - and it is the same
class of defect this PR exists to fix:

```
MATCH (n:T {set : 1}) RETURN n                        -> parses, runs, returns no rows
RETURN COUNT { MATCH (n:T {set : 1}) RETURN n } AS v  -> InvalidClauseComposition
```

The grammar accepts that shape; only the guard rejected it. `containsUpdateClause` now looks past any
whitespace after a matched keyword and treats a following colon as a map key, since no update clause
is ever followed by one. `matchesAnyKeywordAt` was split so the matched keyword's length is available
for that lookahead, and tests cover both the false positive and that a genuine clause with extra
whitespace (`MATCH (n)   SET   n.x = 1`) is still caught.

Noted, no action: `containsUpdateClause` calls `toUpperCase()` without a `Locale`. That matches
`startsWithClauseKeyword` and `injectWhereConditions` beside it, so the Turkish-locale caveat is
engine-wide rather than introduced here; changing one of the three would be the inconsistency.

## Follow-ups

- **Investigated under #5461, not a defect.** The neighbouring
  `subquery.toUpperCase().contains("RETURN ")` test in `parseCountExpression` /
  `parseExistsExpression` is also an imprecise substring match, but it does not produce a wrong
  answer: a `MATCH`-only body executes fine without the synthesized `RETURN`. Left alone.

## Impact

Any read-only `COUNT { }`, `EXISTS { }` or `COLLECT { }` filtering on user data that happens to
contain `SET`, `CREATE`, `DELETE`, `MERGE` or `REMOVE` failed to parse. The guard keeps its full
strength against real update clauses, and gains the `CREATE(` case the trailing-space form missed.
