# Issue #6574: GQL standalone FILTER clause parses but has zero effect

## Root cause

`Cypher25Parser.g4` defines a standalone `filterClause` (`FILTER WHERE? expression`), part of
ISO/IEC 39075:2024 GQL, in the same family as the already-supported `forUnwindClause`
(`FOR ... IN ...`, a synonym for `UNWIND`). The grammar rule parses successfully, but:

- `ClauseDispatcher` registered a handler for every other clause type except `filterClause`.
- `dispatch()` looped its handler map, found no match, and silently fell through (a comment
  said "this should not happen with valid grammar").
- `CypherASTBuilder` had no `visitFilterClause`.

Net effect: `FILTER (<any expression>)` was accepted syntactically and completely ignored,
regardless of whether the predicate was true or false - a silent-wrong-results bug, not a
hard failure.

## Fix

Followed the issue's suggested approach, mirroring the precedent `handleOrderBySkipLimit`
already sets for rewriting a GQL-only clause into the existing WHERE/WITH machinery:

- `CypherASTBuilder.visitFilterClause(FilterClauseContext)` parses `FILTER WHERE? expression`
  into a `WhereClause`, reusing the same boolean-expression parsing and AST-rewriter
  simplification `visitWhereClause` uses.
- `ClauseDispatcher` registers a `filterClause` handler that wraps the resulting `WhereClause`
  in an implicit `WITH * WHERE <predicate>`, so the filter is applied at the correct position
  in `clausesInOrder` - the same technique `handleOrderBySkipLimit` uses for a standalone
  `ORDER BY` / `SKIP` / `LIMIT` clause (issue #3950).

This required no grammar change (the `filterClause` rule already existed) and no changes to
the optimizer or legacy execution paths, since both consume the shared `WithClause` AST node
that `ClauseDispatcher` now produces for `FILTER`.

## Files changed

- `engine/src/main/java/com/arcadedb/query/opencypher/parser/CypherASTBuilder.java`
  - added `visitFilterClause`
- `engine/src/main/java/com/arcadedb/query/opencypher/parser/ClauseDispatcher.java`
  - registered `filterClause` -> `handleFilter`
  - added `handleFilter`
- `engine/src/test/java/com/arcadedb/query/opencypher/OpenCypherCypher25ClausesTest.java`
  - added a "FILTER clause (issue #6574)" section with 5 regression tests

## Tests

New tests in `OpenCypherCypher25ClausesTest`:

- `filterFalsePredicateProducesNoRows` - the issue's exact reproduction
  (`FILTER (0 IN range(12, 1, -1)) RETURN 1 AS x`), asserts zero rows.
- `filterTruePredicateProducesRow` - the true-predicate counterpart, asserts the row passes.
- `filterWithWhereKeywordFalsePredicateProducesNoRows` / `...TrueProducesRow` - covers the
  grammar's optional `WHERE` keyword form (`FILTER WHERE <expr>`).
- `filterAfterMatchRestrictsRowsByProperty` - proves FILTER actually restricts rows from a
  preceding `MATCH` by a property predicate, not just a constant.

Before the fix, 3 of the 5 new tests failed (the two "true predicate" tests pass either way
since a no-op also lets rows through, which is exactly the bug: no observable difference
between true and false).

## Verification

- `mvn -pl engine -am test -Dtest=OpenCypherCypher25ClausesTest` - 45/45 pass (all new FILTER
  tests included).
- Full `com.arcadedb.query.opencypher.**` package (excluding `benchmark`, `slow`, `vector`
  lanes) - 8907/8907 pass, 98 skipped, 0 failures, 0 errors. No regressions.
- `mvn -pl engine -am compile` - clean compile.
