# Design: Seal the JavaCC SQL Parser Escape Hatches

**Date**: 2026-03-28
**Status**: Approved

## Problem

The legacy JavaCC SQL parser is no longer the runtime parser — ArcadeDB migrated to ANTLR4. However, several escape hatches allow code to bypass `SQLAntlrParser` and invoke `SqlParser` directly:

1. `GlobalConfiguration.SQL_PARSER_IMPLEMENTATION = "javacc"` — a config option that switches the entire statement/script parsing pipeline back to JavaCC
2. `AbstractProperty.getDefaultValueExpression()` — hardwires `new SqlParser(...).ParseExpression()`
3. `SQLFunctionEval.execute()` — hardwires `new SqlParser(...).ParseCondition()`
4. 11 test files — directly instantiate `SqlParser` instead of going through `SQLAntlrParser`

This caused a concrete bug: parser tests were silently testing the wrong parser for an unknown period, hiding grammar gaps (see #3738).

## Goal

Make it impossible to accidentally invoke the JavaCC parser without deleting it. Keep the generated source files as dead code for now.

## Out of Scope

- Deleting the JavaCC source files (follow-up task)
- Removing `SqlParserTreeConstants` constants still imported by `SelectStatement`/`MatchStatement`
- Migrating `FunctionReferenceGenerator` (dev tool, not runtime)

---

## Part 1 — Remove the config switch

**Files**: `StatementCache.java`, `SQLScriptQueryEngine.java`, `GlobalConfiguration.java`

- Mark `GlobalConfiguration.SQL_PARSER_IMPLEMENTATION` as `@Deprecated` (or remove it)
- In `StatementCache`: delete `parseWithJavaCC()` and `useAntlrParser()`; always invoke `SQLAntlrParser`
- In `SQLScriptQueryEngine.parseScript()`: delete the `if ("javacc"...)` branch; always use ANTLR

## Part 2 — Migrate the two direct production usages

Both replacement methods already exist on `SQLAntlrParser`:

| File | Old call | New call |
|---|---|---|
| `AbstractProperty.getDefaultValueExpression()` | `new SqlParser(db, is).ParseExpression()` | `new SQLAntlrParser(db).parseExpression(text)` |
| `SQLFunctionEval.execute()` | `new SqlParser(db, is).ParseCondition()` | `new SQLAntlrParser(db).parseCondition(text)` |

Exception type changes from `ParseException` (JavaCC) to `CommandSQLParsingException` (ANTLR) — both `RuntimeException`, callers unchanged.

## Part 3 — Migrate the 11 test files

Files to migrate:

- `SelectStatementTest.java` (720 lines — highest risk)
- `UpdateStatementTest.java`
- `InsertStatementTest.java`
- `DeleteStatementTest.java`
- `MatchStatementTest.java`
- `CreateVertexStatementTest.java`
- `CreateEdgeStatementTest.java`
- `TraverseStatementTest.java`
- `ProjectionTest.java`
- `BatchScriptTest.java`
- `OperationTypeTest.java`

Migration approach per file:
- Extend `AbstractParserTest` where possible (free `checkRightSyntax`/`checkWrongSyntax`)
- Replace inline `new SqlParser(null, is).Parse()` with `new SQLAntlrParser(null).parse(query)`
- Run each file after migration and fix any surfaced grammar/AST gaps

## Part 4 — What stays and verification

**Intentional dead code** (untouched):
- All JavaCC-generated source files (`SqlParser.java`, `SqlParserTokenManager.java`, etc.)
- `SqlParserTreeConstants` constant imports in `SelectStatement`/`MatchStatement`

**Verification steps** (in order):
1. After Part 1: compile; confirm no JavaCC references in `StatementCache`/`SQLScriptQueryEngine`
2. After Part 2: run property default value tests and `eval()` function tests
3. After Part 3: run each migrated `*StatementTest` individually; fix grammar gaps; finish with full `mvn test -Dtest="*StatementTest*,*ParserTest*"` pass
4. Final: `grep -r "new SqlParser" src/main src/test` returns zero results
