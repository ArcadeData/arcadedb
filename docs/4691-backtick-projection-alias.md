# Issue #4691 - Backtick-quoted identifier in SELECT projection not resolved as column reference

## Summary

Backtick-quoting an identifier in a SELECT projection position caused the output column key to retain the literal backticks instead of stripping them, and caused GROUP BY values to resolve to `null`.

## Root Cause

`ProjectionItem.getProjectionAlias()` built the default alias for an unaliased projection item as:

```java
result = new Identifier(expression.toString());
```

For a quoted column reference like `` `col1` ``, `expression.toString()` renders the backtick-quoted text. The `Identifier(String)` constructor calls `setStringValue()` which backslash-escapes any backticks in the string - so the stored value became `\`col1\`` (with escape sequences). This became the output key, causing symptom 1 (wrong key) and symptom 2 (GROUP BY null, because the backtick-bearing key didn't match the property `col1`).

The same issue propagated to explicit quoted aliases (`AS \`alias1\``) when the aggregate planner reconstructed projection items for the GROUP BY split, calling `new Expression(item.getProjectionAlias())` and then querying `aggItem.getProjectionAlias()` on the new item - which again used `new Identifier(expression.toString())` and re-injected backticks from the identifier's `toString()` (which adds backticks for `quoted=true` identifiers).

## Fix

Changed the one buggy line in `ProjectionItem.getProjectionAlias()`:

```java
// Before (buggy):
result = new Identifier(expression.toString());

// After:
return expression.getDefaultAlias();
```

`Expression.getDefaultAlias()` already existed and correctly handled this: for a base identifier it navigates to the underlying `Identifier` and calls `getStringValue()` (which returns the bare value without backticks), rather than `toString()` (which adds backticks for quoted identifiers). For non-base-identifier expressions (function calls, multi-hop access, etc.) it falls back to `new Identifier(this.toString())` - identical to the old behavior.

The method also simplified from 7 lines to 4.

## Affected Files

- `engine/src/main/java/com/arcadedb/query/sql/parser/ProjectionItem.java` - one-line fix in `getProjectionAlias()`

## Tests

New regression test class: `engine/src/test/java/com/arcadedb/query/sql/BacktickProjectionAliasTest.java`

Four test cases:
1. `backtickProjectionKeyIsStripped` - `SELECT \`col1\` FROM T` produces key `col1` (not `` `col1` ``)
2. `backtickProjectionGroupByValueIsNotNull` - `SELECT \`col1\`, count(*) AS n ... GROUP BY \`col1\`` produces correct key and non-null values
3. `quotedAliasOnQuotedColumnIsStripped` - `SELECT \`col1\` AS \`alias1\` ... GROUP BY \`col1\`` produces key `alias1` with correct value
4. `unquotedProjectionStillWorks` - regression guard: unquoted `SELECT col1 ... GROUP BY col1` still works

## Test Results

- All 4 new tests: PASS
- 224 related SQL/query tests: PASS (GroupByMixedNumericTypesTest, QueryTest, DDLTest, ProjectionTest, etc.)
