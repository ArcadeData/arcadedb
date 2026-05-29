# Issue #4389 - SQL expand() projection ignores AS alias

## Problem

`SELECT expand([1,2,3,4]) AS test` returns results with property `"value"` instead of `"test"`.

The `AS` alias on an `expand()` call is silently dropped when plain (non-document) values are expanded.

## Root Cause

In `ProjectionItem.getExpandContent()`, the alias is not preserved when extracting the expand content.
More critically, in `ExpandStep.fetchNext()`, for plain values (not `Identifiable`, `Result`, `Iterator`, or `Iterable`),
the property name is hardcoded as `"value"` regardless of any alias:

```java
} else {
    nextElement = new ResultInternal(context.getDatabase());
    ((ResultInternal) nextElement).setProperty("value", nextElementObj);
}
```

## Fix

1. Add `expandAlias` field to `QueryPlanningInfo` to carry the alias from the expand projection item.
2. In `SelectExecutionPlanner.optimizeQuery()`, when detecting an expand projection, capture the alias
   from the original `ProjectionItem` and store it in `info.expandAlias`.
3. Pass the alias to `ExpandStep` constructor.
4. In `ExpandStep.fetchNext()`, use the alias instead of hardcoded `"value"` for plain values.
5. Backward compatibility: when no alias is given, falls back to `"value"` as before.

## Status

- [x] Regression tests written (3 new tests in cycle 1)
- [x] Implementation complete
- [x] Tests pass (20 total, all pass)
- [x] Review cycles complete

## PR

https://github.com/ArcadeData/arcadedb/pull/4411

## Review Cycles

### Cycle 1 (HEAD: b9f7dba)

**Gemini COMMENTED (inline):**
- Chain constructors in ExpandStep to avoid duplicating super(context)

**Claude COMMENTED (issue comment at 15:13:38):**
- Fully qualified `java.util.List.of` in test violates CLAUDE.md (minor)
- The no-arg constructor is unreachable (minor)
- Remove block comments before test methods (nit)
- Suggestion: test actual values not just property names (optional)

**Actions taken:**
- Chained single-arg constructor via `this(context, null)` - satisfies both bots
- Replaced `java.util.List.of()` with `List.of()` using existing import
- Removed block comments before the three new test methods
- Committed as `8a334dd49`

### Cycle 2 (HEAD: 8a334dd)

**Gemini COMMENTED (inline):**
- Same constructor chaining suggestion - already applied (stale)

**Claude COMMENTED (issue comment at 15:26:34):**
- `alias` field should be `final` per CLAUDE.md (actionable)
- `expandAlias` in `QueryPlanningInfo` could be `final` (minor/observation)
- Add test for document-expand with alias to confirm alias does not affect document path
- Minor comment style observation

**Actions taken:**
- Made `alias` field `final` in `ExpandStep` (using constructor chaining so single-arg always assigns it)
- Added `expandDocumentListWithAliasPreservesDocumentProperties` test
- Committed as `bd01650bd`

### Cycle 3 (HEAD: bd01650)

**Gemini COMMENTED (inline):**
- Same constructor chaining suggestion - already applied (stale, repetitive)

**Claude Code Review CI workflow:**
- `claude-review` job ran and completed with success (no new issues posted)
- No new issue comment - clean approval

**Outcome:** Clean approval - no actionable items from either bot on this HEAD.

## Final State

`clean-approval` - 3 cycles run, all review feedback addressed.

Files changed:
- `engine/src/main/java/com/arcadedb/query/sql/executor/ExpandStep.java` - alias field + constructor chaining
- `engine/src/main/java/com/arcadedb/query/sql/executor/QueryPlanningInfo.java` - expandAlias field
- `engine/src/main/java/com/arcadedb/query/sql/executor/SelectExecutionPlanner.java` - capture alias in optimizeQuery
- `engine/src/test/java/com/arcadedb/query/sql/executor/ExpandStepTest.java` - 4 new regression tests
