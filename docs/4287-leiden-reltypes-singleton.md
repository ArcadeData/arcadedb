# Fix #4287 — algo.leiden() relTypes parameter produces singleton communities

## Issue

`algo.leiden(relTypes)` returns every node as its own singleton community when passed a
comma- or pipe-separated relTypes string. Calling without arguments works correctly.

Same root-cause bug affects all `algo.*` procedures (they all call
`AbstractAlgoProcedure.extractRelTypes()`), and `AbstractPathProcedure.extractRelTypes()`
handles `|` only, not `,`.

## Root Cause

`AbstractAlgoProcedure.extractRelTypes(Object)` wraps the entire string in a
single-element array instead of splitting:

```java
case String s -> new String[]{s};
```

So `"BELONGS_TO,PREREQUISITE_FOR"` becomes `["BELONGS_TO,PREREQUISITE_FOR"]` — a
single type name that no edge has, causing every node to appear isolated (no
edges match) and be placed in its own community.

`AbstractPathProcedure.extractRelTypes()` handles `|` but not `,`.

## Fix

`AbstractAlgoProcedure.extractRelTypes()` — split on `[,|]`, trim whitespace, drop
empty tokens, return `null` for an empty input string.

`AbstractPathProcedure.extractRelTypes()` — extend the pipe-split branch to also
handle comma separators.

Both fixes are a single-method change in the two base classes; all derived
procedures inherit the corrected behaviour automatically.

## Verification

- Add regression tests to `AlgoLeidenTest`:
  - `leidenWithCommaRelTypes` — `algo.leiden('KNOWS,KNOWS2')` on a graph with two
    edge types finds multi-node communities (not all singletons).
  - `leidenWithPipeRelTypes` — same with `'KNOWS|KNOWS2'` separator.
- Compile engine module.
- Run `AlgoLeidenTest`.

## Changes Made

- `engine/src/main/java/.../procedures/algo/AbstractAlgoProcedure.java`
  — `extractRelTypes()` now splits on comma/pipe and trims tokens.
- `engine/src/main/java/.../procedures/path/AbstractPathProcedure.java`
  — `extractRelTypes()` extended to also split on comma.
- `engine/src/test/java/.../procedures/algo/AlgoLeidenTest.java`
  — Two new regression tests for comma and pipe relTypes separators.

## PR

https://github.com/ArcadeData/arcadedb/pull/4360 (PR #4360)

Commit: `9353de88fc20a5b904cc026654853a1c04bf0d71`

## Review Cycles

### Cycle 1

- Head SHA: `9353de88` (fix(#4287): split comma/pipe-separated relTypes string in extractRelTypes)
- gemini-code-assist: COMMENTED (2 medium-priority inline suggestions)
  1. `AbstractAlgoProcedure.splitRelTypeString()` - trim source string first for consistency with split-token trimming
  2. `AbstractPathProcedure.extractRelTypes()` - same trimming + return `null` instead of `String[0]` for delimiter-only input
- Both suggestions applied. Follow-up commit: `79f9dd0c3` (address review: trim source string before split for consistency)

### Cycle 2

- Head SHA: `79f9dd0c3` (address review: trim source string before split for consistency)
- Skipped: gemini does not re-review follow-up pushes on this repository (see memory reference_arcadedb_repo_bot_reviewers)

## Final State: max-cycles-reached (gemini no re-review) - effectively clean after cycle 1
