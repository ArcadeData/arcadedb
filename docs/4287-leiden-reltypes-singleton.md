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
