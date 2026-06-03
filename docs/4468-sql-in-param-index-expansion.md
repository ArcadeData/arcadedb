# Issue #4468 - SQL `IN :param` with a collection parameter returns no rows when an index is used

## Summary

SQL queries using `IN :param` (named parameter) or `IN ?` (positional parameter) with a collection
value return no rows when the field has an index and the index path is chosen by the planner.

The same collection works on the non-indexed path (`WHERE (field + 0) IN :param`) and the literal
form (`WHERE field IN [1, 2, 3]`) works without restriction.

## Root Cause

`PostCommandHandler.PostCommandHandler` uses `json.toMap(true)` to deserialize the HTTP request body.
The `true` flag enables an optimized path that converts JSON numeric arrays (`[1, 2, 3]`) to
primitive arrays (`long[]`, `double[]`) rather than `List<Long>` to reduce boxing allocations.

When a parameter value like `codes = [1, 2, 3]` arrives at the server, the deserialized map entry
is `"codes" -> long[]`.

`FetchFromIndexStep.cartesianProduct()` is responsible for expanding a multi-value expression into
one index lookup per element. Before this fix its check was:

```java
if (value instanceof Iterable<?> iterable && !(value instanceof Identifiable)) {
```

Primitive arrays (`long[]`, `int[]`, `Object[]`) are **not** `Iterable<?>` in Java.
The condition was false, the `else` branch ran, and the whole array was passed as a single index
key - matching nothing.

The non-indexed evaluator uses `MultiValue.getMultiValueIterable()` which handles arrays correctly,
so `WHERE (field + 0) IN :param` worked fine.

## Fix

`engine/src/main/java/com/arcadedb/query/sql/executor/FetchFromIndexStep.java`

Replaced the `Iterable<?>`-only predicate in `cartesianProduct()` with the existing `MultiValue`
utility, which already detects and iterates every array type (`long[]`, `int[]`, `double[]`,
`Object[]`) as well as `Collection`/`Iterable`. This is the same utility the sibling
`processInCondition()` path uses (lines 258-261 of the same file), so both multi-value detection
sites now share one code path. `MultiValue.isMultiValue(null)` is internally null-safe, so no
explicit null guard is needed.

```java
// Before
if (value instanceof Iterable<?> iterable && !(value instanceof Identifiable)) {
    for (final Object elemInKey : iterable) {

// After
if (!(value instanceof Identifiable) && MultiValue.isMultiValue(value)) {
    for (final Object elemInKey : MultiValue.getMultiValueIterable(value)) {
```

## Files Changed

- `engine/src/main/java/com/arcadedb/query/sql/executor/FetchFromIndexStep.java` - fix
- `engine/src/test/java/com/arcadedb/index/InParamIndexExpansionTest.java` - regression tests

## Test Results

New regression test `InParamIndexExpansionTest` (11 test methods):
- `literalInListUsesIndexAndReturnsRows` - passes before and after fix (baseline)
- `namedListParamInWithIndexReturnsRows` - passes before and after fix (`List<Long>` is Iterable)
- `namedPrimitiveLongArrayParamInWithIndexReturnsRows` - **failed before fix, passes after**
- `namedPrimitiveIntArrayParamInWithIndexReturnsRows` - **failed before fix, passes after**
- `namedPrimitiveDoubleArrayParamInWithIndexReturnsRows` - **failed before fix, passes after** (double coerces to INTEGER key)
- `namedObjectArrayParamInWithIndexReturnsRows` - **failed before fix, passes after**
- `positionalListParamInWithIndexReturnsRows` - passes (List is Iterable in embedded mode)
- `namedListParamInExplainUsesIndex` - confirms index is used
- `namedListParamInWithoutIndexReturnsRows` - control: non-indexed path works
- `namedListParamInWithIndexReturnsPartialRows` - partial match works
- `namedListParamInWithIndexReturnsSingleRow` - single-element list works

Broader suite: index (embedded list, list-by-item, composite, map) and SQL query tests pass with
0 failures, 0 errors.

## Review cycles

### Cycle 1 - `b940f325`

- **gemini-code-assist** (1 comment): hoist the `value != null` null check to the front of the
  `cartesianProduct` condition. Subsumed by the cycle-1 change below - switching to
  `MultiValue.isMultiValue` (internally null-safe) removed the need for any explicit null guard.
- **claude** (review comment): suggested reusing the already-imported `MultiValue` utility instead
  of `IterableObjectArray` for consistency with `processInCondition`; add `int[]`/`double[]` test
  cases; remove issue-reference comments from test method bodies; remove the docs file.
  - Applied: switched to `MultiValue.isMultiValue` + `getMultiValueIterable` (verified safe across
    embedded-list, list-by-item, map, and composite index suites); added `int[]` and `double[]`
    test cases; removed issue-reference comments and em-dashes from test method bodies.
  - Declined: removing this docs file. The `docs/<issue>-<name>.md` tracking doc is an established,
    committed convention (38 such files already in the repo, e.g. `docs/4337-*`, `docs/4364-*`) and
    is produced by the resolve-issue workflow. The claim that the convention does not exist is not
    accurate.

### Cycle 2 - `342fd2b2`

- **claude** (Approved with minor suggestions):
  - Noted that switching to `MultiValue` quietly broadens behavior for `Map` parameters (expanded by
    `map.values()`), consistent with `processInCondition` but previously not reached. Added a concise
    behavioral comment above the condition explaining the multi-value expansion. Did not add a `Map`
    test, since that would lock incidental Map-expansion into a tested contract; the fix targets the
    array/collection shapes produced by JSON parameter deserialization.
  - Declined: trimming the class Javadoc per a cited "never write multi-paragraph docstrings" rule -
    that rule is not present in CLAUDE.md, and the Javadoc documents a non-obvious root cause.
  - Declined: removing the "Review cycles" section from this doc. 15+ committed tracking docs in the
    repo (e.g. `docs/4274-*`, `docs/4331-*`) contain exactly this section; it is a convention.
