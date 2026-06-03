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

Extended the predicate in `cartesianProduct()` to also match arrays, and instantiate an
`IterableObjectArray` wrapper (already in the utility package) when the value is not `Iterable`:

```java
// Before
if (value instanceof Iterable<?> iterable && !(value instanceof Identifiable)) {
    for (final Object elemInKey : iterable) {

// After
if (!(value instanceof Identifiable) && (value instanceof Iterable<?> || (value != null && value.getClass().isArray()))) {
    final Iterable<?> iterable = value instanceof Iterable<?> iter ? iter : new IterableObjectArray<>(value);
    for (final Object elemInKey : iterable) {
```

## Files Changed

- `engine/src/main/java/com/arcadedb/query/sql/executor/FetchFromIndexStep.java` - fix
- `engine/src/test/java/com/arcadedb/index/InParamIndexExpansionTest.java` - regression tests

## Test Results

New regression test `InParamIndexExpansionTest` (9 test methods):
- `literalInListUsesIndexAndReturnsRows` - passes before and after fix (baseline)
- `namedListParamInWithIndexReturnsRows` - passes before and after fix (`List<Long>` is Iterable)
- `namedPrimitiveArrayParamInWithIndexReturnsRows` - **failed before fix, passes after**
- `namedObjectArrayParamInWithIndexReturnsRows` - **failed before fix, passes after**
- `positionalListParamInWithIndexReturnsRows` - passes (List is Iterable in embedded mode)
- `namedListParamInExplainUsesIndex` - confirms index is used
- `namedListParamInWithoutIndexReturnsRows` - control: non-indexed path works
- `namedListParamInWithIndexReturnsPartialRows` - partial match works
- `namedListParamInWithIndexReturnsSingleRow` - single-element list works

Broader suite: 314 index and SQL query tests, 0 failures, 0 errors.
