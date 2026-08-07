# Issue #5801: `reverse()` silently returns null for unsupported non-null scalar inputs

## Problem

`reverse()` in the OpenCypher engine (`com.arcadedb.function.misc.ReverseFunction`) returns
`null` when handed a scalar value outside its input domain (e.g. `reverse(5)`, `reverse(true)`).
This is indistinguishable from legal null propagation (`reverse(null)` also returns `null`),
so a type error in the client's query silently produces a wrong-but-plausible result instead of
a client-facing error.

Neo4j documents `reverse()`'s input as `STRING | LIST` and raises a type error for anything else
(except explicit `null`, which still propagates).

This is the same class of defect as #5477 (`size()`) and #5476 (`head()`/`last()`/`tail()`), both
already fixed by raising `CommandSemanticException` (HTTP 400) via
`CypherFunctionHelper.typeMismatch()`. `isEmpty()` (sibling of #5477) got the identical runtime-only
treatment without an additional static (parse-time) check, since its type is not always known until
the query runs. This fix mirrors `isEmpty()`'s precedent for `reverse()`.

## Fix

`ReverseFunction.execute()`: when the argument is not `null`, not a `String`, and not resolvable via
`MultiValue.getMultiValueAsList()` (covers `List`/`Collection`/array), throw
`CypherFunctionHelper.typeMismatch("reverse", "a STRING or a LIST<ANY>", args[0])` instead of
returning `null`.

## Tests

New file `engine/src/test/java/com/arcadedb/query/opencypher/CypherReverseArgumentIssue5801Test.java`:

- `reverse(5)`, `reverse(3.14)`, `reverse(true)` each throw `CommandSemanticException` mentioning
  `reverse()` and the offending Cypher type name.
- `reverse(null)` still answers `null` (no regression in null propagation).
- `reverse('abc')` and `reverse([1,2,3])` still work as before (no regression on the supported domain).
- A node argument (`reverse(n)`) is also a type error.

## Status

Implemented, tests written and passing. See PR for review history.
