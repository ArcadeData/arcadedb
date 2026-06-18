# Fix #4654: LocalDateTime inside arrays fails JSON serialization

## Summary

`JSONObject.objectToElement()` is the static method used when placing objects into `JSONArray`.
It handles common types (String, Number, Boolean, JSONObject, JSONArray, Collection, Map,
Document, Identifiable) but was missing branches for temporal types:
`Date`, `LocalDate`, `TemporalAccessor` (covers LocalDateTime, ZonedDateTime, Instant, etc.),
and `Duration`.

Because `objectToElement()` is `static` it has no access to configured date/datetime formats.
Temporal values are therefore serialized as epoch-millisecond timestamps (the default when no
format string is configured), which matches the existing behavior in `JSONObject.put(String,
Object)` when `dateFormatAsString == null`.

The default `throw` in the old switch statement has also been replaced with a `toString()`
fallback, consistent with the generic case in `put(String, Object)`.

## Root Cause

`JSONObject.objectToElement()` switch statement lacked cases for:
- `Date`
- `LocalDate`
- `TemporalAccessor` (LocalDateTime, ZonedDateTime, Instant, OffsetDateTime, etc.)
- `Duration`
- `Enum`

`JSONArray.put(Object)`, `new JSONArray(Collection)`, and `new JSONArray(Object[])` all route
through `objectToElement()`, so any temporal value nested in an array hit the default `throw`.

## Fix

Added the missing branches to `JSONObject.objectToElement()` in:
`engine/src/main/java/com/arcadedb/serializer/json/JSONObject.java`

## Verification

Regression test added to:
`engine/src/test/java/com/arcadedb/serializer/json/JSONTest.java`

Test `temporalTypesInArrays` covers:
- `LocalDateTime` in a `List`
- `LocalDate` in a `List`
- `Date` in a `List`
- `LocalDateTime` in `JSONArray.put(Object)`
- `JSONArray(Collection)` constructor with temporal elements
- `JSONArray(Object[])` constructor with temporal elements
- Mixed temporal types in one array

Run: `mvn test -pl engine -Dtest=JSONTest`

## Status

Implementation complete, tests passing.
