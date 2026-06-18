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

## Release-note callout

The `Duration` serialization formula was corrected in both `put(String, Object)` and
`objectToElement()`. The old `"%d.%d".formatted(toSeconds(), toNanosPart())` form was lossy
(e.g. a 5.000000005s duration serialized as `5.5`, and negative durations as `-6.999999995`).
The new arithmetic form (`toSeconds() + toNanosPart()/1e9`) is correct. Applications that
previously stored a `Duration` via JSON and parsed it back numerically will see a different
(now correct) value. This is a behavioral change worth a release note.

The `LocalDate` epoch-millis path in `put()` was also unified with `objectToElement()` to use
the DST-correct `atStartOfDay(ZoneId.systemDefault())` form, so the two paths no longer diverge
on dates that fall in a different DST period than the moment of serialization.

## Known limitation (out of scope)

`Duration` is serialized as a `double` of seconds. For very large second counts (> ~10^7) the
nanosecond part can be lost to floating-point rounding. Preserving sub-nanosecond precision for
large durations would require a different wire format (e.g. total nanoseconds as a `long`) and is
out of scope for this fix.

## Review cycles

- **a972ec9** (initial): added temporal/Enum cases to `objectToElement()`, regression test.
  gemini-code-assist flagged 3 high-priority issues (LocalDate DST offset, TemporalAccessor null
  NPE, Duration precision/sign).
- **318c3ca**: applied all 3 gemini fixes + 2 edge-case tests. claude bot review: align Duration
  between `put()` and array path, add `Class<?>` case, replace tautological assertions with concrete
  values, add Enum coverage; declined removing this docs file (established convention).
- **090d4eb**: fixed `put()` Duration to match the array path (kills latent bug), added `Class<?>`
  case to `objectToElement()`, concrete value assertions, Enum/Class tests, cross-path consistency
  test. claude bot review: unify `LocalDate` path between `put()` and array path (the remaining
  divergence), noted Duration-double precision limit and breaking-change release note.
- **(this commit)**: unified `put()` LocalDate on the DST-correct form, removed now-unused `Instant`
  import, added `localDateConsistentBetweenPutAndArrayPaths` test, documented the release-note
  callout and known limitation above.

- **e8b2d8e** (final): post-review poll returned only a non-actionable `test` stub comment from
  the claude bot; no further substantive feedback. gemini-code-assist (being sunset 2026-06-18)
  did not re-review after cycle 1; all its cycle-1 findings were addressed. All actionable items
  across all cycles are resolved.

## PR

https://github.com/ArcadeData/arcadedb/pull/4655

## Final state

max-cycles-reached (4 cycles) - all actionable bot feedback resolved; remaining notes are
documented out-of-scope/pre-existing items. Working tree clean, all tests passing.

## Status

Implementation complete, all tests passing.
