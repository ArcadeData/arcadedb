# Issue #6469: Type.convert Date -> ZonedDateTime/Instant copy-paste bug, missing Number branch

## Root cause

`com.arcadedb.schema.Type.convert()` has one branch per configured `DATE_TIME_IMPLEMENTATION` target class
(`LocalDateTime`, `ZonedDateTime`, `Instant`). The `ZonedDateTime` and `Instant` branches' `java.util.Date` arm
each passed the wrong target class to `DateUtils.dateTime(...)`:

- `ZonedDateTime` target's `Date` arm passed `LocalDateTime.class` instead of `ZonedDateTime.class`.
- `Instant` target's `Date` arm passed `LocalDateTime.class` instead of `Instant.class`.

Both `Calendar` arms right below them already used the correct target class, confirming the copy-paste-slip
diagnosis in the issue. Neither the `ZonedDateTime` nor the `Instant` target had a `Number` branch at all
(the `LocalDateTime` target does), so an epoch-millis `Number` set on such a property fell through `convert`
unconverted.

## Affected component

`engine/src/main/java/com/arcadedb/schema/Type.java`, the `convert()` method's `ZonedDateTime.class` and
`Instant.class` target branches.

## Expected vs actual behavior

- Expected: converting a `java.util.Date` into a `ZonedDateTime`-backed (or `Instant`-backed) DATETIME
  property returns a value of that runtime type, and a `Number` (epoch millis) converts the same way a
  `Date`/`Calendar` does.
- Actual (before fix): the `Date` conversion returned a `LocalDateTime` instance regardless of the configured
  target class, breaking `instanceof`/`equals` checks against the configured type. A `Number` value fell
  through `convert` unconverted for both targets.

## Fix

In both branches, added a `Number` arm (mirroring the `LocalDateTime` target's `Number` handling, routed
through `DateUtils.dateTime(..., ChronoUnit.MILLIS, <target>, ...)` to match how the existing `Date`/`Calendar`
arms already treat their input as epoch millis) and corrected the `Date` arm's target class:

- `ZonedDateTime` target: `Date` arm now passes `ZonedDateTime.class`; added `Number` arm.
- `Instant` target: `Date` arm now passes `Instant.class`; added `Number` arm.

## Tests added (TDD)

`engine/src/test/java/com/arcadedb/schema/TypeTest.java` - added 4 new regression tests (existing tests were
not modified, per project constraints):

- `convertToZonedDateTimeFromDateReturnsZonedDateTime` - asserts the result `isInstanceOf(ZonedDateTime.class)`
  and that the instant is preserved.
- `convertToZonedDateTimeFromNumber` - asserts a `Number` (epoch millis) converts to a `ZonedDateTime` with
  the same instant.
- `convertToInstantFromDateReturnsInstant` - asserts the result `isInstanceOf(Instant.class)` and that the
  instant is preserved.
- `convertToInstantFromNumber` - asserts a `Number` (epoch millis) converts to an `Instant` with the same
  instant.

All 4 tests were confirmed to fail against the pre-fix code (proving the bug), then confirmed to pass after
the fix.

## Test results

- `TypeTest`: 127/127 passed (including the 4 new regression tests).
- `DateUtilsTest`: 15/15 passed.
- `OffsetDateTimeStorageTest`: 2/2 passed.
- `TypeConversionTest`: 12/12 passed.
- `JsonSerializerTest`: 15/15 passed.
- Full `com.arcadedb.schema.**` + `com.arcadedb.utility.**` packages: 859/859 passed.

No regressions found.

## Impact analysis

Low-medium severity, as noted in the issue: the underlying instant was always preserved (both the buggy and
fixed paths compute UTC millis identically), so this was an in-memory type-inconsistency bug, not an on-disk
data-loss bug - after persist and reload the value re-materializes with the correct configured type regardless
of this fix. It only affects installations that explicitly configure `DATE_TIME_IMPLEMENTATION` to
`ZonedDateTime` or `Instant` (the default is `java.util.Date`), and only the in-memory value between a
`java.util.Date`/`Number` being set and the record being flushed/reloaded.
