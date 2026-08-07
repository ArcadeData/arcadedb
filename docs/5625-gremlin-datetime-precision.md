# Issue #5625: Gremlin DATETIME comparison broken for far-future dates

https://github.com/ArcadeData/arcadedb/issues/5625

## Reported symptom

A user filtering edges by a `_validFrom`/`_validTo` DATETIME overlap check via
Gremlin's `where(startKey, P).by().by()` construct got wrong results whenever
one side was a far-future "no expiration" sentinel (`2499-12-31 23:59:59.999`).
Their own isolation probe found `_validFrom <= _validTo` false on ~1/3 of
sampled edges, always with the sentinel on one side. Projecting the property
to a `String` first (`by(__.values('_validFrom').asString())`) worked around
it.

## Root cause

TinkerPop 3.8.x's `Compare.lt/lte/gt/gte` biPredicates route through
`org.apache.tinkerpop.gremlin.util.GremlinValueComparator` - a class ArcadeDB
supplies under the exact same fully-qualified name as TinkerPop's own,
replacing it on the classpath (`gremlin/src/main/java/org/apache/tinkerpop/gremlin/util/GremlinValueComparator.java`).
Its date comparator always normalises both operands to `ChronoUnit.NANOS`
precision via `DateUtils.dateTimeToTimestamp(value, ChronoUnit.NANOS)`,
regardless of the property's actual stored precision.

That NANOS conversion multiplies epoch-seconds by one billion. For any date
beyond roughly the year 2262 the multiplication already saturates to
`Long.MAX_VALUE` (via `TimeUnit.NANOSECONDS.convert`, which is documented as
saturating). Until this fix, every date/time branch in
`DateUtils.dateTimeToTimestamp` except `OffsetDateTime` then added the
sub-second nanosecond fraction on top of that already-saturated value with no
overflow guard - overflowing a SECOND time and silently wrapping around to a
large NEGATIVE number. That inverted chronological order: the far-future
sentinel compared as LESS than an ordinary near-present date wherever the
NANOS-precision timestamp drove the comparison.

`LocalDate`'s NANOS/MICROS branches had an even more aggressive variant of the
same bug: a raw `epochMillis * 1_000_000_000L` multiplication that overflows
for almost any real-world date, not just far-future ones.

Interestingly, `OffsetDateTime`'s branch already carried an overflow guard
(presumably added for an earlier, unfiled issue), but the fix was never
applied to the sibling `LocalDateTime`, `ZonedDateTime`, `Instant`, and
`LocalDate` branches - a "dual-path" gap where one type was fixed and its
siblings were not.

## Fix

`engine/src/main/java/com/arcadedb/utility/DateUtils.java`:
- Extracted the existing `OffsetDateTime` overflow guard into a shared
  `addNanosClampingOverflow(long, int)` helper and applied it to
  `LocalDateTime`, `ZonedDateTime`, and `Instant`'s NANOS branches (in
  addition to refactoring `OffsetDateTime` to use it too).
- Replaced `LocalDate`'s raw `* 1_000_000L` / `* 1_000_000_000L`
  multiplications with `TimeUnit.convert()`, which saturates instead of
  silently wrapping.

This is the single, shared root of the comparison for every consumer of
`DateUtils.dateTimeToTimestamp` - `GremlinValueComparator` (Gremlin
`P.lt/lte/gt/gte` and `order()`), `BinaryComparator` (SQL/index comparisons
of `DATETIME_NANOS` values), and `MathExpression` - so fixing it here fixes
the bug for all of them at once, not just the reported Gremlin path.

## Tests

- `engine/src/test/java/com/arcadedb/utility/DateUtilsTest.java`: added 5
  tests asserting `dateTimeToTimestamp(farFutureDate, ChronoUnit.NANOS)`
  clamps to `Long.MAX_VALUE` and stays greater than a near-present date's
  NANOS timestamp, for `LocalDateTime`, `ZonedDateTime`, `Instant`,
  `OffsetDateTime`, and `LocalDate`. 4 of the 5 fail without the fix (the
  `OffsetDateTime` one already passed, since its guard pre-existed).
- `gremlin/src/test/java/com/arcadedb/gremlin/GremlinDateTimeFarFutureComparisonTest.java`:
  a full end-to-end reproduction using a real `ArcadeGraph`, an edge type
  with `_validFrom`/`_validTo` DATETIME properties, and the exact
  `where(startKey, P).by().by()` shape from the issue (plus a `project()`
  variant and an `order()` regression). All 4 tests fail against the
  pre-fix code with the exact wrong counts (1 instead of 2) reported in the
  issue, and pass with the fix.

Verified both suites fail on a `git stash` of the fix and pass with it
restored, per the "prove a test can fail" testing discipline.

## Commands run

```
mvn -pl engine,gremlin -am install -DskipTests
mvn -pl engine test -Dtest=DateUtilsTest
mvn -pl gremlin install -DskipTests   # produces the shaded/tests artifacts gremlin-it consumes
mvn -pl gremlin-it test -Dtest=GremlinDateTimeFarFutureComparisonTest
mvn -pl engine test -Dtest='DateUtils*,BinaryComparator*,Type*'   # no regressions
mvn -pl gremlin-it test   # full gremlin-it suite, no regressions
```
