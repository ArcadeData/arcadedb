# #5649 - SQL `abs()` mishandles the sign of a `Duration`

Issue: https://github.com/ArcadeData/arcadedb/issues/5649

## Root cause

`SQLFunctionAbsoluteValue.execute` handled the `Duration` branch by taking the duration apart into its
`toSecondsPart()` / `toNanosPart()` components, testing each for negativity, then recombining the two
magnitudes with `Duration.ofSeconds(Math.abs(seconds), Math.abs(nanos))`.

Both halves are wrong, and for reasons that compound:

- `toSecondsPart()` is the seconds component **within the minute** (`(int) (seconds % 60)`), not the
  duration's total seconds. Recombining it with the nanos part therefore silently drops every whole
  minute of the input.
- `Duration` normalizes a negative value as a **negative seconds field plus a positive nanos
  adjustment**. `toNanosPart()` is consequently never negative, so the `nanos > -1` half of the guard
  is always true, and taking the two parts' magnitudes independently does not reconstruct the
  magnitude of the whole.

## Behaviour measured on `main` before the fix

Probed directly against the pre-fix expression. `truth` is `d.isNegative() ? d.negated() : d`.

| input | pre-fix result | correct | |
|---|---|---|---|
| `PT-0.5S` | `PT1.5S` | `PT0.5S` | wrong |
| `PT-1M` (-60s) | `PT-1M` | `PT1M` | wrong - **returns a negative "absolute value"** |
| `PT-2M` (-120s) | `PT-2M` | `PT2M` | wrong - **returns a negative "absolute value"** |
| `PT-1M-29.5S` | `PT30.5S` | `PT1M29.5S` | wrong - whole minute dropped |
| `PT-0.000000001S` | `PT1.999999999S` | `PT0.000000001S` | wrong |
| `PT-5S` | `PT5S` | `PT5S` | ok |
| `PT-1S` | `PT1S` | `PT1S` | ok |
| `PT0S` | `PT0S` | `PT0S` | ok |
| positive durations | unchanged | unchanged | ok |

So the branch was correct only for negative durations whose magnitude is a whole number of seconds
strictly under one minute. Everything else was either off by up to a minute or, for exact multiples of
a minute, returned the input unchanged - a *negative* result from `abs()`.

### Correction to the issue's own analysis

The issue predicted that for `Duration.ofNanos(-500_000_000)` "the seconds part is `0`, so
`seconds > -1` holds". Measured, `toSecondsPart()` is `-1` for that input, not `0`, so the guard
does **not** hold and the else branch fires. The value is still wrong (`PT1.5S`), just by the
recombination defect rather than the guard defect. The guard defect is real but bites on a different
input class than the issue guessed: exact multiples of a minute (`-60s`, `-120s`), where both parts
are `0` and the negative input is handed straight back.

The issue also understated the blast radius: it framed this as an edge case in `(-1s, 0s)`, when in
fact every negative duration of a minute or more is wrong too.

## Fix

Replace the whole branch with the direct expression of the intent, in a private `absExact(Duration)`
overload sitting next to the existing integral one so both unrepresentable-magnitude cases read the same:

```java
private static Duration absExact(final Duration duration) {
  if (!duration.isNegative())
    return duration;

  try {
    return duration.negated();
  } catch (final ArithmeticException e) {
    throw new ArithmeticErrorException("duration overflow", e);
  }
}
```

`Duration.abs()` (Java 18+) would read slightly better than `negated()`, but `negated()` keeps the
branch buildable on the `java17` branch and the two are equivalent.

### Overflow boundary

`Duration`'s range is *almost* symmetric: negating `Duration.ofSeconds(Long.MIN_VALUE, 0)` overflows,
because the magnitude needs `Long.MAX_VALUE + 1` seconds. `negated()` signals that with a raw
`java.lang.ArithmeticException("Exceeds capacity of Duration")`, which would escape the query as an
HTTP 500 - the exact defect class #5545 and #5647 are about. The value is reachable from SQL via
`duration('-9223372036854775808', 'second')`, so it is guarded and reported as
`ArithmeticErrorException("duration overflow")`, matching how the four integral MIN_VALUEs are already
handled in the same function and answering 400 instead of 500.

Note this is exactly *one* input, not a range: `Duration.ofSeconds(Long.MIN_VALUE, 500_000_000)`
negates fine, since the positive nanos adjustment pulls the magnitude back inside `Long`.

## Changes

- `engine/src/main/java/com/arcadedb/function/sql/math/SQLFunctionAbsoluteValue.java` - the `Duration`
  branch now delegates to a private `absExact(Duration)` overload alongside the existing integral one.
- `engine/src/test/java/com/arcadedb/function/sql/math/SQLFunctionAbsoluteValueTest.java` - 8 new tests
  appended. No existing test was modified or removed.

## Test results

The branch had zero coverage before this change, so the tests were written first and proven to fail
against the unfixed code.

**Before the fix** - `SQLFunctionAbsoluteValueTest`, 39 tests, **5 failures**, each reproducing a
distinct class from the table above:

| test | expected | actual before fix |
|---|---|---|
| `negativeSubSecondDurationDoesNotGainASecond` | `0.5S` | `1.5S` |
| `negativeWholeMinuteDurationIsNotReturnedNegative` | `1M` | `-1M` |
| `negativeDurationLongerThanAMinuteKeepsItsMinutes` | `1M29.5S` | `30.5S` |
| `fromQueryOverNegativeDuration` | `1M30S` | `30S` |
| `durationOverflowIsAnArithmeticError` | throws | no throw |

The remaining 3 new tests (`positiveDurationIsReturnedUnchanged`,
`negativeWholeSecondDurationUnderAMinuteStillWorks`, `zeroDuration`) pass both before and after by
design - they pin the cases the old branch already handled, so the fix is demonstrably a strict
improvement rather than a trade of one failure set for another.

**After the fix**, all exit 0:

| suite | result |
|---|---|
| `SQLFunctionAbsoluteValueTest` | 39/39 |
| `com.arcadedb.function.**` | 1082/1082 |
| `com.arcadedb.query.sql.**` | 2315/2315 (8 skipped) |
| `com.arcadedb.query.opencypher.**` | 7785/7785 (98 skipped) |
| `TypeConversionTest` | 12/12 |

## Impact

Any query calling `abs()` on a negative `Duration` could return a wrong magnitude, and for exact
multiples of a minute a negative one. The result is a plain value that can be persisted by
`UPDATE ... SET`, so this was silent data corruption rather than a display wart. Positive durations and
every other argument type are untouched.

The Cypher-side `AbsFunction` is a separate class and does not have this branch, so it is unaffected.

## Review

PR: https://github.com/ArcadeData/arcadedb/pull/5654

### Cycle 1 - `1586a4545`

`claude[bot]` reviewed and recommended merging. Nothing blocking, and the two points raised under
"minor / optional" both explicitly ask for no change, so no code moved this cycle. Nothing was deferred.

**1. "The unit tests overlap somewhat with `fromQueryOverNegativeDuration`" - no action, and the review
agrees it is desirable.** The overlap is deliberate: the unit tests pin the function's contract per
defect class, the query test pins that the fix is actually reachable through the SQL engine rather than
only through a direct call. For a silent-corruption regression with no prior coverage, losing either
side would leave a real gap.

**2. "The two `absExact` overloads carry different failure semantics - equality guard vs. try/catch" -
no change, flagged for future readers only.** This is inherent to the types. A fixed-width integer has a
single MIN_VALUE known statically, so an equality guard states the boundary exactly and costs nothing on
the hot path. `Duration` has no such constant to compare against, and deriving the boundary by hand
would restate `Duration`'s own normalization rules, which is precisely the reasoning the original bug
got wrong. Letting `negated()` be the authority on what it cannot represent, and translating its
exception, is the safer construction even though it reads less symmetrically.

The review also noted it could not run Maven in its sandbox and relied on the reported suite results.
Those were run locally and are recorded under "Test results" above, each with exit 0.

## Follow-ups

None. Unlike #5647 there is no remaining silent-wrap path here: the one unrepresentable input now
fails the query.
