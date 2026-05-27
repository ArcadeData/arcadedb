# Fix #4336: Simple-8b ZigZag Validation Bypassed for Long.MAX_VALUE / Long.MIN_VALUE

## Issue

`Simple8bCodec.encode()` silently truncates `Long.MAX_VALUE` and `Long.MIN_VALUE` instead of
throwing `IllegalArgumentException`.

## Root Cause

`zigzagEncode(Long.MAX_VALUE)` returns `-2` (0xFFFFFFFFFFFFFFFE) and
`zigzagEncode(Long.MIN_VALUE)` returns `-1` (0xFFFFFFFFFFFFFFFF). Both are negative
in signed arithmetic, so the validation check `encoded > MAX_ZIGZAG_VALUE` — where
`MAX_ZIGZAG_VALUE = (1L << 60) - 1` is a large positive value — evaluates to `false`
for both extreme inputs. Validation is bypassed, and the encoder proceeds to mask
with `(1L << 60) - 1`, silently dropping the top 4 bits and corrupting the stored value.

## Affected Code

`engine/src/main/java/com/arcadedb/engine/timeseries/codec/Simple8bCodec.java`, line 69:
```java
if (encoded > MAX_ZIGZAG_VALUE)   // signed comparison — incorrect for negative encoded
```

## Fix

Replace the signed comparison with `Long.compareUnsigned`, which correctly treats the
large negative zigzag-encoded values as unsigned integers exceeding the 60-bit limit:

```java
if (Long.compareUnsigned(encoded, MAX_ZIGZAG_VALUE) > 0)
```

The arithmetic proof:
- `zigzagEncode(Long.MAX_VALUE)` = `0xFFFFFFFFFFFFFFFE` (18446744073709551614 unsigned)
  which exceeds `0x0FFFFFFFFFFFFFFF` (MAX_ZIGZAG_VALUE) as an unsigned comparison.
- `zigzagEncode(Long.MIN_VALUE)` = `0xFFFFFFFFFFFFFFFF` (18446744073709551615 unsigned)
  which also exceeds MAX_ZIGZAG_VALUE unsigned.
- Boundary value `-(2^59)` zigzag-encodes to exactly `MAX_ZIGZAG_VALUE`; the unsigned
  comparison returns 0 so it is still accepted.

## Changes

### `Simple8bCodec.java`
- Line 69: `encoded > MAX_ZIGZAG_VALUE` → `Long.compareUnsigned(encoded, MAX_ZIGZAG_VALUE) > 0`

### `Simple8bCodecTest.java`
- Added `longMaxValueThrows()` — `Long.MAX_VALUE` must throw `IllegalArgumentException`
- Added `longMinValueThrows()` — `Long.MIN_VALUE` must throw `IllegalArgumentException`
- Added `nearLongMaxValueThrows()` — values just outside range from above still throw

## Test Results

All tests pass. See verification section below.

## Impact Analysis

Only affects callers that pass values with `|v| >= 2^59` to `Simple8bCodec.encode()`.
Time-series use of nanosecond timestamps (values up to ~10^18) can exceed `2^59` (~5.7 × 10^17),
making this a real data-corruption risk in production. The fix changes a silent corruption
into a correctly-thrown exception, allowing callers to handle the out-of-range condition.

---

## PR

https://github.com/ArcadeData/arcadedb/pull/4361

## Review Cycles

### Cycle 1 - HEAD d230f84b8

**Changes:** Initial fix commit - `Long.compareUnsigned` validation + 3 regression tests.

**gemini-code-assist review:** COMMENTED
- Inline comment on `docs/4336-simple8b-zigzag-validation.md` line 49: test name in tracking
  doc said `extremeNegativeBoundaryStillThrows()` but actual test is `nearLongMaxValueThrows()`.
  Categorized: actionable and clear. Applied.

**claude review:** Not received (bot not configured on ArcadeData/arcadedb).

**Outcome:** Follow-up commit `936f8c408` pushed to address gemini feedback.

### Cycle 2 - HEAD 936f8c408

**Changes:** Doc-only fix: corrected test name in tracking doc.

**gemini-code-assist review:** No new review (does not re-review follow-up pushes per known repo behavior).

**claude review:** Not received (bot not configured on ArcadeData/arcadedb).

**Outcome:** Timeout — no reviews received in cycle 2.

## Final State

`timeout` - gemini reviewed cycle 1 only; claude bot not present in this repository.
Feedback from cycle 1 was addressed. PR is ready for human review and merge.
