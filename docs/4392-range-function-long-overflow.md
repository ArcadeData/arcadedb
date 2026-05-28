# Fix #4392: RangeFunction long overflow infinite loop

## Issue

`RangeFunction.java` lines 58-63 loop forever when `i += step` wraps silently near
`Long.MAX_VALUE` / `Long.MIN_VALUE`. After the wrap the loop condition remains true
and iteration never ends, hanging the server and exhausting memory.

Reported example: `range(9223372036854775800, 9223372036854775807, 1000)` hangs.

## Root Cause

The for-loops use `i += step` unconditionally. Java signed-long arithmetic silently
wraps on overflow: a value just below `Long.MAX_VALUE` plus a large step wraps to a
very negative number that still satisfies `i <= end`, restarting the iteration cycle
indefinitely.

## Fix

Added pre-increment overflow guards in both branches of `RangeFunction.execute()`:

- Positive step: `if (i > Long.MAX_VALUE - step) break;`
- Negative step: `if (i < Long.MIN_VALUE - step) break;`

The check is an arithmetic comparison only - no exceptions, no extra objects.
`Long.MAX_VALUE - step` (step > 0) is always in [0, Long.MAX_VALUE-1] so the
subtraction never overflows. `Long.MIN_VALUE - step` (step < 0) equals
`Long.MIN_VALUE + |step|` which is in [Long.MIN_VALUE+1, -1] for any negative step
except `Long.MIN_VALUE` itself (which yields 0, still a safe boundary).

## Files Changed

- `engine/src/main/java/com/arcadedb/function/coll/RangeFunction.java` - overflow guards
- `engine/src/test/java/com/arcadedb/function/coll/RangeFunctionTest.java` - new test class

## Test Results

Tests written covering:
- Overflow at positive end: range with step that would carry past Long.MAX_VALUE
- Full range up to Long.MAX_VALUE with step 1
- Underflow at negative end: range with negative step that would carry past Long.MIN_VALUE
- Full range down to Long.MIN_VALUE with step -1
- Exact single-element ranges at both extremes
