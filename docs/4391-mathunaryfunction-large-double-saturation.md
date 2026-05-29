# Fix #4391: MathUnaryFunction/MathBinaryFunction saturate large doubles to Long.MAX_VALUE

## Root Cause

In `MathUnaryFunction.java` (line 53) and `MathBinaryFunction.java` (line 54), the
integer-collapse guard checks only:

```java
if (result == Math.floor(result) && !Double.isInfinite(result))
  return (long) result;
```

For any double whose magnitude exceeds Long.MAX_VALUE (i.e. values above ~9.2e18, which
includes any double above 2^52 that is equal to its own floor), the cast `(long) result`
silently saturates to `Long.MAX_VALUE` or `Long.MIN_VALUE`. For example,
`math.floor(1e30)` returns `9223372036854775807L` instead of `1.0e30`.

## Fix

Add a range guard before the cast so that only values that actually fit in a `long` are
converted:

```java
if (result >= Long.MIN_VALUE && result <= Long.MAX_VALUE
    && result == Math.floor(result) && !Double.isInfinite(result))
  return (long) result;
return result;
```

`!Double.isInfinite(result)` is now redundant (infinity is outside `[Long.MIN_VALUE,
Long.MAX_VALUE]`) but kept for clarity since the original test was there.

Actually, since `Long.MIN_VALUE` and `Long.MAX_VALUE` as doubles are slightly different
from the true long bounds (due to rounding), we use `(double) Long.MIN_VALUE` and
`(double) Long.MAX_VALUE` which are the nearest representable doubles.

## Affected Files

- `engine/src/main/java/com/arcadedb/function/math/MathUnaryFunction.java`
- `engine/src/main/java/com/arcadedb/function/math/MathBinaryFunction.java`

## Test

New test class: `engine/src/test/java/com/arcadedb/function/math/MathFunctionLargeDoubleTest.java`

Covers:
- `MathUnaryFunction` with floor(1e30) - should return Double not long-saturated value
- `MathUnaryFunction` with ceil(-1e30) - should return Double
- `MathUnaryFunction` with floor(1.5e18) - fits in long, should return long
- `MathBinaryFunction` with atan2 of large values - should return Double
- SQL query: `SELECT math.floor(1e30)` should return 1.0e30 as a Double

## Verification Steps

1. Tests written - confirm they fail before the fix
2. Apply fix to both files
3. Tests pass
4. Compile passes
