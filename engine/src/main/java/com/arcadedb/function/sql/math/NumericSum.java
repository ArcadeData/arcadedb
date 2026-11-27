/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.function.sql.math;

import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.schema.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

/**
 * The running total of {@code sum()} and {@code avg()}: the type of the total, and its value for integral and decimal
 * inputs, are what folding every input into a {@link Number} with {@link Type#increment(Number, Number)} gives, without
 * allocating a boxed number per row while the inputs are {@link Double}, {@link Float}, {@link Long}, {@link Integer},
 * {@link Short} or {@link Byte} (issue #9496). The primitive adders ({@link #addInt}, {@link #addLong}, {@link #addDouble},
 * {@link #addFloat}, {@link #addShort}, {@link #addByte}) take the same transitions as {@link #add} of the boxed value, so a
 * caller that reads primitives (the STATS facet over a columnar projection) gets what {@code sum()} gets.
 * <p>
 * The state mirrors the boxed total it stands for: {@link #DOUBLE} is a {@code Double} total, {@link #FLOAT} a
 * {@code Float} one, {@link #LONG} a {@code Long} one, {@link #INT} an {@code Integer} one, or a {@code Long} one once an
 * integer total left the {@code int} range (as {@code Type.increment} widens it), or the single {@code Short} or
 * {@code Byte} added so far (a one-row sum keeps the row's type). Each transition does what {@code Type.increment} does for
 * that pair of types: an integral total meeting a {@code Double} becomes a {@code double} sum, a {@code long} overflow and
 * every other input type ({@code BigDecimal}...) materialize the boxed total and carry on through {@code Type.increment}
 * itself, so their results are exactly what they were.
 * <p>
 * A floating-point total ({@link #DOUBLE}, {@link #FLOAT}) is not a running {@code double}: it is the EXACT sum of what was
 * added, read as that sum correctly rounded (to a {@code double}, and from it to a {@code float} for a {@link #FLOAT} total).
 * Floating-point addition is not associative, so a running total depends on the order the values arrive in, and the parallel
 * scan merges partial totals in an order that depends on thread timing: the same read query over unchanged data answered a
 * different value on almost every run, and never the one of the sequential scan (issue #12696). The exact sum does not depend
 * on the order, so every split of the values in partials, merged in any order ({@link #merge} folds the other side's exact sum
 * in, not its rounded total), answers the same value as the sequential scan. An integral total meeting a floating-point one is
 * added exactly too.
 * <p>
 * The exact sum is a fixed-point integer in units of 2^-1074 (the smallest double), every finite double being an integer
 * multiple of it: base 2^32 digits held in {@code long}s, a {@code double} of mantissa {@code m} landing on the three digits
 * its bits cover. A digit takes the signed 32-bit parts without propagating the carry, which the {@code long} has room for
 * across {@link #NORMALIZE_EVERY} additions, so an addition is a few integer additions, with no dependency on the previous one
 * and nothing to allocate (a sum of Shewchuk partials, as {@code math.fsum} keeps it, chains several dependent floating-point
 * additions per value and was ten times slower). Only the digits the values reach are held (a handful for ordinary data). The
 * infinities and NaN are summed apart (their sum does not depend on the order) and win over any finite total, and a total of
 * zeros is -0.0 only when every value was -0.0, as IEEE addition gives. The integer cannot overflow, so a total back in the
 * {@code double} range after an intermediate one that is not is exact too.
 * <p>
 * A sum built with {@code integralOverflowFails} (the openCypher {@code sum()}, issue #12993) does not widen a {@code long}
 * overflow to a {@code BigDecimal}, a type Cypher does not have: the integral total goes on as the exact fixed-point sum
 * ({@link #EXACT_INTEGRAL}), so neither the order of the values nor the split of a parallel scan changes the answer. Read, it is
 * the {@code long} when it is back in range, and the {@code long overflow} ArithmeticError otherwise, as the integer operators
 * of the language answer; a floating-point value added to it makes it the floating-point total of the exact sum, as it does to a
 * {@code long} total.
 */
public final class NumericSum {
  private static final byte EMPTY          = 0;
  private static final byte DOUBLE         = 1;
  private static final byte LONG           = 2;
  private static final byte INT            = 3;
  private static final byte FLOAT          = 4;
  private static final byte BOXED          = 5;
  // integralOverflowFails ONLY: AN INTEGRAL TOTAL THAT LEFT THE long RANGE ON THE WAY, HELD EXACT IN THE DIGITS (#12993)
  private static final byte EXACT_INTEGRAL = 6;

  // INT ONLY: THE TOTAL IS THE ONE Short OR Byte ADDED SO FAR (Type.increment() OF TWO OF THEM IS AN Integer)
  private static final byte MANY  = 0;
  private static final byte SHORT = 1;
  private static final byte BYTE  = 2;

  private static final long NEGATIVE_ZERO_BITS = Double.doubleToRawLongBits(-0.0);
  private static final long DIGIT_MASK         = 0xFFFFFFFFL;
  // THE POSITION OF 2^0 IN UNITS OF 2^-1074: WHERE A long IS ADDED
  private static final int  INTEGER_POSITION   = 1074;
  // ADDITIONS BETWEEN TWO CARRY PROPAGATIONS: A DIGIT THEN HOLDS LESS THAN (NORMALIZE_EVERY + 1) * 2^32 IN MAGNITUDE
  private static final int  NORMALIZE_EVERY    = 1 << 20;
  // THE POSITION OF THE SMALLEST SUBNORMAL float, 2^-149
  private static final int  FLOAT_MIN_POSITION = INTEGER_POSITION - 149;

  /**
   * An upper bound of the heap the digits of a floating-point total take: the digit indexes reach from -1 (one below the first
   * value's) to 63 + 3 (the highest double's three digits and the carry digit above them), one more for a merge: 69 digits, 72 here,
   * held in a {@code long[]} with its header. What a caller reserving the memory of a sum adds for it.
   */
  public static final long MAX_DIGITS_HEAP_BYTES = 16 + 8 * 72;

  // A long OVERFLOW STAYS AN EXACT INTEGRAL TOTAL INSTEAD OF WIDENING TO A BigDecimal, AND READS AS long overflow (#12993)
  private final boolean integralOverflowFails;
  private byte    state = EMPTY;
  private long    longSum;
  // INT ONLY: THE TOTAL LEFT THE int RANGE ONCE, SO THE BOXED TOTAL IS A Long FROM THEN ON, AS Type.increment() WIDENS IT
  private boolean widened;
  private byte    single;
  private Number  boxed;

  // DOUBLE AND FLOAT: THE EXACT SUM OF THE FINITE VALUES IN UNITS OF 2^-1074, digits[i] WEIGHING 2^(32 * (digitBase + i)) (#12696).
  // ONCE NORMALIZED EVERY DIGIT IS IN [0, 2^32) BUT THE LAST, WHICH IS SIGNED AND IN [-2^31, 2^31)
  private long[]  digits;
  private int     digitBase;
  private int     pendingAdds;
  // DOUBLE AND FLOAT: THE SUM OF THE INFINITE AND NaN VALUES, NOT 0 (NaN != 0 TOO) ONCE ONE WAS ADDED
  private double  nonFinite;
  // DOUBLE AND FLOAT: EVERY VALUE ADDED SO FAR WAS -0.0, SO A ZERO TOTAL IS -0.0
  private boolean negativeZero;

  public NumericSum() {
    this(false);
  }

  /**
   * @param integralOverflowFails true for the openCypher semantics: an integral total out of the {@code long} range is the
   *                              {@code long overflow} ArithmeticError when read, not a {@code BigDecimal}
   */
  public NumericSum(final boolean integralOverflowFails) {
    this.integralOverflowFails = integralOverflowFails;
  }

  /** Adds a non-null value. */
  public void add(final Number value) {
    switch (value) {
    case Integer i -> addInt(i);
    case Long l -> addLong(l);
    case Double d -> addDouble(d);
    case Float f -> addFloat(f);
    case Short s -> addShort(s);
    case Byte b -> addByte(b);
    default -> addBoxed(value);
    }
  }

  /** Adds an {@code Integer} value without boxing it. */
  public void addInt(final int value) {
    switch (state) {
    case DOUBLE, FLOAT -> exactAdd(value);
    case EXACT_INTEGRAL -> exactAddLong(value);
    case LONG -> addChecked(value);
    case INT -> addToInt(value);
    case EMPTY -> {
      state = INT;
      longSum = value;
      widened = false;
      single = MANY;
    }
    default -> boxed = Type.increment(boxed, value);
    }
  }

  /** Adds a {@code Short} value without boxing it: {@code Type.increment} adds a short as it adds an int. */
  public void addShort(final short value) {
    addSmall(value, SHORT);
  }

  /** Adds a {@code Byte} value without boxing it: {@code Type.increment} widens a byte to a short first. */
  public void addByte(final byte value) {
    addSmall(value, BYTE);
  }

  private void addSmall(final int value, final byte kind) {
    switch (state) {
    case DOUBLE, FLOAT -> exactAdd(value);
    case EXACT_INTEGRAL -> exactAddLong(value);
    case LONG -> addChecked(value);
    case INT -> addToInt(value);
    case EMPTY -> {
      state = INT;
      longSum = value;
      widened = false;
      single = kind;
    }
    default -> boxed = Type.increment(boxed, kind == SHORT ? (Number) (short) value : (Number) (byte) value);
    }
  }

  /** Adds a {@code Long} value without boxing it. */
  public void addLong(final long value) {
    switch (state) {
    case DOUBLE, FLOAT, EXACT_INTEGRAL -> exactAddLong(value);
    case LONG -> addChecked(value);
    case INT -> {
      // Integer + Long AND Long + Long ARE BOTH addExactOrWiden() IN Type.increment(): A Long TOTAL FROM HERE ON
      single = MANY;
      state = LONG;
      addChecked(value);
    }
    case EMPTY -> {
      state = LONG;
      longSum = value;
    }
    default -> boxed = Type.increment(boxed, value);
    }
  }

  /** Adds a {@code Double} value without boxing it. */
  public void addDouble(final double value) {
    switch (state) {
    case DOUBLE -> exactAdd(value);
    case FLOAT -> {
      // Float + Double IS THE SHORTEST DECIMAL WIDENING OF THE FLOAT PLUS THE DOUBLE IN Type.increment()
      floatToDouble();
      exactAdd(value);
    }
    case LONG, INT -> {
      // Double + Long AND Double + Integer ARE A double SUM IN Type.increment(): THE INTEGRAL TOTAL IS ADDED EXACTLY
      integralToFloating(DOUBLE);
      exactAdd(value);
    }
    case EXACT_INTEGRAL -> {
      // THE DIGITS ALREADY HOLD THE EXACT INTEGRAL TOTAL: THE SAME double SUM AS A long TOTAL MEETING A Double
      state = DOUBLE;
      exactAdd(value);
    }
    case EMPTY -> {
      // THE FIRST VALUE IS THE TOTAL AS IT IS (A -0.0 STAYS -0.0)
      startFloating(DOUBLE);
      exactAdd(value);
    }
    default -> boxed = Type.increment(boxed, value);
    }
  }

  /** Adds a {@code Float} value without boxing it: a float is exact as a double, so a float total is the exact sum too. */
  public void addFloat(final float value) {
    switch (state) {
    case FLOAT -> exactAdd(value);
    // Double + Float IS THE DOUBLE PLUS THE SHORTEST DECIMAL WIDENING OF THE FLOAT
    case DOUBLE -> exactAdd(Type.widenFloat(value));
    case LONG, INT -> {
      // AN INTEGRAL TOTAL PLUS A Float IS A float IN Type.increment()
      integralToFloating(FLOAT);
      exactAdd(value);
    }
    case EXACT_INTEGRAL -> {
      state = FLOAT;
      exactAdd(value);
    }
    case EMPTY -> {
      startFloating(FLOAT);
      exactAdd(value);
    }
    default -> boxed = Type.increment(boxed, value);
    }
  }

  /**
   * Folds {@code other} in, as {@code sum()} merges a partial total; an empty one adds nothing. A floating-point total is folded
   * in by its exact sum, not by its rounded value, so the result does not depend on how the values were split in partials nor on
   * the order the partials are merged in (issue #12696). Every other total is added as one value, as {@link #add} adds it.
   * <p>
   * That holds for values of one floating-point type, mixed with integral ones or not. Where {@code Type.increment()} itself
   * makes the result depend on which types meet first, the merge keeps its semantics: a {@code Float} total meeting a
   * {@code Double} is widened as a whole, its values one by one once the total is a {@code Double}, and a {@code long}
   * overflow widens to a {@code BigDecimal}, which takes a {@code Double} by its decimal form.
   * <p>
   * With {@code integralOverflowFails} an integral total that left the {@code long} range is folded in by its exact sum too, so a
   * total back in range, or past it, does not depend on the split either (issue #12993).
   */
  public void merge(final NumericSum other) {
    switch (other.state) {
    case EMPTY -> {
    }
    case DOUBLE -> {
      switch (state) {
      case DOUBLE -> mergeExact(other);
      case FLOAT -> {
        floatToDouble();
        mergeExact(other);
      }
      case LONG, INT -> {
        integralToFloating(DOUBLE);
        mergeExact(other);
      }
      case EXACT_INTEGRAL -> {
        state = DOUBLE;
        mergeExact(other);
      }
      case EMPTY -> {
        startFloating(DOUBLE);
        mergeExact(other);
      }
      default -> add(other.get());
      }
    }
    case FLOAT -> {
      switch (state) {
      case FLOAT -> mergeExact(other);
      case LONG, INT -> {
        integralToFloating(FLOAT);
        mergeExact(other);
      }
      case EXACT_INTEGRAL -> {
        state = FLOAT;
        mergeExact(other);
      }
      case EMPTY -> {
        startFloating(FLOAT);
        mergeExact(other);
      }
      // A Double TOTAL PLUS A Float ONE IS THE DOUBLE PLUS THE WIDENED FLOAT, AS addFloat() ADDS IT
      default -> add(other.get());
      }
    }
    case EXACT_INTEGRAL -> {
      switch (state) {
      // A FLOATING-POINT TOTAL PLUS AN INTEGRAL ONE STAYS OF ITS FLOATING-POINT KIND, AS addLong() ADDS TO IT
      case DOUBLE, FLOAT, EXACT_INTEGRAL -> mergeExact(other);
      case LONG, INT -> {
        integralToFloating(EXACT_INTEGRAL);
        mergeExact(other);
      }
      case EMPTY -> {
        startFloating(EXACT_INTEGRAL);
        mergeExact(other);
      }
      default -> add(other.exactIntegral());
      }
    }
    default -> add(other.get());
    }
  }

  private void addToInt(final int value) {
    single = MANY;
    if (widened) {
      addChecked(value);
    } else {
      // int + int CANNOT OVERFLOW A long: Type.increment() WIDENS THE TOTAL TO Long WHEN IT LEAVES THE int RANGE
      longSum += value;
      if (longSum != (int) longSum)
        widened = true;
    }
  }

  /** Any other input type: the boxed total through {@code Type.increment()}, as before. */
  private void addBoxed(final Number value) {
    if (state == EMPTY)
      boxed = value;
    else
      // AN INTEGRAL TOTAL PAST THE long RANGE MEETS THE VALUE AS THE DECIMAL IT IS, AS A WIDENED ONE WOULD
      boxed = Type.increment(state == EXACT_INTEGRAL ? exactIntegral() : get(), value);
    state = BOXED;
  }

  private void addChecked(final long value) {
    final long result = longSum + value;
    // HACKER'S DELIGHT: OVERFLOW ONLY WHEN BOTH OPERANDS HAVE THE SIGN OPPOSITE TO THE RESULT. Type.increment() THEN
    // WIDENS TO BigDecimal: LET IT (Long + Long, Long + Integer AND Long + Short ALL WIDEN THE SAME WAY)
    if (((longSum ^ result) & (value ^ result)) < 0) {
      if (integralOverflowFails) {
        // NO BigDecimal: THE EXACT INTEGRAL TOTAL GOES ON IN THE DIGITS, AND ONLY ITS FINAL VALUE DECIDES (#12993)
        integralToFloating(EXACT_INTEGRAL);
        exactAddLong(value);
      } else {
        boxed = Type.increment(get(), value);
        state = BOXED;
      }
    } else
      longSum = result;
  }

  /** Starts an empty floating-point total of the {@code floatingState} kind. */
  private void startFloating(final byte floatingState) {
    state = floatingState;
    digits = null;
    pendingAdds = 0;
    nonFinite = 0.0;
    negativeZero = true;
  }

  /** An integral total becomes a floating-point one of the {@code floatingState} kind, starting from the integral total, exact. */
  private void integralToFloating(final byte floatingState) {
    final long integral = longSum;
    single = MANY;
    startFloating(floatingState);
    exactAddLong(integral);
  }

  /** A {@code Float} total becomes a {@code Double} one starting from the shortest decimal widening of the float, as Type.increment(). */
  private void floatToDouble() {
    final double widened = Type.widenFloat(floatTotal());
    startFloating(DOUBLE);
    exactAdd(widened);
  }

  /** Adds a {@code long} exactly. */
  private void exactAddLong(final long value) {
    negativeZero = false;
    if (value != 0L) {
      final long sign = value >> 63;
      // Long.MIN_VALUE NEGATES TO ITSELF, WHICH READ UNSIGNED IS ITS MAGNITUDE, 2^63
      addScaled((value ^ sign) - sign, INTEGER_POSITION, sign);
    }
  }

  /** Adds a {@code double} exactly: its mantissa, at the position of its lowest bit. */
  private void exactAdd(final double value) {
    if (!Double.isFinite(value)) {
      nonFinite += value;
      return;
    }
    final long bits = Double.doubleToRawLongBits(value);
    if (bits != NEGATIVE_ZERO_BITS)
      negativeZero = false;
    final long fraction = bits & 0xFFFFFFFFFFFFFL;
    // A NORMAL double IS (2^52 + fraction) * 2^(exponent - 52), A SUBNORMAL ONE fraction * 2^-1074
    final int exponent = Math.getExponent(value);
    if (exponent >= Double.MIN_EXPONENT)
      addScaled(fraction | (1L << 52), exponent + 1022, bits >> 63);
    else if (fraction != 0L)
      addScaled(fraction, 0, bits >> 63);
  }

  /**
   * Adds {@code magnitude} (unsigned) times 2^({@code position} - 1074), negated when {@code sign} is -1: the three base 2^32
   * digits its bits cover, the carry left for {@link #normalize}.
   */
  private void addScaled(final long magnitude, final int position, final long sign) {
    final int digit = position >>> 5;
    final int shift = position & 31;
    // THE ROOM FOR THE THREE DIGITS AND ONE ABOVE THEM, WHICH ONLY EVER TAKES A CARRY
    if (digits == null) {
      digits = new long[6];
      digitBase = digit - 1;
    } else if (digit < digitBase || digit + 3 >= digitBase + digits.length)
      growTo(Math.min(digit, digitBase), Math.max(digit + 3, digitBase + digits.length - 1));

    final int i = digit - digitBase;
    final long low = (magnitude << shift) & DIGIT_MASK;
    final long middle = (magnitude >>> (32 - shift)) & DIGIT_MASK;
    // (magnitude >>> 32) >>> (32 - shift), NOT magnitude >>> (64 - shift): A SHIFT OF 64 IS A SHIFT OF 0 IN JAVA
    final long high = (magnitude >>> 32) >>> (32 - shift);
    digits[i] += (low ^ sign) - sign;
    digits[i + 1] += (middle ^ sign) - sign;
    digits[i + 2] += (high ^ sign) - sign;
    if (++pendingAdds >= NORMALIZE_EVERY)
      normalize();
  }

  /** Widens the digits to the digit indexes {@code first} to {@code last}, both included. */
  private void growTo(final int first, final int last) {
    final long[] grown = new long[last - first + 1];
    System.arraycopy(digits, 0, grown, digitBase - first, digits.length);
    digits = grown;
    digitBase = first;
  }

  /** Propagates the carries: every digit in [0, 2^32) but the last, signed, in [-2^31, 2^31). The value does not change. */
  private void normalize() {
    pendingAdds = 0;
    final int last = digits.length - 1;
    long carry = 0;
    for (int i = 0; i < last; i++) {
      final long v = digits[i] + carry;
      digits[i] = v & DIGIT_MASK;
      carry = v >> 32;
    }
    final long top = digits[last] + carry;
    if (top >= Integer.MIN_VALUE && top <= Integer.MAX_VALUE)
      digits[last] = top;
    else {
      growTo(digitBase, digitBase + last + 1);
      digits[last] = top & DIGIT_MASK;
      digits[last + 1] = top >> 32;
    }
  }

  /** Folds the exact sum of another floating-point total into this one. */
  private void mergeExact(final NumericSum other) {
    nonFinite += other.nonFinite;
    negativeZero &= other.negativeZero;
    if (other.digits == null)
      return;
    // BOTH SIDES NORMALIZED (THEIR VALUE DOES NOT CHANGE): EVERY DIGIT THEN STAYS FAR FROM THE long RANGE
    other.normalize();
    final long[] theirs = other.digits;
    // THEIR DIGITS AND ONE ABOVE THEM, SO THEIR SIGNED LAST DIGIT LANDS BELOW OUR LAST
    if (digits == null) {
      digits = new long[theirs.length + 1];
      digitBase = other.digitBase;
    } else if (other.digitBase < digitBase || other.digitBase + theirs.length >= digitBase + digits.length)
      growTo(Math.min(other.digitBase, digitBase), Math.max(other.digitBase + theirs.length, digitBase + digits.length - 1));
    normalize();
    final int offset = other.digitBase - digitBase;
    for (int k = 0; k < theirs.length; k++)
      digits[offset + k] += theirs[k];
    normalize();
  }

  /** The exact sum correctly rounded to a {@code double}, half to even. */
  private double doubleTotal() {
    return roundExact(53, 0);
  }

  /** The exact sum correctly rounded to a {@code float}, half to even: rounded once, from the exact sum, not from its double. */
  private float floatTotal() {
    // THE double HOLDS THE ROUNDED float EXACTLY (OR AN INFINITY), SO THE CAST DOES NOT ROUND AGAIN
    return (float) roundExact(24, FLOAT_MIN_POSITION);
  }

  /**
   * The exact sum rounded half to even to {@code precision} significant bits, and never below the bit at {@code minPosition}
   * (in units of 2^-1074: the step of the subnormals of the target type), as a {@code double} that holds it exactly.
   */
  private double roundExact(final int precision, final int minPosition) {
    if (nonFinite != 0.0)
      // AN INFINITY OR NaN WINS OVER ANY FINITE TOTAL; +INFINITY AND -INFINITY GIVE NaN
      return nonFinite;
    if (digits == null)
      return negativeZero ? -0.0 : 0.0;
    normalize();

    final int last = digits.length - 1;
    int lowest = 0;
    while (lowest <= last && digits[lowest] == 0L)
      lowest++;
    if (lowest > last)
      // VALUES THAT CANCEL EXACTLY: +0.0, AS IEEE ADDITION GIVES
      return 0.0;

    final boolean negative = digits[last] < 0L;
    int highest = last;
    while (magnitudeDigit(highest, negative, lowest) == 0L)
      highest--;

    // THE MAGNITUDE'S THREE HIGHEST DIGITS, a * 2^64 + b * 2^32 + c, HOLD AT LEAST 65 SIGNIFICANT BITS: ENOUGH FOR 53, THE ROUND
    // BIT, AND THE FIRST STICKY BITS. EVERY NON-ZERO DIGIT BELOW THEM IS STICKY TOO
    final long a = magnitudeDigit(highest, negative, lowest);
    final long b = highest >= 1 ? magnitudeDigit(highest - 1, negative, lowest) : 0L;
    final long c = highest >= 2 ? magnitudeDigit(highest - 2, negative, lowest) : 0L;
    final int aBits = 64 - Long.numberOfLeadingZeros(a);
    // THE 64 HIGHEST SIGNIFICANT BITS, BIT 63 SET, AND WHAT THEY LEAVE OUT OF c
    final long top = (((a << 32) | b) << (32 - aBits)) | (c >>> aBits);
    final boolean stickyBelowTop = lowest < highest - 2 || (c & ((1L << aBits) - 1)) != 0L;

    // THE POSITION OF THE BIT 0 OF top, AND THE ONE THE ROUNDED VALUE KEEPS AS ITS LOWEST: drop BITS OF top GO
    final int topPosition = 32 * (digitBase + highest - 2) + aBits;
    final int keptPosition = Math.max(topPosition + 64 - precision, minPosition);
    final int drop = keptPosition - topPosition;
    final double magnitude;
    if (drop > 64)
      // LESS THAN HALF THE SMALLEST STEP: A ZERO OF THE SUM'S SIGN
      magnitude = 0.0;
    else {
      long mantissa = drop == 64 ? 0L : top >>> drop;
      final boolean roundBit = ((top >>> (drop - 1)) & 1L) != 0L;
      final boolean sticky = stickyBelowTop || (top & ((1L << (drop - 1)) - 1)) != 0L;
      if (roundBit && (sticky || (mantissa & 1L) != 0L))
        mantissa++;
      // EXACT: AT MOST precision + 1 BITS, ABOVE THE SUBNORMAL STEP, OR AN INFINITY PAST THE RANGE
      magnitude = Math.scalb((double) mantissa, keptPosition - INTEGER_POSITION);
    }
    return negative ? -magnitude : magnitude;
  }

  /**
   * The digit {@code i} of the magnitude of the normalized sum: the digit itself when the sum is positive, its two's complement
   * when it is negative ({@code lowest} is the lowest non-zero digit, where the +1 of the complement stops).
   */
  private long magnitudeDigit(final int i, final boolean negative, final int lowest) {
    final long digit = digits[i];
    if (!negative)
      return digit;
    if (i < lowest)
      return 0L;
    if (i == digits.length - 1)
      // THE SIGNED LAST DIGIT: -digit WHEN NO DIGIT BELOW IS NON-ZERO, ~digit = -digit - 1 OTHERWISE
      return i == lowest ? -digit : ~digit;
    return i == lowest ? -digit & DIGIT_MASK : ~digit & DIGIT_MASK;
  }

  /** The exact sum of the finite values, divided by {@code count}, rounded to 34 significant digits. */
  private BigDecimal exactMean(final long count) {
    BigInteger units = exactUnits();
    // digits[0] WEIGHS 2^(32 * digitBase) UNITS OF 2^-1074
    final int exponent = 32 * digitBase - INTEGER_POSITION;
    BigInteger denominator = BigInteger.valueOf(count);
    if (exponent >= 0)
      units = units.shiftLeft(exponent);
    else
      denominator = denominator.shiftLeft(-exponent);
    return new BigDecimal(units).divide(new BigDecimal(denominator), MathContext.DECIMAL128);
  }

  /** The normalized digits as one integer, in units of 2^(32 * digitBase - 1074). */
  private BigInteger exactUnits() {
    normalize();
    BigInteger units = BigInteger.ZERO;
    for (int i = digits.length - 1; i >= 0; i--)
      units = units.shiftLeft(32).add(BigInteger.valueOf(digits[i]));
    return units;
  }

  /** The {@link #EXACT_INTEGRAL} total, exactly: an integer, so the units divide exactly. */
  private BigInteger exactIntegerTotal() {
    if (digits == null)
      return BigInteger.ZERO;
    final BigInteger units = exactUnits();
    final int exponent = 32 * digitBase - INTEGER_POSITION;
    return exponent >= 0 ? units.shiftLeft(exponent) : units.shiftRight(-exponent);
  }

  /** The {@link #EXACT_INTEGRAL} total as the decimal a widened total would have been. */
  private BigDecimal exactIntegral() {
    return new BigDecimal(exactIntegerTotal());
  }

  /** The {@link #EXACT_INTEGRAL} total as a {@code long}, or the {@code long overflow} ArithmeticError when it is out of range. */
  private long exactIntegralLong() {
    try {
      return exactIntegerTotal().longValueExact();
    } catch (final ArithmeticException e) {
      throw new ArithmeticErrorException("long overflow", e);
    }
  }

  /**
   * The total as {@code Type.increment()} would have typed it, or null when no value was added. With {@code integralOverflowFails}
   * an integral total out of the {@code long} range throws {@link ArithmeticErrorException} ({@code long overflow}).
   */
  public Number get() {
    return switch (state) {
      case EMPTY -> null;
      case DOUBLE -> doubleTotal();
      case FLOAT -> floatTotal();
      case LONG -> longSum;
      case EXACT_INTEGRAL -> exactIntegralLong();
      case INT -> widened ? (Number) longSum : single == SHORT ? (Number) (short) longSum : single == BYTE ? (Number) (byte) longSum :
          (Number) (int) longSum;
      default -> boxed;
    };
  }

  /** The total as a {@code double}, without boxing it when it is not boxed already. */
  public double doubleValue() {
    return switch (state) {
      case DOUBLE, EXACT_INTEGRAL -> doubleTotal();
      // THE PRIMITIVE WIDENING, AS Float.doubleValue() OF THE BOXED TOTAL
      case FLOAT -> floatTotal();
      case LONG, INT -> longSum;
      default -> boxed.doubleValue();
    };
  }

  /**
   * The total divided by {@code count}, as a {@code double}: what {@code avg()} answers. The mean of finite values is finite even
   * when their sum is past the {@code double} (or {@code float}) range, so a floating-point total that rounds to an infinity is
   * divided exactly first.
   */
  public double doubleMean(final long count) {
    final double total = doubleValue();
    if (Double.isInfinite(total) && (state == DOUBLE || state == FLOAT) && nonFinite == 0.0)
      return exactMean(count).doubleValue();
    return total / count;
  }

  /** Whether no value was added. */
  public boolean isEmpty() {
    return state == EMPTY;
  }

  /** Whether the total is a {@code BigDecimal}. */
  public boolean isDecimal() {
    return state == BOXED && boxed instanceof BigDecimal;
  }
}
