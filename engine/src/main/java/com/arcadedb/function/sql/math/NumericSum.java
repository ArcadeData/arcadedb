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

import com.arcadedb.schema.Type;

import java.math.BigDecimal;

/**
 * The running total of {@code sum()} and {@code avg()}: the same value, of the same type, as folding every input into a
 * {@link Number} with {@link Type#increment(Number, Number)}, without allocating a boxed number per row while the inputs
 * are {@link Double}, {@link Float}, {@link Long}, {@link Integer}, {@link Short} or {@link Byte} (issue #9496). The
 * primitive adders ({@link #addInt}, {@link #addLong}, {@link #addDouble}, {@link #addFloat}, {@link #addShort},
 * {@link #addByte}) take the same transitions as {@link #add} of the boxed value, so a caller that reads primitives (the
 * STATS facet over a columnar projection) gets what {@code sum()} gets.
 * <p>
 * The state mirrors the boxed total it stands for: {@link #DOUBLE} is a {@code Double} total, {@link #FLOAT} a
 * {@code Float} one (float arithmetic, as {@code Type.increment} adds two floats), {@link #LONG} a {@code Long} one,
 * {@link #INT} an {@code Integer} one, or a {@code Long} one once an integer total left the {@code int} range (as
 * {@code Type.increment} widens it), or the single {@code Short} or {@code Byte} added so far (a one-row sum keeps the
 * row's type). Each transition does what {@code Type.increment} does for that pair of types: an integral total meeting a
 * {@code Double} becomes the {@code double} sum, a {@code long} overflow and every other input type ({@code BigDecimal}...)
 * materialize the boxed total and carry on through {@code Type.increment} itself, so their results are exactly what they
 * were.
 */
public final class NumericSum {
  private static final byte EMPTY  = 0;
  private static final byte DOUBLE = 1;
  private static final byte LONG   = 2;
  private static final byte INT    = 3;
  private static final byte FLOAT  = 4;
  private static final byte BOXED  = 5;

  // INT ONLY: THE TOTAL IS THE ONE Short OR Byte ADDED SO FAR (Type.increment() OF TWO OF THEM IS AN Integer)
  private static final byte MANY   = 0;
  private static final byte SHORT  = 1;
  private static final byte BYTE   = 2;

  private byte    state = EMPTY;
  private double  doubleSum;
  private float   floatSum;
  private long    longSum;
  // INT ONLY: THE TOTAL LEFT THE int RANGE ONCE, SO THE BOXED TOTAL IS A Long FROM THEN ON, AS Type.increment() WIDENS IT
  private boolean widened;
  private byte    single;
  private Number  boxed;

  public NumericSum() {
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
    case DOUBLE -> doubleSum += value;
    case FLOAT -> floatSum += value;
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
    case DOUBLE -> doubleSum += value;
    case FLOAT -> floatSum += value;
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
    case DOUBLE -> doubleSum += value;
    case FLOAT -> floatSum += value;
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
    case DOUBLE -> doubleSum += value;
    case FLOAT -> {
      // Float + Double IS THE SHORTEST DECIMAL WIDENING OF THE FLOAT PLUS THE DOUBLE IN Type.increment()
      state = DOUBLE;
      doubleSum = Type.widenFloat(floatSum) + value;
    }
    case LONG, INT -> {
      // Double + Long AND Double + Integer ARE THE double SUM IN Type.increment()
      state = DOUBLE;
      single = MANY;
      doubleSum = longSum + value;
    }
    case EMPTY -> {
      // THE FIRST VALUE IS THE TOTAL AS IT IS (A -0.0 STAYS -0.0)
      state = DOUBLE;
      doubleSum = value;
    }
    default -> boxed = Type.increment(boxed, value);
    }
  }

  /** Adds a {@code Float} value without boxing it: a float total is summed in float arithmetic, as {@code Type.increment} does. */
  public void addFloat(final float value) {
    switch (state) {
    case FLOAT -> floatSum += value;
    // Double + Float IS THE DOUBLE PLUS THE SHORTEST DECIMAL WIDENING OF THE FLOAT
    case DOUBLE -> doubleSum += Type.widenFloat(value);
    case LONG, INT -> {
      // AN INTEGRAL TOTAL PLUS A Float IS A float IN Type.increment()
      state = FLOAT;
      single = MANY;
      floatSum = longSum + value;
    }
    case EMPTY -> {
      state = FLOAT;
      floatSum = value;
    }
    default -> boxed = Type.increment(boxed, value);
    }
  }

  /** Folds the total of {@code other} in as one value, as {@code sum()} merges a partial total; an empty one adds nothing. */
  public void merge(final NumericSum other) {
    final Number total = other.get();
    if (total != null)
      add(total);
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
      boxed = Type.increment(get(), value);
    state = BOXED;
  }

  private void addChecked(final long value) {
    final long result = longSum + value;
    // HACKER'S DELIGHT: OVERFLOW ONLY WHEN BOTH OPERANDS HAVE THE SIGN OPPOSITE TO THE RESULT. Type.increment() THEN
    // WIDENS TO BigDecimal: LET IT (Long + Long, Long + Integer AND Long + Short ALL WIDEN THE SAME WAY)
    if (((longSum ^ result) & (value ^ result)) < 0) {
      boxed = Type.increment(get(), value);
      state = BOXED;
    } else
      longSum = result;
  }

  /** The total as {@code Type.increment()} would have built it, or null when no value was added. */
  public Number get() {
    return switch (state) {
      case EMPTY -> null;
      case DOUBLE -> doubleSum;
      case FLOAT -> floatSum;
      case LONG -> longSum;
      case INT -> widened ? (Number) longSum : single == SHORT ? (Number) (short) longSum : single == BYTE ? (Number) (byte) longSum :
          (Number) (int) longSum;
      default -> boxed;
    };
  }

  /** The total as a {@code double}, without boxing it when it is not boxed already. */
  public double doubleValue() {
    return switch (state) {
      case DOUBLE -> doubleSum;
      // THE PRIMITIVE WIDENING, AS Float.doubleValue() OF THE BOXED TOTAL
      case FLOAT -> floatSum;
      case LONG, INT -> longSum;
      default -> boxed.doubleValue();
    };
  }

  /** Whether the total is a {@code BigDecimal}. */
  public boolean isDecimal() {
    return state == BOXED && boxed instanceof BigDecimal;
  }
}
