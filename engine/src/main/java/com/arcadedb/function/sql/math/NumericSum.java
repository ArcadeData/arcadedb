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
 * are {@link Double}, {@link Long} or {@link Integer} (issue #9496).
 * <p>
 * The state mirrors the boxed total it stands for: {@link #DOUBLE} is a {@code Double} total, {@link #LONG} a {@code Long}
 * one, {@link #INT} an {@code Integer} one, or a {@code Long} one once an integer total left the {@code int} range (as
 * {@code Type.increment} widens it). Each transition does what {@code Type.increment} does for that pair of types: an
 * integral total meeting a {@code Double} becomes the {@code double} sum, a {@code long} overflow and every other input
 * type ({@code Short}, {@code Byte}, {@code Float}, {@code BigDecimal}...) materialize the boxed total and carry on through
 * {@code Type.increment} itself, so their results are exactly what they were.
 */
final class NumericSum {
  private static final byte EMPTY  = 0;
  private static final byte DOUBLE = 1;
  private static final byte LONG   = 2;
  private static final byte INT    = 3;
  private static final byte BOXED  = 4;

  private byte    state = EMPTY;
  private double  doubleSum;
  private long    longSum;
  // INT ONLY: THE TOTAL LEFT THE int RANGE ONCE, SO THE BOXED TOTAL IS A Long FROM THEN ON, AS Type.increment() WIDENS IT
  private boolean widened;
  private Number  boxed;

  /** Adds a non-null value. */
  void add(final Number value) {
    switch (state) {
    case DOUBLE -> {
      // Double + Long AND Double + Integer ARE THE double SUM IN Type.increment()
      if (value instanceof Double || value instanceof Long || value instanceof Integer) {
        doubleSum += value instanceof Double d ? d : value.longValue();
        return;
      }
    }
    case LONG -> {
      if (value instanceof Long || value instanceof Integer) {
        addLong(value.longValue(), value);
        return;
      }
      if (value instanceof Double d) {
        state = DOUBLE;
        doubleSum = longSum + d;
        return;
      }
    }
    case INT -> {
      if (value instanceof Integer i) {
        if (widened) {
          addLong(i, value);
        } else {
          // int + int CANNOT OVERFLOW A long: Type.increment() WIDENS THE TOTAL TO Long WHEN IT LEAVES THE int RANGE
          longSum += i;
          if (longSum != (int) longSum)
            widened = true;
        }
        return;
      }
      if (value instanceof Long) {
        // Integer + Long AND Long + Long ARE BOTH addExactOrWiden() IN Type.increment(): A Long TOTAL FROM HERE ON
        state = LONG;
        addLong(value.longValue(), value);
        return;
      }
      if (value instanceof Double d) {
        state = DOUBLE;
        doubleSum = longSum + d;
        return;
      }
    }
    case EMPTY -> {
      // THE FIRST VALUE IS THE TOTAL AS IT IS (A -0.0 STAYS -0.0)
      switch (value) {
      case Double d -> {
        state = DOUBLE;
        doubleSum = d;
      }
      case Long l -> {
        state = LONG;
        longSum = l;
      }
      case Integer i -> {
        state = INT;
        longSum = i;
        widened = false;
      }
      default -> {
        state = BOXED;
        boxed = value;
      }
      }
      return;
    }
    default -> {
    }
    }
    // ANY OTHER COMBINATION: THE BOXED TOTAL THROUGH Type.increment(), AS BEFORE
    boxed = Type.increment(get(), value);
    state = BOXED;
  }

  private void addLong(final long value, final Number original) {
    final long result = longSum + value;
    // HACKER'S DELIGHT: OVERFLOW ONLY WHEN BOTH OPERANDS HAVE THE SIGN OPPOSITE TO THE RESULT. Type.increment() THEN
    // WIDENS TO BigDecimal: LET IT
    if (((longSum ^ result) & (value ^ result)) < 0) {
      boxed = Type.increment(get(), original);
      state = BOXED;
    } else
      longSum = result;
  }

  /** The total as {@code Type.increment()} would have built it, or null when no value was added. */
  Number get() {
    return switch (state) {
      case EMPTY -> null;
      case DOUBLE -> doubleSum;
      case LONG -> longSum;
      case INT -> widened ? (Number) longSum : (Number) (int) longSum;
      default -> boxed;
    };
  }

  /** The total as a {@code double}, without boxing it when it is not boxed already. */
  double doubleValue() {
    return switch (state) {
      case DOUBLE -> doubleSum;
      case LONG, INT -> longSum;
      default -> boxed.doubleValue();
    };
  }

  /** Whether the total is a {@code BigDecimal}. */
  boolean isDecimal() {
    return state == BOXED && boxed instanceof BigDecimal;
  }
}
