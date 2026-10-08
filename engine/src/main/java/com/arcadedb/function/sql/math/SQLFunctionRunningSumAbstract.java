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

import com.arcadedb.function.sql.SQLAggregatedFunction;
import com.arcadedb.schema.Type;

/**
 * The cross-row sum {@code sum()} and {@code avg()} accumulate, kept in a primitive while the values allow it (issue #9496).
 * <p>
 * Adding a value used to be {@link Type#increment} on the boxed running sum, which allocates a new {@link Number} for every
 * row of every group: on a GROUP BY over millions of rows that was most of the garbage an aggregate made. The sum is now held
 * as a {@code long} while every value is an {@link Integer} or a {@link Long}, and as a {@code double} once a {@link Double}
 * joins them, which covers the numeric property types a schema declares for measures. Any other value - a {@link Short},
 * a {@link Float}, a {@link java.math.BigDecimal}, or a {@code long} sum that overflows - moves the sum to the boxed form,
 * where {@link Type#increment} carries on exactly as it always did.
 * <p>
 * The answer is the one {@link Type#increment} gives for the same values in the same order, type included: integers sum to an
 * {@link Integer} until their sum leaves the {@code int} range, and to a {@link Long} from then on, a {@code long} sum that
 * overflows widens to a {@link java.math.BigDecimal}, and an integral sum meeting a double becomes the double
 * {@code (double) sum + value}, the same expression {@link Type#increment} evaluates.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class SQLFunctionRunningSumAbstract extends SQLAggregatedFunction {
  private static final byte EMPTY   = 0;
  // EVERY VALUE AN Integer, AND THE SUM STILL IN THE int RANGE: Type.increment() ANSWERS AN Integer
  private static final byte INTEGER = 1;
  // EVERY VALUE AN Integer OR A Long: Type.increment() ANSWERS A Long
  private static final byte LONG    = 2;
  // A Double MET Integer/Long/Double VALUES ONLY: Type.increment() ANSWERS A Double
  private static final byte DOUBLE  = 3;
  // ANY OTHER COMBINATION: THE SUM IS boxedSum, AND EVERY VALUE GOES THROUGH Type.increment()
  private static final byte BOXED   = 4;

  private byte   sumMode = EMPTY;
  private long   longSum;
  private double doubleSum;
  private Number boxedSum;

  protected SQLFunctionRunningSumAbstract(final String name) {
    super(name);
  }

  /** Adds {@code value} to the running sum. A null is ignored. */
  protected void addToSum(final Number value) {
    if (value == null)
      return;

    switch (sumMode) {
    case EMPTY -> {
      if (value instanceof Double d) {
        doubleSum = d;
        sumMode = DOUBLE;
      } else if (value instanceof Integer i) {
        longSum = i;
        sumMode = INTEGER;
      } else if (value instanceof Long l) {
        longSum = l;
        sumMode = LONG;
      } else {
        boxedSum = value;
        sumMode = BOXED;
      }
    }
    case DOUBLE -> {
      if (value instanceof Double d)
        doubleSum += d;
      else if (value instanceof Integer i)
        doubleSum += i;
      else if (value instanceof Long l)
        doubleSum += l;
      else
        addBoxed(value);
    }
    case INTEGER, LONG -> {
      if (value instanceof Integer || value instanceof Long) {
        final long v = value.longValue();
        final long result = longSum + v;
        if (((longSum ^ result) & (v ^ result)) < 0) {
          // A long OVERFLOW: Type.increment() WIDENS TO A BigDecimal, AND THE SUM GOES ON FROM THERE
          addBoxed(value);
          return;
        }
        longSum = result;
        // Integer + Integer LEAVING THE int RANGE AND Integer + Long BOTH ANSWER A Long, AND A Long STAYS ONE
        if (sumMode == INTEGER && (value instanceof Long || result != (int) result))
          sumMode = LONG;
      } else if (value instanceof Double d) {
        doubleSum = longSum + d;
        sumMode = DOUBLE;
      } else
        addBoxed(value);
    }
    default -> boxedSum = Type.increment(boxedSum, value);
    }
  }

  /** Adds the running sum of {@code other}, an instance fed other rows. */
  protected void addToSum(final SQLFunctionRunningSumAbstract other) {
    switch (other.sumMode) {
    case EMPTY -> {
    }
    case DOUBLE -> addToSum(other.doubleSum);
    case INTEGER -> addToSum((int) other.longSum);
    case LONG -> addToSum(other.longSum);
    default -> addToSum(other.boxedSum);
    }
  }

  /** The running sum, or null when no value was added. */
  protected Number getSum() {
    return switch (sumMode) {
      case EMPTY -> null;
      case INTEGER -> (int) longSum;
      case LONG -> longSum;
      case DOUBLE -> doubleSum;
      default -> boxedSum;
    };
  }

  private void addBoxed(final Number value) {
    boxedSum = Type.increment(getSum(), value);
    sumMode = BOXED;
  }
}
