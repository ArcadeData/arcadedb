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
package com.arcadedb.index;

import com.arcadedb.serializer.BinaryTypes;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Tells when a bound cannot be held by a floating point index key (DOUBLE, FLOAT) and widens it to the keys that can answer for it
 * (issue #8970).
 * <p>
 * An index converts a bound to its key type before it seeks, and rounds a bound it cannot hold: {@code 2^53 + 1} becomes
 * {@code 2^53} on a DOUBLE, a {@code BigDecimal} becomes the double nearest to it. A scan compares the exact values instead, so the
 * index answered for another bound than the scan. The planner leaves a literal bound like that to the scan, but a plan with a
 * parameter is cached and the decision depends on the value, so it is taken when the step runs.
 * <p>
 * The key a bound rounds to is at most one step away from the keys the exact comparison selects, so the seek is widened by one
 * representable value on each side (inclusive) and the caller checks each entry it gets against the original condition. Unlike
 * {@link IntegralKeyBound} there is no exact key to map to: the scan reads a {@code BigDecimal} of up to 17 significant digits as
 * the double it narrows to, which the check reuses instead of repeating.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class FloatingKeyBound {
  /** 2^53: up to it a {@code double} holds every integer exactly. */
  private static final long EXACT_DOUBLE_LIMIT = 1L << 53;
  /** 2^24: up to it a {@code float} holds every integer exactly. */
  private static final long EXACT_FLOAT_LIMIT  = 1L << 24;

  private FloatingKeyBound() {
  }

  /** True when {@code keyType} is DOUBLE or FLOAT. */
  public static boolean isFloating(final byte keyType) {
    return keyType == BinaryTypes.TYPE_DOUBLE || keyType == BinaryTypes.TYPE_FLOAT;
  }

  /**
   * True when {@code bound} is a number the floating point key type {@code keyType} cannot hold: an integer past the range a
   * double (2^53) or a float (2^24) holds exactly, or a {@code BigDecimal} that is not exactly a double (a float). A {@code Double}
   * or {@code Float} bound is not: it is read as the key it narrows to, on both sides (issue #8882). Allocates nothing for an
   * integer or a floating point bound.
   */
  public static boolean isLossy(final byte keyType, final Object bound) {
    if (!isFloating(keyType))
      return false;
    final boolean isDouble = keyType == BinaryTypes.TYPE_DOUBLE;
    final long limit = isDouble ? EXACT_DOUBLE_LIMIT : EXACT_FLOAT_LIMIT;
    if (bound instanceof Long value)
      return value > limit || value < -limit;
    if (bound instanceof Integer value)
      return !isDouble && (value > limit || value < -limit);
    if (bound instanceof BigInteger value)
      return value.bitLength() > 63 || value.longValue() > limit || value.longValue() < -limit;
    if (bound instanceof BigDecimal value) {
      final double rounded = isDouble ? value.doubleValue() : value.floatValue();
      // an overflow to an infinity holds no exact value either
      return Double.isInfinite(rounded) || new BigDecimal(rounded).compareTo(value) != 0;
    }
    return false;
  }

  /** The key one step below the one {@code bound} rounds to: the lower end of a seek that holds every key the bound can select. */
  public static Number below(final byte keyType, final Object bound) {
    final Number number = (Number) bound;
    // not a ternary: the two branches would be promoted to one double type
    if (keyType == BinaryTypes.TYPE_DOUBLE)
      return Math.nextDown(number.doubleValue());
    return Math.nextDown(number.floatValue());
  }

  /** The key one step above the one {@code bound} rounds to: the upper end of a seek that holds every key the bound can select. */
  public static Number above(final byte keyType, final Object bound) {
    final Number number = (Number) bound;
    if (keyType == BinaryTypes.TYPE_DOUBLE)
      return Math.nextUp(number.doubleValue());
    return Math.nextUp(number.floatValue());
  }
}
