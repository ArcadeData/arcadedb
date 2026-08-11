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
package com.arcadedb.utility;

import java.math.BigDecimal;
import java.math.BigInteger;

public class NumberUtils {

  private static final BigInteger BIGINTEGER_MAX_INT = BigInteger.valueOf(Integer.MAX_VALUE);
  private static final BigInteger BIGINTEGER_MIN_INT = BigInteger.valueOf(Integer.MIN_VALUE);
  private static final BigDecimal BIGDECIMAL_MAX_INT = BigDecimal.valueOf(Integer.MAX_VALUE);
  private static final BigDecimal BIGDECIMAL_MIN_INT = BigDecimal.valueOf(Integer.MIN_VALUE);

  public static Integer parsePositiveInteger(final String s) {
    for (int i = 0; i < s.length(); i++) {
      if (!Character.isDigit(s.charAt(i)))
        return null;
    }
    return Integer.parseInt(s);
  }

  public static boolean isIntegerNumber(final String s) {
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      if (!Character.isDigit(c) && c != '+' && s.charAt(i) != '-')
        return false;
    }
    return true;
  }

  /**
   * Narrows a numeric value to an int without wrapping: a value outside the int range saturates to
   * the nearest int bound instead of overflowing into an unrelated value. `BigInteger`/`BigDecimal`
   * are compared by magnitude directly, because `Number.longValue()` on one of those far outside the
   * `Long` range is documented as lossy in an unspecified way (it can even flip sign).
   */
  public static int saturateToInt(final Number value) {
    if (value instanceof BigInteger bigInteger) {
      if (bigInteger.compareTo(BIGINTEGER_MAX_INT) > 0)
        return Integer.MAX_VALUE;
      if (bigInteger.compareTo(BIGINTEGER_MIN_INT) < 0)
        return Integer.MIN_VALUE;
      return bigInteger.intValue();
    }

    if (value instanceof BigDecimal bigDecimal) {
      if (bigDecimal.compareTo(BIGDECIMAL_MAX_INT) > 0)
        return Integer.MAX_VALUE;
      if (bigDecimal.compareTo(BIGDECIMAL_MIN_INT) < 0)
        return Integer.MIN_VALUE;
      return bigDecimal.intValue();
    }

    final long longValue = value.longValue();
    if (longValue > Integer.MAX_VALUE)
      return Integer.MAX_VALUE;
    if (longValue < Integer.MIN_VALUE)
      return Integer.MIN_VALUE;
    return (int) longValue;
  }

  /**
   * Narrows a numeric value to an int, throwing {@link ArithmeticException} if it does not fit -
   * the strict counterpart to {@link #saturateToInt(Number)}. Use this for parameters where an
   * out-of-range value should fail loudly rather than saturate or wrap, because a saturated/wrapped
   * value would still look plausible and silently drive a degenerate computation (e.g. an iteration
   * count, embedding dimension, or cluster count) instead of an obviously-bounded result (e.g. LIMIT).
   * `BigInteger`/`BigDecimal` are compared by magnitude directly for the same reason as
   * {@link #saturateToInt(Number)}: `Number.longValue()` on one of those far outside the `Long` range
   * is documented as lossy in an unspecified way.
   */
  public static int toIntExact(final Number value) {
    if (value instanceof BigInteger bigInteger) {
      if (bigInteger.compareTo(BIGINTEGER_MAX_INT) > 0 || bigInteger.compareTo(BIGINTEGER_MIN_INT) < 0)
        throw new ArithmeticException("integer overflow: " + bigInteger);
      return bigInteger.intValue();
    }

    if (value instanceof BigDecimal bigDecimal) {
      if (bigDecimal.compareTo(BIGDECIMAL_MAX_INT) > 0 || bigDecimal.compareTo(BIGDECIMAL_MIN_INT) < 0)
        throw new ArithmeticException("integer overflow: " + bigDecimal);
      return bigDecimal.intValue();
    }

    return Math.toIntExact(value.longValue());
  }
}
