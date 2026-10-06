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
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * JDK17: {@link Float#toString(float)} and {@link Double#toString(double)} as specified since Java 19 (JDK-4511638),
 * which render the SHORTEST decimal that rounds back to the value. Java 17 can render a longer one - {@code 1.0E16f}
 * prints as {@code 1.00000003E16} - and the engine reads a float's decimal as "the number its user wrote" when it
 * compares it against a double, a long or a decimal.
 * <p>
 * On a Java 19+ runtime both methods delegate to the JDK. On Java 17 they search the shortest decimal with
 * {@link BigDecimal}, following the Java 19 specification: the shortest length n among the decimals that round to the
 * value, at least 2; among those of length n the one closest to the value, ties to the even digit; rendered in the
 * same plain or computerized scientific notation as the JDK.
 */
public final class ShortestDecimal {
  private static final boolean JDK_IS_SHORTEST = Runtime.version().feature() >= 19;

  private ShortestDecimal() {
  }

  public static String toString(final float value) {
    if (JDK_IS_SHORTEST || !Float.isFinite(value) || value == 0F)
      return Float.toString(value);
    final BigDecimal shortest = search(new BigDecimal(value), 9, value, true);
    return render(shortest, value < 0F);
  }

  public static String toString(final double value) {
    if (JDK_IS_SHORTEST || !Double.isFinite(value) || value == 0D)
      return Double.toString(value);
    final BigDecimal shortest = search(new BigDecimal(value), 17, value, false);
    return render(shortest, value < 0D);
  }

  private static BigDecimal search(final BigDecimal exact, final int maxDigits, final double value, final boolean isFloat) {
    for (int length = 1; length <= maxDigits; length++) {
      if (pick(exact, length, value, isFloat) == null)
        continue;
      // THE SPECIFICATION CONSIDERS LENGTH 2 WHEN THE SHORTEST IS 1 DIGIT LONG: 1.4E-45, NOT 1.0E-45
      return pick(exact, Math.max(length, 2), value, isFloat);
    }
    // UNREACHABLE: maxDigits DIGITS ALWAYS IDENTIFY A FLOAT (9) OR A DOUBLE (17)
    return exact;
  }

  private static BigDecimal pick(final BigDecimal exact, final int length, final double value, final boolean isFloat) {
    final BigDecimal down = exact.round(new MathContext(length, RoundingMode.DOWN));
    final BigDecimal up = exact.round(new MathContext(length, RoundingMode.UP));
    final boolean downOk = roundsBack(down, value, isFloat);
    final boolean upOk = roundsBack(up, value, isFloat);
    if (downOk && upOk) {
      if (down.compareTo(up) == 0)
        return down;
      final int cmp = exact.subtract(down).abs().compareTo(up.subtract(exact).abs());
      if (cmp != 0)
        return cmp < 0 ? down : up;
      return down.unscaledValue().testBit(0) ? up : down;
    }
    return downOk ? down : upOk ? up : null;
  }

  private static boolean roundsBack(final BigDecimal candidate, final double value, final boolean isFloat) {
    return isFloat ? candidate.floatValue() == (float) value : candidate.doubleValue() == value;
  }

  /** The JDK layout: plain for 10^-3 <= |v| < 10^7, otherwise d.dddE[-]n, always with at least one fraction digit. */
  private static String render(final BigDecimal decimal, final boolean negative) {
    final BigDecimal abs = decimal.abs().stripTrailingZeros();
    final String digits = abs.unscaledValue().toString();
    // DECIMAL EXPONENT OF THE FIRST DIGIT
    final int exponent = digits.length() - 1 - abs.scale();
    final StringBuilder out = new StringBuilder(digits.length() + 8);
    if (negative)
      out.append('-');
    if (exponent >= -3 && exponent < 7) {
      if (exponent >= 0) {
        if (digits.length() <= exponent + 1) {
          out.append(digits);
          for (int i = digits.length(); i <= exponent; i++)
            out.append('0');
          out.append(".0");
        } else
          out.append(digits, 0, exponent + 1).append('.').append(digits, exponent + 1, digits.length());
      } else {
        out.append("0.");
        for (int i = -1; i > exponent; i--)
          out.append('0');
        out.append(digits);
      }
    } else {
      out.append(digits.charAt(0)).append('.');
      if (digits.length() > 1)
        out.append(digits, 1, digits.length());
      else
        out.append('0');
      out.append('E').append(exponent);
    }
    return out.toString();
  }
}
