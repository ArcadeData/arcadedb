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
package com.arcadedb.integration.importer;

/**
 * When a source's text IS a number, for the importers that decide it themselves: the type inference of
 * {@link AnalyzedProperty} and the vertex identities of {@code GraphImporter}.
 * <p>
 * The question is "is this the decimal text of a number", not "can Java parse it". {@code Long.parseLong} and
 * {@code Double.parseDouble} accept Java's source-literal grammar, which admits a leading {@code +}, leading zeros, a
 * {@code f}/{@code d} suffix, the hexadecimal {@code 0x1p3} form, {@code NaN}, {@code Infinity} and surrounding
 * whitespace. A column inferred numeric through them rewrote {@code 10D} as {@code 10.0} and the ZIP code
 * {@code 02134} as {@code 2134}, and the original text was kept nowhere (issue #10755). Every method here answers
 * without allocating or throwing, so it can run on every value of a column.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CanonicalNumber {
  /**
   * What {@link #parseLong(String)} answers for a text that is not the canonical decimal form of a {@code long}. It
   * is {@code Long.MIN_VALUE}, so that value's own text answers it too: {@link #isLong(String)} tells them apart.
   */
  public static final long NOT_CANONICAL_LONG = Long.MIN_VALUE;

  private static final String MIN_LONG_TEXT = Long.toString(Long.MIN_VALUE);

  private CanonicalNumber() {
  }

  /**
   * Reads {@code text} as the canonical decimal form of a {@code long}, returning {@link #NOT_CANONICAL_LONG} when it
   * is not one. Neither allocates nor throws: an identity column is read once per row, and {@code Long.parseLong} in a
   * {@code try} block would fill in a stack trace for every row of a string-keyed file.
   * <p>
   * "Canonical" is the form {@code Long.toString} would produce: {@code "007"} and {@code "7"} are two different
   * values of a column - a ZIP code, an identity - so only that form is allowed to become a number. Everything else -
   * leading zeros, a leading {@code +}, {@code "-0"}, surrounding space, anything non-numeric - stays text.
   */
  public static long parseLong(final String text) {
    if (text == null)
      return NOT_CANONICAL_LONG;
    final int len = text.length();
    if (len == 0)
      return NOT_CANONICAL_LONG;

    final boolean negative = text.charAt(0) == '-';
    final int first = negative ? 1 : 0;
    if (len - first < 1 || len - first > 19)
      return NOT_CANONICAL_LONG;

    final char firstDigit = text.charAt(first);
    if (firstDigit < '0' || firstDigit > '9')
      return NOT_CANONICAL_LONG;
    // "0" is canonical, "00" and "007" are not, and "-0" is not the canonical form of zero
    if (firstDigit == '0' && (len - first > 1 || negative))
      return NOT_CANONICAL_LONG;

    // Accumulate negatively: the negative range is the wider one, so this overflows only on values
    // that genuinely do not fit, and Long.MIN_VALUE itself is rejected because it is the sentinel
    long value = 0;
    for (int i = first; i < len; i++) {
      final char c = text.charAt(i);
      if (c < '0' || c > '9')
        return NOT_CANONICAL_LONG;
      if (value < -922337203685477580L)
        return NOT_CANONICAL_LONG;
      value *= 10;
      final int digit = c - '0';
      if (value < Long.MIN_VALUE + digit)
        return NOT_CANONICAL_LONG;
      value -= digit;
    }
    if (value == NOT_CANONICAL_LONG)
      return NOT_CANONICAL_LONG;
    return negative ? value : -value;
  }

  /**
   * Whether {@code text} is the canonical decimal form of a {@code long}, {@code Long.MIN_VALUE} included.
   */
  public static boolean isLong(final String text) {
    return parseLong(text) != NOT_CANONICAL_LONG || MIN_LONG_TEXT.equals(text);
  }

  /**
   * Whether {@code text} is a plain decimal number whose value a {@code double} holds without changing what the text
   * says beyond the rounding of its fraction:
   * <pre>
   *   decimal  := '-'? integer ( '.' digit+ )? ( ('e' | 'E') ('+' | '-')? digit+ )?
   *   integer  := '0' | [1-9] digit*
   * </pre>
   * That is the decimal and scientific notation a data file uses, and none of Java's literal extras. Two more
   * refusals keep a value from changing silently: an integer with no fraction and no exponent that does not fit a
   * {@code long} (a 20-digit identifier, which a {@code double} would round), and a value whose magnitude a
   * {@code double} cannot represent ({@code 1e400}, which would become {@code Infinity}).
   */
  public static boolean isDecimal(final String text) {
    if (text == null)
      return false;
    final int len = text.length();
    int i = 0;
    if (i < len && text.charAt(i) == '-')
      ++i;

    // INTEGER PART: '0' ALONE, OR A NON-ZERO DIGIT FOLLOWED BY DIGITS
    final int integerStart = i;
    if (i >= len)
      return false;
    if (text.charAt(i) == '0')
      ++i;
    else
      while (i < len && isDigit(text.charAt(i)))
        ++i;
    final int integerDigits = i - integerStart;
    if (integerDigits == 0)
      return false;

    boolean integral = true;

    // FRACTION: AT LEAST ONE DIGIT AFTER THE DOT, SO "1." IS NOT A NUMBER
    if (i < len && text.charAt(i) == '.') {
      integral = false;
      final int fractionStart = ++i;
      while (i < len && isDigit(text.charAt(i)))
        ++i;
      if (i == fractionStart)
        return false;
    }

    // EXPONENT
    boolean exponent = false;
    if (i < len && (text.charAt(i) == 'e' || text.charAt(i) == 'E')) {
      integral = false;
      exponent = true;
      ++i;
      if (i < len && (text.charAt(i) == '+' || text.charAt(i) == '-'))
        ++i;
      final int exponentStart = i;
      while (i < len && isDigit(text.charAt(i)))
        ++i;
      if (i == exponentStart)
        return false;
    }

    if (i != len)
      return false;

    if (integral)
      // AN INTEGER IS A DECIMAL ONLY WHEN IT IS ALSO A LONG: A LARGER ONE IS AN IDENTIFIER A DOUBLE WOULD ROUND
      return isLong(text);

    // ONLY AN EXPONENT OR AN INTEGER PART OF MORE THAN 308 DIGITS CAN TAKE THE MAGNITUDE PAST WHAT A DOUBLE HOLDS, SO
    // THE TEXT, NOW KNOWN TO BE IN THE STRICT GRAMMAR, IS PARSED ONLY THEN
    return (!exponent && integerDigits <= 308) || Double.isFinite(Double.parseDouble(text));
  }

  private static boolean isDigit(final char c) {
    return c >= '0' && c <= '9';
  }
}
