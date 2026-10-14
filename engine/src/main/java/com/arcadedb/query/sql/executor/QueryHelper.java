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
package com.arcadedb.query.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.utility.TimeBoundRegex;

public class QueryHelper {
  protected static final char WILDCARD_ANYCHAR = '?';
  protected static final char WILDCARD_ANY     = '%';
  // THE DEADLINE IS CHECKED EVERY 4096 STEPS OF A LIKE MATCH: System.nanoTime() IS NOT FREE, AND A MATCH THAT TAKES
  // FEWER STEPS ENDS ESSENTIALLY AT ONCE
  private static final int  DEADLINE_CHECK_INTERVAL_MASK = 4095;
  // A PATTERN IS USER INPUT OF ANY LENGTH: THE TIMEOUT MESSAGE QUOTES ONLY ITS START
  private static final int  MAX_PATTERN_IN_MESSAGE       = 100;

  /**
   * Answers {@code currentValue LIKE value}: {@code %} matches any sequence of characters, the empty one included,
   * {@code ?} exactly one character (a code point, so a surrogate pair is one character), and {@code \%} and
   * {@code \?} the literal characters. A backslash before anything else, or closing the pattern, is a literal backslash,
   * and every other character of the pattern stands for itself. A {@code null} operand never matches.
   * <p>
   * The pattern is matched as it is, with no translation to a regular expression, so nothing is compiled per evaluated
   * row, and a pattern made of many {@code %} segments, which made the {@code .*} translation backtrack
   * exponentially (issue #5886), costs at most {@code currentValue.length() * value.length()} steps here, since the last
   * {@code %} met is the only choice ever revisited. That bound is still honoured against the deadline, so a huge value
   * matched against a long pattern cannot run unbounded either.
   *
   * @param timeoutMillis maximum time allowed for the match, in milliseconds; a value {@code <= 0} disables the
   *                      bound
   *
   * @throws TimeoutException if the match does not complete within {@code timeoutMillis}
   */
  public static boolean like(final String currentValue, final String value, final long timeoutMillis) {
    return likeUntil(currentValue, value, TimeBoundRegex.newDeadline(timeoutMillis));
  }

  /**
   * Same as {@link #like(String, String, long)}, but against an absolute deadline shared across a series of
   * related matches (e.g. every item of a multi-value property matched against the same pattern) rather than
   * each getting its own full timeout budget - see {@link TimeBoundRegex#newDeadline(long)}.
   *
   * @throws TimeoutException if the match does not complete before {@code deadlineNanos}
   */
  public static boolean likeUntil(final String currentValue, final String value, final long deadlineNanos) {
    if (currentValue == null || value == null)
      return false;

    final int valueLength = currentValue.length();
    final int patternLength = value.length();
    int v = 0;
    int p = 0;
    // WHERE THE LAST % MET RESUMES THE PATTERN, AND THE POSITION IN THE VALUE ITS MATCH ENDS AT: ON A MISMATCH THAT %
    // TAKES ONE MORE CHARACTER, AND NO EARLIER % EVER NEEDS TO BE REVISITED
    int afterAny = -1;
    int anyEnd = 0;
    int steps = 0;

    while (v < valueLength) {
      if ((++steps & DEADLINE_CHECK_INTERVAL_MASK) == 0 && deadlineNanos != Long.MAX_VALUE && System.nanoTime() - deadlineNanos > 0)
        throw new TimeoutException("LIKE operation aborted (arcadedb.command.regexTimeout): pattern '" + (patternLength > MAX_PATTERN_IN_MESSAGE ?
            value.substring(0, MAX_PATTERN_IN_MESSAGE) + "...' of length " + patternLength :
            value + "'") + " against input of length " + valueLength);

      if (p < patternLength) {
        final char c = value.charAt(p);
        if (c == WILDCARD_ANY) {
          afterAny = ++p;
          anyEnd = v;
          continue;
        }
        if (c == WILDCARD_ANYCHAR) {
          v += Character.charCount(currentValue.codePointAt(v));
          ++p;
          continue;
        }
        char literal = c;
        int literalLength = 1;
        if (c == '\\' && p + 1 < patternLength) {
          final char next = value.charAt(p + 1);
          if (next == WILDCARD_ANY || next == WILDCARD_ANYCHAR) {
            literal = next;
            literalLength = 2;
          }
        }
        // A LITERAL IS COMPARED ONE UTF-16 UNIT AT A TIME: THE TWO HALVES OF A PAIR IN THE PATTERN MATCH THE PAIR IN THE VALUE,
        // AND ONLY A PATTERN HOLDING HALF A PAIR, WHICH IS NO CHARACTER AT ALL, COULD MATCH HALF OF ONE
        if (currentValue.charAt(v) == literal) {
          ++v;
          p += literalLength;
          continue;
        }
      }

      if (afterAny < 0)
        return false;

      anyEnd += Character.charCount(currentValue.codePointAt(anyEnd));
      v = anyEnd;
      p = afterAny;
    }

    // THE VALUE IS CONSUMED: WHAT IS LEFT OF THE PATTERN MATCHES IT ONLY IF IT IS MADE OF % ALONE
    while (p < patternLength && value.charAt(p) == WILDCARD_ANY)
      ++p;
    return p == patternLength;
  }

  /**
   * A string that sorts after every string starting with {@code prefix}: the prefix with its last code point that can be
   * bumped replaced by the next one, so {@code [prefix, successor)} holds every key that starts with the prefix, and
   * nothing that sorts before it, whether an index orders its string keys by UTF-16 unit or by UTF-8 byte (issue #8666).
   * <p>
   * Two consecutive code points sort the same way in both orders, but the step from U+D7FF (to a surrogate), from U+FFFF
   * (to a supplementary character, which UTF-16 sorts below it) and from U+10FFFF (to nothing) does not stay inside one
   * order. A code point that cannot be bumped, or a lone surrogate, is dropped and the one before it bumped instead: the
   * range only grows, which a caller that keeps the original predicate as a filter never notices.
   *
   * @return the exclusive upper bound, or null when nothing bounds the range from above: an empty prefix, or one made of
   * such code points alone
   */
  public static String prefixSuccessor(final String prefix) {
    int end = prefix.length();
    while (end > 0) {
      final int codePoint = prefix.codePointBefore(end);
      final int start = end - Character.charCount(codePoint);
      if (!(codePoint >= 0xD800 && codePoint <= 0xDFFF) && codePoint != 0xD7FF && codePoint != 0xFFFF && codePoint != Character.MAX_CODE_POINT)
        return prefix.substring(0, start) + new String(Character.toChars(codePoint + 1));
      end = start;
    }
    return null;
  }

  /**
   * The characters every match of a {@code LIKE} pattern starts with: those before the first wildcard, read the way
   * {@link #like(String, String, long)} reads them ({@code \%} and {@code \?} are the literal characters, a backslash
   * before anything else is a literal backslash). Empty when the pattern opens with a wildcard.
   */
  public static String likeLiteralPrefix(final String pattern) {
    final StringBuilder prefix = new StringBuilder();
    for (int i = 0; i < pattern.length(); i++) {
      final char c = pattern.charAt(i);
      if (c == WILDCARD_ANY || c == WILDCARD_ANYCHAR)
        break;
      if (c == '\\' && i + 1 < pattern.length() && (pattern.charAt(i + 1) == WILDCARD_ANY || pattern.charAt(i + 1) == WILDCARD_ANYCHAR))
        prefix.append(pattern.charAt(++i));
      else
        prefix.append(c);
    }
    return prefix.toString();
  }
}
