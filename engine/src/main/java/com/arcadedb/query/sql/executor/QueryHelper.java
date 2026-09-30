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

import com.arcadedb.utility.TimeBoundRegex;

import java.util.regex.Pattern;

public class QueryHelper {
  protected static final char WILDCARD_ANYCHAR = '?';
  protected static final char WILDCARD_ANY     = '%';

  /**
   * A sequence of several {@code %}/{@code ?} wildcards (translated to {@code .*}/{@code .} by {@link
   * #convertForRegExp(String)}) reproduces the same catastrophic-backtracking shape as the issue #5886
   * reproducer even without grouping, alternation, or nested quantifiers - a pattern like {@code %a%a%a%a%a%}
   * against input that does not fully match it hangs exactly like {@code (.*a){20}$} does. Bounded the same
   * way as {@code MATCHES}/{@code =~}: see {@link TimeBoundRegex}.
   *
   * @param timeoutMillis maximum time allowed for the match, in milliseconds; a value {@code <= 0} disables the
   *                      bound
   */
  public static boolean like(final String currentValue, final String value, final long timeoutMillis) {
    return likeUntil(currentValue, value, TimeBoundRegex.newDeadline(timeoutMillis));
  }

  /**
   * Same as {@link #like(String, String, long)}, but against an absolute deadline shared across a series of
   * related matches (e.g. every item of a multi-value property matched against the same pattern) rather than
   * each getting its own full timeout budget - see {@link TimeBoundRegex#matchesUntil(Pattern, CharSequence,
   * long)}.
   */
  public static boolean likeUntil(String currentValue, String value, final long deadlineNanos) {
    if (currentValue == null || value == null || (currentValue.isEmpty() ^ value.isEmpty()))
      // EMPTY/NULL PARAMETERS
      return false;
    else if (currentValue.isEmpty() && value.isEmpty())
      return true;

    value = convertForRegExp(value);
    return TimeBoundRegex.matchesUntil(Pattern.compile(value), currentValue, deadlineNanos);
  }

  public static String convertForRegExp(String value) {

    final StringBuilder sb = new StringBuilder("(?s)");

    for (int i = 0; i < value.length();++i) {

      final char c = value.charAt(i);
      switch (c) {
      case '\\':
        if (i + 1 < value.length()) {
          final char next = value.charAt(i + 1);
          if (next == WILDCARD_ANY) {
            sb.append(next);
            ++i;
            break;
          } else if (next == WILDCARD_ANYCHAR) {
            sb.append("\\" + next);
            ++i;
            break;
          }
        }
      case '[':
      case ']':
      case '{':
      case '}':
      case '(':
      case ')':
      case '|':
      case '*':
      case '+':
      case '$':
      case '^':
      case '.':
        sb.append("\\" + c);
        break;
      case WILDCARD_ANYCHAR:
        sb.append(".");
        break;
      case WILDCARD_ANY:
        sb.append(".*");
        break;
      default:
        sb.append(c);
        break;
      }
    }
    return sb.toString();
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
   * {@link #convertForRegExp(String)} reads them ({@code \%} and {@code \?} are the literal characters, a backslash
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
