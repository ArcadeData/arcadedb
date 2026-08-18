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

import java.util.IllegalFormatException;

/**
 * String helpers that the engine needs on paths where the obvious form allocates.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class StringUtils {
  protected StringUtils() {
  }

  /**
   * Case-insensitive {@code contains} that allocates nothing.
   * <p>
   * The obvious form, {@code haystack.toUpperCase().contains(needle)}, copies the whole haystack to answer a question
   * about a needle a dozen characters long - which is why every caller of this had written its own copy rather than
   * use it: the async DDL classifier scanning a whole script, the Cypher parser deciding on its own parse hot path,
   * the HTTP handler looking for a LIMIT in a command, and the MCP prompt fence looking for its own delimiter. Four
   * private copies of six lines, and this is the one they share.
   *
   * @return true when {@code needle} occurs anywhere in {@code haystack}, ignoring case. An empty needle is contained
   *     by everything, as {@code String.contains} has it.
   */
  public static boolean containsIgnoreCase(final String haystack, final String needle) {
    final int needleLength = needle.length();
    if (needleLength == 0)
      return true;

    final int last = haystack.length() - needleLength;
    for (int i = 0; i <= last; i++)
      if (haystack.regionMatches(true, i, needle, 0, needleLength))
        return true;

    return false;
  }

  /**
   * Widest field a {@code %<width>s`-style} conversion may ask for in a user-supplied format. Deliberately generous -
   * a padded report column is nowhere near it - and the point is only that {@code format('%99999999s', 'x')} must not
   * turn eight characters of query text into a 100MB string (issue #6389).
   */
  public static final int MAX_FORMAT_WIDTH = 1_000_000;

  /**
   * Applies a user-supplied {@link java.util.Formatter} pattern, answering a typed argument error for everything the
   * JDK reports by throwing.
   * <p>
   * {@code String.format} raises a family of unchecked exceptions for input that is simply wrong - a conversion that
   * does not match its argument ({@code format('%d','x')}), a conversion that does not exist ({@code format('%y')}),
   * a null pattern - and every one of them used to escape the SQL {@code format()} function and its method twin as a
   * raw JDK exception, i.e. an HTTP 500 for a mistake in the query (issue #6389). The width ceiling is checked BEFORE
   * formatting, because that one is not an error the JDK reports at all: it allocates.
   *
   * @param caller the function or method name, for the error message
   * @param format the caller-supplied format pattern
   * @param args   the format arguments
   *
   * @return the formatted string
   *
   * @throws IllegalArgumentException if the pattern is null, malformed, mismatched or asks for an excessive width
   */
  public static String format(final String caller, final String format, final Object... args) {
    if (format == null)
      throw new IllegalArgumentException(caller + "() requires a non-null format");

    checkFormatWidth(caller, format);

    try {
      return String.format(format, args);
    } catch (final IllegalFormatException e) {
      throw new IllegalArgumentException(
          caller + "() cannot apply the format '" + format + "': " + e.getClass().getSimpleName() + " - " + e.getMessage(), e);
    }
  }

  /**
   * Rejects a conversion whose width or precision exceeds {@link #MAX_FORMAT_WIDTH}. Scans the pattern once rather
   * than compiling a regex: this runs per formatted value.
   */
  private static void checkFormatWidth(final String caller, final String format) {
    for (int i = 0; i < format.length(); i++) {
      if (format.charAt(i) != '%')
        continue;

      // Skip the argument index, the flags, then read the width - and, past a '.', the precision - as plain digits.
      // A number too long to be an int is over the ceiling by definition, so digits are counted rather than parsed.
      int j = i + 1;
      long number = 0;
      int digits = 0;
      while (j < format.length()) {
        final char c = format.charAt(j);
        if (c >= '0' && c <= '9') {
          if (++digits <= 10)
            number = number * 10 + (c - '0');
          else
            number = Long.MAX_VALUE;
        } else if (c == '$' || c == '.') {
          // '$' closed an argument index and '.' opens the precision: either way the digits read so far were not a
          // width, so start counting again.
          number = 0;
          digits = 0;
        } else if (c != '-' && c != '#' && c != '+' && c != ' ' && c != '0' && c != ',' && c != '(') {
          // Not a flag either: the conversion character, so the specifier ends here.
          break;
        }
        j++;
      }

      if (number > MAX_FORMAT_WIDTH)
        throw new IllegalArgumentException(
            caller + "() format '" + format + "' asks for a field of " + number + " characters, over the " + MAX_FORMAT_WIDTH
                + " limit");

      i = j;
    }
  }
}
