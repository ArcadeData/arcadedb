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
package com.arcadedb.function.text;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.utility.TimeBoundRegex;

import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Abstract base class for text functions.
 * All text functions share the "text." namespace prefix.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractTextFunction implements StatelessFunction {
  protected static final String NAMESPACE           = "text";
  /**
   * The longest regular expression a text function compiles, against patterns built to backtrack (ReDoS).
   */
  protected static final int    MAX_PATTERN_LENGTH = 500;
  /**
   * The longest string a text function that generates one from a length argument (pad, random) produces: a length is a few characters
   * of query text and must not become a heap-sized allocation (issue #12727).
   */
  protected static final int    MAX_RESULT_LENGTH  = 10 * 1024 * 1024;

  /**
   * A pattern with the text it was compiled from. Immutable, so that the field below is read and replaced without a lock.
   */
  private record CompiledRegex(String regex, Pattern pattern) {
  }

  private volatile CompiledRegex lastRegex;

  /**
   * Returns the simple name without namespace prefix.
   */
  protected abstract String getSimpleName();

  @Override
  public String getName() {
    return NAMESPACE + "." + getSimpleName();
  }

  /**
   * Safely converts an argument to String, handling null values.
   */
  protected String asString(final Object arg) {
    return arg == null ? null : arg.toString();
  }

  /**
   * The characters of a string, one {@code int} each: a character outside the BMP is one element, not the two UTF-16 units of its
   * surrogate pair. A lone surrogate is a character of its own.
   */
  protected static int[] codePointsOf(final String s) {
    final int length = s.length();
    final int[] codePoints = new int[s.codePointCount(0, length)];
    for (int i = 0, n = 0; i < length; n++) {
      final int codePoint = s.codePointAt(i);
      codePoints[n] = codePoint;
      i += Character.charCount(codePoint);
    }
    return codePoints;
  }

  /**
   * Reads an int argument, with a default value for null. A number has to be a whole one in the int range: intValue()
   * read 2.9 as 2 and wrapped 4294967298 to 2, so a wrong argument answered as a plausible one (issue #10850). A text is
   * parsed as a whole number too, and one that is not is the same client error (issue #10954).
   */
  protected int asInt(final Object arg, final int defaultValue, final String argumentName) {
    if (arg == null)
      return defaultValue;
    return CypherFunctionHelper.requireWholeIntOrTextArgument(arg, getName(), argumentName);
  }

  /**
   * Compiles {@code regex} and runs {@code operation} with it over {@code text}, bounded by the regex deadline of the query (issue
   * #5886): one deadline is shared by every row the function runs against in the same query, so a pathological pattern cannot cost
   * the row count times the timeout. {@code context} is null in some direct invocations, which then get a deadline of their own. The
   * operation has to read the text only through the sequence it is given, which is what carries the deadline.
   * <p>
   * The pattern of the last call is kept, as a query runs the function with the same pattern for every row.
   *
   * @throws IllegalArgumentException when the pattern is too long, does not compile, or backtracks past the deadline
   */
  protected <T> T applyRegex(final String regex, final String text, final CommandContext context,
      final BiFunction<Pattern, CharSequence, T> operation) {
    if (regex.length() > MAX_PATTERN_LENGTH)
      throw new IllegalArgumentException(
          "Regex pattern exceeds maximum allowed length (" + MAX_PATTERN_LENGTH + "): " + regex.length());

    final long regexDeadline = context != null ?
        context.getRegexDeadline() :
        TimeBoundRegex.newDeadline(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong(null));

    final Pattern pattern = compile(regex);

    try {
      return TimeBoundRegex.applyUntil(pattern, text, regexDeadline, bounded -> operation.apply(pattern, bounded));
    } catch (final TimeoutException e) {
      throw new IllegalArgumentException("Regex pattern caused catastrophic backtracking and was aborted: " + regex, e);
    } catch (final StackOverflowError e) {
      // TimeBoundRegex turns a StackOverflowError into a TimeoutException only when regexTimeout is active; with it disabled
      // (arcadedb.command.regexTimeout <= 0) the overflow arrives as itself
      throw new IllegalArgumentException("Regex pattern caused stack overflow (possible catastrophic backtracking): " + regex, e);
    }
  }

  private Pattern compile(final String regex) {
    final CompiledRegex last = lastRegex;
    if (last != null && last.regex.equals(regex))
      return last.pattern;

    final Pattern pattern;
    try {
      pattern = Pattern.compile(regex);
    } catch (final PatternSyntaxException e) {
      throw new IllegalArgumentException("Invalid regex pattern: " + e.getMessage(), e);
    }
    lastRegex = new CompiledRegex(regex, pattern);
    return pattern;
  }

  /**
   * Answers whether the UTF-16 offset {@code index} of {@code text} falls between the two halves of a surrogate pair, where a regex
   * match can start or end but a character boundary cannot (issue #10485).
   */
  protected static boolean insideSurrogatePair(final String text, final int index) {
    return index > 0 && index < text.length() && Character.isHighSurrogate(text.charAt(index - 1))
        && Character.isLowSurrogate(text.charAt(index));
  }

  /**
   * Answers whether a regex match from {@code start} to {@code end} (UTF-16 offsets of {@code text}) begins or ends between the two
   * halves of a surrogate pair, so that it is not a whole number of characters (issue #10485). The engine moves one UTF-16 unit past
   * a match of no characters, which lands inside a pair, and a pattern can match one half of an emoji.
   */
  protected static boolean splitsSurrogatePair(final String text, final int start, final int end) {
    return insideSurrogatePair(text, start) || end != start && insideSurrogatePair(text, end);
  }

  /**
   * The word with its first character in title case, as APOC's capitalize of a word ({@code StringUtils.capitalize}): {@code 'ǆ'} is
   * {@code 'ǅ'}, and a character outside the BMP is one character.
   */
  protected static String titleCaseFirst(final String word) {
    if (word.isEmpty())
      return word;
    final int first = word.codePointAt(0);
    final int title = Character.toTitleCase(first);
    if (title == first)
      return word;
    return new StringBuilder(word.length()).appendCodePoint(title).append(word, Character.charCount(first), word.length()).toString();
  }
}
