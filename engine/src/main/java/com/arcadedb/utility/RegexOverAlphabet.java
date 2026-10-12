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

import com.arcadedb.exception.TimeoutException;

import java.util.regex.Pattern;

/**
 * Decides whether a pattern can match some non-empty string written only with the characters of an alphabet and, when a
 * shape is given, also matching that shape (issue #9853): the shape describes every text a value of some type can be written
 * as, the alphabet the characters those texts use.
 * <p>
 * The search walks the strings over the alphabet depth first and prunes a prefix as soon as the regex engine reports that
 * no longer input starting with it can match the pattern, or the shape ({@link TimeBoundRegex#fullMatchUntil}, built on
 * {@link java.util.regex.Matcher#hitEnd()}). {@link Verdict#UNSATISFIABLE} is therefore a proof: every branch died. When
 * the search runs out of length, probes or time before that, the answer is {@link Verdict#UNDECIDED}, never a guess.
 */
public final class RegexOverAlphabet {
  public enum Verdict {
    /** some non-empty string over the alphabet, of the shape, matches */
    SATISFIABLE,
    /** no non-empty string over the alphabet, of the shape, matches */
    UNSATISFIABLE,
    /** the budget ran out before either could be shown */
    UNDECIDED
  }

  private static final int DEADLINE_CHECK_INTERVAL = 256;

  private RegexOverAlphabet() {
  }

  /**
   * @param pattern       the pattern, matched in full ({@link java.util.regex.Matcher#matches()})
   * @param shape         a pattern every candidate string must match as well, or null for any string over the alphabet
   * @param alphabet      the characters a candidate string is made of, in the order they are tried
   * @param maxLength     the longest candidate string tried
   * @param maxProbes     the most candidate strings tried
   * @param deadlineNanos an absolute {@link System#nanoTime()} deadline, see {@link TimeBoundRegex#newDeadline}
   */
  public static Verdict check(final Pattern pattern, final Pattern shape, final String alphabet, final int maxLength,
      final int maxProbes, final long deadlineNanos) {
    if (alphabet.isEmpty() || maxLength < 1)
      return Verdict.UNDECIDED;

    final StringBuilder prefix = new StringBuilder(maxLength);
    // next[d] = the index in the alphabet of the next character to try after a prefix of length d
    final int[] next = new int[maxLength];
    boolean cutOff = false;
    int probes = 0;
    int depth = 0;
    try {
      while (true) {
        if (next[depth] == alphabet.length()) {
          // every extension of this prefix is explored
          if (depth == 0)
            return cutOff ? Verdict.UNDECIDED : Verdict.UNSATISFIABLE;
          prefix.setLength(--depth);
          continue;
        }

        prefix.append(alphabet.charAt(next[depth]++));
        if (++probes > maxProbes)
          return Verdict.UNDECIDED;
        // a short probe never reaches the in-match deadline check, which fires every few hundred characters read
        if ((probes & (DEADLINE_CHECK_INTERVAL - 1)) == 0 && System.nanoTime() - deadlineNanos >= 0)
          return Verdict.UNDECIDED;

        final TimeBoundRegex.FullMatch inShape =
            shape == null ? TimeBoundRegex.FullMatch.MATCH : TimeBoundRegex.fullMatchUntil(shape, prefix, deadlineNanos);
        if (inShape != TimeBoundRegex.FullMatch.NEVER_WITH_THIS_PREFIX) {
          final TimeBoundRegex.FullMatch inPattern = TimeBoundRegex.fullMatchUntil(pattern, prefix, deadlineNanos);
          if (inPattern == TimeBoundRegex.FullMatch.MATCH && inShape == TimeBoundRegex.FullMatch.MATCH)
            return Verdict.SATISFIABLE;

          // a full match says nothing about a longer string ('a|ab' matches 'a' and 'ab'), so only a failed one prunes
          if (inPattern != TimeBoundRegex.FullMatch.NEVER_WITH_THIS_PREFIX) {
            if (depth + 1 < maxLength) {
              next[++depth] = 0;
              continue;
            }
            // alive, but too long to extend: whatever lies below it is unknown
            cutOff = true;
          }
        }
        prefix.setLength(depth);
      }
    } catch (final TimeoutException e) {
      return Verdict.UNDECIDED;
    } catch (final StackOverflowError e) {
      // java.util.regex recurses per repetition and per nesting level: a deep enough pattern overflows the stack on a probe
      // before any deadline is reached, and that says nothing about the pattern either way
      return Verdict.UNDECIDED;
    }
  }
}
