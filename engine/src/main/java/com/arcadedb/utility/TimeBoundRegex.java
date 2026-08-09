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
 * Bounds a {@link Pattern} match against catastrophic backtracking. {@code java.util.regex} never polls
 * {@code Thread.interrupted()} or checks a deadline while backtracking, so a pathological pattern (e.g. nested
 * quantifiers like {@code (.*a){20}$}) keeps its calling thread busy for as long as the backtracking takes, no
 * matter what timeout the surrounding query execution enforces. What it does call, on every single backtracking
 * step, is {@link CharSequence#charAt(int)} on the input it is matching. Wrapping that input in a sequence that
 * throws once a deadline elapses turns that call site into the interruption point {@code Matcher.matches()} does
 * not otherwise offer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TimeBoundRegex {
  // Deadline is checked every CHECK_INTERVAL charAt() calls (a power of two, checked via bitmask) rather than on
  // every call, since charAt() is on the hottest possible path here (called on every backtracking step) and
  // System.nanoTime() is not free. A pattern that finishes in fewer than CHECK_INTERVAL total charAt() calls
  // finishes essentially instantly anyway, so the coarser granularity only matters for the runaway case this
  // class exists to bound.
  private static final int CHECK_INTERVAL = 256;

  private TimeBoundRegex() {
  }

  /**
   * Matches {@code input} fully against {@code pattern}, aborting if it runs past {@code timeoutMillis}.
   *
   * @param pattern       the compiled pattern to match
   * @param input         the input to match against
   * @param timeoutMillis maximum time allowed for the match, in milliseconds; a value {@code <= 0} disables the
   *                      bound and matches as plain {@code pattern.matcher(input).matches()} would
   *
   * @return {@code true} if {@code input} matches {@code pattern} in its entirety
   *
   * @throws TimeoutException if the match does not complete within {@code timeoutMillis}
   */
  public static boolean matches(final Pattern pattern, final CharSequence input, final long timeoutMillis) {
    if (timeoutMillis <= 0)
      return pattern.matcher(input).matches();

    final long deadline = System.nanoTime() + (timeoutMillis * 1_000_000L);
    try {
      return pattern.matcher(new DeadlineBoundCharSequence(input, deadline)).matches();
    } catch (final RegexDeadlineExceeded e) {
      throw new TimeoutException(
          "Regular expression evaluation aborted after exceeding the " + timeoutMillis + "ms limit (arcadedb.command.regexTimeout): pattern '"
              + pattern.pattern() + "' against input of length " + input.length());
    }
  }

  private static final class DeadlineBoundCharSequence implements CharSequence {
    private final CharSequence wrapped;
    private final long         deadlineNanos;
    private       int          calls;

    private DeadlineBoundCharSequence(final CharSequence wrapped, final long deadlineNanos) {
      this.wrapped = wrapped;
      this.deadlineNanos = deadlineNanos;
    }

    @Override
    public int length() {
      return wrapped.length();
    }

    @Override
    public char charAt(final int index) {
      if ((++calls & (CHECK_INTERVAL - 1)) == 0 && System.nanoTime() > deadlineNanos)
        throw RegexDeadlineExceeded.INSTANCE;
      return wrapped.charAt(index);
    }

    @Override
    public CharSequence subSequence(final int start, final int end) {
      return new DeadlineBoundCharSequence(wrapped.subSequence(start, end), deadlineNanos);
    }

    @Override
    public String toString() {
      return wrapped.toString();
    }
  }

  /**
   * Unchecked signal used only to unwind out of {@code Matcher.matches()} once the deadline elapses; caught and
   * converted to a {@link TimeoutException} inside {@link #matches} and never seen outside this class. A single
   * shared, stack-trace-less instance is used since the failure carries no per-occurrence information and this
   * can be thrown a very large number of times on a single runaway match.
   */
  private static final class RegexDeadlineExceeded extends RuntimeException {
    private static final RegexDeadlineExceeded INSTANCE = new RegexDeadlineExceeded();

    private RegexDeadlineExceeded() {
      super(null, null, false, false);
    }
  }
}
