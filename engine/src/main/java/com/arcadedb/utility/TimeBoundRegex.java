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

import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Bounds a {@link Pattern} operation against catastrophic backtracking. {@code java.util.regex} never polls
 * {@code Thread.interrupted()} or checks a deadline while backtracking, so a pathological pattern (e.g. nested
 * quantifiers like {@code (.*a){20}$}, or several sequential {@code .*} segments like {@code (.*a){20}c} - no
 * nesting needed for that shape) keeps its calling thread busy for as long as the backtracking takes, no matter
 * what timeout the surrounding query execution enforces. What it does call, on every single backtracking step,
 * is {@link CharSequence#charAt(int)} on the input it is operating on. Wrapping that input in a sequence that
 * throws once a deadline elapses turns that call site into the interruption point {@code Matcher} does not
 * otherwise offer. Verified empirically (not just by reading the JDK source) that both {@code matches()} and
 * {@code replaceAll()} go through this {@code charAt()} path even for the literal-heavy patterns callers here
 * use ({@code MATCHES}/{@code =~}'s nested-quantifier shape and {@code LIKE}/wildcard's sequential-{@code .*}
 * shape both abort correctly) - some JDK regex fast paths read a pattern's literal prefix through a different
 * mechanism, and this class would silently stop bounding anything that took one of those paths instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TimeBoundRegex {
  // Deadline is checked every CHECK_INTERVAL charAt() calls (a power of two, checked via bitmask) rather than on
  // every call, since charAt() is on the hottest possible path here (called on every backtracking step) and
  // System.nanoTime() is not free. An operation that finishes in fewer than CHECK_INTERVAL total charAt() calls
  // finishes essentially instantly anyway, so the coarser granularity only matters for the runaway case this
  // class exists to bound.
  private static final int CHECK_INTERVAL = 256;

  private TimeBoundRegex() {
  }

  /**
   * Matches {@code input} fully against {@code pattern}, aborting if it runs past {@code timeoutMillis}.
   * <p>
   * Evaluating several inputs against the same logical bound (e.g. one property holding a list of strings, each
   * matched in turn against the same pattern) should not call this method once per item: each call starts a fresh
   * {@code timeoutMillis} budget, so N items would be bounded by {@code N * timeoutMillis} instead of by
   * {@code timeoutMillis} overall. Use {@link #newDeadline(long)} once up front and {@link #matchesUntil(Pattern,
   * CharSequence, long)} for each item instead.
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
    return matchesUntil(pattern, input, newDeadline(timeoutMillis));
  }

  /**
   * Computes an absolute deadline, in {@link System#nanoTime()} terms, {@code timeoutMillis} from now, for use
   * with {@link #matchesUntil(Pattern, CharSequence, long)} across a series of related operations that must share
   * one overall time budget rather than each getting their own.
   *
   * @param timeoutMillis maximum time allowed from now, in milliseconds; a value {@code <= 0} disables the bound
   *
   * @return an absolute deadline for {@link #matchesUntil(Pattern, CharSequence, long)}, or a sentinel that never
   * triggers if {@code timeoutMillis <= 0} or if {@code timeoutMillis} is large enough that computing the
   * deadline would overflow - {@code arcadedb.command.regexTimeout} is admin-configurable, and an overflowed
   * deadline landing in the past would make every match abort immediately instead of applying the (very long)
   * requested timeout, the opposite of what an oversized value should do
   */
  public static long newDeadline(final long timeoutMillis) {
    if (timeoutMillis <= 0)
      return Long.MAX_VALUE;
    try {
      return Math.addExact(System.nanoTime(), Math.multiplyExact(timeoutMillis, 1_000_000L));
    } catch (final ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }

  /**
   * Matches {@code input} fully against {@code pattern}, aborting if {@code System.nanoTime()} passes
   * {@code deadlineNanos} - an absolute deadline from {@link #newDeadline(long)}, shared across every call in a
   * series so the series as a whole is bounded rather than each call individually.
   *
   * @param pattern       the compiled pattern to match
   * @param input         the input to match against
   * @param deadlineNanos an absolute {@link System#nanoTime()} deadline, as returned by {@link #newDeadline(long)}
   *
   * @return {@code true} if {@code input} matches {@code pattern} in its entirety
   *
   * @throws TimeoutException if the match does not complete before {@code deadlineNanos}
   */
  public static boolean matchesUntil(final Pattern pattern, final CharSequence input, final long deadlineNanos) {
    return run(pattern, input, deadlineNanos, bounded -> pattern.matcher(bounded).matches());
  }

  /**
   * Replaces every match of {@code pattern} in {@code input} with {@code replacement}, aborting if it runs past
   * {@code timeoutMillis}. Same rationale as {@link #matches(Pattern, CharSequence, long)}: {@code replaceAll()}
   * backtracks through the same {@code java.util.regex} machinery and is just as exposed to a pathological
   * pattern.
   *
   * @param pattern       the compiled pattern to replace matches of
   * @param input         the input to search
   * @param replacement   the replacement string, per {@link java.util.regex.Matcher#replaceAll(String)}
   * @param timeoutMillis maximum time allowed for the operation, in milliseconds; a value {@code <= 0} disables
   *                      the bound
   *
   * @return {@code input} with every match of {@code pattern} replaced by {@code replacement}
   *
   * @throws TimeoutException if the operation does not complete within {@code timeoutMillis}
   */
  public static String replaceAll(final Pattern pattern, final CharSequence input, final String replacement, final long timeoutMillis) {
    return replaceAllUntil(pattern, input, replacement, newDeadline(timeoutMillis));
  }

  /**
   * Same as {@link #replaceAll(Pattern, CharSequence, String, long)}, but against an absolute deadline shared
   * across a series of related operations (e.g. the same regex applied to every row of a query result) rather
   * than each getting its own full timeout budget - see {@link #matchesUntil(Pattern, CharSequence, long)} for
   * the equivalent on the matching side.
   *
   * @param pattern       the compiled pattern to replace matches of
   * @param input         the input to search
   * @param replacement   the replacement string, per {@link java.util.regex.Matcher#replaceAll(String)}
   * @param deadlineNanos an absolute {@link System#nanoTime()} deadline, as returned by {@link #newDeadline(long)}
   *
   * @return {@code input} with every match of {@code pattern} replaced by {@code replacement}
   *
   * @throws TimeoutException if the operation does not complete before {@code deadlineNanos}
   */
  public static String replaceAllUntil(final Pattern pattern, final CharSequence input, final String replacement, final long deadlineNanos) {
    return run(pattern, input, deadlineNanos, bounded -> pattern.matcher(bounded).replaceAll(replacement));
  }

  private static <T> T run(final Pattern pattern, final CharSequence input, final long deadlineNanos, final Function<CharSequence, T> operation) {
    try {
      return operation.apply(deadlineNanos == Long.MAX_VALUE ? input : new DeadlineBoundCharSequence(input, deadlineNanos));
    } catch (final RegexDeadlineExceeded e) {
      // Only reachable when the bound is active (deadlineNanos != Long.MAX_VALUE skips the wrapper entirely, so
      // this can't be thrown when disabled).
      throw timeoutException(pattern, input, e);
    } catch (final StackOverflowError e) {
      if (deadlineNanos == Long.MAX_VALUE)
        // The bound is explicitly disabled (arcadedb.command.regexTimeout <= 0): this stack overflow is an
        // unrelated JDK regex recursion-depth issue, not a timeout, so let it propagate as itself instead of
        // mislabeling it via a setting the caller deliberately turned off.
        throw e;
      // A sufficiently pathological pattern/input combination can blow the (recursive) backtracking stack before
      // the next CHECK_INTERVAL checkpoint is reached; treated the same as an explicit deadline trip, since both
      // are catastrophic backtracking manifesting through a different symptom.
      throw timeoutException(pattern, input, e);
    }
  }

  private static TimeoutException timeoutException(final Pattern pattern, final CharSequence input, final Throwable cause) {
    return new TimeoutException(
        "Regular expression operation aborted (arcadedb.command.regexTimeout): pattern '" + pattern.pattern() + "' against input of length "
            + input.length(), cause);
  }

  private static final class DeadlineBoundCharSequence implements CharSequence {
    private final CharSequence wrapped;
    private final long         deadlineNanos;
    // Shared (not copied) with every CharSequence subSequence() derives from this one, so the check cadence
    // keeps counting from where the parent left off instead of restarting at each subSequence() call.
    private final int[]        calls;

    private DeadlineBoundCharSequence(final CharSequence wrapped, final long deadlineNanos) {
      this(wrapped, deadlineNanos, new int[1]);
    }

    private DeadlineBoundCharSequence(final CharSequence wrapped, final long deadlineNanos, final int[] calls) {
      this.wrapped = wrapped;
      this.deadlineNanos = deadlineNanos;
      this.calls = calls;
    }

    @Override
    public int length() {
      return wrapped.length();
    }

    @Override
    public char charAt(final int index) {
      // Overflow-safe form recommended by System.nanoTime()'s own Javadoc, in place of a plain ">" comparison.
      if ((++calls[0] & (CHECK_INTERVAL - 1)) == 0 && System.nanoTime() - deadlineNanos >= 0)
        throw RegexDeadlineExceeded.INSTANCE;
      return wrapped.charAt(index);
    }

    @Override
    public CharSequence subSequence(final int start, final int end) {
      return new DeadlineBoundCharSequence(wrapped.subSequence(start, end), deadlineNanos, calls);
    }

    @Override
    public String toString() {
      return wrapped.toString();
    }
  }

  /**
   * Unchecked signal used only to unwind out of a {@code Matcher} operation once the deadline elapses; caught and
   * converted to a {@link TimeoutException} inside {@link #run} and never seen outside this class. A single
   * shared, stack-trace-less instance is used since the failure carries no per-occurrence information and this
   * can be thrown a very large number of times on a single runaway operation.
   */
  private static final class RegexDeadlineExceeded extends RuntimeException {
    private static final RegexDeadlineExceeded INSTANCE = new RegexDeadlineExceeded();

    private RegexDeadlineExceeded() {
      super(null, null, false, false);
    }
  }
}
