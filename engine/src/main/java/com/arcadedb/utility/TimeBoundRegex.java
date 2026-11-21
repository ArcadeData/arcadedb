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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.regex.Matcher;
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
 * <p>
 * The same property is the limit of the bound: a pattern that backtracks WITHOUT reading a character (anchors, empty
 * alternatives and optional anchors in sequence: {@code \A(?:\A|\A)(?:\A|\A)...\z} takes 2^n steps and never calls
 * {@code charAt()}) gives the deadline nothing to look from, and nothing in {@code java.util.regex} can be told to stop. Those
 * are refused before they run, by the shape of the pattern ({@link RegexReadFreeWork}, issue #12595), with the same
 * {@link TimeoutException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TimeBoundRegex {
  // Deadline is checked every CHECK_INTERVAL charAt() calls (a power of two, checked via bitmask) rather than on
  // every call, since charAt() is on the hottest possible path here (called on every backtracking step) and
  // System.nanoTime() is not free. An operation that finishes in fewer than CHECK_INTERVAL total charAt() calls
  // finishes essentially instantly anyway, so the coarser granularity only matters for the runaway case this
  // class exists to bound.
  static final int CHECK_INTERVAL = 256;

  // The most different ways a run of elements that read nothing may succeed (see RegexReadFreeWork). 1,024 paths cost a few
  // microseconds, far below anything the deadline is for; the pattern of the issue that found this needs 2^11 of them to
  // pass it, and no pattern written by hand has more than a handful.
  private static final long   MAX_READ_FREE_WAYS       = 1L << 10;
  // The most read-free node visits ONE attempt may make, those no character read accompanies. It bounds counted loops of bodies
  // that match nothing (a nested (?:(?:(?:\A){1000}){1000}){1000} is 10^9 visits, one way, no read, 1.5 s), and the work of each
  // attempt that reads once, since the clock is looked at once every CHECK_INTERVAL reads: 2^16 visits, times the 256 attempts
  // that can go by between two looks, is 2^24 visits, about 25 ms at the JDK's 1.5 ns a visit.
  private static final long   MAX_READ_FREE_WORK       = 1L << 16;
  // What a scan may spend on read-free visits: the visits one start position makes in the middle of the input times the length of
  // the input, 0.13 to 0.45 s on the JDK at 1 to 3 ns a visit (4,096 ways of an empty option over 200,000 characters took 8 s,
  // and a hundred anchored alternatives over 5,000,000 half a second, with no read for the deadline to hear)
  private static final double MAX_READ_FREE_SCAN_STEPS = 1L << 27;
  private static final int    MAX_CACHED_PATTERNS      = 4096;
  // A pattern longer than this is not cached by its text, which the cache would keep alive: it is cached by the Pattern itself
  private static final int    MAX_CACHED_TEXT_LENGTH   = 1024;

  // The measure of each pattern text, since the same pattern is run against every row of a scan. One map per way the text is read
  // (plain, comments mode, UNIX_LINES, both): the same text can mean something else in each, and a measure taken for one must
  // never answer for another.
  private static final List<ConcurrentHashMap<String, RegexReadFreeWork.Measure>> MEASURES = List.of(new ConcurrentHashMap<>(),
      new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
  // The measure of the long ones, which live as long as their Pattern does and not a moment longer
  private static final Map<Pattern, RegexReadFreeWork.Measure>                    LONG_MEASURES  = Collections.synchronizedMap(
      new WeakHashMap<>());
  // A literal pattern has nothing to backtrack
  private static final RegexReadFreeWork.Measure                                  LITERAL_MEASURE = new RegexReadFreeWork.Measure(1, 0, 0, 0);

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
    return run(pattern, input, deadlineNanos, false, bounded -> pattern.matcher(bounded).matches());
  }

  /** The outcome of {@link #fullMatchUntil}. */
  public enum FullMatch {
    /** the input matches the pattern in its entirety; a longer input starting with it may or may not */
    MATCH,
    /** the input does not match, but the engine ran out of input while trying: some longer input starting with it might */
    MAY_MATCH_LONGER,
    /** the input does not match, and no longer input starting with it can (the engine never read past its end) */
    NEVER_WITH_THIS_PREFIX
  }

  /**
   * Matches {@code input} fully against {@code pattern} like {@link #matchesUntil}, and on a failure also tells whether a
   * longer input starting with {@code input} could still match ({@link java.util.regex.Matcher#hitEnd()}): the primitive a
   * search over the inputs a pattern could accept prunes on.
   *
   * @throws TimeoutException if the match does not complete before {@code deadlineNanos}
   */
  public static FullMatch fullMatchUntil(final Pattern pattern, final CharSequence input, final long deadlineNanos) {
    return run(pattern, input, deadlineNanos, false, bounded -> {
      final Matcher matcher = pattern.matcher(bounded);
      if (matcher.matches())
        return FullMatch.MATCH;
      return matcher.hitEnd() ? FullMatch.MAY_MATCH_LONGER : FullMatch.NEVER_WITH_THIS_PREFIX;
    });
  }

  /**
   * Looks for {@code pattern} anywhere in {@code input} ({@link java.util.regex.Matcher#find()}, as opposed to the full
   * match of {@link #matchesUntil(Pattern, CharSequence, long)}), aborting if {@code System.nanoTime()} passes
   * {@code deadlineNanos}.
   *
   * @return {@code true} if {@code pattern} is found in {@code input}
   *
   * @throws TimeoutException if the search does not complete before {@code deadlineNanos}
   */
  public static boolean findUntil(final Pattern pattern, final CharSequence input, final long deadlineNanos) {
    return run(pattern, input, deadlineNanos, true, bounded -> pattern.matcher(bounded).find());
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
    return run(pattern, input, deadlineNanos, true, bounded -> pattern.matcher(bounded).replaceAll(replacement));
  }

  /**
   * Runs any {@code operation} that drives a {@link Matcher} of {@code pattern} over the {@link CharSequence} it receives (a
   * {@code find()} loop that splits, or a {@code replaceAll} with a replacer function), aborting if it runs past {@code deadlineNanos}.
   * The operation has to read the input only through the sequence it is given, or the deadline does not apply. Such an operation
   * searches from every start position, so the pattern is measured as a scan, as {@link #findUntil} and {@link #replaceAllUntil} are.
   *
   * @throws TimeoutException if the operation does not complete before {@code deadlineNanos}
   */
  public static <T> T applyUntil(final Pattern pattern, final CharSequence input, final long deadlineNanos,
      final Function<CharSequence, T> operation) {
    return run(pattern, input, deadlineNanos, true, operation);
  }

  private static <T> T run(final Pattern pattern, final CharSequence input, final long deadlineNanos, final boolean scans,
      final Function<CharSequence, T> operation) {
    if (deadlineNanos != Long.MAX_VALUE) {
      // the bound is only as good as the reads it hears about: refuse what could backtrack without making any (issue #12595)
      refuseIfUnbounded(pattern, input, scans);
      // The deadline is looked at from charAt() every CHECK_INTERVAL calls of a new wrapper, so an operation on a short input never
      // looks at all: one that starts after the shared deadline has gone by, the measure of a pattern met for the first time
      // included, must not be given a match
      if (System.nanoTime() - deadlineNanos >= 0)
        throw timeoutException(pattern, input, RegexDeadlineExceeded.INSTANCE);
    }
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

  /**
   * Refuses a pattern whose backtracking the deadline could not interrupt. The deadline is looked at from
   * {@link CharSequence#charAt(int)} only, and a pattern can backtrack for as long as it likes without calling it: anchors,
   * empty alternatives, optional anchors and negative look-aheads at the end of the input read nothing, so
   * {@code \A(?:\A|\A)...\z} costs 2^n steps and no deadline ever fires. {@link RegexReadFreeWork} counts the ways a run of such
   * elements can succeed and the visits one attempt makes through them; the pattern is refused beyond
   * {@link #MAX_READ_FREE_WAYS} ways or {@link #MAX_READ_FREE_WORK} visits, and for an operation that tries the pattern at every
   * start position ({@code scans}) the visits of one position times the length of the input are held to
   * {@link #MAX_READ_FREE_SCAN_STEPS}, because none of those attempts reads either.
   */
  private static void refuseIfUnbounded(final Pattern pattern, final CharSequence input, final boolean scans) {
    final RegexReadFreeWork.Measure measure = readFreeMeasure(pattern);
    final String reason;
    if (measure.maxWays() > MAX_READ_FREE_WAYS)
      reason = "can succeed in " + count(measure.maxWays()) + " different ways";
    else if (measure.work() > MAX_READ_FREE_WORK)
      reason = "makes about " + count(measure.work()) + " steps in a single attempt";
    else if (scans && measure.scanWork() > 0 && (double) measure.scanWork() * ((double) input.length() + 1) > MAX_READ_FREE_SCAN_STEPS)
      reason = "makes about " + count(measure.scanWork()) + " steps at each of the " + input.length() + " start positions of this input";
    else
      return;
    throw new TimeoutException(
        "Regular expression operation refused (arcadedb.command.regexTimeout): pattern '" + pattern.pattern() + "' " + reason
            + " without reading its input, so its backtracking could not be aborted by a deadline; rewrite it without the empty"
            + " alternatives, optional anchors or repeated empty groups");
  }

  private static String count(final long value) {
    return value >= RegexReadFreeWork.SATURATED ? "too many" : Long.toString(value);
  }

  private static RegexReadFreeWork.Measure readFreeMeasure(final Pattern pattern) {
    final int flags = pattern.flags();
    // A literal pattern is only characters, so there is nothing to backtrack; and the same text compiled without the flag is a
    // different pattern, which no measure taken here may answer for (nor the other way round)
    if ((flags & Pattern.LITERAL) != 0)
      return LITERAL_MEASURE;
    final String regex = pattern.pattern();
    if (regex.length() > MAX_CACHED_TEXT_LENGTH) {
      RegexReadFreeWork.Measure measure = LONG_MEASURES.get(pattern);
      if (measure == null) {
        measure = RegexReadFreeWork.measureOrUnbounded(regex, flags);
        LONG_MEASURES.put(pattern, measure);
      }
      return measure;
    }
    final ConcurrentHashMap<String, RegexReadFreeWork.Measure> cache = MEASURES.get(
        ((flags & Pattern.COMMENTS) != 0 ? 1 : 0) | ((flags & Pattern.UNIX_LINES) != 0 ? 2 : 0));
    RegexReadFreeWork.Measure measure = cache.get(regex);
    if (measure == null) {
      measure = RegexReadFreeWork.measureOrUnbounded(regex, flags);
      // Distinct patterns can come from the data being queried, so the cache must not grow with it. Emptying it costs one more
      // parse per pattern, which is less than the Pattern.compile each of them has already paid
      if (cache.size() >= MAX_CACHED_PATTERNS)
        cache.clear();
      cache.put(regex, measure);
    }
    return measure;
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
