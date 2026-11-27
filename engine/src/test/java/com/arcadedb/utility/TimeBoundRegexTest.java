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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Regression tests for issue #5886: catastrophic regex backtracking is not bounded by any timeout. */
class TimeBoundRegexTest {

  @Test
  void matchingPatternReturnsTrue() {
    assertThat(TimeBoundRegex.matches(Pattern.compile("Aa.*"), "Aardvark", 1000)).isTrue();
  }

  @Test
  void nonMatchingPatternReturnsFalse() {
    assertThat(TimeBoundRegex.matches(Pattern.compile("Aa.*"), "BBking", 1000)).isFalse();
  }

  @Test
  void nonPositiveTimeoutDisablesTheBound() {
    // A timeout <= 0 must behave exactly like a plain, unbounded Matcher.matches() call.
    assertThat(TimeBoundRegex.matches(Pattern.compile("Aa.*"), "Aardvark", 0)).isTrue();
    assertThat(TimeBoundRegex.matches(Pattern.compile("Aa.*"), "Aardvark", -1)).isTrue();
  }

  @Test
  void catastrophicBacktrackingIsAbortedWithinTheDeadline() {
    // Issue #5886 reproducer: (.*a){20}$ against "a".repeat(40) + "!" triggers catastrophic
    // backtracking in java.util.regex and is still running after 30s with no bound in place.
    final Pattern pathological = Pattern.compile("(.*a){20}$");
    final String input = "a".repeat(40) + "!";

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> TimeBoundRegex.matches(pathological, input, 200))
        .isInstanceOf(TimeoutException.class);

    // Generous upper bound: proves the match was aborted near the requested deadline rather than
    // merely being slow (the unbounded match takes tens of seconds).
    stopwatch.assertGaveUpWithin(5000, "the requested 200ms deadline from an unbounded match");
  }

  @Test
  void findUntilSearchesAnywhereInTheInput() {
    assertThat(TimeBoundRegex.findUntil(Pattern.compile("b+"), "aabbcc", TimeBoundRegex.newDeadline(1000))).isTrue();
    assertThat(TimeBoundRegex.findUntil(Pattern.compile("^b+"), "aabbcc", TimeBoundRegex.newDeadline(1000))).isFalse();
  }

  @Test
  void findUntilAbortsCatastrophicBacktracking() {
    final Pattern pathological = Pattern.compile("(.*a){20}$");
    final String input = "a".repeat(40) + "!";

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> TimeBoundRegex.findUntil(pathological, input, TimeBoundRegex.newDeadline(200)))
        .isInstanceOf(TimeoutException.class);
    stopwatch.assertGaveUpWithin(5000, "the requested 200ms deadline from an unbounded search");
  }

  @Test
  void replaceAllReplacesEveryMatch() {
    assertThat(TimeBoundRegex.replaceAll(Pattern.compile("a"), "banana", "o", 1000)).isEqualTo("bonono");
  }

  @Test
  void replaceAllWithNonPositiveTimeoutDisablesTheBound() {
    assertThat(TimeBoundRegex.replaceAll(Pattern.compile("a"), "banana", "o", 0)).isEqualTo("bonono");
  }

  @Test
  void replaceAllAbortsCatastrophicBacktrackingWithinTheDeadline() {
    // text.regexReplace() (issue #5886 follow-up) runs Matcher.replaceAll(), which backtracks through the same
    // java.util.regex machinery as matches() and is exposed to the same catastrophic-backtracking gap.
    final Pattern pathological = Pattern.compile("(.*a){20}$");
    final String input = "a".repeat(40) + "!";

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> TimeBoundRegex.replaceAll(pathological, input, "x", 200))
        .isInstanceOf(TimeoutException.class);

    stopwatch.assertGaveUpWithin(5000, "the requested 200ms deadline from an unbounded match");
  }

  @Test
  void replaceAllUntilSharesOneDeadlineAcrossASeries() {
    // Issue #5886, 12th review pass: TextRegexReplace/.normalize() need to share one deadline across every row
    // of a query, the same "shared budget across a series" replaceAll() needed matches()/matchesUntil() to
    // already have.
    final Pattern pathological = Pattern.compile("(.*a){20}$");
    final String input = "a".repeat(40) + "!";
    final long deadline = TimeBoundRegex.newDeadline(200);

    // First call consumes the whole 200ms budget and trips the deadline.
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> TimeBoundRegex.replaceAllUntil(pathological, input, "x", deadline))
        .isInstanceOf(TimeoutException.class);
    // A second call against the same (now-expired) shared deadline must fail almost immediately, not run for
    // another full budget - proving the deadline is a shared, absolute point in time, not a per-call timeout.
    assertThatThrownBy(() -> TimeBoundRegex.replaceAllUntil(pathological, input, "x", deadline))
        .isInstanceOf(TimeoutException.class);

    // Two independent 200ms budgets would take >= 400ms; a shared deadline keeps both calls close to the
    // single 200ms bound instead.
    stopwatch.assertStayedUnder(1000, "one 200ms deadline shared by both calls, not one each");
  }

  /**
   * Issue #12595: a pattern that backtracks without reading a character never calls charAt(), so the deadline wrapper has
   * no chance to throw. Anchors and zero-width alternation are enough, and the pattern stays far under 500 characters.
   */
  @Nested
  @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)  // a hang detector: a pattern nobody bounds runs for hours
  class BacktrackingThatReadsNothing {
    // 11 groups are 2,048 ways, the smallest fixture past the limit: were the refusal to regress, the match would take
    // microseconds and the test would fail, instead of a pattern the size of the issue's (n = 40) hanging the whole JVM
    private final String zeroWidthAlternation = "\\A" + "(?:\\A|\\A)".repeat(11) + "\\z";
    private final String zeroWidthOptionals   = "(?:\\A?)".repeat(11) + "\\z";

    @Test
    void matchesIsAbortedWithinTheDeadline() {
      final Pattern pattern = Pattern.compile(zeroWidthAlternation);
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> TimeBoundRegex.matches(pattern, "ab", 200)).isInstanceOf(TimeoutException.class);
      stopwatch.assertGaveUpWithin(5000, "the requested 200ms deadline from an unbounded zero-width backtrack");
    }

    @Test
    void findIsAbortedWithinTheDeadline() {
      final Pattern pattern = Pattern.compile(zeroWidthAlternation);
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> TimeBoundRegex.findUntil(pattern, "ab", TimeBoundRegex.newDeadline(200))).isInstanceOf(TimeoutException.class);
      stopwatch.assertGaveUpWithin(5000, "the requested 200ms deadline from an unbounded zero-width backtrack");
    }

    @Test
    void replaceAllIsAbortedWithinTheDeadline() {
      final Pattern pattern = Pattern.compile(zeroWidthAlternation);
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> TimeBoundRegex.replaceAll(pattern, "ab", "x", 200)).isInstanceOf(TimeoutException.class);
      stopwatch.assertGaveUpWithin(5000, "the requested 200ms deadline from an unbounded zero-width backtrack");
    }

    @Test
    void fullMatchIsAbortedWithinTheDeadline() {
      final Pattern pattern = Pattern.compile(zeroWidthAlternation);
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> TimeBoundRegex.fullMatchUntil(pattern, "ab", TimeBoundRegex.newDeadline(200))).isInstanceOf(
          TimeoutException.class);
      stopwatch.assertGaveUpWithin(5000, "the requested 200ms deadline from an unbounded zero-width backtrack");
    }

    @Test
    void optionalAnchorsAreAbortedToo() {
      // no alternation at all: every '\A?' can be taken or skipped, and neither reads the input
      final Pattern pattern = Pattern.compile(zeroWidthOptionals);
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> TimeBoundRegex.findUntil(pattern, "ab", TimeBoundRegex.newDeadline(200))).isInstanceOf(TimeoutException.class);
      stopwatch.assertGaveUpWithin(5000, "the requested 200ms deadline from an unbounded zero-width backtrack");
    }

    @Test
    void aScanOfALongInputIsAbortedToo() {
      // twelve empty alternations are only 4096 paths, but a find() runs them again at EVERY start position, and not one of
      // them reads: 200,000 characters took 8 seconds on the JDK before the bound looked at the shape of the pattern
      final Pattern pattern = Pattern.compile("(?:|)".repeat(12) + "\\z");
      final String input = "x".repeat(200_000);
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> TimeBoundRegex.findUntil(pattern, input, TimeBoundRegex.newDeadline(200))).isInstanceOf(
          TimeoutException.class);
      stopwatch.assertGaveUpWithin(5000, "the requested 200ms deadline from a scan that reads nothing");
    }

    @Test
    void theRefusalSaysWhyAndNamesThePattern() {
      final Pattern pattern = Pattern.compile(zeroWidthAlternation);
      assertThatThrownBy(() -> TimeBoundRegex.matches(pattern, "ab", 200)).isInstanceOf(TimeoutException.class)
          .hasMessageContaining("arcadedb.command.regexTimeout").hasMessageContaining("without reading");
    }

    @Test
    void aDisabledBoundRunsThePatternAsPlainJavaWould() {
      // regexTimeout <= 0 is the admin's explicit opt-out: nothing is refused, whatever the pattern. Small enough to finish.
      final Pattern pattern = Pattern.compile("\\A" + "(?:\\A|\\A)".repeat(14) + "\\z");
      assertThat(TimeBoundRegex.matches(pattern, "ab", 0)).isFalse();
      assertThat(TimeBoundRegex.findUntil(pattern, "ab", TimeBoundRegex.newDeadline(0))).isFalse();
    }

    @Test
    void aSmallAmbiguityStillRuns() {
      // eight anchors alternated with themselves are 256 paths: cheap, and refusing them would refuse patterns people write
      final Pattern pattern = Pattern.compile("\\A" + "(?:\\A|\\A)".repeat(8) + "ab");
      assertThat(TimeBoundRegex.findUntil(pattern, "abc", TimeBoundRegex.newDeadline(1000))).isTrue();
      assertThat(TimeBoundRegex.matches(pattern, "ab", 1000)).isTrue();
      assertThat(TimeBoundRegex.replaceAll(pattern, "abc", "x", 1000)).isEqualTo("xc");
    }

    @Test
    void failingAlternativesOverALongInputAreRefusedToo() {
      // one way, so no run to multiply: but each of the 125 anchored alternatives fails, without a read, at every one of the
      // 5,000,000 start positions of a find() (half a second of the JDK before the bound counted visits and not only ways)
      final Pattern pattern = Pattern.compile("(?:" + "\\Ax|".repeat(124) + "\\Ax)");
      final String input = "y".repeat(5_000_000);

      assertThatThrownBy(() -> TimeBoundRegex.findUntil(pattern, input, TimeBoundRegex.newDeadline(200))).isInstanceOf(TimeoutException.class)
          .hasMessageContaining("start positions");
      assertThatThrownBy(() -> TimeBoundRegex.replaceAll(pattern, input, "x", 200)).isInstanceOf(TimeoutException.class);
      // the same pattern over a short input, and a full match (one attempt, at the start), are cheap and run
      assertThat(TimeBoundRegex.findUntil(pattern, "y".repeat(1000), TimeBoundRegex.newDeadline(10_000))).isFalse();
      assertThat(TimeBoundRegex.matches(pattern, input, 10_000)).isFalse();
    }

    @Test
    void nestedCountedLoopsOfAnEmptyBodyAreRefused() {
      // 10^9 iterations of an anchor, one way, and not one read: 1.5 s of the JDK that the deadline never heard of
      final Pattern pattern = Pattern.compile("(?:(?:(?:\\A){1000}){1000}){1000}x");
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> TimeBoundRegex.matches(pattern, "ab", 200)).isInstanceOf(TimeoutException.class)
          .hasMessageContaining("steps in a single attempt");
      stopwatch.assertGaveUpWithin(5000, "the refusal, which looks at the pattern and not at the input");
      // a loop of a few iterations is nothing
      assertThat(TimeBoundRegex.matches(Pattern.compile("(?:(?:\\A){10}){10}a"), "a", 10_000)).isTrue();
    }

    @Test
    void aCountedGroupOfACountedEmptyGroupIsRefusedWhateverTheInput() {
      // 4.3 billion empty matches over an input with no character to read (17 s of the JDK, with no deadline to hear of them)
      final Pattern pattern = Pattern.compile("((){21474836}){200}");
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> TimeBoundRegex.findUntil(pattern, "", TimeBoundRegex.newDeadline(200))).isInstanceOf(
          TimeoutException.class);
      assertThatThrownBy(() -> TimeBoundRegex.matches(pattern, "", 200)).isInstanceOf(TimeoutException.class);
      assertThatThrownBy(() -> TimeBoundRegex.replaceAll(pattern, "ab", "x", 200)).isInstanceOf(TimeoutException.class);
      stopwatch.assertGaveUpWithin(5000, "the refusal, which looks at the pattern and not at the input");
    }

    @Test
    void aBodyRepeatedNoTimesIsNotCounted() {
      // the body is never entered, so a match that does not go through it must not be refused for what it would cost
      final Pattern pattern = Pattern.compile("(?:\\A|\\A){0}".repeat(20) + "a");
      assertThat(TimeBoundRegex.matches(pattern, "a", 10_000)).isTrue();
    }

    @Test
    void aLongPatternIsRefusedOrAcceptedOnEveryCall() {
      // past 1,024 characters the measure is kept per Pattern, not by its text: the answer is the same on each call
      final Pattern refused = Pattern.compile("\\A" + "(?:\\A|\\A)".repeat(150) + "\\z");
      assertThat(refused.pattern().length()).isGreaterThan(1024);
      assertThatThrownBy(() -> TimeBoundRegex.matches(refused, "ab", 200)).isInstanceOf(TimeoutException.class);
      assertThatThrownBy(() -> TimeBoundRegex.matches(refused, "ab", 200)).isInstanceOf(TimeoutException.class);

      // the positive checks test acceptance, not latency: a deadline no pause of the JVM reaches
      final Pattern benign = Pattern.compile("a".repeat(2000));
      assertThat(TimeBoundRegex.matches(benign, "a".repeat(2000), 60_000)).isTrue();
      assertThat(TimeBoundRegex.matches(benign, "a".repeat(2000), 60_000)).isTrue();
    }

    @Test
    void workAfterACharacterIsNotHiddenBehindIt() {
      // the character in front is read, and the 10^9 empty iterations after it are not: they have no read to be polled from
      final Pattern pattern = Pattern.compile("a(?:(?:(?:){1000}){1000}){1000}");
      assertThatThrownBy(() -> TimeBoundRegex.matches(pattern, "a", 10_000)).isInstanceOf(TimeoutException.class)
          .hasMessageContaining("steps in a single attempt");
    }

    @Test
    void theWorkOfEachTurnThatReadsOnceIsCountedForTheTurnsBetweenTwoLooksAtTheClock() {
      // the clock is looked at once every 256 reads and each of the 255 turns reads once: 255 turns of 16,000,000 visits are
      // 4 billion between two looks, and the work of one turn alone (16,000,000) is under the 2^24 the limit used to be
      final Pattern pattern = Pattern.compile("(?:a(?:(?:){1000}){16000}){255}");
      assertThatThrownBy(() -> TimeBoundRegex.matches(pattern, "a".repeat(255), 10_000)).isInstanceOf(TimeoutException.class)
          .hasMessageContaining("steps in a single attempt");
    }

    @Test
    void aLookBehindIsChargedForEveryStartPositionItTries() {
      // a look-behind of a range tries its body at each position of it and one inside another multiplies them, none of which
      // reads when the body fails first: ~10^10 attempts from 40 characters of pattern
      final Pattern pattern = Pattern.compile("(?<=(?<=\\A\\z.{0,100000}).{0,100000})");
      assertThatThrownBy(() -> TimeBoundRegex.matches(pattern, "", 10_000)).isInstanceOf(TimeoutException.class)
          .hasMessageContaining("steps in a single attempt");
      // a look-behind of a few characters is nothing
      assertThat(TimeBoundRegex.matches(Pattern.compile("a(?<=\\d{0,3}a)b?"), "a", 10_000)).isTrue();
    }

    @Test
    void aTrailingInlineFlagDoesNotHideTheStateThePatternStartedIn() {
      // Pattern.flags() answers the state at the END of the pattern, so the trailing (?x) makes it COMMENTS and the '#' in front
      // of 2,048 ways is a character that has to be read, not the start of a comment that would hide them
      final Pattern hidden = Pattern.compile("#" + "(?:\\A|\\A)".repeat(11) + "(?x)");
      assertThat(hidden.flags() & Pattern.COMMENTS).isNotZero();
      assertThatThrownBy(() -> TimeBoundRegex.matches(hidden, "#", 10_000)).isInstanceOf(TimeoutException.class)
          .hasMessageContaining("different ways");

      // and the other way round: COMMENTS passed, switched off at the end, which flags() reports as no COMMENTS at all
      final Pattern shown = Pattern.compile("# note\n" + "(?:\\A|\\A)".repeat(11) + "(?-x)", Pattern.COMMENTS);
      assertThat(shown.flags() & Pattern.COMMENTS).isZero();
      assertThatThrownBy(() -> TimeBoundRegex.matches(shown, "", 10_000)).isInstanceOf(TimeoutException.class)
          .hasMessageContaining("different ways");
    }

    @Test
    void aPatternWithATrailingInlineFlagStillRunsWhenItIsHarmless() {
      // read as comments from the start the '#b)(?x)' would swallow the group's end: the state it really started in parses
      final Pattern pattern = Pattern.compile("(a#b)(?x)");
      assertThat(TimeBoundRegex.matches(pattern, "a#b", 10_000)).isTrue();
    }

    @Test
    void anOperationStartedAfterTheDeadlineHasGoneByIsNotGivenAMatch() {
      // a short input never reaches the clock check of charAt(), and one deadline is shared by every row of a scan
      final long gone = System.nanoTime() - 1_000_000L;
      assertThatThrownBy(() -> TimeBoundRegex.matchesUntil(Pattern.compile("\\A\\z"), "", gone)).isInstanceOf(TimeoutException.class);
      assertThatThrownBy(() -> TimeBoundRegex.findUntil(Pattern.compile("a"), "a", gone)).isInstanceOf(TimeoutException.class);
      assertThatThrownBy(() -> TimeBoundRegex.replaceAllUntil(Pattern.compile("a"), "a", "b", gone)).isInstanceOf(TimeoutException.class);
      assertThatThrownBy(() -> TimeBoundRegex.fullMatchUntil(Pattern.compile("a"), "a", gone)).isInstanceOf(TimeoutException.class);
      // a disabled bound has no deadline to have gone by
      assertThat(TimeBoundRegex.matchesUntil(Pattern.compile("a"), "a", Long.MAX_VALUE)).isTrue();
    }

    @Test
    void aLiteralPatternIsNotMeasuredAsTheSameTextCompiledWithoutTheFlag() {
      // the measure is cached by text: a LITERAL pattern (nothing to backtrack) must not leave 1 way behind for the same text
      // compiled as a pattern, or the refusal is bypassed by whoever runs the literal one first
      final Pattern literal = Pattern.compile(zeroWidthAlternation, Pattern.LITERAL);
      final Pattern pattern = Pattern.compile(zeroWidthAlternation);

      assertThat(TimeBoundRegex.matches(literal, "ab", 10_000)).isFalse();
      assertThatThrownBy(() -> TimeBoundRegex.matches(pattern, "ab", 200)).isInstanceOf(TimeoutException.class);
    }

    @Test
    void aPatternRefusedIsNotRefusedForTheSameTextCompiledAsALiteral() {
      // and the other way round: running the pattern first must not turn the literal one into a refusal
      final String text = "\\A" + "(?:\\A|\\A)".repeat(12) + "\\z";
      final Pattern pattern = Pattern.compile(text);
      final Pattern literal = Pattern.compile(text, Pattern.LITERAL);

      assertThatThrownBy(() -> TimeBoundRegex.matches(pattern, "ab", 200)).isInstanceOf(TimeoutException.class);
      assertThat(TimeBoundRegex.matches(literal, text, 10_000)).isTrue();
    }

    @Test
    void aMeasureTakenInCommentsModeDoesNotAnswerForThePlainText() {
      // the spaces are ignored in comments mode and literals (that must read) without it: the same text is two patterns
      final String text = "\\A" + "(?:\\A| \\A )".repeat(11) + "\\z";
      final Pattern plain = Pattern.compile(text);
      final Pattern comments = Pattern.compile(text, Pattern.COMMENTS);

      assertThat(TimeBoundRegex.matches(plain, "ab", 10_000)).isFalse();
      assertThatThrownBy(() -> TimeBoundRegex.matches(comments, "ab", 200)).isInstanceOf(TimeoutException.class);
      // in the other order too, and from the cache the second time
      assertThatThrownBy(() -> TimeBoundRegex.matches(comments, "ab", 200)).isInstanceOf(TimeoutException.class);
      assertThat(TimeBoundRegex.matches(plain, "ab", 10_000)).isFalse();
    }

    @Test
    void theJdkEndsALoopAfterATurnThatMatchedNothing() {
      // the model the analysis rests on: a COUNTED group that can match nothing is not 2^40 paths, because Pattern leaves the
      // loop after the first turn that consumed nothing. If a JDK ever stops doing that, the analysis has to count the turns
      final Pattern pattern = Pattern.compile("\\A(?:\\A|\\A){40}\\z");
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThat(pattern.matcher("ab").find()).isFalse();
      stopwatch.assertGaveUpWithin(5000, "a counted zero-width loop, which the JDK cuts after one turn");
    }
  }

  @Test
  void newDeadlineDoesNotOverflowOnAnOversizedTimeout() {
    // arcadedb.command.regexTimeout is admin-configurable; a value large enough that timeoutMillis * 1_000_000L
    // (or adding System.nanoTime() to it) overflows a long must fall back to "effectively unbounded" rather than
    // silently wrapping into a deadline that's already in the past - which would make every match abort
    // immediately instead of applying the (very long) requested timeout.
    assertThat(TimeBoundRegex.newDeadline(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
    assertThat(TimeBoundRegex.newDeadline(Long.MAX_VALUE / 1_000_000L + 1)).isEqualTo(Long.MAX_VALUE);

    // And an overflow-inducing timeout must still let a normal match through rather than abort it, proving the
    // fallback really disables the bound instead of just returning a large-but-still-wrong deadline.
    assertThat(TimeBoundRegex.matches(Pattern.compile("Aa.*"), "Aardvark", Long.MAX_VALUE)).isTrue();
  }

  /** The measure a caller that counts the reads of a match, and has no clock to look at, takes of a pattern. */
  @Nested
  class ReadFreeWorkOfAPattern {
    @Test
    void aPatternThatCanStepAtLengthWithoutReadingIsRefused() {
      assertThat(TimeBoundRegex.readFreeWork(Pattern.compile("((){2147483647}){2147483647}")).refused()).isTrue();
      assertThat(TimeBoundRegex.readFreeWork(Pattern.compile("((){1000}){1000}")).refused()).isTrue();
      assertThat(TimeBoundRegex.readFreeWork(Pattern.compile("(|)".repeat(12) + "(?!)")).refused()).isTrue();
      assertThat(TimeBoundRegex.readFreeWork(Pattern.compile("^?".repeat(12) + "(?!)")).refused()).isTrue();
      assertThat(TimeBoundRegex.readFreeWork(Pattern.compile("(a*)?".repeat(12) + "(?!)")).refused()).isTrue();
    }

    @Test
    void anOrdinaryPatternIsNotRefusedAndCostsAFewSteps() {
      for (final String regex : new String[] { "^(pg_catalog|public)$", "^pg_toast", "^(.*)(.*)(.*)Y$", "[a-z]+", "e?", "(a|b|c|d)*z",
          "" }) {
        final TimeBoundRegex.ReadFreeWork work = TimeBoundRegex.readFreeWork(Pattern.compile(regex));
        assertThat(work.refused()).as(regex).isFalse();
        assertThat(work.perAttempt()).as(regex).isLessThan(10);
        assertThat(work.perStart()).as(regex).isLessThan(10);
      }
    }

    @Test
    void theStepsOfAPatternThatCanTakeSomeAreCounted() {
      // 240 x 240 empty matches in one attempt, whether the attempt starts at the end of the input or in the middle of it
      final TimeBoundRegex.ReadFreeWork work = TimeBoundRegex.readFreeWork(Pattern.compile("((){240}){240}"));
      assertThat(work.refused()).isFalse();
      assertThat(work.perAttempt()).isEqualTo(57_600);
      assertThat(work.perStart()).isEqualTo(57_600);

      // eight anchors that fail at every position but the first are steps of each start position of a search, not a way to succeed
      final TimeBoundRegex.ReadFreeWork anchors = TimeBoundRegex.readFreeWork(Pattern.compile("(?:^|^|^|^|^|^|^|^)c"));
      assertThat(anchors.refused()).isFalse();
      assertThat(anchors.perStart()).isEqualTo(8);
    }

    @Test
    void aLiteralPatternHasNothingToStepThrough() {
      final TimeBoundRegex.ReadFreeWork work = TimeBoundRegex.readFreeWork(Pattern.compile("(|)".repeat(12) + "(?!)", Pattern.LITERAL));
      assertThat(work.refused()).isFalse();
      assertThat(work.perAttempt()).isZero();
      assertThat(work.perStart()).isZero();
    }

    @Test
    void thePatternsOfALikeAreMeasuredAsReadsOnly() {
      // what a LIKE turns into: a wildcard and quoted characters, which the match reads one by one
      final TimeBoundRegex.ReadFreeWork work = TimeBoundRegex.readFreeWork(Pattern.compile(".*\\Qa\\E.*\\Qa\\E.*\\Qb\\E"));
      assertThat(work.refused()).isFalse();
      assertThat(work.perAttempt()).isZero();
      assertThat(work.perStart()).isZero();
    }
  }
}
