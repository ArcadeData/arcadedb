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
import org.junit.jupiter.api.Test;

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

    final long begin = System.nanoTime();
    assertThatThrownBy(() -> TimeBoundRegex.matches(pathological, input, 200))
        .isInstanceOf(TimeoutException.class);
    final long elapsedMillis = (System.nanoTime() - begin) / 1_000_000L;

    // Generous upper bound: proves the match was aborted near the requested deadline rather than
    // merely being slow (the unbounded match takes tens of seconds).
    assertThat(elapsedMillis).isLessThan(5000);
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

    final long begin = System.nanoTime();
    assertThatThrownBy(() -> TimeBoundRegex.replaceAll(pathological, input, "x", 200))
        .isInstanceOf(TimeoutException.class);
    final long elapsedMillis = (System.nanoTime() - begin) / 1_000_000L;

    assertThat(elapsedMillis).isLessThan(5000);
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
    final long begin = System.nanoTime();
    assertThatThrownBy(() -> TimeBoundRegex.replaceAllUntil(pathological, input, "x", deadline))
        .isInstanceOf(TimeoutException.class);
    // A second call against the same (now-expired) shared deadline must fail almost immediately, not run for
    // another full budget - proving the deadline is a shared, absolute point in time, not a per-call timeout.
    assertThatThrownBy(() -> TimeBoundRegex.replaceAllUntil(pathological, input, "x", deadline))
        .isInstanceOf(TimeoutException.class);
    final long elapsedMillis = (System.nanoTime() - begin) / 1_000_000L;

    // Two independent 200ms budgets would take >= 400ms; a shared deadline keeps both calls close to the
    // single 200ms bound instead.
    assertThat(elapsedMillis).isLessThan(1000);
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
}
