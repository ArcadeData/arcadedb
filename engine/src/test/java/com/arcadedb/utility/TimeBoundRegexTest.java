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
}
