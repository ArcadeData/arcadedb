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

import com.arcadedb.utility.RegexOverAlphabet.Verdict;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** Issue #9853: the search behind refusing a REGEXP no value of a number or a temporal can match. */
class RegexOverAlphabetTest {
  private static final String NUMBER = "0123456789-";

  private static Verdict check(final String regexp, final String alphabet) {
    // no deadline: the verdicts below are decided by the probes alone, never by how fast the machine is
    return RegexOverAlphabet.check(Pattern.compile(regexp), null, alphabet, 64, 65_536, TimeBoundRegex.newDeadline(0));
  }

  private static Verdict check(final String regexp, final String shape, final String alphabet) {
    return RegexOverAlphabet.check(Pattern.compile(regexp), Pattern.compile(shape), alphabet, 64, 65_536,
        TimeBoundRegex.newDeadline(0));
  }

  @Test
  void theShapeNarrowsTheStringsTried() {
    // 'E' is a character of a FLOAT, but no FLOAT is written as letters alone
    final String floating = "-?([0-9]+\\.[0-9]+(E-?[0-9]+)?|Infinity)|NaN";
    assertThat(check("[A-Z]+", "0123456789-.EINafinty")).isEqualTo(Verdict.SATISFIABLE);
    assertThat(check("[A-Z]+", floating, "0123456789-.EINafinty")).isEqualTo(Verdict.UNSATISFIABLE);
    assertThat(check("[A-Za-z]+", floating, "0123456789-.EINafinty")).isEqualTo(Verdict.SATISFIABLE);
    // a bounded shape makes a search over an infinite alphabet language finite
    assertThat(check("[0-9]{5}", "-?[0-9]{1,3}", NUMBER)).isEqualTo(Verdict.UNSATISFIABLE);
    assertThat(check("[0-9]{3}", "-?[0-9]{1,3}", NUMBER)).isEqualTo(Verdict.SATISFIABLE);
    // a pattern that matches a prefix of the shape only: 'a|ab' style alternation is not pruned by its first match
    assertThat(check("1|12", "[0-9]{2}", NUMBER)).isEqualTo(Verdict.SATISFIABLE);
  }

  @Test
  void aPatternSomeStringOverTheAlphabetMatchesIsSatisfiable() {
    assertThat(check("[0-9]{5}", NUMBER)).isEqualTo(Verdict.SATISFIABLE);
    assertThat(check("-?[1-9][0-9]*", NUMBER)).isEqualTo(Verdict.SATISFIABLE);
    assertThat(check(".*", NUMBER)).isEqualTo(Verdict.SATISFIABLE);
    // the witness is deep and only one branch reaches it
    assertThat(check("1{30}2", NUMBER)).isEqualTo(Verdict.SATISFIABLE);
    assertThat(check("0*9", NUMBER)).isEqualTo(Verdict.SATISFIABLE);
    assertThat(check("(?i)e", "0123456789E")).isEqualTo(Verdict.SATISFIABLE);
  }

  @Test
  void aPatternNoStringOverTheAlphabetMatchesIsUnsatisfiable() {
    assertThat(check("[A-Z]+", NUMBER)).isEqualTo(Verdict.UNSATISFIABLE);
    assertThat(check("[0-9]{2},[0-9]", NUMBER)).isEqualTo(Verdict.UNSATISFIABLE);
    assertThat(check("1{3}x", NUMBER)).isEqualTo(Verdict.UNSATISFIABLE);
    // only the empty string matches, and no value is written as the empty string
    assertThat(check("", NUMBER)).isEqualTo(Verdict.UNSATISFIABLE);
    assertThat(check("x?", NUMBER)).isEqualTo(Verdict.UNSATISFIABLE);
    assertThat(check("(?!)", NUMBER)).isEqualTo(Verdict.UNSATISFIABLE);
  }

  @Test
  void lookaroundsBoundariesAndAnchorsAreNeverAFalseRefusal() {
    // the constructs Matcher.hitEnd() is subtle about: a refusal here would reject a pattern a LONG satisfies
    final String longShape = "-?[0-9]{1,19}";
    for (final String regexp : new String[] { "(?=.*5)[0-9]+", "[0-9]+(?<=5)", "\\b[0-9]+\\b", "[0-9]+$", "^-?[0-9]+\\z",
        "(?>[0-9]+)", "[0-9]++", "(?!0)[0-9]+", "[0-9]+(?<!0)", "([0-9])\\1", "-?([1-9])[0-9]*\\1", "(?<![0-9])[0-9]+",
        "[0-9]+(?=$)" }) {
      assertThat(check(regexp, longShape, NUMBER)).as(regexp).isEqualTo(Verdict.SATISFIABLE);
      assertThat(check(regexp, NUMBER)).as(regexp).isEqualTo(Verdict.SATISFIABLE);
    }
    // a lookbehind for a character before the start of the text: matches() never sees one, so no value can satisfy it
    assertThat(check("(?<=-)[0-9]+", longShape, NUMBER)).isEqualTo(Verdict.UNSATISFIABLE);
    // a lookahead for a character no number has is never satisfiable, whatever the search manages to show
    assertThat(check("(?=.*x)[0-9]+", longShape, NUMBER)).isNotEqualTo(Verdict.SATISFIABLE);
    assertThat(check("[0-9]+(?<=x)", longShape, NUMBER)).isNotEqualTo(Verdict.SATISFIABLE);
  }

  @Test
  void aSearchThatRunsOutOfRoomIsUndecidedNotAVerdict() {
    // every string of digits may still grow into a match
    assertThat(check("[0-9]*[A-Z]", NUMBER)).isEqualTo(Verdict.UNDECIDED);
    // a pattern that needs a character outside the alphabet only after an unbounded run of ones inside it: the search cannot
    // rule out every run, so it does not claim to
    assertThat(check("1+x", NUMBER)).isEqualTo(Verdict.UNDECIDED);
    // the probes run out before the shape does
    assertThat(RegexOverAlphabet.check(Pattern.compile("[0-9]{5}"), Pattern.compile("-?[0-9]{1,3}"), NUMBER, 64, 100,
        TimeBoundRegex.newDeadline(0))).isEqualTo(Verdict.UNDECIDED);
    // the only match is longer than the search goes
    assertThat(check("[0-9]{100}", NUMBER)).isEqualTo(Verdict.UNDECIDED);
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void aCatastrophicPatternIsUndecidedWithinTheDeadline() {
    // exponential backtracking on a long run of zeros: the in-match deadline aborts the probe
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThat(RegexOverAlphabet.check(Pattern.compile("(0|00)*1x"), null, "0", 64, 4_096, TimeBoundRegex.newDeadline(100)))
        .isEqualTo(Verdict.UNDECIDED);
    assertThat(RegexOverAlphabet.check(Pattern.compile("([0-9]+)+-"), Pattern.compile("-?[0-9]{1,19}"), "0123456789-", 64,
        65_536, TimeBoundRegex.newDeadline(100))).isNotEqualTo(Verdict.SATISFIABLE);
    stopwatch.assertGaveUpWithin(10_000, "a 100 ms search budget against a backtracking that would otherwise run for ages");
  }
}
