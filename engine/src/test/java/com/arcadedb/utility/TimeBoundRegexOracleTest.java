/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link TimeBoundRegex} must answer what the plain {@link Matcher} answers, whether the bound is on or off, and its
 * {@link TimeBoundRegex.FullMatch} classification is what {@link RegexOverAlphabet} proves {@code UNSATISFIABLE} from: a verdict of
 * "never" is checked against every extension of the prefix over a small alphabet, on generated patterns with anchors, boundaries,
 * look-arounds and lazy and possessive quantifiers.
 */
class TimeBoundRegexOracleTest {
  private static final String ALPHABET = "ab";

  private final Random random = new Random(0x7b3dL);

  private String atom(final int depth) {
    return switch (random.nextInt(depth > 2 ? 9 : 16)) {
      case 0, 1, 2 -> "a";
      case 3, 4 -> "b";
      case 5 -> ".";
      case 6 -> "[ab]";
      case 7 -> "[^a]";
      case 8 -> random.nextBoolean() ? "^" : "$";
      case 9 -> "(" + alternation(depth + 1) + ")";
      case 10 -> "(?:" + alternation(depth + 1) + ")";
      case 11 -> "\\b";
      case 12 -> "(?=" + alternation(depth + 1) + ")";
      case 13 -> "(?!" + alternation(depth + 1) + ")";
      case 14 -> "(?<=a)";
      default -> "\\Z";
    };
  }

  private String piece(final int depth) {
    final String atom = atom(depth);
    if (atom.equals("^") || atom.equals("$") || atom.equals("\\b") || atom.startsWith("(?=") || atom.startsWith("(?!")
        || atom.startsWith("(?<") || atom.equals("\\Z"))
      return atom;
    return switch (random.nextInt(9)) {
      case 0 -> atom + "*";
      case 1 -> atom + "+";
      case 2 -> atom + "?";
      case 3 -> atom + "{1,2}";
      case 4 -> atom + "*?";
      case 5 -> atom + "+?";
      case 6 -> atom + "*+";
      default -> atom;
    };
  }

  private String sequence(final int depth) {
    final StringBuilder text = new StringBuilder();
    for (int i = 1 + random.nextInt(3); i > 0; i--)
      text.append(piece(depth));
    return text.toString();
  }

  private String alternation(final int depth) {
    final String first = sequence(depth);
    return random.nextInt(4) == 0 ? first + "|" + sequence(depth) : first;
  }

  private static List<String> strings(final int maxLength) {
    final List<String> all = new ArrayList<>();
    all.add("");
    int from = 0;
    for (int length = 1; length <= maxLength; length++) {
      final int to = all.size();
      for (int i = from; i < to; i++)
        for (final char c : ALPHABET.toCharArray())
          all.add(all.get(i) + c);
      from = to;
    }
    return all;
  }

  @Test
  void aVerdictOfNeverIsSoundAndMatchAgreesWithTheMatcher() {
    final List<String> prefixes = strings(3);
    final List<String> suffixes = strings(4);
    int patterns = 0;
    int never = 0;
    int mayMatchLonger = 0;
    for (int round = 0; round < 1500; round++) {
      final String regex = alternation(0);
      final Pattern pattern;
      try {
        pattern = Pattern.compile(regex);
      } catch (final PatternSyntaxException e) {
        continue;
      }
      patterns++;
      for (final String prefix : prefixes) {
        final TimeBoundRegex.FullMatch verdict = TimeBoundRegex.fullMatchUntil(pattern, prefix, Long.MAX_VALUE);
        assertThat(verdict == TimeBoundRegex.FullMatch.MATCH).as("'" + prefix + "' against " + regex).isEqualTo(pattern.matcher(prefix).matches());
        if (verdict == TimeBoundRegex.FullMatch.MAY_MATCH_LONGER)
          mayMatchLonger++;
        if (verdict != TimeBoundRegex.FullMatch.NEVER_WITH_THIS_PREFIX)
          continue;
        never++;
        for (final String suffix : suffixes)
          if (!suffix.isEmpty())
            assertThat(pattern.matcher(prefix + suffix).matches()).as(regex + ": 'never' for prefix '" + prefix + "' yet it matches '" + prefix + suffix + "'")
                .isFalse();
      }
    }
    assertThat(patterns).isGreaterThan(1000);
    assertThat(never).as("prefixes that no extension can rescue").isGreaterThan(1000);
    assertThat(mayMatchLonger).as("prefixes that an extension may rescue").isGreaterThan(1000);
  }

  @Test
  void everyOperationAnswersWhatThePlainMatcherAnswersWithTheBoundOnAndOff() {
    final List<String> inputs = strings(4);
    int compared = 0;
    for (int round = 0; round < 300; round++) {
      final String regex = alternation(0);
      final Pattern pattern;
      try {
        pattern = Pattern.compile(regex);
      } catch (final PatternSyntaxException e) {
        continue;
      }
      for (final String input : inputs) {
        final Matcher plain = pattern.matcher(input);
        final boolean matches = plain.matches();
        final boolean found = pattern.matcher(input).find();
        final String replaced = pattern.matcher(input).replaceAll("<$0>");
        final String what = "'" + input + "' against " + regex;
        for (final long timeout : new long[] { 0L, -1L, 60_000L }) {
          assertThat(TimeBoundRegex.matches(pattern, input, timeout)).as(what + " matches " + timeout).isEqualTo(matches);
          assertThat(TimeBoundRegex.matchesUntil(pattern, input, TimeBoundRegex.newDeadline(timeout))).as(what + " matchesUntil " + timeout)
              .isEqualTo(matches);
          assertThat(TimeBoundRegex.findUntil(pattern, input, TimeBoundRegex.newDeadline(timeout))).as(what + " findUntil " + timeout)
              .isEqualTo(found);
          assertThat(TimeBoundRegex.replaceAll(pattern, input, "<$0>", timeout)).as(what + " replaceAll " + timeout).isEqualTo(replaced);
          assertThat(TimeBoundRegex.replaceAllUntil(pattern, input, "<$0>", TimeBoundRegex.newDeadline(timeout))).as(what + " replaceAllUntil " + timeout)
              .isEqualTo(replaced);
        }
        compared++;
      }
    }
    assertThat(compared).isGreaterThan(5000);
  }

  @Test
  void anOperationStartedAfterItsDeadlineIsAbortedWhateverTheInputAndAnUnboundedOneIsAnswered() {
    final Pattern pattern = Pattern.compile("(a|b)*c");
    final long passed = System.nanoTime() - 1_000_000_000L;
    for (final String input : new String[] { "", "ab", "ab".repeat(2_000) }) {
      assertThatThrownBy(() -> TimeBoundRegex.matchesUntil(pattern, input, passed)).isInstanceOf(TimeoutException.class)
          .hasMessageContaining("(a|b)*c").hasMessageContaining("length " + input.length());
      assertThatThrownBy(() -> TimeBoundRegex.findUntil(pattern, input, passed)).isInstanceOf(TimeoutException.class);
      assertThatThrownBy(() -> TimeBoundRegex.replaceAllUntil(pattern, input, "x", passed)).isInstanceOf(TimeoutException.class);
      assertThatThrownBy(() -> TimeBoundRegex.fullMatchUntil(pattern, input, passed)).isInstanceOf(TimeoutException.class);
    }
    // the same input with no bound, or one that has not passed, is answered
    final String longInput = "ab".repeat(100);
    assertThat(TimeBoundRegex.matchesUntil(pattern, longInput, Long.MAX_VALUE)).isFalse();
    assertThat(TimeBoundRegex.matchesUntil(pattern, longInput + "c", TimeBoundRegex.newDeadline(60_000L))).isTrue();
  }
}
