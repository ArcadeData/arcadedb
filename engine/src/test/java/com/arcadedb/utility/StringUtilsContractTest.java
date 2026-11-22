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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Contract of {@link StringUtils}: the width ceiling of {@link StringUtils#format} against the specifier grammar of
 * {@link java.util.Formatter}, {@link StringUtils#editDistance} against a textbook recursion, {@link StringUtils#containsIgnoreCase}
 * against {@code String.contains}, the strict boolean and the "did you mean" threshold.
 */
class StringUtilsContractTest {
  private static final String[] FLAGS  = { "", "-", "0", "+", ",", "-0", "#", " ", "(", "<" };
  private static final String[] WIDTHS = { null, "1", "7", "999999", "1000000", "1000001", "2000000", "99999999999", "4294967297" };
  private static final String[] CONVS  = { "s", "d", "x", "f", "S" };
  private static final String[] LITERALS = { "ab", "99999999", "%%", " ", "2000000", "-", "." };

  /** One specifier, and the widest number it names (the ceiling is on that). */
  private record Spec(String text, long widest) {
  }

  private static Spec spec(final Random random, final boolean allowHuge) {
    final StringBuilder text = new StringBuilder("%");
    final boolean index = random.nextInt(4) == 0;
    if (index)
      text.append(1 + random.nextInt(3)).append('$');
    text.append(FLAGS[random.nextInt(FLAGS.length)]);
    long widest = 0;
    final String width = WIDTHS[random.nextInt(WIDTHS.length)];
    if (width != null && (allowHuge || Long.parseLong(width) <= 1_000_000)) {
      text.append(width);
      widest = Long.parseLong(width);
    }
    if (random.nextInt(3) == 0) {
      final String precision = WIDTHS[1 + random.nextInt(WIDTHS.length - 1)];
      if (allowHuge || Long.parseLong(precision) <= 1_000_000) {
        text.append('.').append(precision);
        widest = Math.max(widest, Long.parseLong(precision));
      }
    }
    text.append(CONVS[random.nextInt(CONVS.length)]);
    return new Spec(text.toString(), widest);
  }

  @Test
  void aFieldOverTheCeilingIsRefusedWhereverTheSpecifierSitsAndNothingElseIsRefusedForIt() {
    final Random random = new Random(0x6389L);
    int refused = 0;
    int accepted = 0;
    for (int round = 0; round < 600; round++) {
      final StringBuilder format = new StringBuilder();
      long widest = 0;
      final int parts = 1 + random.nextInt(3);
      // a huge field is rare enough to keep the accepted ones (up to a megabyte each) cheap
      final boolean allowHuge = random.nextInt(2) == 0;
      for (int i = 0; i < parts; i++) {
        if (random.nextBoolean())
          format.append(LITERALS[random.nextInt(LITERALS.length)]);
        final Spec spec = spec(random, allowHuge);
        format.append(spec.text);
        widest = Math.max(widest, spec.widest);
        if (random.nextInt(5) == 0)
          // reuse of the previous argument, a flag the scanner must not take for the end of the specifier
          format.append("%<").append(random.nextInt(2) == 0 ? "" : "9").append("s");
      }
      final String pattern = format.toString();
      final Object[] arguments = { 1, 2, 3, 4 };

      if (widest > StringUtils.MAX_FORMAT_WIDTH) {
        assertThatThrownBy(() -> StringUtils.format("fn", pattern, arguments)).as(pattern).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("limit");
        refused++;
      } else {
        String expected;
        try {
          expected = String.format(Locale.ROOT, pattern, arguments);
        } catch (final IllegalFormatException e) {
          expected = null;
        }
        if (expected == null)
          assertThatThrownBy(() -> StringUtils.format("fn", pattern, arguments)).as(pattern).isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("cannot apply the format");
        else
          assertThat(StringUtils.format("fn", pattern, arguments)).as(pattern).isEqualTo(expected);
        accepted++;
      }
    }
    assertThat(refused).as("patterns over the ceiling").isGreaterThan(50);
    assertThat(accepted).as("patterns at or under it").isGreaterThan(50);
  }

  @Test
  void theCeilingIsExactlyOneMillion() {
    assertThat(StringUtils.format("fn", "%1000000s", "x")).hasSize(1_000_000);
    assertThatThrownBy(() -> StringUtils.format("fn", "%1000001s", "x")).hasMessageContaining("1000001");
    assertThatThrownBy(() -> StringUtils.format("fn", "%.1000001f", 1.0)).hasMessageContaining("1000001");
    assertThatThrownBy(() -> StringUtils.format("fn", "%s%<2000000s", "x")).hasMessageContaining("2000000");
    assertThatThrownBy(() -> StringUtils.format("fn", "%2000000.1s", "x")).hasMessageContaining("2000000");
    assertThatThrownBy(() -> StringUtils.format("fn", null)).hasMessageContaining("fn() requires a non-null format");
    // a number that is only an argument index is not a size: the refusal is the JDK's (no such argument), not the ceiling's
    assertThatThrownBy(() -> StringUtils.format("fn", "%2000000$s", "x")).hasMessageContaining("cannot apply the format");
  }

  /** The textbook definition, memoized on the pair of suffix positions. */
  private static int reference(final String a, final int i, final String b, final int j, final int[][] memo) {
    if (i == a.length())
      return b.length() - j;
    if (j == b.length())
      return a.length() - i;
    if (memo[i][j] >= 0)
      return memo[i][j];
    int best = Math.min(reference(a, i + 1, b, j, memo), reference(a, i, b, j + 1, memo)) + 1;
    best = Math.min(best, reference(a, i + 1, b, j + 1, memo) + (a.charAt(i) == b.charAt(j) ? 0 : 1));
    memo[i][j] = best;
    return best;
  }

  private static int referenceDistance(final String a, final String b) {
    final int[][] memo = new int[a.length() + 1][b.length() + 1];
    for (final int[] row : memo)
      Arrays.fill(row, -1);
    return reference(a, 0, b, 0, memo);
  }

  private static String word(final Random random) {
    final int length = random.nextInt(9);
    final StringBuilder word = new StringBuilder();
    for (int i = 0; i < length; i++)
      word.append("abc".charAt(random.nextInt(3)));
    return word.toString();
  }

  @Test
  void editDistanceIsTheLevenshteinDistance() {
    final Random random = new Random(11311L);
    for (int round = 0; round < 2000; round++) {
      final String a = word(random);
      final String b = word(random);
      assertThat(StringUtils.editDistance(a, b)).as("'" + a + "' to '" + b + "'").isEqualTo(referenceDistance(a, b));
    }
    assertThat(StringUtils.editDistance("", "")).isZero();
    assertThat(StringUtils.editDistance("kitten", "sitting")).isEqualTo(3);
  }

  @Test
  void closestMatchIsTheNearestCandidateWithinHalfTheNameAndTiesGoToTheNaturalOrder() {
    final Random random = new Random(11523L);
    for (int round = 0; round < 1500; round++) {
      final String name = word(random);
      final List<String> candidates = List.of(word(random), word(random), word(random), word(random));
      String expected = null;
      int expectedDistance = Integer.MAX_VALUE;
      for (final String candidate : candidates) {
        final int distance = referenceDistance(name, candidate);
        if (distance < expectedDistance || (distance == expectedDistance && candidate.compareTo(expected) < 0)) {
          expected = candidate;
          expectedDistance = distance;
        }
      }
      if (expectedDistance > name.length() / 2)
        expected = null;
      assertThat(StringUtils.closestMatch(name, candidates)).as("'" + name + "' among " + candidates).isEqualTo(expected);
    }
    // case does not count, the candidate is answered as it was written
    assertThat(StringUtils.closestMatch("MAXSIZE", List.of("maxSize", "minSize"))).isEqualTo("maxSize");
    assertThat(StringUtils.closestMatch("abcd", List.of("zzzz"))).isNull();
    assertThat(StringUtils.closestMatch("abcd", List.of("abzz"))).isEqualTo("abzz");
    assertThat(StringUtils.closestMatch("abcd", List.of("azzz"))).isNull();
    assertThat(StringUtils.closestMatch("abcde", List.of("abzzz"))).isNull();
    assertThat(StringUtils.closestMatch("abcde", List.of("abczz"))).isEqualTo("abczz");
    assertThat(StringUtils.closestMatch("a", List.of())).isNull();
  }

  @Test
  void containsIgnoreCaseAgreesWithContainsOnAsciiText() {
    final Random random = new Random(21L);
    final String alphabet = "aAbB-_ ";
    for (int round = 0; round < 3000; round++) {
      final StringBuilder haystack = new StringBuilder();
      for (int i = random.nextInt(12); i > 0; i--)
        haystack.append(alphabet.charAt(random.nextInt(alphabet.length())));
      final StringBuilder needle = new StringBuilder();
      for (int i = random.nextInt(5); i > 0; i--)
        needle.append(alphabet.charAt(random.nextInt(alphabet.length())));
      final boolean expected = haystack.toString().toLowerCase(Locale.ROOT).contains(needle.toString().toLowerCase(Locale.ROOT));
      assertThat(StringUtils.containsIgnoreCase(haystack.toString(), needle.toString())).as("'" + needle + "' in '" + haystack + "'")
          .isEqualTo(expected);
    }
    assertThat(StringUtils.containsIgnoreCase("", "")).isTrue();
    assertThat(StringUtils.containsIgnoreCase("abc", "")).isTrue();
    assertThat(StringUtils.containsIgnoreCase("", "a")).isFalse();
    assertThat(StringUtils.containsIgnoreCase("ab", "abc")).isFalse();
    assertThat(StringUtils.containsIgnoreCase("xxLIMIT", "limit")).isTrue();
    assertThat(StringUtils.containsIgnoreCase("LIMITxx", "limit")).isTrue();
  }

  @Test
  void aBooleanSettingIsTrueOrFalseAndNothingElse() {
    assertThat(StringUtils.parseStrictBoolean("true")).isTrue();
    assertThat(StringUtils.parseStrictBoolean(" TRUE ")).isTrue();
    assertThat(StringUtils.parseStrictBoolean("False")).isFalse();
    assertThat(StringUtils.parseStrictBoolean("\tfalse\n")).isFalse();
    for (final String text : new String[] { "yes", "1", "on", "", "tru", "truee", "t", "null" })
      assertThatThrownBy(() -> StringUtils.parseStrictBoolean(text)).as(text).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("neither true nor false");
  }
}
