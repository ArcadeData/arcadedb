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
package com.arcadedb.function.cypher;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The code-point string helpers of {@link CypherFunctionHelper} against references written on an {@code int[]} of code points
 * (or on {@code String.replace} / {@code String.split} where no surrogate is involved), over random strings that mix BMP
 * characters, whole surrogate pairs and lone surrogates.
 */
class CypherFunctionHelperCodePointOracleTest {
  private static final String[] PIECES = { "a", "b", "\uD83D\uDE00", "\uD83D", "\uDE00", "\u00e9", "ab", "\uD83D\uDE00\uD83D\uDE00" };

  private static String random(final Random random) {
    final StringBuilder text = new StringBuilder();
    for (int i = random.nextInt(7); i > 0; i--)
      text.append(PIECES[random.nextInt(PIECES.length)]);
    return text.toString();
  }

  private static boolean hasLoneSurrogate(final int[] codePoints) {
    for (final int codePoint : codePoints)
      if (codePoint >= Character.MIN_SURROGATE && codePoint <= Character.MAX_SURROGATE)
        return true;
    return false;
  }

  private static int indexIn(final int[] text, final int[] needle, final int from) {
    for (int i = Math.max(0, from); i + needle.length <= text.length; i++)
      if (Arrays.equals(text, i, i + needle.length, needle, 0, needle.length))
        return i;
    return -1;
  }

  @Test
  void leftRightSubstringAndCharAtCountCodePoints() {
    final Random random = new Random(7);
    for (int round = 0; round < 50_000; round++) {
      final String text = random(random);
      final int[] cps = text.codePoints().toArray();
      final int length = random.nextInt(9);
      assertThat(CypherFunctionHelper.leftCodePoints(text, length)).as("left %s %s", text, length)
          .isEqualTo(new String(cps, 0, Math.min(length, cps.length)));
      assertThat(CypherFunctionHelper.rightCodePoints(text, length)).as("right %s %s", text, length)
          .isEqualTo(new String(cps, Math.max(0, cps.length - length), Math.min(length, cps.length)));

      final int start = random.nextInt(8);
      final long window = random.nextInt(4) == 0 ? Long.MAX_VALUE : random.nextInt(8);
      final int from = Math.min(start, cps.length);
      final int to = (int) Math.min(cps.length, window == Long.MAX_VALUE ? cps.length : from + window);
      assertThat(CypherFunctionHelper.substringCodePoints(text, start, window)).as("substring %s %s %s", text, start, window)
          .isEqualTo(new String(cps, from, to - from));

      final int index = random.nextInt(9) - 1;
      assertThat(CypherFunctionHelper.charAtCodePoint(text, index)).as("charAt %s %s", text, index)
          .isEqualTo(index < 0 || index >= cps.length ? null : new String(cps, index, 1));
    }
  }

  @Test
  void indexOfAndLastIndexOfCountCodePoints() {
    final Random random = new Random(8);
    for (int round = 0; round < 50_000; round++) {
      final String text = random(random);
      final int[] cps = text.codePoints().toArray();
      final String needle = PIECES[random.nextInt(PIECES.length)] + (random.nextBoolean() ? PIECES[random.nextInt(PIECES.length)] : "");
      final int[] needleCps = needle.codePoints().toArray();
      // a needle with a lone surrogate may match half of a pair, which no reference over code points can say
      if (hasLoneSurrogate(needleCps))
        continue;

      final int from = random.nextInt(6) - 1;
      assertThat(CypherFunctionHelper.indexOfCodePoints(text, needle, from)).as("indexOf %s %s %s", text, needle, from)
          .isEqualTo(indexIn(cps, needleCps, from));

      final int last = random.nextInt(8) - 1;
      int expected = -1;
      if (last >= 0)
        for (int i = Math.min(last, cps.length - needleCps.length); i >= 0 && expected < 0; i--)
          if (Arrays.equals(cps, i, i + needleCps.length, needleCps, 0, needleCps.length))
            expected = i;
      assertThat(CypherFunctionHelper.lastIndexOfCodePoints(text, needle, last)).as("lastIndexOf %s %s %s", text, needle, last)
          .isEqualTo(expected);
    }
  }

  @Test
  void splitAndReplaceLiteralAgreeWithTheJdkOnTextWithoutSurrogates() {
    final Random random = new Random(3);
    final String[] delimiters = { "a", "b", "ab", "ba", "aa", "abc", "c", "bb", ",", "a," };
    for (int round = 0; round < 50_000; round++) {
      final StringBuilder builder = new StringBuilder();
      for (int i = random.nextInt(9); i > 0; i--)
        builder.append("abc,".charAt(random.nextInt(4)));
      final String text = builder.toString();

      final String delimiter = delimiters[random.nextInt(delimiters.length)];
      assertThat(CypherFunctionHelper.splitLiteral(text, delimiter)).as("split %s by %s", text, delimiter)
          .isEqualTo(Arrays.asList(text.split(Pattern.quote(delimiter), -1)));

      final String replacement = random.nextBoolean() ? "" : "XY";
      assertThat(CypherFunctionHelper.replaceLiteral(text, delimiter, replacement)).as("replace %s %s %s", text, delimiter, replacement)
          .isEqualTo(text.replace(delimiter, replacement));

      // several delimiters: cut at the leftmost one, the first of the list when two start at the same place
      final List<String> list = new ArrayList<>();
      for (int i = 1 + random.nextInt(3); i > 0; i--)
        list.add(delimiters[random.nextInt(delimiters.length)]);
      final List<String> expected = new ArrayList<>();
      int from = 0;
      while (true) {
        int best = -1;
        String found = null;
        for (final String candidate : list) {
          final int at = text.indexOf(candidate, from);
          if (at >= 0 && (best < 0 || at < best)) {
            best = at;
            found = candidate;
          }
        }
        if (best < 0)
          break;
        expected.add(text.substring(from, best));
        from = best + found.length();
      }
      expected.add(text.substring(from));
      assertThat(CypherFunctionHelper.splitLiteral(text, list)).as("split %s by %s", text, list).isEqualTo(expected);
    }
  }
}
