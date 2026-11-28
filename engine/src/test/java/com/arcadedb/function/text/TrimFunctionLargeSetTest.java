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
package com.arcadedb.function.text;

import com.arcadedb.query.opencypher.executor.CypherTrimFunction;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The trim functions that take a set of characters to remove cost time in proportion to the text plus the set, not to their
 * product, and answer for a set of any size what they answer for a small one.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class TrimFunctionLargeSetTest {
  /** Characters in the text and in the set of the timed cases: a product of them is far out of reach, their sum is nothing. */
  private static final int    SIZE      = 400_000;
  /** Every character of the text but one is in this set, and the first one it names is the last of the set. */
  private static final String SET       = "b".repeat(SIZE - 1) + "a";
  private static final String LEADING   = "a".repeat(SIZE - 1) + "z";
  private static final String TRAILING  = "z" + "a".repeat(SIZE - 1);
  private static final String BOTH      = "a".repeat(SIZE / 2) + "z" + "a".repeat(SIZE / 2);
  private static final long   BOUND_MS  = 4_000;
  private static final String CLAIM     = "work in proportion to the text and the set, not to their product";

  private static final String ASTRAL_A = new String(Character.toChars(0x1F600));
  private static final String ASTRAL_B = new String(Character.toChars(0x1F601));

  // ---------------------------------------------------------------- time

  @Test
  void sqlTrimWithALargeSetCostsTheSizeOfItsArguments() {
    final TrimFunction function = new TrimFunction();

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThat(function.execute(new Object[] { "LEADING", SET, LEADING }, null)).isEqualTo("z");
    assertThat(function.execute(new Object[] { "TRAILING", SET, TRAILING }, null)).isEqualTo("z");
    assertThat(function.execute(new Object[] { "BOTH", SET, BOTH }, null)).isEqualTo("z");
    stopwatch.assertStayedUnder(BOUND_MS, CLAIM);
  }

  @Test
  void lTrimAndRTrimWithALargeSetCostTheSizeOfTheirArguments() {
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThat(new LTrimFunction().execute(new Object[] { LEADING, SET }, null)).isEqualTo("z");
    assertThat(new RTrimFunction().execute(new Object[] { TRAILING, SET }, null)).isEqualTo("z");
    stopwatch.assertStayedUnder(BOUND_MS, CLAIM);
  }

  @Test
  void cypherTrimWithALargeSetCostsTheSizeOfItsArguments() {
    final CypherTrimFunction function = new CypherTrimFunction();

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThat(function.execute(new Object[] { BOTH, SET }, null)).isEqualTo("z");
    assertThat(function.execute(new Object[] { "LEADING", SET, LEADING }, null)).isEqualTo("z");
    assertThat(function.execute(new Object[] { "TRAILING", SET, TRAILING }, null)).isEqualTo("z");
    assertThat(function.execute(new Object[] { "BOTH", SET, BOTH }, null)).isEqualTo("z");
    stopwatch.assertStayedUnder(BOUND_MS, CLAIM);
  }

  // ---------------------------------------------------------------- a set of any size answers as a small one does

  @Test
  void aLargeSetStripsTheEndsOfTheText() {
    final String set = "0123456789abcdef";

    assertThat(TrimFunction.stripLeading("00ff-x-ff00", set)).isEqualTo("-x-ff00");
    assertThat(TrimFunction.stripTrailing("00ff-x-ff00", set)).isEqualTo("00ff-x-");
    assertThat(TrimFunction.strip("00ff-x-ff00", set, true, true)).isEqualTo("-x-");
    assertThat(TrimFunction.strip("00ff-x-ff00", set, true, false)).isEqualTo("-x-ff00");
    assertThat(TrimFunction.strip("00ff-x-ff00", set, false, true)).isEqualTo("00ff-x-");
    assertThat(TrimFunction.strip("00ff-x-ff00", set, false, false)).isEqualTo("00ff-x-ff00");
  }

  @Test
  void textMadeOfTheSetIsStrippedToNothing() {
    final String set = "0123456789abcdef";

    assertThat(TrimFunction.strip("cafe0123", set, true, true)).isEmpty();
    assertThat(TrimFunction.stripLeading("cafe0123", set)).isEmpty();
    assertThat(TrimFunction.stripTrailing("cafe0123", set)).isEmpty();
    assertThat(TrimFunction.strip("", set, true, true)).isEmpty();
  }

  @Test
  void textWithNothingToStripIsReturnedAsItIs() {
    final String text = "-x-";
    assertThat(TrimFunction.strip(text, "0123456789abcdef", true, true)).isSameAs(text);
    assertThat(TrimFunction.strip(text, "x", true, true)).isSameAs(text);
  }

  @Test
  void aLargeSetNeverStripsHalfOfAPair() {
    // the set holds U+1F600 and many other characters; U+1F601 shares its high surrogate and U+1F200 shares its low one
    final String set = "0123456789abcdef" + ASTRAL_A;
    final String sameHigh = ASTRAL_B;
    final String sameLow = new String(Character.toChars(0x1F200));

    assertThat(TrimFunction.strip(ASTRAL_A + "x" + ASTRAL_A, set, true, true)).isEqualTo("x");
    assertThat(TrimFunction.strip(sameHigh + "x", set, true, true)).isEqualTo(sameHigh + "x");
    assertThat(TrimFunction.strip("x" + sameHigh, set, true, true)).isEqualTo("x" + sameHigh);
    assertThat(TrimFunction.strip(sameLow + "x", set, true, true)).isEqualTo(sameLow + "x");
    assertThat(TrimFunction.strip("x" + sameLow, set, true, true)).isEqualTo("x" + sameLow);
  }

  @Test
  void aLoneSurrogateOfALargeSetMatchesALoneSurrogateOnly() {
    final String lone = String.valueOf((char) 0xD83D);
    final String setWithLone = "0123456789abcdef" + lone;
    final String setWithPair = "0123456789abcdef" + ASTRAL_A;

    assertThat(TrimFunction.strip(lone + "x" + lone, setWithLone, true, true)).isEqualTo("x");
    assertThat(TrimFunction.strip(lone + "x", setWithPair, true, true)).isEqualTo(lone + "x");
    assertThat(TrimFunction.strip(ASTRAL_A + "x", setWithLone, true, true)).isEqualTo(ASTRAL_A + "x");
  }

  @Test
  void aSetOfAnySizeAnswersAsTheCharacterByCharacterDefinitionDoes() {
    // text and sets drawn from characters that tell the cases apart: basic plane, both halves of pairs, whole pairs, lone halves
    final int[] pool = { 'a', 'b', 'c', 'd', 'e', 0xE9, 0x4E2D, 0xD83D, 0xDE00, 0x1F600, 0x1F601, 0x1F200, 0x10FFFF, 0x10000 };
    final Random random = new Random(20260101L);

    for (int round = 0; round < 30_000; round++) {
      final String text = randomText(random, pool, random.nextInt(24));
      final String set = randomText(random, pool, 1 + random.nextInt(40));

      assertThat(TrimFunction.stripLeading(text, set)).as("leading %s / %s", escape(text), escape(set))
          .isEqualTo(referenceLeading(text, set));
      assertThat(TrimFunction.stripTrailing(text, set)).as("trailing %s / %s", escape(text), escape(set))
          .isEqualTo(referenceTrailing(text, set));
      assertThat(TrimFunction.strip(text, set, true, true)).as("both %s / %s", escape(text), escape(set))
          .isEqualTo(referenceLeading(referenceTrailing(text, set), set));
    }
  }

  // ---------------------------------------------------------------- helpers

  private static String randomText(final Random random, final int[] pool, final int length) {
    final StringBuilder builder = new StringBuilder();
    for (int i = 0; i < length; i++)
      builder.appendCodePoint(pool[random.nextInt(pool.length)]);
    return builder.toString();
  }

  private static String escape(final String text) {
    final StringBuilder builder = new StringBuilder();
    for (int i = 0; i < text.length(); i++)
      builder.append(String.format("\\u%04x", (int) text.charAt(i)));
    return builder.toString();
  }

  /** The definition: a character is a code point, and a lone surrogate matches a lone surrogate of the set only. */
  private static boolean inSet(final String set, final int codePoint) {
    for (int i = 0; i < set.length(); ) {
      final int member = set.codePointAt(i);
      if (member == codePoint)
        return true;
      i += Character.charCount(member);
    }
    return false;
  }

  private static String referenceLeading(final String text, final String set) {
    int start = 0;
    while (start < text.length()) {
      final int codePoint = text.codePointAt(start);
      if (!inSet(set, codePoint))
        break;
      start += Character.charCount(codePoint);
    }
    return text.substring(start);
  }

  private static String referenceTrailing(final String text, final String set) {
    int end = text.length();
    while (end > 0) {
      final int codePoint = text.codePointBefore(end);
      if (!inSet(set, codePoint))
        break;
      end -= Character.charCount(codePoint);
    }
    return text.substring(0, end);
  }
}
