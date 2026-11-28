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
package com.arcadedb.function.cypher;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link CypherFunctionHelper#splitLiteral} and {@link CypherFunctionHelper#replaceLiteral} against {@code java.util.regex} on the
 * quoted delimiter, which reads a text by code points as they are meant to: over texts made of ordinary letters, whole surrogate
 * pairs and lone surrogate halves, and delimiters that are letters, pairs and lone halves.
 */
class CypherFunctionHelperLiteralOracleTest {
  private static final String[] PIECES     = { "a", "b", "ab", "aa", ",", "\uD83D\uDE00", "\uD83D", "\uDE00", "\uD83D\uDE01", "x" };
  private static final String[] DELIMITERS = { "a", "b", "ab", "aa", "\uD83D\uDE00", "\uD83D", "\uDE00", "\uDE00a", "b\uD83D", ",a", "x" };

  private static String text(final Random random) {
    final StringBuilder text = new StringBuilder();
    for (int i = random.nextInt(9); i > 0; i--)
      text.append(PIECES[random.nextInt(PIECES.length)]);
    return text.toString();
  }

  @Test
  void splittingAtOneDelimiterIsWhatTheQuotedPatternSplitsKeepingEveryEmptyPiece() {
    final Random random = new Random(0x12396L);
    int pieces = 0;
    for (int round = 0; round < 6000; round++) {
      final String text = text(random);
      final String delimiter = DELIMITERS[random.nextInt(DELIMITERS.length)];
      final List<String> expected = Arrays.asList(Pattern.compile(Pattern.quote(delimiter)).split(text, -1));
      assertThat(CypherFunctionHelper.splitLiteral(text, delimiter)).as(escape(text) + " split at " + escape(delimiter)).isEqualTo(expected);
      pieces += expected.size();
    }
    assertThat(pieces).as("pieces compared").isGreaterThan(7_000);
  }

  @Test
  void splittingAtSeveralDelimitersCutsAtTheLeftmostAndTheFirstOfTheListWinsATie() {
    final Random random = new Random(0x12397L);
    int cuts = 0;
    for (int round = 0; round < 6000; round++) {
      final String text = text(random);
      final List<String> delimiters = new ArrayList<>();
      for (int i = 1 + random.nextInt(3); i > 0; i--)
        delimiters.add(DELIMITERS[random.nextInt(DELIMITERS.length)]);
      final StringBuilder regex = new StringBuilder();
      for (final String delimiter : delimiters)
        regex.append(regex.length() == 0 ? "" : "|").append(Pattern.quote(delimiter));
      final List<String> expected = Arrays.asList(Pattern.compile(regex.toString()).split(text, -1));
      assertThat(CypherFunctionHelper.splitLiteral(text, delimiters)).as(escape(text) + " split at " + escape(delimiters.toString()))
          .isEqualTo(expected);
      cuts += expected.size() - 1;
    }
    assertThat(cuts).as("cuts compared").isGreaterThan(3_000);
  }

  @Test
  void anEmptyDelimiterSplitsIntoCharactersAndAPairIsOne() {
    assertThat(CypherFunctionHelper.splitLiteral("", "")).containsExactly("");
    assertThat(CypherFunctionHelper.splitLiteral("a\uD83D\uDE00b", "")).containsExactly("a", "\uD83D\uDE00", "b");
    assertThat(CypherFunctionHelper.splitLiteral("\uD83D\uDE00\uD83D", "")).containsExactly("\uD83D\uDE00", "\uD83D");
    assertThat(CypherFunctionHelper.splitLiteral("abc", List.of())).containsExactly("abc");
    assertThat(CypherFunctionHelper.splitLiteral("abc", List.of("", "a"))).containsExactly("b", "c");
    assertThat(CypherFunctionHelper.splitLiteral("aaa", List.of("", "a"))).isEmpty();
    assertThat(CypherFunctionHelper.splitLiteral("", List.of(""))).containsExactly("");
    assertThat(CypherFunctionHelper.splitLiteral("", List.of("a"))).containsExactly("");
  }

  @Test
  void replacingALiteralIsWhatTheQuotedPatternReplacesAndTheTextItselfWhenNothingIsReplaced() {
    final Random random = new Random(0x10485L);
    int replaced = 0;
    for (int round = 0; round < 6000; round++) {
      final String text = text(random);
      final String search = DELIMITERS[random.nextInt(DELIMITERS.length)];
      final String replacement = random.nextBoolean() ? "$1\\" : random.nextBoolean() ? "" : "\uD83D\uDE00-";
      final Matcher matcher = Pattern.compile(Pattern.quote(search)).matcher(text);
      final String expected = matcher.replaceAll(Matcher.quoteReplacement(replacement));
      final String actual = CypherFunctionHelper.replaceLiteral(text, search, replacement);
      assertThat(actual).as(escape(text) + " replacing " + escape(search) + " by " + escape(replacement)).isEqualTo(expected);
      if (expected.equals(text))
        assertThat(actual).isSameAs(text);
      else
        replaced++;
    }
    assertThat(replaced).as("texts that changed").isGreaterThan(1_000);
  }

  @Test
  void anEmptySearchPutsTheReplacementBeforeEveryCharacterAndAfterTheLastWithoutCuttingAPair() {
    assertThat(CypherFunctionHelper.replaceLiteral("", "", "-")).isEqualTo("-");
    assertThat(CypherFunctionHelper.replaceLiteral("a\uD83D\uDE00", "", "-")).isEqualTo("-a-\uD83D\uDE00-");
    assertThat(CypherFunctionHelper.replaceLiteral("ab", "", "")).isEqualTo("ab");
    assertThat(CypherFunctionHelper.replaceLiteral("\uD83D\uDE00\uDE00", "", "|")).isEqualTo("|\uD83D\uDE00|\uDE00|");
  }

  private static String escape(final String text) {
    final StringBuilder escaped = new StringBuilder();
    for (final char c : text.toCharArray())
      escaped.append(c < 128 && c >= 32 ? String.valueOf(c) : String.format("\\u%04x", (int) c));
    return escaped.toString();
  }
}
