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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10484: after #10378 and #10417 the string functions counted and cut characters, but {@code ltrim()}/{@code rtrim()} with a
 * trim set, the {@code text.capitalize} family, {@code text.camelCase()} and friends, and the empty-search {@code replace()} /
 * {@code text.replace()} / {@code text.split()} still read a string one UTF-16 unit at a time: they stripped half of a surrogate
 * pair, left a cased letter outside the BMP as it was, treated a letter outside the BMP as a word break, and cut inside a pair. Each
 * reads the string by code point now, so the answer for a character outside the BMP is the answer for its BMP twin.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10484TextFunctionsCodePointsTest extends TestHelper {
  private static final String P = cp(0x1F600); // grinning face
  private static final String Q = cp(0x1F601); // its neighbour: the high surrogate is shared
  private static final String X = cp(0x1F200); // the low surrogate is shared with P
  private static final String SMALL_DESERET = cp(0x10428);
  private static final String CAPITAL_DESERET = cp(0x10400);
  private static final String SMALL_DESERET_2 = cp(0x10429);
  private static final String CAPITAL_DESERET_2 = cp(0x10401);
  private static final String E1 = cp(0x20000); // CJK Extension B, a letter with no case
  private static final String E2 = cp(0x20001);

  private static String cp(final int codePoint) {
    return new String(Character.toChars(codePoint));
  }

  // ---- ltrim / rtrim / btrim with a trim set

  @Test
  void ltrimNeverStripsHalfOfAPair() {
    // the high surrogate of Q is the high surrogate of P: a unit-wise trim removed it and returned the lone low half of Q
    assertThat(cypher("RETURN ltrim($s, $c) AS v", "s", Q + P, "c", P)).isEqualTo(Q + P);
    assertThat(cypher("RETURN ltrim($s, $c) AS v", "s", P + P + Q + "a", "c", P)).isEqualTo(Q + "a");
  }

  @Test
  void rtrimNeverStripsHalfOfAPair() {
    // the low surrogate of X is the low surrogate of P
    assertThat(cypher("RETURN rtrim($s, $c) AS v", "s", "x" + X, "c", P)).isEqualTo("x" + X);
    assertThat(cypher("RETURN rtrim($s, $c) AS v", "s", "a" + X + P + P, "c", P)).isEqualTo("a" + X);
  }

  @Test
  void trimWithASetOfCharactersOutsideTheBmpStripsWholeCharacters() {
    assertThat(cypher("RETURN ltrim($s, $c) AS v", "s", P + Q + "a" + P, "c", P + Q)).isEqualTo("a" + P);
    assertThat(cypher("RETURN rtrim($s, $c) AS v", "s", P + "a" + Q + P, "c", P + Q)).isEqualTo(P + "a");
    assertThat(cypher("RETURN btrim($s, $c) AS v", "s", P + Q + "a" + Q + P, "c", P + Q)).isEqualTo("a");
    assertThat(cypher("RETURN trim(BOTH $c FROM $s) AS v", "s", P + "a" + P, "c", P)).isEqualTo("a");
    assertThat(cypher("RETURN trim(LEADING $c FROM $s) AS v", "s", P + "a" + P, "c", P)).isEqualTo("a" + P);
    assertThat(cypher("RETURN trim(TRAILING $c FROM $s) AS v", "s", P + "a" + P, "c", P)).isEqualTo(P + "a");
  }

  @Test
  void trimBetweenAPairAndALoneSurrogateOfItsSetIsExact() {
    // a lone surrogate in the string is a character of its own: it matches a lone surrogate of the set, never half of a pair in it
    final String lone = "\uD83D";
    assertThat(cypher("RETURN ltrim($s, $c) AS v", "s", lone + "a", "c", P)).isEqualTo(lone + "a");
    assertThat(cypher("RETURN ltrim($s, $c) AS v", "s", lone + "a", "c", lone)).isEqualTo("a");
  }

  @Test
  void trimOfBasicPlaneTextKeepsItsBehavior() {
    assertThat(cypher("RETURN ltrim($s, $c) AS v", "s", "xxabcxx", "c", "x")).isEqualTo("abcxx");
    assertThat(cypher("RETURN rtrim($s, $c) AS v", "s", "xxabcxx", "c", "x")).isEqualTo("xxabc");
    assertThat(cypher("RETURN btrim($s, $c) AS v", "s", "xyabcyx", "c", "xy")).isEqualTo("abc");
    assertThat(cypher("RETURN ltrim($s, $c) AS v", "s", "abc", "c", "")).isEqualTo("abc");
  }

  // ---- the capitalize family

  @Test
  void capitalizeAndDecapitalizeCaseMapTheFirstCharacterOutsideTheBmp() {
    assertThat(both("text.capitalize($s)", "s", SMALL_DESERET + "b")).isEqualTo(CAPITAL_DESERET + "b");
    assertThat(both("text.decapitalize($s)", "s", CAPITAL_DESERET + "A")).isEqualTo(SMALL_DESERET + "A");
    // the BMP twin keeps working
    assertThat(both("text.capitalize($s)", "s", "жb")).isEqualTo("Жb");
    assertThat(both("text.decapitalize($s)", "s", "ЖB")).isEqualTo("жB");
  }

  @Test
  void capitalizeAllAndDecapitalizeAllCaseMapTheFirstCharacterOfEveryWord() {
    assertThat(both("text.capitalizeAll($s)", "s", SMALL_DESERET + " " + SMALL_DESERET_2 + "x")).isEqualTo(
        CAPITAL_DESERET + " " + CAPITAL_DESERET_2 + "x");
    assertThat(both("text.decapitalizeAll($s)", "s", CAPITAL_DESERET + " " + CAPITAL_DESERET_2 + "X")).isEqualTo(
        SMALL_DESERET + " " + SMALL_DESERET_2 + "X");
    assertThat(both("text.capitalizeAll($s)", "s", "ж з")).isEqualTo("Ж З");
  }

  @Test
  void aCharacterThatHasNoCaseIsLeftAsIsWholeAndNothingIsLost() {
    assertThat(both("text.capitalize($s)", "s", P + "b")).isEqualTo(P + "b");
    assertThat(both("text.capitalizeAll($s)", "s", P + " " + Q)).isEqualTo(P + " " + Q);
    assertThat(both("text.capitalize($s)", "s", "")).isEqualTo("");
  }

  // ---- camelCase, snakeCase, upperCamelCase

  @Test
  void aLetterOutsideTheBmpIsNotAWordBreak() {
    final String twin = "一丁x";
    final String supplementary = E1 + E2 + "x";
    // the letters have no case, so the answer is the input, as for the BMP twin
    assertThat(both("text.camelCase($s)", "s", twin)).isEqualTo(twin);
    assertThat(both("text.camelCase($s)", "s", supplementary)).isEqualTo(supplementary);
    assertThat(both("text.snakeCase($s)", "s", twin)).isEqualTo(twin);
    assertThat(both("text.snakeCase($s)", "s", supplementary)).isEqualTo(supplementary);
    assertThat(both("text.upperCamelCase($s)", "s", twin)).isEqualTo(twin);
    assertThat(both("text.upperCamelCase($s)", "s", supplementary)).isEqualTo(supplementary);
  }

  @Test
  void theWordFunctionsCaseMapLettersOutsideTheBmp() {
    assertThat(both("text.camelCase($s)", "s", SMALL_DESERET + " " + SMALL_DESERET)).isEqualTo(SMALL_DESERET + CAPITAL_DESERET);
    assertThat(both("text.camelCase($s)", "s", CAPITAL_DESERET + "_" + CAPITAL_DESERET_2)).isEqualTo(SMALL_DESERET + CAPITAL_DESERET_2);
    assertThat(both("text.upperCamelCase($s)", "s", SMALL_DESERET + " " + SMALL_DESERET_2 + "X")).isEqualTo(
        CAPITAL_DESERET + CAPITAL_DESERET_2 + "x");
    assertThat(both("text.snakeCase($s)", "s", "a" + CAPITAL_DESERET + "b")).isEqualTo("a_" + SMALL_DESERET + "b");
  }

  @Test
  void aSymbolOutsideTheBmpIsStillAWordBreak() {
    // emoji are not letters or digits: they separate words, as the punctuation of the BMP does
    assertThat(both("text.camelCase($s)", "s", "ab" + P + "cd")).isEqualTo("abCd");
    assertThat(both("text.upperCamelCase($s)", "s", "ab" + P + "cd")).isEqualTo("AbCd");
    assertThat(both("text.snakeCase($s)", "s", "ab" + P + "cd")).isEqualTo("ab_cd");
  }

  // ---- empty-search replace and empty-delimiter split

  @Test
  void replaceWithAnEmptySearchInsertsBetweenCharactersAndNeverInsideAPair() {
    final String expected = "-a-" + P + "-";
    assertThat(cypher("RETURN replace($s, '', '-') AS v", "s", "a" + P)).isEqualTo(expected);
    assertThat(both("text.replace($s, '', '-')", "s", "a" + P)).isEqualTo(expected);
    assertThat(cypher("RETURN replace($s, '', '-') AS v", "s", "")).isEqualTo("-");
    assertThat(cypher("RETURN replace($s, '', '-') AS v", "s", "abc")).isEqualTo("-a-b-c-");
  }

  @Test
  void textSplitWithAnEmptyDelimiterSplitsIntoCharacters() {
    assertThat(both("text.split($s, '')", "s", "a" + P)).isEqualTo(List.of("a", P));
    assertThat(cypher("RETURN split($s, '') AS v", "s", "a" + P)).isEqualTo(List.of("a", P));
  }

  @Test
  void textSplitKeepsTheEmptyTrailingPieceLikeSplit() {
    assertThat(both("text.split($s, $c)", "s", "a" + P, "c", P)).isEqualTo(List.of("a", ""));
    assertThat(cypher("RETURN split($s, $c) AS v", "s", "a" + P, "c", P)).isEqualTo(List.of("a", ""));
  }

  /** The functions of one family answer one way: the same string through the twin functions gives the same answer. */
  @Test
  void theTextFunctionsAgreeWithTheirCypherTwins() {
    for (final String s : new String[] { "", "a", "a" + P, P + P, "a" + P + "b" + Q + "c", E1 + "x" }) {
      assertThat(both("text.replace($s, '', '-')", "s", s)).as("replace in '%s'", s).isEqualTo(cypher("RETURN replace($s, '', '-') AS v", "s", s));
      assertThat(both("text.split($s, '')", "s", s)).as("split '%s'", s).isEqualTo(cypher("RETURN split($s, '') AS v", "s", s));
      assertThat(both("text.split($s, 'b')", "s", s)).as("split '%s' by b", s).isEqualTo(cypher("RETURN split($s, 'b') AS v", "s", s));
    }
  }

  @Test
  void randomDrawsWholeCharactersOutOfASetHoldingEmoji() {
    final String text = (String) cypher("RETURN text.random(40, $c) AS v", "c", P + Q);
    assertThat(text.codePointCount(0, text.length())).isEqualTo(40);
    assertThat(text.codePoints().allMatch(codePoint -> codePoint == 0x1F600 || codePoint == 0x1F601)).isTrue();
  }

  // ---- helpers

  private Object cypher(final String query, final Object... keyValues) {
    try (final ResultSet rs = database.query("opencypher", query, params(keyValues))) {
      return rs.next().getProperty("v");
    }
  }

  /** The expression in SQL and in openCypher, asserted to agree, and the answer. */
  private Object both(final String expression, final Object... keyValues) {
    final Map<String, Object> params = params(keyValues);
    final Object sql;
    try (final ResultSet rs = database.query("sql", "SELECT " + expression.replace("$", ":") + " AS v", params)) {
      sql = rs.next().getProperty("v");
    }
    final Object cypher;
    try (final ResultSet rs = database.query("opencypher", "RETURN " + expression + " AS v", params)) {
      cypher = rs.next().getProperty("v");
    }
    assertThat(cypher).as("openCypher and SQL answers for %s", expression).isEqualTo(sql);
    return sql;
  }

  private static Map<String, Object> params(final Object... keyValues) {
    final Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2)
      map.put((String) keyValues[i], keyValues[i + 1]);
    return map;
  }
}
