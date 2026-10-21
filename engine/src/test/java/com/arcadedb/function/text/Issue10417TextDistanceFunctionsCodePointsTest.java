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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

/**
 * Issue #10417: after #10378 and #10248 the string functions counted characters, but the distance and similarity functions of the
 * {@code text.*} family ({@code hammingDistance}, {@code levenshteinDistance}, {@code levenshteinSimilarity},
 * {@code jaroWinklerDistance}, {@code sorensenDiceSimilarity}) still read a string one UTF-16 unit at a time, so a character outside
 * the BMP counted twice, half of it could match half of another, and a bigram could be cut inside a surrogate pair. Each reads the
 * string by code point now: the number for a pair of strings is the number for the same pair written with BMP characters.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10417TextDistanceFunctionsCodePointsTest extends TestHelper {
  private static final String P = cp(0x1F600); // grinning face
  private static final String Q = cp(0x1F601); // its neighbour: the high surrogate is shared
  private static final String R = cp(0x1F800); // differs from P in both halves of the pair

  private static String cp(final int codePoint) {
    return new String(Character.toChars(codePoint));
  }

  /** The same string with every character replaced by a supplementary one; the high surrogate is shared by all of them. */
  private static String outsideTheBmp(final String s) {
    final StringBuilder result = new StringBuilder();
    s.codePoints().forEach(c -> result.appendCodePoint(0x1F000 + c));
    return result.toString();
  }

  private static Stream<Arguments> distanceFunctions() {
    return Stream.of( //
        Arguments.of("text.levenshteinDistance"), //
        Arguments.of("text.levenshteinSimilarity"), //
        Arguments.of("text.jaroWinklerDistance"), //
        Arguments.of("text.sorensenDiceSimilarity"), //
        Arguments.of("text.hammingDistance"));
  }

  private static Stream<Arguments> distanceFunctionsOfAnyLengthPair() {
    return distanceFunctions().filter(a -> !"text.hammingDistance".equals(a.get()[0]));
  }

  private static final String[][] SAME_LENGTH_PAIRS = { //
      { "abc", "abd" }, { "abcd", "abdc" }, { "martha", "marhta" }, { "night", "nacht" }, { "hello", "hallo" }, { "abab", "baba" },
      { "a", "b" }, { "a", "a" }, { "", "" }, { "dwayne", "duanex" } };

  private static final String[][] ANY_LENGTH_PAIRS = { //
      { "kitten", "sitting" }, { "abc", "" }, { "", "abc" }, { "a", "abc" }, { "dixon", "dicksonx" }, { "abcdef", "fedcba" },
      { "banana", "bandana" }, { "ab", "ba" }, { "night", "nacht" } };

  // ---- the examples of the issue

  @Test
  void hammingDistanceCountsACharacterOnce() {
    // one emoji replaced by another is one differing character, whichever half of the pair differs
    assertThat(both("text.hammingDistance($a, $b)", "a", "a" + P + "b", "b", "a" + Q + "b")).isEqualTo(1L);
    assertThat(both("text.hammingDistance($a, $b)", "a", "a" + P + "b", "b", "a" + R + "b")).isEqualTo(1L);
    assertThat(both("text.hammingDistance($a, $b)", "a", P + P, "b", R + R)).isEqualTo(2L);
    assertThat(both("text.hammingDistance($a, $b)", "a", P + Q, "b", P + Q)).isEqualTo(0L);
  }

  @Test
  void hammingDistanceComparesTheNumberOfCharactersNotUnits() {
    // 'a' + P is 3 units and 2 characters: it is not as long as "abc"
    assertThatThrownBy(() -> both("text.hammingDistance($a, $b)", "a", "a" + P, "b", "abc")).hasMessageContaining("equal length");
    // and it is as long as "ab", which has fewer units
    assertThat(both("text.hammingDistance($a, $b)", "a", "a" + P, "b", "ab")).isEqualTo(1L);
  }

  @Test
  void levenshteinDistanceOfASupplementaryCharacterIsOneEdit() {
    assertThat(both("text.levenshteinDistance($a, $b)", "a", "a" + P + "b", "b", "a" + Q + "b")).isEqualTo(1L);
    assertThat(both("text.levenshteinDistance($a, $b)", "a", "a" + P + "b", "b", "a" + R + "b")).isEqualTo(1L);
    assertThat(both("text.levenshteinDistance($a, $b)", "a", "a" + P + "b", "b", "ab")).isEqualTo(1L);
    assertThat(both("text.levenshteinDistance($a, $b)", "a", P, "b", "")).isEqualTo(1L);
  }

  @Test
  void levenshteinSimilarityDividesByTheNumberOfCharacters() {
    // one edit over 3 characters, not over 4 units
    assertThat((Double) both("text.levenshteinSimilarity($a, $b)", "a", "a" + P + "b", "b", "a" + R + "b")).isCloseTo(2.0 / 3.0,
        offset(1e-9));
  }

  @Test
  void sorensenDiceBigramsNeverCutAPair() {
    // bigrams of a+P+b are (a,P) and (P,b); a unit-wise bigram cut inside P and made (a,hi), (hi,lo), (lo,b)
    assertThat(both("text.sorensenDiceSimilarity($a, $b)", "a", "a" + P + "b", "b", "a" + P + "b")).isEqualTo(1.0);
    assertThat(both("text.sorensenDiceSimilarity($a, $b)", "a", "a" + P + "b", "b", "a" + Q + "b")).isEqualTo(0.0);
    // a single character outside the BMP is a string too short for a bigram, so the answer is the equality of the two
    assertThat(both("text.sorensenDiceSimilarity($a, $b)", "a", P, "b", P)).isEqualTo(1.0);
    assertThat(both("text.sorensenDiceSimilarity($a, $b)", "a", P, "b", Q)).isEqualTo(0.0);
  }

  @Test
  void sorensenDiceLowercasesSupplementaryLetters() {
    final String capital = cp(0x10400); // DESERET CAPITAL LETTER LONG I
    final String small = cp(0x10428);
    assertThat(both("text.sorensenDiceSimilarity($a, $b)", "a", "x" + capital + "y", "b", "x" + small + "y")).isEqualTo(1.0);
  }

  @Test
  void jaroWinklerMatchesCharactersAndSharesAPrefixOfCharacters() {
    final double bmp = (Double) both("text.jaroWinklerDistance($a, $b)", "a", "martha", "b", "marhta");
    // the 4-character prefix bonus counts characters: "mart" is 4 characters, 8 units
    assertThat((Double) both("text.jaroWinklerDistance($a, $b)", "a", outsideTheBmp("martha"), "b", outsideTheBmp("marhta"))).isEqualTo(bmp);
    // two strings with the same first character only in the high half of its pair share no prefix
    assertThat((Double) both("text.jaroWinklerDistance($a, $b)", "a", P + "xyz", "b", Q + "xyz")).isEqualTo(
        (Double) both("text.jaroWinklerDistance($a, $b)", "a", "1xyz", "b", "2xyz"));
  }

  @Test
  void levenshteinMaximumLengthCountsCharacters() {
    // MAX_STRING_LENGTH characters are 2 * MAX_STRING_LENGTH units: the string is within the limit
    final String atTheLimit = P.repeat(TextLevenshteinDistance.MAX_STRING_LENGTH);
    assertThat(both("text.levenshteinDistance($a, $b)", "a", atTheLimit, "b", P)).isEqualTo((long) TextLevenshteinDistance.MAX_STRING_LENGTH - 1);
    // one more character is refused
    final String overTheLimit = P.repeat(TextLevenshteinDistance.MAX_STRING_LENGTH + 1);
    assertThatThrownBy(() -> both("text.levenshteinDistance($a, $b)", "a", overTheLimit, "b", P)).hasMessageContaining("maximum");
  }

  @Test
  void aLoneSurrogateIsACharacterOfItsOwn() {
    final String lone = "\uD83D";
    // the lone high surrogate is not half of P
    assertThat(both("text.levenshteinDistance($a, $b)", "a", lone, "b", P)).isEqualTo(1L);
    assertThat(both("text.hammingDistance($a, $b)", "a", lone, "b", P)).isEqualTo(1L);
    assertThat(both("text.hammingDistance($a, $b)", "a", lone, "b", lone)).isEqualTo(0L);
  }

  @Test
  void nullAndEqualStringsKeepTheirAnswers() {
    assertThat(both("text.levenshteinDistance($a, $b)", "a", null, "b", "x")).isNull();
    assertThat(both("text.levenshteinDistance($a, $b)", "a", P, "b", P)).isEqualTo(0L);
    assertThat(both("text.jaroWinklerDistance($a, $b)", "a", P, "b", P)).isEqualTo(1.0);
    assertThat(both("text.jaroWinklerDistance($a, $b)", "a", P, "b", "")).isEqualTo(0.0);
    assertThat(both("text.levenshteinSimilarity($a, $b)", "a", "", "b", "")).isEqualTo(1.0);
  }

  // ---- the property of the issue: a pair of BMP strings and the same pair outside the BMP give the same number

  @ParameterizedTest(name = "{0} gives one number for a BMP pair and the same pair outside the BMP")
  @MethodSource("distanceFunctionsOfAnyLengthPair")
  void anyLengthPairsAreIndependentOfTheBmp(final String function) {
    for (final String[] pair : ANY_LENGTH_PAIRS)
      assertSamePairSameNumber(function, pair[0], pair[1]);
  }

  @ParameterizedTest(name = "{0} gives one number for an equal-length BMP pair and the same pair outside the BMP")
  @MethodSource("distanceFunctions")
  void sameLengthPairsAreIndependentOfTheBmp(final String function) {
    for (final String[] pair : SAME_LENGTH_PAIRS)
      assertSamePairSameNumber(function, pair[0], pair[1]);
  }

  private void assertSamePairSameNumber(final String function, final String a, final String b) {
    final String expression = function + "($a, $b)";
    final Object bmp = both(expression, "a", a, "b", b);
    assertThat(both(expression, "a", outsideTheBmp(a), "b", outsideTheBmp(b))).as("%s('%s', '%s') outside the BMP", function, a, b)
        .isEqualTo(bmp);
  }

  // ---- helpers

  /** The expression in SQL and in openCypher, asserted to agree, and the answer. */
  private Object both(final String expression, final Object... keyValues) {
    final Map<String, Object> params = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2)
      params.put((String) keyValues[i], keyValues[i + 1]);
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
}
