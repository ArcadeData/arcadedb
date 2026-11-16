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
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

/**
 * Regression tests of the {@code text.*} functions and the SQL string methods on characters outside the Basic Multilingual Plane,
 * and of their locale independence. Each nested class keeps the fixture of the regression it was written for.
 */
class TextFunctionsRegressionTest {
  /**
   * Regression for #10378: {@code text.charAt()}, {@code text.indexOf()}, {@code text.lpad()}, {@code text.rpad()} and the SQL
   * {@code size()} method still counted UTF-16 code units after #10248 moved their neighbours to Unicode characters, so
   * {@code text.charAt('a😀b', 1)} answered half a surrogate pair, {@code text.indexOf('a😀b', 'b')} answered 3 for a string whose
   * {@code size()} is 3, and {@code text.lpad} measured an emoji as two characters.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SizeAndPaddingCodePoints extends TestHelper {
    private static final String EMOJI = "😀";
    private static final String VALUE = "a" + EMOJI + "b";

    @BeforeEach
    void createData() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.transaction(() -> database.newDocument("T").set("s", VALUE).save());
    }

    @Test
    void charAtReturnsTheWholeCharacter() {
      assertThat(both("text.charAt('" + VALUE + "', 0)")).isEqualTo("a");
      assertThat(both("text.charAt('" + VALUE + "', 1)")).isEqualTo(EMOJI);
      assertThat(both("text.charAt('" + VALUE + "', 2)")).isEqualTo("b");
      assertThat(both("text.charAt('" + VALUE + "', 3)")).isNull();
      assertThat(both("text.charAt('" + VALUE + "', -1)")).isNull();
    }

    @Test
    void charAtAgreesWithTheCharAtMethod() {
      try (final ResultSet rs = database.query("sql", "SELECT text.charAt(s, 1) AS f, s.charAt(1) AS m FROM T")) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("f")).isEqualTo(EMOJI).isEqualTo(row.<String>getProperty("m"));
      }
    }

    @Test
    void indexOfIsAPositionInTheStringTheEngineReportsAndNeverPastItsSize() {
      assertThat(both("text.indexOf('" + VALUE + "', 'b')")).isEqualTo(2L);
      assertThat(both("text.indexOf('" + VALUE + "', '" + EMOJI + "')")).isEqualTo(1L);
      assertThat(both("text.indexOf('" + VALUE + "', 'b', 2)")).isEqualTo(2L);
      assertThat(both("text.indexOf('" + VALUE + "', 'b', 3)")).isEqualTo(-1L);
      assertThat(both("text.indexOf('" + VALUE + "', 'z')")).isEqualTo(-1L);
      assertThat(both("text.indexOf('" + VALUE + "', 'a', -4)")).isEqualTo(0L);
    }

    @Test
    void indexOfNeverMatchesInsideAPair() {
      assertThat(both("text.indexOf('" + VALUE + "', '\uDE00')")).isEqualTo(-1L);
      assertThat(both("text.indexOf('" + VALUE + "', '\uD83D')")).isEqualTo(-1L);
    }

    @Test
    void indexOfAgreesWithTheIndexOfMethod() {
      try (final ResultSet rs = database.query("sql", "SELECT text.indexOf(s, 'b') AS f, s.indexOf('b') AS m FROM T")) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("f").longValue()).isEqualTo(2L).isEqualTo(row.<Number>getProperty("m").longValue());
      }
    }

    @Test
    void padsMeasureCharactersNotUtf16Units() {
      // "a😀b" is three characters: a width of 3 needs no padding, a width of 4 needs one pad character
      assertThat(both("text.lpad('" + VALUE + "', 3, '.')")).isEqualTo(VALUE);
      assertThat(both("text.rpad('" + VALUE + "', 3, '.')")).isEqualTo(VALUE);
      assertThat(both("text.lpad('" + VALUE + "', 4, '.')")).isEqualTo("." + VALUE);
      assertThat(both("text.rpad('" + VALUE + "', 4, '.')")).isEqualTo(VALUE + ".");
      assertThat(both("text.lpad('" + VALUE + "', 6, '.')")).isEqualTo("..." + VALUE);
      assertThat(both("text.rpad('" + VALUE + "', 6, '.')")).isEqualTo(VALUE + "...");
    }

    @Test
    void aPadCharacterOutsideTheBasicPlaneIsWholeAndCounted() {
      assertThat(both("text.lpad('a', 3, '" + EMOJI + "')")).isEqualTo(EMOJI + EMOJI + "a");
      assertThat(both("text.rpad('a', 3, '" + EMOJI + "')")).isEqualTo("a" + EMOJI + EMOJI);
      // a string of emoji is as wide as it has emoji
      assertThat(both("text.lpad('" + EMOJI + EMOJI + "', 2, '.')")).isEqualTo(EMOJI + EMOJI);
      assertThat(both("text.lpad('" + EMOJI + EMOJI + "', 3, '.')")).isEqualTo("." + EMOJI + EMOJI);
    }

    @Test
    void onlyTheFirstCharacterOfAPadStringIsUsed() {
      assertThat(both("text.lpad('a', 3, 'xy')")).isEqualTo("xxa");
      assertThat(both("text.rpad('a', 3, 'xy')")).isEqualTo("axx");
      // the first character of the pad is the emoji, whole, and not its high half, nor the character after it
      assertThat(both("text.lpad('a', 3, '" + EMOJI + "x')")).isEqualTo(EMOJI + EMOJI + "a");
      assertThat(both("text.rpad('a', 3, '" + EMOJI + "x')")).isEqualTo("a" + EMOJI + EMOJI);
    }

    @Test
    void padsOfPlainTextKeepTheirBehavior() {
      assertThat(both("text.lpad('abc', 5, '*')")).isEqualTo("**abc");
      assertThat(both("text.rpad('abc', 5, '*')")).isEqualTo("abc**");
      assertThat(both("text.lpad('abc', 2, '*')")).isEqualTo("abc");
      assertThat(both("text.lpad('abc', 5, '')")).isEqualTo("  abc");
      assertThat(both("text.rpad('abc', 5, '')")).isEqualTo("abc  ");
    }

    @Test
    void sizeOfAStringCountsCharactersLikeLength() {
      try (final ResultSet rs = database.query("sql", "SELECT s.size() AS sz, s.length() AS len FROM T")) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("sz").intValue()).isEqualTo(3).isEqualTo(row.<Number>getProperty("len").intValue());
      }
    }

    @Test
    void sizeOfACollectionIsUnchanged() {
      try (final ResultSet rs = database.query("sql", "SELECT [1,2,3].size() AS a, {'a':1}.size() AS b")) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("a").intValue()).isEqualTo(3);
        assertThat(row.<Number>getProperty("b").intValue()).isEqualTo(1);
      }
    }

    /** The functions of the registry are enumerated, so a text function added later cannot keep counting units unnoticed. */
    @Test
    void noTextFunctionHandsBackHalfOfAPair() {
      final Collection<StatelessFunction> functions = CypherFunctionRegistry.getAllFunctions();
      final List<String> checked = new ArrayList<>();
      for (final StatelessFunction function : functions) {
        if (!function.getName().startsWith("text."))
          continue;
        for (final Object[] args : argumentsFor(function)) {
          final Object result;
          try {
            result = function.execute(args, null);
          } catch (final RuntimeException e) {
            // the combination is not one the function takes (a pattern that does not compile, an argument of another type)
            continue;
          }
          checked.add(function.getName());
          assertNoLoneSurrogate(function.getName(), result);
        }
      }
      assertThat(checked).contains("text.charAt", "text.lpad", "text.rpad", "text.indexOf");
    }

    private static List<Object[]> argumentsFor(final StatelessFunction function) {
      final List<Object[]> out = new ArrayList<>();
      final int min = function.getMinArgs();
      final int max = Math.min(function.getMaxArgs(), 4);
      // the string under test first, then the usual companions of a string function: an index, a width, a one character pad
      final Object[][] companions = { { 1 }, { 1, 2 }, { 4, "." }, { "b" }, { "b", 1 }, { "b", "c" }, { 2, EMOJI } };
      for (int count = Math.max(min, 1); count <= max; count++) {
        if (count == 1)
          out.add(new Object[] { VALUE });
        for (final Object[] companion : companions)
          if (companion.length + 1 == count) {
            final Object[] args = new Object[count];
            args[0] = VALUE;
            System.arraycopy(companion, 0, args, 1, companion.length);
            out.add(args);
          }
      }
      return out;
    }

    private static void assertNoLoneSurrogate(final String function, final Object result) {
      if (result instanceof String s) {
        for (int i = 0; i < s.length(); i++) {
          final char c = s.charAt(i);
          if (Character.isHighSurrogate(c) && i + 1 < s.length() && Character.isLowSurrogate(s.charAt(i + 1))) {
            i++;
            continue;
          }
          assertThat(Character.isSurrogate(c)).as("%s returned an unpaired surrogate in '%s'", function, s).isFalse();
        }
      } else if (result instanceof Collection<?> collection) {
        for (final Object element : collection)
          assertNoLoneSurrogate(function, element);
      }
    }

    /** Runs the expression from SQL and from openCypher, asserts they agree, and returns the answer. */
    private Object both(final String expression) {
      final Object sql;
      try (final ResultSet rs = database.query("sql", "SELECT " + expression + " AS v")) {
        sql = rs.next().getProperty("v");
      }
      final Object cypher;
      try (final ResultSet rs = database.query("opencypher", "RETURN " + expression + " AS v", Map.of())) {
        cypher = rs.next().getProperty("v");
      }
      assertThat(cypher).as("openCypher and SQL answers for %s", expression).isEqualTo(sql);
      return sql;
    }
  }

  /**
   * Regression for #10417: after #10378 and #10248 the string functions counted characters, but the distance and similarity functions of the
   * {@code text.*} family ({@code hammingDistance}, {@code levenshteinDistance}, {@code levenshteinSimilarity},
   * {@code jaroWinklerDistance}, {@code sorensenDiceSimilarity}) still read a string one UTF-16 unit at a time, so a character outside
   * the BMP counted twice, half of it could match half of another, and a bigram could be cut inside a surrogate pair. Each reads the
   * string by code point now: the number for a pair of strings is the number for the same pair written with BMP characters.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DistanceFunctionsCodePoints extends TestHelper {
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

  /**
   * Regression for #10484: after #10378 and #10417 the string functions counted and cut characters, but {@code ltrim()}/{@code rtrim()} with a
   * trim set, the {@code text.capitalize} family, {@code text.camelCase()} and friends, and the empty-search {@code replace()} /
   * {@code text.replace()} / {@code text.split()} still read a string one UTF-16 unit at a time: they stripped half of a surrogate
   * pair, left a cased letter outside the BMP as it was, treated a letter outside the BMP as a word break, and cut inside a pair. Each
   * reads the string by code point now, so the answer for a character outside the BMP is the answer for its BMP twin.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CaseAndTrimCodePoints extends TestHelper {
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

  /**
   * Regression for #10524: {@code text.camelCase()}, {@code text.upperCamelCase()} and {@code text.snakeCase()} deleted every character above
   * U+FFFF and put a word break where it was ({@code text.snakeCase('x𝐚y')} answered {@code 'x_y'}). The three were fixed with #10484, which
   * named them; this test is the one the report asks for, which names none: it enumerates every registered {@code text.*} function that
   * takes one string and answers a string, and holds each to the two rules a transformation of a string has to keep.
   * <ul>
   *   <li>the answer holds no unpaired surrogate, whatever the input</li>
   *   <li>a letter outside the BMP is kept wherever the same function keeps a BMP letter of the same case: the loss is the encoding, not
   *   the letter class</li>
   * </ul>
   * A function added later is held to both without being listed here.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class KeepEveryCharacter {
    // lower case letters: é, and MATHEMATICAL BOLD SMALL A
    private static final String BMP_LOWER   = "é";
    private static final String ASTRAL_LOWER = new String(Character.toChars(0x1D41A));
    // upper case letters: É, and MATHEMATICAL BOLD CAPITAL A
    private static final String BMP_UPPER   = "É";
    private static final String ASTRAL_UPPER = new String(Character.toChars(0x1D400));
    // a letter with no case outside the BMP, and one inside: CJK Extension B, and a CJK ideograph
    private static final String BMP_CASELESS   = "中";
    private static final String ASTRAL_CASELESS = new String(Character.toChars(0x20000));

    @Test
    void everyStringToStringTextFunctionKeepsALetterOutsideTheBmpWhereItKeepsItsTwin() {
      final List<StatelessFunction> functions = singleStringTextFunctions();
      // the registry is not empty and holds the three of the report, or the loop below proves nothing
      assertThat(functions).extracting(StatelessFunction::getName).contains("text.camelCase", "text.upperCamelCase", "text.snakeCase");

      final List<String> lost = new ArrayList<>();
      for (final StatelessFunction function : functions) {
        check(function, BMP_LOWER, ASTRAL_LOWER, lost);
        check(function, BMP_UPPER, ASTRAL_UPPER, lost);
        check(function, BMP_CASELESS, ASTRAL_CASELESS, lost);
      }

      assertThat(lost).as("functions that lose or split a character outside the BMP").isEmpty();
    }

    private static void check(final StatelessFunction function, final String bmp, final String astral, final List<String> lost) {
      final String twin = answer(function, "x" + bmp + "y");
      if (twin == null)
        // does not answer a string for this input, or refuses it: nothing to hold the astral answer to
        return;

      final String answer;
      try {
        answer = (String) function.execute(new Object[] { "x" + astral + "y" }, new BasicCommandContext());
      } catch (final RuntimeException e) {
        lost.add(function.getName() + " fails on '" + astral + "' where it answers '" + twin + "' for '" + bmp + "': " + e);
        return;
      }
      if (answer == null) {
        // a function that answers null for the astral letter, where its BMP twin is kept, has refused the letter
        if (twin.contains(bmp))
          lost.add(function.getName() + " keeps '" + bmp + "' but answers null for '" + astral + "'");
        return;
      }

      if (hasUnpairedSurrogate(answer))
        lost.add(function.getName() + " answers an unpaired surrogate for 'x" + astral + "y': " + escaped(answer));
      else if (twin.contains(bmp) && !answer.contains(astral))
        lost.add(function.getName() + " keeps '" + bmp + "' but drops '" + astral + "': " + escaped(twin) + " vs " + escaped(answer));
    }

    private static List<StatelessFunction> singleStringTextFunctions() {
      final List<StatelessFunction> functions = new ArrayList<>();
      for (final StatelessFunction function : CypherFunctionRegistry.getAllFunctions())
        if (function.getName().startsWith("text.") && function.getMinArgs() <= 1 && function.getMaxArgs() >= 1 && answer(function, "ab") != null
            && onlyArgumentIsTheString(function))
          functions.add(function);
      return functions;
    }

    /** Functions that need more than the string are not held to the rule here: a second argument changes what they are asked. */
    private static boolean onlyArgumentIsTheString(final StatelessFunction function) {
      return function.getMinArgs() == 1;
    }

    private static String answer(final StatelessFunction function, final String input) {
      try {
        return function.execute(new Object[] { input }, new BasicCommandContext()) instanceof final String text ? text : null;
      } catch (final RuntimeException e) {
        return null;
      }
    }

    private static boolean hasUnpairedSurrogate(final String text) {
      for (int i = 0; i < text.length(); i++) {
        final char c = text.charAt(i);
        if (Character.isHighSurrogate(c)) {
          if (i + 1 >= text.length() || !Character.isLowSurrogate(text.charAt(i + 1)))
            return true;
          i++;
        } else if (Character.isLowSurrogate(c))
          return true;
      }
      return false;
    }

    private static String escaped(final String text) {
      final StringBuilder escaped = new StringBuilder("'");
      text.codePoints().forEach(codePoint -> escaped.append(codePoint < 0x80 ? String.valueOf((char) codePoint) : "\\u{" + Integer.toHexString(codePoint) + "}"));
      return escaped.append('\'').toString();
    }
  }

  /**
   * Regression for #7788: {@code text.format()} called {@code String.format(format, args)} with no
   * {@link Locale}, so its numeric output depended on the server JVM's default locale instead of being
   * locale-independent, unlike {@code number.format()}.
   * <p>
   * Mutates the JVM default locale for the duration of the test and restores it afterward - tests in this module run
   * sequentially, but this is still process-wide state.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FormatLocale {

    private final TextFormat function = new TextFormat();

    private Locale originalDefault;

    @BeforeEach
    void saveDefaultLocale() {
      originalDefault = Locale.getDefault();
    }

    @AfterEach
    void restoreDefaultLocale() {
      Locale.setDefault(originalDefault);
    }

    @Test
    void formatIgnoresTheJvmDefaultLocale() {
      Locale.setDefault(Locale.GERMANY);
      try {
        assertThat(function.execute(new Object[] { "%,.2f", 1234567.891 }, null)).isEqualTo("1,234,567.89");
      } finally {
        Locale.setDefault(originalDefault);
      }
    }

    @Test
    void formatIsConsistentAcrossLocales() {
      Locale.setDefault(Locale.US);
      final Object usResult;
      try {
        usResult = function.execute(new Object[] { "%,.2f", 1234567.891 }, null);
      } finally {
        Locale.setDefault(originalDefault);
      }

      Locale.setDefault(Locale.GERMANY);
      final Object germanResult;
      try {
        germanResult = function.execute(new Object[] { "%,.2f", 1234567.891 }, null);
      } finally {
        Locale.setDefault(originalDefault);
      }

      assertThat(germanResult).isEqualTo(usResult);
    }
  }
}
