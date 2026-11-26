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
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

/**
 * The text functions that compare two strings character by character (Levenshtein distance and similarity, Jaro-Winkler distance) cost
 * a number of steps proportional to the product of the two lengths, so each of them answers a client error for a string longer than
 * {@link TextLevenshteinDistance#MAX_STRING_LENGTH} characters, and answers as before up to that length.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TextDistanceFunctionsLengthLimitTest {
  private static final int    LIMIT = TextLevenshteinDistance.MAX_STRING_LENGTH;
  /** A character outside the BMP: two UTF-16 units, one character. */
  private static final String P     = new String(Character.toChars(0x1F600));

  private static Stream<Arguments> comparingFunctions() {
    return Stream.of( //
        Arguments.of(new TextLevenshteinDistance()), //
        Arguments.of(new TextLevenshteinSimilarity()), //
        Arguments.of(new TextJaroWinklerDistance()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("comparingFunctions")
  void aFirstArgumentPastTheLimitIsRefused(final StatelessFunction function) {
    final String tooLong = "a".repeat(LIMIT + 1);
    assertThatThrownBy(() -> function.execute(new Object[] { tooLong, "test" }, null)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds maximum allowed").hasMessageContaining(String.valueOf(LIMIT));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("comparingFunctions")
  void aSecondArgumentPastTheLimitIsRefused(final StatelessFunction function) {
    final String tooLong = "a".repeat(LIMIT + 1);
    assertThatThrownBy(() -> function.execute(new Object[] { "test", tooLong }, null)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds maximum allowed").hasMessageContaining(String.valueOf(LIMIT));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("comparingFunctions")
  void twoEqualStringsPastTheLimitAreRefusedToo(final StatelessFunction function) {
    final String tooLong = "a".repeat(LIMIT + 1);
    assertThatThrownBy(() -> function.execute(new Object[] { tooLong, tooLong }, null)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds maximum allowed");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("comparingFunctions")
  void theLimitCountsCharactersNotUtf16Units(final StatelessFunction function) {
    // LIMIT characters are 2 * LIMIT units: within the limit
    assertThat(function.execute(new Object[] { P.repeat(LIMIT), P }, null)).isNotNull();
    // one more character is past it
    assertThatThrownBy(() -> function.execute(new Object[] { P.repeat(LIMIT + 1), P }, null)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds maximum allowed");
    assertThatThrownBy(() -> function.execute(new Object[] { P, P.repeat(LIMIT + 1) }, null)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds maximum allowed");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("comparingFunctions")
  void aNullArgumentStillAnswersNull(final StatelessFunction function) {
    assertThat(function.execute(new Object[] { null, "a".repeat(LIMIT + 1) }, null)).isNull();
    assertThat(function.execute(new Object[] { "a".repeat(LIMIT + 1), null }, null)).isNull();
  }

  @Test
  void jaroWinklerDistanceAtTheLimitStillAnswersTheSameValue() {
    // LIMIT - 1 of the characters match in place and no character is transposed: jaro = (m / n + m / n + 1) / 3 with m = n - 1, then
    // the Winkler bonus of the 4-character common prefix
    final double n = LIMIT;
    final double m = LIMIT - 1;
    final double jaro = (m / n + m / n + 1) / 3;
    final double expected = 1 - (jaro + 0.1 * 4 * (1.0 - jaro));

    final Object distance = new TextJaroWinklerDistance().execute(new Object[] { "a".repeat(LIMIT - 1) + "b", "a".repeat(LIMIT) }, null);
    assertThat((Double) distance).isCloseTo(expected, offset(1e-12));

    assertThat(new TextJaroWinklerDistance().execute(new Object[] { "a".repeat(LIMIT), "a".repeat(LIMIT) }, null)).isEqualTo(0.0);
  }

  @Test
  void jaroWinklerDistanceBelowTheLimitIsUnchanged() {
    assertThat((Double) new TextJaroWinklerDistance().execute(new Object[] { "martha", "marhta" }, null)).isCloseTo(1 - 0.9611111111, offset(1e-9));
    assertThat(new TextJaroWinklerDistance().execute(new Object[] { "hello", "hello" }, null)).isEqualTo(0.0);
    assertThat(new TextJaroWinklerDistance().execute(new Object[] { "abc", "xyz" }, null)).isEqualTo(1.0);
    assertThat(new TextJaroWinklerDistance().execute(new Object[] { "", "" }, null)).isEqualTo(0.0);
  }

  @Test
  void levenshteinSimilarityAtTheLimitStillAnswersTheSameValue() {
    final TextLevenshteinSimilarity similarity = new TextLevenshteinSimilarity();
    // LIMIT - 1 deletions over LIMIT characters
    assertThat((Double) similarity.execute(new Object[] { "a".repeat(LIMIT), "a" }, null)).isCloseTo(1.0 / LIMIT, offset(1e-12));
    assertThat(similarity.execute(new Object[] { "a".repeat(LIMIT), "a".repeat(LIMIT) }, null)).isEqualTo(1.0);
    assertThat(similarity.execute(new Object[] { "", "" }, null)).isEqualTo(1.0);
    assertThat((Double) similarity.execute(new Object[] { "kitten", "sitting" }, null)).isCloseTo(1.0 - 3.0 / 7.0, offset(1e-12));
  }

  @Test
  void levenshteinDistanceAtTheLimitStillAnswersTheSameValue() {
    assertThat(new TextLevenshteinDistance().execute(new Object[] { "a".repeat(LIMIT), "a" }, null)).isEqualTo((long) LIMIT - 1);
  }

  /**
   * The same refusal reaches a client of each query language, and the {@code apoc.} spelling resolves to the same function.
   */
  @Nested
  class OverTheLimitInQueries extends TestHelper {
    private void assertRefused(final String language, final String query) {
      final Map<String, Object> params = Map.of("a", "a".repeat(LIMIT + 1), "b", "test");
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query(language, query, params)) {
          while (rs.hasNext())
            rs.next();
        }
      }).hasMessageContaining("exceeds maximum allowed");
    }

    @Test
    void sqlRefusesAStringPastTheLimit() {
      assertRefused("sql", "SELECT text.jaroWinklerDistance(:a, :b) AS v");
      assertRefused("sql", "SELECT text.levenshteinSimilarity(:a, :b) AS v");
      assertRefused("sql", "SELECT text.levenshteinDistance(:a, :b) AS v");
    }

    @Test
    void openCypherRefusesAStringPastTheLimit() {
      assertRefused("opencypher", "RETURN text.jaroWinklerDistance($a, $b) AS v");
      assertRefused("opencypher", "RETURN text.levenshteinSimilarity($a, $b) AS v");
      assertRefused("opencypher", "RETURN text.levenshteinDistance($a, $b) AS v");
    }

    @Test
    void theApocSpellingIsRefusedToo() {
      assertRefused("opencypher", "RETURN apoc.text.jaroWinklerDistance($a, $b) AS v");
      assertRefused("opencypher", "RETURN apoc.text.levenshteinSimilarity($a, $b) AS v");
    }

    @Test
    void aStringAtTheLimitIsAnsweredInBothLanguages() {
      final Map<String, Object> params = Map.of("a", "a".repeat(LIMIT), "b", "a".repeat(LIMIT));
      try (final ResultSet rs = database.query("sql", "SELECT text.jaroWinklerDistance(:a, :b) AS v", params)) {
        assertThat(rs.next().<Double>getProperty("v")).isEqualTo(0.0);
      }
      try (final ResultSet rs = database.query("opencypher", "RETURN text.levenshteinSimilarity($a, $b) AS v", params)) {
        assertThat(rs.next().<Double>getProperty("v")).isEqualTo(1.0);
      }
    }
  }
}
