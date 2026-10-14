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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10005: {@code '' LIKE '%'} answered false, so a {@code LIKE '%'} filter silently lost every empty string, and the
 * counts and groups computed over it came back short.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10005LikeEmptyStringTest extends TestHelper {
  private static final String[] VALUES = { "", "a", "0000-01-01", "2020-01-01", "x".repeat(6000), "é日本語", "a\nb", " " };

  @Test
  void percentMatchesTheEmptyString() {
    assertThat(QueryHelper.like("", "%", 0)).isTrue();
    assertThat(QueryHelper.like("", "%%", 0)).isTrue();
    assertThat(QueryHelper.like("", "%%%", 0)).isTrue();
    assertThat(QueryHelper.like("", "", 0)).isTrue();
  }

  @Test
  void anythingElseStillRejectsTheEmptyString() {
    assertThat(QueryHelper.like("", "?", 0)).isFalse();
    assertThat(QueryHelper.like("", "%?", 0)).isFalse();
    assertThat(QueryHelper.like("", "a", 0)).isFalse();
    assertThat(QueryHelper.like("", "%a%", 0)).isFalse();
    assertThat(QueryHelper.like("", "\\%", 0)).isFalse();
    assertThat(QueryHelper.like("a", "", 0)).isFalse();
    assertThat(QueryHelper.like(" ", "", 0)).isFalse();
  }

  @Test
  void nullOperandsNeverMatch() {
    assertThat(QueryHelper.like(null, "%", 0)).isFalse();
    assertThat(QueryHelper.like("", null, 0)).isFalse();
    assertThat(QueryHelper.like(null, null, 0)).isFalse();
  }

  @Test
  void sqlLikePercentMatchesEveryStoredStringScanned() {
    populate(false);
    assertEveryValueMatched();
  }

  @Test
  void sqlLikePercentMatchesEveryStoredStringIndexed() {
    populate(true);
    assertEveryValueMatched();
  }

  @Test
  void ilikeAndNotLikeAgreeOnTheEmptyString() {
    populate(false);
    assertThat(count("SELECT count(*) AS n FROM T WHERE s ILIKE '%'")).isEqualTo(VALUES.length);
    assertThat(count("SELECT count(*) AS n FROM T WHERE NOT (s LIKE '%')")).isZero();
    assertThat(count("SELECT count(*) AS n FROM T WHERE s LIKE ''")).isEqualTo(1);
    assertThat(count("SELECT count(*) AS n FROM T WHERE s LIKE '?'")).isEqualTo(2); // "a" and " "
  }

  @Test
  void groupByOverALikePercentFilterKeepsTheEmptyGroup() {
    populate(false);
    final Map<Integer, Long> groups = new HashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT s.length() AS len, count(*) AS n FROM T WHERE s LIKE '%' GROUP BY len")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        groups.put(((Number) row.getProperty("len")).intValue(), ((Number) row.getProperty("n")).longValue());
      }
    }
    assertThat(groups).containsEntry(0, 1L);
    assertThat(groups.values().stream().mapToLong(Long::longValue).sum()).isEqualTo(VALUES.length);
  }

  @Test
  void multiValueLikeMatchesAnEmptyItem() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Tags");
      database.command("sql", "INSERT INTO Tags SET tags = ['']");
    });
    assertThat(count("SELECT count(*) AS n FROM Tags WHERE tags LIKE '%'")).isEqualTo(1);
  }

  /**
   * The matcher replaced the translation of the pattern to a regular expression; every pattern built from the characters
   * LIKE gives a meaning to, plus the ones a regular expression does, must answer what that translation answered.
   */
  @Test
  void agreesWithTheRegularExpressionTranslationItReplaced() {
    final String[] patternAlphabet = { "%", "?", "\\", "a", "b", ".", "*", "[", "]", "(", ")", "|", "+", "$", "^", "{", "}", "\n", "😀", "é" };
    final String[] valueAlphabet = { "a", "b", "%", "?", "\\", ".", "*", "[", "\n", "😀", "é" };
    final Random random = new Random(10005);
    for (int i = 0; i < 200_000; i++) {
      final StringBuilder pattern = new StringBuilder();
      for (int p = random.nextInt(7); p > 0; p--)
        pattern.append(patternAlphabet[random.nextInt(patternAlphabet.length)]);
      final StringBuilder value = new StringBuilder();
      for (int v = random.nextInt(7); v > 0; v--)
        value.append(valueAlphabet[random.nextInt(valueAlphabet.length)]);

      final boolean expected = Pattern.compile(regexTranslation(pattern.toString())).matcher(value).matches();
      assertThat(QueryHelper.like(value.toString(), pattern.toString(), 0)).as("'%s' LIKE '%s'", value, pattern).isEqualTo(expected);
    }
  }

  @Test
  void questionMarkTakesOneCodePoint() {
    assertThat(QueryHelper.like("😀", "?", 0)).isTrue();
    assertThat(QueryHelper.like("😀", "??", 0)).isFalse();
    assertThat(QueryHelper.like("a😀b", "a?b", 0)).isTrue();
    assertThat(QueryHelper.like("x😀", "%?", 0)).isTrue();
  }

  private void populate(final boolean indexed) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.s STRING");
      if (indexed)
        database.getSchema().getType("T").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "s");
      for (final String v : VALUES)
        database.command("sql", "INSERT INTO T SET s = ?", v);
    });
  }

  private void assertEveryValueMatched() {
    assertThat(count("SELECT count(*) AS n FROM T WHERE s LIKE '%'")).isEqualTo(VALUES.length);
    assertThat(count("SELECT count(*) AS n FROM T WHERE s IS NOT NULL AND s LIKE '%'")).isEqualTo(VALUES.length);
    assertThat(count("SELECT count(DISTINCT s) AS n FROM T WHERE s LIKE '%%'")).isEqualTo(VALUES.length);
    for (final String v : VALUES)
      assertThat(count("SELECT count(*) AS n FROM T WHERE s = ? AND s LIKE '%'", v)).as("value of length %d", v.length()).isEqualTo(1);
  }

  private long count(final String sql, final Object... args) {
    try (final ResultSet rs = database.query("sql", sql, args)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  /**
   * The translation {@code QueryHelper} applied before issue #10005: {@code %} to {@code .*}, {@code ?} to {@code .},
   * {@code \%} and {@code \?} to the literal characters, everything else literal.
   */
  private static String regexTranslation(final String pattern) {
    final StringBuilder sb = new StringBuilder("(?s)");
    for (int i = 0; i < pattern.length(); ++i) {
      final char c = pattern.charAt(i);
      if (c == '\\' && i + 1 < pattern.length() && (pattern.charAt(i + 1) == '%' || pattern.charAt(i + 1) == '?')) {
        sb.append(Pattern.quote(String.valueOf(pattern.charAt(++i))));
      } else if (c == '?')
        sb.append('.');
      else if (c == '%')
        sb.append(".*");
      else if (Character.isSurrogate(c))
        sb.append(c); // UNQUOTED, AS THE TRANSLATION DID: THE TWO HALVES OF A PAIR STAY ONE CODE POINT TO THE REGEX
      else
        sb.append(Pattern.quote(String.valueOf(c)));
    }
    return sb.toString();
  }
}
