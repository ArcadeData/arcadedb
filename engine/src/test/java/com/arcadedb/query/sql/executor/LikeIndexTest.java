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
 * SQL {@code LIKE}: the pattern matcher itself, and the index range the planner derives from a pattern's literal prefix. The
 * answer must be the same with and without an index, whatever the pattern.
 */
class LikeIndexTest extends TestHelper {

  // ---- #8666: LIKE 'prefix%' served by an index range ----
  /*
   * LIKE 'prefix%' on an indexed string property scanned the whole type while the index answers it as the range from the prefix
   * to the prefix with its last character bumped. The planner now adds that range next to the LIKE, which stays as a filter, so
   * the answer does not depend on the index's collation.
   */

  private static final int      VERTICES = 3_000;
  private static final String[] EXTRAS   = { "ab\\c%", "ab\\cd", "ab\uD7FF", "ab\uD7FFz", "\uFFFF", "\uFFFFq", "\u1F600", "\u1F600a",
      "\u1F601", "abXd", "ab?d", "Abc" };

  private void likePrefixSetup() {
    database.command("sql", "CREATE VERTEX TYPE D");
    database.command("sql", "CREATE PROPERTY D.s STRING");
    database.command("sql", "CREATE PROPERTY D.n INTEGER");
    database.command("sql", "CREATE PROPERTY D.u STRING");
    database.command("sql", "CREATE INDEX ON D (s) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.newVertex("D").set("s", String.format("%04x", i)).set("n", i % 10).set("u", "v" + i).save();
      for (final String extra : EXTRAS)
        database.newVertex("D").set("s", extra).set("n", 1).set("u", extra).save();
      database.newVertex("D").set("n", 2).save();
    });
  }

  /** What the LIKE means, worked out over the data with no index and no SQL engine. */
  private long expected(final String prefix, final String mustAlsoEndWith) {
    long count = 0;
    for (int i = 0; i < VERTICES; i++) {
      final String key = String.format("%04x", i);
      if (key.startsWith(prefix) && key.endsWith(mustAlsoEndWith))
        count++;
    }
    for (final String extra : EXTRAS)
      if (extra.startsWith(prefix) && extra.endsWith(mustAlsoEndWith))
        count++;
    return count;
  }

  private long countD(final String where, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM D WHERE " + where, params)) {
      return rs.next().<Long>getProperty("n");
    }
  }

  private String planD(final String where, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT count(*) AS n FROM D WHERE " + where, params)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  /** Regression test for issue #8666. */
  @Test
  void aLiteralPrefixIsServedByTheIndex() {
    likePrefixSetup();
    assertThat(countD("s LIKE '0a%'", Map.of())).isEqualTo(expected("0a", ""));
    assertThat(countD("s LIKE 'ab%'", Map.of())).isEqualTo(expected("ab", ""));
    assertThat(planD("s LIKE '0a%'", Map.of())).contains("FETCH FROM INDEX D[s]").doesNotContain("FETCH FROM TYPE");
  }

  /** Regression test for issue #8666. */
  @Test
  void aParameterPrefixIsServedByTheIndexAndTheStatementIsReplanned() {
    likePrefixSetup();
    final String where = "s LIKE :p";
    for (final String prefix : new String[] { "0a", "0a1", "ff", "zzz", "ab\\", "ab", "ab\uD7FF", "\uFFFF", "\uFFFFq", "\u1F600",
        "\u1F601", "Abc", "0", "1", "0a1f" })
      assertThat(countD(where, Map.of("p", prefix + "%"))).as("prefix '%s'", prefix).isEqualTo(expectedLike(prefix + "%"));
    assertThat(planD(where, Map.of("p", "0a%"))).contains("FETCH FROM INDEX D[s]");

    final Map<String, Object> nullParam = new HashMap<>();
    nullParam.put("p", null);
    assertThat(countD(where, nullParam)).isEqualTo(0L);
    // The same text, run again with a pattern that has no prefix: the cached plan must not carry the earlier bounds
    assertThat(countD(where, Map.of("p", "%"))).isEqualTo(VERTICES + EXTRAS.length);
    assertThat(countD(where, Map.of("p", "%1"))).isEqualTo(expected("", "1"));
  }

  /** Regression test for issue #8666. */
  @Test
  void aCachedPlanIsNotReusedForAnotherPattern() {
    likePrefixSetup();
    // The rows are listed, not counted: a plain fetch is what the plan cache holds
    final String query = "SELECT s FROM D WHERE s LIKE :p";
    for (final String pattern : new String[] { "0a%", "1b%", "ff%", "%1", "0a%", "\uFFFF%", "\uD83D\uDE00%", "ab\\%%" }) {
      long rows = 0;
      try (final ResultSet rs = database.query("sql", query, Map.of("p", pattern))) {
        while (rs.hasNext()) {
          assertThat(QueryHelper.like(rs.next().<String>getProperty("s"), pattern, 0)).isTrue();
          rows++;
        }
      }
      assertThat(rows).as("pattern '%s'", pattern).isEqualTo(expectedLike(pattern));
    }
  }

  /** Regression test for issue #8666. */
  @Test
  void escapesAndWildcardsEndThePrefix() {
    likePrefixSetup();
    // \% is a literal percent, \? a literal question mark, ? any one character, and \ before anything else a literal backslash
    for (final String pattern : new String[] { "ab\\%%", "ab\\?%", "ab?d%", "ab\\\\c%", "ab\\c%", "0a%1", "0a_1", "0a?1", "0?%", "a%",
        "ab\\", "\\%", "%" }) {
      final long expected = expectedLike(pattern);
      assertThat(countD("s LIKE :p", Map.of("p", pattern))).as("pattern '%s'", pattern).isEqualTo(expected);
    }
    assertThat(planD("s LIKE :p", Map.of("p", "0a%1"))).contains("FETCH FROM INDEX D[s]");
    assertThat(planD("s LIKE :p", Map.of("p", "0a?1"))).contains("FETCH FROM INDEX D[s]");
    assertThat(planD("s LIKE :p", Map.of("p", "?0a%"))).doesNotContain("FETCH FROM INDEX");
    assertThat(countD("s LIKE '0a%1'", Map.of())).isEqualTo(expected("0a", "1"));
    assertThat(planD("s LIKE '0a%1'", Map.of())).contains("FETCH FROM INDEX D[s]");
  }

  /** Regression test for issue #8666. */
  @Test
  void theLikeStaysAsAFilter() {
    likePrefixSetup();
    assertThat(countD("s LIKE '0a%' AND n = 3", Map.of())).isEqualTo(countWith("0a", 3));
    assertThat(countD("s LIKE '0a%' AND u LIKE '%7'", Map.of())).isEqualTo(countU("0a", "7"));
    assertThat(countD("s LIKE '0a%1'", Map.of())).isEqualTo(expected("0a", "1"));
    assertThat(countD("s LIKE '0a%' OR s LIKE '1b%'", Map.of())).isEqualTo(expected("0a", "") + expected("1b", ""));
  }

  /** Regression test for issue #8666. */
  @Test
  void whereTheIndexCannotHelpTheScanIsKept() {
    likePrefixSetup();
    assertThat(countD("s LIKE '%a'", Map.of())).isEqualTo(expected("", "a"));
    assertThat(planD("s LIKE '%a'", Map.of())).doesNotContain("FETCH FROM INDEX");
    // the record without s is left out: NULL LIKE '0a%' is unknown and so is its negation (#10186)
    assertThat(countD("NOT (s LIKE '0a%')", Map.of())).isEqualTo(VERTICES + EXTRAS.length - expected("0a", ""));
    assertThat(planD("NOT (s LIKE '0a%')", Map.of())).doesNotContain("FETCH FROM INDEX D[s]");
    assertThat(countD("u LIKE 'v12%'", Map.of())).isEqualTo(countU("v12"));
    assertThat(planD("u LIKE 'v12%'", Map.of())).doesNotContain("FETCH FROM INDEX");
    assertThat(countD("n LIKE '1%'", Map.of())).isEqualTo(countD("n = 1", Map.of()));
  }

  /** Regression test for issue #8666. */
  @Test
  void aCaseInsensitiveIndexIsNotBounded() {
    likePrefixSetup();
    // The same data with and without the case-insensitive index: the answers must agree
    for (final boolean indexed : new boolean[] { false, true }) {
      final String type = indexed ? "CI1" : "CI0";
      database.command("sql", "CREATE VERTEX TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".s STRING");
      if (indexed)
        database.command("sql", "CREATE INDEX ON " + type + " (s COLLATE ci) NOTUNIQUE");
      database.transaction(() -> {
        for (final String v : new String[] { "Abcd", "abce", "ABCF", "abd", "Ab" })
          database.newVertex(type).set("s", v).save();
      });
    }
    for (final String pattern : new String[] { "Abc%", "abc%", "ABC%", "Ab%" })
      try (final ResultSet indexed = database.query("sql", "SELECT count(*) AS n FROM CI1 WHERE s LIKE :p", Map.of("p", pattern));
          final ResultSet scanned = database.query("sql", "SELECT count(*) AS n FROM CI0 WHERE s LIKE :p", Map.of("p", pattern))) {
        assertThat(indexed.next().<Long>getProperty("n")).as(pattern).isEqualTo(scanned.next().<Long>getProperty("n"));
      }
  }

  /** Regression test for issue #8666. */
  @Test
  void theSecondFieldOfACompositeIndex() {
    likePrefixSetup();
    database.command("sql", "CREATE VERTEX TYPE K");
    database.command("sql", "CREATE PROPERTY K.a INTEGER");
    database.command("sql", "CREATE PROPERTY K.b STRING");
    database.command("sql", "CREATE INDEX ON K (a, b) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 500; i++)
        database.newVertex("K").set("a", i % 5).set("b", String.format("%03x", i)).save();
    });
    long expected = 0;
    for (int i = 0; i < 500; i++)
      if (i % 5 == 2 && String.format("%03x", i).startsWith("0a"))
        expected++;
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM K WHERE a = 2 AND b LIKE '0a%'")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(expected);
    }
    try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT count(*) AS n FROM K WHERE a = 2 AND b LIKE '0a%'")) {
      assertThat((String) rs.next().getProperty("executionPlanAsString")).contains("FETCH FROM INDEX");
    }
  }

  private long expectedLike(final String pattern) {
    long count = 0;
    for (int i = 0; i < VERTICES; i++)
      if (QueryHelper.like(String.format("%04x", i), pattern, 0))
        count++;
    for (final String extra : EXTRAS)
      if (QueryHelper.like(extra, pattern, 0))
        count++;
    return count;
  }

  private long countWith(final String prefix, final int n) {
    long count = 0;
    for (int i = 0; i < VERTICES; i++)
      if (String.format("%04x", i).startsWith(prefix) && i % 10 == n)
        count++;
    return count;
  }

  private long countU(final String prefix, final String suffix) {
    long count = 0;
    for (int i = 0; i < VERTICES; i++)
      if (String.format("%04x", i).startsWith(prefix) && ("v" + i).endsWith(suffix))
        count++;
    return count;
  }

  private long countU(final String prefix) {
    long count = 0;
    for (int i = 0; i < VERTICES; i++)
      if (("v" + i).startsWith(prefix))
        count++;
    for (final String extra : EXTRAS)
      if (extra.startsWith(prefix))
        count++;
    return count;
  }

  // ---- #10005: '' LIKE '%' must be true ----
  /*
   * '' LIKE '%' answered false, so a LIKE '%' filter silently lost every empty string, and the counts and groups computed over it
   * came back short.
   */

  private static final String[] VALUES = { "", "a", "0000-01-01", "2020-01-01", "x".repeat(6000), "é日本語", "a\nb", " " };

  /** Regression test for issue #10005. */
  @Test
  void percentMatchesTheEmptyString() {
    assertThat(QueryHelper.like("", "%", 0)).isTrue();
    assertThat(QueryHelper.like("", "%%", 0)).isTrue();
    assertThat(QueryHelper.like("", "%%%", 0)).isTrue();
    assertThat(QueryHelper.like("", "", 0)).isTrue();
  }

  /** Regression test for issue #10005. */
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

  /** Regression test for issue #10005. */
  @Test
  void nullOperandsNeverMatch() {
    assertThat(QueryHelper.like(null, "%", 0)).isFalse();
    assertThat(QueryHelper.like("", null, 0)).isFalse();
    assertThat(QueryHelper.like(null, null, 0)).isFalse();
  }

  /** Regression test for issue #10005. */
  @Test
  void sqlLikePercentMatchesEveryStoredStringScanned() {
    populate(false);
    assertEveryValueMatched();
  }

  /** Regression test for issue #10005. */
  @Test
  void sqlLikePercentMatchesEveryStoredStringIndexed() {
    populate(true);
    assertEveryValueMatched();
  }

  /** Regression test for issue #10005. */
  @Test
  void ilikeAndNotLikeAgreeOnTheEmptyString() {
    populate(false);
    assertThat(count("SELECT count(*) AS n FROM T WHERE s ILIKE '%'")).isEqualTo(VALUES.length);
    assertThat(count("SELECT count(*) AS n FROM T WHERE NOT (s LIKE '%')")).isZero();
    assertThat(count("SELECT count(*) AS n FROM T WHERE s LIKE ''")).isEqualTo(1);
    assertThat(count("SELECT count(*) AS n FROM T WHERE s LIKE '?'")).isEqualTo(2); // "a" and " "
  }

  /** Regression test for issue #10005. */
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

  /** Regression test for issue #10005. */
  @Test
  void multiValueLikeMatchesAnEmptyItem() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Tags");
      database.command("sql", "INSERT INTO Tags SET tags = ['']");
    });
    assertThat(count("SELECT count(*) AS n FROM Tags WHERE tags LIKE '%'")).isEqualTo(1);
  }

  /**
   * Regression test for issue #10005.
   * <p>
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

  /** Regression test for issue #10005. */
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
