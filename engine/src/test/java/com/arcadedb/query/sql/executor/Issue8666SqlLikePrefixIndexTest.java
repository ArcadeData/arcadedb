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
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8666: {@code LIKE 'prefix%'} on an indexed string property scanned the whole type while the index answers it as
 * the range from the prefix to the prefix with its last character bumped. The planner now adds that range next to the
 * {@code LIKE}, which stays as a filter, so the answer does not depend on the index's collation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8666SqlLikePrefixIndexTest extends TestHelper {
  private static final int      VERTICES = 3_000;
  private static final String[] EXTRAS   = { "ab\\c%", "ab\\cd", "ab\uD7FF", "ab\uD7FFz", "\uFFFF", "\uFFFFq", "\u1F600", "\u1F600a",
      "\u1F601", "abXd", "ab?d", "Abc" };

  @Override
  protected void beginTest() {
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

  private long count(final String where, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM D WHERE " + where, params)) {
      return rs.next().<Long>getProperty("n");
    }
  }

  private String plan(final String where, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT count(*) AS n FROM D WHERE " + where, params)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  @Test
  void aLiteralPrefixIsServedByTheIndex() {
    assertThat(count("s LIKE '0a%'", Map.of())).isEqualTo(expected("0a", ""));
    assertThat(count("s LIKE 'ab%'", Map.of())).isEqualTo(expected("ab", ""));
    assertThat(plan("s LIKE '0a%'", Map.of())).contains("FETCH FROM INDEX D[s]").doesNotContain("FETCH FROM TYPE");
  }

  @Test
  void aParameterPrefixIsServedByTheIndexAndTheStatementIsReplanned() {
    final String where = "s LIKE :p";
    for (final String prefix : new String[] { "0a", "0a1", "ff", "zzz", "ab\\", "ab", "ab\uD7FF", "\uFFFF", "\uFFFFq", "\u1F600",
        "\u1F601", "Abc", "0", "1", "0a1f" })
      assertThat(count(where, Map.of("p", prefix + "%"))).as("prefix '%s'", prefix).isEqualTo(expectedLike(prefix + "%"));
    assertThat(plan(where, Map.of("p", "0a%"))).contains("FETCH FROM INDEX D[s]");

    final Map<String, Object> nullParam = new HashMap<>();
    nullParam.put("p", null);
    assertThat(count(where, nullParam)).isEqualTo(0L);
    // The same text, run again with a pattern that has no prefix: the cached plan must not carry the earlier bounds
    assertThat(count(where, Map.of("p", "%"))).isEqualTo(VERTICES + EXTRAS.length);
    assertThat(count(where, Map.of("p", "%1"))).isEqualTo(expected("", "1"));
  }

  @Test
  void aCachedPlanIsNotReusedForAnotherPattern() {
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

  @Test
  void escapesAndWildcardsEndThePrefix() {
    // \% is a literal percent, \? a literal question mark, ? any one character, and \ before anything else a literal backslash
    for (final String pattern : new String[] { "ab\\%%", "ab\\?%", "ab?d%", "ab\\\\c%", "ab\\c%", "0a%1", "0a_1", "0a?1", "0?%", "a%",
        "ab\\", "\\%", "%" }) {
      final long expected = expectedLike(pattern);
      assertThat(count("s LIKE :p", Map.of("p", pattern))).as("pattern '%s'", pattern).isEqualTo(expected);
    }
    assertThat(plan("s LIKE :p", Map.of("p", "0a%1"))).contains("FETCH FROM INDEX D[s]");
    assertThat(plan("s LIKE :p", Map.of("p", "0a?1"))).contains("FETCH FROM INDEX D[s]");
    assertThat(plan("s LIKE :p", Map.of("p", "?0a%"))).doesNotContain("FETCH FROM INDEX");
    assertThat(count("s LIKE '0a%1'", Map.of())).isEqualTo(expected("0a", "1"));
    assertThat(plan("s LIKE '0a%1'", Map.of())).contains("FETCH FROM INDEX D[s]");
  }

  @Test
  void theLikeStaysAsAFilter() {
    assertThat(count("s LIKE '0a%' AND n = 3", Map.of())).isEqualTo(countWith("0a", 3));
    assertThat(count("s LIKE '0a%' AND u LIKE '%7'", Map.of())).isEqualTo(countU("0a", "7"));
    assertThat(count("s LIKE '0a%1'", Map.of())).isEqualTo(expected("0a", "1"));
    assertThat(count("s LIKE '0a%' OR s LIKE '1b%'", Map.of())).isEqualTo(expected("0a", "") + expected("1b", ""));
  }

  @Test
  void whereTheIndexCannotHelpTheScanIsKept() {
    assertThat(count("s LIKE '%a'", Map.of())).isEqualTo(expected("", "a"));
    assertThat(plan("s LIKE '%a'", Map.of())).doesNotContain("FETCH FROM INDEX");
    assertThat(count("NOT (s LIKE '0a%')", Map.of())).isEqualTo(VERTICES + EXTRAS.length + 1 - expected("0a", ""));
    assertThat(plan("NOT (s LIKE '0a%')", Map.of())).doesNotContain("FETCH FROM INDEX D[s]");
    assertThat(count("u LIKE 'v12%'", Map.of())).isEqualTo(countU("v12"));
    assertThat(plan("u LIKE 'v12%'", Map.of())).doesNotContain("FETCH FROM INDEX");
    assertThat(count("n LIKE '1%'", Map.of())).isEqualTo(count("n = 1", Map.of()));
  }

  @Test
  void aCaseInsensitiveIndexIsNotBounded() {
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

  @Test
  void theSecondFieldOfACompositeIndex() {
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
}
