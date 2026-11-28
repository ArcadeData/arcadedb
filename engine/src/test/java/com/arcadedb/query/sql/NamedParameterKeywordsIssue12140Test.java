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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.grammar.SQLLexer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12140: a named parameter called like a keyword was a syntax error, {@code WHERE k < :before} failed beside a working
 * {@code WHERE k > :after}, and so did 64 other keywords ({@code :select}, {@code :match}, {@code :update}, {@code :let},
 * {@code :not} ...). The position right after the colon can only hold the parameter's name, so every word is one.
 * <p>
 * The census walks the lexer's vocabulary, not a list: a keyword added to the lexer is covered by this test without touching it.
 */
class NamedParameterKeywordsIssue12140Test extends TestHelper {

  @BeforeEach
  void createType() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.transaction(() -> {
      database.newDocument("T").set("k", "a").save();
      database.newDocument("T").set("k", "c").save();
    });
  }

  @Test
  void everyKeywordOfTheLexerNamesAParameter() {
    final List<String> refused = new ArrayList<>();
    int keywords = 0;
    for (int tokenType = 1; tokenType <= SQLLexer.VOCABULARY.getMaxTokenType(); tokenType++) {
      final String symbolic = SQLLexer.VOCABULARY.getSymbolicName(tokenType);
      if (symbolic == null)
        continue;
      final String literal = SQLLexer.VOCABULARY.getLiteralName(tokenType);
      final String word = (literal != null ? literal.replace("'", "") : symbolic.replace("_KW", "")).toLowerCase(Locale.ROOT);
      if (!word.matches("[a-z_]+"))
        continue;
      ++keywords;

      try (final ResultSet rs = database.query("sql", "SELECT k FROM T WHERE k < :" + word, Map.of(word, "b"))) {
        final List<Object> values = new ArrayList<>();
        while (rs.hasNext())
          values.add(rs.next().getProperty("k"));
        if (!values.equals(List.of("a")))
          refused.add(symbolic + " answered " + values);
      } catch (final Exception e) {
        refused.add(symbolic + ": " + e.getMessage());
      }
    }

    assertThat(keywords).as("the census reads the keywords from the lexer").isGreaterThan(200);
    assertThat(refused).as("keywords refused as a parameter name").isEmpty();
  }

  @Test
  void aKeysetPageBackwardsTakesBeforeAsItsForwardTwinTakesAfter() {
    try (final ResultSet rs = database.query("sql", "SELECT k FROM T WHERE k < :before ORDER BY k DESC LIMIT :limit",
        Map.of("before", "z", "limit", 1))) {
      assertThat(rs.next().<String>getProperty("k")).isEqualTo("c");
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT k FROM T WHERE k > :after ORDER BY k LIMIT :limit",
        Map.of("after", "", "limit", 1))) {
      assertThat(rs.next().<String>getProperty("k")).isEqualTo("a");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void aKeywordNameIsBoundAsWrittenAndFromKeepsItsLowerCase() {
    try (final ResultSet rs = database.query("sql", "SELECT :SELECT AS upper, :select AS lower, :FROM AS f",
        Map.of("SELECT", 1, "select", 2, "from", 3))) {
      final Result row = rs.next();
      assertThat(row.<Integer>getProperty("upper")).isEqualTo(1);
      assertThat(row.<Integer>getProperty("lower")).isEqualTo(2);
      assertThat(row.<Integer>getProperty("f")).isEqualTo(3);
    }
  }

  @Test
  void keywordParametersWorkInEveryParameterPosition() {
    try (final ResultSet rs = database.query("sql", "SELECT k, :not AS n FROM T WHERE k IN :in AND k <> :null SKIP :skip LIMIT :match",
        Map.of("not", true, "in", List.of("a", "c"), "null", "x", "skip", 1, "match", 5))) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("k")).isEqualTo("c");
      assertThat(row.<Boolean>getProperty("n")).isTrue();
      assertThat(rs.hasNext()).isFalse();
    }
    // a map literal whose values are keyword literals still reads them as the literals
    try (final ResultSet rs = database.query("sql", "SELECT {a: null, b: true, c: :true} AS m", Map.of("true", 7))) {
      final Map<String, Object> map = rs.next().getProperty("m");
      assertThat(map).containsEntry("a", null).containsEntry("b", true).containsEntry("c", 7);
    }
    database.transaction(() -> database.command("sql", "UPDATE T SET k = :update WHERE k = :where", Map.of("update", "b", "where", "c")));
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM T WHERE k = :let", Map.of("let", "b"))) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(1L);
    }
  }
}
