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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13439: a clause keyword left dangling after a target ({@code DELETE FROM person WHERE;}) was read as the bare alias of
 * that target, so the statement ran with no condition and no error, and the DELETE removed every record. Such a keyword is no
 * longer a bare alias: it needs {@code AS} or backticks.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class DanglingClauseKeywordAliasIssue13439Test extends TestHelper {
  private static final String DOC    = "Doc13439";
  private static final String VERTEX = "V13439";
  private static final String EDGE   = "E13439";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE " + DOC);
    database.command("sql", "CREATE VERTEX TYPE " + VERTEX);
    database.command("sql", "CREATE EDGE TYPE " + EDGE);
    database.transaction(() -> {
      for (final int age : new int[] { 10, 20, 30 }) {
        database.command("sql", "INSERT INTO " + DOC + " SET age = " + age + ", tags = ['a', 'b']");
        database.command("sql", "CREATE VERTEX " + VERTEX + " SET age = " + age);
      }
      database.command("sql",
          "CREATE EDGE " + EDGE + " FROM (SELECT FROM " + VERTEX + " WHERE age = 10) TO (SELECT FROM " + VERTEX + " WHERE age > 10)");
    });
  }

  private static Statement parse(final String sql) {
    return new SQLAntlrParser(null).parse(sql);
  }

  private long count(final String type) {
    // count(@rid) scans: the ground truth, not the cached counter of count(*)
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM " + type)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private void assertNothingChanged() {
    assertThat(count(DOC)).isEqualTo(3);
    assertThat(count(VERTEX)).isEqualTo(3);
    assertThat(count(EDGE)).isEqualTo(2);
    try (final ResultSet rs = database.query("sql", "SELECT sum(age) AS s FROM " + DOC)) {
      assertThat(rs.next().<Number>getProperty("s").longValue()).isEqualTo(60);
    }
  }

  private void assertRefused(final String sql, final String keyword) {
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", sql).close()))
        .as(sql)
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("'" + keyword + "'")
        .hasMessageContaining("alias");
    assertNothingChanged();
  }

  @ParameterizedTest
  @ValueSource(strings = { "WHERE", "where", "Where", "LIMIT", "SKIP", "OFFSET", "WHILE", "RETURNING", "AS", "ORDER", "order", "GROUP" })
  void danglingKeywordAfterTheTargetOfADeleteIsRefused(final String keyword) {
    assertRefused("DELETE FROM " + DOC + " " + keyword, keyword);
    assertRefused("DELETE FROM " + DOC + " " + keyword + ";", keyword);
    assertRefused("DELETE FROM " + DOC + " " + keyword + " UNSAFE", keyword);
    assertRefused("DELETE FROM (SELECT FROM " + DOC + ") " + keyword, keyword);
    assertRefused("DELETE VERTEX FROM " + VERTEX + " " + keyword, keyword);
  }

  @Test
  void keywordAliasFollowedByAnotherClauseIsRefused() {
    // the alias reading also survived whenever what followed the keyword was a clause of its own
    assertRefused("DELETE FROM " + DOC + " WHERE LIMIT 1", "WHERE");
    assertRefused("DELETE FROM " + DOC + " WHERE RETURNING age", "WHERE");
    assertRefused("DELETE FROM " + DOC + " WHERE BATCH 10", "WHERE");
    assertRefused("DELETE VERTEX FROM " + VERTEX + " WHERE LIMIT 1", "WHERE");
  }

  @ParameterizedTest
  @ValueSource(strings = { "WHERE", "LIMIT", "SKIP", "OFFSET", "WHILE", "RETURNING", "AS", "ORDER", "GROUP" })
  void danglingKeywordAfterTheTargetOfAReadIsRefused(final String keyword) {
    assertRefused("SELECT FROM " + DOC + " " + keyword, keyword);
    assertRefused("SELECT FROM (SELECT FROM " + DOC + ") " + keyword, keyword);
    assertRefused("TRAVERSE out() FROM " + VERTEX + " " + keyword, keyword);
    assertRefused("SELECT FROM " + DOC + " UNWIND tags " + keyword, keyword);
    assertThatThrownBy(() -> parse("SELECT FROM cypherRID(:id) " + keyword)).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("alias");
  }

  @ParameterizedTest
  @ValueSource(strings = { "WHERE", "LIMIT", "SKIP", "OFFSET", "WHILE", "RETURNING", "AS", "ORDER", "GROUP" })
  void danglingKeywordAfterAReturnProjectionOfAnUpdateIsRefused(final String keyword) {
    // UPDATE ... RETURN AFTER age WHERE read WHERE as the alias of the projection and updated every record
    assertRefused("UPDATE " + DOC + " SET age = 1 RETURN AFTER age " + keyword, keyword);
  }

  @Test
  void updateWithADanglingWhereStaysASyntaxError() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE " + DOC + " SET age = 1 WHERE").close()))
        .isInstanceOf(CommandSQLParsingException.class);
    assertNothingChanged();
  }

  @ParameterizedTest
  @ValueSource(strings = { "where", "limit", "skip", "offset", "while", "returning", "as", "order", "group" })
  void keywordIsStillAnAliasWithAsOrBackticks(final String keyword) {
    for (final String alias : new String[] { "AS " + keyword, "`" + keyword + "`", "AS `" + keyword + "`" }) {
      final SelectStatement select = (SelectStatement) parse("SELECT FROM " + DOC + " " + alias + " WHERE age = 20");
      assertThat(select.getTarget().getItem().getAlias().getStringValue()).as(alias).isEqualTo(keyword);
      assertThat(select.getWhereClause()).as(alias).isNotNull();

      try (final ResultSet rs = database.query("sql", "SELECT FROM " + DOC + " " + alias + " WHERE " + "`" + keyword + "`.age = 20")) {
        assertThat(rs.stream().count()).as(alias).isEqualTo(1);
      }
      try (final ResultSet rs = database.query("sql", "SELECT FROM (SELECT FROM " + DOC + ") " + alias + " WHERE age = 20")) {
        assertThat(rs.stream().count()).as(alias).isEqualTo(1);
      }
      try (final ResultSet rs = database.query("sql", "SELECT age " + alias + " FROM " + DOC + " WHERE age = 20")) {
        assertThat(rs.next().<Integer>getProperty(keyword)).as(alias).isEqualTo(20);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "where", "limit", "skip", "offset", "while", "returning", "as" })
  void keywordIsStillAPropertyName(final String keyword) {
    database.transaction(() -> database.command("sql", "UPDATE " + DOC + " SET " + keyword + " = age"));
    try (final ResultSet rs = database.query("sql",
        "SELECT " + keyword + " FROM " + DOC + " WHERE " + keyword + " = 20 ORDER BY " + keyword)) {
      assertThat(rs.next().<Integer>getProperty(keyword)).isEqualTo(20);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void completeClausesAndOrdinaryAliasesAreUntouched() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + DOC + " d WHERE d.age = 20")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
    try (final ResultSet rs = database.query("sql", "SELECT age a FROM " + DOC + " ORDER BY age SKIP 1 LIMIT 1")) {
      assertThat(rs.next().<Integer>getProperty("a")).isEqualTo(20);
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + DOC + " GROUP BY age")) {
      assertThat(rs.stream().count()).isEqualTo(3);
    }
    try (final ResultSet rs = database.query("sql", "TRAVERSE out() FROM " + VERTEX + " WHILE $depth < 2")) {
      assertThat(rs.stream().count()).isEqualTo(3);
    }
    database.transaction(() -> database.command("sql", "DELETE FROM " + DOC + " WHERE age = 20"));
    assertThat(count(DOC)).isEqualTo(2);
    database.transaction(() -> database.command("sql", "DELETE FROM " + DOC + " d WHERE d.age = 10 RETURNING age"));
    assertThat(count(DOC)).isEqualTo(1);
    database.transaction(() -> database.command("sql", "DELETE FROM " + DOC + " LIMIT 1"));
    assertThat(count(DOC)).isZero();
  }
}
