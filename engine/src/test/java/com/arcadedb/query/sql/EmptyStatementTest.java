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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.EmptyStatement;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13380: a text that holds no statement (empty, a lone {@code ;}, or only comments) is an empty statement that answers an
 * empty result, where it used to be a syntax error. PostgreSQL clients and poolers ping a connection with such a text. The lexer reads
 * {@code --} as a comment only when a space follows, so the comment texts here carry the space.
 */
class EmptyStatementTest extends TestHelper {
  private static final String[] EMPTY_TEXTS = { "", "   ", ";", " ; ", "-- ping", "-- ping\n", "/* x */", "/* x */;", "/* a */ /* b */ ;",
      "-- a\n/* b */\n-- c", "-- SELECT FROM Person" };

  @Test
  void everyTextWithoutAStatementParsesToTheEmptyStatement() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    for (final String text : EMPTY_TEXTS) {
      final Statement statement = parser.parse(text);
      assertThat(statement).as("'%s'", text).isInstanceOf(EmptyStatement.class);

      // it renders as the empty text, which parses back to it, and a copy is equal to it
      final StringBuilder rendered = new StringBuilder();
      statement.toString(null, rendered);
      assertThat(rendered.toString()).as("'%s'", text).isEmpty();
      assertThat(parser.parse(rendered.toString())).as("'%s'", text).isEqualTo(statement);
      assertThat(statement.copy()).as("'%s'", text).isEqualTo(statement).hasSameHashCodeAs(statement);
      assertThat(statement.isIdempotent()).as("'%s'", text).isTrue();
      assertThat(statement.isDDL()).as("'%s'", text).isFalse();
    }
  }

  @Test
  void aTextWithAStatementIsParsedAsBefore() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    assertThat(parser.parse("SELECT 1")).isNotInstanceOf(EmptyStatement.class);
    assertThat(parser.parse("SELECT 1;")).isNotInstanceOf(EmptyStatement.class);
    assertThat(parser.parse("-- ping\nSELECT 1")).isNotInstanceOf(EmptyStatement.class);
    assertThat(parser.parse("/* x */ SELECT 1 /* y */ ;")).isNotInstanceOf(EmptyStatement.class);
    assertThat(parser.parse("SELECT 1 -- ping")).isNotInstanceOf(EmptyStatement.class);
  }

  @Test
  void textsThatAreNotEmptyAreStillSyntaxErrors() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    // one statement and one ';' at most, as before
    for (final String text : new String[] { ";;", "; ;", "SELECT 1;;", "SELECT 1; SELECT 2", "; SELECT 1",
        // without the space the lexer does not read '--' as a comment
        "--ping", "--" })
      assertThatThrownBy(() -> parser.parse(text)).as("'%s'", text).isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void theEngineAnswersAnEmptyResultToAnyEmptyText() {
    database.transaction(() -> database.getSchema().createDocumentType("Person"));
    for (final String text : EMPTY_TEXTS) {
      try (final ResultSet resultSet = database.query("sql", text)) {
        assertThat(resultSet.hasNext()).as("query '%s'", text).isFalse();
      }
      try (final ResultSet resultSet = database.command("sql", text)) {
        assertThat(resultSet.hasNext()).as("command '%s'", text).isFalse();
      }
      try (final ResultSet resultSet = database.command("sql", text, Map.of("name", "x"))) {
        assertThat(resultSet.hasNext()).as("command with parameters '%s'", text).isFalse();
      }
    }
  }

  @Test
  void aCommentedOutStatementDoesNotRun() {
    database.transaction(() -> database.getSchema().createDocumentType("Person"));
    database.transaction(() -> {
      database.command("sql", "-- INSERT INTO Person SET name = 'a'").close();
      database.command("sql", "/* INSERT INTO Person SET name = 'b' */;").close();
    });
    try (final ResultSet resultSet = database.query("sql", "SELECT count(@rid) AS n FROM Person")) {
      assertThat(resultSet.next().<Long>getProperty("n")).isEqualTo(0L);
    }
  }

  @Test
  void anEmptyTextInsideATransactionLeavesItOpen() {
    database.transaction(() -> database.getSchema().createDocumentType("Person"));
    database.begin();
    database.command("sql", "INSERT INTO Person SET name = 'a'").close();
    database.command("sql", "-- ping").close();
    assertThat(database.isTransactionActive()).isTrue();
    database.commit();
    try (final ResultSet resultSet = database.query("sql", "SELECT count(@rid) AS n FROM Person")) {
      assertThat(resultSet.next().<Long>getProperty("n")).isEqualTo(1L);
    }
  }

  @Test
  void aScriptAlreadyAcceptedEmptyTexts() {
    // unchanged: a script takes any number of statements, none included, though not a ';' with no statement before it
    for (final String text : new String[] { "", "-- ping", "/* x */" })
      try (final ResultSet resultSet = database.command("sqlscript", text)) {
        assertThat(resultSet.hasNext()).as("script '%s'", text).isFalse();
      }
  }
}
