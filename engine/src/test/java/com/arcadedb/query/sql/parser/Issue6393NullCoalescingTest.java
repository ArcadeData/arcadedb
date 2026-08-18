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
import com.arcadedb.query.sql.antlr.SQLASTBuilder;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.grammar.SQLLexer;
import com.arcadedb.query.sql.grammar.SQLParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The SQL null-coalescing operator {@code ??} used to return its RIGHT operand whatever the left one was, because the
 * ANTLR AST builder had no visitor for the {@code nullCoalescing} alternative: the default {@code visitChildren}
 * returned the last child it visited, so the left operand never reached the tree - issue #6393.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6393NullCoalescingTest extends TestHelper {

  @Test
  void leftOperandWinsWhenNotNull() {
    try (final ResultSet rs = database.query("sql", "SELECT 'left' ?? 'right' AS n")) {
      assertThat(rs.next().<String>getProperty("n")).isEqualTo("left");
    }
    try (final ResultSet rs = database.query("sql", "SELECT 1 ?? 2 AS n")) {
      assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(1);
    }
  }

  @Test
  void rightOperandWinsWhenLeftIsNull() {
    try (final ResultSet rs = database.query("sql", "SELECT null ?? 'right' AS n")) {
      assertThat(rs.next().<String>getProperty("n")).isEqualTo("right");
    }
  }

  @Test
  void bothOperandsNullYieldsNull() {
    try (final ResultSet rs = database.query("sql", "SELECT null ?? null AS n")) {
      assertThat(rs.next().<Object>getProperty("n")).isNull();
    }
  }

  @Test
  void missingPropertyFallsBackToTheDefault() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc6393");
      database.command("sql", "INSERT INTO Doc6393 SET name = 'Jay'");
      database.command("sql", "INSERT INTO Doc6393 SET other = 'x'");
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT name ?? 'unknown' AS n FROM Doc6393 ORDER BY n")) {
        assertThat(rs.next().<String>getProperty("n")).isEqualTo("Jay");
        assertThat(rs.next().<String>getProperty("n")).isEqualTo("unknown");
      }
    });
  }

  @Test
  void chainedCoalescingTakesTheFirstNonNull() {
    try (final ResultSet rs = database.query("sql", "SELECT null ?? 'b' ?? 'c' AS n")) {
      assertThat(rs.next().<String>getProperty("n")).isEqualTo("b");
    }
  }

  @Test
  void usableInAWhereClause() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Where6393");
      database.command("sql", "INSERT INTO Where6393 SET id = 1, name = 'Jay'");
      database.command("sql", "INSERT INTO Where6393 SET id = 2");
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT id FROM Where6393 WHERE (name ?? 'unknown') = 'unknown'")) {
        assertThat(rs.next().<Number>getProperty("id").intValue()).isEqualTo(2);
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  /**
   * The short-circuit is a behaviour, not just an optimisation: the right operand may be a function call or a
   * sub-query, and it must not run when the fallback is not taken. Observed with an argument that throws when it IS
   * evaluated, so the two directions are told apart by whether the error escapes.
   */
  @Test
  void rightOperandIsNotEvaluatedWhenTheLeftIsNotNull() {
    try (final ResultSet rs = database.query("sql", "SELECT 1 ?? 'abcdef'.substring('boom') AS n")) {
      assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(1);
    }
    // ...and IS evaluated when the fallback is taken, so the test above cannot pass by the operand being dead.
    assertThatThrownBy(() -> database.query("sql", "SELECT null ?? 'abcdef'.substring('boom') AS n").next())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void renderingKeepsTheLeftOperand() {
    assertThat(renderProjection("SELECT a ?? b AS n FROM V")).contains("a ?? b");
    // `||` binds tighter than `??`, so the right operand is the whole concatenation.
    assertThat(renderProjection("SELECT a ?? b || c AS n FROM V")).contains("a ?? b || c");
  }

  @Test
  void renderedFormReparsesToTheSameTree() {
    for (final String query : new String[] { "SELECT a ?? b AS n FROM V", "SELECT a ?? b || c AS n FROM V",
        "SELECT a || b ?? c AS n FROM V", "SELECT (a ?? b) AS n FROM V", "SELECT a ?? b ?? c AS n FROM V",
        "SELECT a + 1 ?? b AS n FROM V", "SELECT (a ?? b) + 1 AS n FROM V" }) {
      final String rendered = render(query);
      final String reRendered = render(rendered);
      assertThat(reRendered).as("round trip of '%s'", query).isEqualTo(rendered);
    }
  }

  private String renderProjection(final String query) {
    return render(query);
  }

  private static String render(final String query) {
    final SQLLexer lexer = new SQLLexer(CharStreams.fromString(query));
    final SQLParser parser = new SQLParser(new CommonTokenStream(lexer));
    return new SQLASTBuilder().visitParse(parser.parse()).toString();
  }
}
