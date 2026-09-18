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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7922: {@code SQL_MAX_EXPRESSION_DEPTH} and {@code CYPHER_MAX_EXPRESSION_DEPTH} are both
 * declared {@code SCOPE.DATABASE}, and the refusal a user gets tells them to raise exactly that setting - but the
 * parsers read it off the enum, JVM-wide, so {@code ALTER DATABASE} had no effect at all and the only way to allow
 * one legitimately deep query on one database was to lower the protection for every database on the server.
 * <p>
 * Same shape, and same fix, as #7786.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7922PerDatabaseExpressionDepthTest extends TestHelper {

  @Test
  void alterDatabaseMovesTheSqlExpressionDepthLimit() {
    database.command("sql", "CREATE DOCUMENT TYPE T");

    final int jvmDefault = GlobalConfiguration.SQL_MAX_EXPRESSION_DEPTH.getValueAsInteger();
    final String tooDeep = "SELECT FROM T WHERE " + nested(jvmDefault + 20, "1 = 1");

    assertThatThrownBy(() -> database.query("sql", tooDeep))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Expression nesting exceeds the maximum allowed depth of " + jvmDefault);

    database.command("sql", "ALTER DATABASE `arcadedb.sql.maxExpressionDepth` " + (jvmDefault + 100));
    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.SQL_MAX_EXPRESSION_DEPTH))
        .isEqualTo(jvmDefault + 100);

    // The per-database value is what the parser must consult now; the JVM default is untouched.
    try (final ResultSet rs = database.query("sql", tooDeep)) {
      assertThat(rs.hasNext()).isFalse();
    }
    assertThat(GlobalConfiguration.SQL_MAX_EXPRESSION_DEPTH.getValueAsInteger())
        .as("raising it for one database must not touch the JVM-wide default")
        .isEqualTo(jvmDefault);
  }

  /**
   * Lowering it must bite too, which is what proves the value is read per parse rather than frozen when the database
   * - and with it the statement cache and the parser it holds - was opened.
   * <p>
   * The query after the ALTER is deliberately a DIFFERENT text from the one before it: a statement already in the
   * per-database cache is served from the AST and never re-parsed, so re-running the identical text would prove
   * nothing either way. That is not specific to this setting - it is how every parse-time setting behaves - and it
   * is why the refusal in the issue is recoverable: a query that was refused never made it into the cache.
   */
  @Test
  void alterDatabaseCanAlsoLowerTheSqlExpressionDepthLimit() {
    database.command("sql", "CREATE DOCUMENT TYPE T");

    try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE " + nested(10, "1 = 1"))) {
      assertThat(rs.hasNext()).isFalse();
    }

    database.command("sql", "ALTER DATABASE `arcadedb.sql.maxExpressionDepth` 3");

    assertThatThrownBy(() -> database.query("sql", "SELECT FROM T WHERE " + nested(10, "2 = 2")))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("maximum allowed depth of 3");
  }

  @Test
  void alterDatabaseMovesTheCypherExpressionDepthLimit() {
    database.command("sql", "CREATE VERTEX TYPE Person");

    final int jvmDefault = GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH.getValueAsInteger();
    final String tooDeep = "MATCH (p:Person) WHERE " + nested(jvmDefault + 20, "p.n = 1") + " RETURN p";

    assertThatThrownBy(() -> database.query("opencypher", tooDeep))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("too deeply nested");

    database.command("sql", "ALTER DATABASE `arcadedb.cypher.maxExpressionDepth` " + (jvmDefault + 200));

    try (final ResultSet rs = database.query("opencypher", tooDeep)) {
      assertThat(rs.hasNext()).isFalse();
    }
    assertThat(GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH.getValueAsInteger()).isEqualTo(jvmDefault);
  }

  /** The Cypher twin of {@link #alterDatabaseCanAlsoLowerTheSqlExpressionDepthLimit}, cache caveat included. */
  @Test
  void alterDatabaseCanAlsoLowerTheCypherExpressionDepthLimit() {
    database.command("sql", "CREATE VERTEX TYPE Person");

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) WHERE " + nested(8, "p.n = 1") + " RETURN p")) {
      assertThat(rs.hasNext()).isFalse();
    }

    database.command("sql", "ALTER DATABASE `arcadedb.cypher.maxExpressionDepth` 3");

    assertThatThrownBy(() -> database.query("opencypher",
        "MATCH (p:Person) WHERE " + nested(8, "p.n = 2") + " RETURN p"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("maximum allowed depth of 3");
  }

  /**
   * The other arm of the same guard: a long chain of {@code OR} terms, which nests deeply without nesting a single
   * parenthesis. The {@code ExpressionRewriter} behind it reads the same bound value - see
   * {@code Issue7922ExpressionRewriterDepthTest} for that reader on its own, since in practice the parse listener
   * here refuses first and the rewriter's own guard is the backstop underneath it.
   */
  @Test
  void theChainedExpressionGuardAlsoReadsThePerDatabaseLimit() {
    database.command("sql", "CREATE VERTEX TYPE Person");

    final StringBuilder chained = new StringBuilder("MATCH (p:Person) WHERE p.n = 0");
    for (int i = 1; i < 40; i++)
      chained.append(" OR p.n = ").append(i);
    chained.append(" RETURN p");

    try (final ResultSet rs = database.query("opencypher", chained.toString())) {
      assertThat(rs.hasNext()).isFalse();
    }

    database.command("sql", "ALTER DATABASE `arcadedb.cypher.maxExpressionDepth` 5");

    final StringBuilder chainedAgain = new StringBuilder("MATCH (p:Person) WHERE p.n = 100");
    for (int i = 101; i < 140; i++)
      chainedAgain.append(" OR p.n = ").append(i);
    chainedAgain.append(" RETURN p");

    assertThatThrownBy(() -> database.query("opencypher", chainedAgain.toString()))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("maximum allowed depth of 5");
  }

  private static String nested(final int depth, final String innermost) {
    return "(".repeat(depth) + innermost + ")".repeat(depth);
  }
}
