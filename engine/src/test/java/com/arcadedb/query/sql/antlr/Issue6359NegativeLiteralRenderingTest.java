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
package com.arcadedb.query.sql.antlr;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6359, item 2: what the parser builds for a negative literal has to render back as that
 * literal.
 * <p>
 * A unary minus used to be modelled as a subtraction from zero, so the AST for {@code -1} was {@code 0 - 1} and that is
 * what {@code Expression.toString()} produced. The rendering is not cosmetic: it is what EXPLAIN prints, what an
 * unaliased projection is named after, and what a statement that re-reads one of its own settings parses - which is how
 * {@code REBUILD INDEX ... WITH batchSize = -1} reached {@code Integer.parseInt} as the string {@code "0 - 1"} and came
 * back as a raw {@code NumberFormatException} naming neither the setting nor the problem.
 * <p>
 * The same round-trip is asserted for the parentheses the statement was written with, which the renderer dropped for
 * the same reason: {@code (1 + 2) * 3} rendered as {@code 1 + 2 * 3}, which is 7 rather than 9 when read back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6359NegativeLiteralRenderingTest extends TestHelper {

  private static String render(final String query) {
    final Statement statement = new SQLAntlrParser(null).parse(query);
    final StringBuilder builder = new StringBuilder();
    statement.toString(null, builder);
    return builder.toString();
  }

  @Test
  void aNegativeLiteralRendersAsItself() {
    assertThat(render("SELECT -1 AS n")).isEqualTo("SELECT -1 AS n");
    assertThat(render("SELECT -1.5 AS n")).isEqualTo("SELECT -1.5 AS n");
    assertThat(render("SELECT -2147483648 AS n")).isEqualTo("SELECT -2147483648 AS n");
    // Already folded before this change, because 9223372036854775808 does not fit a positive long and the sign has to
    // be applied before the conversion. Asserted so the generalisation cannot regress it.
    assertThat(render("SELECT -9223372036854775808 AS n")).isEqualTo("SELECT -9223372036854775808 AS n");
    assertThat(render("SELECT 2 * -1 AS n")).isEqualTo("SELECT 2 * -1 AS n");
    assertThat(render("SELECT * FROM V WHERE x = -1")).isEqualTo("SELECT * FROM V WHERE x = -1");
    // A unary plus is still dropped, and a minus applied to something that is not a literal is still a subtraction.
    assertThat(render("SELECT +1 AS n")).isEqualTo("SELECT 1 AS n");
    assertThat(render("SELECT -a AS n")).isEqualTo("SELECT 0 - a AS n");
    // A genuine subtraction is untouched.
    assertThat(render("SELECT 1 - 1 AS n")).isEqualTo("SELECT 1 - 1 AS n");
  }

  @Test
  void theParenthesesTheStatementWasWrittenWithSurviveTheRendering() {
    assertThat(render("SELECT (1 + 2) * 3 AS n")).isEqualTo("SELECT (1 + 2) * 3 AS n");
    assertThat(render("SELECT -(1 + 2) AS n")).isEqualTo("SELECT 0 - (1 + 2) AS n");
    assertThat(render("SELECT (a + b) * c AS n")).isEqualTo("SELECT (a + b) * c AS n");
    // Parentheses that were NOT written are not invented: precedence already says what this means.
    assertThat(render("SELECT 1 + 2 * 3 AS n")).isEqualTo("SELECT 1 + 2 * 3 AS n");
  }

  /**
   * And parentheses that carry no meaning are not printed either. Only a COMPOUND arithmetic operand can be
   * re-associated by the precedence around it; around an atom they are decoration, and printing them would rename
   * every unaliased projection that was written that way - {@code SELECT (name) FROM V} has always been a column
   * called {@code name}, and {@code SELECT distinct(name)}, which the parser reads as {@code DISTINCT (name)}, with
   * it.
   */
  @Test
  void parenthesesAroundAnAtomAreNotPrinted() {
    assertThat(render("SELECT (name) FROM V")).isEqualTo("SELECT name FROM V");
    assertThat(render("SELECT (a.b) FROM V")).isEqualTo("SELECT a.b FROM V");
    assertThat(render("SELECT distinct(name) FROM V")).isEqualTo("SELECT DISTINCT name FROM V");
  }

  /**
   * Nesting collapses to the one pair that carries the meaning. Each layer is judged on the operand it wraps, so the
   * layers around an already-parenthesised block wrap something that renders as one atom and print nothing - which
   * leaves the rendering saying exactly what the expression means, once.
   */
  @Test
  void redundantLayersOfParenthesesCollapseToTheOneThatMatters() {
    assertThat(render("SELECT ((a + b)) * c AS n")).isEqualTo("SELECT (a + b) * c AS n");
    assertThat(render("SELECT (((1 + 2))) * 3 AS n")).isEqualTo("SELECT (1 + 2) * 3 AS n");
    assertThat(render("SELECT -((1 + 2)) AS n")).isEqualTo("SELECT 0 - (1 + 2) AS n");
    assertThat(render("SELECT ((a)) FROM V")).isEqualTo("SELECT a FROM V");
  }

  /** And the column name a caller reads back is the one it has always been. */
  @Test
  void anUnaliasedParenthesisedProjectionKeepsItsName() {
    database.transaction(() -> database.getSchema().createDocumentType("V", 1));
    database.transaction(() -> {
      final MutableDocument v = database.newDocument("V");
      v.set("name", "n1");
      v.save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT (name) FROM V")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("n1");
    }
    try (final ResultSet rs = database.query("sql", "SELECT distinct(name) FROM V")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("n1");
    }
  }

  /**
   * The property that matters, stated directly: rendering a parsed statement and parsing the result back must not
   * change what it computes.
   */
  @Test
  void renderingAndReparsingPreservesTheValue() {
    database.transaction(() -> database.getSchema().createDocumentType("V", 1));
    database.transaction(() -> database.newDocument("V").save());

    assertValueSurvivesReparse("SELECT -1 AS n", -1);
    assertValueSurvivesReparse("SELECT 2 * -1 AS n", -2);
    assertValueSurvivesReparse("SELECT -1 + 2 AS n", 1);
    assertValueSurvivesReparse("SELECT -(1 + 2) AS n", -3);
    assertValueSurvivesReparse("SELECT (1 + 2) * 3 AS n", 9);
    assertValueSurvivesReparse("SELECT 1 + 2 * 3 AS n", 7);
    assertValueSurvivesReparse("SELECT -1.5 + 0.5 AS n", -1.0f);
  }

  private void assertValueSurvivesReparse(final String query, final Object expected) {
    final String query2 = "SELECT " + query.substring("SELECT ".length()) + " FROM V";
    try (final ResultSet rs = database.query("sql", query2)) {
      assertThat(rs.next().<Object>getProperty("n")).as(query).isEqualTo(expected);
    }
    final String rendered = render(query) + " FROM V";
    try (final ResultSet rs = database.query("sql", rendered)) {
      assertThat(rs.next().<Object>getProperty("n")).as("re-parsed: " + rendered).isEqualTo(expected);
    }
  }

  /**
   * The setting readers see the number the user typed. {@code batchSize = -1} is still refused - a batch below one is
   * not a smaller batch - but the refusal now names {@code -1} instead of the arithmetic expression the AST used to
   * carry.
   */
  @Test
  void aNumericSettingIsRefusedByTheValueTheUserTyped() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
    });
    database.transaction(() -> {
      final MutableDocument v = database.newDocument("V");
      v.set("id", 1);
      v.save();
    });

    assertThatThrownBy(() -> database.command("sql", "REBUILD INDEX `V[id]` WITH batchSize = -1").close())
        .hasMessageContaining("batchSize").hasMessageContaining("-1")
        .hasMessageNotContaining("0 - 1");

    assertThatThrownBy(() -> database.command("sql", "REBUILD TYPE V WITH batchSize = -1").close())
        .hasMessageContaining("batchSize").hasMessageContaining("-1")
        .hasMessageNotContaining("0 - 1");

    // And a legal one still works, so the guards above are not simply refusing everything.
    database.command("sql", "REBUILD INDEX `V[id]` WITH batchSize = 1000").close();
    database.command("sql", "REBUILD TYPE V WITH batchSize = 1000").close();
    assertThat(database.getSchema().getIndexByName("V[id]").get(new Object[] { 1 }).hasNext()).isTrue();
  }

  /**
   * A numeric setting is read by EVALUATING its expression, not by rendering it, so a BOUND PARAMETER resolves to the
   * number the caller bound. Rendering answers with the placeholder text, which no amount of parsing turns into an
   * integer - the reason the reader takes the evaluated value, exactly like the boolean one next to it.
   */
  @Test
  void aNumericSettingAcceptsABoundParameter() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
    });
    database.transaction(() -> {
      final MutableDocument v = database.newDocument("V");
      v.set("id", 1);
      v.save();
    });

    database.command("sql", "REBUILD INDEX `V[id]` WITH batchSize = :size", Map.of("size", 1000)).close();
    database.command("sql", "REBUILD TYPE V WITH batchSize = :size", Map.of("size", 1000)).close();
    assertThat(database.getSchema().getIndexByName("V[id]").get(new Object[] { 1 }).hasNext()).isTrue();

    // And a bound value that is not a legal batch size is refused by that value, not by its placeholder.
    assertThatThrownBy(() -> database.command("sql", "REBUILD INDEX `V[id]` WITH batchSize = :size",
        Map.of("size", -1)).close()).hasMessageContaining("batchSize").hasMessageContaining("-1");
  }

  /**
   * One value, read the same way whichever shape it arrives in. A whole number is a whole number written as an
   * integer, as a decimal, or as text; a fractional one is not a batch size in any of them, and truncating it would
   * honour a request nobody made.
   */
  @Test
  void aNumericSettingReadsOneValueTheSameWayInEveryShapeItArrivesIn() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("V", 1).createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
    });

    for (final String accepted : new String[] { "1000", "1000.0", "'1000'" })
      database.command("sql", "REBUILD INDEX `V[id]` WITH batchSize = " + accepted).close();

    for (final String refused : new String[] { "1000.5", "'1000.5'", "'not a number'", "0" })
      assertThatThrownBy(() -> database.command("sql", "REBUILD INDEX `V[id]` WITH batchSize = " + refused).close())
          .as(refused).hasMessageContaining("batchSize");
  }
}
