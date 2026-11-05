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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.CreateEdgeTypeStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.schema.EdgeType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11330 (#11299, #11308) and #11307: the rendering of a parsed statement is what CREATE MATERIALIZED
 * VIEW and CREATE CONTINUOUS AGGREGATE store and re-parse, and what EXPLAIN reports, so it has to mean what the statement meant.
 * <ul>
 *   <li>#11299: a parenthesised OR under an AND, or a compound condition under a NOT, lost its brackets and re-parsed as a
 *   different predicate</li>
 *   <li>#11308: a back-ticked {@code @cat} rendered bare, which the grammar could not read</li>
 *   <li>#11307: CREATE EDGE TYPE rendered without LIGHTWEIGHT, UNIDIRECTIONAL and UNIQUE</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11330LossyStatementRenderingTest extends TestHelper {

  /**
   * Every condition is evaluated against all 16 combinations of a, b, c, d in {1, 2}, written as-is and as rendered: the two must
   * select the same rows. Rendering again must also be stable.
   */
  private static final List<String> CONDITIONS = List.of(//
      "a = 1 AND (b = 2 OR c = 1)",//
      "(a = 1 OR b = 2) AND (c = 1 OR d = 2)",//
      "(a = 1 OR b = 2) AND c = 1",//
      "NOT (a = 1 AND b = 2)",//
      "NOT (a = 1 OR b = 2)",//
      "NOT (NOT a = 1)",//
      "NOT (NOT (a = 1 OR b = 2))",//
      "a = 1 AND NOT (b = 2 OR c = 1)",//
      "a = 1 AND (b = 2 OR (c = 1 AND (d = 2 OR a = 2)))",//
      "NOT (a = 1 AND (b = 2 OR c = 1)) OR d = 2",//
      "(a = 1 OR b = 2) AND NOT (c = 1 AND d = 2) AND (a = 2 OR d = 1)",//
      "a BETWEEN 1 AND 1 AND (b = 2 OR c = 1)",//
      "NOT (a BETWEEN 1 AND 1 OR b = 2)",//
      "a = 1 AND (b IN [2] OR c NOT IN [2])",//
      // controls: no grouping that carries a meaning
      "a = 1 AND b = 2 OR c = 1",//
      "NOT a = 1 AND b = 2",//
      "(a = 1 OR b = 2)",//
      "(a = 1 AND b = 2) OR (c = 1 AND d = 2)");

  @Test
  void aRenderedConditionSelectsTheSameRowsAsTheConditionItWasParsedFrom() {
    createTruthTable();

    for (final String condition : CONDITIONS) {
      final String query = "SELECT a, b, c, d FROM T WHERE " + condition + " ORDER BY a, b, c, d";
      final String rendered = render(query);

      assertThat(rows(rendered)).as("rendering of '%s' is '%s'", query, rendered).isEqualTo(rows(query));
      assertThat(render(rendered)).as("rendering of '%s' is stable", query).isEqualTo(rendered);
    }
  }

  @Test
  void theReportedRenderingsKeepTheirGrouping() {
    assertThat(render("SELECT * FROM T WHERE a = 1 AND (b = 2 OR c = 3)")).isEqualTo("SELECT * FROM T WHERE a = 1 AND (b = 2 OR c = 3)");
    assertThat(render("SELECT FROM T WHERE (a = 1 OR b = 2) AND (c = 3 OR d = 4)")).isEqualTo(
        "SELECT FROM T WHERE (a = 1 OR b = 2) AND (c = 3 OR d = 4)");
    assertThat(render("UPDATE T SET x = 1 WHERE a = 1 AND (b = 2 OR c = 3)")).isEqualTo(
        "UPDATE T SET x = 1 WHERE a = 1 AND (b = 2 OR c = 3)");
    assertThat(render("SELECT FROM T WHERE NOT (a = 1 AND b = 2)")).isEqualTo("SELECT FROM T WHERE NOT (a = 1 AND b = 2)");
    // a grouping that carries no meaning is still not printed
    assertThat(render("SELECT FROM T WHERE (a = 1 OR b = 2)")).isEqualTo("SELECT FROM T WHERE a = 1 OR b = 2");
    assertThat(render("SELECT FROM T WHERE (a = 1 AND b = 2) OR c = 3")).isEqualTo("SELECT FROM T WHERE a = 1 AND b = 2 OR c = 3");
  }

  @Test
  void aMaterializedViewHoldsTheRowsOfTheSelectItWasGiven() {
    createTruthTable();

    final String select = "SELECT a, b, c, d FROM T WHERE a = 1 AND (b = 2 OR c = 1)";
    database.command("sql", "CREATE MATERIALIZED VIEW mv AS " + select);

    assertThat(count("SELECT count(*) AS n FROM mv")).isEqualTo(count("SELECT count(*) AS n FROM (" + select + ")"));
    assertThat(count("SELECT count(*) AS n FROM mv")).isEqualTo(6L);
    assertThat(database.getSchema().getMaterializedView("mv").getQuery()).isEqualTo(select);

    final String negated = "SELECT a, b FROM T WHERE NOT (a = 1 AND b = 2)";
    database.command("sql", "CREATE MATERIALIZED VIEW mvnot AS " + negated);
    assertThat(count("SELECT count(*) AS n FROM mvnot")).isEqualTo(12L);

    // the stored text is what a reopen reads back
    reopenDatabase();
    database.command("sql", "REFRESH MATERIALIZED VIEW mv");
    assertThat(count("SELECT count(*) AS n FROM mv")).isEqualTo(6L);
  }

  @Test
  void aBacktickedCatAttributeRendersAsSomethingTheGrammarReads() {
    for (final String query : List.of(//
        "SELECT inE()[0].`@cat` FROM V",//
        "SELECT inE()[0].@cat FROM V",//
        "SELECT `@cat` FROM V",//
        "SELECT @cat FROM V",//
        "SELECT a.`@cat` FROM V",//
        "SELECT FROM V WHERE `@cat` = 'v' ORDER BY `@cat`")) {
      final String rendered = render(query);
      assertThat(render(rendered)).as("rendering of '%s' is '%s'", query, rendered).isEqualTo(rendered);
    }
    // the sibling spelling and the other attributes are untouched, and the token takes only the `@` form
    assertThat(render("SELECT a.`@b` FROM V")).isEqualTo("SELECT a.`@b` FROM V");
    assertThat(render("SELECT cat, category, a.cat FROM V WHERE cat = 1 ORDER BY cat")).isEqualTo(
        "SELECT cat, category, a.cat FROM V WHERE cat = 1 ORDER BY cat ASC");
    assertThat(render("SELECT @rid, @type, @in, @out, @this, @props FROM V")).isEqualTo(
        "SELECT @rid, @type, @in, @out, @this, @props FROM V");
  }

  @Test
  void aMaterializedViewOverABacktickedCatAttributeIsCreated() {
    database.command("sql", "CREATE VERTEX TYPE VV");
    database.command("sql", "CREATE EDGE TYPE EE");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO VV");
      database.command("sql", "CREATE EDGE EE FROM (SELECT FROM VV) TO (SELECT FROM VV)");
    });

    final ResultSet direct = database.query("sql", "SELECT inE()[0].`@cat` AS c FROM VV");
    assertThat(direct.next().<String>getProperty("c")).isEqualTo("e");

    database.command("sql", "CREATE MATERIALIZED VIEW mvcat AS SELECT inE()[0].`@cat` AS c FROM VV");
    final ResultSet view = database.query("sql", "SELECT c FROM mvcat");
    assertThat(view.next().<String>getProperty("c")).isEqualTo("e");

    // the bare spelling, which the grammar now reads, answers the same, and so does the unaliased column name
    assertThat(database.query("sql", "SELECT inE()[0].@cat AS c FROM VV").next().<String>getProperty("c")).isEqualTo("e");
    assertThat(database.query("sql", "SELECT @cat AS c FROM VV").next().<String>getProperty("c")).isEqualTo("v");
    assertThat(database.query("sql", "SELECT inE()[0].`@cat` FROM VV").next().<String>getProperty("inE()[0].@cat")).isEqualTo("e");
  }

  @Test
  void createEdgeTypeRendersItsModifiers() {
    assertEdgeTypeRoundTrip("CREATE EDGE TYPE E1 LIGHTWEIGHT", "create edge type E1 LIGHTWEIGHT", false, true, false);
    assertEdgeTypeRoundTrip("CREATE EDGE TYPE E2 UNIDIRECTIONAL", "create edge type E2 UNIDIRECTIONAL", true, false, false);
    assertEdgeTypeRoundTrip("CREATE EDGE TYPE E3 UNIQUE", "create edge type E3 UNIQUE", false, false, true);
    assertEdgeTypeRoundTrip("CREATE EDGE TYPE E4 IF NOT EXISTS EXTENDS E UNIDIRECTIONAL UNIQUE LIGHTWEIGHT BUCKETS 2",
        "create edge type E4 IF NOT EXISTS EXTENDS E UNIDIRECTIONAL LIGHTWEIGHT UNIQUE BUCKETS 2", true, true, true);
    assertEdgeTypeRoundTrip("CREATE EDGE TYPE E5", "create edge type E5", false, false, false);

    // the three flags are part of the statement's identity
    assertThat(parse("CREATE EDGE TYPE E1 LIGHTWEIGHT")).isNotEqualTo(parse("CREATE EDGE TYPE E1"));
    assertThat(parse("CREATE EDGE TYPE E1 UNIQUE")).isNotEqualTo(parse("CREATE EDGE TYPE E1 UNIDIRECTIONAL"));
    assertThat(parse("CREATE EDGE TYPE E1 UNIQUE")).isEqualTo(parse("CREATE EDGE TYPE E1 UNIQUE"));
    assertThat(parse("CREATE EDGE TYPE E1 UNIQUE").hashCode()).isEqualTo(parse("CREATE EDGE TYPE E1 UNIQUE").hashCode());
  }

  @Test
  void explainOfCreateEdgeTypeReportsItsModifiers() {
    final Result plan = database.command("sql", "EXPLAIN CREATE EDGE TYPE E1 LIGHTWEIGHT").next();
    assertThat(plan.<String>getProperty("executionPlanAsString")).contains("create edge type E1 LIGHTWEIGHT");

    database.command("sql", "CREATE EDGE TYPE E1 LIGHTWEIGHT");
    assertThat(((EdgeType) database.getSchema().getType("E1")).isLightweight()).isTrue();
  }

  private void assertEdgeTypeRoundTrip(final String query, final String expectedRendering, final boolean unidirectional,
      final boolean lightweight, final boolean unique) {
    final String rendered = render(query);
    assertThat(rendered).isEqualTo(expectedRendering);

    final CreateEdgeTypeStatement reparsed = (CreateEdgeTypeStatement) parse(rendered);
    assertThat(reparsed.unidirectional).as(query).isEqualTo(unidirectional);
    assertThat(reparsed.lightweight).as(query).isEqualTo(lightweight);
    assertThat(reparsed.unique).as(query).isEqualTo(unique);
    assertThat(reparsed).isEqualTo(parse(query));
  }

  private void createTruthTable() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.transaction(() -> {
      for (int i = 0; i < 16; i++)
        database.command("sql", "INSERT INTO T SET a = ?, b = ?, c = ?, d = ?", 1 + (i & 1), 1 + (i >> 1 & 1), 1 + (i >> 2 & 1),
            1 + (i >> 3 & 1));
    });
  }

  private List<String> rows(final String query) {
    return database.query("sql", query).stream()
        .map(r -> "" + r.getProperty("a") + r.getProperty("b") + r.getProperty("c") + r.getProperty("d")).toList();
  }

  private long count(final String query) {
    return database.query("sql", query).next().<Number>getProperty("n").longValue();
  }

  private static Statement parse(final String query) {
    return new SQLAntlrParser(null).parse(query);
  }

  private static String render(final String query) {
    final Statement statement = parse(query);
    final StringBuilder builder = new StringBuilder();
    statement.toString(null, builder);
    return builder.toString();
  }
}
