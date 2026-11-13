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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.grammar.SQLLexer;
import com.arcadedb.query.sql.parser.CreateEdgeTypeStatement;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.RecordAttribute;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.Vocabulary;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the rendering of a parsed SQL statement back to text, which CREATE MATERIALIZED VIEW, CREATE CONTINUOUS
 * AGGREGATE and EXPLAIN rely on.
 */
class StatementRenderingTest {

  /**
   * Regression for #11330 (#11299, #11308) and #11307: the rendering of a parsed statement is what CREATE MATERIALIZED
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
  @Nested
  class LossyStatementRendering extends TestHelper {

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

    /**
     * Census over the record attributes the engine defines ({@link Property#METADATA_PROPERTIES} and {@code @this}, the names the AST
     * builder turns into a {@code RecordAttribute} however they are spelled): each one, back-ticked, in any case, renders as a bare
     * token the grammar reads back to the same rendering. The next attribute added to the set without a lexer token goes red here,
     * not in a stored view. The two lexer-only RID parts are read bare; back-ticked they stay quoted identifiers.
     */
    @Test
    void everyRecordAttributeRendersAsABareTokenTheGrammarReads() {
      final Set<String> attributes = new TreeSet<>(Property.METADATA_PROPERTIES);
      attributes.add(Property.THIS_PROPERTY);
      assertThat(attributes).as("the census reaches the attributes Property.METADATA_PROPERTIES defines")
          .contains(Property.CAT_PROPERTY, Property.RID_PROPERTY, Property.PROPERTY_TYPES_PROPERTY).hasSizeGreaterThanOrEqualTo(7);

      for (final String attribute : attributes)
        for (final String spelling : List.of(attribute, attribute.toUpperCase(Locale.ENGLISH)))
          for (final String query : List.of("SELECT `" + spelling + "` FROM V", "SELECT inE()[0].`" + spelling + "` FROM V",
              "SELECT FROM V WHERE `" + spelling + "` IS NOT NULL ORDER BY `" + spelling + "`")) {
            final String rendered = render(query);
            assertThat(rendered).as("rendering of '%s'", query).doesNotContain("`");
            // the lexer is case-insensitive, and a bare @THIS reads back as the @this literal
            assertThat(render(rendered)).as("rendering of '%s' is '%s'", query, rendered).isEqualToIgnoringCase(rendered);
          }

      for (final String query : List.of("SELECT @rid_id, @rid_pos FROM V", "SELECT `@rid_id`, `@rid_pos` FROM V")) {
        final String rendered = render(query);
        assertThat(render(rendered)).as("rendering of '%s' is '%s'", query, rendered).isEqualTo(rendered);
      }
    }

    /**
     * Census over the lexer's vocabulary, the authority on what reads bare: every record attribute token ({@code THIS} and every
     * {@code *_ATTR}) is the token of one of {@link RecordAttribute#BARE_TOKEN_NAMES}, and every one of those names lexes as one such
     * token. A token added to {@code SQLLexer.g4} without its name in the renderer's set goes red here.
     */
    @Test
    void theRenderersBareNamesAreExactlyTheLexersAttributeTokens() {
      final Vocabulary vocabulary = SQLLexer.VOCABULARY;
      final Set<String> attributeTokens = new TreeSet<>();
      for (int type = 1; type <= vocabulary.getMaxTokenType(); type++) {
        final String symbolic = vocabulary.getSymbolicName(type);
        if (symbolic != null && (symbolic.equals("THIS") || symbolic.endsWith("_ATTR")))
          attributeTokens.add(symbolic);
      }
      assertThat(attributeTokens).as("the census reaches the lexer's record attribute tokens").contains("CAT_ATTR", "RID_ATTR", "THIS");

      final Set<String> reached = new TreeSet<>();
      for (final String name : RecordAttribute.BARE_TOKEN_NAMES) {
        final List<? extends Token> tokens = new SQLLexer(CharStreams.fromString(name)).getAllTokens();
        assertThat(tokens).as("'%s' lexes as one token", name).hasSize(1);
        final String symbolic = vocabulary.getSymbolicName(tokens.getFirst().getType());
        assertThat(attributeTokens).as("'%s' lexes as %s, a record attribute token", name, symbolic).contains(symbolic);
        reached.add(symbolic);
      }
      assertThat(reached).as("every record attribute token of the lexer renders bare").isEqualTo(attributeTokens);
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
      // a method call on the bare token is a modifier of the attribute, not a namespaced function call
      assertThat(database.query("sql", "SELECT @cat.toUpperCase() AS c FROM VV").next().<String>getProperty("c")).isEqualTo("V");
      assertThat(database.query("sql", "SELECT FROM VV WHERE @cat = 'v' ORDER BY @cat").stream().count()).isEqualTo(1L);
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

  /**
   * Regression for #11407 (#11391, #11383, #11380): the rendering of a parsed statement, which CREATE MATERIALIZED VIEW
   * and INSERT ... FROM store and parse again, has to parse back into the statement it was rendered from.
   * <ul>
   *   <li>#11391: a long literal written with an {@code L} suffix rendered without it, and parsed back as an int</li>
   *   <li>#11383: SKIP was always rendered before LIMIT, so the positional parameters of {@code LIMIT ? SKIP ?} swapped</li>
   *   <li>#11380: a FROM subquery lost its alias</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LossyRendering extends TestHelper {

    @Test
    void aLongLiteralKeepsItsTypeThroughTheRendering() {
      assertThat(render("SELECT 5L AS x")).isEqualTo("SELECT 5L AS x");
      assertThat(render("SELECT 0xFFL AS x")).isEqualTo("SELECT 255L AS x");
      assertThat(render("SELECT -5L AS x")).isEqualTo("SELECT -5L AS x");
      // controls: an int stays an int, and a long the int range cannot hold needs no suffix to parse back as a long
      assertThat(render("SELECT 5 AS x")).isEqualTo("SELECT 5 AS x");
      assertThat(render("SELECT 2147483648 AS x")).isEqualTo("SELECT 2147483648 AS x");

      for (final String query : List.of("SELECT 5L AS x", "SELECT 0xFFL AS x", "SELECT -5L AS x", "SELECT 5 AS x", "SELECT 2147483648 AS x")) {
        final Object before = value(query);
        final Object after = value(render(query));
        assertThat(after).as("'%s' rendered as '%s'", query, render(query)).isEqualTo(before);
        assertThat(after.getClass()).as("'%s' rendered as '%s'", query, render(query)).isEqualTo(before.getClass());
      }
      assertThat(value("SELECT 5L AS x")).isInstanceOf(Long.class);
      assertThat(render(render("SELECT 5L AS x"))).isEqualTo("SELECT 5L AS x");
    }

    @Test
    void limitAndSkipKeepTheOrderTheirParametersAreNumberedIn() {
      database.getSchema().createDocumentType("T");
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newDocument("T").set("id", i).save();
      });

      final String query = "SELECT id FROM T ORDER BY id LIMIT ? SKIP ?";
      final String rendered = render(query);
      assertThat(rendered).isEqualTo("SELECT id FROM T ORDER BY id ASC LIMIT ? SKIP ?");
      assertThat(ids(query, 2, 3)).containsExactly(3, 4);
      assertThat(ids(rendered, 2, 3)).containsExactly(3, 4);

      // named parameters bind by name and positional literal values carry no binding, but the written order is kept for them too
      assertThat(render("SELECT id FROM T LIMIT :l SKIP :s")).isEqualTo("SELECT id FROM T LIMIT :l SKIP :s");
      assertThat(render("SELECT id FROM T LIMIT 2 SKIP 3")).isEqualTo("SELECT id FROM T LIMIT 2 SKIP 3");
      // controls: the other order, and one of the two alone
      assertThat(render("SELECT id FROM T SKIP ? LIMIT ?")).isEqualTo("SELECT id FROM T SKIP ? LIMIT ?");
      assertThat(render("SELECT id FROM T LIMIT ?")).isEqualTo("SELECT id FROM T LIMIT ?");
      assertThat(render("SELECT id FROM T SKIP ?")).isEqualTo("SELECT id FROM T SKIP ?");

      // a copy, which the planner and the profiled-limit path render and execute, keeps the order as well
      final SelectStatement copy = ((SelectStatement) parse(query)).copy();
      final StringBuilder builder = new StringBuilder();
      copy.toString(null, builder);
      assertThat(builder.toString()).isEqualTo(rendered);
    }

    @Test
    void aSubqueryKeepsItsAlias() {
      database.getSchema().createDocumentType("Character");
      database.transaction(() -> database.newDocument("Character").set("name", "Luke").save());

      for (final String query : List.of("SELECT * FROM (SELECT name FROM Character) spark_q0 WHERE 1 = 0",
          "SELECT * FROM (SELECT name FROM Character) AS sub WHERE name = 'Luke'")) {
        final String rendered = render(query);
        final FromItem before = fromItem(query);
        final FromItem after = fromItem(rendered);
        assertThat(before.getAlias()).isNotNull();
        assertThat(after.getAlias()).as("'%s' rendered as '%s'", query, rendered).isEqualTo(before.getAlias());
        assertThat(render(rendered)).isEqualTo(rendered);
      }
      assertThat(render("SELECT * FROM (SELECT name FROM Character) spark_q0 WHERE 1 = 0")).isEqualTo(
          "SELECT * FROM (SELECT name FROM Character) AS spark_q0 WHERE 1 = 0");

      // a materialized view stores the rendering and parses it again
      database.command("sql", "CREATE MATERIALIZED VIEW mv AS SELECT name FROM (SELECT name FROM Character) AS sub");
      assertThat(database.getSchema().getMaterializedView("mv").getQuery()).contains("AS sub");
      assertThat(database.query("sql", "SELECT count(*) AS n FROM mv").next().<Number>getProperty("n").longValue()).isEqualTo(1L);
    }

    private Object value(final String query) {
      return database.query("sql", query).next().getProperty("x");
    }

    private List<Integer> ids(final String query, final Object... args) {
      return database.query("sql", query, args).stream().map(r -> r.<Integer>getProperty("id")).toList();
    }

    private static FromItem fromItem(final String query) {
      return ((SelectStatement) parse(query)).getTarget().getItem();
    }

    private static Statement parse(final String query) {
      return new SQLAntlrParser(null).parse(query);
    }

    private static String render(final String query) {
      final StringBuilder builder = new StringBuilder();
      parse(query).toString((Map<String, Object>) null, builder);
      return builder.toString();
    }
  }

  /**
   * Regression for #11506: CREATE EDGE rendered UNIDIRECTIONAL before IF NOT EXISTS and the SET/CONTENT body, an order
   * the grammar does not accept, so the rendering of an edge with both did not parse back.
   */
  @Nested
  class CreateEdgeRendering extends TestHelper {

    @Test
    void theClausesAreRenderedInTheOrderTheGrammarAccepts() {
      assertThat(render("CREATE EDGE E FROM #1:0 TO #2:0 CONTENT {\"w\":1} UNIDIRECTIONAL")).isEqualTo(
          "CREATE EDGE E FROM #1:0 TO #2:0 CONTENT {\"w\": 1} UNIDIRECTIONAL");
      assertThat(render("CREATE EDGE E FROM #1:0 TO #2:0 SET w = 1 UNIDIRECTIONAL")).isEqualTo(
          "CREATE EDGE E FROM #1:0 TO #2:0 SET w = 1 UNIDIRECTIONAL");
      assertThat(render("CREATE EDGE E FROM #1:0 TO #2:0 IF NOT EXISTS SET w = 1 UNIDIRECTIONAL")).isEqualTo(
          "CREATE EDGE E FROM #1:0 TO #2:0 IF NOT EXISTS SET w = 1 UNIDIRECTIONAL");

      for (final String query : List.of(//
          "CREATE EDGE E FROM #1:0 TO #2:0 CONTENT {\"w\":1} UNIDIRECTIONAL",//
          "CREATE EDGE E FROM #1:0 TO #2:0 SET w = 1 UNIDIRECTIONAL",//
          "CREATE EDGE E FROM #1:0 TO #2:0 IF NOT EXISTS SET w = 1 UNIDIRECTIONAL",//
          "CREATE EDGE E FROM #1:0 TO #2:0 IF NOT EXISTS CONTENT {\"w\":1} UNIDIRECTIONAL",//
          "CREATE EDGE E FROM #1:0 TO #2:0 IF NOT EXISTS UNIDIRECTIONAL",//
          // controls: rendered in the right order already
          "CREATE EDGE E FROM #1:0 TO #2:0 SET w = 1",//
          "CREATE EDGE E FROM #1:0 TO #2:0 UNIDIRECTIONAL",//
          "CREATE EDGE E FROM #1:0 TO #2:0 IF NOT EXISTS SET w = 1")) {
        final String rendered = render(query);
        assertThat(parse(rendered)).as("'%s' rendered as '%s'", query, rendered).isEqualTo(parse(query));
        assertThat(render(rendered)).isEqualTo(rendered);
      }
    }

    private static Statement parse(final String query) {
      return new SQLAntlrParser(null).parse(query);
    }

    private static String render(final String query) {
      final StringBuilder builder = new StringBuilder();
      parse(query).toString((Map<String, Object>) null, builder);
      return builder.toString();
    }
  }

  /**
   * Regression for #6359, item 2: what the parser builds for a negative literal has to render back as that
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
  @Nested
  class NegativeLiteralRendering extends TestHelper {

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
      // A suffix-less literal is a double, so the sum of two of them is a double too (issue #7609).
      assertValueSurvivesReparse("SELECT -1.5 + 0.5 AS n", -1.0d);
      assertValueSurvivesReparse("SELECT -1.5F + 0.5F AS n", -1.0f);
    }

    /**
     * An {@code F}-suffixed literal past the float range lexes and then overflows to infinity, and
     * {@code "InfinityF"} is a spelling the grammar cannot lex - so appending the suffix unconditionally would
     * render a statement that no longer reads back. The suffix is only written where it round-trips (issue #7609).
     */
    @Test
    void aNonFiniteFloatLiteralStillRendersSomethingParseable() {
      for (final String literal : new String[] { "1.0E50F", "-1.0E50F" }) {
        final String rendered = render("SELECT " + literal + " AS n");
        assertThat(rendered).as("rendered " + literal).doesNotContain("InfinityF");
        assertThatCode(() -> render(rendered)).as("re-parsing " + rendered).doesNotThrowAnyException();
      }
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
}
