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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.executor.steps.ExpandIntoStep;
import com.arcadedb.query.opencypher.optimizer.plan.AnchorSelection;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * EXPLAIN and PROFILE: leading comments, plan locale, runtime errors and write statements without RETURN.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherExplainProfileTest {
  /**
   * Regression test for issue #9077: {@code EXPLAIN} and {@code PROFILE} were not recognised after a leading comment.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9077ExplainWithLeadingComment extends TestHelper {
    @Test
    void explainAndProfileAcceptLeadingComments() {
      database.getSchema().createVertexType("Person");
      database.transaction(() -> database.command("opencypher", "CREATE (:Person {name: 'a'})").close());

      for (final String prefix : new String[] { "// c\n", "/* c */ ", "/* a */\n// b\n  ", "// c\r", "" }) {
        try (final ResultSet rs = database.command("opencypher", prefix + "EXPLAIN MATCH (p:Person) RETURN p.name")) {
          final Object plan = rs.next().getProperty("executionPlan");
          assertThat(plan).as(prefix).isNotNull();
        }
        try (final ResultSet rs = database.command("opencypher", prefix + "profile MATCH (p:Person) RETURN p.name")) {
          final String name = rs.next().getProperty("p.name");
          assertThat(name).as(prefix).isEqualTo("a");
        }
        try (final ResultSet rs = database.query("opencypher", prefix + "EXPLAIN MATCH (p:Person) RETURN p.name")) {
          final Object plan = rs.next().getProperty("executionPlan");
          assertThat(plan).as(prefix).isNotNull();
        }
      }
    }

    @Test
    void unterminatedAndCommentOnlyQueriesFailAsParseErrors() {
      for (final String query : new String[] { "/* never closed EXPLAIN MATCH (n) RETURN n", "// only a comment", "/* only a comment */" })
        assertThatThrownBy(() -> database.command("opencypher", query).close()).as(query).isInstanceOf(CommandParsingException.class);
    }
  }

  /**
   * Regression test for issue #9649: the openCypher EXPLAIN and PROFILE text was built with {@code String.format} and no
   * {@code Locale}, so every cost, percentage and millisecond figure in it used the server JVM's default decimal separator
   * (and, under a locale with its own numbering system, its own digits). The same plan for the same query read
   * {@code Total Estimated Cost: 1234.57} on one server and {@code 1234,57} on another.
   * <p>
   * Two locales are used throughout: {@code de-DE}, whose decimal separator is a comma, and {@code th-TH-u-nu-thai}, whose
   * digits are not ASCII - the second also catches an integer {@code %d}, which carries no decimal separator at all.
   */
  @Nested
  class Issue9649CypherPlanLocale extends TestHelper {
    private static final Locale GERMAN      = Locale.GERMANY;
    private static final Locale THAI_DIGITS = Locale.forLanguageTag("th-TH-u-nu-thai");

    /**
     * A decimal written with a comma and one or two fractional digits: a cost, a percentage or a CREATE phase time under a
     * comma-decimal locale. Three digits after a comma are left out on purpose: that is a step cost grouped by
     * {@code getCostFormatted()} ({@code 1,699μs}), which is the intended rendering, so the three-decimal execution time is
     * pinned by a positive pattern instead.
     */
    private static final String COMMA_DECIMAL = "(?s).*\\d,\\d{1,2}(?!\\d).*";
    /** Any Thai digit, U+0E50 to U+0E59. */
    private static final String THAI_DIGIT    = "(?s).*[๐-๙].*";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.command("opencypher", "CREATE (:Person {name: 'a', age: 1})-[:KNOWS]->(:Person {name: 'b', age: 2})");
        database.command("opencypher", "CREATE (:Person {name: 'c', age: 3})");
      });
    }

    @Test
    void explainOfAnOptimizedMatchRendersItsCostTheSameUnderEveryLocale() {
      final String root = underLocale(Locale.ROOT, () -> planOf("EXPLAIN MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b"));
      assertThat(root).contains("Using Cost-Based Query Optimizer").containsPattern("Total Estimated Cost: \\d+\\.\\d\\d\n");

      final String german = underLocale(GERMAN, () -> planOf("EXPLAIN MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b"));
      assertThat(german).as("a de_DE server must not write the plan's cost with a comma: %s", german)
          .containsPattern("Total Estimated Cost: \\d+\\.\\d\\d\n").doesNotMatch(COMMA_DECIMAL);
      assertThat(german).isEqualTo(root);

      final String thai = underLocale(THAI_DIGITS, () -> planOf("EXPLAIN MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b"));
      assertThat(thai).as("the plan's digits must be ASCII whatever the server's numbering system: %s", thai)
          .doesNotMatch(THAI_DIGIT);
      assertThat(thai).isEqualTo(root);
    }

    @Test
    void profileOfAnOptimizedMatchRendersTimeAndCostWithAPeriod() {
      for (final Locale locale : new Locale[] { GERMAN, THAI_DIGITS }) {
        final String profile = underLocale(locale, () -> planOf("PROFILE MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b"));
        assertThat(profile).as("under %s: %s", locale, profile)
            .containsPattern("Execution Time: \\d+\\.\\d{3} ms\n")
            .containsPattern("Rows Returned: 1\n")
            .containsPattern("Estimated Cost: \\d+\\.\\d\\d\n")
            .doesNotMatch(COMMA_DECIMAL)
            .doesNotMatch(THAI_DIGIT);
      }
    }

    /** PROFILE of a CREATE adds its own breakdown: per-phase times and the share of each phase, as percentages. */
    @Test
    void profileOfACreateRendersItsBreakdownWithAPeriod() {
      for (final Locale locale : new Locale[] { GERMAN, THAI_DIGITS }) {
        final String profile = underLocale(locale, () -> {
          database.begin();
          try {
            return planOf("PROFILE CREATE (:Person {name: 'x', age: 10})-[:KNOWS {since: 2020}]->(:Person {name: 'y', age: 11})");
          } finally {
            database.commit();
          }
        });
        assertThat(profile).as("under %s: %s", locale, profile)
            .contains("+ CREATE")
            .contains("Property eval: ")
            .containsPattern("\\(\\d+\\.\\d%\\)")
            .doesNotMatch(COMMA_DECIMAL)
            .doesNotMatch(THAI_DIGIT);
      }
    }

    /** The physical plan's and the anchor's own descriptions, which a debugging client or a log line reads. */
    @Test
    void physicalPlanAndAnchorDescriptionsRenderTheirCostWithAPeriod() {
      final AnchorSelection anchor = new AnchorSelection("n", null, 1234.567, 10L);
      final PhysicalPlan plan = new PhysicalPlan(null, anchor, 1234.567, 10L);

      for (final Locale locale : new Locale[] { Locale.ROOT, GERMAN, THAI_DIGITS }) {
        underLocale(locale, () -> {
          assertThat(anchor.toString()).as("anchor under %s", locale).contains("cost=1234.57");
          assertThat(plan.toString()).as("plan under %s", locale).contains("cost=1234.57");
          assertThat(plan.explain()).as("plan explain under %s", locale).contains("Estimated Cost: 1234.57\n");
          return null;
        });
      }
    }

    /**
     * {@code ExpandIntoStep} is not built by any planner path today, so it is driven directly: its rendering is fixed
     * here so it is right the day a planner path picks it up.
     */
    @Test
    void expandIntoStepRendersItsCostWithAPeriod() {
      final ExpandIntoStep step = new ExpandIntoStep("a", "b", "r", Direction.OUT, new String[] { "KNOWS" }, 1234.567, 10L,
          new BasicCommandContext());

      for (final Locale locale : new Locale[] { Locale.ROOT, GERMAN, THAI_DIGITS })
        assertThat(underLocale(locale, () -> step.prettyPrint(0, 2))).as("under %s", locale).contains("[cost=1234.57, rows=10]");
    }

    /** A syntax error names a line and a column, which must be ASCII digits whatever the server's numbering system. */
    @Test
    void syntaxErrorPositionsAreAsciiDigits() {
      for (final String query : new String[] { "MATCH (n RETURN n", "RETURN 1 )" }) {
        final CommandParsingException e = underLocale(THAI_DIGITS,
            () -> catchThrowableOfType(CommandParsingException.class, () -> database.query("opencypher", query)));
        assertThat(e).as("query %s", query).isNotNull();
        assertThat(e.getMessage()).as("query %s", query).containsPattern("line 1").doesNotMatch(THAI_DIGIT);
      }
    }

    private String planOf(final String query) {
      try (final ResultSet rs = database.command("opencypher", query)) {
        while (rs.hasNext())
          rs.next();
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    private static <T> T underLocale(final Locale locale, final Supplier<T> action) {
      final Locale previous = Locale.getDefault(Locale.Category.FORMAT);
      try {
        Locale.setDefault(Locale.Category.FORMAT, locale);
        return action.get();
      } finally {
        Locale.setDefault(Locale.Category.FORMAT, previous);
      }
    }
  }

  /**
   * Issue #10252: {@code PROFILE} runs the statement exactly as the statement runs, so a statement that fails fails under
   * {@code PROFILE} too, with the same error, and what it wrote before the error is not committed. The profiled run used to
   * catch the error into the profile text, answer no rows, and let the auto-commit transaction around it commit.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10252ProfileRuntimeError extends TestHelper {
    private static final String FAILING_WRITE = "CREATE (:A) WITH 1 AS z RETURN 1 / (z - 1) AS r";

    @Override
    protected void beginTest() {
      database.transaction(() -> database.getSchema().createVertexType("A"));
    }

    @Test
    void aFailingProfiledWriteFailsAndKeepsNothing() {
      assertThatThrownBy(() -> drain("PROFILE " + FAILING_WRITE)).hasMessageContaining("/ by zero");
      assertThat(countA()).isZero();
      assertThat(database.isTransactionActive()).isFalse();
    }

    @Test
    void aFailingProfiledWriteFailsTheSameAsThePlainStatement() {
      assertThatThrownBy(() -> drain(FAILING_WRITE)).hasMessageContaining("/ by zero");
      assertThat(countA()).isZero();

      assertThatThrownBy(() -> drain("PROFILE " + FAILING_WRITE)).hasMessageContaining("/ by zero");
      assertThat(countA()).isZero();
    }

    @Test
    void aFailingProfiledWriteInsideATransactionReachesTheCaller() {
      database.begin();
      try {
        assertThatThrownBy(() -> drain("PROFILE " + FAILING_WRITE)).hasMessageContaining("/ by zero");
      } finally {
        database.rollback();
      }
      assertThat(countA()).isZero();
    }

    @Test
    void aFailingProfiledReadFails() {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("opencypher", "PROFILE UNWIND [1, 0] AS x RETURN 1 / x AS r")) {
          while (rs.hasNext())
            rs.next();
        }
      }).hasMessageContaining("/ by zero");
    }

    @Test
    void aSucceedingProfiledWriteStillCommitsAndReportsItsProfile() {
      try (final ResultSet rs = database.command("opencypher", "PROFILE CREATE (:A {k: 1}) RETURN 1 AS r")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("r").intValue()).isEqualTo(1);
        assertThat(rs.getExecutionPlan()).isPresent();
        assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("OpenCypher Query Profile");
      }
      assertThat(countA()).isEqualTo(1);
    }

    @Test
    void anExplainedFailingWriteRunsNothing() {
      try (final ResultSet rs = database.command("opencypher", "EXPLAIN " + FAILING_WRITE)) {
        while (rs.hasNext())
          rs.next();
      }
      assertThat(countA()).isZero();
    }

    private void drain(final String cypher) {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        while (rs.hasNext())
          rs.next();
      }
    }

    private long countA() {
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) RETURN count(n) AS c")) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }
  }

  /**
   * Issue #11386: {@code PROFILE} of a statement with no RETURN answers no rows, as the statement it wraps does and as Neo4j's
   * PROFILE does. The eager profiled run collected whatever the last write clause passed on (the matched nodes of a SET, the
   * deleted ones of a DELETE, an empty row of a CREATE) and returned it, so a client iterating a profiled write saw phantom
   * rows. Also covers the declared columns of a statement, which a wire protocol announces for a result that has no row to
   * read them from.
   */
  @Nested
  class Issue11386ProfileReturnlessWrite extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("PF");
        for (int i = 0; i < 5; i++)
          database.command("opencypher", "CREATE (:PF {i: $i})", Map.of("i", i));
      });
    }

    @Test
    void profileOfACreateAnswersNoRows() {
      assertProfiledWriteAnswersNoRows("PROFILE CREATE (:PF {i: 10})");
      assertThat(count("MATCH (n:PF {i: 10}) RETURN count(n) AS c")).isEqualTo(1L);
    }

    @Test
    void profileOfASetAnswersNoRows() {
      assertProfiledWriteAnswersNoRows("PROFILE MATCH (n:PF) SET n.x = 1");
      assertThat(count("MATCH (n:PF) WHERE n.x = 1 RETURN count(n) AS c")).isEqualTo(5L);
    }

    @Test
    void profileOfADeleteAnswersNoRows() {
      assertProfiledWriteAnswersNoRows("PROFILE MATCH (n:PF {i: 0}) DELETE n");
      assertThat(count("MATCH (n:PF) RETURN count(n) AS c")).isEqualTo(4L);
    }

    @Test
    void profileOfAMergeAnswersNoRows() {
      assertProfiledWriteAnswersNoRows("PROFILE MERGE (:PF {i: 11})");
      assertThat(count("MATCH (n:PF {i: 11}) RETURN count(n) AS c")).isEqualTo(1L);
    }

    @Test
    void profileOfARemoveAnswersNoRows() {
      database.transaction(() -> database.command("opencypher", "MATCH (n:PF) SET n.y = 1"));
      assertProfiledWriteAnswersNoRows("PROFILE MATCH (n:PF) REMOVE n.y");
      assertThat(count("MATCH (n:PF) WHERE n.y IS NOT NULL RETURN count(n) AS c")).isZero();
    }

    /** Control: a profiled write with a RETURN keeps its rows. */
    @Test
    void profileOfAWriteWithReturnKeepsItsRows() {
      final List<Object> rows = new ArrayList<>();
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", "PROFILE MATCH (n:PF) SET n.x = 2 RETURN n.i AS i")) {
          while (rs.hasNext())
            rows.add(rs.next().getProperty("i"));
          assertThat(rs.getExecutionPlan()).isPresent();
        }
      });
      assertThat(rows).hasSize(5);
    }

    /** The profile reports the rows the statement answered, not the ones the write clause passed on. */
    @Test
    void theProfileOfAReturnlessWriteReportsZeroRowsReturned() {
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", "PROFILE MATCH (n:PF) SET n.x = 3")) {
          assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("Rows Returned: 0");
        }
      });
    }

    @Test
    void declaredColumnsAreTheReturnItemsInOrder() {
      assertThat(declaredColumns("MATCH (n:PF) WHERE n.i > 100 RETURN n.i AS i, n AS node")).containsExactly("i", "node");
      assertThat(declaredColumns("MATCH (n:PF) RETURN n.i, n")).containsExactly("n.i", "n");
      assertThat(declaredColumns("EXPLAIN MATCH (n:PF) RETURN n.i AS i")).containsExactly("i");
      assertThat(declaredColumns("PROFILE MATCH (n:PF) RETURN n.i AS i")).containsExactly("i");
      assertThat(declaredColumns("  // a comment\n MATCH (n:PF) RETURN n.i AS i;")).containsExactly("i");
      assertThat(declaredColumns("MATCH (n:PF) RETURN n.i AS a UNION MATCH (n:PF) RETURN n.i AS a")).containsExactly("a");
    }

    /** The unaliased names a zero-row result announces are the keys a row of the same statement carries. */
    @Test
    void declaredColumnsOfAnUnaliasedExpressionMatchTheRowKeys() {
      final String query = "MATCH (n:PF) WHERE n.i = 1 RETURN n.i + 1, toUpper('a')";
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(new ArrayList<>(rs.next().getPropertyNames())).isEqualTo(declaredColumns(query));
      }
    }

    @Test
    void aStatementWithNoReturnDeclaresNoColumns() {
      assertThat(declaredColumns("CREATE (:PF {i: 99})")).isNull();
      assertThat(declaredColumns("MATCH (n:PF) WHERE n.i > 100 SET n.x = 1")).isNull();
    }

    private List<String> declaredColumns(final String query) {
      return OpenCypherQueryEngine.declaredColumns((DatabaseInternal) database, query);
    }

    private void assertProfiledWriteAnswersNoRows(final String query) {
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", query)) {
          assertThat(rs.hasNext()).as("PROFILE of a RETURN-less write answers no rows: %s", query).isFalse();
          assertThat(rs.getExecutionPlan()).as("the profile is still reported").isPresent();
          assertThat(rs.getStatistics()).as("the counters are still reported").isPresent();
        }
      });
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }
  }
}
