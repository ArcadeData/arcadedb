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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.executor.steps.ExpandIntoStep;
import com.arcadedb.query.opencypher.optimizer.plan.AnchorSelection;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Regression test for issue #9649: the openCypher EXPLAIN and PROFILE text was built with {@code String.format} and no
 * {@code Locale}, so every cost, percentage and millisecond figure in it used the server JVM's default decimal separator
 * (and, under a locale with its own numbering system, its own digits). The same plan for the same query read
 * {@code Total Estimated Cost: 1234.57} on one server and {@code 1234,57} on another.
 * <p>
 * Two locales are used throughout: {@code de-DE}, whose decimal separator is a comma, and {@code th-TH-u-nu-thai}, whose
 * digits are not ASCII - the second also catches an integer {@code %d}, which carries no decimal separator at all.
 */
class Issue9649CypherPlanLocaleTest extends TestHelper {
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
