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
package com.arcadedb.postgres;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How much work the text patterns of a catalog statement ({@code ~}, {@code !~}, {@code ~*}, {@code !~*}, {@code LIKE},
 * {@code ILIKE}) may ask of a connection thread. A match is limited by the characters it reads; a pattern can also take many
 * steps without reading any (a repeated empty group, empty alternatives, optional anchors), and those steps are part of the
 * work too. A statement whose pattern needs more than the budget allows is refused as a whole, never answered as unreadable.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresCatalogPatternWorkTest {
  private static final String DATABASE_PATH = "./target/databases/PostgresCatalogPatternWorkTest";
  private static final String USER          = "root";

  /** Patterns whose steps read nothing, each taking far more steps than the budget of one match allows. */
  private static final List<String> READ_FREE_PATTERNS = List.of(
      // counted repetitions of an empty group, nested
      "((){1000}){1000}", "(((){100}){100}){100}",
      // empty alternatives in sequence, each doubling the ways a match can succeed
      "(|)".repeat(12) + "(?!)", "(a*|b*)".repeat(12) + "(?!)",
      // optional anchors and optional groups that can match nothing, in sequence
      "^?".repeat(12) + "(?!)", "(a*)?".repeat(12) + "(?!)");

  private static final List<String> OPERATORS = List.of("~", "!~", "~*", "!~*");

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.transaction(() -> {
      database.getSchema().createDocumentType("Person");
      database.getSchema().createDocumentType("Note");
    });
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  // ---------------------------------------------------------------- patterns that read nothing

  @Test
  void aPatternThatStepsWithoutReadingIsRefusedByEveryOperator() {
    for (final String pattern : READ_FREE_PATTERNS)
      for (final String operator : OPERATORS)
        assertThatThrownBy(() -> evaluate("relname " + operator + " '" + pattern + "'", new Row("Article")))
            .as("%s over %s", operator, pattern).isInstanceOf(PostgresCatalogExpression.BudgetExceeded.class);
  }

  @Test
  void aPatternThatStepsWithoutReadingIsRefusedOverAnEmptyText() {
    for (final String pattern : READ_FREE_PATTERNS)
      assertThatThrownBy(() -> evaluate("'' ~ '" + pattern + "'", new Row("Article"))).as(pattern)
          .isInstanceOf(PostgresCatalogExpression.BudgetExceeded.class);
  }

  @Test
  void aPatternThatStepsWithoutReadingRefusesTheStatement() {
    for (final String pattern : READ_FREE_PATTERNS) {
      assertThatThrownBy(() -> PostgresCatalog.resolve("select relname from pg_class where relname ~ '" + pattern + "'", database,
          USER)).as("over a relation: %s", pattern).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
      assertThatThrownBy(() -> PostgresCatalog.resolveProbe("select '' ~ '" + pattern + "', pg_catalog.version()", database, USER, null))
          .as("in a projection: %s", pattern).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    }
  }

  @Test
  void theHugestCountsAreRefusedAtOnce() {
    // 2^62 empty matches if it were run
    final StallAwareStopwatch refusal = StallAwareStopwatch.start();
    assertThatThrownBy(() -> PostgresCatalog.resolveProbe("select '' ~ '((){2147483647}){2147483647}', pg_catalog.version()", database,
        USER, null)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    refusal.assertGaveUpWithin(15_000, "a refusal from the shape of the pattern from running it");
  }

  @Test
  void theStepsOfEveryRowAreChargedToTheStatement() {
    // allowed for one row, but not for every row of a statement: a repeated empty group of 58,000 steps
    final String pattern = "((){240}){240}";
    final PostgresCatalogExpression expression = parse("relname ~ '" + pattern + "'");
    final Row row = new Row("Article");
    assertThat(expression.evaluate(row)).isEqualTo(Boolean.TRUE);

    final int limit = 20_000;
    int rows = 0;
    boolean refused = false;
    try {
      for (; rows < limit; rows++)
        expression.evaluate(row);
    } catch (final PostgresCatalogExpression.BudgetExceeded e) {
      refused = true;
    }
    assertThat(refused).as("refused after %d rows", rows).isTrue();
    assertThat(rows).isLessThan(limit);
  }

  @Test
  void aPatternThatSearchesAtEveryPositionIsChargedForEveryPosition() {
    // Each of the 100,000 start positions takes its steps and fails without reading a character: the bound is on all of them together.
    final String text = "b".repeat(100_000);
    final String pattern = "(?:" + "^|".repeat(254) + "^)c";
    assertThatThrownBy(() -> evaluate("'" + text + "' ~ '" + pattern + "'", new Row("Article")))
        .isInstanceOf(PostgresCatalogExpression.BudgetExceeded.class);
  }

  // ---------------------------------------------------------------- LIKE

  @Test
  void aLikePatternWithManyWildcardsIsBounded() {
    // each wildcard multiplies the places the next piece is tried at: 300 over 300 positions is some 4.5 million reads
    final String text = "a".repeat(300);
    for (final String operator : List.of("LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE"))
      assertThatThrownBy(() -> evaluate("'" + text + "' " + operator + " '%a%a%a%b'", new Row("Article"))).as(operator)
          .isInstanceOf(PostgresCatalogExpression.BudgetExceeded.class);

    assertThatThrownBy(() -> PostgresCatalog.resolveProbe("select '" + text + "' LIKE '%a%a%a%b', pg_catalog.version()", database, USER,
        null)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
  }

  @Test
  void aLikePatternIsBoundedAcrossRows() {
    // 40 characters and three wildcards are some 200,000 reads a row: allowed for one row, not for every row of a statement
    final String text = "a".repeat(40);
    final PostgresCatalogExpression expression = parse("'" + text + "' LIKE '%a%a%a%b'");
    assertThat(expression.evaluate(new Row("Article"))).isEqualTo(Boolean.FALSE);

    final int limit = 300;
    int rows = 0;
    boolean refused = false;
    try {
      for (; rows < limit; rows++)
        expression.evaluate(new Row("Article"));
    } catch (final PostgresCatalogExpression.BudgetExceeded e) {
      refused = true;
    }
    assertThat(refused).as("refused after %d rows", rows).isTrue();
    assertThat(rows).isLessThan(limit);
  }

  // ---------------------------------------------------------------- what keeps being answered

  @Test
  void ordinaryPatternsAreStillAnswered() {
    final Row row = new Row("Article");
    assertThat(evaluate("relname ~ '^(Art|Aut)'", row)).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname ~ '^(pg_catalog|public|Article)$'", row)).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname !~ '^pg_toast'", row)).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname ~* '^article$'", row)).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname !~* '^article$'", row)).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("relname ~ '^(.*)(.*)e$'", row)).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname ~ '^(.*)(.*)Y$'", row)).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("relname ~ '[[:<:]]Article[[:>:]]'", row)).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname ~ '^[[:upper:]][[:lower:]]+$'", row)).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname ~ 'e?'", row)).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname ~ '(a|b|c|d)*z'", row)).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("'' ~ '^$'", row)).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname ~ ''", row)).isEqualTo(Boolean.TRUE);
  }

  @Test
  void ordinaryLikePatternsAreStillAnswered() {
    final Row row = new Row("Article");
    assertThat(evaluate("relname LIKE 'Art%'", row)).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname LIKE '%cl_'", row)).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname LIKE '%t%c%e'", row)).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname LIKE 'Art'", row)).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("relname NOT LIKE 'Aut%'", row)).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname ILIKE 'ARTICLE'", row)).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname NOT ILIKE 'article'", row)).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("relname LIKE ''", row)).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("'' LIKE '%'", row)).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("'50%' LIKE '50\\%'", row)).isEqualTo(Boolean.TRUE);
  }

  @Test
  void ordinaryCatalogFiltersAreStillAnswered() {
    assertThat(names(PostgresCatalog.resolve("select relname from pg_class where relname ~ '^(Person)$'", database, USER)))
        .containsExactly("Person");
    assertThat(names(PostgresCatalog.resolve("select relname from pg_class where relname ~ '^(.*)(.*)e$'", database, USER)))
        .containsExactly("Note");
    assertThat(names(PostgresCatalog.resolve("select relname from pg_class where relname ~* '^p' and relname !~ '^pg_'", database,
        USER))).containsExactly("Person");
    assertThat(names(PostgresCatalog.resolve("select relname from pg_class where relname LIKE 'N%' or relname ILIKE '%SON'",
        database, USER))).containsExactly("Note", "Person");
    assertThat(names(PostgresCatalog.resolve("select relname from pg_class where relname !~ '^(Person)$'", database, USER)))
        .containsExactly("Note");
  }

  // ---------------------------------------------------------------- helpers

  private static PostgresCatalogExpression parse(final String expression) {
    final PostgresCatalogExpression parsed = PostgresCatalogExpression.parse(PostgresCatalogToken.tokenize(expression));
    assertThat(parsed).as("expression did not parse: %s", expression).isNotNull();
    return parsed;
  }

  private static Object evaluate(final String expression, final Row row) {
    return parse(expression).evaluate(row);
  }

  private static List<Object> names(final PostgresCatalog.Answer answer) {
    assertThat(answer).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
    final List<Object> names = new ArrayList<>();
    for (final Map<String, Object> row : answer.rows)
      names.add(row.get("relname"));
    return names;
  }

  /** One row of a made-up relation: only its name is a column. */
  private record Row(String relname) implements PostgresCatalogExpression.Resolver {
    @Override
    public Object column(final String qualifier, final String name) {
      return "relname".equals(name) ? relname : PostgresCatalogExpression.UNKNOWN;
    }

    @Override
    public Object function(final String name, final List<Object> arguments) {
      return PostgresCatalogExpression.UNKNOWN;
    }
  }
}
