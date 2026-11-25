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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The character reads the text patterns of a catalog statement ({@code ~}, {@code LIKE}, ...) may make are counted for the whole
 * statement: over every row, every evaluation of a sub-select and every predicate of it, not once per expression node. A
 * sub-select is parsed again for each row of the query around it, so a budget held by a node would start again at every row.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresCatalogStatementPatternWorkTest {
  private static final String DATABASE_PATH = "./target/databases/PostgresCatalogStatementPatternWorkTest";
  private static final String USER          = "root";

  /** A name of this many characters... */
  private static final String TEXT   = "a".repeat(100);
  /** ...matched by this pattern costs some 170,000 character reads and fails: allowed for a match, not for many of them. */
  private static final String COSTLY = "^(.*)(.*)(.*)Y$";
  /** Outer rows enough that the matches of every one of them are far over what a statement may read. */
  private static final int    ROWS   = 3_000;
  /** The one row of a catalog relation paired with every number: a statement the catalog answers, with an outer row per number. */
  private static final String OUTER_ROWS = "select g.n from pg_database d, generate_series(1, " + ROWS + ") as g(n) where ";

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

  // ---------------------------------------------------------------- sub-selects evaluated once per outer row

  @Test
  void aSubSelectCorrelatedToEveryOuterRowSharesTheBudgetOfTheStatement() {
    for (final String subSelect : List.of(
        "exists (select 1 as x where g.n > 0 and '" + TEXT + "' ~ '" + COSTLY + "')",
        "(select 1 as x where g.n > 0 and '" + TEXT + "' ~ '" + COSTLY + "') is null",
        "g.n in (select g.n as x where '" + TEXT + "' ~ '" + COSTLY + "')",
        "g.n = any (array(select g.n as x where '" + TEXT + "' ~ '" + COSTLY + "'))")) {
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> PostgresCatalog.resolve(OUTER_ROWS + subSelect, database, USER)).as(subSelect)
          .isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
      stopwatch.assertGaveUpWithin(15_000, "the budget of the statement from one budget per outer row");
    }
  }

  @Test
  void aSubSelectThatNamesNoOuterColumnSharesTheBudgetOfTheStatement() {
    for (final String subSelect : List.of(
        "exists (select 1 as x where '" + TEXT + "' ~ '" + COSTLY + "')",
        "g.n in (select 1 as x where '" + TEXT + "' like '%a%a%a%b')")) {
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> PostgresCatalog.resolve(OUTER_ROWS + subSelect, database, USER)).as(subSelect)
          .isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
      stopwatch.assertGaveUpWithin(15_000, "the budget of the statement from one budget per outer row");
    }
  }

  @Test
  void aSubSelectOverACatalogRelationSharesTheBudgetOfTheStatement() {
    // the outer rows are the cross product of two relations, and the sub-select reads the name of each of them
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> PostgresCatalog.resolve("select c.relname from pg_class c, generate_series(1, " + ROWS
        + ") as g(n) where exists (select 1 from pg_class d where d.oid = c.oid and '" + TEXT + "' ~ '" + COSTLY + "')", database,
        USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    stopwatch.assertGaveUpWithin(15_000, "the budget of the statement from one budget per outer row");
  }

  // ---------------------------------------------------------------- patterns compiled once per statement

  @Test
  void aPatternOfASubSelectIsCompiledOnceForTheStatementAndNotForEveryOuterRow() {
    // compiling a long pattern takes a tenth of a second or more, and the sub-select is parsed again for each of the 200 rows
    final String pattern = "a".repeat(20_000);
    final String query = "select g.n from pg_database d, generate_series(1, 200) as g(n) where exists "
        + "(select 1 as x where g.n > 0 and 'b' %s '%s')";

    for (final String operator : List.of("~", "~*", "like", "ilike")) {
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query.formatted(operator, pattern), database, USER);
      stopwatch.assertStayedUnder(10_000, "a pattern compiled once for the statement, not once for each of its outer rows");

      assertThat(answer).as(operator).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
      assertThat(answer.rows).as(operator).isEmpty();
    }
  }

  @Test
  void aPatternBuiltForEveryRowIsAnsweredForEveryRow() {
    // every row names a pattern of its own: the statement keeps some of them, and compiles the rest as they come
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve("select g.n from pg_database d, generate_series(1, " + ROWS
        + ") as g(n) where g.n ~ ('^' || g.n || '$') and g.n !~ ('^x' || g.n)", database, USER);

    assertThat(answer).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
    assertThat(answer.rows).hasSize(ROWS);
  }

  // ---------------------------------------------------------------- many predicates in one statement

  @Test
  void theTextPatternsOfOneStatementShareOneBudget() {
    final int predicates = 60;

    // each predicate alone is a few rows' worth of reads; together they are far more than a statement may read
    final StringBuilder orChain = new StringBuilder();
    final StringBuilder projection = new StringBuilder();
    final StringBuilder arms = new StringBuilder();
    for (int i = 0; i < predicates; i++) {
      final String predicate = "'" + TEXT + "' ~ '^(.*)(.*)(.*)" + (char) ('A' + i % 26) + i + "$'";
      orChain.append(i > 0 ? " or " : "").append(predicate);
      projection.append(i > 0 ? ", " : "").append(predicate).append(" as p").append(i);
      arms.append(" when ").append(predicate).append(" then ").append(i);
    }

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    for (final String statement : List.of(
        "select relname from pg_class where " + orChain,
        "select " + projection + " from pg_class",
        "select case" + arms + " else -1 end as c from pg_class",
        "select bool_or(" + orChain + ") as b from pg_class",
        "select c.relname from pg_class c join pg_namespace n on (" + orChain + ")"))
      assertThatThrownBy(() -> PostgresCatalog.resolve(statement, database, USER))
          .as(statement.substring(0, Math.min(60, statement.length()))).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    stopwatch.assertGaveUpWithin(15_000, "the budget of the statement from one budget per predicate");
  }

  @Test
  void theTextPatternsOfAProbeShareOneBudget() {
    final StringBuilder projection = new StringBuilder("pg_catalog.version()");
    for (int i = 0; i < 60; i++)
      projection.append(", '").append(TEXT).append("' ~ '^(.*)(.*)(.*)").append((char) ('A' + i % 26)).append(i).append("$'");

    assertThatThrownBy(() -> PostgresCatalog.resolveProbe("select " + projection, database, USER, null))
        .isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
  }

  // ---------------------------------------------------------------- what is still answered

  @Test
  void aSubSelectWithACheapPatternIsAnsweredForEveryOuterRow() {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(OUTER_ROWS
        + "exists (select 1 as x where g.n > 0 and 'Person' ~ '^(Person|Note)$') "
        + "and g.n in (select g.n as x where 'Person' like 'Pers%')", database, USER);

    assertThat(answer).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
    assertThat(answer.rows).hasSize(ROWS);
  }

  @Test
  void aSubSelectOverTheNamesOfTheCatalogIsAnsweredForEveryOuterRow() {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve("select c.relname from pg_class c where exists "
        + "(select 1 from pg_class d where d.oid = c.oid and d.relname ~ '^(Person|Note)$')", database, USER);

    assertThat(answer).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
    assertThat(answer.rows).extracting(row -> row.get("relname")).containsExactly("Note", "Person");
  }

  @Test
  void aSeparateStatementStartsWithAFullBudget() {
    // a statement that spends most of its budget does not leave the next one short of it
    final String spender = "select relname from pg_class where '" + TEXT + "' ~ '" + COSTLY + "'";
    for (int i = 0; i < 20; i++)
      assertThat(PostgresCatalog.resolve(spender, database, USER)).as("statement %d", i).isNotNull().isNotSameAs(
          PostgresCatalog.DECLINED);
  }
}
