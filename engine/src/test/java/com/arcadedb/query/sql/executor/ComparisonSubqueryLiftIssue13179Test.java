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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13179: an uncorrelated subquery read by any operator other than {@code IN} ({@code =}, {@code <}, {@code CONTAINS},
 * {@code BETWEEN}, a projection...) was not lifted into a LET by the planner, so it ran again for every row evaluated. It now
 * runs once per execution, whatever reads it, and answers the same rows as before; a {@code $parent} subquery still runs per row.
 * <p>
 * The subqueries pass their rows through a counting function, so the count is the number of rows the subquery produced over the
 * whole query: one execution of the one-row subquery counts 1, one per outer row counts 100.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class ComparisonSubqueryLiftIssue13179Test extends TestHelper {
  private static final String        COUNTED = "issue13179Counted";
  private static final int           ROWS    = 100;
  private final        AtomicInteger calls   = new AtomicInteger();

  @Override
  protected void beginTest() {
    functions().register(new SQLFunctionAbstract(COUNTED) {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        calls.incrementAndGet();
        return params[0];
      }

      @Override
      public String getSyntax() {
        return COUNTED + "(<value>)";
      }
    });

    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE P");
      database.command("sql", "CREATE DOCUMENT TYPE L");
      for (int i = 0; i < ROWS; i++)
        database.newDocument("P").set("year", i).set("tags", List.of(i, i + 1)).save();
      database.newDocument("L").set("year", 5).save();
    });
  }

  @Override
  protected void endTest() {
    functions().unregister(COUNTED);
  }

  private DefaultSQLFunctionFactory functions() {
    return ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory();
  }

  private List<Integer> years(final String sql) {
    calls.set(0);
    final List<Integer> years = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      rs.forEachRemaining(r -> years.add(r.<Integer>getProperty("year")));
    }
    years.sort(null);
    return years;
  }

  @Test
  void inStillRunsOnce() {
    assertThat(years("SELECT FROM P WHERE year IN (SELECT issue13179Counted(year) AS year FROM L)")).containsExactly(5);
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void equalsWithModifierRunsOnce() {
    assertThat(years("SELECT FROM P WHERE year = (SELECT issue13179Counted(year) AS year FROM L).year[0]")).containsExactly(5);
    assertThat(calls.get()).as("the subquery ran once, not once per row").isEqualTo(1);
  }

  @Test
  void equalsAgainstTheWholeListRunsOnceAndStillMatchesNothing() {
    // The value is the list of result rows, lifted or not: an integer never equals it
    assertThat(years("SELECT FROM P WHERE year = (SELECT issue13179Counted(year) AS year FROM L)")).isEmpty();
    assertThat(calls.get()).as("the subquery ran once, not once per row").isEqualTo(1);
  }

  @Test
  void lessThanRunsOnce() {
    assertThat(years("SELECT FROM P WHERE year < (SELECT issue13179Counted(year) AS year FROM L).year[0]")).containsExactly(0, 1, 2,
        3, 4);
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void containsRunsOnce() {
    assertThat(years("SELECT FROM P WHERE tags CONTAINS (SELECT issue13179Counted(year) AS year FROM L).year[0]")).containsExactly(4,
        5);
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void betweenRunsOnce() {
    assertThat(years("""
        SELECT FROM P WHERE year BETWEEN (SELECT issue13179Counted(year) AS year FROM L).year[0]
                                     AND (SELECT issue13179Counted(year) + 2 AS year FROM L).year[0]""")).containsExactly(5, 6, 7);
    assertThat(calls.get()).as("each of the two subqueries ran once").isEqualTo(2);
  }

  @Test
  void projectionRunsOnceAndKeepsItsColumnName() {
    calls.set(0);
    int rows = 0;
    try (final ResultSet rs = database.query("sql",
        "SELECT year, (SELECT issue13179Counted(year) AS year FROM L).year[0] AS fromL, (SELECT year FROM L).year[0] FROM P")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<Integer>getProperty("fromL")).isEqualTo(5);
        assertThat(row.<Integer>getProperty("(SELECT year FROM L).year[0]")).isEqualTo(5);
        ++rows;
      }
    }
    assertThat(rows).isEqualTo(ROWS);
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void aCachedPlanRunsTheSubqueryOncePerExecution() {
    final String sql = "SELECT FROM P WHERE year = (SELECT issue13179Counted(year) AS year FROM L).year[0]";
    for (int i = 0; i < 3; i++) {
      assertThat(years(sql)).containsExactly(5);
      assertThat(calls.get()).isEqualTo(1);
    }
  }

  @Test
  void aParentCorrelatedSubqueryStillRunsPerRow() {
    // The counting function is in the subquery's WHERE, so it counts every L row each execution reads: once per outer row
    assertThat(years("""
        SELECT FROM P WHERE year = (SELECT year FROM L WHERE issue13179Counted(year) = $parent.$current.year).year[0]""")).containsExactly(
        5);
    assertThat(calls.get()).as("the correlated subquery ran once per outer row").isEqualTo(ROWS);
  }

  /**
   * A subquery that reads a variable of the per-record LET is correlated without naming {@code $parent}: it has to see each row's
   * value, so it stays per row. The {@code IN} form, lifted long before #13179, used to run it once with the variable unset and
   * match nothing.
   */
  @Test
  void aSubqueryReadingAPerRecordLetStillRunsPerRow() {
    assertThat(years("SELECT FROM P LET $a = year WHERE year = (SELECT year FROM L WHERE issue13179Counted(year) = $a).year[0]"))
        .containsExactly(5);
    assertThat(calls.get()).isEqualTo(ROWS);

    assertThat(years("SELECT FROM P LET $a = year WHERE year IN (SELECT year FROM L WHERE year = $a)")).containsExactly(5);
  }

  /**
   * A correlated subquery is computed by a per-record LET, which has to expose the row as {@code $parent.$current} itself: a RID
   * target, unlike a type scan, sets no current row, so the lifted subquery read none.
   */
  @Test
  void aCorrelatedSubqueryOverARidTargetReadsItsRow() {
    final Object rid;
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM P WHERE year = 5")) {
      rid = rs.next().getProperty("rid");
    }

    assertThat(years("SELECT FROM " + rid + " WHERE year = (SELECT year FROM $parent.$current).year[0]")).containsExactly(5);
    assertThat(years("SELECT FROM " + rid + " WHERE year IN (SELECT year FROM $parent.$current)")).containsExactly(5);
    try (final ResultSet rs = database.query("sql", "SELECT (SELECT year FROM $parent.$current).year[0] AS y FROM " + rid)) {
      assertThat(rs.next().<Integer>getProperty("y")).isEqualTo(5);
    }
  }
}
