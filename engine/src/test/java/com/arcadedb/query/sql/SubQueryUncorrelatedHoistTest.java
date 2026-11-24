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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12714: an uncorrelated inline sub-query, {@code (SELECT max(x) FROM U)} written as an operand, ran again for every row
 * of the enclosing statement. Issue #13179 lifted every {@code (SELECT ...)} operand into a LET, but the planner's walk stopped at
 * a parenthesized sub-expression: {@code ((SELECT max(id) AS m FROM U)[0].m + 1) * 2}, or a sub-query inside a map literal, still
 * ran once per outer row. The walk now descends into it, so those run once per execution too, and a
 * correlated one inside the parentheses still runs once per row.
 * <p>
 * The sub-queries read their rows through a counting function, so the count is the number of rows the sub-query produced over
 * the whole statement: one execution of the one-row sub-query counts 1, one per outer row counts {@code N}.
 */
class SubQueryUncorrelatedHoistTest extends TestHelper {
  private static final String        COUNTED = "issue12714Counted";
  private static final int           N       = 120;
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

    database.getSchema().createDocumentType("T");
    database.getSchema().createDocumentType("U");
    database.getSchema().createDocumentType("M");
    database.transaction(() -> {
      for (int i = 0; i < N; i++) {
        database.newDocument("T").set("id", i).set("g", i % 7).save();
        database.newDocument("U").set("id", i).set("g", i % 5).save();
      }
      // ONE ROW, SO A COUNTED SUB-QUERY OVER M COUNTS ONE CALL PER EXECUTION
      database.newDocument("M").set("id", N - 1).set("g", 4).save();
    });
  }

  @Override
  protected void endTest() {
    functions().unregister(COUNTED);
  }

  private DefaultSQLFunctionFactory functions() {
    return ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory();
  }

  /** The max of U.id ({@code N - 1}), read through the counting function from the one-row type M. */
  private static final String MAX_ID = "(SELECT " + COUNTED + "(id) AS m FROM M)[0].m";
  /** The max of U.g ({@code 4}), read the same way. */
  private static final String MAX_G  = "(SELECT " + COUNTED + "(g) AS m FROM M)[0].m";

  private List<Result> run(final String sql) {
    calls.set(0);
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private static List<Object> column(final List<Result> rows, final String name) {
    final List<Object> values = new ArrayList<>();
    for (final Result row : rows)
      values.add(row.getProperty(name));
    return values;
  }

  private static List<Object> idsWhere(final IntPredicate predicate) {
    final List<Object> ids = new ArrayList<>();
    for (int i = 0; i < N; i++)
      if (predicate.test(i))
        ids.add(i);
    return ids;
  }

  @Test
  void aSubQueryInsideAParenthesizedProjectionRunsOnce() {
    final List<Result> rows = run("SELECT id, (" + MAX_ID + " + 1) * 2 AS c FROM T");
    assertThat(rows).hasSize(N);
    for (final Result row : rows)
      assertThat(row.<Integer>getProperty("c")).isEqualTo(N * 2);
    assertThat(calls.get()).as("the sub-query ran once, not once per row").isEqualTo(1);

    final List<Result> nested = run("SELECT id, ((" + MAX_ID + " - id) * 2) AS c FROM T ORDER BY id");
    assertThat(nested).hasSize(N);
    for (final Result row : nested)
      assertThat(row.<Integer>getProperty("c")).isEqualTo((N - 1 - row.<Integer>getProperty("id")) * 2);
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void aSubQueryInsideAParenthesizedComparisonOperandRunsOnce() {
    final List<Result> rows = run("SELECT id FROM T WHERE id >= (" + MAX_ID + " - 10) ORDER BY id");
    assertThat(column(rows, "id")).isEqualTo(idsWhere(i -> i >= N - 11));
    assertThat(calls.get()).isEqualTo(1);

    final List<Result> left = run("SELECT id FROM T WHERE (" + MAX_G + " + 0) = g ORDER BY id");
    assertThat(column(left, "id")).isEqualTo(idsWhere(i -> i % 7 == 4));
    assertThat(calls.get()).isEqualTo(1);

    final List<Result> function = run("SELECT id FROM T WHERE g = abs((" + MAX_G + ") * -1) ORDER BY id");
    assertThat(column(function, "id")).isEqualTo(idsWhere(i -> i % 7 == 4));
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void aSubQueryInsideAMapLiteralRunsOnce() {
    final List<Result> rows = run("SELECT id, {'m': " + MAX_ID + "} AS map FROM T");
    assertThat(rows).hasSize(N);
    for (final Result row : rows) {
      final Map<String, Object> map = row.getProperty("map");
      assertThat(map.get("m")).isEqualTo(N - 1);
    }
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void aSubQueryInsideAParenthesizedConditionInTheProjectionRunsOnce() {
    final List<Result> rows = run("SELECT id, (g = " + MAX_G + ") AS hit FROM T ORDER BY id");
    assertThat(rows).hasSize(N);
    for (final Result row : rows)
      assertThat(row.<Boolean>getProperty("hit")).isEqualTo(row.<Integer>getProperty("id") % 7 == 4);
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void anUnaliasedParenthesizedProjectionKeepsItsColumnName() {
    final String text = "(" + MAX_ID + " + 1) * 2";
    final List<Result> rows = run("SELECT " + text + " FROM T LIMIT 3");
    assertThat(rows).hasSize(3);
    for (final Result row : rows) {
      assertThat(row.getPropertyNames()).containsExactly(text);
      assertThat(row.<Integer>getProperty(text)).isEqualTo(N * 2);
    }
  }

  @Test
  void anOrderByAndAGroupByReadTheLiftedValue() {
    final List<Result> ordered = run("SELECT id FROM T ORDER BY (" + MAX_G + " - g) * 1, id LIMIT 3");
    assertThat(column(ordered, "id")).containsExactly(6, 13, 20);
    assertThat(calls.get()).isEqualTo(1);

    final List<Result> grouped = run("SELECT g, sum(id) AS s FROM T WHERE id < (" + MAX_ID + " - 9) GROUP BY g ORDER BY g");
    assertThat(grouped).hasSize(7);
    for (final Result row : grouped) {
      final int g = row.getProperty("g");
      long expected = 0;
      for (int i = 0; i < N - 10; i++)
        if (i % 7 == g)
          expected += i;
      assertThat(row.<Number>getProperty("s").longValue()).isEqualTo(expected);
    }
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void aCorrelatedSubQueryInsideParenthesesStillAnswersPerRow() {
    // the counting function sits in the sub-query's WHERE over the one-row M: one call per outer row
    final List<Result> rows = run(
        "SELECT id, ((SELECT count(*) AS c FROM M WHERE " + COUNTED + "(g) = $parent.$current.g)[0].c + 0) AS c FROM T ORDER BY id");
    assertThat(rows).hasSize(N);
    for (final Result row : rows)
      assertThat(row.<Long>getProperty("c")).isEqualTo(row.<Integer>getProperty("id") % 7 == 4 ? 1L : 0L);
    assertThat(calls.get()).as("the correlated sub-query ran once per outer row").isEqualTo(N);

    final List<Result> where = run(
        "SELECT id FROM T WHERE ((SELECT count(*) AS c FROM M WHERE g = $parent.$current.g)[0].c * 1) > 0 ORDER BY id");
    assertThat(column(where, "id")).isEqualTo(idsWhere(i -> i % 7 == 4));
  }

  @Test
  void aSubQueryReadingTheOuterRowThroughAMapLiteralStillAnswersPerRow() {
    // the $parent reference sits in a map literal of the sub-query's projection: the sub-query is correlated all the same
    for (final String sql : new String[] {
        "SELECT id, ((SELECT {'v': $parent.$current.id} AS m FROM M)[0].m.v + 0) AS c FROM T ORDER BY id",
        "SELECT id, (SELECT {'v': $parent.$current.id} AS m FROM M)[0].m.v AS c FROM T ORDER BY id" }) {
      final List<Result> rows = run(sql);
      assertThat(rows).as(sql).hasSize(N);
      for (final Result row : rows)
        assertThat(row.<Integer>getProperty("c")).as(sql).isEqualTo(row.<Integer>getProperty("id"));
    }

    // a map literal that reads nothing of the outer row keeps the sub-query uncorrelated: once per execution
    final List<Result> rows = run("SELECT id, ((SELECT {'v': " + COUNTED + "(id)} AS m FROM M)[0].m.v + 0) AS c FROM T");
    assertThat(rows).hasSize(N);
    for (final Result row : rows)
      assertThat(row.<Integer>getProperty("c")).isEqualTo(N - 1);
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void aSubQueryReadingAPerRecordLetInsideParenthesesStillAnswersPerRow() {
    final List<Result> rows = run(
        "SELECT id FROM T LET $a = g WHERE ((SELECT count(*) AS c FROM M WHERE " + COUNTED + "(g) = $a)[0].c + 0) > 0 ORDER BY id");
    assertThat(column(rows, "id")).isEqualTo(idsWhere(i -> i % 7 == 4));
    assertThat(calls.get()).isEqualTo(N);
  }

  @Test
  void aSubQueryInACaseBranchInsideParenthesesStaysLazy() {
    // a CASE branch that is never taken must not run its sub-query, lifted or not
    final List<Result> rows = run("SELECT id, (CASE WHEN id < 0 THEN " + MAX_ID + " ELSE 0 END) AS c FROM T");
    assertThat(rows).hasSize(N);
    for (final Result row : rows)
      assertThat(row.<Integer>getProperty("c")).isEqualTo(0);
    assertThat(calls.get()).isEqualTo(0);
  }

  @Test
  void updateAndDeleteRunTheParenthesizedSubQueryOnce() {
    calls.set(0);
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE T SET hit = true WHERE g = (" + MAX_G + " + 0)")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo((long) idsWhere(i -> i % 7 == 4).size());
      }
    });
    assertThat(calls.get()).isEqualTo(1);
    assertThat(column(run("SELECT id FROM T WHERE hit = true ORDER BY id"), "id")).isEqualTo(idsWhere(i -> i % 7 == 4));

    calls.set(0);
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "DELETE FROM T WHERE id < (" + MAX_ID + " * 1)")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo(N - 1L);
      }
    });
    assertThat(calls.get()).isEqualTo(1);
    assertThat(column(run("SELECT id FROM T"), "id")).containsExactly(N - 1);
  }

  @Test
  void aCachedPlanRunsTheParenthesizedSubQueryOncePerExecutionAndSeesNewData() {
    final String sql = "SELECT id FROM T WHERE g = (" + MAX_G + " + 0) ORDER BY id LIMIT 1";
    for (int i = 0; i < 3; i++) {
      assertThat(column(run(sql), "id")).containsExactly(4);
      assertThat(calls.get()).isEqualTo(1);
    }
    database.transaction(() -> database.command("sql", "UPDATE M SET g = 6").close());
    assertThat(column(run(sql), "id")).containsExactly(6);
    assertThat(calls.get()).isEqualTo(1);
  }
}
