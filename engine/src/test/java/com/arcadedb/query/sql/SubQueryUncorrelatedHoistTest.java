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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12714: an uncorrelated inline sub-query, {@code (SELECT max(x) FROM U)} as an operand of a comparison, of arithmetic,
 * of a function call or as a projection item, was executed again for every row of the enclosing statement. It is now lifted
 * into a LET that runs once, when nothing in it depends on the outer row.
 * <p>
 * The proof that it ran once is not a clock: it is the number of records the database counted as read while the statement ran
 * (the outer scan reads {@code N} and the sub-query's scan another {@code N}; a per-row run adds {@code N} for every row). The proof that nothing else changed is every
 * row of every statement, checked against the answer computed here, and against the twin of the statement whose sub-query is
 * made correlated by a condition that is always true - which keeps it running per row.
 */
class SubQueryUncorrelatedHoistTest extends TestHelper {
  private static final int N = 120;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("T");
    database.getSchema().createDocumentType("U");
    database.transaction(() -> {
      for (int i = 0; i < N; i++) {
        database.newDocument("T").set("id", i).set("g", i % 7).save();
        database.newDocument("U").set("id", i).set("g", i % 5).save();
      }
    });
  }

  /** The records the database has read so far: the outer scan reads {@code N}, and every run of the sub-query another {@code N}. */
  private long reads() {
    return (Long) database.getStats().get("readRecord");
  }

  private static final class Run {
    final List<Result> rows  = new ArrayList<>();
    long               reads;
    String             plan;

    List<Object> column(final String name) {
      final List<Object> values = new ArrayList<>();
      for (final Result row : rows)
        values.add(row.getProperty(name));
      return values;
    }
  }

  @SuppressWarnings("unchecked")
  private Run run(final String sql, final Object... params) {
    final Run run = new Run();
    final long before = reads();
    try (final ResultSet rs = params.length == 1 && params[0] instanceof Map<?, ?> named ?
        database.query("sql", sql, (Map<String, Object>) named) :
        database.query("sql", sql, params)) {
      while (rs.hasNext())
        run.rows.add(rs.next());
      run.plan = rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
    }
    run.reads = reads() - before;
    return run;
  }

  /** The statement ran its sub-query once: the scans are the outer one plus the sub-query's, nowhere near one per row. */
  private void assertRanOnce(final Run run) {
    assertThat(run.plan).contains("LET (once)");
    assertThat(run.reads).as("records read").isLessThan(4L * N);
  }

  private void assertRanPerRow(final Run run) {
    assertThat(run.plan).doesNotContain("LET (once)");
    assertThat(run.reads).as("records read").isGreaterThanOrEqualTo((long) N * N);
  }

  private static List<Object> idsWhere(final java.util.function.IntPredicate predicate) {
    final List<Object> ids = new ArrayList<>();
    for (int i = 0; i < N; i++)
      if (predicate.test(i))
        ids.add(i);
    return ids;
  }

  /** The sub-query that answers {@code max(g)} of U, written as the issue does and as a row-aware scalar. */
  private static final String MAX_G = "(SELECT max(g) AS m FROM U)[0].m";

  @Test
  void aComparisonOperandRunsOnce() {
    final Run run = run("SELECT id FROM T WHERE g = " + MAX_G + " ORDER BY id");
    assertRanOnce(run);
    assertThat(run.column("id")).isEqualTo(idsWhere(i -> i % 7 == 4));

    final Run less = run("SELECT id FROM T WHERE id < (SELECT max(id) AS m FROM U)[0].m ORDER BY id");
    assertRanOnce(less);
    assertThat(less.column("id")).isEqualTo(idsWhere(i -> i < N - 1));

    final Run left = run("SELECT id FROM T WHERE " + MAX_G + " = g ORDER BY id");
    assertRanOnce(left);
    assertThat(left.column("id")).isEqualTo(idsWhere(i -> i % 7 == 4));
  }

  @Test
  void aBareSubQueryCompareAsItAlwaysDid() {
    // the sub-query answers a list of rows and a scalar is never equal to it: the same empty answer as before, and as the LET form
    final Run inline = run("SELECT id FROM T WHERE g = (SELECT max(g) FROM U)");
    final Run let = run("SELECT id FROM T LET $m = (SELECT max(g) FROM U) WHERE g = $m");
    assertThat(inline.rows).isEmpty();
    assertThat(let.rows).isEmpty();
    assertRanOnce(inline);
  }

  @Test
  void aProjectionItemAndArithmeticRunOnce() {
    final Run run = run("SELECT id, (SELECT max(id) AS m FROM U)[0].m + 1 AS c FROM T ORDER BY id");
    assertRanOnce(run);
    assertThat(run.rows).hasSize(N);
    for (final Result row : run.rows)
      assertThat(row.<Integer>getProperty("c")).isEqualTo(N);

    final Run parenthesized = run("SELECT id, ((SELECT max(id) AS m FROM U)[0].m + 1) * 2 AS c FROM T");
    assertRanOnce(parenthesized);
    for (final Result row : parenthesized.rows)
      assertThat(row.<Integer>getProperty("c")).isEqualTo(N * 2);

    final Run wholeRows = run("SELECT id, (SELECT max(id) AS m FROM U) AS sub FROM T");
    assertRanOnce(wholeRows);
    for (final Result row : wholeRows.rows) {
      final List<Result> sub = row.getProperty("sub");
      assertThat(sub).hasSize(1);
      assertThat(sub.getFirst().<Integer>getProperty("m")).isEqualTo(N - 1);
    }
  }

  @Test
  void aFunctionArgumentRunsOnce() {
    final Run run = run("SELECT id FROM T WHERE g = abs(" + MAX_G + ") ORDER BY id");
    assertRanOnce(run);
    assertThat(run.column("id")).isEqualTo(idsWhere(i -> i % 7 == 4));
  }

  @Test
  void anUnaliasedProjectionKeepsItsColumnName() {
    final String text = "(SELECT max(id) AS m FROM U)[0].m";
    final Run run = run("SELECT " + text + " FROM T LIMIT 3");
    assertRanOnce(run);
    assertThat(run.rows).hasSize(3);
    for (final Result row : run.rows) {
      assertThat(row.getPropertyNames()).containsExactly(text);
      assertThat(row.<Integer>getProperty(text)).isEqualTo(N - 1);
    }

    final Run whole = run("SELECT (SELECT max(id) AS m FROM U) FROM T LIMIT 1");
    assertThat(whole.rows.getFirst().getPropertyNames()).containsExactly("(SELECT max(id) AS m FROM U)");
  }

  @Test
  void aNestedSubQueryRunsOnce() {
    final Run run = run(
        "SELECT id FROM T WHERE g = (SELECT max(g) AS m FROM U WHERE id < (SELECT max(id) AS i FROM U)[0].i)[0].m ORDER BY id");
    assertRanOnce(run);
    assertThat(run.column("id")).isEqualTo(idsWhere(i -> i % 7 == 4));
  }

  @Test
  void parametersAreConstantWithinACommandAndFreshForTheNext() {
    final String positional = "SELECT id FROM T WHERE g = (SELECT max(g) AS m FROM U WHERE id < ?)[0].m ORDER BY id";
    final Run three = run(positional, 3);
    assertRanOnce(three);
    assertThat(three.column("id")).isEqualTo(idsWhere(i -> i % 7 == 2));
    // the plan is reused, the parameter is not
    final Run two = run(positional, 2);
    assertThat(two.column("id")).isEqualTo(idsWhere(i -> i % 7 == 1));

    final Run named = run("SELECT id FROM T WHERE g = (SELECT max(g) AS m FROM U WHERE id < :n)[0].m ORDER BY id", Map.of("n", 4));
    assertRanOnce(named);
    assertThat(named.column("id")).isEqualTo(idsWhere(i -> i % 7 == 3));
    final Run namedAgain = run("SELECT id FROM T WHERE g = (SELECT max(g) AS m FROM U WHERE id < :n)[0].m ORDER BY id", Map.of("n", 1));
    assertThat(namedAgain.column("id")).isEqualTo(idsWhere(i -> i % 7 == 0));

    // the ordinal of a parameter is its place in the whole statement, not in the sub-query that holds it
    final Run both = run("SELECT id FROM T WHERE id > ? AND g = (SELECT max(g) AS m FROM U WHERE id < ?)[0].m ORDER BY id", 50, 3);
    assertRanOnce(both);
    assertThat(both.column("id")).isEqualTo(idsWhere(i -> i > 50 && i % 7 == 2));
  }

  @Test
  void anOuterLimitAndAnOrderByStillSeeEveryRow() {
    final Run limited = run("SELECT id FROM T WHERE g = " + MAX_G + " ORDER BY id LIMIT 4");
    assertRanOnce(limited);
    assertThat(limited.column("id")).isEqualTo(idsWhere(i -> i % 7 == 4).subList(0, 4));

    final Run ordered = run("SELECT id FROM T ORDER BY " + MAX_G + " - g, id LIMIT 3");
    assertRanOnce(ordered);
    assertThat(ordered.column("id")).containsExactly(6, 13, 20);
  }

  @Test
  void aGroupedStatementRunsItsSubQueryOnce() {
    final Run run = run("SELECT g, count(*) AS c FROM T WHERE id < (SELECT max(id) AS m FROM U)[0].m GROUP BY g ORDER BY g");
    assertRanOnce(run);
    assertThat(run.rows).hasSize(7);

    final Run scaled = run("SELECT g, sum(id) * (SELECT count(*) AS c FROM U)[0].c AS s FROM T GROUP BY g ORDER BY g");
    assertRanOnce(scaled);
    for (final Result row : scaled.rows) {
      final int g = row.getProperty("g");
      long expected = 0;
      for (int i = 0; i < N; i++)
        if (i % 7 == g)
          expected += i;
      assertThat(row.<Number>getProperty("s").longValue()).isEqualTo(expected * N);
    }
  }

  @Test
  void anIndexedColumnIsProbedWithTheValue() {
    database.command("sql", "CREATE PROPERTY T.g INTEGER").close();
    database.command("sql", "CREATE INDEX ON T (g) NOTUNIQUE").close();
    final Run run = run("SELECT id FROM T WHERE g = " + MAX_G + " ORDER BY id");
    assertRanOnce(run);
    assertThat(run.plan).contains("FETCH FROM INDEX");
    assertThat(run.column("id")).isEqualTo(idsWhere(i -> i % 7 == 4));
  }

  @Test
  void updateAndDeleteEvaluateTheSubQueryOnce() {
    final long before = reads();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE T SET hit = true WHERE g = " + MAX_G)) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo((long) idsWhere(i -> i % 7 == 4).size());
      }
    });
    assertThat(reads() - before).as("records read").isLessThan(8L * N);
    assertThat(run("SELECT id FROM T WHERE hit = true ORDER BY id").column("id")).isEqualTo(idsWhere(i -> i % 7 == 4));

    final long beforeDelete = reads();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "DELETE FROM T WHERE id < (SELECT max(id) AS m FROM U)[0].m")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo(N - 1L);
      }
    });
    assertThat(reads() - beforeDelete).as("records read").isLessThan(8L * N);
    assertThat(run("SELECT id FROM T").column("id")).containsExactly(N - 1);
  }

  @Test
  void aLaterCommandSeesWhatChangedInBetween() {
    final String sql = "SELECT id FROM T WHERE g = " + MAX_G + " ORDER BY id LIMIT 1";
    assertThat(run(sql).column("id")).containsExactly(4);
    database.transaction(() -> database.newDocument("U").set("id", N).set("g", 6).save());
    assertThat(run(sql).column("id")).containsExactly(6);
  }

  @Test
  void aSubQueryOfTheOuterRowRunsForEveryRow() {
    final Run run = run("SELECT id, (SELECT count(*) AS c FROM U WHERE g = $parent.$current.g)[0].c AS c FROM T ORDER BY id");
    assertThat(run.plan).doesNotContain("LET (once)");
    assertThat(run.rows).hasSize(N);
    for (final Result row : run.rows) {
      final int g = (row.<Integer>getProperty("id")) % 7;
      assertThat(row.<Long>getProperty("c")).isEqualTo(g < 5 ? (long) N / 5 : 0L);
    }

    final Run inWhere = run("SELECT id FROM T WHERE (SELECT count(*) AS c FROM U WHERE g = $parent.$current.g)[0].c > 0 ORDER BY id");
    assertThat(inWhere.column("id")).isEqualTo(idsWhere(i -> i % 7 < 5));
    assertThat(inWhere.plan).doesNotContain("LET (once)");
  }

  @Test
  void aSubQueryOfAnOuterLetVariableOrCurrentRunsForEveryRow() {
    final Run let = run("SELECT id FROM T LET $a = g + 1 WHERE (SELECT count(*) AS c FROM U WHERE g = $a - 1)[0].c > 0 ORDER BY id");
    assertThat(let.column("id")).isEqualTo(idsWhere(i -> i % 7 < 5));
    assertThat(let.plan).doesNotContain("LET (once)").doesNotContain("_$$$SUBQUERY$$$_0 =");

    final Run current = run("SELECT id FROM T WHERE (SELECT $parent.$current.g AS x)[0].x = g ORDER BY id");
    assertThat(current.column("id")).isEqualTo(idsWhere(i -> true));
    assertThat(current.plan).doesNotContain("LET (once)");
  }

  @Test
  void aSubQueryWithoutATargetIsNotLifted() {
    final Run run = run("SELECT id FROM T WHERE g = (SELECT 3 AS x)[0].x ORDER BY id");
    assertThat(run.plan).doesNotContain("LET (once)");
    assertThat(run.column("id")).isEqualTo(idsWhere(i -> i % 7 == 3));
  }

  @Test
  void aSubQueryThatReadsTheClockOrRandomnessRunsForEveryRow() {
    final Run random = run("SELECT (SELECT math_random() AS r FROM U LIMIT 1)[0].r AS r FROM T");
    assertRanPerRow(random);
    assertThat(new HashSet<>(random.column("r"))).hasSizeGreaterThan(N / 2);

    final Run clock = run("SELECT id FROM T WHERE (SELECT sysdate().asLong() AS t FROM U LIMIT 1)[0].t > 0");
    assertRanPerRow(clock);
    assertThat(clock.rows).hasSize(N);
  }

  @Test
  void aSubQueryOfAFunctionTheEngineDidNotRegisterRunsForEveryRow() {
    final String name = "issue12714fn";
    final AtomicInteger calls = new AtomicInteger();
    DefaultSQLFunctionFactory.getInstance().register(new SQLFunctionAbstract(name) {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        calls.incrementAndGet();
        return 1;
      }

      @Override
      public String getSyntax() {
        return name + "()";
      }
    });
    try {
      final Run run = run("SELECT id FROM T WHERE (SELECT " + name + "() AS v FROM U LIMIT 1)[0].v = 1");
      assertThat(run.rows).hasSize(N);
      assertThat(calls.get()).isEqualTo(N);
      assertThat(run.plan).doesNotContain("LET (once)");
    } finally {
      DefaultSQLFunctionFactory.getInstance().unregister(name);
    }
  }

  /** The twin of an uncorrelated sub-query that an always-true condition on the outer row keeps running per row. */
  @Test
  void theLiftedStatementAnswersAsItsPerRowTwinDoes() {
    final String[][] pairs = {
        { "SELECT id FROM T WHERE id < (SELECT max(id) AS m FROM U%s)[0].m AND g <> (SELECT min(g) AS m FROM U%s)[0].m ORDER BY id" },
        { "SELECT id, (SELECT sum(g) AS s FROM U%s)[0].s + g AS c FROM T ORDER BY id" },
        { "SELECT id FROM T WHERE g = abs((SELECT max(g) AS m FROM U%s)[0].m) ORDER BY id" },
        { "SELECT id FROM T WHERE (SELECT max(g) AS m FROM U%s) IN [4] ORDER BY id" },
        { "SELECT id, (SELECT g FROM U%s ORDER BY id DESC LIMIT 2) AS pair FROM T ORDER BY id LIMIT 7" },
        { "SELECT id, (SELECT g FROM U WHERE g > 99%a) AS none FROM T ORDER BY id LIMIT 7" },
        { "SELECT id FROM T WHERE g = (SELECT max(g) AS m FROM U%s)[0].m OR id = 0 ORDER BY id" } };
    for (final String[] pair : pairs) {
      final String lifted = pair[0].replace("%s", "").replace("%a", "");
      final String perRow = pair[0].replace("%s", " WHERE $parent.$current.id >= 0").replace("%a", " AND $parent.$current.id >= 0");
      final Run a = run(lifted);
      final Run b = run(perRow);
      assertThat(a.plan).as(lifted).contains("LET (once)");
      assertThat(b.plan).as(perRow).doesNotContain("LET (once)");
      assertThat(a.rows).as(lifted).hasSameSizeAs(b.rows);
      for (int i = 0; i < a.rows.size(); i++)
        assertThat(a.rows.get(i).toJSON().toString()).as(lifted + " row " + i).isEqualTo(b.rows.get(i).toJSON().toString());
    }
  }

  @Test
  void aRepeatedStatementIsLiftedEveryTime() {
    final String sql = "SELECT id FROM T WHERE g = " + MAX_G + " AND id <= " + MAX_G + " + 100 ORDER BY id";
    for (int i = 0; i < 3; i++) {
      final Run run = run(sql);
      assertThat(run.plan).contains("LET (once)");
      assertThat(run.reads).as("records read").isLessThan(4L * N);
      assertThat(run.column("id")).isEqualTo(idsWhere(x -> x % 7 == 4 && x <= 104));
    }
  }
}
