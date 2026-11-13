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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GroupByExecutionTest extends TestHelper {
  public GroupByExecutionTest() {
    autoStartTx = true;
  }

  @Test
  void groupByCount() {
    database.getSchema().createDocumentType("InputTx");

    for (int i = 0; i < 100; i++) {
      final String hash = UUID.randomUUID().toString();
      database.command("sql", "insert into InputTx set address = '" + hash + "'");

      // CREATE RANDOM NUMBER OF COPIES
      final int random = ThreadLocalRandom.current().nextInt(10);
      for (int j = 0; j < random; j++) {
        database.command("sql", "insert into InputTx set address = '" + hash + "'");
      }
    }

    final ResultSet result = database.query("sql", """
        select address, count(*) as occurrences
        from InputTx where address is not null
        group by address
        limit 10
        """);
    while (result.hasNext()) {
      final Result row = result.next();
      assertThat(row.<String>getProperty("address")).isNotNull();
      assertThat(row.<Long>getProperty("occurrences")).isNotNull();
    }
    result.close();
  }

  // Regression test for issue #4855: GROUP BY was silently ignored on the second execution of the
  // same statement because the cached execution plan copy dropped the aggregation step.
  @Test
  void groupByRepeatedExecutionUsesCachedPlanCorrectly() {
    database.getSchema().createDocumentType("Tags");
    database.command("sql", "insert into Tags set tag = 'a'");
    database.command("sql", "insert into Tags set tag = 'a'");
    database.command("sql", "insert into Tags set tag = 'b'");

    for (int i = 0; i < 3; i++) {
      final ResultSet result = database.query("sql", "select tag from Tags group by tag");
      final Set<String> tags = new HashSet<>();
      int rowCount = 0;
      while (result.hasNext()) {
        tags.add(result.next().<String>getProperty("tag"));
        rowCount++;
      }
      result.close();

      assertThat(rowCount).as("iteration %d", i).isEqualTo(2);
      assertThat(tags).as("iteration %d", i).containsExactlyInAnyOrder("a", "b");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "select tag, count(*) as occurrences from Tags2 group by tag",
      "select tag, count(*) as occurrences from Tags2 group by tag;" })
  void groupBySqlScriptWithTrailingSemicolonIsNotIgnored(String sql) {
    database.getSchema().createDocumentType("Tags2");
    database.command("sql", "insert into Tags2 set tag = 'a'");
    database.command("sql", "insert into Tags2 set tag = 'a'");
    database.command("sql", "insert into Tags2 set tag = 'b'");

    final ResultSet result = database.command("sqlscript", sql);
    final Set<String> tags = new HashSet<>();
    final Map<String, Long> occurrencesByTag = new HashMap<>();
    int rowCount = 0;
    while (result.hasNext()) {
      final Result row = result.next();
      final String tag = row.getProperty("tag");
      tags.add(tag);
      occurrencesByTag.put(tag, row.getProperty("occurrences"));
      rowCount++;
    }
    result.close();

    assertThat(rowCount).isEqualTo(2);
    assertThat(tags).containsExactlyInAnyOrder("a", "b");
    assertThat(occurrencesByTag).containsEntry("a", 2L).containsEntry("b", 1L);
  }

  // ---- #6924: multi-key GROUP BY synthetic alias ----
  // SelectExecutionPlanner.addGroupByExpressionsToProjections synthesized the _$$$GROUP_BY_ALIAS$$$_ name from a counter
  // that was declared `final int i = 0` outside the loop and never incremented. Every GROUP BY key that was not already a
  // plain projected identifier was therefore appended to the pre-aggregate projection under the very same alias, so the
  // later values overwrote the earlier ones and the query silently grouped on the last key alone.

  /** Regression test for issue #6924. */
  @Test
  void multiKeyGroupByOnNonProjectedIdentifiersKeepsEveryKey() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE G");
      database.command("SQL", "INSERT INTO G SET a = 1, b = 1, v = 10");
      database.command("SQL", "INSERT INTO G SET a = 1, b = 2, v = 20");
      database.command("SQL", "INSERT INTO G SET a = 2, b = 1, v = 30");

      final List<Long> counts = collectLongs("SELECT count(*) AS c FROM G GROUP BY a, b", "c");

      // (1,1) (1,2) (2,1) are three distinct groups of one record each.
      // Before the fix the plan grouped on `b` alone and returned two rows: 2 and 1.
      assertThat(counts).hasSize(3);
      assertThat(counts).containsExactly(1L, 1L, 1L);
    });
  }

  /** Regression test for issue #6924. */
  @Test
  void multiKeyGroupByExposesEachSyntheticAliasSeparately() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE G2");
      database.command("SQL", "INSERT INTO G2 SET a = 1, b = 1, v = 10");
      database.command("SQL", "INSERT INTO G2 SET a = 1, b = 2, v = 20");
      database.command("SQL", "INSERT INTO G2 SET a = 2, b = 1, v = 30");

      // The GROUP BY keys are projected too, so the grouping must be observable in the output rows.
      final ResultSet rs = database.query("SQL", "SELECT a, b, sum(v) AS s FROM G2 GROUP BY a, b ORDER BY a, b");

      final List<String> rows = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(r.getProperty("a") + "/" + r.getProperty("b") + "/" + ((Number) r.getProperty("s")).longValue());
      }

      assertThat(rows).containsExactly("1/1/10", "1/2/20", "2/1/30");
    });
  }

  /** Regression test for issue #6924. */
  @Test
  void multiKeyGroupByOnComputedExpressionsKeepsEveryKey() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE G3");
      database.command("SQL", "INSERT INTO G3 SET a = 1, b = 1, v = 10");
      database.command("SQL", "INSERT INTO G3 SET a = 1, b = 2, v = 20");
      database.command("SQL", "INSERT INTO G3 SET a = 2, b = 1, v = 30");

      // `a + 0` / `b + 0` are not base identifiers, so both keys take the synthetic-alias branch.
      final ResultSet rs = database.query("SQL", "SELECT count(*) AS c FROM G3 GROUP BY a + 0, b + 0");

      final List<Long> counts = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        counts.add(((Number) r.getProperty("c")).longValue());
        // the synthetic pre-aggregate aliases must never reach the caller
        assertThat(r.getPropertyNames()).allSatisfy(name -> assertThat(name).doesNotContain("_$$$GROUP_BY_ALIAS$$$_"));
      }

      assertThat(counts).hasSize(3);
      assertThat(counts).containsExactly(1L, 1L, 1L);
    });
  }

  /** Regression test for issue #6924. */
  @Test
  void threeKeyGroupByWithOneProjectedKeyKeepsTheOtherTwo() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE G4");
      database.command("SQL", "INSERT INTO G4 SET a = 1, b = 1, c = 1, v = 10");
      database.command("SQL", "INSERT INTO G4 SET a = 1, b = 1, c = 2, v = 20");
      database.command("SQL", "INSERT INTO G4 SET a = 1, b = 2, c = 1, v = 30");

      // `a` is a plain projected identifier and reuses its own projection; `b` and `c` both need a synthetic alias.
      final List<Long> counts = collectLongs("SELECT a, count(*) AS c FROM G4 GROUP BY a, b, c", "c");

      assertThat(counts).hasSize(3);
      assertThat(counts).containsExactly(1L, 1L, 1L);
    });
  }

  /** Regression test for issue #6924. */
  @Test
  void singleComputedGroupByKeyStillWorks() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE G5");
      database.command("SQL", "INSERT INTO G5 SET a = 1, v = 10");
      database.command("SQL", "INSERT INTO G5 SET a = 1, v = 20");
      database.command("SQL", "INSERT INTO G5 SET a = 2, v = 50");

      // distinct per-group sums, so the assertion pins which records landed in which group
      final List<Long> sums = collectLongs("SELECT sum(v) AS c FROM G5 GROUP BY a", "c");

      assertThat(sums).hasSize(2);
      assertThat(sums).containsExactlyInAnyOrder(30L, 50L);
    });
  }

  private List<Long> collectLongs(final String query, final String property) {
    final ResultSet rs = database.query("SQL", query);
    final List<Long> values = new ArrayList<>();
    while (rs.hasNext())
      values.add(((Number) rs.next().getProperty(property)).longValue());
    return values;
  }

  // ---- #9402: GROUP BY heap budget independent of the number of workers ----
  // The query heap budget refused a GROUP BY with many groups on 4 or more cores and let it run on 1 or 2, with the same
  // data and the same heap. Every worker of the parallel scan met most of the keys and charged its own copy of the groups,
  // so the reservation grew with the number of workers. A worker now folds its groups into one shared set once it holds
  // more than its share of the budget, so the memory is about one copy of the groups plus a bounded partial per worker.
  // The GlobalConfiguration values these tests change are restored by restoreBudget(), and the database-level overrides
  // die with the database dropped after each test.

  private static final int BUDGET_ROWS   = 400_000;
  private static final int BUDGET_GROUPS = 80_000;

  private final long budgetMb = GlobalConfiguration.QUERY_MAX_HEAP_RAM.getValueAsLong();

  @AfterEach
  void restoreBudget() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(budgetMb);
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.reset();
  }

  private void createBudgetWorkersFixture() {
    // THE AUTO-STARTED TRANSACTION OF THIS CLASS WOULD WRAP THE LOAD AND THE PARALLEL SCAN: THE FIXTURE RUNS OUTSIDE IT
    if (database.isTransactionActive())
      database.commit();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.getSchema().createDocumentType("T", 4);
    for (int base = 0; base < BUDGET_ROWS; base += 100_000) {
      final int lo = base;
      database.transaction(() -> {
        for (int i = lo; i < lo + 100_000; i++)
          database.newDocument("T").set("k", i % BUDGET_GROUPS, "v", (double) (i % 1000)).save();
      });
    }
  }

  /** Regression test for issue #9402. */
  @Test
  void manyGroupsFitTheBudgetWhateverTheNumberOfWorkers() {
    createBudgetWorkersFixture();
    final String query = "SELECT k, sum(v) AS rev, count(*) AS n FROM T GROUP BY k ORDER BY rev DESC, k ASC LIMIT 10";
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(10_000_000L);

    // THE ANSWER WITHOUT THE BUDGET AND WITHOUT PARALLELISM: WHAT THE PARALLEL ONE MUST GIVE UNDER THE BUDGET
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(0L);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    final List<String> expected = budgetRows(query, null);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    assertThat(expected).hasSize(10);

    // A BUDGET THE GROUPS FIT ONCE, BUT NOT ONCE PER WORKER: ALL OF IT FOR THIS QUERY, WHICH MUST LEAVE NOTHING HELD
    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(96L);
    final StringBuilder plan = new StringBuilder();
    final List<String> parallel = budgetRows(query, plan);

    assertThat(plan.toString()).contains("CALCULATE AGGREGATE PROJECTIONS (parallel:");
    assertThat(parallel).isEqualTo(expected);
    assertThat(QueryHeapBudget.getReservedBytes()).as("everything the query held is given back").isZero();
  }

  /** Regression test for issue #9402. */
  @Test
  void flushedGroupsKeepTheirFirstRowAndEveryAggregate() {
    createBudgetWorkersFixture();
    // A BUDGET SMALL ENOUGH THAT THE WORKERS FLUSH MANY TIMES: THE GROUPS ARE MERGED ACROSS FLUSHES AND FINAL PARTIALS
    final String query = "SELECT k, sum(v) AS s, count(*) AS n, min(v) AS lo, max(v) AS hi, avg(v) AS a FROM T GROUP BY k";
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(10_000_000L);

    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(0L);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    final List<String> expected = budgetRows(query, null);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    assertThat(expected).hasSize(BUDGET_GROUPS);

    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(96L);
    final List<String> parallel = budgetRows(query, null);
    assertThat(parallel).isEqualTo(expected);
  }

  private List<String> budgetRows(final String query, final StringBuilder plan) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder sb = new StringBuilder();
        for (final String p : row.getPropertyNames()) {
          final Object value = row.getProperty(p);
          sb.append(p).append('=').append(value instanceof Double d ? String.format("%.6f", d) : String.valueOf(value)).append(';');
        }
        rows.add(sb.toString());
      }
      if (plan != null)
        plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
    }
    return rows;
  }
}
