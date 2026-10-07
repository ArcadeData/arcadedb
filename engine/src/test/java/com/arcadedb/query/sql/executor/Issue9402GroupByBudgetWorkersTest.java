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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9402: the query heap budget refused a GROUP BY with many groups on 4 or more cores and let it run on 1 or 2, with
 * the same data and the same heap. Every worker of the parallel scan met most of the keys and charged its own copy of the
 * groups, so the reservation grew with the number of workers. A worker now folds its groups into one shared set once it
 * holds more than its share of the budget, so the memory is about one copy of the groups plus a bounded partial per worker.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9402GroupByBudgetWorkersTest extends TestHelper {
  private static final int ROWS   = 400_000;
  private static final int GROUPS = 80_000;

  private final long budgetMb = GlobalConfiguration.QUERY_MAX_HEAP_RAM.getValueAsLong();

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.getSchema().createDocumentType("T", 4);
    for (int base = 0; base < ROWS; base += 100_000) {
      final int lo = base;
      database.transaction(() -> {
        for (int i = lo; i < lo + 100_000; i++)
          database.newDocument("T").set("k", i % GROUPS, "v", (double) (i % 1000)).save();
      });
    }
  }

  @AfterEach
  void restoreBudget() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(budgetMb);
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.reset();
  }

  @Test
  void manyGroupsFitTheBudgetWhateverTheNumberOfWorkers() {
    final String query = "SELECT k, sum(v) AS rev, count(*) AS n FROM T GROUP BY k ORDER BY rev DESC, k ASC LIMIT 10";
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(10_000_000L);

    // THE ANSWER WITHOUT THE BUDGET AND WITHOUT PARALLELISM: WHAT THE PARALLEL ONE MUST GIVE UNDER THE BUDGET
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(0L);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    final List<String> expected = rows(query, null);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    assertThat(expected).hasSize(10);

    // A BUDGET THE GROUPS FIT ONCE, BUT NOT ONCE PER WORKER
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(96L);
    final StringBuilder plan = new StringBuilder();
    final List<String> parallel = rows(query, plan);

    assertThat(plan.toString()).contains("CALCULATE AGGREGATE PROJECTIONS (parallel:");
    assertThat(parallel).isEqualTo(expected);
    assertThat(QueryHeapBudget.getReservedBytes()).as("everything the query held is given back").isZero();
  }

  @Test
  void flushedGroupsKeepTheirFirstRowAndEveryAggregate() {
    // A BUDGET SMALL ENOUGH THAT THE WORKERS FLUSH MANY TIMES: THE GROUPS ARE MERGED ACROSS FLUSHES AND FINAL PARTIALS
    final String query = "SELECT k, sum(v) AS s, count(*) AS n, min(v) AS lo, max(v) AS hi, avg(v) AS a FROM T GROUP BY k";
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(10_000_000L);

    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(0L);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    final List<String> expected = rows(query, null);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    assertThat(expected).hasSize(GROUPS);

    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(96L);
    final List<String> parallel = rows(query, null);
    assertThat(parallel).isEqualTo(expected);
  }

  private List<String> rows(final String query, final StringBuilder plan) {
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
