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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9496: a parallel GROUP BY with many keys hands the rows of the keys a worker does not hold to an exchange
 * partitioned by key, so each key is aggregated once instead of once per worker. The answer must be the sequential one:
 * the groups in the order the scan first meets them, each with the non-aggregate values of its first row, the same groups
 * under a LIMIT, and the same values (integers, so the sums are exact in any order).
 */
class Issue9496GroupByExchangeTest extends TestHelper {
  private static final int ROWS = 60_000;
  private static final int KEYS = 40_000;

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.getSchema().createDocumentType("Item", 4);
    final Random rnd = new Random(9496);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument("Item").set("id", i, "k", (long) rnd.nextInt(KEYS), "s", "s" + rnd.nextInt(7), "q", rnd.nextInt(50),
            "p", (double) rnd.nextInt(100_000), "l", (long) rnd.nextInt(1_000_000)).save();
    });
  }

  @Test
  void manyKeysMatchTheSequentialAggregation() {
    final String grouped = "SELECT k, id, sum(q) AS sq, sum(p * 2) AS sp, sum(l) AS sl, avg(q) AS aq, min(p) AS mn, max(s) AS mx, "
        + "count(*) AS n FROM Item GROUP BY k";
    assertThat(plan(grouped)).contains("CALCULATE AGGREGATE PROJECTIONS (parallel)");
    final List<String> parallel = new ArrayList<>();
    assertThat(rowsAndPlan(grouped, parallel)).contains("groups by key exchange");
    assertThat(parallel).hasSizeGreaterThan(30_000).isEqualTo(sequentialRows(grouped));

    for (final String query : new String[] { grouped + " LIMIT 5000", grouped + " ORDER BY sp DESC, k LIMIT 20",
        "SELECT k, s, count(*) AS n, sum(q) AS sq FROM Item WHERE q < 40 GROUP BY k, s",
        "SELECT s, count(*) AS n, sum(q) AS sq FROM Item GROUP BY s ORDER BY s" })
      assertThat(rows(query)).as(query).isNotEmpty().isEqualTo(sequentialRows(query));
  }

  /** The same query twice in a row: the second execution starts from a clean exchange. */
  @Test
  void repeatedExecutionsAgree() {
    final String query = "SELECT k, id, sum(q) AS sq, count(*) AS n FROM Item GROUP BY k";
    final List<String> first = rows(query);
    assertThat(rows(query)).isEqualTo(first).isEqualTo(sequentialRows(query));
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    rowsAndPlan(query, rows);
    return rows;
  }

  private String rowsAndPlan(final String query, final List<String> rows) {
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder sb = new StringBuilder();
        for (final String p : row.getPropertyNames()) {
          final Object v = row.getProperty(p);
          sb.append(p).append('=').append(v).append(':').append(v == null ? "null" : v.getClass().getSimpleName()).append(';');
        }
        rows.add(sb.toString());
      }
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private List<String> sequentialRows(final String query) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    try {
      assertThat(plan(query)).doesNotContain("(parallel");
      return rows(query);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
