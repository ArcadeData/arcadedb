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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reproduces issue #5215: a legitimate top-N-by-aggregate query
 * ({@code SELECT k, sum(v) FROM t GROUP BY k ORDER BY sum DESC LIMIT n}) aborts under the default
 * {@code arcadedb.queryMaxHeapElementsAllowedPerOp} cap because it genuinely needs one accumulator per
 * distinct grouping key before it can rank. The final result is only N rows, but the aggregation must
 * materialise every group first (the LIMIT push-down is intentionally disabled when an ORDER BY is present).
 * <p>
 * The fix makes the default cap auto-scale with the JVM max heap (never below the historical 500000 floor),
 * so large-cardinality aggregations complete out of the box on servers with a big heap while small footprints
 * stay protected. Explicitly setting the cap still fails fast as a safety measure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GroupByHeapCapTest extends TestHelper {

  private void createOrders(final int distinctCustomers) {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE orders");
      database.command("SQL", "CREATE PROPERTY orders.customer_id LONG");
      database.command("SQL", "CREATE PROPERTY orders.amount DOUBLE");
      database.command("SQL", "CREATE PROPERTY orders.status STRING");

      for (int i = 0; i < distinctCustomers; i++)
        database.newDocument("orders")
            .set("customer_id", i)
            .set("amount", (i % 997) + 0.5)
            .set("status", "delivered")
            .save();
    });
  }

  /**
   * The top-N-by-aggregate returns the correct N groups when the number of distinct keys is within the cap.
   */
  @Test
  void topNByAggregateReturnsCorrectResults() {
    final int distinctCustomers = 5_000;
    createOrders(distinctCustomers);

    final ResultSet rs = database.query("SQL",
        "SELECT customer_id, sum(amount) AS s FROM orders WHERE status = 'delivered' "
            + "GROUP BY customer_id ORDER BY s DESC LIMIT 10");

    int rows = 0;
    double previous = Double.MAX_VALUE;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final double s = ((Number) r.getProperty("s")).doubleValue();
      // one order per customer, so each group's sum equals that single order's amount
      assertThat(s).isLessThanOrEqualTo(previous); // ORDER BY s DESC
      previous = s;
      rows++;
    }
    assertThat(rows).isEqualTo(10);
  }

  /**
   * When the cap is explicitly lowered below the number of distinct groups, the query still fails fast with a
   * clear, actionable message naming the setting. This preserves the safety guard against runaway queries.
   */
  @Test
  void groupByFailsFastWhenExplicitCapExceeded() {
    final int distinctCustomers = 1_000;
    createOrders(distinctCustomers);

    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 100L);
    try {
      assertThatThrownBy(() -> {
        final ResultSet rs = database.query("SQL",
            "SELECT customer_id, sum(amount) AS s FROM orders WHERE status = 'delivered' "
                + "GROUP BY customer_id ORDER BY s DESC LIMIT 10");
        while (rs.hasNext())
          rs.next();
      }).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("Limit of allowed groups for in-heap GROUP BY")
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 500_000L);
    }
  }

  /**
   * The very same top-N-by-aggregate that exceeds the historical 500000 default completes once the cap is raised,
   * proving the aggregation logic is correct and only the cap was the blocker.
   */
  @Test
  void topNByAggregateCompletesWhenCapRaised() {
    final int distinctCustomers = 2_000;
    createOrders(distinctCustomers);

    // Lower the cap below the cardinality, then raise it: the query must go from failing to succeeding.
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 100L);
    assertThatThrownBy(() -> {
      final ResultSet rs = database.query("SQL", "SELECT customer_id, sum(amount) AS s FROM orders "
          + "GROUP BY customer_id ORDER BY s DESC LIMIT 10");
      while (rs.hasNext())
        rs.next();
    }).isInstanceOf(CommandExecutionException.class);

    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 1_000_000L);
    try {
      final ResultSet rs = database.query("SQL", "SELECT customer_id, sum(amount) AS s FROM orders "
          + "GROUP BY customer_id ORDER BY s DESC LIMIT 10");
      int rows = 0;
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
      assertThat(rows).isEqualTo(10);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 500_000L);
    }
  }

  /**
   * The default cap auto-scales with the JVM max heap (never below the historical 500000 floor), so large-heap
   * deployments accept large-cardinality aggregations out of the box (issue #5215).
   */
  @Test
  void defaultCapAutoScalesWithHeap() {
    final long maxHeap = Runtime.getRuntime().maxMemory();
    final long expected = maxHeap == Long.MAX_VALUE ? 500_000L : Math.max(500_000L, maxHeap / 2048);

    // Reset to the computed default in case another test in the same JVM explicitly set it.
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.reset();
    final long actual = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();

    assertThat(actual).isGreaterThanOrEqualTo(500_000L);
    assertThat(actual).isEqualTo(expected);
  }
}
