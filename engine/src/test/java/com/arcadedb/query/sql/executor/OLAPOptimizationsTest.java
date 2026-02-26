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
import com.arcadedb.database.MutableDocument;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for OLAP performance optimizations including:
 * - Column projection pushdown
 * - Predicate pushdown (ScanWithFilter)
 * - Parallel bucket scanning
 * - Streaming aggregation
 */
class OLAPOptimizationsTest extends TestHelper {

  private static final int RECORD_COUNT = 10_000;
  private static final int CATEGORY_COUNT = 10;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      // Create a "wide" document type with many properties
      database.getSchema().createDocumentType("WideTable");

      for (int i = 0; i < RECORD_COUNT; i++) {
        final MutableDocument doc = database.newDocument("WideTable");
        doc.set("id", i);
        doc.set("name", "Record_" + i);
        doc.set("category", "cat_" + (i % CATEGORY_COUNT));
        doc.set("amount", (i % 100) * 10.5);
        doc.set("status", i % 3 == 0 ? "active" : i % 3 == 1 ? "pending" : "closed");
        doc.set("col6", "padding_" + i);
        doc.set("col7", i * 1.1);
        doc.set("col8", "extra_data_" + (i % 50));
        doc.set("col9", i % 7);
        doc.set("col10", "wide_column_" + (i % 200));
        doc.save();
      }
    });
  }

  // =========== PREDICATE PUSHDOWN (ScanWithFilter) TESTS ===========

  @Test
  void testPredicatePushdownEquality() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL", "SELECT FROM WideTable WHERE status = 'active'");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(r.<String>getProperty("status")).isEqualTo("active");
        count++;
      }
      // Every 3rd record has status 'active'
      assertThat(count).isEqualTo(RECORD_COUNT / 3 + (RECORD_COUNT % 3 > 0 ? 1 : 0));
    });
  }

  @Test
  void testPredicatePushdownRange() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL", "SELECT FROM WideTable WHERE id < 100");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(r.<Integer>getProperty("id")).isLessThan(100);
        count++;
      }
      assertThat(count).isEqualTo(100);
    });
  }

  @Test
  void testPredicatePushdownWithProjection() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL", "SELECT name, amount FROM WideTable WHERE status = 'pending'");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(r.getPropertyNames()).contains("name", "amount");
        count++;
      }
      // Every 3rd record starting from 1 has status 'pending'
      assertThat(count).isGreaterThan(0);
    });
  }

  @Test
  void testPredicatePushdownAnd() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL",
          "SELECT FROM WideTable WHERE status = 'active' AND category = 'cat_0'");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(r.<String>getProperty("status")).isEqualTo("active");
        assertThat(r.<String>getProperty("category")).isEqualTo("cat_0");
        count++;
      }
      assertThat(count).isGreaterThan(0);
    });
  }

  @Test
  void testPredicatePushdownOr() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL",
          "SELECT FROM WideTable WHERE status = 'active' OR status = 'closed'");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        final String status = r.getProperty("status");
        assertThat(status).isIn("active", "closed");
        count++;
      }
      assertThat(count).isGreaterThan(0);
    });
  }

  // =========== COLUMN PROJECTION PUSHDOWN TESTS ===========

  @Test
  void testProjectionOnlyRequestedColumns() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL", "SELECT name, category FROM WideTable");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(r.getPropertyNames()).contains("name", "category");
        count++;
      }
      assertThat(count).isEqualTo(RECORD_COUNT);
    });
  }

  @Test
  void testProjectionSelectStar() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL", "SELECT * FROM WideTable LIMIT 10");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        // SELECT * should return all properties
        assertThat(r.getPropertyNames()).contains("id", "name", "category", "amount", "status");
        count++;
      }
      assertThat(count).isEqualTo(10);
    });
  }

  // =========== PARALLEL BUCKET SCANNING TESTS ===========

  @Test
  void testParallelScanResults() {
    database.transaction(() -> {
      // Ensure parallel scan is enabled
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);

      final ResultSet rs = database.query("SQL", "SELECT FROM WideTable");

      int count = 0;
      final Set<Integer> seenIds = new HashSet<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        final int id = r.getProperty("id");
        assertThat(seenIds.add(id)).as("Duplicate id found: " + id).isTrue();
        count++;
      }
      assertThat(count).isEqualTo(RECORD_COUNT);
    });
  }

  @Test
  void testParallelScanDisabled() {
    database.transaction(() -> {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
      try {
        final ResultSet rs = database.query("SQL", "SELECT FROM WideTable");

        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }
        assertThat(count).isEqualTo(RECORD_COUNT);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
      }
    });
  }

  @Test
  void testParallelScanWithFilter() {
    database.transaction(() -> {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);

      final ResultSet rs = database.query("SQL", "SELECT FROM WideTable WHERE status = 'active'");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(r.<String>getProperty("status")).isEqualTo("active");
        count++;
      }
      assertThat(count).isGreaterThan(0);
    });
  }

  @Test
  void testParallelScanWithLimitApplied() {
    database.transaction(() -> {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);

      final ResultSet rs = database.query("SQL", "SELECT FROM WideTable LIMIT 50");

      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(50);
    });
  }

  // =========== AGGREGATION TESTS ===========

  @Test
  void testAggregationGroupBy() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL",
          "SELECT category, sum(amount) as total, count(*) as cnt FROM WideTable GROUP BY category");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(r.getPropertyNames()).contains("category", "total", "cnt");
        final String cat = r.getProperty("category");
        assertThat(cat).startsWith("cat_");
        final long cnt = r.getProperty("cnt");
        assertThat(cnt).isGreaterThan(0);
        count++;
      }
      assertThat(count).isEqualTo(CATEGORY_COUNT);
    });
  }

  @Test
  void testAggregationNoGroupBy() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL",
          "SELECT count(*) as cnt, sum(amount) as total, min(id) as minId, max(id) as maxId FROM WideTable");

      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      final long cnt = r.getProperty("cnt");
      assertThat(cnt).isEqualTo(RECORD_COUNT);
      final int minId = r.getProperty("minId");
      assertThat(minId).isEqualTo(0);
      final int maxId = r.getProperty("maxId");
      assertThat(maxId).isEqualTo(RECORD_COUNT - 1);
    });
  }

  @Test
  void testAggregationWithFilterAndGroupBy() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL",
          "SELECT category, count(*) as cnt FROM WideTable WHERE status = 'active' GROUP BY category");

      int groupCount = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        final long cnt = r.getProperty("cnt");
        assertThat(cnt).isGreaterThan(0);
        groupCount++;
      }
      assertThat(groupCount).isEqualTo(CATEGORY_COUNT);
    });
  }

  // =========== TWO-PHASE DESERIALIZATION TESTS ===========

  @Test
  void testTwoPhaseDeserializationProjectionWithFilter() {
    // This tests the optimization: SELECT a, max(b) FROM c WHERE a IS NOT NULL AND d > 100
    // - Phase 1: deserialize only 'a' and 'd' for WHERE evaluation
    // - Phase 2: deserialize only 'a' and 'amount' for projection (skip 'd' if not in SELECT)
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL",
          "SELECT name, amount FROM WideTable WHERE name IS NOT NULL AND id > 9000");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(r.getPropertyNames()).contains("name", "amount");
        assertThat(r.<String>getProperty("name")).isNotNull();
        count++;
      }
      assertThat(count).isEqualTo(RECORD_COUNT - 9001);
    });
  }

  @Test
  void testTwoPhaseDeserializationWithAggregation() {
    // SELECT category, sum(amount), count(*) FROM WideTable WHERE status = 'active' GROUP BY category
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL",
          "SELECT category, sum(amount) as total, count(*) as cnt FROM WideTable WHERE status = 'active' GROUP BY category");

      int groupCount = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(r.getPropertyNames()).contains("category", "total", "cnt");
        groupCount++;
      }
      assertThat(groupCount).isEqualTo(CATEGORY_COUNT);
    });
  }

  @Test
  void testProjectionWithFilterSameField() {
    // When 'a' appears in both WHERE and SELECT, it should not be deserialized twice
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL",
          "SELECT name FROM WideTable WHERE name IS NOT NULL");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(r.<String>getProperty("name")).isNotNull();
        count++;
      }
      assertThat(count).isEqualTo(RECORD_COUNT);
    });
  }

  // =========== COMBINED OPTIMIZATION TESTS ===========

  @Test
  void testFilterWithOrderByAndLimit() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL",
          "SELECT name, amount FROM WideTable WHERE status = 'active' ORDER BY amount DESC LIMIT 10");

      int count = 0;
      double prevAmount = Double.MAX_VALUE;
      while (rs.hasNext()) {
        final Result r = rs.next();
        final double amount = r.getProperty("amount");
        assertThat(amount).isLessThanOrEqualTo(prevAmount);
        prevAmount = amount;
        count++;
      }
      assertThat(count).isEqualTo(10);
    });
  }

  @Test
  void testCountWithFilter() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL",
          "SELECT count(*) as cnt FROM WideTable WHERE status = 'active'");

      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      final long cnt = r.getProperty("cnt");
      assertThat(cnt).isEqualTo(RECORD_COUNT / 3 + (RECORD_COUNT % 3 > 0 ? 1 : 0));
    });
  }

  @Test
  void testExecutionPlanShowsScanWithFilter() {
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL",
          "EXPLAIN SELECT FROM WideTable WHERE status = 'active'");

      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      final String plan = r.getProperty("executionPlan").toString();
      // Should show SCAN WITH FILTER instead of separate FETCH + FILTER
      assertThat(plan).contains("SCAN WITH FILTER");
    });
  }
}
