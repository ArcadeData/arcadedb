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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for OpenCypher predicate pushdown optimization.
 * Verifies that WHERE predicates are correctly evaluated inline during
 * node scanning (pushed down into MatchNodeStep) while producing
 * identical results to the non-optimized path.
 */
class OpenCypherPredicatePushdownTest {
  private Database database;

  private static final int RECORD_COUNT = 500;
  private static final int CATEGORY_COUNT = 10;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testCypherPredicatePushdown").create();
    database.getSchema().createVertexType("Product");
    database.getSchema().createVertexType("Category");

    database.transaction(() -> {
      for (int i = 0; i < RECORD_COUNT; i++) {
        database.command("opencypher",
            "CREATE (p:Product {id: " + i + ", name: 'Product_" + i + "', " +
                "category: 'cat_" + (i % CATEGORY_COUNT) + "', " +
                "price: " + ((i % 100) * 10.5) + ", " +
                "status: '" + (i % 3 == 0 ? "active" : i % 3 == 1 ? "pending" : "closed") + "'})");
      }

      // Create some categories for relationship tests
      for (int i = 0; i < CATEGORY_COUNT; i++) {
        database.command("opencypher",
            "CREATE (c:Category {name: 'cat_" + i + "', priority: " + (i % 3) + "})");
      }
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testPredicatePushdownEquality() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Product) WHERE p.status = 'active' RETURN p");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Vertex v = r.getProperty("p");
        assertThat(v.<String>get("status")).isEqualTo("active");
        count++;
      }
      // Every 3rd record has status 'active' (i % 3 == 0)
      assertThat(count).isEqualTo(RECORD_COUNT / 3 + (RECORD_COUNT % 3 > 0 ? 1 : 0));
    });
  }

  @Test
  void testPredicatePushdownRange() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Product) WHERE p.id < 50 RETURN p");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Vertex v = r.getProperty("p");
        assertThat(((Number) v.get("id")).intValue()).isLessThan(50);
        count++;
      }
      assertThat(count).isEqualTo(50);
    });
  }

  @Test
  void testPredicatePushdownAnd() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Product) WHERE p.status = 'active' AND p.category = 'cat_0' RETURN p");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Vertex v = r.getProperty("p");
        assertThat(v.<String>get("status")).isEqualTo("active");
        assertThat(v.<String>get("category")).isEqualTo("cat_0");
        count++;
      }
      assertThat(count).isGreaterThan(0);
    });
  }

  @Test
  void testPredicatePushdownOr() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Product) WHERE p.status = 'active' OR p.status = 'closed' RETURN p");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Vertex v = r.getProperty("p");
        final String status = (String) v.get("status");
        assertThat(status).isIn("active", "closed");
        count++;
      }
      assertThat(count).isGreaterThan(0);
    });
  }

  @Test
  void testPredicatePushdownIsNotNull() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Product) WHERE p.name IS NOT NULL RETURN p");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Vertex v = r.getProperty("p");
        assertThat(v.<String>get("name")).isNotNull();
        count++;
      }
      assertThat(count).isEqualTo(RECORD_COUNT);
    });
  }

  @Test
  void testPredicatePushdownStringComparison() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Product) WHERE p.name = 'Product_42' RETURN p");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Vertex v = r.getProperty("p");
        assertThat(v.<String>get("name")).isEqualTo("Product_42");
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }

  @Test
  void testPredicatePushdownWithReturn() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Product) WHERE p.id < 10 RETURN p.name AS name, p.price AS price");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(r.getPropertyNames()).contains("name", "price");
        count++;
      }
      assertThat(count).isEqualTo(10);
    });
  }

  @Test
  void testPredicatePushdownWithOrderByAndLimit() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Product) WHERE p.status = 'active' RETURN p ORDER BY p.price DESC LIMIT 5");

      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(5);
    });
  }

  @Test
  void testPredicatePushdownWithCount() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Product) WHERE p.status = 'active' RETURN count(p) AS cnt");

      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      final long cnt = ((Number) r.getProperty("cnt")).longValue();
      assertThat(cnt).isEqualTo(RECORD_COUNT / 3 + (RECORD_COUNT % 3 > 0 ? 1 : 0));
    });
  }

  @Test
  void testPredicatePushdownMultipleMatches() {
    database.transaction(() -> {
      // Two separate node patterns with different filters
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Product), (c:Category) WHERE p.category = 'cat_0' AND c.name = 'cat_0' RETURN p, c");

      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Vertex product = r.getProperty("p");
        final Vertex category = r.getProperty("c");
        assertThat(product.<String>get("category")).isEqualTo("cat_0");
        assertThat(category.<String>get("name")).isEqualTo("cat_0");
        count++;
      }
      // 50 products with cat_0 * 1 category with cat_0
      assertThat(count).isEqualTo(RECORD_COUNT / CATEGORY_COUNT);
    });
  }

  @Test
  void testPredicatePushdownNoMatchingRecords() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Product) WHERE p.status = 'nonexistent' RETURN p");

      assertThat(rs.hasNext()).isFalse();
    });
  }

  @Test
  void testPredicatePushdownAllRecordsMatch() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Product) WHERE p.id >= 0 RETURN p");

      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(RECORD_COUNT);
    });
  }

  @Test
  void testPredicatePushdownNoDuplicates() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Product) WHERE p.status = 'active' RETURN p");

      final Set<Integer> seenIds = new HashSet<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Vertex v = r.getProperty("p");
        final int id = ((Number) v.get("id")).intValue();
        assertThat(seenIds.add(id)).as("Duplicate id found: " + id).isTrue();
      }
    });
  }

  @Test
  void testPredicatePushdownWithAggregation() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (p:Product) WHERE p.status = 'active' RETURN p.category AS cat, count(p) AS cnt " +
              "ORDER BY cat");

      int groupCount = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        final long cnt = ((Number) r.getProperty("cnt")).longValue();
        assertThat(cnt).isGreaterThan(0);
        groupCount++;
      }
      assertThat(groupCount).isEqualTo(CATEGORY_COUNT);
    });
  }
}
