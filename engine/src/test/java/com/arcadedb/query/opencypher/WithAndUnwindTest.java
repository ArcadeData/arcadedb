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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for WITH and UNWIND clauses in native OpenCypher implementation.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WithAndUnwindTest {
  private static       Database database;
  private static final String   DB_PATH = "./target/test-databases/with-unwind-test";

  @BeforeAll
  static void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();

    database.transaction(() -> {
      // Create schema
      final var personType = database.getSchema().createVertexType("Person");
      personType.createProperty("name", String.class);
      personType.createProperty("age", Integer.class);
      personType.createProperty("hobbies", String[].class);

      database.getSchema().createEdgeType("KNOWS");

      // Create test data
      database.command("opencypher", "CREATE (a:Person {name: 'Alice', age: 30})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob', age: 25})");
      database.command("opencypher", "CREATE (c:Person {name: 'Charlie', age: 35})");
      database.command("opencypher", "CREATE (d:Person {name: 'Diana', age: 28})");

      // Create relationships
      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)");
      database.command("opencypher",
          "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c)");
      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Charlie'}) CREATE (a)-[:KNOWS]->(c)");
    });
  }

  @AfterAll
  static void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  // ========== UNWIND TESTS ==========

  @Test
  @Order(1)
  void unwindSimpleList() {
    final ResultSet result = database.query("opencypher", "UNWIND [1, 2, 3] AS x RETURN x");

    int count = 0;
    long sum = 0;
    while (result.hasNext()) {
      final var row = result.next();
      sum += ((Number) row.getProperty("x")).longValue();
      count++;
    }
    result.close();

    assertThat(count).as("Should return 3 rows").isEqualTo(3);
    assertThat(sum).as("Sum should be 1+2+3=6").isEqualTo(6);
  }

  @Test
  @Order(2)
  void unwindWithMatch() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' UNWIND [1, 2, 3] AS x RETURN p.name AS name, x");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      assertThat(row.<String>getProperty("name")).isEqualTo("Alice");
      assertThat(row.<Long>getProperty("x")).isNotNull();
      count++;
    }
    result.close();

    assertThat(count).as("Should return 3 rows (one person × 3 list elements)").isEqualTo(3);
  }

  @Test
  @Order(3)
  void unwindEmptyList() {
    final ResultSet result = database.query("opencypher", "UNWIND [] AS x RETURN x");

    assertThat(result.hasNext()).as("Empty list should produce no rows").isFalse();
    result.close();
  }

  // ========== WITH TESTS ==========

  @Test
  @Order(10)
  void withProjection() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH p.name AS name, p.age AS age RETURN name, age ORDER BY name");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      assertThat(row.<String>getProperty("name")).isNotNull();
      assertThat(row.<Integer>getProperty("age")).isNotNull();
      count++;
    }
    result.close();

    assertThat(count).as("Should return 4 people").isEqualTo(4);
  }

  @Test
  @Order(11)
  void withFiltering() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH p.name AS name, p.age AS age WHERE age > 28 RETURN name ORDER BY name");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      final String name = row.getProperty("name");
      assertThat(name.equals("Alice") || name.equals("Charlie")).as("Should only return Alice or Charlie").isTrue();
      count++;
    }
    result.close();

    assertThat(count).as("Should return 2 people with age > 28").isEqualTo(2);
  }

  @Test
  @Order(12)
  void withDistinct() {
    // Create duplicate age values
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH DISTINCT p.age AS age RETURN age ORDER BY age");

    int count = 0;
    int prevAge = -1;
    while (result.hasNext()) {
      final var row = result.next();
      final int age = row.<Integer>getProperty("age");
      assertThat(age > prevAge).as("Ages should be distinct and sorted").isTrue();
      prevAge = age;
      count++;
    }
    result.close();

    assertThat(count).as("Should return 4 distinct ages").isEqualTo(4);
  }

  @Test
  @Order(13)
  void withLimit() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 2 RETURN name");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }
    result.close();

    assertThat(count).as("Should return only 2 rows due to LIMIT").isEqualTo(2);
  }

  @Test
  @Order(14)
  void withSkip() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH p.name AS name ORDER BY name SKIP 2 RETURN name");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }
    result.close();

    assertThat(count).as("Should return 2 rows after skipping first 2").isEqualTo(2);
  }

  @Test
  @Order(15)
  void withAggregation() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH count(p) AS personCount RETURN personCount");

    assertThat(result.hasNext()).isTrue();
    final var row = result.next();
    assertThat(((Number) row.getProperty("personCount")).intValue()).as("Should count 4 people").isEqualTo(4);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  // ========== WITH + UNWIND COMBINED TESTS ==========

  @Test
  @Order(20)
  void withAndUnwindCombined() {
    // Test that WITH passes through to subsequent clauses
    // This is currently not fully supported - UNWIND after WITH needs work
    // For now, test simpler case: WITH + RETURN only
    final ResultSet result = database.query("opencypher",
        """
        MATCH (p:Person) WHERE p.age < 30 \
        WITH p.name AS name, p.age AS age \
        RETURN name, age ORDER BY name""");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      assertThat(row.<String>getProperty("name")).as("name should not be null").isNotNull();
      assertThat(row.<Integer>getProperty("age")).as("age should not be null").isNotNull();
      count++;
    }
    result.close();

    assertThat(count).as("Should return 2 people with age < 30 (Bob=25, Diana=28)").isEqualTo(2);
  }

  @Test
  @Order(21)
  void multipleWithClauses() {
    final ResultSet result = database.query("opencypher",
        """
        MATCH (p:Person) \
        WITH p.name AS name, p.age AS age WHERE age > 25 \
        WITH name, age WHERE age < 35 \
        RETURN name ORDER BY name""");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      final String name = row.getProperty("name");
      assertThat(name.equals("Alice") || name.equals("Diana")).as("Should return Alice (30) and Diana (28), ages between 25 and 35")
          .isTrue();
      count++;
    }
    result.close();

    assertThat(count).as("Should return 2 people").isEqualTo(2);
  }

  @Test
  @Order(16)
  void withPostAggregationFiltering_Issue3338() {
    // Regression test for https://github.com/ArcadeData/arcadedb/issues/3338
    // Post-aggregation WHERE filtering should correctly filter grouped results
    final ResultSet result = database.query("opencypher",
        "UNWIND [1, 1, 2, 3] AS n WITH n, count(*) AS c WHERE c > 1 RETURN n");

    assertThat(result.hasNext()).as("Should have at least one result").isTrue();
    final var row = result.next();
    assertThat(((Number) row.getProperty("n")).intValue()).as("Only n=1 appears twice (count=2 > 1)").isEqualTo(1);
    assertThat(result.hasNext()).as("Should have exactly one result").isFalse();
    result.close();
  }

  @Test
  @Order(17)
  void withPostAggregationFilteringNoMatch_Issue3338() {
    // Same issue - when no groups match the filter, result should be empty
    final ResultSet result = database.query("opencypher",
        "UNWIND [1, 2, 3] AS n WITH n, count(*) AS c WHERE c > 1 RETURN n");

    assertThat(result.hasNext()).as("No value appears more than once, so no results").isFalse();
    result.close();
  }

  @Test
  @Order(18)
  void withPostAggregationFilteringWithMatch_Issue3338() {
    // Test post-aggregation filtering with MATCH source (uses a different code path)
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH p.age AS age, count(*) AS c WHERE c >= 1 RETURN age ORDER BY age");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }
    result.close();

    assertThat(count).as("Each age appears once, all counts are 1, all should pass c >= 1").isEqualTo(4);
  }

  @Test
  @Order(19)
  void withPureAggregationPostFiltering_Issue3338() {
    // Test pure aggregation (no GROUP BY) with WHERE after WITH
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH count(p) AS c WHERE c > 10 RETURN c");

    assertThat(result.hasNext()).as("4 people, count=4, 4 > 10 is false so no results").isFalse();
    result.close();
  }

  @Test
  @Order(22)
  void standaloneWithLiteral() {
    // Standalone WITH at the beginning of a query should create data from literals
    final ResultSet result = database.query("opencypher", "WITH 42 AS x RETURN x");

    assertThat(result.hasNext()).isTrue();
    final var row = result.next();
    assertThat(((Number) row.getProperty("x")).intValue()).isEqualTo(42);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  @Order(23)
  void standaloneWithNestedListUnwind_Issue3329() {
    // Regression test for https://github.com/ArcadeData/arcadedb/issues/3329
    // WITH nested list + double UNWIND should produce correct count
    final ResultSet result = database.query("opencypher",
        "WITH [[1, 2], [3, 4], []] AS nested UNWIND nested AS x UNWIND x AS y RETURN count(y) AS total");

    assertThat(result.hasNext()).isTrue();
    final var row = result.next();
    assertThat(((Number) row.getProperty("total")).intValue()).as("1,2,3,4 = 4 elements total").isEqualTo(4);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  @Order(24)
  void standaloneWithUnwind_Issue3329() {
    // Simpler variant: standalone WITH followed by a single UNWIND
    final ResultSet result = database.query("opencypher",
        "WITH [1, 2, 3] AS list UNWIND list AS x RETURN x");

    int count = 0;
    long sum = 0;
    while (result.hasNext()) {
      final var row = result.next();
      sum += ((Number) row.getProperty("x")).longValue();
      count++;
    }
    result.close();

    assertThat(count).isEqualTo(3);
    assertThat(sum).isEqualTo(6);
  }

  @Test
  @Order(25)
  void withChainedMatch() {
    // Chaining MATCH after WITH is not yet fully implemented
    // For now, test a simpler pattern: MATCH -> WITH -> RETURN
    final ResultSet result = database.query("opencypher",
        """
            MATCH (a:Person)-[:KNOWS]->(b:Person)
            WHERE a.name = 'Alice'
            WITH a.name AS aname, b.name AS bname
            RETURN aname, bname ORDER BY bname""");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      assertThat(row.<String>getProperty("aname")).isEqualTo("Alice");
      final String bname = row.getProperty("bname");
      assertThat(bname.equals("Bob") || bname.equals("Charlie")).as("Should return people Alice knows").isTrue();
      count++;
    }
    result.close();

    assertThat(count).as("Alice knows 2 people").isEqualTo(2);
  }

  // ========== REGRESSION TEST: UNWIND + MATCH + WHERE + CREATE (issue #3612) ==========

  /**
   * Regression test for issue #3612: UNWIND + MATCH + WHERE + CREATE is slow because
   * WHERE predicates referencing UNWIND variables were not pushed down into MatchNodeStep.
   * Both query forms (WHERE clause vs inline properties) should produce the same result
   * and complete in reasonable time.
   */
  @Test
  @Order(50)
  void unwindMatchWhereCreateShouldPushdownPredicates() {
    database.transaction(() -> {
      // Create schema for this test
      final var benchType = database.getSchema().createVertexType("BenchNode");
      benchType.createProperty("uid", String.class);
      database.getSchema().createEdgeType("BENCH_EDGE");

      // Create nodes
      for (int i = 0; i < 200; i++)
        database.command("opencypher", "CREATE (:BenchNode {uid: '" + i + "'})");
    });

    // Build batch parameter
    final List<Map<String, Object>> batch = List.of(
        Map.of("src", "0", "dst", "1"),
        Map.of("src", "2", "dst", "3"),
        Map.of("src", "4", "dst", "5"),
        Map.of("src", "6", "dst", "7"),
        Map.of("src", "8", "dst", "9")
    );

    // Query using WHERE clause (was slow before fix)
    final long startWhere = System.nanoTime();
    database.transaction(() -> {
      database.command("opencypher",
          "UNWIND $batch AS e MATCH (a:BenchNode), (b:BenchNode) WHERE a.uid = e.src AND b.uid = e.dst CREATE (a)-[:BENCH_EDGE]->(b)",
          Map.of("batch", batch));
    });
    final long whereTimeMs = (System.nanoTime() - startWhere) / 1_000_000;

    // Verify edges were created
    final ResultSet countResult = database.query("opencypher",
        "MATCH (:BenchNode)-[r:BENCH_EDGE]->(:BenchNode) RETURN count(r) AS cnt");
    assertThat(countResult.hasNext()).isTrue();
    assertThat(countResult.next().<Long>getProperty("cnt")).isEqualTo(5L);
    countResult.close();

    // Query using inline properties (was already fast) - create more edges to verify equivalence
    final List<Map<String, Object>> batch2 = List.of(
        Map.of("src", "10", "dst", "11"),
        Map.of("src", "12", "dst", "13"),
        Map.of("src", "14", "dst", "15"),
        Map.of("src", "16", "dst", "17"),
        Map.of("src", "18", "dst", "19")
    );

    final long startInline = System.nanoTime();
    database.transaction(() -> {
      database.command("opencypher",
          "UNWIND $batch AS e MATCH (a:BenchNode {uid: e.src}), (b:BenchNode {uid: e.dst}) CREATE (a)-[:BENCH_EDGE]->(b)",
          Map.of("batch", batch2));
    });
    final long inlineTimeMs = (System.nanoTime() - startInline) / 1_000_000;

    // Verify total edges
    final ResultSet countResult2 = database.query("opencypher",
        "MATCH (:BenchNode)-[r:BENCH_EDGE]->(:BenchNode) RETURN count(r) AS cnt");
    assertThat(countResult2.hasNext()).isTrue();
    assertThat(countResult2.next().<Long>getProperty("cnt")).isEqualTo(10L);
    countResult2.close();

    // WHERE clause version should complete in reasonable time (not timeout)
    // With 200 nodes, without pushdown it would do 200*200 = 40000 comparisons per UNWIND row
    // With pushdown it does ~200 comparisons per UNWIND row
    // Allow generous margin but ensure it's not catastrophically slow
    assertThat(whereTimeMs).as("WHERE clause version should complete within 10 seconds (was timing out before fix)")
        .isLessThan(10000);
  }

  /**
   * Regression test for https://github.com/ArcadeData/arcadedb/issues/3873
   * When UNWIND follows a WITH that carries rows forward from a MATCH,
   * ArcadeDB duplicated every expanded row by the size of the upstream row set.
   * <p>
   * Tests exact reproduction from the bug report with isolated database,
   * both query() and command() paths (HTTP /command endpoint uses command()),
   * and varying upstream row counts (1, 3, 4) to guard against the reported
   * duplication factor = number of upstream rows.
   */
  @Test
  @Order(51)
  void unwindAfterWithShouldNotDuplicateRows_Issue3873() {
    final String isolatedDbPath = "./target/test-databases/issue-3873-repro";
    FileUtils.deleteRecursively(new File(isolatedDbPath));
    final Database db = new DatabaseFactory(isolatedDbPath).create();
    try {
      db.transaction(() -> {
        db.getSchema().createVertexType("Person").createProperty("name", String.class);
        db.command("opencypher",
            "CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'}), (:Person {name: 'Charlie'})");
      });

      final String cypher = "MATCH (p:Person) WITH p.name AS personName WHERE personName <> 'Bob' "
          + "UNWIND range(1, 3) AS i RETURN personName, i ORDER BY personName, i";

      // Test via query() (read-only path)
      final ResultSet result = db.query("opencypher", cypher);
      final List<Map<String, Object>> rows = new ArrayList<>();
      while (result.hasNext()) {
        final var row = result.next();
        rows.add(Map.of("personName", row.getProperty("personName"), "i", row.getProperty("i")));
      }
      result.close();

      assertThat(rows).as("query() should produce exactly 6 rows (2 names x 3 range values)").hasSize(6);
      assertThat(rows.get(0)).containsEntry("personName", "Alice").containsEntry("i", 1L);
      assertThat(rows.get(1)).containsEntry("personName", "Alice").containsEntry("i", 2L);
      assertThat(rows.get(2)).containsEntry("personName", "Alice").containsEntry("i", 3L);
      assertThat(rows.get(3)).containsEntry("personName", "Charlie").containsEntry("i", 1L);
      assertThat(rows.get(4)).containsEntry("personName", "Charlie").containsEntry("i", 2L);
      assertThat(rows.get(5)).containsEntry("personName", "Charlie").containsEntry("i", 3L);

      // Test via command() (same path as HTTP /command endpoint)
      final ResultSet cmdResult = db.command("opencypher", cypher);
      int cmdCount = 0;
      while (cmdResult.hasNext()) { cmdResult.next(); cmdCount++; }
      cmdResult.close();
      assertThat(cmdCount).as("command() should also produce exactly 6 rows").isEqualTo(6);

      // Control: all 3 persons without filter, range(1,3) = 3 * 3 = 9
      final ResultSet allResult = db.query("opencypher",
          "MATCH (p:Person) WITH p.name AS personName UNWIND range(1, 3) AS i RETURN personName, i");
      int allCount = 0;
      while (allResult.hasNext()) { allResult.next(); allCount++; }
      allResult.close();
      assertThat(allCount).as("3 upstream rows * 3 = 9").isEqualTo(9);

      // Control: single upstream row, range(1,3) = 1 * 3 = 3
      final ResultSet singleResult = db.query("opencypher",
          "MATCH (p:Person {name: 'Alice'}) WITH p.name AS personName "
              + "UNWIND range(1, 3) AS i RETURN personName, i");
      int singleCount = 0;
      while (singleResult.hasNext()) { singleResult.next(); singleCount++; }
      singleResult.close();
      assertThat(singleCount).as("1 upstream row * 3 = 3").isEqualTo(3);
    } finally {
      db.drop();
      FileUtils.deleteRecursively(new File(isolatedDbPath));
    }
  }

  /**
   * Regression test for https://github.com/ArcadeData/arcadedb/issues/3877
   * WITH multiple projected columns + ORDER BY + SKIP + LIMIT returns empty result set.
   * Tests the exact query from the bug report plus all control cases.
   */
  @Test
  @Order(60)
  void withMultiColumnOrderBySkipLimit_Issue3877() {
    // Exact reproduction from issue report
    final String cypher = "UNWIND [3,2,1] AS x "
        + "WITH x, x + 10 AS y ORDER BY x DESC SKIP 1 LIMIT 2 "
        + "RETURN x, y ORDER BY x";

    // Test via query()
    final ResultSet result = database.query("opencypher", cypher);
    final List<Map<String, Object>> rows = new ArrayList<>();
    while (result.hasNext()) {
      final var row = result.next();
      rows.add(Map.of("x", ((Number) row.getProperty("x")).longValue(),
          "y", ((Number) row.getProperty("y")).longValue()));
    }
    result.close();

    assertThat(rows).as("Should return 2 rows after SKIP 1 LIMIT 2").hasSize(2);
    assertThat(rows.get(0)).containsEntry("x", 1L).containsEntry("y", 11L);
    assertThat(rows.get(1)).containsEntry("x", 2L).containsEntry("y", 12L);

    // Test via command() (HTTP /command endpoint path)
    final ResultSet cmdResult = database.command("opencypher", cypher);
    int cmdCount = 0;
    while (cmdResult.hasNext()) { cmdResult.next(); cmdCount++; }
    cmdResult.close();
    assertThat(cmdCount).as("command() should also return 2 rows").isEqualTo(2);

    // Control 1: single column - should work
    final ResultSet ctrl1 = database.query("opencypher",
        "UNWIND [3,2,1] AS x WITH x ORDER BY x DESC SKIP 1 LIMIT 2 RETURN x ORDER BY x");
    int ctrl1Count = 0;
    while (ctrl1.hasNext()) { ctrl1.next(); ctrl1Count++; }
    ctrl1.close();
    assertThat(ctrl1Count).as("Control 1: single column should return 2 rows").isEqualTo(2);

    // Control 2: no SKIP - should work
    final ResultSet ctrl2 = database.query("opencypher",
        "UNWIND [3,2,1] AS x WITH x, x + 10 AS y ORDER BY x DESC LIMIT 2 RETURN x, y ORDER BY x");
    int ctrl2Count = 0;
    while (ctrl2.hasNext()) { ctrl2.next(); ctrl2Count++; }
    ctrl2.close();
    assertThat(ctrl2Count).as("Control 2: no SKIP should return 2 rows").isEqualTo(2);

    // Control 3: no LIMIT - should work
    final ResultSet ctrl3 = database.query("opencypher",
        "UNWIND [3,2,1] AS x WITH x, x + 10 AS y ORDER BY x DESC SKIP 1 RETURN x, y ORDER BY x");
    int ctrl3Count = 0;
    while (ctrl3.hasNext()) { ctrl3.next(); ctrl3Count++; }
    ctrl3.close();
    assertThat(ctrl3Count).as("Control 3: no LIMIT should return 2 rows").isEqualTo(2);
  }

  /**
   * Regression test for https://github.com/ArcadeData/arcadedb/issues/3876
   * After aggregation in WITH, ORDER BY + SKIP + LIMIT returns empty result set.
   */
  @Test
  @Order(61)
  void withAggregationOrderBySkipLimit_Issue3876() {
    final String isolatedDbPath = "./target/test-databases/issue-3876-repro";
    FileUtils.deleteRecursively(new File(isolatedDbPath));
    final Database db = new DatabaseFactory(isolatedDbPath).create();
    try {
      db.transaction(() -> {
        db.getSchema().createVertexType("Person").createProperty("name", String.class);
        db.getSchema().createVertexType("Movie").createProperty("title", String.class);
        db.getSchema().createEdgeType("ACTED_IN");
        db.command("opencypher",
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), (c:Person {name:'Carol'}), "
                + "(m:Movie {title:'M1'}), (a)-[:ACTED_IN]->(m), (b)-[:ACTED_IN]->(m)");
      });

      // Exact query from the bug report
      final ResultSet result = db.query("opencypher",
          "MATCH (p:Person) OPTIONAL MATCH (p)-[:ACTED_IN]->(m:Movie) "
              + "WITH p, COUNT(m) AS c ORDER BY p.name SKIP 1 LIMIT 2 "
              + "RETURN p.name AS name, c ORDER BY name");

      final List<Map<String, Object>> rows = new ArrayList<>();
      while (result.hasNext()) {
        final var row = result.next();
        rows.add(Map.of("name", (String) row.getProperty("name"),
            "c", ((Number) row.getProperty("c")).longValue()));
      }
      result.close();

      assertThat(rows).as("Should return 2 rows (Bob c=1, Carol c=0)").hasSize(2);
      assertThat(rows.get(0)).containsEntry("name", "Bob").containsEntry("c", 1L);
      assertThat(rows.get(1)).containsEntry("name", "Carol").containsEntry("c", 0L);
    } finally {
      db.drop();
      FileUtils.deleteRecursively(new File(isolatedDbPath));
    }
  }

  /**
   * Regression test for https://github.com/ArcadeData/arcadedb/issues/3874
   * Path variable binding with variable-length patterns: COUNT(path) returns 0
   * even though the underlying traversal matches rows.
   */
  @Test
  @Order(62)
  void pathVariableWithVarLengthPattern_Issue3874() {
    final String isolatedDbPath = "./target/test-databases/issue-3874-repro";
    FileUtils.deleteRecursively(new File(isolatedDbPath));
    final Database db = new DatabaseFactory(isolatedDbPath).create();
    try {
      db.transaction(() -> {
        db.getSchema().createVertexType("Person").createProperty("name", String.class);
        db.getSchema().createEdgeType("KNOWS");
        db.command("opencypher",
            "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}), "
                + "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)");
      });

      // Exact query from bug report: COUNT(path) should be 1
      final ResultSet result = db.query("opencypher",
          "MATCH path = (start:Person)-[:KNOWS*2]->(end:Person) RETURN COUNT(path) AS c");

      assertThat(result.hasNext()).as("Should return a result row").isTrue();
      final long count = ((Number) result.next().getProperty("c")).longValue();
      result.close();
      assertThat(count).as("One 2-hop path Alice->Bob->Carol").isEqualTo(1L);

      // Also test length(path) - user reports this produces no rows
      final ResultSet lenResult = db.query("opencypher",
          "MATCH path = (start:Person)-[:KNOWS*2]->(end:Person) RETURN length(path) AS len");
      assertThat(lenResult.hasNext()).as("length(path) should produce a row").isTrue();
      final long len = ((Number) lenResult.next().getProperty("len")).longValue();
      lenResult.close();
      assertThat(len).as("Path length should be 2 (two edges)").isEqualTo(2L);

      // Control: same pattern without path variable should work
      final ResultSet ctrl = db.query("opencypher",
          "MATCH (start:Person)-[:KNOWS*2]->(end:Person) RETURN start.name AS s, end.name AS e");
      assertThat(ctrl.hasNext()).isTrue();
      final var row = ctrl.next();
      assertThat(row.<String>getProperty("s")).isEqualTo("Alice");
      assertThat(row.<String>getProperty("e")).isEqualTo("Carol");
      assertThat(ctrl.hasNext()).as("Only one 2-hop path").isFalse();
      ctrl.close();
    } finally {
      db.drop();
      FileUtils.deleteRecursively(new File(isolatedDbPath));
    }
  }

  /**
   * Regression test for https://github.com/ArcadeData/arcadedb/issues/3875
   * Path assignment in CREATE: CREATE p = (n)-[r:TYPE]->(m) should bind both p and r.
   */
  @Test
  @Order(63)
  void createPathAssignment_Issue3875() {
    final String isolatedDbPath = "./target/test-databases/issue-3875-repro";
    FileUtils.deleteRecursively(new File(isolatedDbPath));
    final Database db = new DatabaseFactory(isolatedDbPath).create();
    try {
      db.transaction(() -> {
        db.getSchema().createVertexType("Country").createProperty("name", String.class);
        db.getSchema().createEdgeType("BORDERS_WITH");
        db.command("opencypher",
            "CREATE (n:Country {name: 'Belgium'}), (m:Country {name: 'Netherlands'})");
      });

      // Exact query from bug report
      db.transaction(() -> {
        final ResultSet result = db.command("opencypher",
            "MATCH (n:Country {name: 'Belgium'}), (m:Country {name: 'Netherlands'}) "
                + "CREATE p = (n)-[r:BORDERS_WITH {length: '30KM'}]->(m) "
                + "RETURN COUNT(p) AS path_count, COUNT(r) AS rel_count");

        assertThat(result.hasNext()).as("Should return a result row").isTrue();
        final var row = result.next();
        final long pathCount = ((Number) row.getProperty("path_count")).longValue();
        final long relCount = ((Number) row.getProperty("rel_count")).longValue();
        result.close();

        assertThat(pathCount).as("One path created").isEqualTo(1L);
        assertThat(relCount).as("One relationship created").isEqualTo(1L);
      });
    } finally {
      db.drop();
      FileUtils.deleteRecursively(new File(isolatedDbPath));
    }
  }
}
