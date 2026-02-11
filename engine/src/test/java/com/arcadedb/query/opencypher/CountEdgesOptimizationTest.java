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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the OPTIONAL MATCH + count() → countEdges() optimization.
 * Verifies that the optimization produces correct results and falls back
 * to full materialization when the pattern is not optimizable.
 */
class CountEdgesOptimizationTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testcountedgesopt").create();
    database.transaction(() -> {
      database.getSchema().createVertexType("Question");
      database.getSchema().createVertexType("Comment");
      database.getSchema().createVertexType("Answer");
      database.getSchema().createEdgeType("COMMENTED_ON");
      database.getSchema().createEdgeType("ANSWERED");

      // Create questions
      final MutableVertex q1 = database.newVertex("Question").set("name", "q1").save();
      final MutableVertex q2 = database.newVertex("Question").set("name", "q2").save();
      final MutableVertex q3 = database.newVertex("Question").set("name", "q3").save();

      // Create comments on q1 (3 comments), q2 (1 comment), q3 (0 comments)
      for (int i = 0; i < 3; i++) {
        final MutableVertex c = database.newVertex("Comment").set("text", "c" + i).save();
        c.newEdge("COMMENTED_ON", q1);
      }
      final MutableVertex c3 = database.newVertex("Comment").set("text", "c3").save();
      c3.newEdge("COMMENTED_ON", q2);

      // Create answers on q1 (2 answers), q2 (0 answers)
      for (int i = 0; i < 2; i++) {
        final MutableVertex a = database.newVertex("Answer").set("text", "a" + i).save();
        a.newEdge("ANSWERED", q1);
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
  void basicOptionalMatchCount() {
    // Basic optimization: count comments per question
    final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question) OPTIONAL MATCH (c:Comment)-[:COMMENTED_ON]->(q) WITH q, count(c) AS cnt RETURN q.name AS name, cnt ORDER BY name");

    final Map<String, Long> results = collectNameCount(rs);
    assertThat(results).hasSize(3);
    assertThat(results.get("q1")).isEqualTo(3L);
    assertThat(results.get("q2")).isEqualTo(1L);
    assertThat(results.get("q3")).isEqualTo(0L);
  }

  @Test
  void noEdgesReturnsZero() {
    // q3 has no comments — should return 0
    final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question {name: 'q3'}) OPTIONAL MATCH (c:Comment)-[:COMMENTED_ON]->(q) WITH q, count(c) AS cnt RETURN cnt");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(0L);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void typedEdgeFilter() {
    // Count only COMMENTED_ON edges (not ANSWERED)
    final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question {name: 'q1'}) OPTIONAL MATCH (c:Comment)-[:COMMENTED_ON]->(q) WITH q, count(c) AS cnt RETURN cnt");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(3L);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void multiTypePattern() {
    // Count both COMMENTED_ON and ANSWERED edges using pipe syntax
    final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question {name: 'q1'}) OPTIONAL MATCH (x)-[:COMMENTED_ON|ANSWERED]->(q) WITH q, count(x) AS cnt RETURN cnt");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(5L);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void reverseDirection() {
    // OPTIONAL MATCH (q)<-[:COMMENTED_ON]-(c) — should produce same results as (c)-[:COMMENTED_ON]->(q)
    final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question) OPTIONAL MATCH (q)<-[:COMMENTED_ON]-(c:Comment) WITH q, count(c) AS cnt RETURN q.name AS name, cnt ORDER BY name");

    final Map<String, Long> results = collectNameCount(rs);
    assertThat(results).hasSize(3);
    assertThat(results.get("q1")).isEqualTo(3L);
    assertThat(results.get("q2")).isEqualTo(1L);
    assertThat(results.get("q3")).isEqualTo(0L);
  }

  @Test
  void bothDirection() {
    // Undirected: counts both in and out edges of the specified type
    final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question {name: 'q1'}) OPTIONAL MATCH (q)-[:COMMENTED_ON]-(c) WITH q, count(c) AS cnt RETURN cnt");

    assertThat(rs.hasNext()).isTrue();
    // q1 has 3 incoming COMMENTED_ON edges, 0 outgoing = 3 total
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(3L);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void whereClausePreventsOptimization() {
    // WHERE clause should prevent optimization, but still produce correct results via fallback
    final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question) OPTIONAL MATCH (c:Comment)-[:COMMENTED_ON]->(q) WHERE c.text = 'c0' WITH q, count(c) AS cnt RETURN q.name AS name, cnt ORDER BY name");

    final Map<String, Long> results = collectNameCount(rs);
    assertThat(results).hasSize(3);
    assertThat(results.get("q1")).isEqualTo(1L);
    assertThat(results.get("q2")).isEqualTo(0L);
    assertThat(results.get("q3")).isEqualTo(0L);
  }

  @Test
  void distinctPreventsOptimization() {
    // count(DISTINCT c) should prevent optimization but still produce correct results
    final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question) OPTIONAL MATCH (c:Comment)-[:COMMENTED_ON]->(q) WITH q, count(DISTINCT c) AS cnt RETURN q.name AS name, cnt ORDER BY name");

    final Map<String, Long> results = collectNameCount(rs);
    assertThat(results).hasSize(3);
    assertThat(results.get("q1")).isEqualTo(3L);
    assertThat(results.get("q2")).isEqualTo(1L);
    assertThat(results.get("q3")).isEqualTo(0L);
  }

  @Test
  void variableLengthPreventsOptimization() {
    // Variable-length path should prevent optimization but still produce correct results
    final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question {name: 'q1'}) OPTIONAL MATCH (c)-[:COMMENTED_ON*1..1]->(q) WITH q, count(c) AS cnt RETURN cnt");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(3L);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void multipleOptionalMatchCountChains() {
    // Integration test: multiple OPTIONAL MATCH + count chains (stackoverflow-like pattern)
    final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question) " +
        "OPTIONAL MATCH (c:Comment)-[:COMMENTED_ON]->(q) " +
        "WITH q, count(c) AS commentCount " +
        "OPTIONAL MATCH (a:Answer)-[:ANSWERED]->(q) " +
        "WITH q, commentCount, count(a) AS answerCount " +
        "RETURN q.name AS name, commentCount, answerCount ORDER BY name");

    assertThat(rs.hasNext()).isTrue();
    final Result r1 = rs.next();
    assertThat(r1.<String>getProperty("name")).isEqualTo("q1");
    assertThat(r1.<Long>getProperty("commentCount")).isEqualTo(3L);
    assertThat(r1.<Long>getProperty("answerCount")).isEqualTo(2L);

    assertThat(rs.hasNext()).isTrue();
    final Result r2 = rs.next();
    assertThat(r2.<String>getProperty("name")).isEqualTo("q2");
    assertThat(r2.<Long>getProperty("commentCount")).isEqualTo(1L);
    assertThat(r2.<Long>getProperty("answerCount")).isEqualTo(0L);

    assertThat(rs.hasNext()).isTrue();
    final Result r3 = rs.next();
    assertThat(r3.<String>getProperty("name")).isEqualTo("q3");
    assertThat(r3.<Long>getProperty("commentCount")).isEqualTo(0L);
    assertThat(r3.<Long>getProperty("answerCount")).isEqualTo(0L);

    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void countEdgesVarargs() {
    // Test the varargs countEdges API directly
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("Question").set("name", "varargs_test").save();
      for (int i = 0; i < 3; i++) {
        final MutableVertex c = database.newVertex("Comment").set("text", "vc" + i).save();
        c.newEdge("COMMENTED_ON", v);
      }
      for (int i = 0; i < 2; i++) {
        final MutableVertex a = database.newVertex("Answer").set("text", "va" + i).save();
        a.newEdge("ANSWERED", v);
      }

      // Test single type
      assertThat(v.countEdges(com.arcadedb.graph.Vertex.DIRECTION.IN, "COMMENTED_ON")).isEqualTo(3L);
      assertThat(v.countEdges(com.arcadedb.graph.Vertex.DIRECTION.IN, "ANSWERED")).isEqualTo(2L);

      // Test multiple types via varargs
      assertThat(v.countEdges(com.arcadedb.graph.Vertex.DIRECTION.IN, "COMMENTED_ON", "ANSWERED")).isEqualTo(5L);

      // Test no filter (all types)
      assertThat(v.countEdges(com.arcadedb.graph.Vertex.DIRECTION.IN)).isEqualTo(5L);
    });
  }

  @Test
  void boundVertexNotInGroupingKeysShouldNotOptimize() {
    // Regression test for issue #3399
    // When the bound vertex is NOT in the grouping keys and there are multiple
    // bound vertices per grouping key, the optimization must not apply because
    // CountEdgesStep doesn't aggregate - it emits one row per input row.
    //
    // Simplified test: Multiple answers per question, count comments per question
    // The bound vertex 'a' (answer) is NOT in the grouping keys

    // Use a fresh database for this test
    final Database testDb = new DatabaseFactory("./target/databases/testcountopt-issue3399").create();
    try {
      testDb.transaction(() -> {
        testDb.getSchema().createVertexType("Question");
        testDb.getSchema().createVertexType("Answer");
        testDb.getSchema().createVertexType("Comment");
        testDb.getSchema().createEdgeType("HAS_ANSWER");
        testDb.getSchema().createEdgeType("HAS_COMMENT");

        // Create 1 question
        final MutableVertex q1 = testDb.newVertex("Question").set("id", 1).save();

        // Create 2 answers for q1
        final MutableVertex a1 = testDb.newVertex("Answer").set("id", 1).save();
        final MutableVertex a2 = testDb.newVertex("Answer").set("id", 2).save();
        q1.newEdge("HAS_ANSWER", a1);
        q1.newEdge("HAS_ANSWER", a2);

        // Create 2 comments on a1, 3 comments on a2
        for (int i = 0; i < 2; i++) {
          final MutableVertex c = testDb.newVertex("Comment").set("id", 100 + i).save();
          a1.newEdge("HAS_COMMENT", c);
        }
        for (int i = 2; i < 5; i++) {
          final MutableVertex c = testDb.newVertex("Comment").set("id", 100 + i).save();
          a2.newEdge("HAS_COMMENT", c);
        }
      });

      // Simpler pattern that matches the optimization trigger:
      // MATCH produces 2 answers (a1, a2) for question q
      // OPTIONAL MATCH counts comments per answer
      // WITH groups by q (NOT by a!) and should aggregate counts
      final ResultSet rs = testDb.query("opencypher",
          "MATCH (q:Question)-[:HAS_ANSWER]->(a:Answer) " +
          "OPTIONAL MATCH (a)-[:HAS_COMMENT]->(c:Comment) " +
          "WITH q, count(c) AS comment_count " +
          "RETURN q.id AS qid, comment_count");

      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(r.<Integer>getProperty("qid")).isEqualTo(1);
      // Must aggregate across all answers: 2 + 3 = 5 total
      assertThat(r.<Long>getProperty("comment_count")).isEqualTo(5L);
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    } finally {
      testDb.drop();
    }
  }

  private Map<String, Long> collectNameCount(final ResultSet rs) {
    final Map<String, Long> results = new HashMap<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      results.put(r.getProperty("name"), r.getProperty("cnt"));
    }
    rs.close();
    return results;
  }
}
