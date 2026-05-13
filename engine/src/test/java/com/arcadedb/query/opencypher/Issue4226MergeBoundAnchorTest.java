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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4226.
 * <p>
 * When MERGE includes an edge to a vertex already bound by a preceding clause
 * (e.g. MATCH or UNWIND), the engine must use the bound vertex as a traversal
 * anchor and walk its incident edges (O(degree)) instead of scanning every
 * edge of the relevant type (O(edges)).  Correctness tests check the result
 * shape; the {@code @Tag("slow")} test verifies the run-time scales with the
 * anchor's degree, not the global edge count.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4226MergeBoundAnchorTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-4226-merge-bound-anchor").create();
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createEdgeType("in");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Baseline correctness: MERGE with bound anchor creates a new chunk and edge
   * when the pattern does not exist, and only one new chunk is created.
   */
  @Test
  void mergeWithBoundAnchorCreatesWhenMissing() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:DOCUMENT {name: 'parentA'})");
      database.command("opencypher", "CREATE (:DOCUMENT {name: 'parentB'})");

      database.command("opencypher",
          "MATCH (b:DOCUMENT {name:'parentB'}) "
              + "UNWIND range(1, 5) AS i "
              + "CREATE (c:CHUNK {name:'B_'+toString(i)}) "
              + "CREATE (c)-[:in]->(b)");
    });

    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (a:DOCUMENT {name:'parentA'}) "
              + "MERGE (n:CHUNK {name:'A_only'})-[:in]->(a)");
    });

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:CHUNK {name:'A_only'})-[:in]->(a:DOCUMENT {name:'parentA'}) RETURN count(n) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);

    // The unrelated chunks under parentB must remain intact.
    final ResultSet total = database.query("opencypher", "MATCH (n:CHUNK) RETURN count(n) AS cnt");
    assertThat(total.next().<Number>getProperty("cnt").longValue()).isEqualTo(6L);
  }

  /**
   * MERGE with bound anchor must NOT match nodes that satisfy the chunk pattern
   * but are connected to a different parent.  Even though property values
   * coincide, the edge anchor scopes the match.
   */
  @Test
  void mergeWithBoundAnchorDoesNotMatchNoiseUnderOtherParent() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:DOCUMENT {name: 'parentA'})");
      database.command("opencypher", "CREATE (:DOCUMENT {name: 'parentB'})");

      // Identical CHUNK property values, but anchored to parentB.
      database.command("opencypher",
          "MATCH (b:DOCUMENT {name:'parentB'}) "
              + "CREATE (c:CHUNK {name:'shared', subtype:'CHUNK'})-[:in]->(b)");
    });

    database.transaction(() -> {
      // Should NOT match the parentB-attached chunk; should create a new one under parentA.
      database.command("opencypher",
          "MATCH (a:DOCUMENT {name:'parentA'}) "
              + "MERGE (n:CHUNK {name:'shared', subtype:'CHUNK'})-[:in]->(a)");
    });

    // Two CHUNK nodes exist now: the original one under parentB and the new one under parentA.
    final ResultSet total = database.query("opencypher", "MATCH (n:CHUNK) RETURN count(n) AS cnt");
    assertThat(total.next().<Number>getProperty("cnt").longValue()).isEqualTo(2L);

    final ResultSet underA = database.query("opencypher",
        "MATCH (n:CHUNK)-[:in]->(:DOCUMENT {name:'parentA'}) RETURN count(n) AS cnt");
    assertThat(underA.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);

    final ResultSet underB = database.query("opencypher",
        "MATCH (n:CHUNK)-[:in]->(:DOCUMENT {name:'parentB'}) RETURN count(n) AS cnt");
    assertThat(underB.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);
  }

  /**
   * MERGE with a bound anchor must MATCH (not create) when an edge from the
   * anchor to a matching chunk already exists.  The number of chunks must not
   * grow on a re-run.
   */
  @Test
  void mergeWithBoundAnchorMatchesExistingChunk() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:DOCUMENT {name: 'parentA'})");

      database.command("opencypher",
          "MATCH (a:DOCUMENT {name:'parentA'}) "
              + "CREATE (c:CHUNK {name:'existing'})-[:in]->(a)");
    });

    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (a:DOCUMENT {name:'parentA'}) "
              + "MERGE (n:CHUNK {name:'existing'})-[:in]->(a)");
    });

    final ResultSet rs = database.query("opencypher", "MATCH (n:CHUNK) RETURN count(n) AS cnt");
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);

    final ResultSet edges = database.query("opencypher",
        "MATCH (n:CHUNK)-[r:in]->(:DOCUMENT) RETURN count(r) AS cnt");
    assertThat(edges.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);
  }

  /**
   * MERGE bound by the other endpoint: (bound)-[:in]->(unbound).  Direction
   * inverted with respect to the previous tests to ensure both orientations
   * are anchored correctly.
   */
  @Test
  void mergeWithBoundAnchorIncomingDirection() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:DOCUMENT {name: 'src'})");
      // Pre-existing chunk pointed to from src.
      database.command("opencypher",
          "MATCH (s:DOCUMENT {name:'src'}) "
              + "CREATE (s)-[:in]->(c:CHUNK {name:'target'})");
    });

    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (s:DOCUMENT {name:'src'}) "
              + "MERGE (s)-[:in]->(n:CHUNK {name:'target'})");
    });

    // Must MATCH (not create) the existing chunk.
    final ResultSet rs = database.query("opencypher", "MATCH (n:CHUNK) RETURN count(n) AS cnt");
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);
  }

  /**
   * UNWIND + MERGE pattern as reported in the issue: per-iteration MERGE must
   * match-or-create exactly one chunk per row, even with noise edges of the
   * same type pointing to other parents.
   */
  @Test
  void mergeWithBoundAnchorInsideUnwindCreatesOnePerRow() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:DOCUMENT {name:'parentA'})");
      database.command("opencypher", "CREATE (:DOCUMENT {name:'parentB'})");

      // 50 noise edges under parentB.
      database.command("opencypher",
          "MATCH (b:DOCUMENT {name:'parentB'}) "
              + "UNWIND range(1,50) AS i "
              + "CREATE (c:CHUNK {name:'noise_'+toString(i)})-[:in]->(b)");
    });

    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (a:DOCUMENT {name:'parentA'}) "
              + "UNWIND range(1,10) AS i "
              + "MERGE (n:CHUNK {name:'A_'+toString(i)})-[:in]->(a)");
    });

    final ResultSet underA = database.query("opencypher",
        "MATCH (n:CHUNK)-[:in]->(:DOCUMENT {name:'parentA'}) RETURN count(n) AS cnt");
    assertThat(underA.next().<Number>getProperty("cnt").longValue()).isEqualTo(10L);

    final ResultSet underB = database.query("opencypher",
        "MATCH (n:CHUNK)-[:in]->(:DOCUMENT {name:'parentB'}) RETURN count(n) AS cnt");
    assertThat(underB.next().<Number>getProperty("cnt").longValue()).isEqualTo(50L);

    // Re-run MERGE: nothing new should be created.
    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (a:DOCUMENT {name:'parentA'}) "
              + "UNWIND range(1,10) AS i "
              + "MERGE (n:CHUNK {name:'A_'+toString(i)})-[:in]->(a)");
    });

    final ResultSet total = database.query("opencypher", "MATCH (n:CHUNK) RETURN count(n) AS cnt");
    assertThat(total.next().<Number>getProperty("cnt").longValue()).isEqualTo(60L);
  }

  /**
   * Performance regression test: MERGE with a bound anchor must scale with the
   * anchor's edge degree, not the total number of edges of the relevant type.
   * The buggy implementation iterated every edge of the relationship type
   * before applying the anchor constraint; the per-MERGE elapsed time grew
   * linearly with the noise.  After the fix the cost only depends on the
   * anchor's degree, so a batch of 20 MERGEs against an anchor with no edges
   * must complete in roughly the time it takes to walk a few empty edge
   * lists, regardless of how many unrelated edges of the same type exist.
   */
  @Test
  @Tag("slow")
  void mergeWithBoundAnchorScalesWithAnchorDegree() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:DOCUMENT {name:'parentA'})");
      database.command("opencypher", "CREATE (:DOCUMENT {name:'parentB'})");

      // Large noise under parentB to exaggerate the cost of a full edge-type scan.
      database.command("opencypher",
          "MATCH (b:DOCUMENT {name:'parentB'}) "
              + "UNWIND range(1,200000) AS i "
              + "CREATE (c:CHUNK {name:'noise_'+toString(i)})-[:in]->(b)");
    });

    // Run a batch of MERGEs scoped to parentA; each must NOT touch parentB's edges.
    final long start = System.nanoTime();
    database.transaction(() -> {
      for (int i = 0; i < 30; i++) {
        database.command("opencypher",
            "MATCH (a:DOCUMENT {name:'parentA'}) "
                + "MERGE (n:CHUNK {name:'A_unique_" + i + "'})-[:in]->(a)");
      }
    });
    final long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

    final ResultSet underA = database.query("opencypher",
        "MATCH (n:CHUNK)-[:in]->(:DOCUMENT {name:'parentA'}) RETURN count(n) AS cnt");
    assertThat(underA.next().<Number>getProperty("cnt").longValue()).isEqualTo(30L);

    // With 200k noise edges of type :in the buggy implementation does roughly
    // 30 * 200,000 = 6M edge-property iterations on the hot path - several
    // seconds on the observed CI hardware.  The optimized walker only touches
    // the anchor's 0..N incoming edges per MERGE and finishes in well under
    // 1 s.  The 2 s threshold is loose enough to absorb GC pauses yet tight
    // enough to catch a regression to the buggy O(edges-of-type) path.
    assertThat(elapsedMs)
        .as("MERGE with bound anchor should not scale with noise edge count (elapsed: " + elapsedMs + "ms)")
        .isLessThan(2000L);
  }

  /**
   * Sanity check: the existing "match by bound first" pattern (a)-[:in]->(n)
   * with anchor on the source already works in the previous implementation
   * because the walker starts at index 0; this test guards against regression
   * when the fix is applied.
   */
  @Test
  void mergeWithBoundAnchorAtStartStillWorks() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:DOCUMENT {name:'src'})");
      database.command("opencypher",
          "MATCH (s:DOCUMENT {name:'src'}) "
              + "CREATE (s)-[:in]->(:CHUNK {name:'pre'})");
    });

    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (s:DOCUMENT {name:'src'}) "
              + "MERGE (s)-[:in]->(n:CHUNK {name:'pre'}) "
              + "RETURN n.name AS name");
    });

    final ResultSet rs = database.query("opencypher", "MATCH (n:CHUNK) RETURN count(n) AS cnt");
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);
  }

  /**
   * Verifies that when the MERGE pattern returns the bound anchor in a path,
   * the returned vertex variables still resolve to the right identities.
   */
  @Test
  void mergeWithBoundAnchorReturnsBoundVariable() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:DOCUMENT {name:'parentA'})");
    });

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (a:DOCUMENT {name:'parentA'}) "
              + "MERGE (n:CHUNK {name:'A_1'})-[:in]->(a) "
              + "RETURN a.name AS parent_name, n.name AS chunk_name");
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(r.<String>getProperty("parent_name")).isEqualTo("parentA");
      assertThat(r.<String>getProperty("chunk_name")).isEqualTo("A_1");
    });
  }

  /**
   * Three-node path with the bound anchor in the middle.  Ensures the new
   * bidirectional walker handles interior anchors and matches the expected
   * existing path without creating duplicates.
   */
  @Test
  void mergeWithBoundAnchorInMiddleOfPath() {
    database.transaction(() -> {
      database.getSchema().createVertexType("A");
      database.getSchema().createVertexType("B");
      database.getSchema().createVertexType("C");
      database.getSchema().createEdgeType("R1");
      database.getSchema().createEdgeType("R2");

      database.command("opencypher", "CREATE (a:A {name:'a1'})-[:R1]->(b:B {name:'b1'})-[:R2]->(c:C {name:'c1'})");
      // Noise on B not connected via R1/R2 to anything else.
      database.command("opencypher", "CREATE (:B {name:'b2'})");
    });

    database.transaction(() -> {
      // Bound anchor is b in the middle; the path should match the existing chain.
      database.command("opencypher",
          "MATCH (b:B {name:'b1'}) "
              + "MERGE (a:A {name:'a1'})-[:R1]->(b)-[:R2]->(c:C {name:'c1'})");
    });

    final ResultSet rs = database.query("opencypher", "MATCH (b:B) RETURN count(b) AS cnt");
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(2L);

    final ResultSet aCount = database.query("opencypher", "MATCH (a:A) RETURN count(a) AS cnt");
    assertThat(aCount.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);

    final ResultSet cCount = database.query("opencypher", "MATCH (c:C) RETURN count(c) AS cnt");
    assertThat(cCount.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);
  }
}
