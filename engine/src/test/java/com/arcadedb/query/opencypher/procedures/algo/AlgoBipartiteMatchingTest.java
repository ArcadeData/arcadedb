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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.bipartiteMatching Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoBipartiteMatchingTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-bipartitematching");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Left");
    database.getSchema().createVertexType("Right");
    database.getSchema().createEdgeType("MATCHES");

    // Bipartite graph:
    // L1 - R1, R2
    // L2 - R1
    // L3 - R2, R3
    // Maximum matching: L1-R1 (or R2), L2-R1, L3-R3 → size 3
    // Actually: L1→R2, L2→R1, L3→R3 is a perfect matching of size 3
    database.transaction(() -> {
      final MutableVertex l1 = database.newVertex("Left").set("name", "L1").save();
      final MutableVertex l2 = database.newVertex("Left").set("name", "L2").save();
      final MutableVertex l3 = database.newVertex("Left").set("name", "L3").save();
      final MutableVertex r1 = database.newVertex("Right").set("name", "R1").save();
      final MutableVertex r2 = database.newVertex("Right").set("name", "R2").save();
      final MutableVertex r3 = database.newVertex("Right").set("name", "R3").save();
      l1.newEdge("MATCHES", r1, true, (Object[]) null).save();
      l1.newEdge("MATCHES", r2, true, (Object[]) null).save();
      l2.newEdge("MATCHES", r1, true, (Object[]) null).save();
      l3.newEdge("MATCHES", r2, true, (Object[]) null).save();
      l3.newEdge("MATCHES", r3, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void bipartiteMatchingReturnsMatchedPairs() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.bipartiteMatching() YIELD node1, node2, matchingSize RETURN node1, node2, matchingSize");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).isNotEmpty();
    // All rows should report the same matchingSize
    final int matchingSize = ((Number) results.getFirst().getProperty("matchingSize")).intValue();
    assertThat(matchingSize).isGreaterThan(0);
    for (final Result r : results)
      assertThat(((Number) r.getProperty("matchingSize")).intValue()).isEqualTo(matchingSize);
  }

  @Test
  void bipartiteMatchingMaxSize() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.bipartiteMatching() YIELD node1, node2, matchingSize RETURN matchingSize LIMIT 1");

    assertThat(rs.hasNext()).isTrue();
    final int matchingSize = ((Number) rs.next().getProperty("matchingSize")).intValue();
    // Maximum matching should be 3 (L1-R2, L2-R1, L3-R3)
    assertThat(matchingSize).isEqualTo(3);
  }

  @Test
  void bipartiteMatchingOnNonBipartiteGraphReturnsEmpty() {
    // Create a triangle (non-bipartite)
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-bipartitematching-nonbip");
    if (factory.exists())
      factory.open().drop();
    final Database nonBipDb = factory.create();
    try {
      nonBipDb.getSchema().createVertexType("Node");
      nonBipDb.getSchema().createEdgeType("EDGE");
      nonBipDb.transaction(() -> {
        final MutableVertex a = nonBipDb.newVertex("Node").set("name", "A").save();
        final MutableVertex b = nonBipDb.newVertex("Node").set("name", "B").save();
        final MutableVertex c = nonBipDb.newVertex("Node").set("name", "C").save();
        a.newEdge("EDGE", b, true, (Object[]) null).save();
        b.newEdge("EDGE", c, true, (Object[]) null).save();
        c.newEdge("EDGE", a, true, (Object[]) null).save();
      });

      final ResultSet rs = nonBipDb.query("opencypher",
          "CALL algo.bipartiteMatching() YIELD node1, node2 RETURN node1");
      assertThat(rs.hasNext()).isFalse();
    } finally {
      nonBipDb.drop();
    }
  }
}
