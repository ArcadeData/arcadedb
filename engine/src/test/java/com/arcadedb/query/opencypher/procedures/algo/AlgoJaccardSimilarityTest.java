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
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for the algo.jaccard Cypher procedure (Jaccard Similarity).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoJaccardSimilarityTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-jaccard");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Graph (OUT edges):
    // A→B, A→C, A→D  (A's OUT-neighbors: {B,C,D})
    // E→B, E→C       (E's OUT-neighbors: {B,C})
    // Jaccard(A,E) = |{B,C}| / |{B,C,D}| = 2/3 ≈ 0.667
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      a.newEdge("LINK", c, true, (Object[]) null).save();
      a.newEdge("LINK", d, true, (Object[]) null).save();
      e.newEdge("LINK", b, true, (Object[]) null).save();
      e.newEdge("LINK", c, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void jaccardFindsSimilarNode() {
    // Source=A, direction=OUT, cutoff=0.0 → should find E with similarity≈0.667
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.jaccard(a, null, 'OUT', 0.0) " +
            "YIELD node1, node2, similarity " +
            "RETURN node1, node2, similarity");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    // Should have at least one result (E with similarity≈0.667)
    assertThat(results).isNotEmpty();

    // Find the result with highest similarity — should be E
    boolean foundE = false;
    for (final Result r : results) {
      final double similarity = ((Number) r.getProperty("similarity")).doubleValue();
      if (similarity > 0.6) {
        assertThat(similarity).isCloseTo(2.0 / 3.0, within(0.01));
        foundE = true;
      }
    }
    assertThat(foundE).isTrue();
  }

  @Test
  void jaccardCutoffFiltersResults() {
    // cutoff=0.7: jaccard(A,E)=0.667 ≤ 0.7 → no results returned
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.jaccard(a, null, 'OUT', 0.7) " +
            "YIELD node1, node2, similarity " +
            "RETURN node1, node2, similarity");

    assertThat(rs.hasNext()).isFalse();
  }
}
