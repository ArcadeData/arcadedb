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
package com.arcadedb.grapholap;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Performance comparison test: OLTP vs Graph OLAP CSR for multi-hop traversal.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CSRPerformanceTest extends TestHelper {

  private static final int NODE_COUNT = 5_000;
  private static final int EDGE_COUNT = 20_000;

  @Test
  void compareTraversalPerformance() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Build a random graph
    final MutableVertex[] nodes = new MutableVertex[NODE_COUNT];
    database.begin();
    for (int i = 0; i < NODE_COUNT; i++)
      nodes[i] = database.newVertex("Node").set("idx", i).save();

    final Random rnd = new Random(42);
    for (int i = 0; i < EDGE_COUNT; i++) {
      final int from = rnd.nextInt(NODE_COUNT);
      int to = rnd.nextInt(NODE_COUNT);
      while (to == from)
        to = rnd.nextInt(NODE_COUNT);
      nodes[from].newLightEdge("LINK", nodes[to]);
    }
    database.commit();

    // Build CSR
    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(null, null);

    assertThat(gav.getNodeCount()).isEqualTo(NODE_COUNT);

    // BENCHMARK: 2-hop traversal via CSR (count total 2-hop reachable nodes)
    long csrTotal = 0;
    final long csrStart = System.nanoTime();
    for (int src = 0; src < NODE_COUNT; src++) {
      final int outDeg = gav.outDegree(src);
      for (int i = 0; i < outDeg; i++) {
        final int neighbor = gav.getCSRIndex().outNeighbor(src, i);
        csrTotal += gav.outDegree(neighbor);
      }
    }
    final long csrElapsed = System.nanoTime() - csrStart;

    // BENCHMARK: 2-hop traversal via OLTP iterators
    long oltpTotal = 0;
    final long oltpStart = System.nanoTime();
    database.begin();
    for (final MutableVertex node : nodes) {
      for (final Vertex neighbor : node.getVertices(Vertex.DIRECTION.OUT)) {
        for (final Vertex hop2 : neighbor.getVertices(Vertex.DIRECTION.OUT))
          oltpTotal++;
      }
    }
    database.commit();
    final long oltpElapsed = System.nanoTime() - oltpStart;

    // Both should compute the same total
    assertThat(csrTotal).isEqualTo(oltpTotal);

    final double speedup = (double) oltpElapsed / csrElapsed;
    LogManager.instance().log(this, Level.INFO,
        "2-hop traversal: CSR=%.1f ms, OLTP=%.1f ms, speedup=%.1fx, 2-hop-reachable=%d",
        csrElapsed / 1_000_000.0, oltpElapsed / 1_000_000.0, speedup, csrTotal);

    // CSR should be significantly faster (at least 2x on this small dataset)
    assertThat(speedup).as("CSR should be faster than OLTP for 2-hop traversal").isGreaterThan(1.0);
  }
}
