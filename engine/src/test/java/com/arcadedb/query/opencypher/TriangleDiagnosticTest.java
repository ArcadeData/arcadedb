/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: verifies that the BOTH-direction NeighborView is sorted,
 * which is required for merge-join triangle counting (PartitionedTriangleOp).
 *
 * Before the fix, buildMergedNeighborView() concatenated forward + backward
 * neighbors without sorting, causing sorted intersection to miss common neighbors
 * and produce an undercount (e.g., 276,732 instead of 753,570 on LSQB Q3).
 */
public class TriangleDiagnosticTest extends TestHelper {

  @Test
  public void testBothNeighborViewIsSortedForTriangleCounting() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.getSchema().createVertexType("City");
      database.getSchema().createVertexType("Country");
      database.getSchema().createEdgeType("KNOWS");
      database.getSchema().createEdgeType("IS_LOCATED_IN");
      database.getSchema().createEdgeType("IS_PART_OF");
    });

    database.transaction(() -> {
      final var country1 = database.newVertex("Country").set("name", "C1").save();
      final var city1 = database.newVertex("City").set("name", "City1").save();
      city1.newEdge("IS_PART_OF", country1);

      // 4 persons forming 2 triangles: p0-p1-p2 and p0-p1-p3
      final var p0 = database.newVertex("Person").set("name", "P0").save();
      final var p1 = database.newVertex("Person").set("name", "P1").save();
      final var p2 = database.newVertex("Person").set("name", "P2").save();
      final var p3 = database.newVertex("Person").set("name", "P3").save();

      p0.newEdge("IS_LOCATED_IN", city1);
      p1.newEdge("IS_LOCATED_IN", city1);
      p2.newEdge("IS_LOCATED_IN", city1);
      p3.newEdge("IS_LOCATED_IN", city1);

      // Triangle 1: p0-p1-p2
      p0.newEdge("KNOWS", p1);
      p0.newEdge("KNOWS", p2);
      p1.newEdge("KNOWS", p2);

      // Triangle 2: p0-p1-p3
      p0.newEdge("KNOWS", p3);
      p1.newEdge("KNOWS", p3);
    });

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withEdgeTypes("KNOWS", "IS_LOCATED_IN", "IS_PART_OF")
        .build();

    try {
      final int nodeCount = gav.getNodeCount();
      final NeighborView bothView = gav.getNeighborView(Vertex.DIRECTION.BOTH, "KNOWS");
      assertThat(bothView).as("BOTH KNOWS view should not be null").isNotNull();

      final int[] nbrs = bothView.neighbors();

      // Verify each node's neighbor list is sorted
      for (int u = 0; u < nodeCount; u++) {
        final int start = bothView.offset(u);
        final int end = bothView.offsetEnd(u);
        for (int j = start + 1; j < end; j++)
          assertThat(nbrs[j]).as("BOTH neighbors of node %d must be sorted (index %d)", u, j - start)
              .isGreaterThanOrEqualTo(nbrs[j - 1]);
      }

      // Count triangles using sorted merge-join (as PartitionedTriangleOp does)
      long mergeJoinCount = 0;
      for (int u = 0; u < nodeCount; u++) {
        final int uStart = bothView.offset(u);
        final int uEnd = bothView.offsetEnd(u);
        for (int k = uStart; k < uEnd; k++) {
          final int v = nbrs[k];
          final int vStart = bothView.offset(v);
          final int vEnd = bothView.offsetEnd(v);
          int iu = uStart, iv = vStart;
          while (iu < uEnd && iv < vEnd) {
            final int nu = nbrs[iu], nv = nbrs[iv];
            if (nu < nv)
              iu++;
            else if (nu > nv)
              iv++;
            else {
              mergeJoinCount++;
              iu++;
              iv++;
            }
          }
        }
      }

      // 2 triangles, each enumerated 6 times (all ordered triples) = 12
      assertThat(mergeJoinCount).as("Merge-join triangle count on sorted BOTH view").isEqualTo(12);
    } finally {
      gav.drop();
    }
  }
}
