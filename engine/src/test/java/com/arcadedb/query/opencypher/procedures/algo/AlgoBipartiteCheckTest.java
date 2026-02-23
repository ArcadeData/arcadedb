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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.bipartite Cypher procedure (Bipartite Check).
 */
class AlgoBipartiteCheckTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-bipartite");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private void buildBipartiteGraph() {
    // Bipartite graph: A-B, A-D, C-B, C-D
    // Partition 0: A, C; Partition 1: B, D
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      a.newEdge("LINK", d, true, (Object[]) null).save();
      c.newEdge("LINK", b, true, (Object[]) null).save();
      c.newEdge("LINK", d, true, (Object[]) null).save();
    });
  }

  private void buildNonBipartiteGraph() {
    // Triangle A-B-C-A is NOT bipartite (odd cycle)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      c.newEdge("LINK", a, true, (Object[]) null).save();
    });
  }

  @Test
  void bipartiteGraphIsDetected() {
    buildBipartiteGraph();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.bipartite() YIELD node, partition, isBipartite RETURN node, partition, isBipartite");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);

    // All rows should report isBipartite = true
    for (final Result r : results) {
      final Object isBipartiteObj = r.getProperty("isBipartite");
      assertThat(Boolean.TRUE.equals(isBipartiteObj)).isTrue();
    }
  }

  @Test
  void bipartitePartitionsAreDifferentForAdjacentNodes() {
    buildBipartiteGraph();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.bipartite() YIELD node, partition, isBipartite RETURN node, partition, isBipartite");

    int partA = -1, partB = -1;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      final Object partitionObj = r.getProperty("partition");
      if (nodeObj instanceof Vertex v) {
        final String name = v.getString("name");
        final int partition = ((Number) partitionObj).intValue();
        if ("A".equals(name))
          partA = partition;
        else if ("B".equals(name))
          partB = partition;
      }
    }

    // A and B are adjacent so they must be in different partitions
    assertThat(partA).isNotEqualTo(-1);
    assertThat(partB).isNotEqualTo(-1);
    assertThat(partA).isNotEqualTo(partB);
  }

  @Test
  void nonBipartiteGraphIsDetected() {
    buildNonBipartiteGraph();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.bipartite() YIELD node, partition, isBipartite RETURN node, partition, isBipartite");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3);

    // All rows should report isBipartite = false
    for (final Result r : results) {
      final Object isBipartiteObj = r.getProperty("isBipartite");
      assertThat(Boolean.FALSE.equals(isBipartiteObj)).isTrue();
    }
  }

  @Test
  void bipartitePartitionValuesAreZeroOrOne() {
    buildBipartiteGraph();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.bipartite() YIELD node, partition RETURN partition");

    while (rs.hasNext()) {
      final Object partitionObj = rs.next().getProperty("partition");
      final int partition = ((Number) partitionObj).intValue();
      assertThat(partition).isBetween(0, 1);
    }
  }
}
