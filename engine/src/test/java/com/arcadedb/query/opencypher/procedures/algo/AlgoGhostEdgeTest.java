/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test: graph-algorithm procedures must tolerate ghost edges (a dangling edge-segment
 * pointer whose backing edge record is gone, as can occur after a manual HA leader-to-follower
 * database copy or a rolled-back transaction) by skipping them, rather than throwing
 * {@code RecordNotFoundException} and crashing the whole query.
 * <p>
 * Mirrors the ghost-edge fabrication used by {@code Issue394Test}: delete only the edge record from
 * its bucket, leaving the segment pointer in place.
 */
class AlgoGhostEdgeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/algoGhostEdge");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /**
   * Builds A -[LINK]-> B -[LINK]-> C and then ghosts the A->B edge (deletes only the edge record,
   * leaving the segment pointer dangling). Returns after the ghost is in place.
   */
  private void buildGraphWithGhostEdge() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      a.newEdge("LINK", b).set("w", 1.0).save();
      b.newEdge("LINK", c).set("w", 1.0).save();
    });

    final RID ghostRID;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name:'A'})-[r:LINK]->(b:Node {name:'B'}) RETURN r")) {
      ghostRID = ((Edge) rs.next().getProperty("r")).getIdentity();
    }
    database.transaction(() -> {
      final Bucket bucket = database.getSchema().getBucketById(ghostRID.getBucketId());
      bucket.deleteRecord(ghostRID);
    });
  }

  private void assertProcedureDoesNotThrow(final String cypher) {
    assertThatCode(() -> {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        while (rs.hasNext())
          rs.next();
      }
    }).doesNotThrowAnyException();
  }

  /**
   * Path-finding procedures that traverse live edges must skip the ghost rather than throw.
   */
  @Test
  void pathFindingProceduresSkipGhostEdge() {
    buildGraphWithGhostEdge();

    // allSimplePaths expands live edges from A to C.
    assertProcedureDoesNotThrow(
        "MATCH (a:Node {name:'A'}), (c:Node {name:'C'}) "
            + "CALL algo.allSimplePaths(a, c, ['LINK'], 10) YIELD path RETURN path");

    // dijkstra iterates live edges to rebuild the path / read weights.
    assertProcedureDoesNotThrow(
        "MATCH (a:Node {name:'A'}), (c:Node {name:'C'}) "
            + "CALL algo.dijkstra(a, c, 'LINK', 'w') YIELD path, weight RETURN path, weight");

    // path.expand is an APOC-style traversal over live edges (procedures/path/PathExpand).
    assertProcedureDoesNotThrow(
        "MATCH (a:Node {name:'A'}) "
            + "CALL path.expand(a, 'LINK', null, 1, 10) YIELD path RETURN path");
  }

  /**
   * Whole-graph analytics with more complex inner loops (PageRank: 2 catch sites, Louvain: 3,
   * Betweenness: 1) must also tolerate the ghost edge across their projection/relaxation passes.
   */
  @Test
  void analyticsProceduresSkipGhostEdge() {
    buildGraphWithGhostEdge();

    assertProcedureDoesNotThrow("CALL algo.pagerank() YIELD node, score RETURN node, score");
    assertProcedureDoesNotThrow("CALL algo.louvain() YIELD node, communityId RETURN node, communityId");
    assertProcedureDoesNotThrow("CALL algo.betweenness() YIELD node, score RETURN node, score");
  }
}
