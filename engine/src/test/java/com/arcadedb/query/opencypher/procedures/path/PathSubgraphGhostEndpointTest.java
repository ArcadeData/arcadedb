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
package com.arcadedb.query.opencypher.procedures.path;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A reachable-component walk must drop an adjacency entry whose far endpoint has no record WHOLE: neither the
 * missing vertex in {@code nodes}, nor the edge reaching it in {@code relationships}. Reporting the edge while
 * leaving its endpoint out just moves the failure to whoever reads the edge back.
 * <p>
 * The ghost is fabricated the way {@code AlgoGhostEdgeTest} and {@code Issue394Test} do it - the record is deleted
 * from its bucket directly, so the pointers that name it survive - except that here it is the VERTEX that goes,
 * not the edge.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PathSubgraphGhostEndpointTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-path-subgraph-ghost-endpoint");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // A -LINK-> B (kept) and A -LINK-> GHOST, TWICE, SO THE SECOND REFERENCE EXERCISES THE KNOWN-GHOST PATH
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex ghost = database.newVertex("Node").set("name", "GHOST").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      a.newEdge("LINK", ghost, true, (Object[]) null).save();
      b.newEdge("LINK", ghost, true, (Object[]) null).save();
    });

    // DELETE ONLY THE VERTEX RECORD: THE EDGES AND THE ADJACENCY ENTRIES NAMING IT STAY BEHIND
    final RID ghostRID = (RID) database.query("sql", "SELECT FROM Node WHERE name = 'GHOST'").next().getIdentity().get();
    database.transaction(() -> {
      final Bucket bucket = database.getSchema().getBucketById(ghostRID.getBucketId());
      bucket.deleteRecord(ghostRID);
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void subgraphAllDropsBothTheGhostVertexAndTheEdgesReachingIt() {
    final ResultSet rs = database.query("cypher", """
        MATCH (a:Node {name: 'A'})
        CALL path.subgraphall(a, {relationshipFilter: 'LINK'}) YIELD nodes, relationships
        RETURN nodes, relationships
        """);

    final Result result = rs.next();
    final List<?> nodes = result.getProperty("nodes");
    final List<?> relationships = result.getProperty("relationships");

    assertThat(nodes).hasSize(2);
    assertThat(relationships).hasSize(1);
  }

  @Test
  void subgraphNodesDropsTheGhostVertex() {
    final ResultSet rs = database.query("cypher", """
        MATCH (a:Node {name: 'A'})
        CALL path.subgraphnodes(a, {relationshipFilter: 'LINK'}) YIELD node
        RETURN count(node) AS total
        """);

    assertThat(((Number) rs.next().getProperty("total")).intValue()).isEqualTo(2);
  }
}
