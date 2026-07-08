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
package com.arcadedb.bolt;

import com.arcadedb.bolt.structure.BoltPath;
import com.arcadedb.bolt.structure.BoltPointStructure;
import com.arcadedb.bolt.structure.BoltStructureMapper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Bolt4998PathMappingTest {
  private static final String DB_PATH = "./target/databases/Bolt4998PathMappingTest";
  private Database db;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    db = factory.create();
    db.getSchema().createVertexType("V");
    db.getSchema().createEdgeType("E");
    db.transaction(() -> {
      final MutableVertex a = db.newVertex("V").set("k", "a").save();
      final MutableVertex b = db.newVertex("V").set("k", "b").save();
      final MutableVertex c = db.newVertex("V").set("k", "c").save();
      a.newEdge("E", b).save();
      b.newEdge("E", c).save();
    });
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  @Test
  @DisplayName("[TYPE-003] a 2-hop forward path maps to a native BoltPath")
  void type003_forwardPath() {
    final ResultSet rs = db.query("opencypher", "MATCH p=(a:V {k:'a'})-[:E*2]->(c) RETURN p");
    final TraversalPath tp = (TraversalPath) rs.next().getProperty("p");
    final BoltPath bp = BoltStructureMapper.toPath(tp);
    assertThat(bp.getNodes()).hasSize(3);
    assertThat(bp.getRelationships()).hasSize(2);
    // [relIdx(1-based, +forward), nodeIdx(0-based), ...] -> rel1 fwd to node1, rel2 fwd to node2
    assertThat(bp.getIndices()).containsExactly(1L, 1L, 2L, 2L);
  }

  @Test
  @DisplayName("[TYPE-003] a backward-traversed hop yields a negative rel index")
  void type003_backwardHop() {
    // Path starts at b and walks the incoming edge (a)-[E]->(b) backward to a, so the hop's
    // rel index must be negative: edge.getOut() (a) != the previous path node (b).
    final ResultSet rs = db.query("opencypher", "MATCH p=(b:V {k:'b'})<-[:E]-(a) RETURN p");
    final TraversalPath tp = (TraversalPath) rs.next().getProperty("p");
    final BoltPath bp = BoltStructureMapper.toPath(tp);
    assertThat(bp.getNodes()).hasSize(2);
    assertThat(bp.getRelationships()).hasSize(1);
    assertThat(bp.getIndices().get(0)).isEqualTo(-1L); // backward, negative
    assertThat(bp.getIndices().get(1)).isEqualTo(1L);  // reached node index (a) = 1
  }

  @Test
  @DisplayName("[TYPE-003] a cycle path (a->b->a) dedups the revisited start node")
  void type003_revisitedNodeCyclePath() {
    db.getSchema().createVertexType("CV");
    db.getSchema().createEdgeType("CE");
    db.transaction(() -> {
      final MutableVertex ca = db.newVertex("CV").set("k", "a").save();
      final MutableVertex cb = db.newVertex("CV").set("k", "b").save();
      ca.newEdge("CE", cb).save();
      cb.newEdge("CE", ca).save();
    });

    final ResultSet rs = db.query("opencypher",
        "MATCH p=(x:CV {k:'a'})-[:CE]->(:CV {k:'b'})-[:CE]->(:CV {k:'a'}) RETURN p LIMIT 1");
    final TraversalPath tp = (TraversalPath) rs.next().getProperty("p");
    final BoltPath bp = BoltStructureMapper.toPath(tp);
    // the revisited start node (a) is deduplicated: nodes holds only {a, b}, not {a, b, a}
    assertThat(bp.getNodes()).hasSize(2);
    assertThat(bp.getRelationships()).hasSize(2);
    // rel1 fwd to node index 1 (b), rel2 fwd to node index 0 (a, revisited/reused)
    assertThat(bp.getIndices()).containsExactly(1L, 1L, 2L, 0L);
  }

  @Test
  @DisplayName("[TYPE-012] RETURN point({...}) maps to a native BoltPointStructure")
  void type012_enginePointOutbound() {
    final ResultSet rs = db.query("opencypher", "RETURN point({x: 12.34, y: 56.78}) AS p");
    final Object p = rs.next().getProperty("p");
    final Object out = BoltStructureMapper.toPackStreamValue(p);
    assertThat(out).isInstanceOf(BoltPointStructure.class);
    final BoltPointStructure pt = (BoltPointStructure) out;
    assertThat(pt.getZ()).isNull(); // z absent -> writeTo emits the Point2D (0x58) signature
    assertThat(pt.getSrid()).isEqualTo(7203);
    assertThat(pt.getX()).isEqualTo(12.34);
    assertThat(pt.getY()).isEqualTo(56.78);
  }
}
