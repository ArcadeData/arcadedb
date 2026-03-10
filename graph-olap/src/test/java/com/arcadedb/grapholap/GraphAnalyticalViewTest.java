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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the Graph OLAP module — CSR building, vectorized scan, and analytical operations.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphAnalyticalViewTest extends TestHelper {

  @Test
  void testEmptyGraph() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(new String[] { "Person" }, new String[] { "FOLLOWS" });

    assertThat(gav.getNodeCount()).isEqualTo(0);
    assertThat(gav.getEdgeCount()).isEqualTo(0);
    assertThat(gav.isBuilt()).isTrue();
  }

  @Test
  void testNotBuiltThrows() {
    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    assertThatThrownBy(() -> gav.getNodeCount()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void testSimpleChain() {
    // Create: A -> B -> C
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex b = database.newVertex("Person").set("name", "Bob").save();
    final MutableVertex c = database.newVertex("Person").set("name", "Charlie").save();
    a.newEdge("FOLLOWS", b);
    b.newEdge("FOLLOWS", c);
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(new String[] { "Person" }, new String[] { "FOLLOWS" });

    assertThat(gav.getNodeCount()).isEqualTo(3);
    assertThat(gav.getEdgeCount()).isEqualTo(2);

    final int idA = gav.getNodeId(a.getIdentity());
    final int idB = gav.getNodeId(b.getIdentity());
    final int idC = gav.getNodeId(c.getIdentity());

    assertThat(idA).isGreaterThanOrEqualTo(0);
    assertThat(idB).isGreaterThanOrEqualTo(0);
    assertThat(idC).isGreaterThanOrEqualTo(0);

    // A has 1 out, 0 in
    assertThat(gav.outDegree(idA)).isEqualTo(1);
    assertThat(gav.inDegree(idA)).isEqualTo(0);

    // B has 1 out, 1 in
    assertThat(gav.outDegree(idB)).isEqualTo(1);
    assertThat(gav.inDegree(idB)).isEqualTo(1);

    // C has 0 out, 1 in
    assertThat(gav.outDegree(idC)).isEqualTo(0);
    assertThat(gav.inDegree(idC)).isEqualTo(1);

    // Verify connections
    assertThat(gav.isConnected(idA, idB, Vertex.DIRECTION.OUT)).isTrue();
    assertThat(gav.isConnected(idA, idC, Vertex.DIRECTION.OUT)).isFalse();
    assertThat(gav.isConnected(idB, idC, Vertex.DIRECTION.OUT)).isTrue();

    // Verify backward connections
    assertThat(gav.isConnected(idB, idA, Vertex.DIRECTION.IN)).isTrue();
    assertThat(gav.isConnected(idC, idB, Vertex.DIRECTION.IN)).isTrue();

    // Verify neighbor arrays
    assertThat(gav.getOutNeighbors(idA)).containsExactly(idB);
    assertThat(gav.getOutNeighbors(idB)).containsExactly(idC);
    assertThat(gav.getOutNeighbors(idC)).isEmpty();
    assertThat(gav.getInNeighbors(idC)).containsExactly(idB);
  }

  @Test
  void testStarTopology() {
    // Create hub with 10 spokes: Hub -> Spoke_0..9
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    final MutableVertex hub = database.newVertex("Person").set("name", "Hub").save();
    final List<RID> spokeRIDs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final MutableVertex spoke = database.newVertex("Person").set("name", "Spoke_" + i).save();
      hub.newEdge("FOLLOWS", spoke);
      spokeRIDs.add(spoke.getIdentity());
    }
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(new String[] { "Person" }, new String[] { "FOLLOWS" });

    assertThat(gav.getNodeCount()).isEqualTo(11);
    assertThat(gav.getEdgeCount()).isEqualTo(10);

    final int hubId = gav.getNodeId(hub.getIdentity());
    assertThat(gav.outDegree(hubId)).isEqualTo(10);
    assertThat(gav.inDegree(hubId)).isEqualTo(0);

    // All spokes have 0 out, 1 in
    for (final RID spokeRID : spokeRIDs) {
      final int spokeId = gav.getNodeId(spokeRID);
      assertThat(gav.outDegree(spokeId)).isEqualTo(0);
      assertThat(gav.inDegree(spokeId)).isEqualTo(1);
      assertThat(gav.isConnected(hubId, spokeId, Vertex.DIRECTION.OUT)).isTrue();
    }
  }

  @Test
  void testVectorizedScan() {
    // Create hub with enough edges to fill multiple batches
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    final MutableVertex hub = database.newVertex("Node").set("id", 0).save();
    for (int i = 0; i < 100; i++) {
      final MutableVertex target = database.newVertex("Node").set("id", i + 1).save();
      hub.newEdge("LINK", target);
    }
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(new String[] { "Node" }, new String[] { "LINK" });

    final int hubId = gav.getNodeId(hub.getIdentity());

    // Use vectorized scan
    final CSRScanOperator scan = gav.scanNeighbors(hubId, Vertex.DIRECTION.OUT);
    final DataVector batch = new DataVector(DataVector.Type.INT);

    int totalScanned = 0;
    while (scan.getNextBatch(batch))
      totalScanned += batch.getSize();

    assertThat(totalScanned).isEqualTo(100);
    assertThat(scan.totalNeighbors()).isEqualTo(100);
  }

  @Test
  void testCommonNeighbors() {
    // A -> C, A -> D, B -> C, B -> D, B -> E
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    final MutableVertex c = database.newVertex("Person").set("name", "C").save();
    final MutableVertex d = database.newVertex("Person").set("name", "D").save();
    final MutableVertex e = database.newVertex("Person").set("name", "E").save();
    a.newEdge("KNOWS", c);
    a.newEdge("KNOWS", d);
    b.newEdge("KNOWS", c);
    b.newEdge("KNOWS", d);
    b.newEdge("KNOWS", e);
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(new String[] { "Person" }, new String[] { "KNOWS" });

    final int idA = gav.getNodeId(a.getIdentity());
    final int idB = gav.getNodeId(b.getIdentity());

    // A and B share C and D as common outgoing neighbors
    assertThat(gav.countCommonNeighbors(idA, idB, Vertex.DIRECTION.OUT)).isEqualTo(2);
  }

  @Test
  void testMultipleEdgeTypes() {
    // A -FOLLOWS-> B, A -LIKES-> B, A -FOLLOWS-> C
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");
    database.getSchema().createEdgeType("LIKES");

    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    final MutableVertex c = database.newVertex("Person").set("name", "C").save();
    a.newEdge("FOLLOWS", b);
    a.newEdge("LIKES", b);
    a.newEdge("FOLLOWS", c);
    database.commit();

    // Build for FOLLOWS only
    final GraphAnalyticalView gavFollows = new GraphAnalyticalView(database);
    gavFollows.build(new String[] { "Person" }, new String[] { "FOLLOWS" });
    final int idAFollow = gavFollows.getNodeId(a.getIdentity());
    assertThat(gavFollows.outDegree(idAFollow)).isEqualTo(2); // FOLLOWS to B and C

    // Build for ALL edge types
    final GraphAnalyticalView gavAll = new GraphAnalyticalView(database);
    gavAll.build(new String[] { "Person" }, null);
    final int idAAll = gavAll.getNodeId(a.getIdentity());
    assertThat(gavAll.outDegree(idAAll)).isEqualTo(3); // FOLLOWS B, LIKES B, FOLLOWS C
  }

  @Test
  void testRIDRoundTrip() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    a.newEdge("KNOWS", b);
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(new String[] { "Person" }, new String[] { "KNOWS" });

    // RID → dense ID → RID roundtrip
    final int idA = gav.getNodeId(a.getIdentity());
    final RID ridA = gav.getRID(idA);
    assertThat(ridA.getBucketId()).isEqualTo(a.getIdentity().getBucketId());
    assertThat(ridA.getPosition()).isEqualTo(a.getIdentity().getPosition());
  }

  @Test
  void testNodeIdMapping() {
    final NodeIdMapping mapping = new NodeIdMapping(4);

    final RID r1 = new RID(10, 0);
    final RID r2 = new RID(10, 1);
    final RID r3 = new RID(10, 2);

    assertThat(mapping.addRID(r1)).isEqualTo(0);
    assertThat(mapping.addRID(r2)).isEqualTo(1);
    assertThat(mapping.addRID(r3)).isEqualTo(2);

    // Duplicate should return existing ID
    assertThat(mapping.addRID(r1)).isEqualTo(0);

    assertThat(mapping.size()).isEqualTo(3);
    assertThat(mapping.getId(r1)).isEqualTo(0);
    assertThat(mapping.getId(r2)).isEqualTo(1);
    assertThat(mapping.getId(r3)).isEqualTo(2);
    assertThat(mapping.getId(new RID(99, 99))).isEqualTo(-1);

    // Compact and verify
    mapping.compact();
    assertThat(mapping.getBucketId(0)).isEqualTo(10);
    assertThat(mapping.getOffset(0)).isEqualTo(0);
    assertThat(mapping.getOffset(1)).isEqualTo(1);
  }

  @Test
  void testDataVector() {
    final DataVector vec = new DataVector(DataVector.Type.INT);
    assertThat(vec.getType()).isEqualTo(DataVector.Type.INT);
    assertThat(vec.getSize()).isEqualTo(0);

    vec.setInt(0, 42);
    vec.setInt(1, 99);
    vec.setSize(2);

    assertThat(vec.getInt(0)).isEqualTo(42);
    assertThat(vec.getInt(1)).isEqualTo(99);
    assertThat(vec.isNull(0)).isFalse();

    // Test null
    vec.setNull(2);
    assertThat(vec.isNull(2)).isTrue();

    // Test flat mode
    vec.setFlat(true, 0);
    assertThat(vec.isFlat()).isTrue();
    assertThat(vec.getInt(0)).isEqualTo(42);
    assertThat(vec.getInt(1)).isEqualTo(42); // flat mode broadcasts index 0
    assertThat(vec.getInt(999)).isEqualTo(42);

    // Test reset
    vec.reset();
    assertThat(vec.getSize()).isEqualTo(0);
    assertThat(vec.isFlat()).isFalse();
  }

  @Test
  void testMemoryUsage() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    final MutableVertex hub = database.newVertex("Node").save();
    for (int i = 0; i < 50; i++) {
      final MutableVertex target = database.newVertex("Node").save();
      hub.newEdge("LINK", target);
    }
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(null, null);

    assertThat(gav.getMemoryUsageBytes()).isGreaterThan(0);
    assertThat(gav.getBuildTimestamp()).isGreaterThan(0);
  }

  @Test
  void testLightEdges() {
    // Light edges (no properties) should be handled correctly
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    a.newLightEdge("FOLLOWS", b);
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(new String[] { "Person" }, new String[] { "FOLLOWS" });

    assertThat(gav.getNodeCount()).isEqualTo(2);
    assertThat(gav.getEdgeCount()).isEqualTo(1);

    final int idA = gav.getNodeId(a.getIdentity());
    final int idB = gav.getNodeId(b.getIdentity());
    assertThat(gav.outDegree(idA)).isEqualTo(1);
    assertThat(gav.isConnected(idA, idB, Vertex.DIRECTION.OUT)).isTrue();
  }

  @Test
  void testBuildAllTypes() {
    // Build with null vertex/edge types = all types
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("WORKS_AT");

    database.begin();
    final MutableVertex person = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex company = database.newVertex("Company").set("name", "ArcadeDB").save();
    person.newEdge("WORKS_AT", company);
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(null, null);

    assertThat(gav.getNodeCount()).isEqualTo(2);
    assertThat(gav.getEdgeCount()).isEqualTo(1);
  }

  @Test
  void testSortedNeighbors() {
    // Verify that neighbor lists are sorted (required for binary search and set intersection)
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    final MutableVertex hub = database.newVertex("Node").save();
    final List<MutableVertex> targets = new ArrayList<>();
    for (int i = 0; i < 20; i++)
      targets.add(database.newVertex("Node").save());
    // Add edges in reverse order
    for (int i = targets.size() - 1; i >= 0; i--)
      hub.newEdge("LINK", targets.get(i));
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(null, null);

    final int hubId = gav.getNodeId(hub.getIdentity());
    final int[] neighbors = gav.getOutNeighbors(hubId);

    // Verify sorted
    for (int i = 1; i < neighbors.length; i++)
      assertThat(neighbors[i]).isGreaterThanOrEqualTo(neighbors[i - 1]);
  }
}
