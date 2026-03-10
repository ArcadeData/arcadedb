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
    assertThat(gav.getEdgeTypes()).isEmpty();
  }

  @Test
  void testNotBuiltThrows() {
    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    assertThatThrownBy(() -> gav.getNodeCount()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void testSimpleChain() {
    // A -> B -> C
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
    assertThat(gav.getEdgeCount("FOLLOWS")).isEqualTo(2);
    assertThat(gav.getEdgeTypes()).containsExactly("FOLLOWS");

    final int idA = gav.getNodeId(a.getIdentity());
    final int idB = gav.getNodeId(b.getIdentity());
    final int idC = gav.getNodeId(c.getIdentity());

    assertThat(idA).isGreaterThanOrEqualTo(0);
    assertThat(idB).isGreaterThanOrEqualTo(0);
    assertThat(idC).isGreaterThanOrEqualTo(0);

    // Degrees for specific edge type
    assertThat(gav.outDegree(idA, "FOLLOWS")).isEqualTo(1);
    assertThat(gav.inDegree(idA, "FOLLOWS")).isEqualTo(0);
    assertThat(gav.outDegree(idB, "FOLLOWS")).isEqualTo(1);
    assertThat(gav.inDegree(idB, "FOLLOWS")).isEqualTo(1);
    assertThat(gav.outDegree(idC, "FOLLOWS")).isEqualTo(0);
    assertThat(gav.inDegree(idC, "FOLLOWS")).isEqualTo(1);

    // Cross-type degrees (same since only one edge type)
    assertThat(gav.outDegree(idA)).isEqualTo(1);
    assertThat(gav.inDegree(idC)).isEqualTo(1);

    // Typed connectivity
    assertThat(gav.isConnected(idA, idB, Vertex.DIRECTION.OUT, "FOLLOWS")).isTrue();
    assertThat(gav.isConnected(idA, idC, Vertex.DIRECTION.OUT, "FOLLOWS")).isFalse();
    assertThat(gav.isConnected(idB, idC, Vertex.DIRECTION.OUT, "FOLLOWS")).isTrue();

    // Cross-type connectivity
    assertThat(gav.isConnected(idA, idB, Vertex.DIRECTION.OUT)).isTrue();
    assertThat(gav.isConnected(idB, idA, Vertex.DIRECTION.IN)).isTrue();

    // Typed neighbor arrays
    assertThat(gav.getOutNeighbors(idA, "FOLLOWS")).containsExactly(idB);
    assertThat(gav.getOutNeighbors(idB, "FOLLOWS")).containsExactly(idC);
    assertThat(gav.getOutNeighbors(idC, "FOLLOWS")).isEmpty();

    // Cross-type neighbor arrays
    assertThat(gav.getOutNeighbors(idA)).containsExactly(idB);
    assertThat(gav.getInNeighbors(idC)).containsExactly(idB);
  }

  @Test
  void testStarTopology() {
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
    assertThat(gav.outDegree(hubId, "FOLLOWS")).isEqualTo(10);
    assertThat(gav.inDegree(hubId, "FOLLOWS")).isEqualTo(0);

    for (final RID spokeRID : spokeRIDs) {
      final int spokeId = gav.getNodeId(spokeRID);
      assertThat(gav.outDegree(spokeId, "FOLLOWS")).isEqualTo(0);
      assertThat(gav.inDegree(spokeId, "FOLLOWS")).isEqualTo(1);
      assertThat(gav.isConnected(hubId, spokeId, Vertex.DIRECTION.OUT, "FOLLOWS")).isTrue();
    }
  }

  @Test
  void testVectorizedScan() {
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

    // Vectorized scan with edge type
    final CSRScanOperator scan = gav.scanNeighbors(hubId, Vertex.DIRECTION.OUT, "LINK");
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

    // Typed common neighbors
    assertThat(gav.countCommonNeighbors(idA, idB, Vertex.DIRECTION.OUT, "KNOWS")).isEqualTo(2);
    // Cross-type common neighbors (same result, only one edge type)
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

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(new String[] { "Person" }, null); // all edge types

    final int idA = gav.getNodeId(a.getIdentity());
    final int idB = gav.getNodeId(b.getIdentity());
    final int idC = gav.getNodeId(c.getIdentity());

    assertThat(gav.getEdgeTypes()).containsExactlyInAnyOrder("FOLLOWS", "LIKES");

    // Per-type degree
    assertThat(gav.outDegree(idA, "FOLLOWS")).isEqualTo(2); // B and C
    assertThat(gav.outDegree(idA, "LIKES")).isEqualTo(1);   // B only
    assertThat(gav.inDegree(idB, "FOLLOWS")).isEqualTo(1);
    assertThat(gav.inDegree(idB, "LIKES")).isEqualTo(1);
    assertThat(gav.inDegree(idC, "FOLLOWS")).isEqualTo(1);
    assertThat(gav.inDegree(idC, "LIKES")).isEqualTo(0);

    // Cross-type degree
    assertThat(gav.outDegree(idA)).isEqualTo(3); // FOLLOWS(B,C) + LIKES(B)
    assertThat(gav.inDegree(idB)).isEqualTo(2);  // FOLLOWS + LIKES from A

    // Per-type connectivity
    assertThat(gav.isConnected(idA, idB, Vertex.DIRECTION.OUT, "FOLLOWS")).isTrue();
    assertThat(gav.isConnected(idA, idB, Vertex.DIRECTION.OUT, "LIKES")).isTrue();
    assertThat(gav.isConnected(idA, idC, Vertex.DIRECTION.OUT, "FOLLOWS")).isTrue();
    assertThat(gav.isConnected(idA, idC, Vertex.DIRECTION.OUT, "LIKES")).isFalse();

    // Cross-type connectivity
    assertThat(gav.isConnected(idA, idB, Vertex.DIRECTION.OUT)).isTrue();
    assertThat(gav.isConnected(idA, idC, Vertex.DIRECTION.OUT)).isTrue();

    // Per-type neighbor arrays
    assertThat(gav.getOutNeighbors(idA, "FOLLOWS")).containsExactlyInAnyOrder(idB, idC);
    assertThat(gav.getOutNeighbors(idA, "LIKES")).containsExactly(idB);

    // Cross-type neighbor arrays
    final int[] allOutNeighbors = gav.getOutNeighbors(idA);
    assertThat(allOutNeighbors).hasSize(3); // B (FOLLOWS), B (LIKES), C (FOLLOWS) — B appears twice

    // Per-type edge counts
    assertThat(gav.getEdgeCount("FOLLOWS")).isEqualTo(2);
    assertThat(gav.getEdgeCount("LIKES")).isEqualTo(1);
    assertThat(gav.getEdgeCount()).isEqualTo(3);
  }

  @Test
  void testMultipleVertexTypes() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("WORKS_AT");
    database.getSchema().createEdgeType("KNOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    final MutableVertex acme = database.newVertex("Company").set("name", "ACME").save();
    alice.newEdge("WORKS_AT", acme);
    bob.newEdge("WORKS_AT", acme);
    alice.newEdge("KNOWS", bob);
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(null, null); // all types

    assertThat(gav.getNodeCount()).isEqualTo(3);

    final int idAlice = gav.getNodeId(alice.getIdentity());
    final int idBob = gav.getNodeId(bob.getIdentity());
    final int idAcme = gav.getNodeId(acme.getIdentity());

    // Vertex type tracking
    assertThat(gav.getNodeTypeName(idAlice)).isEqualTo("Person");
    assertThat(gav.getNodeTypeName(idBob)).isEqualTo("Person");
    assertThat(gav.getNodeTypeName(idAcme)).isEqualTo("Company");

    // Per-type degree
    assertThat(gav.outDegree(idAlice, "WORKS_AT")).isEqualTo(1);
    assertThat(gav.outDegree(idAlice, "KNOWS")).isEqualTo(1);
    assertThat(gav.outDegree(idAlice)).isEqualTo(2);
    assertThat(gav.inDegree(idAcme, "WORKS_AT")).isEqualTo(2); // Alice + Bob work there

    // Per-type connectivity
    assertThat(gav.isConnected(idAlice, idAcme, Vertex.DIRECTION.OUT, "WORKS_AT")).isTrue();
    assertThat(gav.isConnected(idAlice, idBob, Vertex.DIRECTION.OUT, "WORKS_AT")).isFalse();
    assertThat(gav.isConnected(idAlice, idBob, Vertex.DIRECTION.OUT, "KNOWS")).isTrue();

    // Edge type counts
    assertThat(gav.getEdgeCount("WORKS_AT")).isEqualTo(2);
    assertThat(gav.getEdgeCount("KNOWS")).isEqualTo(1);
    assertThat(gav.getEdgeCount()).isEqualTo(3);

    // Common neighbors via WORKS_AT: Alice and Bob both work at ACME
    assertThat(gav.countCommonNeighbors(idAlice, idBob, Vertex.DIRECTION.OUT, "WORKS_AT")).isEqualTo(1);
  }

  @Test
  void testSelectiveEdgeTypeBuild() {
    // Build with only FOLLOWS, excluding LIKES
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");
    database.getSchema().createEdgeType("LIKES");

    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    a.newEdge("FOLLOWS", b);
    a.newEdge("LIKES", b);
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(new String[] { "Person" }, new String[] { "FOLLOWS" }); // only FOLLOWS

    final int idA = gav.getNodeId(a.getIdentity());

    assertThat(gav.getEdgeTypes()).containsExactly("FOLLOWS");
    assertThat(gav.outDegree(idA, "FOLLOWS")).isEqualTo(1);
    assertThat(gav.outDegree(idA, "LIKES")).isEqualTo(0); // not in the view
    assertThat(gav.outDegree(idA)).isEqualTo(1);           // only FOLLOWS counted
    assertThat(gav.getEdgeCount()).isEqualTo(1);
  }

  @Test
  void testNonexistentEdgeTypeReturnsZero() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    a.newEdge("FOLLOWS", b);
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(null, null);

    final int idA = gav.getNodeId(a.getIdentity());

    // Querying for edge type not in the view returns 0, not exception
    assertThat(gav.outDegree(idA, "NONEXISTENT")).isEqualTo(0);
    assertThat(gav.inDegree(idA, "NONEXISTENT")).isEqualTo(0);
    assertThat(gav.getOutNeighbors(idA, "NONEXISTENT")).isEmpty();
    assertThat(gav.getInNeighbors(idA, "NONEXISTENT")).isEmpty();
    assertThat(gav.isConnected(idA, 0, Vertex.DIRECTION.OUT, "NONEXISTENT")).isFalse();
    assertThat(gav.getEdgeCount("NONEXISTENT")).isEqualTo(0);
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

    assertThat(mapping.addRID(r1, "Person")).isEqualTo(0);
    assertThat(mapping.addRID(r2, "Person")).isEqualTo(1);
    assertThat(mapping.addRID(r3, "Company")).isEqualTo(2);

    // Duplicate should return existing ID
    assertThat(mapping.addRID(r1, "Person")).isEqualTo(0);

    assertThat(mapping.size()).isEqualTo(3);
    assertThat(mapping.getId(r1)).isEqualTo(0);
    assertThat(mapping.getId(r2)).isEqualTo(1);
    assertThat(mapping.getId(r3)).isEqualTo(2);
    assertThat(mapping.getId(new RID(99, 99))).isEqualTo(-1);

    // Type tracking
    assertThat(mapping.getTypeName(0)).isEqualTo("Person");
    assertThat(mapping.getTypeName(1)).isEqualTo("Person");
    assertThat(mapping.getTypeName(2)).isEqualTo("Company");
    assertThat(mapping.getTypeCount()).isEqualTo(2);
    assertThat(mapping.getTypeIndex("Person")).isEqualTo(0);
    assertThat(mapping.getTypeIndex("Company")).isEqualTo(1);
    assertThat(mapping.getTypeIndex("Unknown")).isEqualTo(-1);

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

    vec.setNull(2);
    assertThat(vec.isNull(2)).isTrue();

    // Flat mode
    vec.setFlat(true, 0);
    assertThat(vec.isFlat()).isTrue();
    assertThat(vec.getInt(0)).isEqualTo(42);
    assertThat(vec.getInt(1)).isEqualTo(42); // flat broadcasts index 0
    assertThat(vec.getInt(999)).isEqualTo(42);

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
    assertThat(gav.outDegree(idA, "FOLLOWS")).isEqualTo(1);
    assertThat(gav.isConnected(idA, idB, Vertex.DIRECTION.OUT, "FOLLOWS")).isTrue();
  }

  @Test
  void testSortedNeighbors() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    final MutableVertex hub = database.newVertex("Node").save();
    final List<MutableVertex> targets = new ArrayList<>();
    for (int i = 0; i < 20; i++)
      targets.add(database.newVertex("Node").save());
    for (int i = targets.size() - 1; i >= 0; i--)
      hub.newEdge("LINK", targets.get(i));
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(null, null);

    final int hubId = gav.getNodeId(hub.getIdentity());
    final int[] neighbors = gav.getOutNeighbors(hubId, "LINK");

    for (int i = 1; i < neighbors.length; i++)
      assertThat(neighbors[i]).isGreaterThanOrEqualTo(neighbors[i - 1]);
  }
}
