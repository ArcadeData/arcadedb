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
 * Tests for the Graph OLAP module — CSR building, columnar storage, vectorized scan, and analytical operations.
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

    // Degrees
    assertThat(gav.countEdges(idA, Vertex.DIRECTION.OUT, "FOLLOWS")).isEqualTo(1);
    assertThat(gav.countEdges(idA, Vertex.DIRECTION.IN, "FOLLOWS")).isEqualTo(0);
    assertThat(gav.countEdges(idB, Vertex.DIRECTION.OUT, "FOLLOWS")).isEqualTo(1);
    assertThat(gav.countEdges(idB, Vertex.DIRECTION.IN, "FOLLOWS")).isEqualTo(1);
    assertThat(gav.countEdges(idC, Vertex.DIRECTION.OUT, "FOLLOWS")).isEqualTo(0);
    assertThat(gav.countEdges(idC, Vertex.DIRECTION.IN, "FOLLOWS")).isEqualTo(1);

    // Cross-type and BOTH
    assertThat(gav.countEdges(idA, Vertex.DIRECTION.OUT)).isEqualTo(1);
    assertThat(gav.countEdges(idC, Vertex.DIRECTION.IN)).isEqualTo(1);
    assertThat(gav.countEdges(idB, Vertex.DIRECTION.BOTH, "FOLLOWS")).isEqualTo(2);

    // Connectivity
    assertThat(gav.isConnectedTo(idA, idB, Vertex.DIRECTION.OUT, "FOLLOWS")).isTrue();
    assertThat(gav.isConnectedTo(idA, idC, Vertex.DIRECTION.OUT, "FOLLOWS")).isFalse();
    assertThat(gav.isConnectedTo(idB, idC, Vertex.DIRECTION.OUT, "FOLLOWS")).isTrue();
    assertThat(gav.isConnectedTo(idA, idB, Vertex.DIRECTION.OUT)).isTrue();
    assertThat(gav.isConnectedTo(idB, idA, Vertex.DIRECTION.IN)).isTrue();

    // Neighbor arrays
    assertThat(gav.getVertices(idA, Vertex.DIRECTION.OUT, "FOLLOWS")).containsExactly(idB);
    assertThat(gav.getVertices(idB, Vertex.DIRECTION.OUT, "FOLLOWS")).containsExactly(idC);
    assertThat(gav.getVertices(idC, Vertex.DIRECTION.OUT, "FOLLOWS")).isEmpty();
    assertThat(gav.getVertices(idA, Vertex.DIRECTION.OUT)).containsExactly(idB);
    assertThat(gav.getVertices(idC, Vertex.DIRECTION.IN)).containsExactly(idB);

    // Property access
    assertThat(gav.getProperty(idA, "name")).isEqualTo("Alice");
    assertThat(gav.getProperty(idB, "name")).isEqualTo("Bob");
    assertThat(gav.getProperty(idC, "name")).isEqualTo("Charlie");
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
    assertThat(gav.countEdges(hubId, Vertex.DIRECTION.OUT, "FOLLOWS")).isEqualTo(10);
    assertThat(gav.countEdges(hubId, Vertex.DIRECTION.IN, "FOLLOWS")).isEqualTo(0);

    for (final RID spokeRID : spokeRIDs) {
      final int spokeId = gav.getNodeId(spokeRID);
      assertThat(gav.countEdges(spokeId, Vertex.DIRECTION.OUT, "FOLLOWS")).isEqualTo(0);
      assertThat(gav.countEdges(spokeId, Vertex.DIRECTION.IN, "FOLLOWS")).isEqualTo(1);
      assertThat(gav.isConnectedTo(hubId, spokeId, Vertex.DIRECTION.OUT, "FOLLOWS")).isTrue();
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

    assertThat(gav.countCommonNeighbors(idA, idB, Vertex.DIRECTION.OUT, "KNOWS")).isEqualTo(2);
    assertThat(gav.countCommonNeighbors(idA, idB, Vertex.DIRECTION.OUT)).isEqualTo(2);
  }

  @Test
  void testMultipleEdgeTypes() {
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
    gav.build(new String[] { "Person" }, null);

    final int idA = gav.getNodeId(a.getIdentity());
    final int idB = gav.getNodeId(b.getIdentity());
    final int idC = gav.getNodeId(c.getIdentity());

    assertThat(gav.getEdgeTypes()).containsExactlyInAnyOrder("FOLLOWS", "LIKES");
    assertThat(gav.countEdges(idA, Vertex.DIRECTION.OUT, "FOLLOWS")).isEqualTo(2);
    assertThat(gav.countEdges(idA, Vertex.DIRECTION.OUT, "LIKES")).isEqualTo(1);
    assertThat(gav.countEdges(idA, Vertex.DIRECTION.OUT)).isEqualTo(3);
    assertThat(gav.countEdges(idB, Vertex.DIRECTION.IN)).isEqualTo(2);

    assertThat(gav.isConnectedTo(idA, idB, Vertex.DIRECTION.OUT, "FOLLOWS")).isTrue();
    assertThat(gav.isConnectedTo(idA, idB, Vertex.DIRECTION.OUT, "LIKES")).isTrue();
    assertThat(gav.isConnectedTo(idA, idC, Vertex.DIRECTION.OUT, "LIKES")).isFalse();

    assertThat(gav.getVertices(idA, Vertex.DIRECTION.OUT, "FOLLOWS")).containsExactlyInAnyOrder(idB, idC);
    assertThat(gav.getVertices(idA, Vertex.DIRECTION.OUT, "LIKES")).containsExactly(idB);

    final int[] allOut = gav.getVertices(idA, Vertex.DIRECTION.OUT);
    assertThat(allOut).hasSize(3);

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
    gav.build(null, null);

    assertThat(gav.getNodeCount()).isEqualTo(3);

    final int idAlice = gav.getNodeId(alice.getIdentity());
    final int idBob = gav.getNodeId(bob.getIdentity());
    final int idAcme = gav.getNodeId(acme.getIdentity());

    assertThat(gav.getNodeTypeName(idAlice)).isEqualTo("Person");
    assertThat(gav.getNodeTypeName(idAcme)).isEqualTo("Company");

    assertThat(gav.countEdges(idAlice, Vertex.DIRECTION.OUT, "WORKS_AT")).isEqualTo(1);
    assertThat(gav.countEdges(idAlice, Vertex.DIRECTION.OUT, "KNOWS")).isEqualTo(1);
    assertThat(gav.countEdges(idAlice, Vertex.DIRECTION.OUT)).isEqualTo(2);
    assertThat(gav.countEdges(idAcme, Vertex.DIRECTION.IN, "WORKS_AT")).isEqualTo(2);

    assertThat(gav.isConnectedTo(idAlice, idAcme, Vertex.DIRECTION.OUT, "WORKS_AT")).isTrue();
    assertThat(gav.isConnectedTo(idAlice, idBob, Vertex.DIRECTION.OUT, "KNOWS")).isTrue();

    assertThat(gav.countCommonNeighbors(idAlice, idBob, Vertex.DIRECTION.OUT, "WORKS_AT")).isEqualTo(1);

    assertThat(gav.getProperty(idAlice, "name")).isEqualTo("Alice");
    assertThat(gav.getProperty(idAcme, "name")).isEqualTo("ACME");
  }

  @Test
  void testSelectiveEdgeTypeBuild() {
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
    gav.build(new String[] { "Person" }, new String[] { "FOLLOWS" });

    final int idA = gav.getNodeId(a.getIdentity());
    assertThat(gav.getEdgeTypes()).containsExactly("FOLLOWS");
    assertThat(gav.countEdges(idA, Vertex.DIRECTION.OUT, "FOLLOWS")).isEqualTo(1);
    assertThat(gav.countEdges(idA, Vertex.DIRECTION.OUT, "LIKES")).isEqualTo(0);
    assertThat(gav.countEdges(idA, Vertex.DIRECTION.OUT)).isEqualTo(1);
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
    assertThat(gav.countEdges(idA, Vertex.DIRECTION.OUT, "NONEXISTENT")).isEqualTo(0);
    assertThat(gav.countEdges(idA, Vertex.DIRECTION.IN, "NONEXISTENT")).isEqualTo(0);
    assertThat(gav.getVertices(idA, Vertex.DIRECTION.OUT, "NONEXISTENT")).isEmpty();
    assertThat(gav.getVertices(idA, Vertex.DIRECTION.IN, "NONEXISTENT")).isEmpty();
    assertThat(gav.isConnectedTo(idA, 0, Vertex.DIRECTION.OUT, "NONEXISTENT")).isFalse();
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
  void testNodeIdMappingPerBucket() {
    final NodeIdMapping mapping = new NodeIdMapping(4);

    // Register two buckets
    final int b0 = mapping.registerBucket(10, "Person", 4);
    final int b1 = mapping.registerBucket(20, "Company", 4);

    assertThat(b0).isEqualTo(0);
    assertThat(b1).isEqualTo(1);

    // Add nodes
    mapping.addNode(b0, 100L);
    mapping.addNode(b0, 200L);
    mapping.addNode(b0, 300L);
    mapping.addNode(b1, 50L);
    mapping.addNode(b1, 150L);

    mapping.compact();

    assertThat(mapping.size()).isEqualTo(5);
    assertThat(mapping.getNumBuckets()).isEqualTo(2);
    assertThat(mapping.getBucketSize(b0)).isEqualTo(3);
    assertThat(mapping.getBucketSize(b1)).isEqualTo(2);

    // Global IDs: bucket 0 gets [0,1,2], bucket 1 gets [3,4]
    assertThat(mapping.getBucketBase(b0)).isEqualTo(0);
    assertThat(mapping.getBucketBase(b1)).isEqualTo(3);

    // RID → globalId
    assertThat(mapping.getGlobalId(new RID(10, 100))).isEqualTo(0);
    assertThat(mapping.getGlobalId(new RID(10, 200))).isEqualTo(1);
    assertThat(mapping.getGlobalId(new RID(10, 300))).isEqualTo(2);
    assertThat(mapping.getGlobalId(new RID(20, 50))).isEqualTo(3);
    assertThat(mapping.getGlobalId(new RID(20, 150))).isEqualTo(4);
    assertThat(mapping.getGlobalId(new RID(99, 0))).isEqualTo(-1);

    // globalId → bucketIdx
    assertThat(mapping.getBucketIdx(0)).isEqualTo(b0);
    assertThat(mapping.getBucketIdx(2)).isEqualTo(b0);
    assertThat(mapping.getBucketIdx(3)).isEqualTo(b1);
    assertThat(mapping.getBucketIdx(4)).isEqualTo(b1);

    // globalId → localId
    assertThat(mapping.getLocalId(0)).isEqualTo(0);
    assertThat(mapping.getLocalId(2)).isEqualTo(2);
    assertThat(mapping.getLocalId(3)).isEqualTo(0);
    assertThat(mapping.getLocalId(4)).isEqualTo(1);

    // Type names
    assertThat(mapping.getTypeName(0)).isEqualTo("Person");
    assertThat(mapping.getTypeName(3)).isEqualTo("Company");
    assertThat(mapping.getBucketTypeName(b0)).isEqualTo("Person");
    assertThat(mapping.getBucketTypeName(b1)).isEqualTo("Company");
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

    vec.setFlat(true, 0);
    assertThat(vec.isFlat()).isTrue();
    assertThat(vec.getInt(0)).isEqualTo(42);
    assertThat(vec.getInt(1)).isEqualTo(42);
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
    assertThat(gav.countEdges(idA, Vertex.DIRECTION.OUT, "FOLLOWS")).isEqualTo(1);
    assertThat(gav.isConnectedTo(idA, idB, Vertex.DIRECTION.OUT, "FOLLOWS")).isTrue();
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
    final int[] neighbors = gav.getVertices(hubId, Vertex.DIRECTION.OUT, "LINK");
    for (int i = 1; i < neighbors.length; i++)
      assertThat(neighbors[i]).isGreaterThanOrEqualTo(neighbors[i - 1]);
  }

  // --- Builder tests ---

  @Test
  void testBuilderBasic() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "Alice").set("age", 30).save();
    final MutableVertex b = database.newVertex("Person").set("name", "Bob").set("age", 25).save();
    a.newEdge("FOLLOWS", b);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .build();

    assertThat(gav.isBuilt()).isTrue();
    assertThat(gav.getNodeCount()).isEqualTo(2);
    assertThat(gav.getEdgeCount()).isEqualTo(1);

    final int idA = gav.getNodeId(a.getIdentity());
    assertThat(gav.getProperty(idA, "name")).isEqualTo("Alice");
    assertThat(gav.getProperty(idA, "age")).isEqualTo(30);
  }

  @Test
  void testBuilderPropertyFilter() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "Alice").set("age", 30).save();
    final MutableVertex b = database.newVertex("Person").set("name", "Bob").set("age", 25).save();
    a.newEdge("FOLLOWS", b);
    database.commit();

    // Only materialize "name", skip "age"
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .withProperties("name")
        .build();

    final int idA = gav.getNodeId(a.getIdentity());
    assertThat(gav.getProperty(idA, "name")).isEqualTo("Alice");
    assertThat(gav.getProperty(idA, "age")).isNull(); // not materialized
  }

  @Test
  void testBuilderNoProperties() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex b = database.newVertex("Person").set("name", "Bob").save();
    a.newEdge("FOLLOWS", b);
    database.commit();

    // Skip all properties
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .withProperties() // empty = no properties
        .build();

    final int idA = gav.getNodeId(a.getIdentity());
    assertThat(gav.getProperty(idA, "name")).isNull();
    assertThat(gav.countEdges(idA, Vertex.DIRECTION.OUT, "FOLLOWS")).isEqualTo(1);
  }

  @Test
  void testAutoUpdate() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex b = database.newVertex("Person").set("name", "Bob").save();
    a.newEdge("FOLLOWS", b);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .withAutoUpdate(true)
        .build();

    assertThat(gav.isAutoUpdate()).isTrue();
    assertThat(gav.getNodeCount()).isEqualTo(2);
    assertThat(gav.getEdgeCount()).isEqualTo(1);

    // Add a new vertex and edge
    database.begin();
    final MutableVertex c = database.newVertex("Person").set("name", "Charlie").save();
    b.newEdge("FOLLOWS", c);
    database.commit();

    // After commit, the view should have been rebuilt automatically
    assertThat(gav.getNodeCount()).isEqualTo(3);
    assertThat(gav.getEdgeCount()).isEqualTo(2);

    final int idC = gav.getNodeId(c.getIdentity());
    assertThat(idC).isGreaterThanOrEqualTo(0);
    assertThat(gav.getProperty(idC, "name")).isEqualTo("Charlie");

    gav.close();
  }

  // --- Columnar storage tests ---

  @Test
  void testColumnStoreBasics() {
    final ColumnStore store = new ColumnStore(5);

    final Column intCol = store.createColumn("age", Column.Type.INT);
    final Column strCol = store.createColumn("name", Column.Type.STRING);
    final Column dblCol = store.createColumn("score", Column.Type.DOUBLE);

    assertThat(store.getColumnCount()).isEqualTo(3);
    assertThat(store.getPropertyNames()).containsExactlyInAnyOrder("age", "name", "score");

    assertThat(intCol.isNull(0)).isTrue();
    assertThat(intCol.countNonNull()).isEqualTo(0);

    intCol.setInt(0, 30);
    intCol.setInt(2, 25);
    strCol.setString(0, "Alice");
    strCol.setString(1, "Bob");
    strCol.setString(2, "Alice");
    dblCol.setDouble(0, 9.5);

    assertThat(intCol.getInt(0)).isEqualTo(30);
    assertThat(intCol.isNull(0)).isFalse();
    assertThat(intCol.isNull(1)).isTrue();
    assertThat(intCol.countNonNull()).isEqualTo(2);

    assertThat(strCol.getString(0)).isEqualTo("Alice");
    assertThat(strCol.getString(1)).isEqualTo("Bob");
    assertThat(strCol.getString(2)).isEqualTo("Alice");

    assertThat(store.getValue(0, "age")).isEqualTo(30);
    assertThat(store.getValue(0, "name")).isEqualTo("Alice");
    assertThat(store.getValue(1, "age")).isNull();
    assertThat(store.getValue(0, "nonexistent")).isNull();
  }

  @Test
  void testDictionaryEncoding() {
    final DictionaryEncoding dict = new DictionaryEncoding();
    assertThat(dict.size()).isEqualTo(0);

    final int codeA = dict.encode("Alice");
    final int codeB = dict.encode("Bob");
    final int codeA2 = dict.encode("Alice");

    assertThat(codeA).isEqualTo(0);
    assertThat(codeB).isEqualTo(1);
    assertThat(codeA2).isEqualTo(0);

    assertThat(dict.size()).isEqualTo(2);
    assertThat(dict.decode(0)).isEqualTo("Alice");
    assertThat(dict.decode(1)).isEqualTo("Bob");
    assertThat(dict.getCode("Unknown")).isEqualTo(-1);
  }

  @Test
  void testColumnScanOperatorFullScan() {
    final Column col = new Column("age", Column.Type.INT, 5);
    col.setInt(0, 30);
    col.setInt(1, 25);
    col.setInt(3, 40);
    col.setInt(4, 35);

    final ColumnScanOperator scan = new ColumnScanOperator(col);
    final DataVector batch = new DataVector(DataVector.Type.INT);

    assertThat(scan.getNextBatch(batch)).isTrue();
    assertThat(batch.getSize()).isEqualTo(5);
    assertThat(batch.getIntData()[0]).isEqualTo(30);
    assertThat(batch.isNull(0)).isFalse();
    assertThat(batch.isNull(2)).isTrue();

    assertThat(scan.getNextBatch(batch)).isFalse();

    scan.reset();
    assertThat(scan.getNextBatch(batch)).isTrue();
  }

  @Test
  void testColumnScanOperatorSelective() {
    final Column col = new Column("score", Column.Type.DOUBLE, 10);
    for (int i = 0; i < 10; i++)
      col.setDouble(i, i * 1.5);

    final int[] selection = { 2, 5, 7 };
    final ColumnScanOperator scan = new ColumnScanOperator(col, selection);
    final DataVector batch = new DataVector(DataVector.Type.DOUBLE);

    assertThat(scan.getNextBatch(batch)).isTrue();
    assertThat(batch.getSize()).isEqualTo(3);
    assertThat(batch.getDoubleData()[0]).isEqualTo(3.0);
    assertThat(batch.getDoubleData()[1]).isEqualTo(7.5);
    assertThat(batch.getDoubleData()[2]).isEqualTo(10.5);

    assertThat(scan.getNextBatch(batch)).isFalse();
  }

  @Test
  void testColumnarPropertyIntegration() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").set("age", 30).save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").set("age", 25).save();
    final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").save();
    alice.newEdge("KNOWS", bob);
    bob.newEdge("KNOWS", charlie);
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(null, null);

    final int idAlice = gav.getNodeId(alice.getIdentity());
    final int idBob = gav.getNodeId(bob.getIdentity());
    final int idCharlie = gav.getNodeId(charlie.getIdentity());

    assertThat(gav.getProperty(idAlice, "name")).isEqualTo("Alice");
    assertThat(gav.getProperty(idBob, "name")).isEqualTo("Bob");
    assertThat(gav.getProperty(idCharlie, "name")).isEqualTo("Charlie");

    assertThat(gav.getProperty(idAlice, "age")).isEqualTo(30);
    assertThat(gav.getProperty(idBob, "age")).isEqualTo(25);
    assertThat(gav.getProperty(idCharlie, "age")).isNull();
  }

  @Test
  void testColumnarMultipleTypes() {
    database.getSchema().createVertexType("Item");
    database.getSchema().createEdgeType("RELATED");

    database.begin();
    final MutableVertex v1 = database.newVertex("Item")
        .set("name", "Widget").set("price", 19.99).set("quantity", 100).set("sku", 12345L).save();
    final MutableVertex v2 = database.newVertex("Item")
        .set("name", "Gadget").set("price", 29.99).set("quantity", 50).set("sku", 67890L).save();
    v1.newEdge("RELATED", v2);
    database.commit();

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(null, null);

    final int id1 = gav.getNodeId(v1.getIdentity());
    final int id2 = gav.getNodeId(v2.getIdentity());

    assertThat(gav.getProperty(id1, "name")).isEqualTo("Widget");
    assertThat(gav.getProperty(id1, "price")).isEqualTo(19.99);
    assertThat(gav.getProperty(id1, "quantity")).isEqualTo(100);
    assertThat(gav.getProperty(id1, "sku")).isEqualTo(12345L);

    assertThat(gav.getProperty(id2, "name")).isEqualTo("Gadget");
    assertThat(gav.getProperty(id2, "price")).isEqualTo(29.99);
  }

  @Test
  void testNullBitset() {
    final Column col = new Column("val", Column.Type.INT, 128);

    for (int i = 0; i < 128; i++)
      assertThat(col.isNull(i)).isTrue();
    assertThat(col.countNonNull()).isEqualTo(0);

    for (int i = 0; i < 128; i += 2)
      col.setInt(i, i);

    assertThat(col.countNonNull()).isEqualTo(64);
    assertThat(col.isNull(0)).isFalse();
    assertThat(col.isNull(1)).isTrue();

    col.setNull(0);
    assertThat(col.isNull(0)).isTrue();
    assertThat(col.countNonNull()).isEqualTo(63);
  }
}
