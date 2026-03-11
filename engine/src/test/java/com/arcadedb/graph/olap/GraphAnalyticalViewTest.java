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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
  void testAutoUpdate() throws Exception {
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
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();

    assertThat(gav.isAutoUpdate()).isTrue();
    assertThat(gav.getNodeCount()).isEqualTo(2);
    assertThat(gav.getEdgeCount()).isEqualTo(1);

    // Add a new vertex and edge
    database.begin();
    final MutableVertex c = database.newVertex("Person").set("name", "Charlie").save();
    b.newEdge("FOLLOWS", c);
    database.commit();

    // SYNCHRONOUS: overlay applied immediately after commit
    assertThat(gav.getNodeCount()).isEqualTo(3);
    assertThat(gav.getEdgeCount()).isEqualTo(2);

    final int idC = gav.getNodeId(c.getIdentity());
    assertThat(idC).isGreaterThanOrEqualTo(0);
    assertThat(gav.getProperty(idC, "name")).isEqualTo("Charlie");

    gav.close();
  }

  // --- Status and async build tests ---

  @Test
  void testStatusLifecycle() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .buildAsync();

    // Initial status before build completes
    assertThat(gav.getStatus()).isIn(GraphAnalyticalView.Status.BUILDING, GraphAnalyticalView.Status.READY);

    // Wait for build to complete
    assertThat(gav.awaitReady(5, TimeUnit.SECONDS)).isTrue();
    assertThat(gav.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(gav.getNodeCount()).isEqualTo(0);

    gav.close();
  }

  @Test
  void testAsyncBuildWithData() throws Exception {
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
        .buildAsync();

    assertThat(gav.awaitReady(5, TimeUnit.SECONDS)).isTrue();
    assertThat(gav.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(gav.getNodeCount()).isEqualTo(2);
    assertThat(gav.getEdgeCount()).isEqualTo(1);

    final int idA = gav.getNodeId(a.getIdentity());
    assertThat(gav.getProperty(idA, "name")).isEqualTo("Alice");

    gav.close();
  }

  @Test
  void testStatusNotBuilt() {
    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    assertThat(gav.getStatus()).isEqualTo(GraphAnalyticalView.Status.NOT_BUILT);
  }

  @Test
  void testSyncBuildSetsReady() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .build();

    assertThat(gav.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
  }

  @Test
  void testAsyncAutoUpdate() throws Exception {
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
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .buildAsync();

    assertThat(gav.awaitReady(5, TimeUnit.SECONDS)).isTrue();
    assertThat(gav.getNodeCount()).isEqualTo(2);

    // Add more data — SYNCHRONOUS mode applies overlay immediately
    database.begin();
    final MutableVertex c = database.newVertex("Person").set("name", "Charlie").save();
    b.newEdge("FOLLOWS", c);
    database.commit();

    assertThat(gav.getNodeCount()).isEqualTo(3);

    gav.close();
  }

  // --- Registry tests ---

  @Test
  void testRegistryRegisterAndGet() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("social-graph")
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .build();

    assertThat(GraphAnalyticalViewRegistry.get(database, "social-graph")).isSameAs(gav);
    assertThat(GraphAnalyticalViewRegistry.getAll(database)).hasSize(1);

    gav.close();
    assertThat(GraphAnalyticalViewRegistry.get(database, "social-graph")).isNull();
  }

  @Test
  void testRegistryMultipleViews() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");
    database.getSchema().createEdgeType("BLOCKS");

    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex b = database.newVertex("Person").set("name", "Bob").save();
    a.newEdge("FOLLOWS", b);
    a.newEdge("BLOCKS", b);
    database.commit();

    final GraphAnalyticalView follows = GraphAnalyticalView.builder(database)
        .withName("follows-view")
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .build();

    final GraphAnalyticalView blocks = GraphAnalyticalView.builder(database)
        .withName("blocks-view")
        .withVertexTypes("Person")
        .withEdgeTypes("BLOCKS")
        .build();

    assertThat(GraphAnalyticalViewRegistry.getAll(database)).hasSize(2);
    assertThat(GraphAnalyticalViewRegistry.get(database, "follows-view")).isSameAs(follows);
    assertThat(GraphAnalyticalViewRegistry.get(database, "blocks-view")).isSameAs(blocks);
    assertThat(follows.getEdgeCount("FOLLOWS")).isEqualTo(1);
    assertThat(blocks.getEdgeCount("BLOCKS")).isEqualTo(1);

    follows.close();
    blocks.close();
  }

  @Test
  void testGavName() {
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("test-view")
        .build();

    assertThat(gav.getName()).isEqualTo("test-view");
    gav.close();
  }

  @Test
  void testGavConfigAccessors() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("config-test")
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .withProperties("name", "age")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();

    assertThat(gav.getName()).isEqualTo("config-test");
    assertThat(gav.getVertexTypes()).containsExactly("Person");
    assertThat(gav.getEdgeTypeFilter()).containsExactly("FOLLOWS");
    assertThat(gav.getPropertyFilter()).containsExactly("name", "age");
    assertThat(gav.isAutoUpdate()).isTrue();

    gav.close();
  }

  // --- Schema persistence tests ---

  @Test
  void testPersistenceRoundTrip() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    database.newVertex("Person").set("name", "Alice").save();
    database.newVertex("Person").set("name", "Bob").save();
    database.commit();

    // Create a named GAV — should auto-save to schema
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("social-persist")
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .withProperties("name")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();

    // Verify it's persisted in schema extensions
    final com.arcadedb.serializer.json.JSONObject ext = database.getSchema().getExtension("graphAnalyticalViews");
    assertThat(ext).isNotNull();
    assertThat(ext.has("social-persist")).isTrue();

    final com.arcadedb.serializer.json.JSONObject gavDef = ext.getJSONObject("social-persist");
    assertThat(gavDef.getString("name")).isEqualTo("social-persist");
    assertThat(gavDef.getString("updateMode")).isEqualTo("SYNCHRONOUS");

    // Close removes from schema
    gav.close();
    final com.arcadedb.serializer.json.JSONObject extAfter = database.getSchema().getExtension("graphAnalyticalViews");
    assertThat(extAfter).isNull();
  }

  @Test
  void testRestoreFromSchema() throws Exception {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    final var alice = database.newVertex("Person").set("name", "Alice").save();
    final var bob = database.newVertex("Person").set("name", "Bob").save();
    alice.newEdge("FOLLOWS", bob);
    database.commit();

    // Create a named GAV
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("restore-test")
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .build();

    assertThat(gav.getNodeCount()).isEqualTo(2);

    // Simulate "unload" — close without removing from schema
    GraphTraversalProviderRegistry.clearAll(database);
    GraphAnalyticalViewRegistry.unregister(database, "restore-test");

    // Restore from schema — should rebuild asynchronously
    final int restored = GraphAnalyticalViewPersistence.restoreAll(database);
    assertThat(restored).isEqualTo(1);

    // Wait for async rebuild
    final GraphAnalyticalView restoredGav = GraphAnalyticalViewRegistry.get(database, "restore-test");
    assertThat(restoredGav).isNotNull();
    assertThat(restoredGav.awaitReady(5, TimeUnit.SECONDS)).isTrue();
    assertThat(restoredGav.getNodeCount()).isEqualTo(2);
    assertThat(restoredGav.getEdgeCount()).isEqualTo(1);

    // Clean up both
    restoredGav.close();
  }

  @Test
  void testSchemaExtensionGeneric() {
    // Test the generic extension mechanism
    assertThat(database.getSchema().getExtension("nonexistent")).isNull();

    final com.arcadedb.serializer.json.JSONObject testExt = new com.arcadedb.serializer.json.JSONObject();
    testExt.put("key", "value");
    database.getSchema().setExtension("testModule", testExt);

    final com.arcadedb.serializer.json.JSONObject loaded = database.getSchema().getExtension("testModule");
    assertThat(loaded).isNotNull();
    assertThat(loaded.getString("key")).isEqualTo("value");

    // Remove
    database.getSchema().setExtension("testModule", null);
    assertThat(database.getSchema().getExtension("testModule")).isNull();
  }

  // --- Traversal provider integration tests ---

  @Test
  void testTraversalProviderRegistration() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    // Clear any providers left by previous tests
    GraphTraversalProviderRegistry.clearAll(database);

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .build();

    // GAV should auto-register as a traversal provider
    assertThat(GraphTraversalProviderRegistry.getProviders(database)).hasSize(1);
    assertThat(GraphTraversalProviderRegistry.findProvider(database, "FOLLOWS")).isSameAs(gav);

    // Provider should report correct coverage
    assertThat(gav.coversVertexType("Person")).isTrue();
    assertThat(gav.coversVertexType("Company")).isFalse();
    assertThat(gav.coversEdgeType("FOLLOWS")).isTrue();
    assertThat(gav.coversEdgeType("BLOCKS")).isFalse();

    gav.close();
    assertThat(GraphTraversalProviderRegistry.getProviders(database)).isEmpty();
  }

  @Test
  void testTraversalProviderAllTypes() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    // GAV with no type filters (covers all types)
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database).build();

    assertThat(gav.coversVertexType(null)).isTrue();
    assertThat(gav.coversVertexType("Person")).isTrue();
    assertThat(gav.coversVertexType("Anything")).isTrue();
    assertThat(gav.coversEdgeType(null)).isTrue();
    assertThat(gav.coversEdgeType("FOLLOWS")).isTrue();

    gav.close();
  }

  @Test
  void testCypherQueryWithGAV() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").save();
    alice.newEdge("KNOWS", bob);
    bob.newEdge("KNOWS", charlie);
    database.commit();

    // Build GAV covering KNOWS edges
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("social")
        .withEdgeTypes("KNOWS")
        .build();

    // Run a Cypher query — the optimizer should use GAVExpandAll when edge variable is not captured
    final ResultSet rs = database.command("cypher",
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS source, b.name AS target ORDER BY source, target");

    final List<String> results = new ArrayList<>();
    while (rs.hasNext()) {
      final var r = rs.next();
      results.add(r.getProperty("source") + "->" + r.getProperty("target"));
    }
    rs.close();

    assertThat(results).containsExactly("Alice->Bob", "Bob->Charlie");

    gav.close();
  }

  @Test
  void testCypherQueryWithEdgeVariable() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    alice.newEdge("KNOWS", bob);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("social2")
        .withEdgeTypes("KNOWS")
        .build();

    // When edge variable is captured [r:KNOWS], GAV can't be used (no edge objects in CSR)
    // Query should still work correctly via standard OLTP path
    final ResultSet rs = database.command("cypher",
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS source, b.name AS target");

    final List<String> results = new ArrayList<>();
    while (rs.hasNext()) {
      final var r = rs.next();
      results.add(r.getProperty("source") + "->" + r.getProperty("target"));
    }
    rs.close();

    assertThat(results).containsExactly("Alice->Bob");

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

  // --- SQL integration tests (Phase 6) ---

  @Test
  void testSqlOutWithGAV() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").save();
    alice.newEdge("KNOWS", bob);
    alice.newEdge("KNOWS", charlie);
    bob.newEdge("KNOWS", charlie);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("sql-out-test")
        .withEdgeTypes("KNOWS")
        .build();

    // SQL out() should be accelerated by GAV
    final ResultSet rs = database.query("sql", "SELECT name, out('KNOWS').size() AS friends FROM Person ORDER BY name");

    final List<String> results = new ArrayList<>();
    while (rs.hasNext()) {
      final var r = rs.next();
      results.add(r.getProperty("name") + ":" + r.getProperty("friends"));
    }
    rs.close();

    assertThat(results).containsExactly("Alice:2", "Bob:1", "Charlie:0");

    // Verify CSR acceleration is visible in PROFILE output
    final ResultSet profileRs = database.command("sql",
        "PROFILE SELECT name, out('KNOWS').size() AS friends FROM Person ORDER BY name");
    assertThat(profileRs.getExecutionPlan().isPresent()).isTrue();
    final String prettyPrint = profileRs.getExecutionPlan().get().prettyPrint(0, 2);
    assertThat(prettyPrint).contains("CSR-accelerated");
    profileRs.close();

    gav.close();
  }

  @Test
  void testSqlInWithGAV() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").save();
    alice.newEdge("KNOWS", bob);
    alice.newEdge("KNOWS", charlie);
    bob.newEdge("KNOWS", charlie);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("sql-in-test")
        .withEdgeTypes("KNOWS")
        .build();

    // SQL in() should be accelerated by GAV
    final ResultSet rs = database.query("sql", "SELECT name, in('KNOWS').size() AS inbound FROM Person ORDER BY name");

    final List<String> results = new ArrayList<>();
    while (rs.hasNext()) {
      final var r = rs.next();
      results.add(r.getProperty("name") + ":" + r.getProperty("inbound"));
    }
    rs.close();

    assertThat(results).containsExactly("Alice:0", "Bob:1", "Charlie:2");

    gav.close();
  }

  @Test
  void testSqlBothWithGAV() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").save();
    alice.newEdge("KNOWS", bob);
    bob.newEdge("KNOWS", charlie);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("sql-both-test")
        .withEdgeTypes("KNOWS")
        .build();

    // SQL both() should return neighbors in both directions
    final ResultSet rs = database.query("sql", "SELECT name, both('KNOWS').size() AS neighbors FROM Person ORDER BY name");

    final List<String> results = new ArrayList<>();
    while (rs.hasNext()) {
      final var r = rs.next();
      results.add(r.getProperty("name") + ":" + r.getProperty("neighbors"));
    }
    rs.close();

    // Alice->Bob, Bob->Charlie: Alice has 1 (Bob), Bob has 2 (Alice+Charlie), Charlie has 1 (Bob)
    assertThat(results).containsExactly("Alice:1", "Bob:2", "Charlie:1");

    gav.close();
  }

  @Test
  void testSqlOutWithEdgeTypeFilter() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("WORKS_WITH");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").save();
    alice.newEdge("KNOWS", bob);
    alice.newEdge("WORKS_WITH", charlie);
    database.commit();

    // GAV covers only KNOWS
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("sql-filter-test")
        .withEdgeTypes("KNOWS")
        .build();

    // Query for KNOWS — should use GAV
    ResultSet rs = database.query("sql", "SELECT name, out('KNOWS').size() AS cnt FROM Person WHERE name = 'Alice'");
    assertThat(rs.hasNext()).isTrue();
    assertThat((int) rs.next().getProperty("cnt")).isEqualTo(1);
    rs.close();

    // Query for WORKS_WITH — GAV doesn't cover this, falls back to OLTP
    rs = database.query("sql", "SELECT name, out('WORKS_WITH').size() AS cnt FROM Person WHERE name = 'Alice'");
    assertThat(rs.hasNext()).isTrue();
    assertThat((int) rs.next().getProperty("cnt")).isEqualTo(1);
    rs.close();

    gav.close();
  }

  @Test
  void testSqlOutENotAccelerated() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    alice.newEdge("KNOWS", bob);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("sql-oute-test")
        .withEdgeTypes("KNOWS")
        .build();

    // outE() returns edges, which CSR doesn't store — should still work via OLTP path
    final ResultSet rs = database.query("sql", "SELECT outE('KNOWS').size() AS cnt FROM Person WHERE name = 'Alice'");
    assertThat(rs.hasNext()).isTrue();
    assertThat((int) rs.next().getProperty("cnt")).isEqualTo(1);
    rs.close();

    gav.close();
  }

  @Test
  void testSqlTraversalWithoutGAV() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    alice.newEdge("KNOWS", bob);
    database.commit();

    // No GAV built — standard OLTP path should work fine
    final ResultSet rs = database.query("sql", "SELECT name, out('KNOWS').size() AS cnt FROM Person ORDER BY name");

    final List<String> results = new ArrayList<>();
    while (rs.hasNext()) {
      final var r = rs.next();
      results.add(r.getProperty("name") + ":" + r.getProperty("cnt"));
    }
    rs.close();

    assertThat(results).containsExactly("Alice:1", "Bob:0");
  }

  // --- Algorithm CSR acceleration tests ---

  @Test
  void testPageRankWithGAV() {
    database.getSchema().createVertexType("Page");
    database.getSchema().createEdgeType("LINKS");

    database.begin();
    final MutableVertex a = database.newVertex("Page").set("name", "A").save();
    final MutableVertex b = database.newVertex("Page").set("name", "B").save();
    final MutableVertex c = database.newVertex("Page").set("name", "C").save();
    a.newEdge("LINKS", b);
    a.newEdge("LINKS", c);
    b.newEdge("LINKS", c);
    c.newEdge("LINKS", a);
    database.commit();

    // Build GAV covering all types (no filter) — algorithms should use CSR path
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("pagerank-test")
        .build();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank() YIELD node, score RETURN node.name AS name, score ORDER BY score DESC");

    final List<String> names = new ArrayList<>();
    double totalScore = 0;
    while (rs.hasNext()) {
      final var r = rs.next();
      names.add((String) r.getProperty("name"));
      totalScore += ((Number) r.getProperty("score")).doubleValue();
    }
    rs.close();

    assertThat(names).hasSize(3);
    // C has most incoming links (from A and B), should rank highest
    assertThat(names.getFirst()).isEqualTo("C");
    // Scores should sum to ~1.0
    assertThat(totalScore).isBetween(0.9, 1.1);

    // Verify CSR acceleration is visible in PROFILE output
    final ResultSet profileRs = database.command("opencypher",
        "PROFILE CALL algo.pagerank() YIELD node, score RETURN node.name AS name, score");
    assertThat(profileRs.getExecutionPlan().isPresent()).isTrue();
    final String prettyPrint = profileRs.getExecutionPlan().get().prettyPrint(0, 2);
    assertThat(prettyPrint).contains("CSR-accelerated");
    profileRs.close();

    gav.close();
  }

  @Test
  void testArticleRankWithGAV() {
    database.getSchema().createVertexType("Page");
    database.getSchema().createEdgeType("LINKS");

    database.begin();
    final MutableVertex a = database.newVertex("Page").set("name", "A").save();
    final MutableVertex b = database.newVertex("Page").set("name", "B").save();
    final MutableVertex c = database.newVertex("Page").set("name", "C").save();
    a.newEdge("LINKS", b);
    a.newEdge("LINKS", c);
    b.newEdge("LINKS", c);
    c.newEdge("LINKS", a);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("articlerank-test")
        .build();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.articlerank() YIELD node, score RETURN node.name AS name, score ORDER BY score DESC");

    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add((String) rs.next().getProperty("name"));
    rs.close();

    assertThat(names).hasSize(3);
    // C has most incoming links, should rank highest (same topology as PageRank)
    assertThat(names.getFirst()).isEqualTo("C");

    gav.close();
  }

  @Test
  void testPersonalizedPageRankWithGAV() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").save();
    alice.newEdge("KNOWS", bob);
    bob.newEdge("KNOWS", charlie);
    charlie.newEdge("KNOWS", alice);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("ppr-test")
        .build();

    // PPR from Alice: Alice should have highest score (source node)
    final ResultSet rs = database.query("opencypher",
        "MATCH (s:Person {name:'Alice'}) CALL algo.personalizedPageRank(s) YIELD nodeId, score RETURN nodeId, score ORDER BY score DESC");

    final List<Double> scores = new ArrayList<>();
    while (rs.hasNext())
      scores.add(((Number) rs.next().getProperty("score")).doubleValue());
    rs.close();

    assertThat(scores).hasSize(3);
    // Source node should have highest PPR score
    assertThat(scores.getFirst()).isGreaterThan(scores.get(1));

    gav.close();
  }

  @Test
  void testPageRankWithPartialGAVFallsBackToOLTP() {
    database.getSchema().createVertexType("Page");
    database.getSchema().createEdgeType("LINKS");
    database.getSchema().createEdgeType("CITES");

    database.begin();
    final MutableVertex a = database.newVertex("Page").set("name", "A").save();
    final MutableVertex b = database.newVertex("Page").set("name", "B").save();
    a.newEdge("LINKS", b);
    a.newEdge("CITES", b);
    database.commit();

    // GAV only covers LINKS, not all types — algorithms should fall back to OLTP
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("partial-test")
        .withEdgeTypes("LINKS")
        .build();

    // Should still produce correct results via OLTP path
    final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank() YIELD node, score RETURN node.name AS name, score");

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    rs.close();

    assertThat(count).isEqualTo(2);

    gav.close();
  }

  // --- SQL shortestPath CSR acceleration ---

  @Test
  void testSqlShortestPathWithGAV() {
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("ROAD");

    // Chain: A → B → C → D
    database.begin();
    final MutableVertex a = database.newVertex("City").set("name", "A").save();
    final MutableVertex b = database.newVertex("City").set("name", "B").save();
    final MutableVertex c = database.newVertex("City").set("name", "C").save();
    final MutableVertex d = database.newVertex("City").set("name", "D").save();
    a.newEdge("ROAD", b);
    b.newEdge("ROAD", c);
    c.newEdge("ROAD", d);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("shortest-path-test")
        .build();

    // shortestPath should find path A → B → C → D (length 4 vertices including endpoints)
    final ResultSet rs = database.query("sql",
        "SELECT shortestPath(" + a.getIdentity() + ", " + d.getIdentity() + ", 'OUT', 'ROAD') AS path");

    assertThat(rs.hasNext()).isTrue();
    final List<?> path = rs.next().getProperty("path");
    rs.close();

    assertThat(path).hasSize(4); // A, B, C, D
    assertThat(path.get(0)).isEqualTo(a.getIdentity());
    assertThat(path.get(3)).isEqualTo(d.getIdentity());

    // Verify CSR acceleration is visible in PROFILE output
    final ResultSet profileRs = database.command("sql",
        "PROFILE SELECT shortestPath(" + a.getIdentity() + ", " + d.getIdentity() + ", 'OUT', 'ROAD') AS path");
    assertThat(profileRs.hasNext()).isTrue();
    assertThat(profileRs.getExecutionPlan().isPresent()).isTrue();
    final String prettyPrint = profileRs.getExecutionPlan().get().prettyPrint(0, 2);
    assertThat(prettyPrint).contains("CSR-accelerated");
    profileRs.close();

    gav.close();
  }

  // --- Algorithm CSR acceleration tests (additional algorithms) ---

  @Test
  void testBetweennessCentralityWithGAV() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Star topology with bidirectional edges: center has highest betweenness
    database.begin();
    final MutableVertex center = database.newVertex("Node").set("name", "Center").save();
    final MutableVertex a = database.newVertex("Node").set("name", "A").save();
    final MutableVertex b = database.newVertex("Node").set("name", "B").save();
    final MutableVertex c = database.newVertex("Node").set("name", "C").save();
    center.newEdge("LINK", a); a.newEdge("LINK", center);
    center.newEdge("LINK", b); b.newEdge("LINK", center);
    center.newEdge("LINK", c); c.newEdge("LINK", center);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("betweenness-test")
        .build();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.betweenness() YIELD node, score RETURN node, score ORDER BY score DESC");

    final List<Double> scores = new ArrayList<>();
    while (rs.hasNext())
      scores.add(((Number) rs.next().getProperty("score")).doubleValue());
    rs.close();

    assertThat(scores).hasSize(4);
    // Center node should have highest betweenness (it mediates all paths)
    assertThat(scores.getFirst()).isGreaterThan(scores.get(1));

    gav.close();
  }

  @Test
  void testWCCWithGAV() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Two disconnected components: {A,B} and {C,D}
    database.begin();
    final MutableVertex a = database.newVertex("Node").set("name", "A").save();
    final MutableVertex b = database.newVertex("Node").set("name", "B").save();
    final MutableVertex c = database.newVertex("Node").set("name", "C").save();
    final MutableVertex d = database.newVertex("Node").set("name", "D").save();
    a.newEdge("LINK", b);
    c.newEdge("LINK", d);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("wcc-test")
        .build();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.wcc() YIELD nodeId, componentId RETURN nodeId, componentId");

    final List<Integer> componentIds = new ArrayList<>();
    while (rs.hasNext())
      componentIds.add(((Number) rs.next().getProperty("componentId")).intValue());
    rs.close();

    assertThat(componentIds).hasSize(4);
    // Should have exactly 2 distinct components
    assertThat(componentIds.stream().distinct().count()).isEqualTo(2);

    gav.close();
  }

  @Test
  void testTriangleCountWithGAV() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Triangle: A-B-C-A
    database.begin();
    final MutableVertex a = database.newVertex("Node").set("name", "A").save();
    final MutableVertex b = database.newVertex("Node").set("name", "B").save();
    final MutableVertex c = database.newVertex("Node").set("name", "C").save();
    a.newEdge("LINK", b);
    b.newEdge("LINK", c);
    c.newEdge("LINK", a);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("triangle-test")
        .build();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.triangleCount() YIELD nodeId, triangles RETURN nodeId, triangles");

    int totalTriangles = 0;
    int nodeCount = 0;
    while (rs.hasNext()) {
      totalTriangles += ((Number) rs.next().getProperty("triangles")).intValue();
      nodeCount++;
    }
    rs.close();

    assertThat(nodeCount).isEqualTo(3);
    // Each node participates in 1 triangle, sum = 3, total unique = 3/3 = 1
    assertThat(totalTriangles).isEqualTo(3);

    gav.close();
  }

  @Test
  void testLouvainWithGAV() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Two dense clusters with weak bridge
    database.begin();
    final MutableVertex a1 = database.newVertex("Node").set("name", "A1").save();
    final MutableVertex a2 = database.newVertex("Node").set("name", "A2").save();
    final MutableVertex a3 = database.newVertex("Node").set("name", "A3").save();
    final MutableVertex b1 = database.newVertex("Node").set("name", "B1").save();
    final MutableVertex b2 = database.newVertex("Node").set("name", "B2").save();
    final MutableVertex b3 = database.newVertex("Node").set("name", "B3").save();
    // Cluster A: fully connected
    a1.newEdge("LINK", a2); a2.newEdge("LINK", a1);
    a1.newEdge("LINK", a3); a3.newEdge("LINK", a1);
    a2.newEdge("LINK", a3); a3.newEdge("LINK", a2);
    // Cluster B: fully connected
    b1.newEdge("LINK", b2); b2.newEdge("LINK", b1);
    b1.newEdge("LINK", b3); b3.newEdge("LINK", b1);
    b2.newEdge("LINK", b3); b3.newEdge("LINK", b2);
    // Bridge
    a3.newEdge("LINK", b1); b1.newEdge("LINK", a3);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("louvain-test")
        .build();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.louvain() YIELD node, communityId, modularity RETURN node, communityId");

    final List<Integer> communities = new ArrayList<>();
    while (rs.hasNext())
      communities.add(((Number) rs.next().getProperty("communityId")).intValue());
    rs.close();

    assertThat(communities).hasSize(6);
    // Should detect at least 2 communities
    assertThat(communities.stream().distinct().count()).isGreaterThanOrEqualTo(2);

    gav.close();
  }

  @Test
  void testBFSWithGAV() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    final MutableVertex a = database.newVertex("Node").set("name", "A").save();
    final MutableVertex b = database.newVertex("Node").set("name", "B").save();
    final MutableVertex c = database.newVertex("Node").set("name", "C").save();
    a.newEdge("LINK", b);
    b.newEdge("LINK", c);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("bfs-test")
        .build();

    final ResultSet rs = database.query("opencypher",
        "MATCH (s:Node {name:'A'}) CALL algo.bfs(s) YIELD node, depth RETURN node, depth ORDER BY depth");

    final List<Integer> depths = new ArrayList<>();
    while (rs.hasNext())
      depths.add(((Number) rs.next().getProperty("depth")).intValue());
    rs.close();

    // BFS does not include the start node itself, so depths start at 1
    assertThat(depths).containsExactly(1, 2);

    gav.close();
  }

  @Test
  void testSCCWithGAV() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Cycle A→B→C→A forms one SCC, D is separate
    database.begin();
    final MutableVertex a = database.newVertex("Node").set("name", "A").save();
    final MutableVertex b = database.newVertex("Node").set("name", "B").save();
    final MutableVertex c = database.newVertex("Node").set("name", "C").save();
    final MutableVertex d = database.newVertex("Node").set("name", "D").save();
    a.newEdge("LINK", b);
    b.newEdge("LINK", c);
    c.newEdge("LINK", a);
    a.newEdge("LINK", d);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("scc-test")
        .build();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.scc() YIELD node, componentId RETURN node, componentId");

    final List<Integer> components = new ArrayList<>();
    while (rs.hasNext())
      components.add(((Number) rs.next().getProperty("componentId")).intValue());
    rs.close();

    assertThat(components).hasSize(4);
    // Should have 2 SCCs: {A,B,C} and {D}
    assertThat(components.stream().distinct().count()).isEqualTo(2);

    gav.close();
  }

  @Test
  void testEigenvectorCentralityWithGAV() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    final MutableVertex a = database.newVertex("Node").set("name", "A").save();
    final MutableVertex b = database.newVertex("Node").set("name", "B").save();
    final MutableVertex c = database.newVertex("Node").set("name", "C").save();
    a.newEdge("LINK", b); b.newEdge("LINK", a);
    a.newEdge("LINK", c); c.newEdge("LINK", a);
    b.newEdge("LINK", c); c.newEdge("LINK", b);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("eigenvector-test")
        .build();

    final ResultSet rs = database.query("opencypher",
        "CALL algo.eigenvector() YIELD node, score RETURN node, score");

    int count = 0;
    while (rs.hasNext()) {
      final double score = ((Number) rs.next().getProperty("score")).doubleValue();
      assertThat(score).isGreaterThan(0.0);
      count++;
    }
    rs.close();

    assertThat(count).isEqualTo(3);

    gav.close();
  }

  // ── SQL DDL Tests ────────────────────────────────────────────────────────

  @Test
  void testCreateGraphAnalyticalViewSQL() {
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("ROAD");
    database.begin();
    final MutableVertex a = database.newVertex("City").set("name", "Rome").save();
    final MutableVertex b = database.newVertex("City").set("name", "Milan").save();
    a.newEdge("ROAD", b);
    database.commit();


    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW cityRoads VERTEX TYPES (City) EDGE TYPES (ROAD)");

    // Verify it's persisted in schema
    final var extension = database.getSchema().getExtension("graphAnalyticalViews");
    assertThat(extension).isNotNull();
    assertThat(extension.has("cityRoads")).isTrue();
    assertThat(extension.getJSONObject("cityRoads").getString("name")).isEqualTo("cityRoads");

    // Verify it shows up in schema:graphAnalyticalViews
    final ResultSet rs = database.query("sql", "SELECT FROM schema:graphAnalyticalViews");
    boolean found = false;
    while (rs.hasNext()) {
      final var result = rs.next();
      if ("cityRoads".equals(result.getProperty("name"))) {
        found = true;
        break;
      }
    }
    rs.close();
    assertThat(found).isTrue();

    // Clean up
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW cityRoads");
    final var afterDrop = database.getSchema().getExtension("graphAnalyticalViews");
    assertThat(afterDrop).isNull();
  }

  @Test
  void testCreateGraphAnalyticalViewWithAutoUpdate() {
    database.getSchema().createVertexType("Sensor");
    database.getSchema().createEdgeType("FEEDS");
    database.begin();
    database.newVertex("Sensor").set("name", "S1").save();
    database.commit();


    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW sensorNet VERTEX TYPES (Sensor) EDGE TYPES (FEEDS) AUTO UPDATE");

    final var extension = database.getSchema().getExtension("graphAnalyticalViews");
    assertThat(extension.getJSONObject("sensorNet").getString("updateMode")).isEqualTo("SYNCHRONOUS");

    database.command("sql", "DROP GRAPH ANALYTICAL VIEW sensorNet");
  }

  @Test
  void testCreateGraphAnalyticalViewIfNotExists() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");


    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW testView");

    // Second creation with IF NOT EXISTS should not throw
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW IF NOT EXISTS testView");

    // Cleanup
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW testView");
  }

  @Test
  void testDropGraphAnalyticalViewIfExists() {
    // Drop non-existent with IF EXISTS should not throw
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW IF EXISTS nonExistentView");
  }

  @Test
  void testCreateGraphAnalyticalViewWithProperties() {
    database.getSchema().createVertexType("Product");
    database.getSchema().createEdgeType("SIMILAR");
    database.begin();
    database.newVertex("Product").set("name", "Widget").set("price", 9.99).save();
    database.commit();


    database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW productGraph VERTEX TYPES (Product) EDGE TYPES (SIMILAR) PROPERTIES (name, price)");

    final var extension = database.getSchema().getExtension("graphAnalyticalViews");
    final var def = extension.getJSONObject("productGraph");
    assertThat(def.getJSONArray("propertyFilter").length()).isEqualTo(2);
    assertThat(def.getJSONArray("propertyFilter").getString(0)).isEqualTo("name");
    assertThat(def.getJSONArray("propertyFilter").getString(1)).isEqualTo("price");

    database.command("sql", "DROP GRAPH ANALYTICAL VIEW productGraph");
  }

  @Test
  void testSchemaGraphAnalyticalViewsQuery() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");


    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW socialGraph VERTEX TYPES (Person) EDGE TYPES (KNOWS) AUTO UPDATE");

    final ResultSet rs = database.query("sql", "SELECT FROM schema:graphAnalyticalViews WHERE name = 'socialGraph'");
    assertThat(rs.hasNext()).isTrue();
    final var result = rs.next();
    assertThat((String) result.getProperty("name")).isEqualTo("socialGraph");
    assertThat((String) result.getProperty("updateMode")).isEqualTo("SYNCHRONOUS");
    rs.close();

    database.command("sql", "DROP GRAPH ANALYTICAL VIEW socialGraph");
  }

  @Test
  void testGavRestoredOnDatabaseReopen() {
    // Setup: create types and data
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("ROAD");
    database.begin();
    final MutableVertex a = database.newVertex("City").set("name", "Rome").save();
    final MutableVertex b = database.newVertex("City").set("name", "Milan").save();
    a.newEdge("ROAD", b);
    database.commit();

    // Create GAV via SQL (persists definition to schema extensions)
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW cityRoads VERTEX TYPES (City) EDGE TYPES (ROAD)");

    // Verify schema extension was persisted
    assertThat(database.getSchema().getExtension("graphAnalyticalViews")).isNotNull();

    // Close database
    final String dbPath = database.getDatabasePath();
    database.close();

    // Reopen database — restoreAll is called during open()
    final DatabaseFactory factory2 = new DatabaseFactory(dbPath);
    final Database db2 = factory2.open();
    try {
      // Verify the GAV was restored in the in-memory registry
      final GraphAnalyticalView restored = GraphAnalyticalViewRegistry.get(db2, "cityRoads");
      assertThat(restored).isNotNull();
      assertThat(restored.getNodeCount()).isEqualTo(2);
      assertThat(restored.getEdgeCount()).isEqualTo(1);

      // Clean up
      db2.command("sql", "DROP GRAPH ANALYTICAL VIEW cityRoads");
    } finally {
      db2.close();
    }

    // Reopen database with the original factory for TestHelper cleanup
    database = new DatabaseFactory(dbPath).open();
  }

  // --- Incremental update (delta overlay) tests ---

  @Test
  void testIncrementalAddVertexAndEdge() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    alice.newEdge("FOLLOWS", bob);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("inc-add-test")
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .withProperties("name")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();

    assertThat(gav.getNodeCount()).isEqualTo(2);
    assertThat(gav.getEdgeCount()).isEqualTo(1);

    // Add a new vertex + edge in a new transaction
    database.begin();
    final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").save();
    bob.asVertex().modify().newEdge("FOLLOWS", charlie);
    database.commit();

    // SYNCHRONOUS: overlay applied immediately, no wait needed
    assertThat(gav.getNodeCount()).isEqualTo(3);
    assertThat(gav.getEdgeCount()).isEqualTo(2);

    // The new vertex should be resolvable
    final int charlieId = gav.getNodeId(charlie.getIdentity());
    assertThat(charlieId).isGreaterThanOrEqualTo(0);

    // Bob should now have Charlie as an out-neighbor
    final int bobId = gav.getNodeId(bob.getIdentity());
    final int[] bobOutNeighbors = gav.getVertices(bobId, Vertex.DIRECTION.OUT, "FOLLOWS");
    assertThat(bobOutNeighbors).contains(charlieId);

    gav.close();
  }

  @Test
  void testIncrementalPropertyUpdate() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").set("age", 30).save();
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("inc-prop-test")
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .withProperties("name", "age")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();

    final int aliceId = gav.getNodeId(alice.getIdentity());
    assertThat(gav.getProperty(aliceId, "name")).isEqualTo("Alice");
    assertThat(gav.getProperty(aliceId, "age")).isEqualTo(30);

    // Update property in a new transaction
    database.begin();
    alice.asVertex().modify().set("age", 31).save();
    database.commit();

    // SYNCHRONOUS: property updated via overlay immediately
    assertThat(gav.getProperty(aliceId, "age")).isEqualTo(31);
    assertThat(gav.getProperty(aliceId, "name")).isEqualTo("Alice"); // unchanged

    gav.close();
  }

  @Test
  void testIncrementalMultipleTransactions() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("inc-multi-tx")
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();

    assertThat(gav.getNodeCount()).isEqualTo(1);

    // First incremental tx
    database.begin();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    alice.asVertex().modify().newEdge("FOLLOWS", bob);
    database.commit();

    assertThat(gav.getNodeCount()).isEqualTo(2);
    assertThat(gav.getEdgeCount()).isEqualTo(1);

    // Second incremental tx
    database.begin();
    final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").save();
    bob.asVertex().modify().newEdge("FOLLOWS", charlie);
    database.commit();

    assertThat(gav.getNodeCount()).isEqualTo(3);
    assertThat(gav.getEdgeCount()).isEqualTo(2);

    // Verify connectivity
    final int aliceId = gav.getNodeId(alice.getIdentity());
    final int bobId = gav.getNodeId(bob.getIdentity());
    final int charlieId = gav.getNodeId(charlie.getIdentity());
    assertThat(gav.countEdges(bobId, Vertex.DIRECTION.OUT, "FOLLOWS")).isEqualTo(1);
    assertThat(gav.getVertices(bobId, Vertex.DIRECTION.OUT, "FOLLOWS")).contains(charlieId);
    assertThat(gav.getVertices(aliceId, Vertex.DIRECTION.OUT, "FOLLOWS")).contains(bobId);

    gav.close();
  }

  @Test
  void testIncrementalDeleteEdge() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    alice.newEdge("FOLLOWS", bob);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("inc-del-edge")
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();

    final int aliceId = gav.getNodeId(alice.getIdentity());
    final int bobId = gav.getNodeId(bob.getIdentity());
    assertThat(gav.isConnectedTo(aliceId, bobId, Vertex.DIRECTION.OUT, "FOLLOWS")).isTrue();
    assertThat(gav.getEdgeCount()).isEqualTo(1);

    // Delete the edge
    database.begin();
    alice.getIdentity().asVertex().getEdges(Vertex.DIRECTION.OUT, "FOLLOWS").forEach(e -> e.delete());
    database.commit();

    // SYNCHRONOUS: overlay reflects deletion immediately
    assertThat(gav.isConnectedTo(aliceId, bobId, Vertex.DIRECTION.OUT, "FOLLOWS")).isFalse();

    gav.close();
  }

  @Test
  void testIncrementalDeleteVertex() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    alice.newEdge("FOLLOWS", bob);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("inc-del-vertex")
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();

    assertThat(gav.getNodeCount()).isEqualTo(2);

    // Delete bob (cascade deletes edge too)
    database.begin();
    bob.getIdentity().asVertex().delete();
    database.commit();

    // SYNCHRONOUS: overlay reflects deletion immediately
    assertThat(gav.getNodeCount()).isEqualTo(1);

    gav.close();
  }

  @Test
  void testIncrementalNewVertexProperties() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("inc-new-props")
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .withProperties("name")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();

    // Add new vertex with properties via incremental update
    database.begin();
    final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
    database.commit();

    // SYNCHRONOUS: overlay reflects new vertex immediately
    final int bobId = gav.getNodeId(bob.getIdentity());
    assertThat(bobId).isGreaterThanOrEqualTo(0);

    // Property of overflow node should be accessible
    assertThat(gav.getProperty(bobId, "name")).isEqualTo("Bob");

    gav.close();
  }

  // --- STALE status tests ---

  @Test
  void testStaleStatusOnNonAutoUpdateGAV() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    database.newVertex("Person").set("name", "Alice").save();
    database.commit();

    // Build without autoUpdate
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("stale-test")
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .build();

    assertThat(gav.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(gav.isStale()).isFalse();
    assertThat(gav.getNodeCount()).isEqualTo(1);

    // Modify the graph — GAV should become STALE
    database.begin();
    database.newVertex("Person").set("name", "Bob").save();
    database.commit();

    assertThat(gav.getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);
    assertThat(gav.isStale()).isTrue();
    // Data is still accessible (stale but usable)
    assertThat(gav.getNodeCount()).isEqualTo(1); // still reflects old state

    // Rebuild to clear stale status
    gav.build();
    assertThat(gav.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(gav.isStale()).isFalse();
    assertThat(gav.getNodeCount()).isEqualTo(2); // now reflects new state

    gav.close();
  }

  @Test
  void testStaleNotTriggeredForUnrelatedTypes() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Product");
    database.getSchema().createEdgeType("FOLLOWS");

    database.begin();
    database.newVertex("Person").set("name", "Alice").save();
    database.commit();

    // GAV only covers Person/FOLLOWS
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("stale-unrelated")
        .withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS")
        .build();

    assertThat(gav.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);

    // Modify an unrelated type — GAV should NOT become stale
    database.begin();
    database.newVertex("Product").set("name", "Widget").save();
    database.commit();

    assertThat(gav.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(gav.isStale()).isFalse();

    gav.close();
  }
}
