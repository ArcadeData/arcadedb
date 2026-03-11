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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for graph algorithms operating on the CSR-based GraphAnalyticalView.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphAlgorithmsTest extends TestHelper {

  // --- PageRank ---

  @Test
  void testPageRankSimpleChain() {
    // A -> B -> C: C should have the highest rank
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
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .build();

    final double[] ranks = GraphAlgorithms.pageRank(gav, "LINK");

    assertThat(ranks).hasSize(3);
    final int aId = gav.getNodeId(a.getIdentity());
    final int bId = gav.getNodeId(b.getIdentity());
    final int cId = gav.getNodeId(c.getIdentity());

    // C (sink node, receives from B) should have highest rank
    assertThat(ranks[cId]).isGreaterThan(ranks[bId]);
    assertThat(ranks[bId]).isGreaterThan(ranks[aId]);

    // Ranks should sum to approximately 1.0
    double sum = 0;
    for (final double r : ranks)
      sum += r;
    assertThat(sum).isCloseTo(1.0, org.assertj.core.data.Offset.offset(0.01));
  }

  @Test
  void testPageRankStarGraph() {
    // Hub -> spoke1, spoke2, spoke3
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    final MutableVertex hub = database.newVertex("Node").set("name", "Hub").save();
    final MutableVertex s1 = database.newVertex("Node").set("name", "S1").save();
    final MutableVertex s2 = database.newVertex("Node").set("name", "S2").save();
    final MutableVertex s3 = database.newVertex("Node").set("name", "S3").save();
    hub.newEdge("LINK", s1);
    hub.newEdge("LINK", s2);
    hub.newEdge("LINK", s3);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .build();

    final double[] ranks = GraphAlgorithms.pageRank(gav, "LINK");

    final int hubId = gav.getNodeId(hub.getIdentity());
    final int s1Id = gav.getNodeId(s1.getIdentity());
    final int s2Id = gav.getNodeId(s2.getIdentity());

    // All spokes should have equal rank
    assertThat(ranks[s1Id]).isCloseTo(ranks[s2Id], org.assertj.core.data.Offset.offset(0.001));
  }

  @Test
  void testPageRankEmptyGraph() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .build();

    final double[] ranks = GraphAlgorithms.pageRank(gav, "LINK");
    assertThat(ranks).isEmpty();
  }

  // --- Connected Components ---

  @Test
  void testConnectedComponentsSingleComponent() {
    // A -- B -- C (all connected)
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
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .build();

    final int[] components = GraphAlgorithms.connectedComponents(gav, "LINK");

    assertThat(components).hasSize(3);
    // All nodes in the same component
    assertThat(components[gav.getNodeId(a.getIdentity())])
        .isEqualTo(components[gav.getNodeId(b.getIdentity())])
        .isEqualTo(components[gav.getNodeId(c.getIdentity())]);
    assertThat(GraphAlgorithms.countComponents(components)).isEqualTo(1);
  }

  @Test
  void testConnectedComponentsTwoComponents() {
    // {A -> B} and {C -> D} — two separate components
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    final MutableVertex a = database.newVertex("Node").set("name", "A").save();
    final MutableVertex b = database.newVertex("Node").set("name", "B").save();
    final MutableVertex c = database.newVertex("Node").set("name", "C").save();
    final MutableVertex d = database.newVertex("Node").set("name", "D").save();
    a.newEdge("LINK", b);
    c.newEdge("LINK", d);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .build();

    final int[] components = GraphAlgorithms.connectedComponents(gav, "LINK");

    assertThat(GraphAlgorithms.countComponents(components)).isEqualTo(2);
    // A and B in same component
    assertThat(components[gav.getNodeId(a.getIdentity())])
        .isEqualTo(components[gav.getNodeId(b.getIdentity())]);
    // C and D in same component
    assertThat(components[gav.getNodeId(c.getIdentity())])
        .isEqualTo(components[gav.getNodeId(d.getIdentity())]);
    // Different components
    assertThat(components[gav.getNodeId(a.getIdentity())])
        .isNotEqualTo(components[gav.getNodeId(c.getIdentity())]);
  }

  @Test
  void testConnectedComponentsIsolatedNodes() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    database.newVertex("Node").set("name", "A").save();
    database.newVertex("Node").set("name", "B").save();
    database.newVertex("Node").set("name", "C").save();
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .build();

    final int[] components = GraphAlgorithms.connectedComponents(gav, "LINK");
    assertThat(GraphAlgorithms.countComponents(components)).isEqualTo(3);
  }

  // --- Shortest Path ---

  @Test
  void testShortestPathDirect() {
    // A -> B -> C -> D
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    final MutableVertex a = database.newVertex("Node").set("name", "A").save();
    final MutableVertex b = database.newVertex("Node").set("name", "B").save();
    final MutableVertex c = database.newVertex("Node").set("name", "C").save();
    final MutableVertex d = database.newVertex("Node").set("name", "D").save();
    a.newEdge("LINK", b);
    b.newEdge("LINK", c);
    c.newEdge("LINK", d);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .build();

    final int aId = gav.getNodeId(a.getIdentity());
    final int bId = gav.getNodeId(b.getIdentity());
    final int cId = gav.getNodeId(c.getIdentity());
    final int dId = gav.getNodeId(d.getIdentity());

    assertThat(GraphAlgorithms.shortestPath(gav, aId, aId, Vertex.DIRECTION.OUT, "LINK")).isEqualTo(0);
    assertThat(GraphAlgorithms.shortestPath(gav, aId, bId, Vertex.DIRECTION.OUT, "LINK")).isEqualTo(1);
    assertThat(GraphAlgorithms.shortestPath(gav, aId, cId, Vertex.DIRECTION.OUT, "LINK")).isEqualTo(2);
    assertThat(GraphAlgorithms.shortestPath(gav, aId, dId, Vertex.DIRECTION.OUT, "LINK")).isEqualTo(3);

    // Reverse direction should not find path (directed edges)
    assertThat(GraphAlgorithms.shortestPath(gav, dId, aId, Vertex.DIRECTION.OUT, "LINK")).isEqualTo(-1);

    // BOTH direction should find path in reverse too
    assertThat(GraphAlgorithms.shortestPath(gav, dId, aId, Vertex.DIRECTION.BOTH, "LINK")).isEqualTo(3);
  }

  @Test
  void testShortestPathNoPath() {
    // A -> B, C (disconnected from A)
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    final MutableVertex a = database.newVertex("Node").set("name", "A").save();
    final MutableVertex b = database.newVertex("Node").set("name", "B").save();
    final MutableVertex c = database.newVertex("Node").set("name", "C").save();
    a.newEdge("LINK", b);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .build();

    final int aId = gav.getNodeId(a.getIdentity());
    final int cId = gav.getNodeId(c.getIdentity());
    assertThat(GraphAlgorithms.shortestPath(gav, aId, cId, Vertex.DIRECTION.OUT, "LINK")).isEqualTo(-1);
  }

  @Test
  void testShortestPathAll() {
    // A -> B -> C, A -> C (shortcut)
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    final MutableVertex a = database.newVertex("Node").set("name", "A").save();
    final MutableVertex b = database.newVertex("Node").set("name", "B").save();
    final MutableVertex c = database.newVertex("Node").set("name", "C").save();
    a.newEdge("LINK", b);
    b.newEdge("LINK", c);
    a.newEdge("LINK", c); // shortcut
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .build();

    final int aId = gav.getNodeId(a.getIdentity());
    final int bId = gav.getNodeId(b.getIdentity());
    final int cId = gav.getNodeId(c.getIdentity());

    final int[] dists = GraphAlgorithms.shortestPathAll(gav, aId, Vertex.DIRECTION.OUT, "LINK");
    assertThat(dists[aId]).isEqualTo(0);
    assertThat(dists[bId]).isEqualTo(1);
    assertThat(dists[cId]).isEqualTo(1); // shortcut: A->C is 1 hop
  }

  // --- Label Propagation ---

  @Test
  void testLabelPropagationTwoCommunities() {
    // Community 1: A -- B -- C (fully connected)
    // Community 2: D -- E -- F (fully connected)
    // Single bridge: C -> D
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    final MutableVertex a = database.newVertex("Node").set("name", "A").save();
    final MutableVertex b = database.newVertex("Node").set("name", "B").save();
    final MutableVertex c = database.newVertex("Node").set("name", "C").save();
    final MutableVertex d = database.newVertex("Node").set("name", "D").save();
    final MutableVertex e = database.newVertex("Node").set("name", "E").save();
    final MutableVertex f = database.newVertex("Node").set("name", "F").save();

    // Community 1: dense internal connections
    a.newEdge("LINK", b);
    b.newEdge("LINK", a);
    b.newEdge("LINK", c);
    c.newEdge("LINK", b);
    a.newEdge("LINK", c);
    c.newEdge("LINK", a);

    // Community 2: dense internal connections
    d.newEdge("LINK", e);
    e.newEdge("LINK", d);
    e.newEdge("LINK", f);
    f.newEdge("LINK", e);
    d.newEdge("LINK", f);
    f.newEdge("LINK", d);

    // Weak bridge
    c.newEdge("LINK", d);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .build();

    final int[] labels = GraphAlgorithms.labelPropagation(gav, "LINK");

    assertThat(labels).hasSize(6);
    final int aId = gav.getNodeId(a.getIdentity());
    final int bId = gav.getNodeId(b.getIdentity());
    final int cId = gav.getNodeId(c.getIdentity());
    final int dId = gav.getNodeId(d.getIdentity());
    final int eId = gav.getNodeId(e.getIdentity());
    final int fId = gav.getNodeId(f.getIdentity());

    // Nodes in the same community should share the same label
    assertThat(labels[aId]).isEqualTo(labels[bId]).isEqualTo(labels[cId]);
    assertThat(labels[dId]).isEqualTo(labels[eId]).isEqualTo(labels[fId]);
  }

  @Test
  void testLabelPropagationSingleNode() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    final MutableVertex a = database.newVertex("Node").set("name", "A").save();
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .build();

    final int[] labels = GraphAlgorithms.labelPropagation(gav, "LINK");
    assertThat(labels).hasSize(1);
    assertThat(labels[0]).isEqualTo(gav.getNodeId(a.getIdentity()));
  }

  @Test
  void testLabelPropagationEmptyGraph() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .build();

    final int[] labels = GraphAlgorithms.labelPropagation(gav, "LINK");
    assertThat(labels).isEmpty();
  }

  // --- Compaction Threshold (builder) ---

  @Test
  void testCompactionThresholdBuilder() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .withCompactionThreshold(5000)
        .build();

    assertThat(gav.getCompactionThreshold()).isEqualTo(5000);
    gav.drop();
  }
}
