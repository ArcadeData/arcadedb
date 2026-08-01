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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers {@link GraphEngine#getEdgesConnectedTo}, which yields only the edges reaching a known
 * neighbour by filtering on the neighbour pointer held in the edge segment, so the edges that do not
 * reach it are never materialised.
 * <p>
 * Every case is asserted against the unfiltered {@code getEdges} walk, because the filter is only
 * worth having if it is indistinguishable from iterating everything and comparing endpoints
 * afterwards - that is precisely what callers replace with it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class EdgesConnectedToTest {
  private static final String DB_PATH = "./target/databases/edgesConnectedTo";

  private Database database;

  @BeforeEach
  void setUp() {
    // a run killed mid-test leaves the directory behind, and create() would then throw on every later run
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> {
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("Knows");
      database.getSchema().createEdgeType("Owns");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void returnsOnlyTheEdgesReachingTheGivenNeighbour() {
    database.transaction(() -> {
      final MutableVertex hub = newNode("hub");
      final MutableVertex wanted = newNode("wanted");
      final MutableVertex other = newNode("other");

      final RID wantedEdge = hub.newEdge("Knows", wanted, "tag", "keep").save().getIdentity();
      hub.newEdge("Knows", other, "tag", "drop").save();
      hub.newEdge("Knows", other, "tag", "drop").save();

      assertThat(ridsOf(connectedTo(hub, Vertex.DIRECTION.OUT, wanted))).containsExactly(wantedEdge);
      // the precondition the filter is supposed to improve on: the unfiltered walk sees all three
      assertThat(ridsOf(hub.getEdges(Vertex.DIRECTION.OUT, "Knows").iterator())).hasSize(3);
    });
  }

  @Test
  void returnsEveryParallelEdgeBetweenThePair() {
    database.transaction(() -> {
      final MutableVertex hub = newNode("hub");
      final MutableVertex wanted = newNode("wanted");

      final RID first = hub.newEdge("Knows", wanted, "seq", 1).save().getIdentity();
      final RID second = hub.newEdge("Knows", wanted, "seq", 2).save().getIdentity();
      hub.newEdge("Knows", newNode("other"), "seq", 3).save();

      // a duplicate check has to see every parallel edge, not just the first one
      assertThat(ridsOf(connectedTo(hub, Vertex.DIRECTION.OUT, wanted))).containsExactlyInAnyOrder(first, second);
    });
  }

  @Test
  void honoursTheEdgeTypeFilter() {
    database.transaction(() -> {
      final MutableVertex hub = newNode("hub");
      final MutableVertex wanted = newNode("wanted");

      final RID knows = hub.newEdge("Knows", wanted).save().getIdentity();
      final RID owns = hub.newEdge("Owns", wanted).save().getIdentity();

      assertThat(ridsOf(connectedTo(hub, Vertex.DIRECTION.OUT, wanted, "Knows"))).containsExactly(knows);
      assertThat(ridsOf(connectedTo(hub, Vertex.DIRECTION.OUT, wanted, "Owns"))).containsExactly(owns);
      // no type filter at all takes the untyped iterator, which must filter on the neighbour just the same
      assertThat(ridsOf(connectedTo(hub, Vertex.DIRECTION.OUT, wanted))).containsExactlyInAnyOrder(knows, owns);
    });
  }

  @Test
  void honoursTheDirection() {
    database.transaction(() -> {
      final MutableVertex middle = newNode("middle");
      final MutableVertex peer = newNode("peer");

      final RID outgoing = middle.newEdge("Knows", peer).save().getIdentity();
      final RID incoming = peer.newEdge("Knows", middle).save().getIdentity();

      assertThat(ridsOf(connectedTo(middle, Vertex.DIRECTION.OUT, peer))).containsExactly(outgoing);
      assertThat(ridsOf(connectedTo(middle, Vertex.DIRECTION.IN, peer))).containsExactly(incoming);
      assertThat(ridsOf(connectedTo(middle, Vertex.DIRECTION.BOTH, peer))).containsExactlyInAnyOrder(outgoing, incoming);
    });
  }

  @Test
  void findsSelfLoops() {
    database.transaction(() -> {
      final MutableVertex node = newNode("self");
      final RID loop = node.newEdge("Knows", node).save().getIdentity();
      node.newEdge("Knows", newNode("other")).save();

      assertThat(ridsOf(connectedTo(node, Vertex.DIRECTION.OUT, node))).containsExactly(loop);
      assertThat(ridsOf(connectedTo(node, Vertex.DIRECTION.IN, node))).containsExactly(loop);
      // BOTH walks the OUT and the IN list, and a self-loop sits in each of them
      assertThat(ridsOf(connectedTo(node, Vertex.DIRECTION.BOTH, node))).containsExactly(loop, loop);
    });
  }

  @Test
  void returnsNothingWhenThePairIsNotConnected() {
    database.transaction(() -> {
      final MutableVertex hub = newNode("hub");
      final MutableVertex stranger = newNode("stranger");
      for (int i = 0; i < 50; i++)
        hub.newEdge("Knows", newNode("n" + i)).save();

      assertThat(connectedTo(hub, Vertex.DIRECTION.OUT, stranger).hasNext()).isFalse();
      // and the answer agrees with the primitive that never materialises anything
      assertThat(hub.isConnectedTo(stranger, Vertex.DIRECTION.OUT)).isFalse();
    });
  }

  @Test
  void worksOnAVertexWithNoEdgesAtAll() {
    database.transaction(() -> {
      final MutableVertex lonely = newNode("lonely");
      final MutableVertex other = newNode("other");
      assertThat(connectedTo(lonely, Vertex.DIRECTION.BOTH, other).hasNext()).isFalse();
    });
  }

  @Test
  void spansEveryChunkOfALongEdgeList() {
    // The edge list of a vertex is a chain of segments; the filter must keep walking past the head,
    // otherwise it would silently miss the older edges - and the oldest are exactly the ones a
    // long-running loader keeps re-checking.
    final int degree = 5_000;
    database.transaction(() -> {
      final MutableVertex hub = newNode("hub");
      RID firstEdge = null;
      RID lastEdge = null;
      for (int i = 0; i < degree; i++) {
        final MutableVertex leaf = newNode("leaf" + i);
        final RID edge = hub.newEdge("Knows", leaf, "seq", i).save().getIdentity();
        if (i == 0)
          firstEdge = edge;
        lastEdge = edge;
      }

      // the neighbour of the very first edge lives in the deepest segment of the chain
      final Vertex firstLeaf = database.lookupByRID(firstEdge, true).asEdge().getInVertex();
      assertThat(ridsOf(connectedTo(hub, Vertex.DIRECTION.OUT, firstLeaf))).containsExactly(firstEdge);

      final Vertex lastLeaf = database.lookupByRID(lastEdge, true).asEdge().getInVertex();
      assertThat(ridsOf(connectedTo(hub, Vertex.DIRECTION.OUT, lastLeaf))).containsExactly(lastEdge);
    });
  }

  @Test
  void agreesWithTheUnfilteredWalkOnEveryNeighbour() {
    database.transaction(() -> {
      final MutableVertex hub = newNode("hub");
      final List<Vertex> leaves = new ArrayList<>();
      for (int i = 0; i < 40; i++) {
        final MutableVertex leaf = newNode("leaf" + i);
        leaves.add(leaf);
        // a variable number of parallel edges per leaf, so the comparison is not trivially one-to-one
        for (int e = 0; e <= i % 3; e++)
          hub.newEdge("Knows", leaf, "seq", e).save();
      }

      for (final Vertex leaf : leaves) {
        final List<RID> expected = new ArrayList<>();
        for (final Edge edge : hub.getEdges(Vertex.DIRECTION.OUT, "Knows"))
          if (edge.getIn().equals(leaf.getIdentity()))
            expected.add(edge.getIdentity());

        assertThat(ridsOf(connectedTo(hub, Vertex.DIRECTION.OUT, leaf, "Knows")))
            .containsExactlyInAnyOrderElementsOf(expected);
      }
    });
  }

  @Test
  void findsParallelEdgesThatSitBehindALongTailOfOtherNeighbours() {
    // The pair's edges are the newest entries of a list long enough to span several segments, and the
    // pair is also joined by an edge of another type and by an edge in the opposite direction.
    database.transaction(() -> {
      final MutableVertex hub = newNode("hub");
      for (int i = 0; i < 200; i++)
        hub.newEdge("Knows", newNode("leaf" + i), "seq", i).save();

      final MutableVertex shared = newNode("shared");
      final RID first = hub.newEdge("Knows", shared, "kind", "payment").save().getIdentity();
      final RID second = hub.newEdge("Knows", shared, "kind", "refund").save().getIdentity();
      hub.newEdge("Owns", shared).save();
      shared.newEdge("Knows", hub, "kind", "reversed").save();

      assertThat(ridsOf(connectedTo(hub, Vertex.DIRECTION.OUT, shared, "Knows")))
          .containsExactlyInAnyOrder(first, second);
    });
  }

  @Test
  void readsTheEdgeListFromTheInstanceTheTransactionHas() {
    // The head of the edge list is a pointer inside the vertex record, so a vertex handle held from
    // before an edge was appended still points at the previous head. Callers reaching the engine
    // directly hand over whatever handle they hold, so the resolution has to happen in the engine -
    // otherwise the newest edges, which are exactly the ones a duplicate check asks about, go missing.
    final RID[] hubRid = new RID[1];
    final RID[] targetRid = new RID[1];
    final Vertex[] staleHandle = new Vertex[1];

    database.transaction(() -> {
      hubRid[0] = newNode("hub").getIdentity();
      targetRid[0] = newNode("target").getIdentity();
      staleHandle[0] = (Vertex) database.lookupByRID(hubRid[0], true);
    });

    database.transaction(() -> {
      final MutableVertex fresh = ((Vertex) database.lookupByRID(hubRid[0], true)).modify();
      final Vertex target = (Vertex) database.lookupByRID(targetRid[0], true);
      final RID appended = fresh.newEdge("Knows", target).save().getIdentity();

      assertThat(ridsOf(connectedTo(staleHandle[0], Vertex.DIRECTION.OUT, target, "Knows"))).containsExactly(appended);
    });
  }

  /**
   * The striped super-node layout (#5156) spreads a hot vertex's edges over several chains, so it overrides the
   * iterator factory and has to narrow every one of them. This is the layout the optimisation exists for, so it
   * cannot be the one branch left un-asserted.
   */
  @Test
  void narrowsEveryChainOfAPromotedSuperNode() {
    final int savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(256);
    try {
      database.transaction(() -> {
        final MutableVertex hub = newNode("hub");
        final List<Vertex> leaves = new ArrayList<>();
        for (int i = 0; i < 2_000; i++) {
          final MutableVertex leaf = newNode("leaf" + i);
          leaves.add(leaf);
          hub.newEdge("Knows", leaf, "seq", i).save();
        }

        final EdgeLinkedList outEdges = ((DatabaseInternal) database).getGraphEngine()
            .getEdgeHeadChunk((VertexInternal) hub, Vertex.DIRECTION.OUT);
        // the precondition: without promotion this would exercise the plain chain again and prove nothing
        assertThat(outEdges).isInstanceOf(StripedEdgeList.class);

        // one leaf per stripe generation: first, last and a few in between
        for (final int i : new int[] { 0, 1, 700, 1_300, 1_999 }) {
          final Vertex leaf = leaves.get(i);
          final List<RID> expected = new ArrayList<>();
          for (final Edge edge : hub.getEdges(Vertex.DIRECTION.OUT, "Knows"))
            if (edge.getIn().equals(leaf.getIdentity()))
              expected.add(edge.getIdentity());

          assertThat(expected).hasSize(1);
          assertThat(ridsOf(connectedTo(hub, Vertex.DIRECTION.OUT, leaf, "Knows")))
              .containsExactlyInAnyOrderElementsOf(expected);
        }

        assertThat(connectedTo(hub, Vertex.DIRECTION.OUT, newNode("stranger"), "Knows").hasNext()).isFalse();
      });
    } finally {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    }
  }

  @Test
  void rejectsAMissingTarget() {
    database.transaction(() -> {
      final MutableVertex hub = newNode("hub");
      assertThatThrownBy(() -> connectedTo(hub, Vertex.DIRECTION.OUT, null))
          .isInstanceOf(IllegalArgumentException.class);
    });
  }

  private MutableVertex newNode(final String name) {
    return database.newVertex("Node").set("name", name).save();
  }

  private Iterator<Edge> connectedTo(final Vertex source, final Vertex.DIRECTION direction, final Vertex target,
      final String... edgeTypes) {
    return ((DatabaseInternal) database).getGraphEngine()
        .getEdgesConnectedTo((VertexInternal) source, direction, target, edgeTypes);
  }

  private static List<RID> ridsOf(final Iterator<Edge> edges) {
    final List<RID> rids = new ArrayList<>();
    while (edges.hasNext())
      rids.add(edges.next().getIdentity());
    return rids;
  }
}
