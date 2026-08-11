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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.StripeDirectory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #6049: a {@link GraphAnalyticalView} was reported to return a promoted super-node's neighbours in the
 * OLTP "concatenated stripe" order (the order #6044/#6047 removed from the direct OLTP iterators), which
 * would make the same query return two different orders depending on whether the planner routed it through
 * the CSR path or the OLTP path.
 * <p>
 * That diagnosis does not match the code: {@code CSRBuilder} never preserves the order it consumes
 * {@code entryIterator()} in. Phase C of the build (`Arrays.sort` on every node's neighbour slice) has
 * unconditionally re-sorted every forward/backward adjacency list by dense node ID since the CSR/GAV feature
 * was first merged (#3618, see the pre-existing {@code sortedNeighbors} test), and Phase D permutes those IDs
 * again for cache locality. Whatever order {@code entryIterator()} produces - concatenated, interleaved, or
 * anything else - is discarded before a single query ever reads the CSR. Rewiring {@code CSRBuilder} onto the
 * interleaved iterator from #6047 (the issue's proposed fix) would therefore be a no-op on the CSR's output.
 * <p>
 * These tests pin the actual, pre-existing contract for a <b>promoted</b> vertex: the CSR neighbour order is
 * sorted ascending by dense node ID, exactly like the non-promoted case, and is independent of the OLTP
 * layout underneath. The real - and correctly-diagnosed - property is that GAV order is simply unrelated to
 * OLTP order for every vertex, promoted or not; that is documented on {@link GraphAnalyticalView} rather than
 * "fixed", since matching OLTP's approximate-recency order would break the ascending-adjacency invariant that
 * {@code isConnectedTo}/{@code countEdgesBetween}/{@code getMeanEdgesPerConnectedPair}/triangle counting rely
 * on for binary search.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6049GAVSupernodeOrderTest extends TestHelper {
  private static final int STRIPES   = 16;
  private static final int THRESHOLD = 64;
  private static final int TOTAL     = 500;

  private int savedThreshold;
  private int savedStripes;

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
  }

  @Test
  void gavNeighborOrderIsSortedForAPromotedSupernode() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);

    createSchema();
    final RID hubRID = createHub();
    insertEdges(hubRID, TOTAL);

    // PRECONDITION: THE HUB MUST REALLY HAVE BEEN PROMOTED, OTHERWISE THE TEST PROVES NOTHING
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(new String[] { "Hub", "Src" }, new String[] { "LINK" });

    final int hubId = gav.getNodeId(hubRID);
    final int[] neighbors = gav.getVertices(hubId, Vertex.DIRECTION.IN, "LINK");

    // NOTHING LOST, NOTHING DUPLICATED
    assertThat(neighbors).hasSize(TOTAL);
    assertThat(Arrays.stream(neighbors).boxed().collect(Collectors.toSet())).hasSize(TOTAL);

    // THE CSR NEIGHBOUR ORDER IS ASCENDING BY DENSE NODE ID - the same invariant proven for a
    // non-promoted hub by GraphAnalyticalViewTest#sortedNeighbors, now shown to also hold across
    // promotion. If CSRBuilder ever started forwarding entryIterator()'s (or an interleaved
    // iterator's) order into the CSR unsorted, this assertion is what would catch it.
    for (int i = 1; i < neighbors.length; i++)
      assertThat(neighbors[i]).isGreaterThan(neighbors[i - 1]);
  }

  @Test
  void gavOrderIntentionallyDivergesFromButLosesNothingRelativeToOltpOrder() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);

    createSchema();
    final RID hubRID = createHub();
    insertEdges(hubRID, TOTAL);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    // OLTP order: approximately newest-first, per #6047's InterleavedIterator.
    final List<RID> oltpOrder = new ArrayList<>();
    database.transaction(() -> {
      for (final Iterator<Vertex> it = hubRID.asVertex(true).getVertices(Vertex.DIRECTION.IN, "LINK").iterator();
           it.hasNext(); )
        oltpOrder.add(it.next().getIdentity());
    });

    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(new String[] { "Hub", "Src" }, new String[] { "LINK" });

    final int hubId = gav.getNodeId(hubRID);
    final int[] neighborIds = gav.getVertices(hubId, Vertex.DIRECTION.IN, "LINK");
    final List<RID> gavOrder = new ArrayList<>(neighborIds.length);
    for (final int neighborId : neighborIds)
      gavOrder.add(gav.getRID(neighborId));

    // The two orders are genuinely different orderings of the same edge set (the property the issue
    // observed is real), but neither loses or duplicates anything relative to the other.
    assertThat(gavOrder).hasSize(TOTAL);
    assertThat(gavOrder).containsExactlyInAnyOrderElementsOf(oltpOrder);
    assertThat(gavOrder).isNotEqualTo(oltpOrder);
  }

  private void createSchema() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub", 1);
      database.getSchema().createVertexType("Src", 8);
      database.getSchema().createEdgeType("LINK", 8);
    });
  }

  private RID createHub() {
    final MutableVertex[] holder = new MutableVertex[1];
    database.transaction(() -> {
      holder[0] = database.newVertex("Hub");
      holder[0].save();
    });
    return holder[0].getIdentity();
  }

  /** Inserts {@code count} edges Src-&gt;Hub, one transaction each. */
  private void insertEdges(final RID hubRID, final int count) {
    for (int i = 0; i < count; i++) {
      database.transaction(() -> {
        final MutableVertex src = database.newVertex("Src");
        src.save();
        src.newEdge("LINK", hubRID);
      });
    }
  }

  private Record loadInHead(final RID hubRID) {
    final Record[] head = new Record[1];
    database.transaction(() -> {
      final RID headRID = ((VertexInternal) hubRID.asVertex(true)).getInEdgesHeadChunk();
      head[0] = database.lookupByRID(headRID, true);
    });
    return head[0];
  }
}
