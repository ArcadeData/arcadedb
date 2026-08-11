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
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #6044: super-node promotion (#5156) must not silently turn the vertex's edge iteration order into an
 * arbitrary one. The classic layout iterates exactly newest-first; the striped layout composes one chain per
 * stripe and used to CONCATENATE them, so the whole of stripe 0 came before the whole of stripe 1 and the
 * global order became a function of {@code hash(neighbour RID)} - with an error proportional to the vertex
 * DEGREE, i.e. worst on exactly the vertices promotion targets.
 * <p>
 * The chains are now INTERLEAVED (round-robin, one entry per chain per turn) within each generation, which
 * reconstructs an approximate newest-first order whose error is proportional to the RANK asked for rather
 * than to the degree. These tests pin the two properties applications actually rely on:
 * <ol>
 * <li>the newest edge is inside the first {@code stripes} entries;</li>
 * <li>an edge of global rank {@code r} shows up at a position bounded by a small multiple of {@code r}.</li>
 * </ol>
 * Generations stay CONCATENATED newest-generation-first: generation 0 is the pre-promotion chain and holds
 * the OLDEST edges, so folding it into the rotation would put ancient entries in the first positions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6044StripeIterationOrderTest extends TestHelper {
  private static final int STRIPES   = 16;
  private static final int THRESHOLD = 64;
  private static final int TOTAL     = 2_000;

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
  void newestEdgesStayAtTheHeadOfAPromotedVertexIteration() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);

    createSchema();
    final RID hubRID = createHub();
    final List<RID> sources = insertEdges(hubRID, TOTAL);

    // PRECONDITION: THE HUB MUST REALLY HAVE BEEN PROMOTED, OTHERWISE THE TEST PROVES NOTHING
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    final Map<RID, Integer> positionOf = positionsOfNeighbours(hubRID);

    // NOTHING LOST, NOTHING DUPLICATED
    assertThat(positionOf).hasSize(TOTAL);
    assertThat(positionOf.keySet()).containsAll(sources);

    // (a) THE NEWEST EDGE IS THE HEAD OF ITS OWN CHAIN, SO ROUND-ROBIN MUST EMIT IT WITHIN THE FIRST TURN
    final int newestPosition = positionOf.get(sources.getLast());
    assertThat(newestPosition).isLessThan(STRIPES);

    // (b) RANK-BOUNDED ERROR: every one of the newest 50 edges lands in the first 400 entries. Concatenation
    // scatters them over the whole list (one cluster per stripe, ~degree/stripes apart), so it cannot pass.
    //
    // The two bounds below are DETERMINISTIC, not sampled: the fixture inserts a fixed number of vertices in a
    // fixed order into a fresh database, so the neighbour RIDs - and therefore StripeDirectory.stripeOf's
    // placement of every edge - are the same on every run. They are stated loosely on purpose, well clear of the
    // measured values (worst-placed ~140, ~88 of the newest 100 in the first 200), so that a change in bucket
    // allocation that shifts the RIDs moves the numbers without failing the test; only a real regression in the
    // rank/position relationship can cross them. Concatenation misses by an order of magnitude (worst ~1762).
    int worstOfTheNewest = 0;
    for (int rank = 0; rank < 50; rank++)
      worstOfTheNewest = Math.max(worstOfTheNewest, positionOf.get(sources.get(TOTAL - 1 - rank)));
    assertThat(worstOfTheNewest).isLessThan(400);

    // AND MOST OF THE NEWEST 100 ARE STILL IN THE FIRST 200 - the "page the newest N" use case
    int newest100InFirst200 = 0;
    for (int rank = 0; rank < 100; rank++)
      if (positionOf.get(sources.get(TOTAL - 1 - rank)) < 200)
        newest100InFirst200++;
    assertThat(newest100InFirst200).isGreaterThanOrEqualTo(75);

    // (c) GENERATION 0 (THE PRE-PROMOTION CHAIN, THE OLDEST EDGES) STAYS AT THE TAIL: the rotation is
    // per-generation, so the oldest edges must NOT be interleaved into the first positions. Generation 0 is
    // one chain walked newest-first and it is walked last, hence the very oldest edge is dead last.
    assertThat(positionOf.get(sources.getFirst())).isEqualTo(TOTAL - 1);
  }

  /**
   * The same guarantee through the edge iterator (and after a reopen, so the whole thing is read back from
   * disk rather than from the transaction's record cache).
   */
  @Test
  void newestEdgeIsAtTheHeadOfTheEdgeIteratorAfterReopen() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);

    createSchema();
    final RID hubRID = createHub();
    final List<RID> sources = insertEdges(hubRID, 600);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    reopenDatabase();

    final RID hub2 = new RID(hubRID.getBucketId(), hubRID.getPosition());
    final RID newestSrc = new RID(sources.getLast().getBucketId(), sources.getLast().getPosition());

    database.transaction(() -> {
      final Vertex hub = hub2.asVertex(true);

      int position = 0;
      int newestAt = -1;
      final Set<RID> seen = new HashSet<>();
      for (final Iterator<Edge> it = hub.getEdges(Vertex.DIRECTION.IN, "LINK").iterator(); it.hasNext(); position++) {
        final RID out = it.next().getOut();
        assertThat(seen.add(out)).isTrue();
        if (out.equals(newestSrc))
          newestAt = position;
      }
      assertThat(seen).hasSize(600);
      assertThat(newestAt).isBetween(0, STRIPES - 1);
    });

    database.transaction(() -> hub2.asVertex(true).delete());
  }

  /** Interleaving must not change what a count sees, nor what the RID-only walk sees. */
  @Test
  void interleavingPreservesCountAndTheRidWalk() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);

    createSchema();
    final RID hubRID = createHub();
    final List<RID> sources = insertEdges(hubRID, 500);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    database.transaction(() -> {
      final Vertex hub = hubRID.asVertex(true);
      assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(500);
      assertThat(hub.getVertices(Vertex.DIRECTION.IN, "LINK").size()).isEqualTo(500);

      final Set<RID> viaVertices = new HashSet<>();
      for (final Vertex v : hub.getVertices(Vertex.DIRECTION.IN, "LINK"))
        assertThat(viaVertices.add(v.getIdentity())).isTrue();
      assertThat(viaVertices).containsExactlyInAnyOrderElementsOf(sources);
    });

    database.transaction(() -> hubRID.asVertex(true).delete());
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

  /** Inserts {@code count} edges Src-&gt;Hub, one transaction each, returning the source RIDs in insertion order. */
  private List<RID> insertEdges(final RID hubRID, final int count) {
    final List<RID> sources = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final RID[] srcHolder = new RID[1];
      database.transaction(() -> {
        final MutableVertex src = database.newVertex("Src");
        src.save();
        src.newEdge("LINK", hubRID);
        srcHolder[0] = src.getIdentity();
      });
      sources.add(srcHolder[0]);
    }
    return sources;
  }

  private Record loadInHead(final RID hubRID) {
    final Record[] head = new Record[1];
    database.transaction(() -> {
      final RID headRID = ((VertexInternal) hubRID.asVertex(true)).getInEdgesHeadChunk();
      head[0] = database.lookupByRID(headRID, true);
    });
    return head[0];
  }

  /** Maps every incoming neighbour of the hub to the position it is returned at. */
  private Map<RID, Integer> positionsOfNeighbours(final RID hubRID) {
    final Map<RID, Integer> positions = new HashMap<>();
    database.transaction(() -> {
      int position = 0;
      for (final Iterator<Vertex> it = hubRID.asVertex(true).getVertices(Vertex.DIRECTION.IN, "LINK").iterator();
           it.hasNext(); position++)
        assertThat(positions.put(it.next().getIdentity(), position)).isNull();
    });
    return positions;
  }
}
