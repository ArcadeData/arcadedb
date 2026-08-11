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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #6048: {@code StripedEdgeList}'s read walks (edgeIterator/vertexIterator/ridIterator) interleave the stripe
 * chains of a promoted super-node so the first entries are approximately newest-first (#6044). That interleaving
 * keeps one resident chunk page PER STRIPE and hops between files every {@code stripes} entries - a locality
 * cost worth paying only while a caller is actually paging (small LIMIT). A caller that reads the whole list -
 * an ordinary {@code MATCH (h)-[:LINK]->(x) RETURN x} with no LIMIT, a Gremlin {@code out()}, an export - was
 * paying that cost for the FULL degree even though it never consults the order.
 * <p>
 * {@link GlobalConfiguration#GRAPH_SUPERNODE_INTERLEAVE_ROUNDS} bounds it: past {@code rounds x stripes} entries
 * of a generation, the walk degrades to plain concatenation of what is left in each chain. These tests pin:
 * <ol>
 * <li>correctness first - nothing lost or duplicated by the degrade, at a threshold that lands well inside the
 *     walk, exactly on a stripe boundary, and past the whole list;</li>
 * <li>the ordering guarantee still holds for the prefix within the threshold;</li>
 * <li>a threshold of 0 reproduces the pre-#6044 concatenated order exactly, since a full walk with no rounds of
 *     interleaving is definitionally the classic layout's degree-dependent chain grouping;</li>
 * <li>the threshold does not leak across the generation boundary - the pre-promotion generation (walked last)
 *     is unaffected by however far the promoted generation's rotation degraded before it.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6048SupernodeFullWalkDegradeTest extends TestHelper {
  private static final int STRIPES   = 16;
  private static final int THRESHOLD = 64;
  private static final int TOTAL     = 2_000;

  private int savedThreshold;
  private int savedStripes;
  private int savedRounds;

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
    savedRounds = GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.getValueAsInteger();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
    GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.setValue(savedRounds);
  }

  /** 3 rounds (mid-walk), exactly 1 round, and 0 (from the very first entry): every one still has to return the
   * complete, duplicate-free neighbour set. One fresh database per value, via TestHelper's per-test lifecycle. */
  @ParameterizedTest
  @ValueSource(ints = { 3, 1, 0 })
  void fullWalkLosesNothingWhicheverRoundTheDegradeFallsIn(final int rounds) {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);
    GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.setValue(rounds);

    createSchema();
    final RID hubRID = createHub();
    final List<RID> sources = insertEdges(hubRID, TOTAL);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    final Set<RID> seen = new HashSet<>();
    database.transaction(() -> {
      for (final Iterator<Vertex> it = hubRID.asVertex(true).getVertices(Vertex.DIRECTION.IN, "LINK").iterator(); it.hasNext(); )
        assertThat(seen.add(it.next().getIdentity())).as("rounds=%d must not duplicate an edge", rounds).isTrue();
    });
    assertThat(seen).as("rounds=%d must return every edge", rounds).hasSize(TOTAL).containsExactlyInAnyOrderElementsOf(sources);
  }

  @Test
  void orderedPrefixGuaranteeStillHoldsWithinTheDegradeThreshold() {
    // 8 rounds x 16 stripes = 128: comfortably larger than the #6044 test's own bound (worst-of-newest-50 < 400
    // positions), so the prefix this test checks never crosses into the degraded, order-agnostic tail.
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);
    GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.setValue(8);

    createSchema();
    final RID hubRID = createHub();
    final List<RID> sources = insertEdges(hubRID, TOTAL);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    final Map<RID, Integer> positionOf = positionsOfNeighbours(hubRID);
    assertThat(positionOf).hasSize(TOTAL);

    // THE NEWEST EDGE IS THE HEAD OF ITS OWN CHAIN: round-robin must emit it within the first turn.
    assertThat(positionOf.get(sources.getLast())).isLessThan(STRIPES);

    // RANK-BOUNDED ERROR for the newest 50, same bound as Issue6044StripeIterationOrderTest: the degrade
    // threshold (128) has not been reached yet at these positions.
    int worstOfTheNewest = 0;
    for (int rank = 0; rank < 50; rank++)
      worstOfTheNewest = Math.max(worstOfTheNewest, positionOf.get(sources.get(TOTAL - 1 - rank)));
    assertThat(worstOfTheNewest).isLessThan(400);
  }

  /**
   * A threshold of 0 must actually take effect, not be a no-op left over from a wiring mistake: it should
   * reproduce the exact failure signature #6044 fixed - the newest edges scattered by {@code hash(neighbour
   * RID)} instead of clustered near the front. Same fixture and same bound Issue6044StripeIterationOrderTest
   * uses for the INTERLEAVED case (worst-of-newest-50 &lt; 400 positions); here the assertion is inverted.
   */
  @Test
  void zeroRoundsReproducesTheClassicPreInterleavingScatter() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);
    GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.setValue(0);

    createSchema();
    final RID hubRID = createHub();
    final List<RID> sources = insertEdges(hubRID, TOTAL);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    final Map<RID, Integer> positionOf = positionsOfNeighbours(hubRID);
    assertThat(positionOf).hasSize(TOTAL);
    assertThat(positionOf.keySet()).containsAll(sources);

    int worstOfTheNewest = 0;
    for (int rank = 0; rank < 50; rank++)
      worstOfTheNewest = Math.max(worstOfTheNewest, positionOf.get(sources.get(TOTAL - 1 - rank)));
    assertThat(worstOfTheNewest).as("rounds=0 must disable interleaving, not merely shrink its window").isGreaterThan(1000);
  }

  /**
   * The degrade threshold is per generation ({@code interleaved()} builds a fresh {@code InterleavedIterator}
   * per generation, so {@code browsed} starts at 0 for each), not accumulated across the whole vertex. Generation
   * 0 - the pre-promotion classic chain, holding only the very first edge ever inserted here - is walked LAST
   * and, being a single chain, bypasses {@code InterleavedIterator} entirely (the "size==1" unwrap in
   * {@code interleaved()}): there is no degrade state to leak into it even in principle, but this pins the
   * observable consequence. With rounds=1 (16-entry threshold), generation 1's ~1,936 entries degrade to
   * concatenation almost immediately; the oldest edge must still land exactly at the tail, unaffected by
   * whatever generation 1's rotation did before it.
   */
  @Test
  void degradeThresholdDoesNotCarryOverToTheNextGeneration() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);
    GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.setValue(1);

    createSchema();
    final RID hubRID = createHub();
    final List<RID> sources = insertEdges(hubRID, TOTAL);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    final Map<RID, Integer> positionOf = positionsOfNeighbours(hubRID);
    assertThat(positionOf).hasSize(TOTAL);

    // GENERATION 0 (pre-promotion, oldest edges) STAYS AT THE TAIL regardless of how generation 1 degraded.
    assertThat(positionOf.get(sources.getFirst())).isEqualTo(TOTAL - 1);

    // GENERATION 1's OWN NEWEST EDGE IS STILL WITHIN THE FIRST TURN: its rotation started fresh, not partway
    // through some carried-over count.
    assertThat(positionOf.get(sources.getLast())).isLessThan(STRIPES);
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
