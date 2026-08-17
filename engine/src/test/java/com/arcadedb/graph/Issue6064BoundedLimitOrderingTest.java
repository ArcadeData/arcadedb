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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #6064: {@code GRAPH_SUPERNODE_INTERLEAVE_ROUNDS} used to be a CLIFF. Up to {@code rounds x stripes} entries a
 * promoted super-node's read walk was round-robin (approximately newest-first, #6044); the very next entry froze
 * the rotation and drained the remaining chains one after another (#6048), so an edge's returned position stopped
 * having anything to do with its recency rank and the error grew with the vertex DEGREE again - exactly the
 * failure #6044 fixed, pushed past a constant.
 * <p>
 * A caller reading a SMALL bounded prefix never notices; a caller reading a LARGE but still bounded one - an
 * export, a batch job, {@code ... RETURN x LIMIT 5000} - falls off the cliff silently, and the only lever it had
 * was raising the threshold for every unbounded read in the database too.
 * <p>
 * The walk now WIDENS instead of degrading: past the threshold it keeps rotating, taking a geometrically growing
 * batch from each chain per turn (see {@code InterleavedIterator}). Both properties survive:
 * <ul>
 * <li>full-walk locality - the number of chain switches stays logarithmic in the degree rather than proportional
 *     to it, which is what #6048 was about;</li>
 * <li>rank fidelity at EVERY depth - an entry of recency rank {@code r} comes back at a position of order
 *     {@code r} for the whole walk, not only for the first {@code rounds x stripes} entries.</li>
 * </ul>
 * These tests pin the second one, which is what the cliff took away. Both fail against the pre-#6064 code by a
 * wide margin (worst position/rank ratio in the hundreds, not the single digits).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6064BoundedLimitOrderingTest extends TestHelper {
  private static final int STRIPES   = 16;
  private static final int ROUNDS    = 4;
  /** Promotion threshold: everything past this lives in the striped generation the rotation walks. */
  private static final int THRESHOLD = 64;
  private static final int TOTAL     = 3_000;

  private int savedThreshold;
  private int savedStripes;
  private int savedRounds;

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
    savedRounds = GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.getValueAsInteger();

    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);
    GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.setValue(ROUNDS);
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
    GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.setValue(savedRounds);
  }

  /**
   * The bounded-but-large LIMIT of the issue: a caller stopping at the 1,000th of 3,000 neighbours must get
   * mostly-newest ones. The newest 1,000 edges have to fit inside the first 2,000 positions - a factor-2 slack
   * over their own count, which the widening rotation satisfies at every depth.
   * <p>
   * Pre-#6064 the rotation froze at entry 64 and drained one chain at a time, so the newest edge of the chain
   * drained LAST came back near position {@code TOTAL}: the newest 1,000 were spread over the whole walk.
   */
  @Test
  void aLargeBoundedLimitStillGetsTheNewestEdges() {
    final Fixture fixture = build();

    int worstPosition = 0;
    for (int rank = 0; rank < 1_000; rank++)
      worstPosition = Math.max(worstPosition, fixture.positionOfRank(rank));

    assertThat(worstPosition)
        .as("the newest 1000 of %d edges must come back inside the first 2000 positions, worst was %d", TOTAL,
            worstPosition)
        .isLessThan(2_000);
  }

  /**
   * The guarantee stated as it is documented: the position an edge of recency rank {@code r} is returned at is of
   * ORDER {@code r} - here, within a factor of 3 plus a constant - for the WHOLE walk and not only for the first
   * {@code rounds x stripes} entries.
   * <p>
   * The constant absorbs the rotation's own width (an entry of rank 0 can legitimately come back at position
   * {@code stripes - 1}) and the unevenness of {@code hash(neighbour RID)} across the stripes. The factor absorbs
   * the geometric batch: an entry reached in the batch that doubled at depth {@code d} is emitted no later than
   * the end of that batch, at most twice as deep as its own rank.
   */
  @Test
  void everyRankComesBackAtAPositionOfOrderOfThatRank() {
    final Fixture fixture = build();

    final int leeway = 6 * STRIPES;
    int worstRank = -1;
    double worstRatio = 0;
    for (int rank = 0; rank < TOTAL; rank++) {
      final int position = fixture.positionOfRank(rank);
      final double ratio = (double) (position + 1) / (rank + 1);
      if (position > 3 * rank + leeway && ratio > worstRatio) {
        worstRatio = ratio;
        worstRank = rank;
      }
    }

    assertThat(worstRank)
        .as("rank %d came back at position %d (ratio %.1f), past 3 x rank + %d", worstRank,
            worstRank < 0 ? -1 : fixture.positionOfRank(worstRank), worstRatio, leeway)
        .isEqualTo(-1);
  }

  /** The walk is still complete: the widening reorders nothing away and duplicates nothing. */
  @Test
  void theWideningWalkStillReturnsEveryEdgeExactlyOnce() {
    final Fixture fixture = build();
    assertThat(fixture.positionOf).hasSize(TOTAL).containsOnlyKeys(fixture.sources.toArray(new RID[0]));
    assertThat(new ArrayList<>(fixture.positionOf.values())).doesNotHaveDuplicates();
  }

  private Fixture build() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> sources = insertEdges(hubRID, TOTAL);
    assertThat(loadInHead(hubRID)).as("the hub must have been promoted to the striped layout")
        .isInstanceOf(StripeDirectory.class);
    return new Fixture(sources, positionsOfNeighbours(hubRID));
  }

  /** Insertion order plus the walk's output positions, with rank 0 = newest edge inserted. */
  private record Fixture(List<RID> sources, Map<RID, Integer> positionOf) {
    private int positionOfRank(final int rank) {
      return positionOf.get(sources.get(sources.size() - 1 - rank));
    }
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
