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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.EdgeType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10406: {@code CHECK DATABASE FIX} rebuilds an unreadable edge list from the edge RECORDS and, since #10377, carries
 * over the LIGHTWEIGHT entries of the part of the dropped list it can still read. A lightweight edge stored in the
 * unreadable part was lost, although for a BIDIRECTIONAL type the same edge is also stored in the far endpoint's list.
 * <p>
 * The repair now rebuilds those from the far endpoints, with the multiplicity the far side holds (a non-unique lightweight
 * type can hold the same pair more than once), and without doubling what the readable part already carried over. A
 * unidirectional lightweight edge has no other copy and stays reported as lost.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10406CheckFixLightEdgeFarEndpointTest extends TestHelper {
  private static final String V = "V10406";
  private static final String E = "E10406";
  private static final String L = "L10406";
  /** LIGHTWEIGHT and UNIDIRECTIONAL. */
  private static final String U = "U10406";

  private int savedThreshold;

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType(V, 1);
      database.getSchema().createEdgeType(E, 1);
      database.getSchema().buildEdgeType().withName(L).withLightweight(true).create();
      database.getSchema().buildEdgeType().withName(U).withLightweight(true).withBidirectional(false).create();
    });
  }

  /** The whole readable part is gone with the head chunk: every lightweight edge comes back from the far endpoints. */
  @Test
  void anUnreadableHeadChunkLosesNoBidirectionalLightweightEdge() {
    final RID a = newVertex();
    final List<RID> targets = new ArrayList<>();
    addRegularOut(a, 500);
    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        final RID t = database.newVertex(V).save().getIdentity();
        targets.add(t);
        a.asVertex(true).newEdge(L, t);
      }
    });

    deleteAtLowLevel(headChunk(a, Vertex.DIRECTION.OUT));

    final Map<String, Object> stats = checkVerticesFix();
    assertThat(((Number) stats.get("reconnectedEdges")).longValue()).as("the rebuild must have run").isGreaterThan(0L);
    assertThat(((Number) stats.get("salvagedLightEdges")).longValue()).isZero();
    assertThat(((Number) stats.get("recoveredLightEdges")).longValue()).isEqualTo(5L);
    assertThat(((Number) stats.get("unsalvageableEdgeLists")).longValue()).isEqualTo(1L);

    assertThat(neighbours(a, Vertex.DIRECTION.OUT, L)).containsExactlyInAnyOrderElementsOf(targets);
    assertThat(count(a, Vertex.DIRECTION.OUT, E)).isEqualTo(500);
    assertCleanAfterwards();
  }

  /**
   * Lightweight edges on both sides of the damage, one pair stored twice with one copy on each side: the readable part
   * carries its copies over, the far endpoints give back only what is missing, so the pair ends up exactly twice.
   */
  @Test
  void theFarEndpointsGiveBackOnlyWhatTheReadablePartDidNotCarryOver() {
    final RID a = newVertex();
    final RID p = newVertex();
    final RID q = newVertex();
    final RID r = newVertex();

    // THE OLDEST CHUNK: a -> p AND a -> q
    database.transaction(() -> {
      a.asVertex(true).newEdge(L, p);
      a.asVertex(true).newEdge(L, q);
    });
    addRegularOut(a, 500);
    // THE HEAD CHUNK: a -> p A SECOND TIME, AND a -> r
    database.transaction(() -> {
      a.asVertex(true).newEdge(L, p);
      a.asVertex(true).newEdge(L, r);
    });

    final RID oldest = oldestChunk(a, Vertex.DIRECTION.OUT);
    assertThat(oldest).isNotEqualTo(headChunk(a, Vertex.DIRECTION.OUT));
    deleteAtLowLevel(oldest);

    final Map<String, Object> stats = checkVerticesFix();
    assertThat(((Number) stats.get("salvagedLightEdges")).longValue()).isEqualTo(2L);
    assertThat(((Number) stats.get("recoveredLightEdges")).longValue()).isEqualTo(2L);

    assertThat(neighbours(a, Vertex.DIRECTION.OUT, L)).containsExactlyInAnyOrder(p, p, q, r);
    assertThat(count(a, Vertex.DIRECTION.OUT, E)).isEqualTo(500);
    assertCleanAfterwards();
  }

  /** The IN side: an unreadable IN list gets its lightweight edges back from the sources' OUT lists. */
  @Test
  void anUnreadableInListGetsItsLightweightEdgesBackFromTheSources() {
    final RID b = newVertex();
    final List<RID> sources = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 500; i++)
        database.newVertex(V).save().newEdge(E, b);
    });
    database.transaction(() -> {
      for (int i = 0; i < 4; i++) {
        final MutableVertex s = database.newVertex(V).save();
        sources.add(s.getIdentity());
        s.newEdge(L, b);
      }
    });

    deleteAtLowLevel(headChunk(b, Vertex.DIRECTION.IN));

    final Map<String, Object> stats = checkVerticesFix();
    assertThat(((Number) stats.get("recoveredLightEdges")).longValue()).isEqualTo(4L);

    assertThat(neighbours(b, Vertex.DIRECTION.IN, L)).containsExactlyInAnyOrderElementsOf(sources);
    assertThat(count(b, Vertex.DIRECTION.IN, E)).isEqualTo(500);
    assertCleanAfterwards();
  }

  /**
   * Both copies of an edge sit in lists being rebuilt: the source's copy in the readable part of its OUT list, the
   * target's copy in the unreadable part of its IN list. The target's copy comes back from what the source's dropped list
   * carried over, since that list no longer exists to be read when the far endpoints are walked.
   */
  @Test
  void aCopyCarriedOverFromAnotherDroppedListIsAFarEndpointToo() {
    final RID a = newVertex();
    final RID t = newVertex();

    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.newVertex(V).save().newEdge(E, t);
    });
    addRegularOut(a, 300);
    database.transaction(() -> a.asVertex(true).newEdge(L, t));
    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.newVertex(V).save().newEdge(E, t);
    });

    final int lightBucket = ((EdgeType) database.getSchema().getType(L)).getFirstBucketId();
    final RID tChunk = chunkHolding(t, Vertex.DIRECTION.IN, lightBucket, a);
    assertThat(tChunk).as("t's copy is not in its head chunk").isNotEqualTo(headChunk(t, Vertex.DIRECTION.IN));
    assertThat(chunkHolding(a, Vertex.DIRECTION.OUT, lightBucket, t)).isEqualTo(headChunk(a, Vertex.DIRECTION.OUT));

    final RID aOlder = olderThanHead(a, Vertex.DIRECTION.OUT);
    deleteAtLowLevel(tChunk);
    deleteAtLowLevel(aOlder);

    final Map<String, Object> stats = checkVerticesFix();
    assertThat(((Number) stats.get("salvagedLightEdges")).longValue()).isEqualTo(1L);
    assertThat(((Number) stats.get("recoveredLightEdges")).longValue()).isEqualTo(1L);

    assertThat(neighbours(a, Vertex.DIRECTION.OUT, L)).containsExactly(t);
    assertThat(neighbours(t, Vertex.DIRECTION.IN, L)).containsExactly(a);
    assertThat(count(a, Vertex.DIRECTION.OUT, E)).isEqualTo(300);
    assertThat(count(t, Vertex.DIRECTION.IN, E)).isEqualTo(600);
    assertCleanAfterwards();
  }

  /**
   * A lightweight self-loop in the unreadable part of the OUT list: its other copy is in the vertex's own IN list, which is
   * readable, so it comes back from there.
   */
  @Test
  void aSelfLoopComesBackFromTheVertexsOwnOtherList() {
    final RID a = newVertex();
    final RID l = newVertex();
    addRegularOut(a, 500);
    database.transaction(() -> {
      a.asVertex(true).newEdge(L, a);
      a.asVertex(true).newEdge(L, l);
    });

    deleteAtLowLevel(headChunk(a, Vertex.DIRECTION.OUT));

    final Map<String, Object> stats = checkVerticesFix();
    assertThat(((Number) stats.get("recoveredLightEdges")).longValue()).isEqualTo(2L);

    assertThat(neighbours(a, Vertex.DIRECTION.OUT, L)).containsExactlyInAnyOrder(a, l);
    assertThat(neighbours(a, Vertex.DIRECTION.IN, L)).containsExactly(a);
    assertCleanAfterwards();
  }

  /** On a promoted super-node: a lost stripe chain that held lightweight entries. */
  @Test
  void aLostStripeLosesNoBidirectionalLightweightEdge() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(8);
    final RID a = newVertex();
    addRegularOut(a, 200);
    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        a.asVertex(true).newEdge(L, database.newVertex(V).save());
    });

    final RID[] lost = new RID[1];
    final long[] lostLight = new long[1];
    database.transaction(() -> {
      final StripeDirectory dir = (StripeDirectory) ((DatabaseInternal) database).lookupByRID(headChunk(a, Vertex.DIRECTION.OUT), true);
      for (int g = dir.getGenerationCount() - 1; g >= 0 && lost[0] == null; g--)
        for (int s = 0; s < dir.getStripes(g) && lost[0] == null; s++) {
          final RID chainHead = dir.getHead(g, s);
          if (chainHead == null)
            continue;
          final EdgeSegment segment = (EdgeSegment) ((DatabaseInternal) database).lookupByRID(chainHead, true);
          final long light = new EdgeLinkedList(a.asVertex(true), Vertex.DIRECTION.OUT, segment).count(L);
          if (light > 0 && segment.getPreviousRID() == null) {
            lost[0] = chainHead;
            lostLight[0] = light;
          }
        }
    });
    assertThat(lost[0]).as("a single-chunk stripe chain holding lightweight entries").isNotNull();
    deleteAtLowLevel(lost[0]);

    final Map<String, Object> stats = checkVerticesFix();
    assertThat(((Number) stats.get("recoveredLightEdges")).longValue()).isEqualTo(lostLight[0]);
    assertThat(((Number) stats.get("salvagedLightEdges")).longValue()).isEqualTo(40L - lostLight[0]);

    assertThat(count(a, Vertex.DIRECTION.OUT, L)).isEqualTo(40);
    assertThat(count(a, Vertex.DIRECTION.OUT, E)).isEqualTo(200);
  }

  /** A unidirectional lightweight edge in the unreadable part has no other copy: not invented, and still reported. */
  @Test
  void aUnidirectionalLightweightEdgeInTheUnreadablePartStaysReportedAsLost() {
    final RID a = newVertex();
    final RID u = newVertex();
    final RID l = newVertex();
    addRegularOut(a, 500);
    database.transaction(() -> {
      a.asVertex(true).newEdge(U, u);
      a.asVertex(true).newEdge(L, l);
    });

    deleteAtLowLevel(headChunk(a, Vertex.DIRECTION.OUT));

    final Map<String, Object> stats = checkVerticesFix();
    assertThat(((Number) stats.get("recoveredLightEdges")).longValue()).isEqualTo(1L);
    assertThat(((Number) stats.get("unsalvageableEdgeLists")).longValue()).isEqualTo(1L);
    assertThat(warnings(stats)).anyMatch(w -> w.contains(a.toString()) && w.contains("unidirectional"));

    assertThat(neighbours(a, Vertex.DIRECTION.OUT, L)).containsExactly(l);
    assertThat(neighbours(a, Vertex.DIRECTION.OUT, U)).isEmpty();
    assertCleanAfterwards();
  }

  // ------------------------------------------------------------------------------------------------------------------

  private RID newVertex() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newVertex(V).save().getIdentity());
    return rid[0];
  }

  private void addRegularOut(final RID a, final int edges) {
    database.transaction(() -> {
      final Vertex v = a.asVertex(true);
      for (int i = 0; i < edges; i++)
        v.newEdge(E, database.newVertex(V).save());
    });
  }

  private RID headChunk(final RID vertex, final Vertex.DIRECTION direction) {
    final RID[] head = new RID[1];
    database.transaction(() -> {
      final VertexInternal v = (VertexInternal) vertex.asVertex(true);
      head[0] = direction == Vertex.DIRECTION.OUT ? v.getOutEdgesHeadChunk() : v.getInEdgesHeadChunk();
    });
    return head[0];
  }

  private RID olderThanHead(final RID vertex, final Vertex.DIRECTION direction) {
    final RID[] older = new RID[1];
    database.transaction(() -> older[0] = segment(headChunk(vertex, direction)).getPreviousRID());
    assertThat(older[0]).as("the list must span more than one chunk").isNotNull();
    return older[0];
  }

  private RID oldestChunk(final RID vertex, final Vertex.DIRECTION direction) {
    final RID[] oldest = new RID[1];
    database.transaction(() -> {
      RID current = headChunk(vertex, direction);
      while (true) {
        final RID previous = segment(current).getPreviousRID();
        if (previous == null)
          break;
        current = previous;
      }
      oldest[0] = current;
    });
    return oldest[0];
  }

  private RID chunkHolding(final RID vertex, final Vertex.DIRECTION direction, final int edgeTypeBucketId, final RID neighbour) {
    final RID[] found = new RID[1];
    database.transaction(() -> {
      RID current = headChunk(vertex, direction);
      while (current != null && found[0] == null) {
        final EdgeSegment segment = segment(current);
        if (segment.containsLightEdge(edgeTypeBucketId, neighbour))
          found[0] = current;
        current = segment.getPreviousRID();
      }
    });
    assertThat(found[0]).isNotNull();
    return found[0];
  }

  private EdgeSegment segment(final RID rid) {
    return (EdgeSegment) ((DatabaseInternal) database).lookupByRID(rid, true);
  }

  private void deleteAtLowLevel(final RID rid) {
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, rid));
  }

  private Map<String, Object> checkVerticesFix() {
    return new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(V, true, 0);
  }

  private List<RID> neighbours(final RID vertex, final Vertex.DIRECTION direction, final String type) {
    final List<RID> result = new ArrayList<>();
    database.transaction(() -> {
      for (final Vertex v : vertex.asVertex(true).getVertices(direction, type))
        result.add(v.getIdentity());
    });
    return result;
  }

  private long count(final RID vertex, final Vertex.DIRECTION direction, final String type) {
    final long[] n = new long[1];
    database.transaction(() -> {
      for (final Edge ignored : vertex.asVertex(true).getEdges(direction, type))
        ++n[0];
    });
    return n[0];
  }

  @SuppressWarnings("unchecked")
  private static Collection<String> warnings(final Map<String, Object> result) {
    return (Collection<String>) result.get("warnings");
  }

  private void assertCleanAfterwards() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result row = rs.next();
      final Number errors = row.getProperty("totalErrors");
      assertThat(errors == null ? 0L : errors.longValue()).as("warnings: " + row.getProperty("warnings")).isEqualTo(0L);
    }
  }
}
