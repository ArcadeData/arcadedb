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
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10377: on a promoted super-node {@code CHECK DATABASE FIX} could not prune one dangling adjacency entry - every
 * striped walk is a {@code MultiIterator}, whose {@code remove()} threw - so it fell back to dropping the whole list and
 * rebuilding it from the edge RECORDS, which deleted every LIGHTWEIGHT edge of the vertex (they have no record).
 * <p>
 * Each test drives one entry point: the prune on the striped layout, the classic control, the rebuild of a list that
 * holds lightweight edges (classic and striped), and {@code Iterator.remove()} through the public API.
 */
class Issue10377SuperNodePruneAndLightEdgeRebuildTest extends TestHelper {
  private static final String V = "V10377";
  private static final String E = "E10377";
  private static final String L = "L10377";

  private int savedThreshold;

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
  }

  @Test
  void checkFixPrunesADanglingEntryOnAPromotedSuperNodeWithoutRebuildingTheList() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(8);
    final RID a = createGraph(200, 5);
    assertThat(headIsStriped(a)).as("the vertex must have been promoted").isTrue();

    makeOneEdgeDangle(a);

    final Map<String, Object> result = checkDatabaseFix();
    final Collection<String> warnings = warnings(result);
    assertThat(warnings).noneMatch(w -> w.contains("MultiIterator.remove()"));
    assertThat(warnings).noneMatch(w -> w.contains("rebuilding the edge list"));
    assertThat(((Number) result.get("reconnectedEdges")).longValue()).isEqualTo(0L);
    assertThat(((Number) result.get("prunedDanglingEntries")).longValue()).isGreaterThanOrEqualTo(1L);

    assertThat(headIsStriped(a)).isTrue();
    assertThat(countOut(a, E)).isEqualTo(199);
    assertThat(countOut(a, L)).isEqualTo(5);
    assertCleanAfterwards();
  }

  @Test
  void classicControlPrunesTheSameWay() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(8);
    final RID a = createGraph(3, 5);
    assertThat(headIsStriped(a)).isFalse();

    makeOneEdgeDangle(a);

    final Map<String, Object> result = checkDatabaseFix();
    assertThat(((Number) result.get("reconnectedEdges")).longValue()).isEqualTo(0L);
    assertThat(countOut(a, E)).isEqualTo(2);
    assertThat(countOut(a, L)).isEqualTo(5);
    assertCleanAfterwards();
  }

  /**
   * The rebuild arm on its own, on the classic layout: an unreadable older chunk forces the drop-and-rebuild, the
   * lightweight edges were written last and so sit in the readable head chunk. They have no record to be rebuilt from,
   * so the repair must carry them over from the list it drops.
   */
  @Test
  void rebuildOfAClassicListKeepsTheLightweightEdgesItCanRead() {
    final RID a = createGraph(500, 5);
    assertThat(headIsStriped(a)).isFalse();

    final RID[] older = new RID[1];
    database.transaction(() -> {
      final RID head = ((VertexInternal) a.asVertex(true)).getOutEdgesHeadChunk();
      final EdgeSegment headChunk = (EdgeSegment) ((DatabaseInternal) database).lookupByRID(head, true);
      older[0] = headChunk.getPreviousRID();
      assertThat(older[0]).as("the list must span more than one chunk").isNotNull();
    });
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, older[0]));

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(V, true, 0);
    assertThat(((Number) stats.get("reconnectedEdges")).longValue()).as("the rebuild must have run").isGreaterThan(0L);
    assertThat(((Number) stats.get("salvagedLightEdges")).longValue()).isEqualTo(5L);

    assertThat(countOut(a, E)).isEqualTo(500);
    assertThat(countOut(a, L)).isEqualTo(5);
    assertCleanAfterwards();
  }

  /**
   * The rebuild arm on the striped layout: one stripe chain head is lost, the others are readable, and the
   * lightweight edges stored in the readable stripes must survive the rebuild.
   */
  @Test
  void rebuildOfAStripedListKeepsTheLightweightEdgesOfTheReadableStripes() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(8);
    final RID a = createGraph(200, 40);
    assertThat(headIsStriped(a)).isTrue();

    // THE STRIPE CHAIN TO LOSE: one holding no lightweight entry, so every lightweight edge is readable and must survive
    final RID[] lost = new RID[1];
    database.transaction(() -> {
      final RID head = ((VertexInternal) a.asVertex(true)).getOutEdgesHeadChunk();
      final StripeDirectory dir = (StripeDirectory) ((DatabaseInternal) database).lookupByRID(head, true);
      for (int g = dir.getGenerationCount() - 1; g >= 0 && lost[0] == null; g--)
        for (int s = 0; s < dir.getStripes(g) && lost[0] == null; s++) {
          final RID chainHead = dir.getHead(g, s);
          if (chainHead == null)
            continue;
          final EdgeLinkedList chain = new EdgeLinkedList(a.asVertex(true), Vertex.DIRECTION.OUT,
              (EdgeSegment) ((DatabaseInternal) database).lookupByRID(chainHead, true));
          if (chain.count(L) == 0 && chain.count(E) > 0 && ((EdgeSegment) ((DatabaseInternal) database).lookupByRID(chainHead,
              true)).getPreviousRID() == null)
            lost[0] = chainHead;
        }
      assertThat(lost[0]).as("a single-chunk stripe chain with regular edges only").isNotNull();
    });
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, lost[0]));

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(V, true, 0);
    assertThat(((Number) stats.get("reconnectedEdges")).longValue()).as("the rebuild must have run").isGreaterThan(0L);
    assertThat(((Number) stats.get("salvagedLightEdges")).longValue()).isEqualTo(40L);

    assertThat(countOut(a, E)).isEqualTo(200);
    assertThat(countOut(a, L)).isEqualTo(40);
  }

  /** What cannot be read cannot be carried over: the run must say so instead of reporting a clean rebuild. */
  @Test
  void rebuildOfAListWhoseLightweightPartIsUnreadableReportsIt() {
    final RID a = createGraph(500, 5);

    final RID[] head = new RID[1];
    database.transaction(() -> head[0] = ((VertexInternal) a.asVertex(true)).getOutEdgesHeadChunk());
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, head[0]));

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(V, true, 0);
    assertThat(((Number) stats.get("unsalvageableEdgeLists")).longValue()).isEqualTo(1L);
    assertThat(warnings(stats)).anyMatch(w -> w.contains(a.toString()) && w.contains("lightweight"));
    assertThat(countOut(a, E)).isEqualTo(500);
  }

  @Test
  void iteratorRemoveWorksOnAPromotedVertexThroughThePublicApi() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(8);
    final RID a = createGraph(200, 5);
    assertThat(headIsStriped(a)).isTrue();

    database.transaction(() -> {
      final Iterator<Edge> it = a.asVertex(true).getEdges(Vertex.DIRECTION.OUT, E).iterator();
      int removed = 0;
      while (it.hasNext() && removed < 10) {
        it.next();
        it.remove();
        ++removed;
      }
    });
    assertThat(countOut(a, E)).isEqualTo(190);
    assertThat(countOut(a, L)).isEqualTo(5);

    database.transaction(() -> {
      final Iterator<Edge> it = a.asVertex(true).getEdges(Vertex.DIRECTION.OUT, L).iterator();
      it.next();
      it.remove();
    });
    assertThat(countOut(a, L)).isEqualTo(4);

    database.transaction(() -> {
      final Iterator<Edge> it = a.asVertex(true).getEdges(Vertex.DIRECTION.BOTH, E).iterator();
      it.next();
      it.remove();
    });
    assertThat(countOut(a, E)).isEqualTo(189);

    database.transaction(() -> {
      final Iterator<Edge> it = a.asVertex(true).getEdges().iterator();
      it.next();
      it.remove();
    });
    assertThat(countOut(a, E) + countOut(a, L)).isEqualTo(189 + 4 - 1);
    assertCleanAfterwards();
  }

  // ------------------------------------------------------------------------------------------------------------------

  /** Vertex {@code a} with {@code regular} edges of E to distinct targets, then {@code light} LIGHTWEIGHT edges of L. */
  private RID createGraph(final int regular, final int light) {
    database.transaction(() -> {
      database.getSchema().createVertexType(V, 1);
      database.getSchema().createEdgeType(E, 1);
      database.getSchema().buildEdgeType().withName(L).withLightweight(true).create();
    });
    final RID[] a = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex(V).set("name", "a").save();
      a[0] = v.getIdentity();
      for (int i = 0; i < regular; i++)
        v.newEdge(E, database.newVertex(V).set("i", i).save());
    });
    database.transaction(() -> {
      final Vertex v = a[0].asVertex(true);
      for (int i = 0; i < light; i++)
        v.newEdge(L, database.newVertex(V).set("l", i).save());
    });
    return a[0];
  }

  /** Frees the record of one E edge of {@code a} without touching the lists: the entry in both lists now dangles. */
  private void makeOneEdgeDangle(final RID a) {
    final RID[] edge = new RID[1];
    database.transaction(() -> edge[0] = a.asVertex(true).getEdges(Vertex.DIRECTION.OUT, E).iterator().next().getIdentity());
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, edge[0]));
  }

  private boolean headIsStriped(final RID a) {
    final boolean[] striped = new boolean[1];
    database.transaction(() -> {
      final RID head = ((VertexInternal) a.asVertex(true)).getOutEdgesHeadChunk();
      final Record record = ((DatabaseInternal) database).lookupByRID(head, true);
      striped[0] = record instanceof StripeDirectory;
    });
    return striped[0];
  }

  private long countOut(final RID a, final String type) {
    final long[] count = new long[1];
    database.transaction(() -> {
      long n = 0;
      for (final Edge ignored : a.asVertex(true).getEdges(Vertex.DIRECTION.OUT, type))
        ++n;
      count[0] = n;
    });
    return count[0];
  }

  private Map<String, Object> checkDatabaseFix() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX")) {
      final Result row = rs.next();
      return row.toMap();
    }
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
