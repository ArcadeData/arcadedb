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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.DenseNodeIdProvider;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10350: right after a commit that deleted a vertex together with the edge it had just been given, {@code algo.wcc} and
 * {@code algo.scc} answered from a Graph Analytical View with one null node for a few milliseconds. Two defects met:
 * <ul>
 *   <li>the edge's {@code save()} reports a create and an update of it. The create never reaches the overlay, its source being
 *   deleted by the same transaction (#10339), and the update, finding no edge of the overlay to land on, marked the edge type's
 *   property columns out of date, which forces a full rebuild. An edge the view does not hold has no column to be out of date:
 *   the same happened for every edge to a vertex type the view does not cover, a rebuild per commit;</li>
 *   <li>the rebuild publishes a base numbered afresh, and an algorithm asks the view several questions - which ids are live, then
 *   the RID of each - each of them answered by the snapshot being served at that moment. One that started on the snapshot with
 *   the overlay and ended on the rebuilt one resolved ids of the first numbering in the second: one past its end came back null,
 *   the others as other vertices. An algorithm now reads one snapshot from start to end ({@link GraphTraversalProvider#pinned()}).</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10350ViewSnapshotSwapTest extends TestHelper {
  private static final int PAD = 2_000;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE Q");
    database.command("sql", "CREATE VERTEX TYPE R");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE L");
  }

  /** The reporter's sequence: the edge created and its source deleted by one transaction leaves the base in place. */
  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void anEdgeGoneWithItsSourceForcesNoRebuild(final String mode) {
    final RID[] ends = createPaddedGraph();
    final GraphAnalyticalView view = createView(mode);
    final Object numbering = view.getNodeIdSpace();

    final RID[] created = new RID[1];
    database.transaction(() -> {
      ends[1].asVertex().delete();
      created[0] = database.newVertex("P").set("id", 3L).save().getIdentity();
    });
    database.transaction(() -> {
      final Vertex c = created[0].asVertex();
      c.newEdge("L", ends[0]).save();
      c.delete();
    });
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();

    assertColumnsUpToDate(view);
    assertThat(view.getNodeIdSpace()).as("the base was not rebuilt").isSameAs(numbering);

    final Set<RID> live = liveVertices();
    assertThat(live).hasSize(PAD + 1);
    for (final String algorithm : new String[] { "wcc", "scc" })
      assertThat(nodesAnswered(algorithm)).as(algorithm).containsExactlyInAnyOrderElementsOf(live);
  }

  /** An edge to a vertex type the view does not cover is not in the view, created or updated: no column of it can be out of date. */
  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void anEdgeToAnUncoveredVertexForcesNoRebuild(final String mode) {
    final RID[] ends = createPaddedGraph();
    final GraphAnalyticalView view = createView(mode);
    final Object numbering = view.getNodeIdSpace();

    final RID[] edge = new RID[1];
    database.transaction(() -> {
      final MutableVertex outside = database.newVertex("R").set("id", 9L).save();
      edge[0] = ends[0].asVertex().newEdge("L", outside).set("ep", 1).save().getIdentity();
    });
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertColumnsUpToDate(view);

    database.transaction(() -> edge[0].asEdge().modify().set("ep", 2).save());
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertColumnsUpToDate(view);

    assertThat(view.getNodeIdSpace()).as("the base was not rebuilt").isSameAs(numbering);
  }

  /**
   * The race itself, made deterministic: a pinned view keeps answering from the snapshot it pinned after the view publishes a
   * base numbered afresh, which a provider asked one question at a time does not.
   */
  @Test
  void aPinnedViewAnswersFromItsSnapshotAcrossARebuild() {
    final RID[] ends = createPaddedGraph();
    final GraphAnalyticalView view = createView("SYNCHRONOUS");
    // a base vertex deleted and an overflow one created: the id space has a hole and an id past the base mapping
    database.transaction(() -> {
      ends[1].asVertex().delete();
      final MutableVertex c = database.newVertex("P").set("id", 3L).save();
      c.newEdge("K", ends[0]);
    });
    assertThat(view.hasPendingChanges()).isTrue();
    final Set<RID> live = liveVertices();

    final GraphTraversalProvider pinned = view.pinned();
    final GraphTraversalProvider dense = DenseNodeIdProvider.wrap(pinned);
    assertThat(dense).as("the overlay holds a deleted node: the ids are renumbered").isNotSameAs(pinned);

    view.build(); // publishes a compacted base: every id after the hole now names another vertex
    assertThat(view.hasPendingChanges()).isFalse();
    assertThat(pinned.hasPendingChanges()).as("the pinned snapshot still has its overlay").isTrue();
    assertThat(pinned.getNodeIdSpace()).isNotSameAs(view.getNodeIdSpace());

    final Set<RID> answered = new HashSet<>();
    for (int i = 0; i < dense.getNodeCount(); i++) {
      final RID rid = dense.getRID(i);
      assertThat(rid).as("dense id %d", i).isNotNull();
      assertThat(dense.getNodeId(rid)).isEqualTo(i);
      answered.add(rid);
    }
    assertThat(answered).containsExactlyInAnyOrderElementsOf(live);

    // the edge added in the overlay is still there, between the vertices it joins
    final int a = dense.getNodeId(ends[0]);
    final int[] in = dense.getNeighborIds(a, Vertex.DIRECTION.IN, "K");
    assertThat(in).hasSize(1);
    assertThat(dense.getRID(in[0]).asVertex().getInteger("id")).isEqualTo(3);
    assertThat(dense.countEdges(a, Vertex.DIRECTION.IN, "K")).isEqualTo(1);
  }

  /**
   * End to end: the view rebuilt while an algorithm streams its rows. The rows read after the rebuild still name the vertices of
   * the snapshot the algorithm ran on.
   */
  @ParameterizedTest(name = "{0}")
  @CsvSource({ "wcc", "scc", "pagerank", "labelpropagation" })
  void anAlgorithmStreamsItsAnswerFromOneSnapshot(final String algorithm) {
    final RID[] ends = createPaddedGraph();
    final GraphAnalyticalView view = createView("SYNCHRONOUS");
    database.transaction(() -> {
      ends[1].asVertex().delete();
      database.newVertex("P").set("id", 3L).save().newEdge("K", ends[0]);
    });
    assertThat(view.hasPendingChanges()).isTrue();
    final Set<RID> live = liveVertices();

    final List<RID> answered = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "CALL algo." + algorithm + "() YIELD node RETURN node")) {
      answered.add(nodeOf(rs.next()));
      view.build(); // the base the algorithm's later rows are resolved against, unless it pinned its own
      while (rs.hasNext())
        answered.add(nodeOf(rs.next()));
    }
    assertThat(answered).doesNotContainNull().doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(live);
  }

  private static RID nodeOf(final Result row) {
    final Object node = row.getProperty("node");
    return node instanceof Identifiable identifiable ? identifiable.getIdentity() : null;
  }

  /** Padding Q vertices, then the P vertices {@code a} and {@code b}: returns their RIDs. */
  private RID[] createPaddedGraph() {
    final RID[] ends = new RID[2];
    database.transaction(() -> {
      for (int i = 0; i < PAD; i++)
        database.newVertex("Q").set("id", (long) i).save();
      ends[0] = database.newVertex("P").set("id", 1L).set("w", 0.5).set("s", "a").save().getIdentity();
      ends[1] = database.newVertex("P").set("id", 2L).set("w", 0.5).set("s", "a").save().getIdentity();
    });
    return ends;
  }

  private GraphAnalyticalView createView(final String mode) {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P, Q) EDGE TYPES (K, L) PROPERTIES (id, w, s) "
        + "EDGE PROPERTIES (ep) UPDATE MODE " + mode).close();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    return view;
  }

  private static void assertColumnsUpToDate(final GraphAnalyticalView view) {
    final DeltaOverlay overlay = view.currentSnapshot().overlay;
    assertThat(overlay == null || !overlay.isEdgePropertiesDirty()).as("no edge column is out of date").isTrue();
  }

  private Set<RID> liveVertices() {
    final Set<RID> live = new HashSet<>();
    for (final String type : new String[] { "P", "Q" })
      database.iterateType(type, false).forEachRemaining(r -> live.add(r.getIdentity()));
    return live;
  }

  private List<RID> nodesAnswered(final String algorithm) {
    final List<RID> nodes = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "CALL algo." + algorithm + "() YIELD node RETURN node")) {
      while (rs.hasNext())
        nodes.add(nodeOf(rs.next()));
    }
    return nodes;
  }
}
