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
import com.arcadedb.database.RID;
import com.arcadedb.graph.DenseNodeIdProvider;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10471: since #10432 a Graph Analytical View that holds an overlay answers {@code getNeighborView()} with the overlay
 * applied to the base, packed for the whole graph and built once per snapshot. A snapshot is replaced by every commit, so a
 * point lookup - a 2-hop count from one indexed vertex, 64 entries - that asked for the view built the adjacency of the whole
 * graph for itself alone, once per commit: 3 to 6 times slower point reads while writes were committed. A read now asks for the
 * view only when the entries it expects to visit pay for the build, and walks per node otherwise, as it did before #10411.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10471PointReadsOverAnOverlayTest extends TestHelper {
  private static final int    VERTICES = 3000;
  private static final int    DEGREE   = 8;
  private static final String POINT    = "MATCH (a:P {pid: $p})-[:K]->(b:P)-[:K]->(c:P) RETURN count(*) AS n";
  private static final String WHOLE    = "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) RETURN count(*) AS n";

  private final List<RID> vertices = new ArrayList<>();
  private final Random    random   = new Random(10471);

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("P").createProperty("pid", Type.INTEGER);
    database.getSchema().createEdgeType("K");
    database.command("sql", "CREATE INDEX ON P (pid) UNIQUE").close();

    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        vertices.add(database.newVertex("P").set("pid", i).save().getIdentity());
      for (int i = 0; i < VERTICES; i++)
        for (int k = 0; k < DEGREE; k++)
          vertices.get(i).asVertex().modify().newEdge("K", vertices.get((i + 1 + random.nextInt(VERTICES - 1)) % VERTICES));
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void pointReadsDoNotBuildTheWholeGraphViewOfEachSnapshot(final String mode) {
    final GraphAnalyticalView view = createView(mode);

    for (int round = 0; round < 6; round++) {
      commitAFewEdges();
      awaitView(view);
      assertThat(view.hasActiveOverlay()).as("the overlay lives until the compaction threshold").isTrue();

      for (int i = 0; i < 20; i++)
        pointCount(random.nextInt(VERTICES));

      assertThat(view.pinned().getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K"))
          .as("round " + round + ": point reads left the whole-graph view unbuilt").isPositive();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aPointReadAnswersWhatTheRecordsAnswer(final String mode) {
    final GraphAnalyticalView view = createView(mode);
    commitAFewEdges();
    awaitView(view);

    for (int i = 0; i < 30; i++) {
      final int pid = random.nextInt(VERTICES);
      assertThat(pointCount(pid)).as("pid " + pid).isEqualTo(pointCountWithoutTheView(view, pid));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aWholeGraphReadStillBuildsTheViewOnceAndLaterReadsReuseIt(final String mode) {
    final GraphAnalyticalView view = createView(mode);
    commitAFewEdges();
    awaitView(view);
    assertThat(view.pinned().getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K")).isPositive();

    final long whole = count(WHOLE);

    assertThat(view.pinned().getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K")).as("the whole-graph read built the view").isZero();
    assertThat(whole).isEqualTo(countWithoutTheView(view, WHOLE));
    // the view is ready: even a read of one node takes it
    assertThat(view.pinned().getNeighborViewForVisits(1, Vertex.DIRECTION.OUT, "K")).isNotNull();
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aViewIsOnlyBuiltForAReadThatPaysForIt(final String mode) {
    final GraphAnalyticalView view = createView(mode);
    commitAFewEdges();
    awaitView(view);
    final GraphTraversalProvider pinned = view.pinned();
    final long cost = pinned.getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K");

    assertThat(cost).as("the nodes and the entries of the adjacency").isGreaterThan((long) VERTICES * DEGREE);
    assertThat(pinned.getNeighborViewForVisits(DEGREE * DEGREE, Vertex.DIRECTION.OUT, "K")).as("64 entries do not pay for it").isNull();
    assertThat(pinned.getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K")).as("and nothing was built").isEqualTo(cost);

    final NeighborView view1 = pinned.getNeighborViewForVisits(cost, Vertex.DIRECTION.OUT, "K");
    assertThat(view1).as("a read that visits as many entries as the build writes takes it").isNotNull();
    assertThat(pinned.getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K")).isZero();
    assertThat(pinned.getMeanDegree(Vertex.DIRECTION.OUT, "K")).isBetween(DEGREE - 0.5, DEGREE + 0.5);
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void theSmallReadsOfASnapshotBuildTheViewOnceTheyHaveSpentWhatItCosts(final String mode) {
    final GraphAnalyticalView view = createView(mode);
    commitAFewEdges();
    awaitView(view);
    final GraphTraversalProvider pinned = view.pinned();
    final long cost = pinned.getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K");
    final long read = DEGREE * DEGREE;

    long spent = 0;
    while (spent + read < cost) {
      assertThat(pinned.getNeighborViewForVisits(read, Vertex.DIRECTION.OUT, "K")).as("after " + spent + " of " + cost).isNull();
      spent += read;
    }

    assertThat(pinned.getNeighborViewForVisits(read, Vertex.DIRECTION.OUT, "K")).as("the read that brings the sum to the cost").isNotNull();
    assertThat(pinned.getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K")).isZero();
    // the next commit replaces the snapshot, and its reads start from nothing
    commitAFewEdges();
    awaitView(view);
    assertThat(view.pinned().getNeighborViewForVisits(read, Vertex.DIRECTION.OUT, "K")).isNull();
  }

  /**
   * The constant-time answer to "does the overlay touch this slice" is the one the walk of every changed node gives: after random
   * additions and deletions of edges, in both directions, for a type that has edges and one that has none.
   */
  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void theConstantTimeCheckOfASliceAgreesWithTheNodesTheOverlayChanges(final String mode) {
    final GraphAnalyticalView view = createView(mode);

    for (int round = 0; round < 12; round++) {
      final int additions = random.nextInt(3);
      database.transaction(() -> {
        for (int i = 0; i < additions; i++)
          vertices.get(random.nextInt(VERTICES)).asVertex().modify().newEdge("K", vertices.get(random.nextInt(VERTICES)));
        if (random.nextBoolean())
          vertices.get(random.nextInt(VERTICES)).asVertex().getEdges(Vertex.DIRECTION.OUT, "K").forEach(edge -> {
            if (random.nextBoolean())
              edge.delete();
          });
      });
      awaitView(view);

      final DeltaOverlay overlay = view.pinned().snapshot().overlay;
      if (overlay == null)
        continue;
      for (final String slice : new String[] { "K", "NoSuchEdgeType" })
        for (final boolean outgoing : new boolean[] { true, false }) {
          final BitSet changed = new BitSet();
          overlay.markChangedNodes(slice, outgoing, changed);
          assertThat(overlay.changesNodes(slice, outgoing)).as("round " + round + " " + slice + (outgoing ? " out" : " in"))
              .isEqualTo(!changed.isEmpty());
        }
    }
  }

  /** A change that only deletes edges still touches the slice, in both directions: the shortcut must not read it as untouched. */
  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aCommitThatOnlyDeletesEdgesIsSeenByTheConstantTimeCheck(final String mode) {
    final GraphAnalyticalView view = createView(mode);
    final List<Edge> deleted = new ArrayList<>();
    for (final Edge edge : vertices.get(3).asVertex().getEdges(Vertex.DIRECTION.OUT, "K"))
      deleted.add(edge);
    database.transaction(() -> deleted.forEach(Edge::delete));
    awaitView(view);

    final DeltaOverlay overlay = view.pinned().snapshot().overlay;
    assertThat(overlay).isNotNull();
    for (final boolean outgoing : new boolean[] { true, false }) {
      final BitSet changed = new BitSet();
      overlay.markChangedNodes("K", outgoing, changed);
      assertThat(changed.isEmpty()).as("the deletion changes nodes " + (outgoing ? "out" : "in")).isFalse();
      assertThat(overlay.changesNodes("K", outgoing)).isTrue();
    }
  }

  /**
   * A supernode costs far more than the mean degree says. The estimate only decides whether the view is built, never what is
   * answered: the count from a hub is the one the records give, and the view is still not built for it.
   */
  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aReadFromAHubAnswersWhatTheRecordsAnswer(final String mode) {
    final GraphAnalyticalView view = createView(mode);
    database.transaction(() -> {
      for (int i = 0; i < 1500; i++)
        vertices.get(0).asVertex().modify().newEdge("K", vertices.get(1 + random.nextInt(VERTICES - 1)));
    });
    awaitView(view);

    assertThat(pointCount(0)).isEqualTo(pointCountWithoutTheView(view, 0));
    assertThat(pointCount(1)).isEqualTo(pointCountWithoutTheView(view, 1));
  }

  /**
   * A hub is billed for its own degree, not for the mean: a read from a vertex of a few edges leaves the view unbuilt, a read from
   * one with thousands of edges visits more entries than the view costs and builds it, and both answer what the records answer.
   */
  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aReadFromAHubIsBilledForItsRealDegree(final String mode) {
    final GraphAnalyticalView view = createView(mode);
    database.transaction(() -> {
      for (int i = 0; i < 6000; i++)
        vertices.get(0).asVertex().modify().newEdge("K", vertices.get(1 + random.nextInt(VERTICES - 1)));
    });
    awaitView(view);
    assertThat(view.pinned().getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K")).isPositive();

    assertThat(pointCount(5)).isEqualTo(pointCountWithoutTheView(view, 5));
    assertThat(view.pinned().getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K")).as("a vertex of a few edges does not build it")
        .isPositive();

    assertThat(pointCount(0)).isEqualTo(pointCountWithoutTheView(view, 0));
    assertThat(view.pinned().getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K")).as("the hub's 6000 edges pay for it").isZero();
  }

  /**
   * The renumbering of a view with deleted vertices asks the view once: the visits of a read are counted once, so the view is
   * built by the read that brings the sum to its cost, not earlier.
   */
  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aRenumberedViewCountsTheVisitsOfAReadOnce(final String mode) {
    final GraphAnalyticalView view = createView(mode);
    database.transaction(() -> {
      commitEdges();
      vertices.get(5).asVertex().delete();
    });
    awaitView(view);

    final GraphTraversalProvider dense = DenseNodeIdProvider.wrap(view.pinned());
    assertThat(dense).as("a view with a deleted vertex has holes in its ids").isInstanceOf(DenseNodeIdProvider.class);
    final long cost = dense.getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K");
    assertThat(cost).isPositive();
    final long read = DEGREE * DEGREE;

    long spent = 0;
    while (spent + read < cost) {
      assertThat(dense.getNeighborViewForVisits(read, Vertex.DIRECTION.OUT, "K")).as("after " + spent + " of " + cost).isNull();
      spent += read;
    }
    assertThat(dense.getNeighborViewForVisits(read, Vertex.DIRECTION.OUT, "K")).isNotNull();

    // the view of the snapshot is given back under memory pressure; the renumbered one is held on its own and still serves
    ((GraphAnalyticalView.Pinned) ((DenseNodeIdProvider) dense).getDelegate()).snapshot().mergedNeighborViews.values()
        .forEach(SoftReference::clear);
    assertThat(dense.getNeighborViewForVisits(read, Vertex.DIRECTION.OUT, "K")).as("the renumbered view is held").isNotNull();
  }

  /**
   * A view the memory pressure gave back (it is held softly) is earned again by the reads, not rebuilt by the first small read
   * after it: what they spent on it went with it.
   */
  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aViewGivenBackUnderMemoryPressureIsEarnedAgain(final String mode) {
    final GraphAnalyticalView view = createView(mode);
    commitAFewEdges();
    awaitView(view);
    final GraphAnalyticalView.Pinned pinned = view.pinned();
    final long cost = pinned.getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K");
    final long read = DEGREE * DEGREE;

    // small reads spend what the view costs, and the last of them builds it
    long spent = 0;
    while (spent + read < cost) {
      assertThat(pinned.getNeighborViewForVisits(read, Vertex.DIRECTION.OUT, "K")).isNull();
      spent += read;
    }
    assertThat(pinned.getNeighborViewForVisits(read, Vertex.DIRECTION.OUT, "K")).isNotNull();
    assertThat(pinned.getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K")).isZero();

    // the memory pressure clears the soft references of the snapshot
    pinned.snapshot().mergedNeighborViews.values().forEach(SoftReference::clear);
    assertThat(pinned.getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K")).as("the view is gone").isEqualTo(cost);

    spent = 0;
    while (spent + read < cost) {
      assertThat(pinned.getNeighborViewForVisits(read, Vertex.DIRECTION.OUT, "K")).as("after " + spent + " of " + cost).isNull();
      spent += read;
    }
    assertThat(pinned.getNeighborViewForVisits(read, Vertex.DIRECTION.OUT, "K")).as("earned again").isNotNull();
  }

  /** The chains that walk both directions, or track their relationships, take the per-node path of a hop with no view. */
  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void chainsWalkingBothDirectionsOrTrackingTheirRelationshipsAnswerWhatTheRecordsAnswer(final String mode) {
    final GraphAnalyticalView view = createView(mode);
    commitAFewEdges();
    awaitView(view);

    for (final String chain : new String[] { "(a:P {pid: $p})-[:K]-(b:P)-[:K]-(c:P)", "(a:P {pid: $p})-[r1:K]->(b:P)-[r2:K]->(c:P)",
        "(a:P {pid: $p})<-[:K]-(b:P)-[:K]->(c:P)" })
      for (int i = 0; i < 10; i++) {
        final int pid = random.nextInt(VERTICES);
        final String query = "MATCH " + chain + " RETURN count(*) AS n";
        assertThat(runCount(query, pid)).as(chain + " from " + pid).isEqualTo(runCountWithoutTheView(view, query, pid));
      }
  }

  private long runCount(final String query, final int pid) {
    try (final ResultSet rs = database.query("opencypher", query, Map.of("p", pid))) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }

  private long runCountWithoutTheView(final GraphAnalyticalView view, final String query, final int pid) {
    GraphTraversalProviderRegistry.unregister(database, view);
    try {
      return runCount(query + " /* records */", pid);
    } finally {
      GraphTraversalProviderRegistry.register(database, view);
    }
  }

  private void commitEdges() {
    for (int i = 0; i < 2; i++)
      vertices.get(random.nextInt(VERTICES)).asVertex().modify().newEdge("K", vertices.get(random.nextInt(VERTICES)));
  }

  private GraphAnalyticalView createView(final String mode) {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P) EDGE TYPES (K) UPDATE MODE " + mode).close();
    final GraphAnalyticalView view = awaitView(null);
    // warm the plans, so the first query of the test is not the one that plans
    pointCount(0);
    count(WHOLE);
    return view;
  }

  private void commitAFewEdges() {
    database.transaction(() -> {
      for (int i = 0; i < 2; i++)
        vertices.get(random.nextInt(VERTICES)).asVertex().modify().newEdge("K", vertices.get(random.nextInt(VERTICES)));
    });
  }

  private long pointCount(final int pid) {
    try (final ResultSet rs = database.query("opencypher", POINT, Map.of("p", pid))) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }

  private long pointCountWithoutTheView(final GraphAnalyticalView view, final int pid) {
    GraphTraversalProviderRegistry.unregister(database, view);
    try {
      try (final ResultSet rs = database.query("opencypher", POINT + " /* records */", Map.of("p", pid))) {
        return rs.next().<Number>getProperty("n").longValue();
      }
    } finally {
      GraphTraversalProviderRegistry.register(database, view);
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }

  private long countWithoutTheView(final GraphAnalyticalView view, final String query) {
    GraphTraversalProviderRegistry.unregister(database, view);
    try {
      return count(query + " /* records */");
    } finally {
      GraphTraversalProviderRegistry.register(database, view);
    }
  }

  private GraphAnalyticalView awaitView(final GraphAnalyticalView known) {
    final GraphAnalyticalView view = known != null ? known : GraphAnalyticalViewRegistry.get(database, "gav");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    return view;
  }
}
