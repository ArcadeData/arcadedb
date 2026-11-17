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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.DenseNodeIdProvider;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.NodeEdgeWeights;
import com.arcadedb.graph.StripeDirectory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests of GavOverlayRead, one nested class per reported case.
 */
class GavOverlayReadTest {
  /**
   * Regression for #10411: a Graph Analytical View that held any overlay answered {@code getNeighborView()} with null, so every
   * whole-graph reader - the Cypher count operators, the algorithms - took the per-node path for as long as the overlay lived:
   * one committed edge made a 2-hop count about 27% slower until the overlay was compacted, and since #10212 an ASYNCHRONOUS
   * view merges every commit into an overlay. The view now serves the overlay applied to the base, built once per snapshot.
   * <p>
   * The view must answer exactly what the per-node reads answer: checked entry for entry against {@code getNeighborIds()} for
   * every node, every direction and every type combination, after random additions, deletions, new vertices, parallel edges and
   * self-loops, and through the Cypher counts against the records.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class OverlayNeighborView extends TestHelper {
    private static final String[][] TYPE_SETS = { { "K" }, { "J" }, { "K", "J" }, null };

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE EDGE TYPE K");
      database.command("sql", "CREATE EDGE TYPE J");
    }

    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void theOverlayViewIsThePerNodeAnswer(final String mode) {
      final Random random = new Random(10411);
      final List<RID> vertices = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < 60; i++)
          vertices.add(database.newVertex("P").set("id", i).save().getIdentity());
        for (int i = 0; i < 240; i++)
          connect(vertices, random);
      });

      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P) EDGE TYPES (K, J) UPDATE MODE " + mode).close();
      final GraphAnalyticalView view = awaitView();
      assertViewIsThePerNodeAnswer(view);

      for (int round = 0; round < 8; round++) {
        database.transaction(() -> {
          for (int i = 0; i < 6; i++)
            connect(vertices, random);
          // a vertex only the overlay holds, with edges both ways and a self-loop
          final MutableVertex added = database.newVertex("P").set("id", vertices.size()).save();
          added.newEdge("K", vertices.get(random.nextInt(vertices.size())));
          vertices.get(random.nextInt(vertices.size())).asVertex().modify().newEdge("J", added);
          added.newEdge("K", added);
          vertices.add(added.getIdentity());
          // an edge deleted, and a vertex deleted with all its edges
          final Vertex source = vertices.get(random.nextInt(vertices.size())).asVertex();
          source.getEdges(Vertex.DIRECTION.OUT).forEach(edge -> {
            if (random.nextBoolean())
              edge.delete();
          });
          vertices.remove(random.nextInt(vertices.size())).asVertex().delete();
        });
        awaitView();

        assertThat(view.hasActiveOverlay()).as("the overlay lives until the compaction threshold").isTrue();
        assertViewIsThePerNodeAnswer(view);
        assertCountsMatchTheRecords();
      }
    }

    private static void connect(final List<RID> vertices, final Random random) {
      final RID from = vertices.get(random.nextInt(vertices.size()));
      // a self-loop now and then, and parallel edges from the small id range
      final RID to = random.nextInt(10) == 0 ? from : vertices.get(random.nextInt(Math.min(vertices.size(), 20)));
      from.asVertex().modify().newEdge(random.nextBoolean() ? "K" : "J", to);
    }

    /** Every node's range of every view is, entry for entry, what {@code getNeighborIds()} answers for it. */
    private void assertViewIsThePerNodeAnswer(final GraphAnalyticalView view) {
      final GraphTraversalProvider pinned = view.pinned();
      final int bound = pinned.getNodeIdUpperBound();
      for (final Vertex.DIRECTION direction : Vertex.DIRECTION.values())
        for (final String[] types : TYPE_SETS) {
          final NeighborView neighbors = types == null ? pinned.getNeighborView(direction) : pinned.getNeighborView(direction, types);
          assertThat(neighbors).as("a view while the overlay is active, " + direction + " " + Arrays.toString(types)).isNotNull();
          assertThat(neighbors.nodeCount()).isEqualTo(bound);
          for (int node = 0; node < bound; node++) {
            final int[] expected = types == null ? pinned.getNeighborIds(node, direction) : pinned.getNeighborIds(node, direction, types);
            final int[] actual = Arrays.copyOfRange(neighbors.neighbors(), neighbors.offset(node), neighbors.offsetEnd(node));
            assertThat(actual).as("node " + node + " " + direction + " " + Arrays.toString(types)).containsExactly(expected);
          }
        }
      final int[] degrees = new int[bound];
      for (final String slice : new String[] { "K", "J" })
        for (final Vertex.DIRECTION direction : Vertex.DIRECTION.values()) {
          final NeighborView neighbors = pinned.getNeighborViewOfSlice(direction, slice);
          assertThat(neighbors).isNotNull();
          pinned.getDegrees(degrees, direction, slice);
          for (int node = 0; node < bound; node++) {
            final int[] expected = pinned.getNeighborIdsOfSlice(node, direction, slice);
            assertThat(Arrays.copyOfRange(neighbors.neighbors(), neighbors.offset(node), neighbors.offsetEnd(node)))
                .containsExactly(expected);
            if (pinned.isNodeLive(node))
              assertThat(degrees[node]).as("degree of node " + node + " " + direction + " " + slice).isEqualTo(expected.length);
          }
        }
    }

    /** The counts the view-served Cypher operators answer are the ones the records give. */
    private void assertCountsMatchTheRecords() {
      for (final String pattern : new String[] {
          "(a:P)-[:K]->(b:P)-[:K]->(c:P)",
          "(a:P)-[:K]->(b:P)-[:J]->(c:P)",
          "(a:P)-[:K|J]-(b:P)",
          "(a:P)-[:K]-(b:P)-[:K]-(c:P)",
          "(a:P)<-[:J]-(b:P)-[:K]->(c:P)" }) {
        final long viewed = count("MATCH " + pattern + " RETURN count(*) AS n");
        final long recorded = countWithoutTheView(pattern);
        assertThat(viewed).as(pattern).isEqualTo(recorded);
      }
    }

    /** The same count off the records: the view withheld from the planner, under a query text no cached plan holds. */
    private long countWithoutTheView(final String pattern) {
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
      GraphTraversalProviderRegistry.unregister(database, view);
      try {
        return count("MATCH " + pattern + " RETURN count(*) AS n /* records */");
      } finally {
        GraphTraversalProviderRegistry.register(database, view);
      }
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return rs.next().<Number>getProperty("n").longValue();
      }
    }

    private GraphAnalyticalView awaitView() {
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      return view;
    }
  }

  /**
   * Regression for #10471: since #10432 a Graph Analytical View that holds an overlay answers {@code getNeighborView()} with the overlay
   * applied to the base, packed for the whole graph and built once per snapshot. A snapshot is replaced by every commit, so a
   * point lookup - a 2-hop count from one indexed vertex, 64 entries - that asked for the view built the adjacency of the whole
   * graph for itself alone, once per commit: 3 to 6 times slower point reads while writes were committed. A read now asks for the
   * view only when the entries it expects to visit pay for the build, and walks per node otherwise, as it did before #10411.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PointReadsOverAnOverlay extends TestHelper {
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

    /** A held renumbered view of a slice is found by the key it was stored under, the delegate's view having been given back. */
    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void aHeldRenumberedSliceViewIsFoundWhateverBecameOfTheDelegatesView(final String mode) {
      final GraphAnalyticalView view = createView(mode);
      database.transaction(() -> {
        commitEdges();
        vertices.get(5).asVertex().delete();
      });
      awaitView(view);
      final GraphTraversalProvider dense = DenseNodeIdProvider.wrap(view.pinned());
      assertThat(dense).as("a view with a deleted vertex has holes in its ids").isInstanceOf(DenseNodeIdProvider.class);
      final GraphTraversalProvider delegate = ((DenseNodeIdProvider) dense).getDelegate();
      final NeighborView built = dense.getNeighborViewOfSlice(Vertex.DIRECTION.OUT, "K");
      assertThat(built).isNotNull();

      ((GraphAnalyticalView.Pinned) delegate).snapshot().mergedNeighborViews.values().forEach(SoftReference::clear);

      // the snapshot gave its view back, so a read of one visit does not pay for it again (or the test proves nothing)
      assertThat(delegate.getNeighborViewOfSliceBuildCost(Vertex.DIRECTION.OUT, "K")).isGreaterThan(1);
      assertThat(delegate.getNeighborViewOfSliceForVisits(1, Vertex.DIRECTION.OUT, "K")).as("the delegate does not rebuild for one visit")
          .isNull();

      // and the renumbered view that was built is still served, found by the key it was stored under
      assertThat(dense.getNeighborViewOfSliceForVisits(1, Vertex.DIRECTION.OUT, "K")).isSameAs(built);
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

    /** An estimate that is negative, or the largest a long holds, neither takes away from the demand nor overflows it. */
    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void anEstimateOutOfRangeIsHarmless(final String mode) {
      final GraphAnalyticalView view = createView(mode);
      commitAFewEdges();
      awaitView(view);
      final GraphAnalyticalView.Pinned pinned = view.pinned();
      final long cost = pinned.getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K");
      final long read = DEGREE * DEGREE;

      assertThat(pinned.getNeighborViewForVisits(Long.MIN_VALUE, Vertex.DIRECTION.OUT, "K")).isNull();
      assertThat(pinned.getNeighborViewForVisits(-1, Vertex.DIRECTION.OUT, "K")).isNull();

      // what the reads spent before is all there is: the negative ones did not take any of it away
      long spent = 0;
      while (spent + read < cost) {
        assertThat(pinned.getNeighborViewForVisits(read, Vertex.DIRECTION.OUT, "K")).isNull();
        spent += read;
      }
      assertThat(pinned.getNeighborViewForVisits(read, Vertex.DIRECTION.OUT, "K")).isNotNull();

      // and the largest estimate a long holds is a read that pays for the view of any snapshot, at once
      commitAFewEdges();
      awaitView(view);
      assertThat(view.pinned().getNeighborViewForVisits(Long.MAX_VALUE, Vertex.DIRECTION.OUT, "K")).isNotNull();
    }

    /**
     * Readers racing at the threshold, with the memory pressure clearing the view meanwhile: nobody fails, every view handed out is a
     * view of the snapshot (the same one while it is held), and the reads end up with it.
     */
    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void readersRacingAtTheThresholdAndTheEvictionAllGetAnAnswer(final String mode) throws Exception {
      final GraphAnalyticalView view = createView(mode);
      commitAFewEdges();
      awaitView(view);
      final GraphAnalyticalView.Pinned pinned = view.pinned();
      final long read = DEGREE * DEGREE;
      final int readers = 8;

      final ExecutorService pool = Executors.newFixedThreadPool(readers + 1);
      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicInteger failures = new AtomicInteger();
      final Set<NeighborView> handedOut = ConcurrentHashMap.newKeySet();
      final CountDownLatch start = new CountDownLatch(1);
      try {
        final List<Future<?>> done = new ArrayList<>();
        for (int r = 0; r < readers; r++)
          done.add(pool.submit(() -> {
            try {
              start.await();
              for (int i = 0; i < 3000; i++) {
                final NeighborView served = pinned.getNeighborViewForVisits(read, Vertex.DIRECTION.OUT, "K");
                if (served != null) {
                  assertThat(served.nodeCount()).isEqualTo(pinned.getNodeIdUpperBound());
                  handedOut.add(served);
                }
              }
            } catch (final Throwable t) {
              failures.incrementAndGet();
            }
          }));
        // the memory pressure: clears the references of the snapshot now and then
        final Future<?> pressure = pool.submit(() -> {
          try {
            start.await();
            while (!stop.get()) {
              pinned.snapshot().mergedNeighborViews.values().forEach(SoftReference::clear);
              Thread.sleep(1);
            }
          } catch (final Throwable t) {
            failures.incrementAndGet();
          }
        });

        start.countDown();
        for (final Future<?> reader : done)
          reader.get(120, TimeUnit.SECONDS);
        stop.set(true);
        pressure.get(30, TimeUnit.SECONDS);
      } finally {
        stop.set(true);
        pool.shutdownNow();
      }

      assertThat(failures.get()).as("readers and eviction that failed").isZero();
      assertThat(handedOut).as("the 24000 reads, many times the cost of the view, got it at least once").isNotEmpty();
      // once the pressure stops, the reads get the view and keep it
      NeighborView last = null;
      for (int i = 0; i < 100_000 && last == null; i++)
        last = pinned.getNeighborViewForVisits(read, Vertex.DIRECTION.OUT, "K");
      assertThat(last).isNotNull();
      assertThat(pinned.getNeighborViewForVisits(read, Vertex.DIRECTION.OUT, "K")).isSameAs(last);
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

  /**
   * Regression for #6315.
   * <p>
   * Regression tests for issue #6315 - the Graph Analytical View answered edge properties from the base CSR while
   * a delta overlay was active, so a caller reading them by neighbour position got a weight belonging to a
   * different edge.
   * <p>
   * The columns are aligned with the base CSR's forward edge slots. {@code getNeighborIds} serves the overlay's
   * view of the node instead: deleted edges dropped, added ones merged in, the whole list re-sorted. Delete the
   * first of three outgoing edges and every remaining neighbour shifts down one position, while the columns do
   * not - so position 1 answered with the deleted edge's weight, which is a wrong shortest path, a wrong MST and
   * a wrong Steiner tree, and never an exception. PR #6306 had closed that for the callers that existed by
   * narrowing {@code hasEdgeProperties()} to {@code false} whenever an overlay was active, which left the sharp
   * accessor in the SPI for the next caller to find.
   * <p>
   * The accessor is gone. {@link com.arcadedb.graph.GraphTraversalProvider#edgeWeightsForSlice} hands back the
   * neighbours and their weights together, paired where the overlay is applied, so there is no position to
   * address across and no rule for a caller to remember. Resolving the two rather than refusing them also keeps
   * the columnar path alive while a view is being updated, which is the state {@code UpdateMode.SYNCHRONOUS}
   * leaves it in after every single commit:
   * <ul>
   *   <li>a base edge the overlay has not deleted answers from its own column slot;</li>
   *   <li>an edge the overlay added answers from the value captured for it at commit time;</li>
   *   <li>an edge whose property was <em>updated</em> has neither - nothing maps a column slot back from an RID -
   *       so the view reports no edge properties until the rebuild that repairs the columns lands.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EdgeWeightsUnderOverlay {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6315-edge-weights-overlay");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("N");
      database.getSchema().createEdgeType("ROAD").createProperty("w", Type.DOUBLE);
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
        database = null;
      }
    }

    /**
     * The shape the issue is about: A has three outgoing edges of distinct weight, one of them is deleted in a
     * committed transaction, and the two that survive must still each answer with their own weight. Positionally
     * they no longer sit where they sat: dropping the first neighbour shifts the other two down one slot each.
     */
    @Test
    void aDeletedEdgeDoesNotShiftItsNeighboursOntoTheWrongWeights() {
      starGraph();

      final GraphAnalyticalView view = syncView("overlay-deleted-edge");
      try {
        final int a = view.getNodeId(vertex("A").getIdentity());
        assertThat(weightsByName(view, a)).containsExactlyInAnyOrderEntriesOf(Map.of("B", 10.0, "C", 20.0, "D", 30.0));

        deleteEdge("A", "B");

        assertThat(view.hasEdgeProperty("ROAD", "w"))
            .as("the overlay is resolved against the columns, not a reason to stop serving them")
            .isTrue();
        assertThat(weightsByName(view, a))
            .as("B is gone; C and D moved down a slot each and must keep their own weights, not B's and C's")
            .containsExactlyInAnyOrderEntriesOf(Map.of("C", 20.0, "D", 30.0));
      } finally {
        view.drop();
      }
    }

    /**
     * The same for the incoming direction, which reaches its columns through the backward-to-forward slot
     * mapping - a second addressing step the merge has to survive.
     */
    @Test
    void aDeletedEdgeDoesNotShiftIncomingNeighboursEither() {
      // B and C and D all point at A, so A's incoming slice is the one the deletion re-shapes.
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("N").set("name", "A").save();
        final MutableVertex b = database.newVertex("N").set("name", "B").save();
        final MutableVertex c = database.newVertex("N").set("name", "C").save();
        final MutableVertex d = database.newVertex("N").set("name", "D").save();
        b.newEdge("ROAD", a, true, new Object[] { "w", 10.0 }).save();
        c.newEdge("ROAD", a, true, new Object[] { "w", 20.0 }).save();
        d.newEdge("ROAD", a, true, new Object[] { "w", 30.0 }).save();
      });

      final GraphAnalyticalView view = syncView("overlay-deleted-incoming-edge");
      try {
        final int a = view.getNodeId(vertex("A").getIdentity());
        deleteEdge("B", "A");

        final Map<String, Double> incoming = new HashMap<>();
        final NodeEdgeWeights edges = view.edgeWeightsForSlice(a, Vertex.DIRECTION.IN, "ROAD", "w", -1.0, null);
        for (int i = 0; i < edges.neighbors().length; i++)
          incoming.put((String) view.getRID(edges.neighbors()[i]).asVertex().get("name"), edges.weights()[i]);

        assertThat(incoming).containsExactlyInAnyOrderEntriesOf(Map.of("C", 20.0, "D", 30.0));
      } finally {
        view.drop();
      }
    }

    /**
     * An edge the overlay added has no column slot at all - the columns were built with the base CSR - and used to
     * be the reason the whole node could not be answered for. It carries the value captured for it at commit time
     * instead, so the answer stays exact rather than defaulting the new edge to a unit weight.
     */
    @Test
    void anEdgeTheOverlayAddedAnswersWithItsOwnWeight() {
      starGraph();

      final GraphAnalyticalView view = syncView("overlay-added-edge");
      try {
        final int a = view.getNodeId(vertex("A").getIdentity());

        database.transaction(() -> {
          final MutableVertex e = database.newVertex("N").set("name", "E").save();
          vertex("A").newEdge("ROAD", e, true, new Object[] { "w", 99.0 }).save();
        });

        assertThat(weightsByName(view, a))
            .as("the added edge weighs what it was created with, not the default")
            .containsExactlyInAnyOrderEntriesOf(Map.of("B", 10.0, "C", 20.0, "D", 30.0, "E", 99.0));
      } finally {
        view.drop();
      }
    }

    /**
     * The limit of what the overlay can resolve, pinned so that it is refused rather than guessed. Deletions are
     * counted per pair - all the neighbour list needs, since dropping any one of a pair's parallel edges leaves
     * the same neighbours behind - but parallel edges need not weigh the same, and nothing says which of them
     * died. Answering would mean handing back a plausible wrong multiset of weights, which is the failure this
     * whole issue is about, so the view says it cannot serve the node and the caller reads the records.
     */
    @Test
    void aPartiallyDeletedRunOfParallelEdgesIsRefusedRatherThanGuessed() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("N").set("name", "A").save();
        final MutableVertex b = database.newVertex("N").set("name", "B").save();
        a.newEdge("ROAD", b, true, new Object[] { "w", 1.0 }).save();
        a.newEdge("ROAD", b, true, new Object[] { "w", 2.0 }).save();
        a.newEdge("ROAD", b, true, new Object[] { "w", 3.0 }).save();
      });

      final GraphAnalyticalView view = syncView("overlay-parallel-edges");
      try {
        final int a = view.getNodeId(vertex("A").getIdentity());

        // Delete the 2.0 edge and add a 4.0 one, both between the same pair.
        database.transaction(() -> {
          for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
            if (Double.valueOf(2.0).equals(edge.get("w")))
              edge.delete();
          vertex("A").newEdge("ROAD", vertex("B"), true, new Object[] { "w", 4.0 }).save();
        });

        assertThat(view.edgeWeightsForSlice(a, Vertex.DIRECTION.OUT, "ROAD", "w", -1.0, null))
            .as("which of the three parallel edges was deleted is not recoverable from a per-pair count")
            .isNull();
        assertThat(view.edgeWeightsOf(a, Vertex.DIRECTION.OUT, "w", -1.0, "ROAD"))
            .as("and a node the provider cannot answer for makes the whole call fall back, not half of it")
            .isNull();
      } finally {
        view.drop();
      }
    }

    /**
     * The counterweight: parallel edges are only ambiguous while some of a pair's run survives a deletion.
     * Deleting the whole run leaves nothing to be ambiguous about, and adding to it identifies each new edge by
     * its own captured value, so both keep answering.
     */
    @Test
    void parallelEdgesStillAnswerWhenTheDeletionIsNotAmbiguous() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("N").set("name", "A").save();
        final MutableVertex b = database.newVertex("N").set("name", "B").save();
        final MutableVertex c = database.newVertex("N").set("name", "C").save();
        a.newEdge("ROAD", b, true, new Object[] { "w", 1.0 }).save();
        a.newEdge("ROAD", b, true, new Object[] { "w", 2.0 }).save();
        a.newEdge("ROAD", c, true, new Object[] { "w", 5.0 }).save();
      });

      final GraphAnalyticalView view = syncView("overlay-parallel-edges-unambiguous");
      try {
        final int a = view.getNodeId(vertex("A").getIdentity());

        // Both A->B edges go, and two more parallel ones are added between the same pair.
        database.transaction(() -> {
          for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
            if ("B".equals(edge.getInVertex().get("name")))
              edge.delete();
          vertex("A").newEdge("ROAD", vertex("B"), true, new Object[] { "w", 7.0 }).save();
          vertex("A").newEdge("ROAD", vertex("B"), true, new Object[] { "w", 8.0 }).save();
        });

        final NodeEdgeWeights edges = view.edgeWeightsForSlice(a, Vertex.DIRECTION.OUT, "ROAD", "w", -1.0, null);
        assertThat(edges.neighbors()).isEqualTo(view.getNeighborIds(a, Vertex.DIRECTION.OUT, "ROAD"));
        assertThat(edges.weights()).containsExactlyInAnyOrder(5.0, 7.0, 8.0);
      } finally {
        view.drop();
      }
    }

    /**
     * The one thing the overlay cannot repair. An edge already in the base CSR is addressed by a column slot and
     * nothing maps that slot back from its RID, so a committed change to its weight leaves the columns holding a
     * value the database no longer has. Serving the added edges exactly while quietly serving this one from a
     * stale column would be the same silent wrong number the issue is about, reached from the other side, so the
     * view reports no edge properties until the rebuild it forces for the update lands.
     */
    @Test
    void anUpdatedEdgeWeightStopsTheColumnsFromBeingServedUntilTheRebuildLands() {
      starGraph();

      final GraphAnalyticalView view = syncView("overlay-updated-weight");
      try {
        final int a = view.getNodeId(vertex("A").getIdentity());

        database.transaction(() -> {
          for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
            if ("B".equals(edge.getInVertex().get("name")))
              edge.modify().set("w", 77.0).save();
        });

        // Either the rebuild has not landed yet - and then the columns are refused rather than served stale - or
        // it has, and then they carry the new value. Never the old one.
        if (view.hasEdgeProperty("ROAD", "w"))
          assertThat(weightsByName(view, a).get("B")).isEqualTo(77.0);
        else
          assertThat(view.edgeWeightsForSlice(a, Vertex.DIRECTION.OUT, "ROAD", "w", -1.0, null)).isNull();

        // The forced rebuild makes it visible for good.
        assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
        waitUntilServed(view);
        assertThat(weightsByName(view, view.getNodeId(vertex("A").getIdentity())).get("B")).isEqualTo(77.0);
      } finally {
        view.drop();
      }
    }

    /**
     * The neighbours {@code edgeWeightsForSlice} pairs its weights with have to be the very ones
     * {@code getNeighborIds} reports, or a caller that mixes the two - and everything reading a Graph Analytical
     * View has both in hand - sees two different graphs.
     */
    @Test
    void theNeighboursAreTheOnesGetNeighborIdsReports() {
      starGraph();

      final GraphAnalyticalView view = syncView("overlay-neighbour-agreement");
      try {
        final int a = view.getNodeId(vertex("A").getIdentity());
        deleteEdge("A", "C");
        database.transaction(() -> {
          final MutableVertex e = database.newVertex("N").set("name", "E").save();
          vertex("A").newEdge("ROAD", e, true, new Object[] { "w", 99.0 }).save();
        });

        final NodeEdgeWeights edges = view.edgeWeightsForSlice(a, Vertex.DIRECTION.OUT, "ROAD", "w", -1.0, null);
        assertThat(edges.neighbors()).isEqualTo(view.getNeighborIds(a, Vertex.DIRECTION.OUT, "ROAD"));
        assertThat(edges.weights()).hasSize(edges.neighbors().length);
      } finally {
        view.drop();
      }
    }

    /**
     * A direction has an adjacency slice; {@code BOTH} does not. Answering it here would mean answering for
     * neither, so the slice accessor refuses it and {@code edgeWeightsOf} splits it into the two that exist -
     * which is the whole of the multi-slice composition, exercised end to end with an overlay in play.
     */
    @Test
    void bothDirectionsAreSplitIntoTheSlicesThatExist() {
      starGraph();
      // A -> B, C, D plus D -> A, so A's BOTH neighbourhood spans the outgoing and the incoming slice alike.
      database.transaction(() -> vertex("D").newEdge("ROAD", vertex("A"), true, new Object[] { "w", 40.0 }).save());

      final GraphAnalyticalView view = syncView("overlay-both-directions");
      try {
        final int a = view.getNodeId(vertex("A").getIdentity());
        deleteEdge("A", "B");

        assertThat(view.edgeWeightsForSlice(a, Vertex.DIRECTION.BOTH, "ROAD", "w", -1.0, null))
            .as("BOTH has no slice of its own to align with")
            .isNull();

        final NodeEdgeWeights both = view.edgeWeightsOf(a, Vertex.DIRECTION.BOTH, "w", -1.0, "ROAD");
        assertThat(both.weights()).containsExactlyInAnyOrder(20.0, 30.0, 40.0);
      } finally {
        view.drop();
      }
    }

    /**
     * The collateral of letting the view serve edge properties through an overlay: {@code algo.dijkstra.singleSource}
     * routes onto a kernel that reads the CSR offset and neighbour arrays directly, and those arrays are the graph
     * as it stood at the last build. It used to be kept off them by the view reporting no edge properties whenever
     * an overlay was active - a gate it never asked for and could not see. The kernel refuses for itself now, so
     * the answer follows the edges that exist rather than the ones the arrays remember.
     */
    @Test
    void dijkstraDoesNotAnswerFromTheStaleBaseCsrWhileAnOverlayIsActive() {
      // A -> B costs 10 directly, or 2 the way round through C.
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("N").set("name", "A").save();
        final MutableVertex b = database.newVertex("N").set("name", "B").save();
        final MutableVertex c = database.newVertex("N").set("name", "C").save();
        a.newEdge("ROAD", b, true, new Object[] { "w", 10.0 }).save();
        a.newEdge("ROAD", c, true, new Object[] { "w", 1.0 }).save();
        c.newEdge("ROAD", b, true, new Object[] { "w", 1.0 }).save();
      });

      final GraphAnalyticalView view = syncView("overlay-dijkstra-kernel");
      try {
        assertThat(shortestPathCostToB()).isEqualTo(2.0);

        deleteEdge("C", "B");

        assertThat(shortestPathCostToB())
            .as("the shortcut through C is gone; only the base CSR arrays still have it")
            .isEqualTo(10.0);
      } finally {
        view.drop();
      }
    }

    /**
     * The other half of a node the provider cannot answer for: the algorithm above it has to notice and read that
     * node's edges itself, not take the {@code null} for an empty neighbourhood or fall over it. One ambiguous
     * node in an otherwise columnar graph is exactly the shape that reaches
     * {@code AbstractAlgoProcedure.weightedAdjacencyFromColumns}'s per-node fallback.
     */
    @Test
    void anAlgorithmReadsTheRecordsForANodeTheViewCannotAnswerFor() {
      // A reaches B directly over one of two parallel edges (1.0 and 100.0), or for 6.0 the way round through C.
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("N").set("name", "A").save();
        final MutableVertex b = database.newVertex("N").set("name", "B").save();
        final MutableVertex c = database.newVertex("N").set("name", "C").save();
        a.newEdge("ROAD", b, true, new Object[] { "w", 1.0 }).save();
        a.newEdge("ROAD", b, true, new Object[] { "w", 100.0 }).save();
        a.newEdge("ROAD", c, true, new Object[] { "w", 5.0 }).save();
        c.newEdge("ROAD", b, true, new Object[] { "w", 1.0 }).save();
      });

      final GraphAnalyticalView view = syncView("overlay-ambiguous-node-fallback");
      try {
        // The cheap direct edge goes, leaving the 100.0 one - and leaving A's run of parallel edges partially
        // deleted, which is the pair the per-pair deletion count cannot tell apart.
        database.transaction(() -> {
          for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
            if (Double.valueOf(1.0).equals(edge.get("w"))) {
              edge.delete();
              return;
            }
        });

        try (final ResultSet rows = database.command("opencypher",
            "MATCH (a:N {name: 'A'}), (b:N {name: 'B'}) CALL algo.bellmanford(a, b, 'ROAD', 'w') "
                + "YIELD weight RETURN weight")) {
          assertThat(rows.hasNext()).isTrue();
          assertThat(((Number) rows.next().getProperty("weight")).doubleValue())
              .as("A-C-B at 6.0: the surviving direct edge weighs 100.0, whichever slot it sits in")
              .isEqualTo(6.0);
        }
      } finally {
        view.drop();
      }
    }

    /**
     * An update to an edge the overlay itself holds is applied there, where the edge's values live, and leaves
     * the base columns alone - they never described that edge to begin with. This is also what keeps the plain
     * {@code newEdge(...).save()} from forcing a rebuild on every insert: an insert reports one create and one
     * update of the same edge.
     */
    @Test
    void updatingAnEdgeTheOverlayHoldsDoesNotPutTheColumnsOutOfDate() {
      starGraph();

      final GraphAnalyticalView view = syncView("overlay-updated-added-edge");
      try {
        final int a = view.getNodeId(vertex("A").getIdentity());

        database.transaction(() -> {
          final MutableVertex e = database.newVertex("N").set("name", "E").save();
          vertex("A").newEdge("ROAD", e, true, new Object[] { "w", 99.0 }).save();
        });
        // A separate transaction, so the update is resolved against the overlay left behind by the previous one
        // rather than against this delta's own additions.
        database.transaction(() -> {
          for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
            if ("E".equals(edge.getInVertex().get("name")))
              edge.modify().set("w", 55.0).save();
        });

        assertThat(view.hasEdgeProperty("ROAD", "w"))
            .as("nothing about the base columns changed, so they are still exact")
            .isTrue();
        assertThat(weightsByName(view, a).get("E"))
            .as("the overlay carries the new value, so no rebuild is needed to see it")
            .isEqualTo(55.0);
      } finally {
        view.drop();
      }
    }

    /**
     * Past a cap, one transaction's edge property changes stop being tracked individually and the delta says only
     * that the columns are out of date - a bulk rewrite would otherwise hold one entry per edge where the old
     * boolean flag held nothing. The view must come back on its own afterwards: the flag has to reach the same
     * rebuild an individually-tracked update reaches, including the follow-up one scheduled for a delta buffered
     * during a rebuild already in flight, or a bulk rewrite that is the last write to the graph leaves the view
     * serving no edge properties for good.
     */
    @Test
    void aBulkEdgePropertyRewriteStopsTrackingIndividuallyAndStillRepairsItself() {
      final int edgeCount = 1100; // over DeltaCollector's 1024 cap, so the individual tracking is given up on
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("N").set("name", "A").save();
        for (int i = 0; i < edgeCount; i++) {
          final MutableVertex target = database.newVertex("N").set("name", "T" + i).save();
          a.newEdge("ROAD", target, true, new Object[] { "w", 1.0 }).save();
        }
      });

      final GraphAnalyticalView view = syncView("overlay-bulk-edge-property-rewrite");
      try {
        final int a = view.getNodeId(vertex("A").getIdentity());
        assertThat(weightsByName(view, a)).hasSize(edgeCount);

        database.transaction(() -> {
          for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
            edge.modify().set("w", 2.0).save();
        });

        // Nothing is served from the columns while they hold the old weights, whether or not the rebuild that
        // repairs them has landed yet.
        if (!view.hasEdgeProperty("ROAD", "w"))
          assertThat(view.edgeWeightsForSlice(a, Vertex.DIRECTION.OUT, "ROAD", "w", -1.0, null)).isNull();

        // And it lands without any further commit to prompt it.
        waitUntilServed(view);
        assertThat(weightsByName(view, view.getNodeId(vertex("A").getIdentity())).values())
            .as("every edge weighs what the bulk rewrite set it to")
            .containsOnly(2.0);
      } finally {
        view.drop();
      }
    }

    /**
     * A view that materialises no edge property columns has nothing that can go out of date when an edge's
     * properties change - the base CSR holds the topology, which an update does not touch - so the full rebuild
     * #4513 forces for such an update has nothing to repair. It was forced anyway, on every edge update, because
     * the flag was raised before anyone asked whether there were columns at all. The overlay surviving is the
     * observable half of that: a rebuild would have compacted it away.
     */
    @Test
    void anEdgeUpdateOnAViewWithoutEdgeColumnsDoesNotForceARebuild() throws InterruptedException {
      starGraph();

      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName("no-edge-columns-update")
          .withVertexTypes("N")
          .withEdgeTypes("ROAD")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
          .build();
      try {
        assertThat(view.hasEdgeProperties()).as("no edge property columns were asked for").isFalse();

        // An added edge, so there is an overlay for the rebuild to compact away if one is forced.
        database.transaction(() -> {
          final MutableVertex e = database.newVertex("N").set("name", "E").save();
          vertex("A").newEdge("ROAD", e, true, new Object[] { "w", 99.0 }).save();
        });
        assertThat(view.getStats().get("overlayActive")).isEqualTo(true);

        database.transaction(() -> {
          for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
            if ("B".equals(edge.getInVertex().get("name")))
              edge.modify().set("w", 77.0).save();
        });

        // Long enough for a rebuild of a five-vertex graph to have finished several times over.
        final long deadline = System.currentTimeMillis() + 1_000;
        while (System.currentTimeMillis() < deadline) {
          assertThat(view.getStats().get("overlayActive"))
              .as("nothing was rebuilt, so the overlay is still the one the added edge went into")
              .isEqualTo(true);
          Thread.sleep(25);
        }
        assertThat(view.getNeighborIds(view.getNodeId(vertex("A").getIdentity()), Vertex.DIRECTION.OUT, "ROAD"))
            .as("and the topology still has all four outgoing edges")
            .hasSize(4);
      } finally {
        view.drop();
      }
    }

    /**
     * The cap on individually-tracked edge property changes counts the edges touched, not the calls made, so a
     * transaction that rewrites one edge's weight over and over - an accumulator - holds one entry and stays
     * resolvable against the overlay. Counting the calls would trip the cap and force a full rebuild for a
     * single edge the overlay was holding all along.
     */
    @Test
    void repeatedUpdatesOfOneEdgeCountOnceAgainstTheTrackingCap() {
      starGraph();

      final GraphAnalyticalView view = syncView("overlay-repeated-updates-of-one-edge");
      try {
        final int a = view.getNodeId(vertex("A").getIdentity());
        database.transaction(() -> {
          final MutableVertex e = database.newVertex("N").set("name", "E").save();
          vertex("A").newEdge("ROAD", e, true, new Object[] { "w", 1.0 }).save();
        });

        // Well past DeltaCollector's 1024 cap in calls, one edge in edges.
        database.transaction(() -> {
          for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
            if ("E".equals(edge.getInVertex().get("name"))) {
              for (int i = 1; i <= 2000; i++)
                edge.modify().set("w", (double) i).save();
              return;
            }
        });

        assertThat(view.hasEdgeProperty("ROAD", "w"))
            .as("one overlay-held edge was touched, so nothing about the base columns went out of date")
            .isTrue();
        assertThat(weightsByName(view, a).get("E")).isEqualTo(2000.0);
      } finally {
        view.drop();
      }
    }

    /**
     * The same contract at the boundary, which is where the check's order matters rather than its number: with
     * exactly the cap's worth of distinct edges already tracked, a repeat update of one of them does not grow the
     * set at all and so must not trip it either. Sized from the constant rather than from a literal, so retuning
     * the cap is a decision rather than a test failure.
     */
    @Test
    void aRepeatUpdateDoesNotTripTheCapEvenWithItAlreadyFull() {
      final int distinctEdges = DeltaCollector.MAX_TRACKED_EDGE_UPDATES;
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("N").set("name", "A").save();
        final MutableVertex b = database.newVertex("N").set("name", "B").save();
        // One edge at build time, so the view materialises a `w` column to begin with.
        a.newEdge("ROAD", b, true, new Object[] { "w", 1.0 }).save();
      });

      final GraphAnalyticalView view = syncView("overlay-repeat-update-at-the-cap");
      try {
        // Added in their own transaction, so the overlay holds them and their updates resolve against it.
        database.transaction(() -> {
          final Vertex a = vertex("A");
          for (int i = 0; i < distinctEdges; i++) {
            final MutableVertex target = database.newVertex("N").set("name", "T" + i).save();
            a.newEdge("ROAD", target, true, new Object[] { "w", 1.0 }).save();
          }
        });
        assertThat(view.hasEdgeProperty("ROAD", "w")).isTrue();

        // Only the overlay-held ones: the edge that was there at build time is a base edge, and updating it
        // would put the columns out of date for a reason that has nothing to do with the cap.
        database.transaction(() -> {
          Edge first = null;
          for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD")) {
            if ("B".equals(edge.getInVertex().get("name")))
              continue;
            edge.modify().set("w", 2.0).save();
            if (first == null)
              first = edge;
          }
          // The cap is now exactly full of distinct edges. This one is already in it.
          first.modify().set("w", 3.0).save();
        });

        assertThat(view.hasEdgeProperty("ROAD", "w"))
            .as("a repeat update grows nothing, so it cannot be what tips the set over its cap")
            .isTrue();
        assertThat(weightsByName(view, view.getNodeId(vertex("A").getIdentity())).values())
            .as("every edge took the update, one of them twice")
            .containsOnlyOnce(3.0);
      } finally {
        view.drop();
      }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /** The {@code algo.dijkstra.singleSource} cost from A to B over the {@code w} property. */
    private double shortestPathCostToB() {
      try (final ResultSet rows = database.command("opencypher",
          "MATCH (a:N {name: 'A'}) CALL algo.dijkstra.singleSource(a, 'ROAD', 'w', 'OUT') YIELD node, cost "
              + "WITH node, cost WHERE node.name = 'B' RETURN cost")) {
        assertThat(rows.hasNext()).isTrue();
        return ((Number) rows.next().getProperty("cost")).doubleValue();
      }
    }

    /** A -> B at 10.0, A -> C at 20.0, A -> D at 30.0. */
    private void starGraph() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("N").set("name", "A").save();
        final MutableVertex b = database.newVertex("N").set("name", "B").save();
        final MutableVertex c = database.newVertex("N").set("name", "C").save();
        final MutableVertex d = database.newVertex("N").set("name", "D").save();
        a.newEdge("ROAD", b, true, new Object[] { "w", 10.0 }).save();
        a.newEdge("ROAD", c, true, new Object[] { "w", 20.0 }).save();
        a.newEdge("ROAD", d, true, new Object[] { "w", 30.0 }).save();
      });
    }

    private GraphAnalyticalView syncView(final String name) {
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName(name)
          .withVertexTypes("N")
          .withEdgeTypes("ROAD")
          .withEdgeProperties("w")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
          .build();
      assertThat(view.hasEdgeProperty("ROAD", "w")).isTrue();
      return view;
    }

    /** The outgoing ROAD weights of a node, keyed by the neighbour's name. */
    private Map<String, Double> weightsByName(final GraphAnalyticalView view, final int nodeId) {
      final NodeEdgeWeights edges = view.edgeWeightsForSlice(nodeId, Vertex.DIRECTION.OUT, "ROAD", "w", -1.0, null);
      assertThat(edges).isNotNull();
      final Map<String, Double> byName = new HashMap<>();
      for (int i = 0; i < edges.neighbors().length; i++)
        byName.put((String) view.getRID(edges.neighbors()[i]).asVertex().get("name"), edges.weights()[i]);
      return byName;
    }

    private Vertex vertex(final String name) {
      return database.query("sql", "select from N where name = ?", name).next().getRecord().get().asVertex();
    }

    private void deleteEdge(final String from, final String to) {
      database.transaction(() -> {
        for (final Edge edge : vertex(from).getEdges(Vertex.DIRECTION.OUT, "ROAD"))
          if (to.equals(edge.getInVertex().get("name"))) {
            edge.delete();
            return;
          }
      });
    }

    /** Waits for the rebuild an edge-property update forces to have swapped its fresh columns in. */
    private void waitUntilServed(final GraphAnalyticalView view) {
      final long deadline = System.currentTimeMillis() + 30_000;
      while (!view.hasEdgeProperty("ROAD", "w") && System.currentTimeMillis() < deadline)
        Thread.yield();
      assertThat(view.hasEdgeProperty("ROAD", "w"))
          .as("the rebuild forced by an edge property update must restore the columns")
          .isTrue();
    }
  }

  /**
   * Regression for #6943.
   * <p>
   * Regression tests for issue #6943's second finding: {@code GraphAnalyticalView.countCommonNeighbors} was the one
   * pair accessor on this class that never consulted {@code snap.overlay} - both {@code isConnectedTo} and
   * {@code countEdgesBetween} already subtract overlay deletions and add overlay additions for the same kind of pair
   * question. It also omitted the {@code nodeA < snap.nodeMapping.size()} guard both siblings carry, so a node id the
   * overlay handed out for a newly added vertex (legal once {@link GraphAnalyticalView#getNodeIdUpperBound()} exceeds
   * the base node count) indexed the base CSR's offsets array straight past its end.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CountCommonNeighborsOverlay extends TestHelper {

    @Test
    void overlayDeletedCommonNeighbourIsExcluded() {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("KNOWS");

      database.begin();
      final MutableVertex a = database.newVertex("Person").set("name", "A").save();
      final MutableVertex b = database.newVertex("Person").set("name", "B").save();
      final MutableVertex x = database.newVertex("Person").set("name", "X").save();
      a.newEdge("KNOWS", x);
      b.newEdge("KNOWS", x);
      database.commit();

      final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
          .withName("common-neighbors-overlay-delete")
          .withVertexTypes("Person")
          .withEdgeTypes("KNOWS")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
          .build();

      final int idA = gav.getNodeId(a.getIdentity());
      final int idB = gav.getNodeId(b.getIdentity());

      assertThat(gav.countCommonNeighbors(idA, idB, Vertex.DIRECTION.OUT, "KNOWS"))
          .as("precondition: X is a common OUT-neighbour of A and B before the overlay touches anything").isEqualTo(1);

      // Delete A -> X within the overlay window (no compaction in between).
      database.begin();
      a.getIdentity().asVertex().getEdges(Vertex.DIRECTION.OUT, "KNOWS").forEach(e -> {
        if (e.getIn().equals(x.getIdentity()))
          e.delete();
      });
      database.commit();

      assertThat(gav.countCommonNeighbors(idA, idB, Vertex.DIRECTION.OUT, "KNOWS"))
          .as("an overlay-deleted common neighbour must no longer be counted (#6943)").isEqualTo(0);

      gav.drop();
    }

    @Test
    void overlayAddedCommonNeighbourIsIncluded() {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("KNOWS");

      database.begin();
      final MutableVertex a = database.newVertex("Person").set("name", "A").save();
      final MutableVertex b = database.newVertex("Person").set("name", "B").save();
      final MutableVertex x = database.newVertex("Person").set("name", "X").save();
      database.commit();

      final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
          .withName("common-neighbors-overlay-add")
          .withVertexTypes("Person")
          .withEdgeTypes("KNOWS")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
          .build();

      final int idA = gav.getNodeId(a.getIdentity());
      final int idB = gav.getNodeId(b.getIdentity());

      assertThat(gav.countCommonNeighbors(idA, idB, Vertex.DIRECTION.OUT, "KNOWS"))
          .as("precondition: no common neighbour before the overlay adds anything").isEqualTo(0);

      // Add A -> X and B -> X within the overlay window (no compaction in between).
      database.begin();
      a.getIdentity().asVertex().modify().newEdge("KNOWS", x);
      b.getIdentity().asVertex().modify().newEdge("KNOWS", x);
      database.commit();

      assertThat(gav.countCommonNeighbors(idA, idB, Vertex.DIRECTION.OUT, "KNOWS"))
          .as("an overlay-added common neighbour must be counted (#6943)").isEqualTo(1);

      gav.drop();
    }

    @Test
    void overlayAddedNodeIdDoesNotThrowArrayIndexOutOfBounds() {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("KNOWS");

      database.begin();
      final MutableVertex b = database.newVertex("Person").set("name", "B").save();
      final MutableVertex x = database.newVertex("Person").set("name", "X").save();
      b.newEdge("KNOWS", x);
      database.commit();

      final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
          .withName("common-neighbors-overlay-new-node")
          .withVertexTypes("Person")
          .withEdgeTypes("KNOWS")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
          .build();

      final int idB = gav.getNodeId(b.getIdentity());
      assertThat(gav.getNodeCount()).as("precondition").isEqualTo(2);

      // Add a brand-new vertex C within the overlay window: its node id is >= the base node count, the
      // "legal once getNodeIdUpperBound() exceeds getNodeCount()" shape #6943 describes.
      database.begin();
      final MutableVertex c = database.newVertex("Person").set("name", "C").save();
      c.newEdge("KNOWS", x);
      database.commit();

      final int idC = gav.getNodeId(c.getIdentity());
      // No deletions precede this add, so C's id is deterministically the next slot after the 2 base nodes (B, X).
      assertThat(idC).as("precondition: C's node id must be the overlay-added id past the base node count")
          .isEqualTo(2);

      // Before the fix, nodeA=idC indexed the base CSR's offsets array without a bounds check and threw AIOOBE.
      assertThat(gav.countCommonNeighbors(idC, idB, Vertex.DIRECTION.OUT, "KNOWS"))
          .as("X is a common OUT-neighbour of the overlay-added C and the base node B (#6943)").isEqualTo(1);
      assertThat(gav.countCommonNeighbors(idB, idC, Vertex.DIRECTION.OUT, "KNOWS"))
          .as("the same question with the overlay-added id as nodeB").isEqualTo(1);

      gav.drop();
    }
  }

  /**
   * Regression for #9437.
   * <p>
   * A committed change to the weight of an edge already in a view's base CSR (issue #9437). When the edge is the only one
   * of its type between its two vertices, its pair names its column slot, so the new value is served from the overlay
   * at once and the view does not rebuild its columns - which is what lets a contraction hierarchy follow a traffic update
   * in milliseconds. A pair joined by parallel edges is still ambiguous and still rebuilds, as before.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BaseEdgeWeightOverlay {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-9437-base-edge-weight-overlay");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("N");
      database.getSchema().createEdgeType("ROAD").createProperty("w", Type.DOUBLE);
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
        database = null;
      }
    }

    @Test
    void aSoleEdgeWeightIsServedFromTheOverlayWithoutARebuild() {
      graph(false);
      final GraphAnalyticalView view = syncView();
      try {
        final long built = view.getBuildTimestamp();
        setWeight("A", "B", 77.0);
        setWeight("A", "B", 78.0); // a second update of the same edge replaces the first

        assertThat(view.hasEdgeProperty("ROAD", "w")).as("the columns are not out of date").isTrue();
        assertThat(view.hasPendingChanges()).isTrue();
        final int a = view.getNodeId(vertex("A").getIdentity());
        final int b = view.getNodeId(vertex("B").getIdentity());
        assertThat(weightsByName(view, a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrderEntriesOf(
            Map.of("B", 78.0, "C", 20.0));
        assertThat(weightsByName(view, b, Vertex.DIRECTION.IN)).containsExactlyInAnyOrderEntriesOf(Map.of("A", 78.0));

        // the weighted algorithms reading the view see it too
        assertThat(costTo("B")).isEqualTo(78.0);
        assertThat(view.getBuildTimestamp()).as("no rebuild was forced").isEqualTo(built);

        // a later deletion of the same edge still removes it, value and all
        deleteEdge("A", "B");
        assertThat(weightsByName(view, a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrderEntriesOf(Map.of("C", 20.0));
      } finally {
        view.drop();
      }
    }

    @Test
    void parallelEdgesStillRebuildTheColumns() {
      graph(true);
      final GraphAnalyticalView view = syncView();
      try {
        final double replaced = setWeight("A", "B", 77.0);
        // the pair A -> B has two edges: which slot changed cannot be told, so the columns are rebuilt and then serve it
        assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
        final long deadline = System.currentTimeMillis() + 30_000;
        while (!view.hasEdgeProperty("ROAD", "w") && System.currentTimeMillis() < deadline)
          Thread.yield();
        final int a = view.getNodeId(vertex("A").getIdentity());
        final NodeEdgeWeights edges = view.edgeWeightsForSlice(a, Vertex.DIRECTION.OUT, "ROAD", "w", -1.0, null);
        assertThat(edges).isNotNull();
        assertThat(edges.weights()).contains(77.0);
        // the edge that kept its weight is the other one of the two
        assertThat(costTo("B")).isEqualTo(Math.min(77.0, 10.0 + 15.0 - replaced));
      } finally {
        view.drop();
      }
    }

    /** An edge added after the build is a parallel twin of the overlay's, not of the base: each keeps its own value. */
    @Test
    void aSoleBaseEdgeUpdatedAfterAParallelEdgeWasAdded() {
      graph(false);
      final GraphAnalyticalView view = syncView();
      try {
        final long built = view.getBuildTimestamp();
        database.transaction(() -> vertex("A").newEdge("ROAD", vertex("B"), true, new Object[] { "w", 15.0 }).save());
        setWeightOfBaseEdge(3.0);

        final int a = view.getNodeId(vertex("A").getIdentity());
        final NodeEdgeWeights edges = view.edgeWeightsForSlice(a, Vertex.DIRECTION.OUT, "ROAD", "w", -1.0, null);
        assertThat(edges).isNotNull();
        assertThat(edges.weights()).containsExactlyInAnyOrder(3.0, 15.0, 20.0);
        assertThat(costTo("B")).isEqualTo(3.0);
        assertThat(view.getBuildTimestamp()).as("no rebuild was forced").isEqualTo(built);
      } finally {
        view.drop();
      }
    }

    /**
     * An update carries every materialised value of the edge as committed, so a change to another property keeps the
     * weight, and a removed weight reads as the default, exactly as a rebuilt column would.
     */
    @Test
    void otherPropertiesAndARemovedWeight() {
      graph(false);
      final GraphAnalyticalView view = syncView();
      try {
        final long built = view.getBuildTimestamp();
        final int a = view.getNodeId(vertex("A").getIdentity());
        updateEdge("A", "B", edge -> edge.set("note", "resurfaced"));
        assertThat(weightsByName(view, a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrderEntriesOf(
            Map.of("B", 10.0, "C", 20.0));

        updateEdge("A", "B", edge -> edge.remove("w"));
        assertThat(weightsByName(view, a, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrderEntriesOf(
            Map.of("B", -1.0, "C", 20.0));
        assertThat(view.getBuildTimestamp()).as("no rebuild was forced").isEqualTo(built);
      } finally {
        view.drop();
      }
    }

    // ---------------------------------------------------------------------------------------------------------------

    /** Sets the weight of the A -> B edge that was there when the view was built (the one at 10). */
    private void setWeightOfBaseEdge(final double weight) {
      database.transaction(() -> {
        for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
          if ("B".equals(edge.getInVertex().get("name")) && ((Number) edge.get("w")).doubleValue() == 10.0) {
            edge.modify().set("w", weight).save();
            return;
          }
      });
    }

    private void updateEdge(final String from, final String to, final Consumer<MutableEdge> change) {
      database.transaction(() -> {
        for (final Edge edge : vertex(from).getEdges(Vertex.DIRECTION.OUT, "ROAD"))
          if (to.equals(edge.getInVertex().get("name"))) {
            final MutableEdge mutable = edge.modify();
            change.accept(mutable);
            mutable.save();
            return;
          }
      });
    }

    /** A -> B at 10, A -> C at 20, plus a second A -> B at 15 when {@code parallel}. */
    private void graph(final boolean parallel) {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("N").set("name", "A").save();
        final MutableVertex b = database.newVertex("N").set("name", "B").save();
        final MutableVertex c = database.newVertex("N").set("name", "C").save();
        a.newEdge("ROAD", b, true, new Object[] { "w", 10.0 }).save();
        a.newEdge("ROAD", c, true, new Object[] { "w", 20.0 }).save();
        if (parallel)
          a.newEdge("ROAD", b, true, new Object[] { "w", 15.0 }).save();
      });
    }

    private GraphAnalyticalView syncView() {
      return GraphAnalyticalView.builder(database).withName("roads").withVertexTypes("N").withEdgeTypes("ROAD")
          .withEdgeProperties("w").withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).build();
    }

    /** Sets the weight of the first edge found from {@code from} to {@code to}; returns the weight it replaced. */
    private double setWeight(final String from, final String to, final double weight) {
      final double[] replaced = new double[1];
      database.transaction(() -> {
        for (final Edge edge : vertex(from).getEdges(Vertex.DIRECTION.OUT, "ROAD"))
          if (to.equals(edge.getInVertex().get("name"))) {
            replaced[0] = ((Number) edge.get("w")).doubleValue();
            edge.modify().set("w", weight).save();
            return;
          }
      });
      return replaced[0];
    }

    private void deleteEdge(final String from, final String to) {
      database.transaction(() -> {
        for (final Edge edge : vertex(from).getEdges(Vertex.DIRECTION.OUT, "ROAD"))
          if (to.equals(edge.getInVertex().get("name"))) {
            edge.delete();
            return;
          }
      });
    }

    private double costTo(final String name) {
      try (final var rows = database.command("opencypher",
          "MATCH (a:N {name: 'A'}) CALL algo.dijkstra.singleSource(a, 'ROAD', 'w', 'OUT') YIELD node, cost "
              + "WITH node, cost WHERE node.name = $name RETURN cost", Map.of("name", name))) {
        return ((Number) rows.next().getProperty("cost")).doubleValue();
      }
    }

    private Map<String, Double> weightsByName(final GraphAnalyticalView view, final int nodeId, final Vertex.DIRECTION direction) {
      final NodeEdgeWeights edges = view.edgeWeightsForSlice(nodeId, direction, "ROAD", "w", -1.0, null);
      assertThat(edges).isNotNull();
      final Map<String, Double> byName = new HashMap<>();
      for (int i = 0; i < edges.neighbors().length; i++)
        byName.put((String) view.getRID(edges.neighbors()[i]).asVertex().get("name"), edges.weights()[i]);
      return byName;
    }

    private Vertex vertex(final String name) {
      return database.query("sql", "select from N where name = ?", name).next().getRecord().get().asVertex();
    }
  }

  /**
   * Regression for #9572, the bulk side: {@link GraphBatch} writes its edges straight into the edge lists - a lightweight edge
   * has no record, and a regular one is created in bulk - so no record event reports them either.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphBatchReachesView extends TestHelper {
    private static final String COUNT = "MATCH (a:P)-[:K]->(b:P) RETURN count(*) AS n";

    @Test
    void lightweightEdgesOfABatchReachASynchronousView() throws Exception {
      assertBatchReachesView("K LIGHTWEIGHT", "SYNCHRONOUS");
    }

    @Test
    void regularEdgesOfABatchReachASynchronousView() throws Exception {
      assertBatchReachesView("K", "SYNCHRONOUS");
    }

    @Test
    void lightweightEdgesOfABatchReachAnOffView() throws Exception {
      assertBatchReachesView("K LIGHTWEIGHT", "OFF");
    }

    @Test
    void regularEdgesOfABatchReachAnAsynchronousView() throws Exception {
      assertBatchReachesView("K", "ASYNCHRONOUS");
    }

    /**
     * A batch writing only a type the view does not cover leaves it alone; one writing that type and a covered one, in
     * either storage shape, rebuilds it.
     */
    @Test
    void onlyABatchWritingACoveredTypeRebuildsTheView() throws Exception {
      final RID[] v = createGraph("K", "SYNCHRONOUS");
      database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v1");
      final long builtAt = view.getBuildTimestamp();

      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        batch.newEdge(v[1], "L", v[0]);
      }
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
      assertThat(view.getBuildTimestamp()).as("no rebuild for an uncovered type").isEqualTo(builtAt);

      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        batch.newEdge(v[2], "L", v[1]);
        batch.newEdge(v[1], "K", v[0]);
      }
      assertThat(count()).isEqualTo(4L);
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      assertThat(view.getEdgeCount()).isEqualTo(4);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW v1");
    }

    /** A listener that fails, told before the view, neither fails the batch nor keeps the view from rebuilding. */
    @Test
    void aFailingListenerDoesNotKeepTheViewFromTheBatch() throws Exception {
      final GraphEngine graphEngine = ((DatabaseInternal) database).getGraphEngine();
      final GraphEngine.EdgeWriteListener failing = new GraphEngine.EdgeWriteListener() {
        @Override
        public void onLightEdgeCreated(final Edge edge) {
        }

        @Override
        public void onEdgesWrittenInBulk(final Set<String> edgeTypeNames) {
          throw new IllegalStateException("listener failure the batch must survive");
        }
      };
      graphEngine.registerEdgeWriteListener(failing);
      try {
        final RID[] v = createGraph("K LIGHTWEIGHT", "SYNCHRONOUS");
        final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v1");
        try (final GraphBatch batch = GraphBatch.builder(database).build()) {
          batch.newEdge(v[1], "K", v[0]);
        }
        assertThat(count()).isEqualTo(4L);
        assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
        assertThat(view.getEdgeCount()).isEqualTo(4);
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW v1");
      } finally {
        graphEngine.unregisterEdgeWriteListener(failing);
      }
    }

    /** A second batch on a view that a first batch sent rebuilding, before that rebuild is awaited. */
    @Test
    void twoBatchesInARowLeaveASynchronousViewComplete() throws Exception {
      final RID[] v = createGraph("K LIGHTWEIGHT", "SYNCHRONOUS");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v1");
      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        batch.newEdge(v[1], "K", v[0]);
      }
      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        batch.newEdge(v[2], "K", v[1]);
      }
      assertThat(count()).isEqualTo(5L);
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      assertThat(view.getEdgeCount()).isEqualTo(5);
      assertThat(count()).isEqualTo(5L);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW v1");
    }

    private void assertBatchReachesView(final String edgeType, final String updateMode) throws Exception {
      final RID[] v = createGraph(edgeType, updateMode);
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v1");

      // Every vertex already has an edge list with room in both directions, so the batch only appends to them: it
      // rewrites no vertex record, and no record event of any kind fires
      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        batch.newEdge(v[1], "K", v[0]);
        batch.newEdge(v[2], "K", v[1]);
      }

      // Right after the batch: no view may answer without its edges
      assertThat(count()).isEqualTo(5L);
      if ("OFF".equals(updateMode))
        assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);
      else {
        assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
        assertThat(view.getEdgeCount()).isEqualTo(5);
        assertThat(count()).isEqualTo(5L);
      }

      database.command("sql", "DROP GRAPH ANALYTICAL VIEW v1");
      assertThat(count()).isEqualTo(5L);
    }

    /** A triangle a -> b -> c -> a: every vertex has an outgoing and an incoming edge list. */
    private RID[] createGraph(final String edgeType, final String updateMode) throws InterruptedException {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE EDGE TYPE " + edgeType);
      final RID[] v = new RID[3];
      database.transaction(() -> {
        for (int i = 0; i < v.length; i++)
          v[i] = database.newVertex("P").save().getIdentity();
        for (int i = 0; i < v.length; i++)
          v[i].asVertex().modify().newEdge("K", v[(i + 1) % v.length]);
      });
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW v1 VERTEX TYPES (P) EDGE TYPES (K) UPDATE MODE " + updateMode);
      assertThat(GraphAnalyticalViewRegistry.get(database, "v1").awaitReady(60, TimeUnit.SECONDS)).isTrue();
      return v;
    }

    private long count() {
      try (final ResultSet rs = database.query("opencypher", COUNT)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }

  /**
   * Regression for #9572: the reconciliation a CSR build makes with the commits its scan raced ({@link BuildWatch}, issue #8378)
   * for lightweight edges. The scan reads every lightweight edge of a type as the same bucket marker, so the watch cannot
   * tell them apart by identity and counts them per far end instead. Each case drives one ordering of the changes
   * against the scan deterministically and checks the published base plus the overlay against the live graph.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LightEdgeBuildWatch extends TestHelper {
    private RID a;
    private RID b;

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE EDGE TYPE K LIGHTWEIGHT");
      database.transaction(() -> {
        a = database.newVertex("P").save().getIdentity();
        b = database.newVertex("P").save().getIdentity();
      });
    }

    @Test
    void anAdditionTheScanReadIsNotAddedAgain() {
      final BuildWatch watch = openWatch();
      final TxDelta added = newEdge();
      final Scan scan = scan(watch);

      assertThat(scan.fresh()).isEqualTo(1);
      assertReconciled(scan, watch, List.of(added), 1);
    }

    @Test
    void anAdditionAfterTheScanIsAdded() {
      final BuildWatch watch = openWatch();
      final Scan scan = scan(watch);
      final TxDelta added = newEdge();

      assertThat(scan.fresh()).isZero();
      assertReconciled(scan, watch, List.of(added), 1);
    }

    @Test
    void aDeletionTheScanAlreadyMissedSpendsNoBudget() {
      newEdge();
      final BuildWatch watch = openWatch();
      final TxDelta deleted = deleteEdge();
      final Scan scan = scan(watch);
      final TxDelta added = newEdge();

      assertThat(scan.fresh()).isZero();
      assertReconciled(scan, watch, List.of(deleted, added), 1);
    }

    @Test
    void aDeletionAfterTheScanIsBudgetedAgainstTheBase() {
      newEdge();
      final BuildWatch watch = openWatch();
      final Scan scan = scan(watch);
      final TxDelta deleted = deleteEdge();
      final TxDelta added = newEdge();

      assertThat(scan.fresh()).isEqualTo(1);
      assertReconciled(scan, watch, List.of(deleted, added), 1);
    }

    @Test
    void aReadAdditionThenADeletionAndAnAdditionAfterTheScan() {
      final BuildWatch watch = openWatch();
      final TxDelta first = newEdge();
      final Scan scan = scan(watch);
      final TxDelta deleted = deleteEdge();
      final TxDelta second = newEdge();

      assertThat(scan.fresh()).isEqualTo(1);
      assertReconciled(scan, watch, List.of(first, deleted, second), 1);
    }

    /** A deletion delivered before the addition it follows, which a commit callback arriving late can do. */
    @Test
    void anAdditionAfterTheScanDeletedAgainLeavesNothing() {
      final BuildWatch watch = openWatch();
      final Scan scan = scan(watch);
      final TxDelta added = newEdge();
      final TxDelta deleted = deleteEdge();

      assertThat(scan.fresh()).isZero();
      assertReconciled(scan, watch, List.of(added, deleted), 0);
    }

    /**
     * Duplicated copies the counts can still tell apart: every copy is one of the buffered changes, so the copy the scan
     * read is the first addition and the duplicate added after the scan is added.
     */
    @Test
    void aDuplicateCopyAddedAfterTheScanIsAddedWhenEveryCopyIsBuffered() {
      final BuildWatch watch = openWatch();
      final TxDelta first = newEdge();
      final Scan scan = scan(watch);
      final TxDelta twin = newEdge();

      assertThat(scan.fresh()).isEqualTo(1);
      assertReconciled(scan, watch, List.of(first, twin), 2);
    }

    /**
     * The documented limit: a copy that existed before the build and a duplicate added after the scan read its source
     * look, to the counts, exactly like no earlier copy and a duplicate the scan read. Any copy older than the watch is
     * counted as if it were one of the buffered additions. Nothing recorded tells the two
     * apart, so the answer errs towards "read" and the view holds one copy too few until its next rebuild or compaction.
     * Only duplicated copies of one pair, which the {@code UNIQUE} flag rules out, reach this.
     */
    @Test
    void aDuplicateCopyAddedAfterTheScanIsTakenAsRead() {
      newEdge();
      final BuildWatch watch = openWatch();
      final Scan scan = scan(watch);
      final TxDelta twin = newEdge();

      assertThat(scan.fresh()).isEqualTo(1);
      watch.close();
      watch.bindTo(scan.result().getCsrPerType());
      watch.account(twin);
      final DeltaOverlay overlay = new DeltaOverlay(scan.result().getMapping().size())
          .merge(twin, scan.result().getMapping(), scan.result().getCsrPerType(), watch);
      assertThat(liveEdges()).isEqualTo(2);
      assertThat(scan.fresh() + overlay.getDeltaEdgeCount()).isEqualTo(1);
      // Which is why the pair is handed to the view, to be checked against the graph before it is served
      assertThat(watch.takeRacedLightPairs())
          .containsExactly(new BuildWatch.RacedLightPair(a, database.getSchema().getType("K").getFirstBucketId(), b));
      assertThat(watch.takeRacedLightPairs()).as("handed over once").isEmpty();
    }

    /**
     * A change reported after the watch closed cannot have been read by the scan, so it is answered exactly, without
     * counting: here a duplicate of a copy older than the build, the case counting cannot settle, is added.
     */
    @Test
    void aDuplicateCopyReportedAfterTheWatchClosedIsAdded() {
      newEdge();
      final BuildWatch watch = openWatch();
      final Scan scan = scan(watch);
      watch.close();
      watch.bindTo(scan.result().getCsrPerType());
      final TxDelta twin = newEdge();

      DeltaOverlay overlay = new DeltaOverlay(scan.result().getMapping().size());
      watch.account(twin);
      overlay = overlay.merge(twin, scan.result().getMapping(), scan.result().getCsrPerType(), watch);
      assertThat(liveEdges()).isEqualTo(2);
      assertThat(scan.fresh() + overlay.getDeltaEdgeCount()).isEqualTo(2);

      // And a deletion reported after it removes a copy, never one the counts take as already missing
      final TxDelta deleted = deleteEdge();
      final TxDelta deletedAgain = deleteEdge();
      for (final TxDelta delta : List.of(deleted, deletedAgain)) {
        watch.account(delta);
        overlay = overlay.merge(delta, scan.result().getMapping(), scan.result().getCsrPerType(), watch);
      }
      assertThat(liveEdges()).isZero();
      assertThat(scan.fresh() + overlay.getDeltaEdgeCount()).isZero();
      assertThat(watch.takeRacedLightPairs()).as("nothing for the view to check").isEmpty();
    }

    private BuildWatch openWatch() {
      final BuildWatch watch = new BuildWatch(1_000);
      // What the record listener does before every change of a's out-edges commits
      watch.watchSource(a);
      return watch;
    }

    private record Scan(CSRBuilder.CSRResult result, int fresh) {
    }

    private Scan scan(final BuildWatch watch) {
      final CSRBuilder builder = new CSRBuilder(database);
      builder.setScanObserver(watch);
      final CSRBuilder.CSRResult result = builder.build(new String[] { "P" }, new String[] { "K" });
      final CSRAdjacencyIndex csr = result.getCsrPerType().get("K");
      final int fresh = csr == null ? 0 :
          csr.forwardEdgeCount(result.getMapping().getGlobalId(a), result.getMapping().getGlobalId(b));
      return new Scan(result, fresh);
    }

    private void assertReconciled(final Scan scan, final BuildWatch watch, final List<TxDelta> buffered, final long expected) {
      watch.close();
      watch.bindTo(scan.result().getCsrPerType());
      DeltaOverlay overlay = new DeltaOverlay(scan.result().getMapping().size());
      for (final TxDelta delta : buffered) {
        watch.account(delta);
        overlay = overlay.merge(delta, scan.result().getMapping(), scan.result().getCsrPerType(), watch);
      }
      assertThat(liveEdges()).isEqualTo(expected);
      assertThat(scan.fresh() + overlay.getDeltaEdgeCount()).as("the published base plus its overlay").isEqualTo(expected);
    }

    private TxDelta newEdge() {
      database.transaction(() -> a.asVertex().modify().newEdge("K", b));
      return change(true);
    }

    private TxDelta deleteEdge() {
      database.transaction(() -> {
        final List<Edge> edges = new ArrayList<>();
        a.asVertex().getEdges(Vertex.DIRECTION.OUT, "K").forEach(edges::add);
        edges.getFirst().delete();
      });
      return change(false);
    }

    /** The delta the collector reports for one lightweight edge change of a -> b. */
    private TxDelta change(final boolean addition) {
      final TxDelta delta = new TxDelta();
      final TxDelta.EdgeDelta edge = new TxDelta.EdgeDelta("K", a, b,
          TxDelta.lightEdgeKey(database.getSchema().getType("K").getFirstBucketId()));
      (addition ? delta.addedEdges : delta.deletedEdges).add(edge);
      return delta;
    }

    private long liveEdges() {
      return a.asVertex().countEdges(Vertex.DIRECTION.OUT, "K");
    }
  }

  /**
   * Regression for #9572: a lightweight edge has no record, so the record listeners a Graph Analytical View learns about changes
   * through never heard of its creation. A {@code SYNCHRONOUS} view answered as if the edge did not exist, and a
   * transaction that only created lightweight edges left an {@code OFF} view READY and an {@code ASYNCHRONOUS} one
   * without the rebuild it needed. Every assertion is checked against the same query once the view is dropped.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LightEdgeReachesView extends TestHelper {
    private static final String COUNT     = "MATCH (a:P)-[:K]->(b:P) RETURN count(*) AS n";
    private static final String ROW_COUNT = "MATCH (a:P)-[:K]->(b:P) WITH * RETURN count(*) AS n";

    @Test
    void synchronousViewSeesLightEdgesCreatedAfterTheBuild() throws Exception {
      createSchema("K");
      createView("SYNCHRONOUS");

      database.transaction(() -> {
        final MutableVertex c = database.newVertex("P").set("name", "c").save();
        vertex("a").newLightEdge("K", c);
        vertex("a").newEdge("K", c);
      });
      assertCounts(3);

      // A self loop, alone in its transaction
      database.transaction(() -> vertex("a").newLightEdge("K", vertex("a")));
      assertCounts(4);
      assertThat(view().getEdgeCount()).isEqualTo(4);
      assertThat(view().getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);

      dropViewAndAssertCounts(4);
    }

    @Test
    void synchronousViewSeesEdgesOfALightweightTypeFromEveryCreationPath() throws Exception {
      createSchema("K LIGHTWEIGHT");
      createView("SYNCHRONOUS");

      // Java API, SQL and Cypher all store the type's edges as lightweight ones
      database.transaction(() -> vertex("b").newEdge("K", vertex("a")));
      assertCounts(2);
      database.transaction(
          () -> database.command("sql", "CREATE EDGE K FROM (SELECT FROM P WHERE name = 'b') TO (SELECT FROM P WHERE name = 'b')"));
      assertCounts(3);
      database.transaction(() -> database.command("opencypher", "MATCH (a:P {name: 'a'}) CREATE (a)-[:K]->(:P {name: 'd'})"));
      assertCounts(4);

      dropViewAndAssertCounts(4);
    }

    @Test
    void synchronousViewDropsLightEdgesDeletedAfterTheBuild() throws Exception {
      createSchema("K LIGHTWEIGHT");
      createView("SYNCHRONOUS");

      database.transaction(() -> vertex("b").newEdge("K", vertex("a")));
      assertCounts(2);

      // One the overlay added, one from the base CSR
      database.transaction(() -> outEdges("b").getFirst().delete());
      assertCounts(1);
      database.transaction(() -> outEdges("a").getFirst().delete());
      assertCounts(0);
      assertThat(view().getEdgeCount()).isZero();

      dropViewAndAssertCounts(0);
    }

    /**
     * Two lightweight edges of one type over the same pair share their identity, which is the triple. Each is still an
     * edge of its own to a query (issue #9573), so the view has to count both, and a delete removes exactly one of them.
     */
    @Test
    void synchronousViewCountsEveryCopyOfADuplicatedLightEdge() throws Exception {
      createSchema("K");
      createView("SYNCHRONOUS");

      database.transaction(() -> {
        vertex("b").newLightEdge("K", vertex("a"));
        vertex("b").newLightEdge("K", vertex("a"));
      });
      assertCounts(3);
      database.transaction(() -> vertex("b").newLightEdge("K", vertex("a")));
      assertCounts(4);

      database.transaction(() -> outEdges("b").getFirst().delete());
      assertCounts(3);
      database.transaction(() -> {
        outEdges("b").getFirst().delete();
        outEdges("b").getFirst().delete();
      });
      assertCounts(1);

      // A copy added and deleted in one transaction is no change at all
      database.transaction(() -> {
        vertex("b").newLightEdge("K", vertex("a"));
        vertex("b").newLightEdge("K", vertex("a"));
        outEdges("b").getFirst().delete();
      });
      assertCounts(2);

      dropViewAndAssertCounts(2);
    }

    /** A twin of a base edge: deleting one copy, whichever the delete finds first, leaves exactly one. */
    @Test
    void synchronousViewKeepsTheSurvivingCopyOfABaseLightEdge() throws Exception {
      createSchema("K");
      database.transaction(() -> vertex("a").newLightEdge("K", vertex("b")));
      createView("SYNCHRONOUS");
      assertCounts(2);

      database.transaction(() -> vertex("a").newLightEdge("K", vertex("b")));
      assertCounts(3);
      database.transaction(() -> outEdges("a").getFirst().delete());
      assertCounts(2);
      database.transaction(() -> outEdges("a").getFirst().delete());
      assertCounts(1);

      dropViewAndAssertCounts(1);
    }

    /**
     * The light edge goes between vertices whose edge lists in both directions already exist: the append then rewrites
     * no vertex record, so no record event of any kind reaches the view and only the light-edge hook can tell it.
     */
    @Test
    void offViewGoesStaleOnATransactionThatOnlyCreatesLightEdges() throws Exception {
      createSchema("K LIGHTWEIGHT");
      database.transaction(() -> vertex("b").newEdge("K", vertex("a")));
      createView("OFF");

      database.transaction(() -> vertex("a").newEdge("K", vertex("a")));
      assertThat(view().getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);

      database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW v1");
      assertThat(view().awaitReady(60, TimeUnit.SECONDS)).isTrue();
      assertCounts(3);

      database.transaction(() -> outEdges("b").getFirst().delete());
      assertThat(view().getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);

      dropViewAndAssertCounts(2);
    }

    @Test
    void asynchronousViewRebuildsOnATransactionThatOnlyCreatesLightEdges() throws Exception {
      createSchema("K LIGHTWEIGHT");
      database.transaction(() -> vertex("b").newEdge("K", vertex("a")));
      createView("ASYNCHRONOUS");

      database.transaction(() -> vertex("a").newEdge("K", vertex("a")));
      awaitEdgeCount(3);
      assertCounts(3);

      database.transaction(() -> outEdges("b").getFirst().delete());
      awaitEdgeCount(2);
      assertCounts(2);

      dropViewAndAssertCounts(2);
    }

    /**
     * The case the reconciliation cannot settle by counting, driven through a real rebuild: a duplicate of a copy older
     * than the build, created by a transaction that reports it before the scan reads its source and commits after the
     * rebuild published. The counts take the new copy as one the scan read; the view checks the pair against the graph,
     * finds one copy missing and rebuilds, so it never stays READY one copy short.
     */
    @Test
    void aDuplicateRacingARebuildIsNotLeftOutOfTheView() throws Exception {
      createSchema("K");
      database.transaction(() -> vertex("a").newLightEdge("K", vertex("b")));
      createView("SYNCHRONOUS");
      assertCounts(2);

      final CountDownLatch reported = new CountDownLatch(1);
      final CountDownLatch commit = new CountDownLatch(1);
      final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          database.transaction(() -> {
            vertex("a").newLightEdge("K", vertex("b"));
            reported.countDown();
            try {
              commit.await();
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
        } catch (final Throwable t) {
          writerFailure.set(t);
          reported.countDown();
        }
      }, "issue9572-duplicate-writer");

      final GraphAnalyticalView view = view();
      view.setBeforeBuildScanForTest(() -> {
        writer.start();
        try {
          assertThat(reported.await(60, TimeUnit.SECONDS)).isTrue();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      try {
        view.build();
      } finally {
        view.setBeforeBuildScanForTest(null);
        commit.countDown();
      }
      writer.join(60_000);
      assertThat(writerFailure.get()).isNull();

      final long deadline = System.currentTimeMillis() + 60_000;
      while (!(view.getStatus() == GraphAnalyticalView.Status.READY && view.getEdgeCount() == 3)
          && System.currentTimeMillis() < deadline)
        Thread.sleep(20);
      assertThat(view.getEdgeCount()).as("the view must end up with every copy").isEqualTo(3);
      assertCounts(3);
      dropViewAndAssertCounts(3);
    }

    /** A dropped view stops listening for light edges, as it stops listening for records. */
    @Test
    void droppingTheViewUnregistersItsLightEdgeListener() throws Exception {
      createSchema("K LIGHTWEIGHT");
      final int before = edgeWriteListeners();
      createView("SYNCHRONOUS");
      assertThat(edgeWriteListeners()).isEqualTo(before + 1);

      database.command("sql", "ALTER GRAPH ANALYTICAL VIEW v1 UPDATE MODE OFF");
      assertThat(edgeWriteListeners()).as("a mode change replaces the listener").isEqualTo(before + 1);

      dropViewAndAssertCounts(1);
      assertThat(edgeWriteListeners()).isEqualTo(before);
    }

    private int edgeWriteListeners() {
      return ((DatabaseInternal) database).getGraphEngine().getEdgeWriteListenerCount();
    }

    /** A rolled back light edge never reaches the view. */
    @Test
    void rolledBackLightEdgeLeavesTheViewUntouched() throws Exception {
      createSchema("K LIGHTWEIGHT");
      createView("SYNCHRONOUS");

      database.begin();
      vertex("b").newEdge("K", vertex("a"));
      database.rollback();
      assertCounts(1);

      database.transaction(() -> vertex("a").newEdge("K", vertex("a")));
      assertCounts(2);

      dropViewAndAssertCounts(2);
    }

    /** A compaction folds the overlay's light edges into a fresh base, which must then hold each of them once. */
    @Test
    void lightEdgesAddedAfterTheBuildSurviveACompaction() throws Exception {
      createSchema("K LIGHTWEIGHT");
      createView("SYNCHRONOUS");
      view().setCompactionThreshold(1);

      database.transaction(() -> {
        vertex("b").newEdge("K", vertex("a"));
        vertex("b").newEdge("K", vertex("b"));
      });
      assertCounts(3);

      final long deadline = System.currentTimeMillis() + 60_000;
      while (Boolean.TRUE.equals(view().getStats().get("overlayActive")) && System.currentTimeMillis() < deadline)
        Thread.sleep(20);
      assertThat(view().getStats().get("overlayActive")).as("the compaction must have folded the overlay").isEqualTo(false);
      assertCounts(3);
      assertThat(view().getEdgeCount()).isEqualTo(3);
      view().setCompactionThreshold(GraphAnalyticalView.DEFAULT_COMPACTION_THRESHOLD);

      database.transaction(() -> outEdges("b").getFirst().delete());
      assertCounts(2);

      dropViewAndAssertCounts(2);
    }

    private void createSchema(final String edgeTypeDeclaration) {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE PROPERTY P.name STRING");
      database.command("sql", "CREATE EDGE TYPE " + edgeTypeDeclaration);
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("P").set("name", "a").save();
        final MutableVertex b = database.newVertex("P").set("name", "b").save();
        a.newEdge("K", b);
      });
    }

    private void createView(final String updateMode) throws InterruptedException {
      database.command("sql",
          "CREATE GRAPH ANALYTICAL VIEW v1 VERTEX TYPES (P) EDGE TYPES (K) UPDATE MODE " + updateMode);
      assertThat(view().awaitReady(60, TimeUnit.SECONDS)).isTrue();
    }

    private GraphAnalyticalView view() {
      return GraphAnalyticalViewRegistry.get(database, "v1");
    }

    private void awaitEdgeCount(final int expected) throws InterruptedException {
      final long deadline = System.currentTimeMillis() + 60_000;
      while (System.currentTimeMillis() < deadline) {
        final GraphAnalyticalView view = view();
        if (view.getStatus() == GraphAnalyticalView.Status.READY && view.getEdgeCount() == expected)
          return;
        Thread.sleep(20);
      }
      assertThat(view().getEdgeCount()).as("the asynchronous rebuild must pick the light edges up").isEqualTo(expected);
    }

    private void assertCounts(final long expected) {
      assertThat(count(COUNT)).as(COUNT).isEqualTo(expected);
      assertThat(count(ROW_COUNT)).as(ROW_COUNT).isEqualTo(expected);
    }

    private void dropViewAndAssertCounts(final long expected) {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW v1");
      assertCounts(expected);
    }

    private long count(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }

    private List<Edge> outEdges(final String name) {
      final List<Edge> edges = new ArrayList<>();
      for (final Edge e : vertex(name).getEdges(Vertex.DIRECTION.OUT, "K"))
        edges.add(e);
      return edges;
    }

    private MutableVertex vertex(final String name) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM P WHERE name = ?", name)) {
        return rs.next().getVertex().get().modify();
      }
    }
  }

  /**
   * Regression for #6049.
   * <p>
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
  @Nested
  class GAVSupernodeOrder extends TestHelper {
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
}
