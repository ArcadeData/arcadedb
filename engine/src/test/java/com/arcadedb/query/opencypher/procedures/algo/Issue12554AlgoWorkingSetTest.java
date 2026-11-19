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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.NodeIdMapping;
import com.arcadedb.query.opencypher.procedures.algo.AbstractAlgoProcedure.GraphData;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.ref.Reference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12554: {@code algo.pagerank} and {@code algo.wcc} reserved about a tenth of the heap a call held, so the working-memory budget
 * did not protect the server: two calls at once ran out of memory instead of the second being refused.
 * <p>
 * The call held every vertex it loaded as a record object and indexed them in a boxed {@code HashMap}: about 250 bytes a vertex,
 * priced at 96. The graph is now held as the RIDs of its vertices alone, numbered in a primitive table, and priced at that.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12554AlgoWorkingSetTest {
  private static final String DB_PATH = "./target/databases/test-issue-12554-algo-working-set";

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void teardown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /** {@code vertices} vertices of {@code Node}, every vertex linked to two others. */
  private void createRing(final int vertices) {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");
    final RID[] rids = new RID[vertices];
    final int batch = 10_000;
    for (int from = 0; from < vertices; from += batch) {
      final int start = from;
      database.transaction(() -> {
        for (int i = start; i < Math.min(vertices, start + batch); i++)
          rids[i] = database.newVertex("Node").set("i", i, "name", "node" + i).save().getIdentity();
      });
    }
    for (int from = 0; from < vertices; from += batch) {
      final int start = from;
      database.transaction(() -> {
        for (int i = start; i < Math.min(vertices, start + batch); i++) {
          final MutableVertex v = rids[i].asVertex().modify();
          v.newEdge("LINK", rids[(i + 1) % vertices]).save();
          v.newEdge("LINK", rids[(int) (((long) i * 31 + 7) % vertices)]).save();
        }
      });
    }
  }

  private BasicCommandContext context() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  private static long usedHeapAfterCollection() {
    final Runtime runtime = Runtime.getRuntime();
    long used = Long.MAX_VALUE;
    // The reading settles once a collection frees nothing more: the smallest of a few is the live set
    for (int i = 0; i < 5; i++) {
      System.gc();
      try {
        Thread.sleep(30);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      used = Math.min(used, runtime.totalMemory() - runtime.freeMemory());
    }
    return used;
  }

  /**
   * One call to have the pages in the cache: what is measured is what a call holds, not what it caused to be read. A method of its own,
   * so that nothing of the call is still referenced from a slot of the caller's frame when the heap is read.
   */
  private void warmThePageCache(final Supplier<AbstractAlgoProcedure> procedure) {
    try (final Stream<Result> stream = procedure.get().execute(new Object[0], null, context())) {
      stream.count();
    }
  }

  /** The heap a call keeps once it has answered: its stream is held, as the statement holds it until the rows are read. */
  private long heapHeldByTheCall(final Supplier<AbstractAlgoProcedure> procedure) {
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1024L * 1024 * 1024);
    warmThePageCache(procedure);
    final long before = usedHeapAfterCollection();
    final Stream<Result> stream = procedure.get().execute(new Object[0], null, context());
    final long after = usedHeapAfterCollection();
    Reference.reachabilityFence(stream);
    stream.close();
    return after - before;
  }

  /** The smallest budget the call is accepted with: what it reserves. */
  private long reservedByTheCall(final Supplier<AbstractAlgoProcedure> procedure) {
    long refused = 1L;
    long accepted = 512L * 1024 * 1024;
    while (accepted - refused > 4096) {
      final long budget = refused + (accepted - refused) / 2;
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, budget);
      boolean fits;
      try (final Stream<Result> stream = procedure.get().execute(new Object[0], null, context())) {
        fits = true;
      } catch (final IllegalArgumentException e) {
        assertThat(e.getMessage()).contains("bytes allowed");
        fits = false;
      }
      if (fits)
        accepted = budget;
      else
        refused = budget;
    }
    return accepted;
  }

  @Test
  void pageRankReservesWhatItHolds() {
    final int vertices = 100_000;
    createRing(vertices);

    final long held = heapHeldByTheCall(AlgoPageRank::new);
    final long reserved = reservedByTheCall(AlgoPageRank::new);

    // The measurement has to have reached the call: it holds the RID of every vertex at least
    assertThat(held).as("heap held by algo.pagerank, measured").isGreaterThan(8L * vertices);
    assertThat(held).as("heap held by algo.pagerank on %d vertices", vertices).isLessThanOrEqualTo(reserved);
  }

  @Test
  void wccReservesWhatItHolds() {
    final int vertices = 100_000;
    createRing(vertices);

    final long held = heapHeldByTheCall(AlgoWCC::new);
    final long reserved = reservedByTheCall(AlgoWCC::new);

    // The measurement has to have reached the call: it holds the RID of every vertex at least
    assertThat(held).as("heap held by algo.wcc, measured").isGreaterThan(8L * vertices);
    assertThat(held).as("heap held by algo.wcc on %d vertices", vertices).isLessThanOrEqualTo(reserved);
  }

  /** A budget below what the call reserves refuses it, with the client error naming what it was asked for. */
  @Test
  void aBudgetBelowTheReservationRefusesTheCall() {
    createRing(5_000);
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 20_000L);

    assertThatThrownBy(() -> new AlgoWCC().execute(new Object[0], null, context()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("the loaded graph")
        .hasMessageContaining("bytes allowed");
    assertThatThrownBy(() -> new AlgoPageRank().execute(new Object[0], null, context()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("bytes allowed");
  }

  /** The nodes of a loaded graph are numbered in scan order: type by type, bucket by bucket, position by position. */
  @Test
  void aLoadedGraphNumbersItsNodesInScanOrder() {
    final VertexType first = database.getSchema().createVertexType("First", 3);
    final VertexType second = database.getSchema().createVertexType("Second", 2);
    // Many buckets for few vertices: each bucket holds an array of the size of what it holds, not a fixed minimum
    final VertexType tiny = database.getSchema().createVertexType("Tiny", 30);
    database.getSchema().createEdgeType("LINK");
    final List<RID> scanOrder = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        database.newVertex(first.getName()).set("i", i).save();
      for (int i = 0; i < 30; i++)
        database.newVertex(second.getName()).set("i", i).save();
      for (int i = 0; i < 7; i++)
        database.newVertex(tiny.getName()).set("i", i).save();
    });
    GraphEngine.getAllVertices(database, null).forEachRemaining(vertex -> scanOrder.add(vertex.getIdentity()));

    final GraphData graph = new AlgoWCC().loadGraphFromRecords(database, null, context());

    assertThat(graph.nodeCount).isEqualTo(77);
    for (int i = 0; i < scanOrder.size(); i++) {
      assertThat(graph.getRID(i)).as("node %d", i).isEqualTo(scanOrder.get(i));
      assertThat(graph.indexOf(scanOrder.get(i))).as("index of %s", scanOrder.get(i)).isEqualTo(i);
      assertThat(graph.getVertex(i).getIdentity()).isEqualTo(scanOrder.get(i));
    }
    assertThat(graph.indexOf(new RID(first.getBuckets(false).getFirst().getFileId(), 123_456L))).isEqualTo(-1);
  }

  /** The adjacency built from the id table is the one built from the loaded records, whatever the direction and the edge types. */
  @Test
  void theAdjacencyOfTheIdTableIsTheAdjacencyOfTheLoadedRecords() {
    database.getSchema().createVertexType("First", 3);
    database.getSchema().createVertexType("Second", 2);
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().buildEdgeType().withName("ONEWAY").withBidirectional(false).create();
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 60; i++)
        rids.add(database.newVertex(i % 3 == 0 ? "Second" : "First").set("i", i).save().getIdentity());
      for (int i = 0; i < 60; i++) {
        final MutableVertex source = rids.get(i).asVertex().modify();
        source.newEdge("KNOWS", rids.get((i * 7 + 3) % 60)).save();
        source.newEdge("ONEWAY", rids.get((i * 11 + 5) % 60)).save();
        if (i % 4 == 0)
          source.newEdge("KNOWS", rids.get(i)).save();
      }
    });

    final AlgoWCC procedure = new AlgoWCC();
    final NodeIdMapping ids = procedure.loadVertexIds(database, null, procedure.newMemoryBudget(database));
    final List<Vertex> vertices = new ArrayList<>();
    GraphEngine.getAllVertices(database, null).forEachRemaining(vertices::add);
    final Map<RID, Integer> index = GraphEngine.buildRidIndex(vertices);
    assertThat(ids.size()).isEqualTo(vertices.size());

    final long[] entries = new long[1];
    for (final Vertex.DIRECTION direction : Vertex.DIRECTION.values())
      for (final String[] types : new String[][] { null, { "KNOWS" }, { "ONEWAY" }, { "KNOWS", "ONEWAY" } }) {
        final int[][] expected = GraphEngine.buildAdjacencyList(vertices, index, direction, types);
        final int[][] actual = GraphEngine.buildAdjacencyList(ids, database, direction, types, count -> entries[0] = count, null);

        assertThat(actual).as("%s %s", direction, types == null ? "all types" : String.join(",", types)).isDeepEqualTo(expected);
        long total = 0;
        for (final int[] row : expected)
          total += row.length;
        assertThat(entries[0]).as("entries counted before the fill").isEqualTo(total);
      }
  }

  /** The rows of a build are checkpointed, so a guard can abort a build over a large graph. */
  @Test
  void theBuildOfTheAdjacencyCanBeAborted() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");
    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.newVertex("Node").set("i", i).save();
    });
    final AlgoWCC procedure = new AlgoWCC();
    final List<Integer> checkpoints = new ArrayList<>();

    assertThatThrownBy(() -> GraphEngine.buildAdjacencyList(procedure.loadVertexIds(database, null, procedure.newMemoryBudget(database)),
        database, Vertex.DIRECTION.BOTH, null, null, row -> {
          checkpoints.add(row);
          if (row == 7)
            throw new IllegalStateException("aborted at row 7");
        })).isInstanceOf(IllegalStateException.class).hasMessage("aborted at row 7");
    assertThat(checkpoints).containsExactly(0, 1, 2, 3, 4, 5, 6, 7);
  }

  /**
   * An edge-list entry whose edge record is gone (a ghost) is followed by the adjacency of a graph loaded from the records, as the Graph
   * Analytical View build and every other fallback adjacency follow it (issue #11104): the call neither throws nor skips the neighbour.
   */
  @Test
  void aGhostEdgeEntryIsFollowedNotSkipped() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");
    final RID[] rids = new RID[3];
    final RID[] ghost = new RID[1];
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        rids[i] = database.newVertex("Node").set("i", i).save().getIdentity();
      ghost[0] = rids[0].asVertex().modify().newEdge("LINK", rids[1]).save().getIdentity();
      rids[1].asVertex().modify().newEdge("LINK", rids[2]).save();
    });
    // Only the edge record goes, the entry in the edge lists of its two ends stays
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, ghost[0]));

    final GraphData graph = new AlgoWCC().loadGraphFromRecords(database, null, context());
    final int[][] adjacency = graph.adjacency(Vertex.DIRECTION.OUT);

    assertThat(adjacency[graph.indexOf(rids[0])]).containsExactly(graph.indexOf(rids[1]));
    assertThat(adjacency[graph.indexOf(rids[1])]).containsExactly(graph.indexOf(rids[2]));
    try (final Stream<Result> pageRank = new AlgoPageRank().execute(new Object[0], null, context())) {
      assertThat(pageRank.count()).isEqualTo(3);
    }
    try (final Stream<Result> wcc = new AlgoWCC().execute(new Object[0], null, context())) {
      assertThat(wcc.map(row -> row.<Integer>getProperty("componentId")).distinct().count()).isEqualTo(1);
    }
  }

  /** A hub of 40 edges to 40 leaves, in one bucket so that the rows are read in the order they were created. Answers the hub. */
  private RID createStar(final boolean hubFirst) {
    database.getSchema().createVertexType("Node", 1);
    database.getSchema().createEdgeType("LINK");
    final RID[] hubRid = new RID[1];
    database.transaction(() -> {
      final List<MutableVertex> leaves = new ArrayList<>();
      MutableVertex hub = null;
      if (hubFirst)
        hub = database.newVertex("Node").set("i", -1).save();
      for (int i = 0; i < 40; i++)
        leaves.add(database.newVertex("Node").set("i", i).save());
      if (!hubFirst)
        hub = database.newVertex("Node").set("i", -1).save();
      for (final MutableVertex leaf : leaves)
        hub.newEdge("LINK", leaf).set("w", 1.0).save();
      hubRid[0] = hub.getIdentity();
    });
    return hubRid[0];
  }

  /**
   * The buffers a supernode's row grows are priced before they are allocated: the new ones must fit beside the old ones, and what
   * is kept is charged once. 41 nodes cost 656 bytes and the row headers of the weighted adjacency 2624: 3280.
   */
  @Test
  void theBuffersOfASupernodeRowArePricedBeforeTheyGrow() {
    createStar(true);
    final AlgoWCC procedure = new AlgoWCC();

    // The hub's row outgrows the 16 entries its buffers start with at its 17th. The new buffers, 32 entries of 12 bytes (384), are
    // alive beside the old ones, which are reserved already: they do not fit the 120 bytes left
    assertThatThrownBy(() -> weightedAdjacencyUnder(procedure, 3400L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("the neighbour buffers of one node would need 384 bytes")
        .hasMessageContaining("a row of more than 16 edge entries");

    // The 16 entries the growth adds (192) fit the 220 bytes left, but the new buffers beside the old ones (384) do not: refused before
    // the allocation, not at the next doubling when the reserved growth has used the budget
    assertThatThrownBy(() -> weightedAdjacencyUnder(procedure, 3500L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("the neighbour buffers of one node would need 384 bytes")
        .hasMessageContaining("a row of more than 16 edge entries");

    // The buffers reach 64 entries, 576 bytes more than they started with: 3856, the last doubling (768 beside the 3472 reserved) fitting
    // 4240. The copy of the hub's row, 40 entries of 12 bytes, is refused before it is made when the 444 bytes left cannot hold it
    assertThatThrownBy(() -> weightedAdjacencyUnder(procedure, 4300L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("the weighted adjacency list would need 480 bytes")
        .hasMessageContaining("a row of 40 edge entries, 0 more not reserved yet");

    // A budget that holds all of it accepts the call: 3856, and the 80 entries of the two ends of its 40 edges, 960 bytes, make 4816.
    // The buffers are charged once, not again with the row they were copied into
    final int[][] neighbors = weightedAdjacencyUnder(procedure, 4900L);
    int longest = 0;
    for (final int[] row : neighbors)
      longest = Math.max(longest, row.length);
    assertThat(longest).isEqualTo(40);
  }

  /** The rows read before a long one, whose entries are not reserved yet, are on the heap beside its copy. */
  @Test
  void theCopyOfALongRowIsCheckedBesideTheRowsNotReservedYet() {
    createStar(false);
    final AlgoWCC procedure = new AlgoWCC();

    // The 40 leaves come first: one entry each, 40 waiting to be reserved. The last doubling of the hub's buffers, to 64 entries (768
    // bytes) beside the 40 entries (480) and the 3472 reserved, fits 4720. The buffers then reach 3856, and the copy of the row fits the
    // bytes left taken alone (480) but not with the 40 entries before it (960, 4816)
    assertThatThrownBy(() -> weightedAdjacencyUnder(procedure, 4750L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("the weighted adjacency list would need 960 bytes")
        .hasMessageContaining("a row of 40 edge entries, 40 more not reserved yet");

    // The growth of the buffers has the same rows beside it: the new buffers alone, 32 entries (384 bytes), fit the 620 bytes left, but not
    // with the 40 entries before the hub (864 bytes). Refused at the first doubling, not at the second when the reserved growth is used
    assertThatThrownBy(() -> weightedAdjacencyUnder(procedure, 3900L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("the neighbour buffers of one node would need 864 bytes")
        .hasMessageContaining("a row of more than 16 edge entries, 40 more not reserved yet");

    // Everything fits at 4816, with what the leaves reserve
    final int[][] neighbors = weightedAdjacencyUnder(procedure, 5000L);
    int longest = 0;
    for (final int[] row : neighbors)
      longest = Math.max(longest, row.length);
    assertThat(longest).isEqualTo(40);
  }

  /**
   * The single-source search keeps the same accounts through its own row build. 41 nodes cost 656 bytes and its rows and search state
   * 3444 (84 bytes a node): 4100. The 40 leaves come first, one entry each, not reserved yet when the hub's buffers grow.
   */
  @Test
  void theSingleSourceSearchChecksItsRowBuffersBesideTheRowsNotReservedYet() {
    final RID hub = createStar(false);

    // The new buffers alone, 32 entries (384 bytes), fit the 600 bytes left, but not with the 40 entries before the hub (864 bytes)
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 4700L);
    assertThatThrownBy(() -> new AlgoDijkstraSingleSource().execute(new Object[] { hub.asVertex(), "LINK", "w", "BOTH" }, null, context()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("the neighbour buffers of one node would need 864 bytes")
        .hasMessageContaining("a row of more than 16 edge entries, 40 more not reserved yet");

    // Everything fits at 5636: 4100, the buffers reaching 64 entries (576) and the 80 entries of the 40 edges (960)
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 6000L);
    try (final Stream<Result> rows = new AlgoDijkstraSingleSource().execute(new Object[] { hub.asVertex(), "LINK", "w", "BOTH" }, null,
        context())) {
      assertThat(rows.count()).isEqualTo(40);
    }
  }

  /** The weighted adjacency of the whole graph, with both directions, under the given working-memory budget. */
  private int[][] weightedAdjacencyUnder(final AlgoWCC procedure, final long budget) {
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, budget);
    final GraphData graph = procedure.loadGraphFromRecords(database, null, context());
    return graph.weightedAdjacency(WorkGuard.forDeadline("algo.test()", Long.MAX_VALUE), Vertex.DIRECTION.BOTH, "w").neighbors();
  }

  /** The entries of a list are checkpointed too: a supernode's list is not one unabortable step. */
  @Test
  void theEntriesOfASupernodeAreCheckpointedToo() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Node").set("i", -1).save();
      for (int i = 0; i < 30; i++)
        hub.newEdge("LINK", database.newVertex("Node").set("i", i).save()).save();
    });
    final AlgoWCC procedure = new AlgoWCC();
    final NodeIdMapping ids = procedure.loadVertexIds(database, null, procedure.newMemoryBudget(database));
    final int[] last = new int[1];

    // The 31 rows of the counting pass are 31 steps, and its 30 entries 30 more: step 40 is inside the list of the hub, before the pass is
    // over. Reaching the count of its entries means the checkpoint counted rows only, whatever the second pass would then have done
    assertThatThrownBy(() -> GraphEngine.buildAdjacencyList(ids, database, Vertex.DIRECTION.OUT, null,
        entries -> {
          throw new AssertionError("the counting pass ended with " + entries + " entries: no checkpoint fell inside the hub's list");
        }, step -> {
          last[0] = step;
          if (step == 40)
            throw new IllegalStateException("aborted at step 40");
        })).isInstanceOf(IllegalStateException.class).hasMessage("aborted at step 40");
    assertThat(last[0]).isEqualTo(40);
  }

  /**
   * The frontier of the single-source search holds a node once, however many shorter ways reach it: every {@code u_i} lowers the same
   * four targets, which a heap with one entry per relaxation held 4 x k entries of. The distances are the ones the entries gave.
   */
  @Test
  void theSingleSourceSearchAnswersTheDistancesWhenManyNodesLowerTheSameTargets() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");
    final int k = 20;
    final RID[] source = new RID[1];
    final List<RID> middle = new ArrayList<>();
    final List<RID> targets = new ArrayList<>();
    database.transaction(() -> {
      final MutableVertex s = database.newVertex("Node").set("name", "s").save();
      source[0] = s.getIdentity();
      for (int t = 0; t < 4; t++)
        targets.add(database.newVertex("Node").set("name", "t" + t).save().getIdentity());
      for (int i = 1; i <= k; i++) {
        final MutableVertex u = database.newVertex("Node").set("name", "u" + i).save();
        middle.add(u.getIdentity());
        s.newEdge("LINK", u).set("w", (double) i).save();
        for (final RID target : targets)
          u.newEdge("LINK", target).set("w", (double) (3 * k - 2 * i)).save();
      }
    });

    final Map<RID, Double> cost = new HashMap<>();
    try (final Stream<Result> rows = new AlgoDijkstraSingleSource().execute(new Object[] { source[0].asVertex(), "LINK", "w" }, null,
        context())) {
      rows.forEach(row -> cost.put(row.<RID>getProperty("node"), row.<Number>getProperty("cost").doubleValue()));
    }

    assertThat(cost).hasSize(k + 4);
    for (int i = 1; i <= k; i++)
      assertThat(cost.get(middle.get(i - 1))).as("u%d", i).isEqualTo((double) i);
    // The way through u_i costs i + 3k - 2i = 3k - i: the best is u_k, at 2k
    for (final RID target : targets)
      assertThat(cost.get(target)).isEqualTo((double) (2 * k));
  }

  /** The ids of a reordered mapping do not follow its buckets: a row is the vertex that has the id, not the one at that place. */
  @Test
  void theAdjacencyOfAReorderedMappingFollowsItsIds() {
    database.getSchema().createVertexType("Node", 1);
    database.getSchema().createEdgeType("LINK");
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      for (int i = 0; i < 2; i++)
        rids[i] = database.newVertex("Node").set("i", i).save().getIdentity();
      rids[0].asVertex().modify().newEdge("LINK", rids[1]).save();
    });
    final NodeIdMapping ids = new NodeIdMapping(1);
    final int bucketIdx = ids.registerBucket(rids[0].getBucketId(), "Node", 2);
    ids.addNode(bucketIdx, rids[0].getPosition());
    ids.addNode(bucketIdx, rids[1].getPosition());
    ids.compact();
    // A is the second node and B the first
    ids.applyReordering(new int[] { 1, 0 });

    assertThat(ids.getGlobalId(rids[0])).isEqualTo(1);
    assertThat(GraphEngine.buildAdjacencyList(ids, database, Vertex.DIRECTION.OUT, null, null, null)).isDeepEqualTo(new int[][] { {}, { 0 } });
    assertThat(GraphEngine.buildAdjacencyList(ids, database, Vertex.DIRECTION.IN, null, null, null)).isDeepEqualTo(new int[][] { { 1 }, {} });
  }

  /** The adjacency of a graph loaded from the records is one of the graph-driven loops of a call: the command deadline stops it. */
  @Test
  void theAdjacencyOfALoadedGraphStopsAtTheCommandDeadline() {
    createRing(100);
    final GraphData graph = new AlgoWCC().loadGraphFromRecords(database, null, context());

    // A deadline in 1970: the first row about to be walked is refused
    assertThatThrownBy(() -> graph.adjacency(WorkGuard.forDeadline("algo.test()", 1L), Vertex.DIRECTION.BOTH))
        .isInstanceOf(TimeoutException.class)
        .hasMessageContaining("algo.test()");
    assertThat(graph.adjacency(WorkGuard.forDeadline("algo.test()", Long.MAX_VALUE), Vertex.DIRECTION.BOTH).length).isEqualTo(100);
  }

  /**
   * A bucket that holds more than its count said makes the table double its array, copying it while the old one is held: the call is
   * refused before the copy when it does not fit, and loads the same nodes in the same order when it does.
   */
  @Test
  void aBucketHoldingMoreThanItsCountIsCheckedBeforeTheTableDoublesIt() {
    database.getSchema().createVertexType("Node", 1);
    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        database.newVertex("Node").set("i", i).save();
    });
    final List<RID> scanOrder = new ArrayList<>();
    GraphEngine.getAllVertices(database, null).forEachRemaining(vertex -> scanOrder.add(vertex.getIdentity()));
    // The count of the bucket is 4: its array starts with 4 slots and doubles at the 5th, 9th, 17th and 33rd node
    final AlgoWCC procedure = new AlgoWCC() {
      @Override
      long bucketCount(final Schema schema, final int bucketId) {
        return 4L;
      }
    };

    // 40 nodes cost 640 bytes, which 700 hold. At the 33rd node the array of 32 slots doubles to 64 beside the 32 nodes of the table: 96 slots
    // of 8 bytes, 768, which 700 do not. The doublings before it, 96, 192 and 384 bytes, fit
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 700L);
    assertThatThrownBy(() -> procedure.loadVertexIds(database, null, procedure.newMemoryBudget(database)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("the loaded graph would need 768 bytes")
        .hasMessageContaining("a bucket of more than 32 nodes");

    // 800 pass the doublings (the last 768) and hold the 640 bytes of the nodes, but the array of 64 slots the table ends with holds 24
    // slots more than the nodes, which it still holds while compact() copies them: 640 and 192 more, 832
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 800L);
    assertThatThrownBy(() -> procedure.loadVertexIds(database, null, procedure.newMemoryBudget(database)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("the loaded graph would need 192 bytes")
        .hasMessageContaining("a table of 64 slots for 40 nodes");

    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1000L);
    final NodeIdMapping ids = procedure.loadVertexIds(database, null, procedure.newMemoryBudget(database));
    assertThat(ids.size()).isEqualTo(40);
    for (int i = 0; i < scanOrder.size(); i++)
      assertThat(ids.getRID(database, i)).as("node %d", i).isEqualTo(scanOrder.get(i));
  }

  /** The slots a bucket did not use are on the heap while the next one is read: the doubling of the second is asked about with them. */
  @Test
  void theSlotsAnEarlierBucketDidNotUseAreCountedWhenALaterOneDoubles() {
    database.getSchema().createVertexType("First", 1);
    database.getSchema().createVertexType("Second", 1);
    database.transaction(() -> {
      for (int i = 0; i < 33; i++) {
        database.newVertex("First").set("i", i).save();
        database.newVertex("Second").set("i", i).save();
      }
    });
    final List<RID> scanOrder = new ArrayList<>();
    GraphEngine.getAllVertices(database, null).forEachRemaining(vertex -> scanOrder.add(vertex.getIdentity()));
    // Both buckets are counted as 4 and hold 33: each doubles its array to 64 at its 33rd node
    final AlgoWCC procedure = new AlgoWCC() {
      @Override
      long bucketCount(final Schema schema, final int bucketId) {
        return 4L;
      }
    };

    // The first bucket ends with an array of 64 slots, 31 of them unused. When the second doubles its 32 slots to 64 the table holds 96
    // slots, and the new array makes 160: 1280 bytes. Counted by the nodes read (65 and the 64 slots) it was 1032 and passed
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1100L);
    assertThatThrownBy(() -> procedure.loadVertexIds(database, null, procedure.newMemoryBudget(database)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("the loaded graph would need 1280 bytes")
        .hasMessageContaining("a bucket of more than 32 nodes");

    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 2000L);
    final NodeIdMapping ids = procedure.loadVertexIds(database, null, procedure.newMemoryBudget(database));
    assertThat(ids.size()).isEqualTo(66);
    for (int i = 0; i < scanOrder.size(); i++)
      assertThat(ids.getRID(database, i)).as("node %d", i).isEqualTo(scanOrder.get(i));
  }

  /** A vertex deleted after the graph was loaded is no node any more: it reads as gone, and the adjacency skips it. */
  @Test
  void aVertexDeletedAfterTheLoadIsSkipped() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        rids[i] = database.newVertex("Node").set("i", i).save().getIdentity();
      rids[0].asVertex().modify().newEdge("LINK", rids[1]).save();
      rids[1].asVertex().modify().newEdge("LINK", rids[2]).save();
    });

    final GraphData graph = new AlgoWCC().loadGraphFromRecords(database, null, context());
    final int middle = graph.indexOf(rids[1]);
    database.transaction(() -> rids[2].asVertex().delete());

    assertThat(graph.getVertex(graph.indexOf(rids[2]))).isNull();
    final int[][] adjacency = graph.adjacency(Vertex.DIRECTION.OUT);
    assertThat(adjacency.length).isEqualTo(3);
    assertThat(adjacency[graph.indexOf(rids[0])]).containsExactly(middle);
  }
}
