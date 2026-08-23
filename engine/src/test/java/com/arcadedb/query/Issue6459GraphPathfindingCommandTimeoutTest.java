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
package com.arcadedb.query;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6459 - {@code arcadedb.command.timeout} is purely cooperative, but the graph
 * path-finding functions consulted it nowhere: {@code astar()}/{@code dijkstra()} had no deadline or interrupt
 * check at all, SQL {@code shortestPath()} checked only a thread interrupt (never the deadline), and the Cypher
 * constrained {@code shortestPath()}/{@code allShortestPaths()} BFS (the edge-property / inline-{@code WHERE}
 * fallback) did the same - unlike the unconstrained BFS right next to it, which was already guarded.
 * <p>
 * Each test below reuses the graph shape proven effective by {@link Issue6266CommandTimeoutCoverageTest}: a
 * 20,000-node ring with two chords per node (out-degree 3, so every node has both directions once the edges are
 * made bidirectional). One pass over a graph this size costs seconds, comfortably clearing the 50 ms deadline used
 * here by orders of magnitude - so a check placed inside the loop aborts almost immediately and one placed nowhere
 * (or only between unrelated checkpoints) runs to graph exhaustion first, which is exactly the distinction the
 * issue is about.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6459GraphPathfindingCommandTimeoutTest {
  private static final int NODES = 20_000;
  /** A vertex with no edges at all: astar()/dijkstra() can never reach it, so they have to exhaust the whole
   *  20,000-node component before giving up - unless the deadline stops them first. */
  private static final int UNREACHABLE_IDX = NODES;

  private static final String DB_PATH = "./target/databases/test-issue-6459-pathfinding-command-timeout";

  private static Database database;
  private static RID[]     nodeRid;
  private static RID       unreachableRid;
  /** A second ring, the same size and shape as the first but sharing no edge with it, so a bidirectional
   *  meet-in-the-middle BFS between the two components can never meet: each side has to exhaust its own
   *  20,000-node component before its frontier goes empty. A far-but-reachable pair on ONE chorded ring does
   *  not do this - the out-degree-3 branching plus the two long chords give it small-world diameter, so a
   *  bidirectional search finds a real path within a handful of layers regardless of the guard. */
  private static RID[]     otherComponentRid;

  @BeforeAll
  static void setup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node").createProperty("v", Type.INTEGER);
    database.getSchema().createEdgeType("LINK");
    database.getSchema().getType("Node").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "v");

    nodeRid = new RID[NODES];
    otherComponentRid = new RID[NODES];

    database.transaction(() -> {
      nodeRid = buildRing(NODES, 0);
      otherComponentRid = buildRing(NODES, NODES + 1);

      unreachableRid = database.newVertex("Node").set("v", UNREACHABLE_IDX).save().getIdentity();
    });
  }

  /**
   * Ring plus two chords per node, exactly as {@link Issue6266CommandTimeoutCoverageTest}: out-degree 3, and
   * bidirectional (the {@code true} argument) so every node also has in-edges. {@code vBase} offsets the {@code v}
   * property so two rings built back to back get disjoint values and share no vertex.
   */
  private static RID[] buildRing(final int size, final int vBase) {
    final MutableVertex[] vertices = new MutableVertex[size];
    for (int i = 0; i < size; i++)
      vertices[i] = database.newVertex("Node").set("v", vBase + i).save();

    for (int i = 0; i < size; i++) {
      vertices[i].newEdge("LINK", vertices[(i + 1) % size], true, new Object[] { "w", 1.0 }).save();
      vertices[i].newEdge("LINK", vertices[(i + 7) % size], true, new Object[] { "w", 1.0 }).save();
      vertices[i].newEdge("LINK", vertices[(i + 31) % size], true, new Object[] { "w", 1.0 }).save();
    }

    final RID[] rids = new RID[size];
    for (int i = 0; i < size; i++)
      rids[i] = vertices[i].getIdentity();
    return rids;
  }

  @AfterAll
  static void teardown() {
    if (database != null)
      database.drop();
  }

  @AfterEach
  void resetTimeout() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
    // A test that arms the interrupt flag must not leave it set for whatever runs next on this thread.
    Thread.interrupted();
  }

  // ── astar() / dijkstra(): had no timeout or interrupt check at all ────────

  @Test
  @Tag("slow")
  @Timeout(120)
  void astarHonoursTheCommandTimeoutOnAnUnreachableDestination() {
    setTimeout(50);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> drainSql(
        "select expand(astar(" + nodeRid[0] + ", " + unreachableRid + ", 'w'))"))
        .as("astar() previously consulted neither the deadline nor a thread interrupt, so it explored the "
            + "whole reachable component before ever answering")
        .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    stopwatch.assertGaveUpWithin(60_000L, "astar() aborted from inside its main loop, not after exhausting the graph");
  }

  @Test
  @Tag("slow")
  @Timeout(120)
  void dijkstraHonoursTheCommandTimeoutOnAnUnreachableDestination() {
    // dijkstra() delegates straight to a fresh SQLFunctionAstar, so it shares the fix as well as the bug.
    setTimeout(50);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> drainSql(
        "select expand(dijkstra(" + nodeRid[0] + ", " + unreachableRid + ", 'w'))"))
        .as("dijkstra() must not survive its own unreachable-destination search past the deadline")
        .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    stopwatch.assertGaveUpWithin(60_000L, "dijkstra() aborted from inside astar()'s main loop");
  }

  @Test
  void astarStillReturnsTheCorrectPathWhenNothingAborts() {
    final List<Result> rows = drainSql("select expand(astar(" + nodeRid[0] + ", " + nodeRid[3] + ", 'w'))");
    assertThat(rows).as("a healthy call must still find a path once the guard is in place").isNotEmpty();
    assertThat(rows.getFirst().getIdentity()).contains(nodeRid[0]);
  }

  @Test
  void dijkstraStillReturnsTheCorrectPathWhenNothingAborts() {
    final List<Result> rows = drainSql("select expand(dijkstra(" + nodeRid[0] + ", " + nodeRid[3] + ", 'w'))");
    assertThat(rows).as("a healthy call must still find a path once the guard is in place").isNotEmpty();
    assertThat(rows.getFirst().getIdentity()).contains(nodeRid[0]);
  }

  @Test
  @Timeout(30)
  void astarStopsOnAThreadInterruptEvenWithoutADeadline() {
    // Before this fix astar() consulted no interrupt either - a caller had no way at all to cancel a run.
    Thread.currentThread().interrupt();

    assertThatThrownBy(() -> drainSql("select expand(astar(" + nodeRid[0] + ", " + unreachableRid + ", 'w'))"))
        .as("astar() must be at least abortable via an explicit cancel")
        .hasStackTraceContaining("astar() has been interrupted");
  }

  // ── SQL shortestPath(): checked Thread.interrupted() but never the deadline ─

  @Test
  @Tag("slow")
  @Timeout(120)
  void sqlShortestPathHonoursTheCommandTimeoutOnAnUnreachableDestination() {
    setTimeout(50);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    // Both endpoints sit on a fully separate ring: the search can never meet, so walkLeft/walkRight together
    // have to exhaust both 20,000-node components before the outer loop notices the frontiers are empty -
    // unless the deadline stops them first.
    assertThatThrownBy(() -> drainSql(
        "select expand(shortestPath(" + nodeRid[0] + ", " + otherComponentRid[0] + "))"))
        .as("shortestPath() checked Thread.interrupted() but never arcadedb.command.timeout")
        .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    stopwatch.assertGaveUpWithin(60_000L, "the bidirectional BFS aborted from inside walkLeft/walkRight's caller");
  }

  @Test
  void sqlShortestPathStillReturnsAPathWhenNothingAborts() {
    final List<Result> rows = drainSql("select expand(shortestPath(" + nodeRid[0] + ", " + nodeRid[3] + "))");
    assertThat(rows).as("a healthy call must still find a path once the guard is in place").isNotEmpty();
  }

  // ── Cypher MATCH shortestPath()/allShortestPaths(): the constrained BFS ────
  // fallback (an inline WHERE or property map on the relationship) had neither check, unlike the unconstrained
  // BFS right next to it.

  @Test
  @Tag("slow")
  @Timeout(120)
  void cypherConstrainedShortestPathHonoursTheCommandTimeout() {
    setTimeout(50);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> drainCypher(
        "MATCH (a:Node {v: $src}), (b:Node {v: $dst}), p = shortestPath((a)-[r:LINK* WHERE r.w > 0]-(b)) RETURN p",
        Map.of("src", 0, "dst", NODES / 2)))
        .as("an inline WHERE forces the edge-aware BFS (computeFilteredShortestPath), which consulted neither "
            + "check before this fix")
        .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    stopwatch.assertGaveUpWithin(60_000L, "the constrained frontier walk aborted from inside its own loop");
  }

  @Test
  @Tag("slow")
  @Timeout(120)
  void cypherConstrainedAllShortestPathsHonoursTheCommandTimeout() {
    setTimeout(50);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> drainCypher(
        "MATCH (a:Node {v: $src}), (b:Node {v: $dst}), p = allShortestPaths((a)-[r:LINK* WHERE r.w > 0]-(b)) RETURN p",
        Map.of("src", 0, "dst", NODES / 2)))
        .as("computeFilteredAllShortestPaths shares the same gap as its computeFilteredShortestPath sibling")
        .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    stopwatch.assertGaveUpWithin(60_000L, "the constrained layered BFS aborted from inside its own loop");
  }

  @Test
  void cypherShortestPathFormsStillReturnCorrectPathsWhenNothingAborts() {
    assertThat(drainCypher(
        "MATCH (a:Node {v: $src}), (b:Node {v: $dst}), p = shortestPath((a)-[:LINK*]-(b)) RETURN p",
        Map.of("src", 0, "dst", 3)))
        .as("the unconstrained MATCH form must still find a path")
        .isNotEmpty();

    assertThat(drainCypher(
        "MATCH (a:Node {v: $src}), (b:Node {v: $dst}), p = shortestPath((a)-[r:LINK* WHERE r.w > 0]-(b)) RETURN p",
        Map.of("src", 0, "dst", 3)))
        .as("the constrained MATCH form (inline WHERE) must still find a path")
        .isNotEmpty();

    assertThat(drainCypher(
        "MATCH (a:Node {v: $src}), (b:Node {v: $dst}), p = allShortestPaths((a)-[r:LINK* WHERE r.w > 0]-(b)) RETURN p",
        Map.of("src", 0, "dst", 3)))
        .as("the constrained allShortestPaths() form must still find at least one path")
        .isNotEmpty();

    assertThat(drainCypher(
        "MATCH (a:Node {v: $src}), (b:Node {v: $dst}) RETURN shortestPath((a)-[r:LINK* WHERE r.w > 0]-(b)) AS p",
        Map.of("src", 0, "dst", 3)))
        .as("the RETURN-position expression form (ShortestPathExpression) shares the same fix")
        .isNotEmpty();
  }

  // ── The default, disabled by default, leaves every form alone ─────────────

  @Test
  void theDisabledDefaultLeavesEveryFormAlone() {
    assertThat(database.getConfiguration().getValueAsLong(GlobalConfiguration.COMMAND_TIMEOUT))
        .as("the setting is off by default, so none of the checks added for this issue may fire")
        .isZero();

    assertThat(drainSql("select expand(astar(" + nodeRid[0] + ", " + nodeRid[3] + ", 'w'))")).isNotEmpty();
    assertThat(drainSql("select expand(dijkstra(" + nodeRid[0] + ", " + nodeRid[3] + ", 'w'))")).isNotEmpty();
    assertThat(drainSql("select expand(shortestPath(" + nodeRid[0] + ", " + nodeRid[3] + "))")).isNotEmpty();
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

  private static void setTimeout(final long millis) {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, millis);
  }

  private static List<Result> drainSql(final String query) {
    return drain("sql", query, Map.of());
  }

  private static List<Result> drainCypher(final String query) {
    return drain("opencypher", query, Map.of());
  }

  private static List<Result> drainCypher(final String query, final Map<String, Object> params) {
    return drain("opencypher", query, params);
  }

  private static List<Result> drain(final String language, final String query, final Map<String, Object> params) {
    final List<Result> rows = new java.util.ArrayList<>();
    try (final ResultSet rs = params.isEmpty() ? database.query(language, query) : database.query(language, query, params)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }
}
