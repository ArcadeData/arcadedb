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
package com.arcadedb.query.opencypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.traversal.BreadthFirstTraverser;
import com.arcadedb.query.opencypher.traversal.DepthFirstTraverser;
import com.arcadedb.query.opencypher.traversal.GraphTraverser;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10658: the ghost-edge probe of the walks that follow adjacency entries without loading the edge (#10148, #10149) costs one
 * edge-bucket page read per vertex reached, about a third of a variable-length hop that reaches many vertices.
 * {@link GlobalConfiguration#GRAPH_GHOST_EDGE_PROBE} keeps the probe by default and lets a database known to hold no dangling entry
 * turn it off, which every one of those walks then honours: each follows the ghost entry, as a fixed-length hop and SQL
 * {@code out()} do, and reports nothing.
 */
class Issue10658GhostEdgeProbeSettingTest extends TestHelper {
  private static final String   FROM_GA = "MATCH (a:G {name: 'ga'})";
  private static final String[] LINK    = { "LINK" };

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // The fixture leaves a dangling adjacency entry on purpose
    return false;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("G");
    database.getSchema().createEdgeType("LINK");
    // ga -LINK-> gb (its record removed, its entries left behind) and ga -LINK-> gc
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("G").set("name", "ga").save();
      final MutableVertex b = database.newVertex("G").set("name", "gb").save();
      final MutableVertex c = database.newVertex("G").set("name", "gc").save();
      a.newEdge("LINK", b).set("tag", "ghost").save();
      a.newEdge("LINK", c).set("tag", "live").save();
    });
    final RID ghost;
    try (final ResultSet rs = database.query("opencypher", "MATCH (:G {name: 'ga'})-[r:LINK {tag: 'ghost'}]->() RETURN r")) {
      ghost = ((Edge) rs.next().getProperty("r")).getIdentity();
    }
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, ghost));
  }

  @Test
  void theProbeIsOnByDefault() {
    assertThat(GlobalConfiguration.GRAPH_GHOST_EDGE_PROBE.getDefValue()).isEqualTo(true);
    assertThat(GhostEdgeReporter.isProbing(database)).isTrue();

    final long before = GhostEdgeReporter.getTotalSkipped();
    assertThat(rows(FROM_GA + "-[:LINK*1..3]->(b:G) RETURN DISTINCT b.name AS n")).containsExactly("gc");
    assertThat(rows("MATCH (a:G {name: 'ga'}) CALL path.subgraphNodes(a, {maxLevel: 3}) YIELD node RETURN node.name AS n"))
        .containsExactly("ga", "gc");
    assertThat(GhostEdgeReporter.getTotalSkipped()).as("the ghost is reported").isGreaterThan(before);
  }

  /** Every walk that probed follows the entry instead, and none of them reports it. */
  @Test
  void turnedOffEveryProbingWalkFollowsTheEntry() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_GHOST_EDGE_PROBE, false);
    assertThat(GhostEdgeReporter.isProbing(database)).isFalse();
    final Vertex start = vertex("ga");

    final long before = GhostEdgeReporter.getTotalSkipped();

    // The Cypher variable-length hops whose relationships are not read
    for (final String hop : new String[] { ":LINK*1..3", "*1..3", ":LINK*1..1" }) {
      assertThat(rows(FROM_GA + "-[" + hop + "]->(b:G) RETURN DISTINCT b.name AS n")).as("DISTINCT %s", hop)
          .containsExactly("gb", "gc");
      assertThat(rows(FROM_GA + "-[" + hop + "]->(b:G) RETURN b.name AS n")).as("rows %s", hop).containsExactly("gb", "gc");
    }

    // The traversers underneath: distinct endpoints, BFS and DFS vertex walks (Neighbours), and the path walks of unread
    // relationships (LiveEdges)
    for (final boolean detectCycles : new boolean[] { true, false }) {
      assertThat(names(new BreadthFirstTraverser(Direction.OUT, LINK, 1, 3, false, detectCycles).traverseDistinctEndpoints(start)))
          .as("distinct endpoints, cycles detected: %s", detectCycles).containsExactly("gb", "gc");
      assertThat(names(new BreadthFirstTraverser(Direction.OUT, LINK, 1, 3, false, detectCycles).traverse(start)))
          .as("BFS, cycles detected: %s", detectCycles).containsExactly("gb", "gc");
      assertThat(names(new DepthFirstTraverser(Direction.OUT, LINK, 1, 3, false, detectCycles).traverse(start)))
          .as("DFS, cycles detected: %s", detectCycles).containsExactly("gb", "gc");
    }
    for (final GraphTraverser traverser : new GraphTraverser[] {
        new BreadthFirstTraverser(Direction.OUT, LINK, 1, 3, true, true).withEdgesUnread(true),
        new DepthFirstTraverser(Direction.OUT, LINK, 1, 3, true, true).withEdgesUnread(true) }) {
      final Set<String> ends = new TreeSet<>();
      final Iterator<TraversalPath> paths = traverser.traversePaths(start);
      while (paths.hasNext())
        ends.add(paths.next().getEndVertex().getString("name"));
      assertThat(ends).as("paths of %s", traverser.getClass().getSimpleName()).containsExactly("gb", "gc");
    }

    // The reachability of path.subgraphNodes and path.subgraphAll (ComponentWalk)
    assertThat(rows("MATCH (a:G {name: 'ga'}) CALL path.subgraphNodes(a, {maxLevel: 3}) YIELD node RETURN node.name AS n"))
        .containsExactly("ga", "gb", "gc");

    assertThat(GhostEdgeReporter.getTotalSkipped()).as("no walk probed, so none reported").isEqualTo(before);
  }

  /** The walks that load the edge are not governed by the setting: they still skip the ghost, from the exception they catch. */
  @Test
  void turnedOffTheWalksThatReadTheEdgeStillSkipTheGhost() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_GHOST_EDGE_PROBE, false);

    assertThat(rows(FROM_GA + "-[r:LINK*1..3]->(b:G) RETURN DISTINCT b.name AS n, size(r) AS s")).containsExactly("gc");
  }

  /** The setting is read per walk, so turning it back on restores the probe without reopening the database. */
  @Test
  void turnedBackOnTheProbeIsBack() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_GHOST_EDGE_PROBE, false);
    assertThat(rows(FROM_GA + "-[:LINK*1..3]->(b:G) RETURN DISTINCT b.name AS n")).containsExactly("gb", "gc");

    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_GHOST_EDGE_PROBE, true);
    assertThat(rows(FROM_GA + "-[:LINK*1..3]->(b:G) RETURN DISTINCT b.name AS n")).containsExactly("gc");
  }

  private Vertex vertex(final String name) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM G WHERE name = ?", name)) {
      return rs.next().getVertex().orElseThrow();
    }
  }

  private static Set<String> names(final Iterator<Vertex> vertices) {
    final Set<String> names = new TreeSet<>();
    while (vertices.hasNext())
      names.add(vertices.next().getString("name"));
    return names;
  }

  /** The values of column {@code n}, one per row, sorted: a row answered twice shows twice. */
  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rows.add(rs.next().getProperty("n"));
    }
    rows.sort(null);
    return rows;
  }
}
