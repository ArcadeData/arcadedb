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

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10149: a dangling adjacency entry (a ghost: the edge record is gone, the entry naming it is still in the list) is skipped and
 * reported by every walk that loads the edge record, and was followed, silently, by every walk that takes its endpoints off the entry.
 * So a variable-length hop answered a different vertex set depending on whether the projection reads the relationship:
 * {@code RETURN DISTINCT b.name} named a vertex that {@code RETURN DISTINCT b.name, size(r)} did not, the engine's own integrity warning
 * fired only for the second, and an operator investigating the anomaly got a clean-looking answer from the query that was wrong.
 * <p>
 * The guarantee is the invariant over the family, not a cell patch: over one fixture holding a ghost, every spelling of a variable-length
 * hop answers the same vertices - and reports the ghost - whatever it returns, and so does each of the traversers underneath. The walks
 * that follow the entry without reading the edge still do not read it: they ask the bucket whether the record of the entry's edge is
 * there, once per vertex they are about to reach, which is what {@code path.subgraphNodes} does since #10148.
 * <p>
 * Fixed-length hops are not in scope: an anonymous {@code (a)-[:T]->(b)} follows the entry on purpose, as SQL {@code out()} does, and
 * {@code PatternPredicateGhostEdgeTest} pins it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10149VariableLengthHopGhostEdgeTest extends TestHelper {
  private static final String FROM_GA = "MATCH (a:G {name: 'ga'})";

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // The fixture leaves dangling adjacency entries on purpose
    return false;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("G");
    database.getSchema().createEdgeType("LINK");
  }

  /** The five rows of the issue's table that are variable-length hops, plus the spellings the table did not list. */
  @Test
  void everySpellingOfAVariableLengthHopAnswersTheSameVertices() {
    ghostEdge(false);

    for (final String hop : new String[] { ":LINK*1..3", ":LINK*1..1", ":LINK*1..", ":LINK*", "*1..3" }) {
      assertThat(names(FROM_GA + "-[" + hop + "]->(b:G) RETURN DISTINCT b.name AS name", "name"))
          .as("DISTINCT, relationship unread: %s", hop).containsExactly("gc");
      assertThat(names(FROM_GA + "-[" + hop + "]->(b:G) RETURN b.name AS name", "name"))
          .as("not DISTINCT, relationship unread: %s", hop).containsExactly("gc");
      assertThat(names(FROM_GA + "-[" + hop + "]->(b:G) RETURN b.name AS name, count(*) AS c", "name"))
          .as("aggregated: %s", hop).containsExactly("gc");
      assertThat(names(FROM_GA + "-[r" + hop + "]->(b:G) RETURN DISTINCT b.name AS name, size(r) AS n", "name"))
          .as("relationship returned: %s", hop).containsExactly("gc");
      assertThat(names("MATCH p = (a:G {name: 'ga'})-[" + hop + "]->(b:G) RETURN DISTINCT b.name AS name", "name"))
          .as("path returned: %s", hop).containsExactly("gc");
    }
  }

  /** A lower bound of 0 makes the start an end vertex, and a lower bound above 1 leaves the distinct-endpoint walk for the path walk. */
  @Test
  void theBoundsOfTheHopDoNotChangeTheAnswer() {
    ghostEdge(false);

    assertThat(names(FROM_GA + "-[:LINK*0..3]->(b:G) RETURN DISTINCT b.name", "b.name")).containsExactly("ga", "gc");
    assertThat(names(FROM_GA + "-[:LINK*0..3]->(b:G) RETURN b.name", "b.name")).containsExactly("ga", "gc");
    assertThat(names(FROM_GA + "-[:LINK*2..3]->(b:G) RETURN DISTINCT b.name", "b.name")).isEmpty();
    assertThat(names(FROM_GA + "-[:LINK*2..3]->(b:G) RETURN b.name", "b.name")).isEmpty();
  }

  /** gb is the far end of the ghost: walking against the direction of the edge, or undirected, its only way to ga is the ghost. */
  @Test
  void theGhostIsNotFollowedFromItsFarEnd() {
    ghostEdge(false);

    for (final String hop : new String[] { "<-[:LINK*1..3]-", "-[:LINK*1..3]-", "<-[*1..3]-" }) {
      assertThat(names("MATCH (a:G {name: 'gb'})" + hop + "(b:G) RETURN DISTINCT b.name", "b.name")).as("DISTINCT %s", hop).isEmpty();
      assertThat(names("MATCH (a:G {name: 'gb'})" + hop + "(b:G) RETURN b.name", "b.name")).as("not DISTINCT %s", hop).isEmpty();
    }
  }

  /** A ghost does not spend the vertex it names: gb is also reachable through gc, one hop later than the ghost pretended. */
  @Test
  void aGhostDoesNotHideALiveRouteToTheSameVertex() {
    ghostEdge(true);

    for (final String distinct : new String[] { "DISTINCT ", "" }) {
      assertThat(names(FROM_GA + "-[:LINK*1..3]->(b:G) RETURN " + distinct + "b.name", "b.name")).containsExactly("gb", "gc");
      assertThat(names(FROM_GA + "-[:LINK*1..1]->(b:G) RETURN " + distinct + "b.name", "b.name")).containsExactly("gc");
      assertThat(names(FROM_GA + "-[:LINK*2..2]->(b:G) RETURN " + distinct + "b.name", "b.name")).containsExactly("gb");
    }
  }

  /** The anomaly is reported by every shape, not only by the one that loads the relationship. */
  @Test
  void everyShapeReportsTheGhost() {
    ghostEdge(false);

    for (final String query : new String[] {
        FROM_GA + "-[:LINK*1..3]->(b:G) RETURN DISTINCT b.name",
        FROM_GA + "-[:LINK*1..3]->(b:G) RETURN b.name",
        FROM_GA + "-[:LINK*0..3]->(b:G) RETURN DISTINCT b.name",
        FROM_GA + "-[*1..3]->(b:G) RETURN DISTINCT b.name",
        FROM_GA + "-[r:LINK*1..3]->(b:G) RETURN DISTINCT b.name, size(r)" }) {
      final long before = GhostEdgeReporter.getTotalSkipped();
      names(query, "b.name");
      assertThat(GhostEdgeReporter.getTotalSkipped()).as("the ghost is reported by: %s", query).isGreaterThan(before);
    }
  }

  /** The traversers under the Cypher operators, called the way a caller that wants the end vertices does. */
  @Test
  void theVertexWalksOfEveryTraverserSkipTheGhost() {
    ghostEdge(false);
    final Vertex start = vertex("ga");

    for (final boolean detectCycles : new boolean[] { true, false }) {
      final long before = GhostEdgeReporter.getTotalSkipped();
      assertThat(names(new BreadthFirstTraverser(Direction.OUT, new String[] { "LINK" }, 1, 3, false, detectCycles).traverse(start)))
          .as("BFS vertices, cycles detected: %s", detectCycles).containsExactly("gc");
      assertThat(GhostEdgeReporter.getTotalSkipped()).as("BFS reports the ghost").isGreaterThan(before);

      final long beforeDfs = GhostEdgeReporter.getTotalSkipped();
      assertThat(names(new DepthFirstTraverser(Direction.OUT, new String[] { "LINK" }, 1, 3, false, detectCycles).traverse(start)))
          .as("DFS vertices, cycles detected: %s", detectCycles).containsExactly("gc");
      assertThat(GhostEdgeReporter.getTotalSkipped()).as("DFS reports the ghost").isGreaterThan(beforeDfs);

      assertThat(names(new BreadthFirstTraverser(Direction.OUT, new String[] { "LINK" }, 1, 3, false, detectCycles)
          .traverseDistinctEndpoints(start))).as("distinct endpoints, cycles detected: %s", detectCycles).containsExactly("gc");
    }

    final Vertex farEnd = vertex("gb");
    for (final Direction direction : new Direction[] { Direction.IN, Direction.BOTH }) {
      assertThat(names(new BreadthFirstTraverser(direction, new String[] { "LINK" }, 1, 3, false, true).traverse(farEnd)))
          .as("BFS from the far end, %s", direction).isEmpty();
      assertThat(names(new DepthFirstTraverser(direction, new String[] { "LINK" }, 1, 3, false, true).traverse(farEnd)))
          .as("DFS from the far end, %s", direction).isEmpty();
      assertThat(names(new BreadthFirstTraverser(direction, new String[] { "LINK" }, 1, 3, false, true).traverseDistinctEndpoints(farEnd)))
          .as("distinct endpoints from the far end, %s", direction).isEmpty();
    }
  }

  /** The path walks of a traverser told that nothing reads the relationships follow the entries too: they probe each one. */
  @Test
  void thePathWalksOfUnreadRelationshipsSkipTheGhost() {
    ghostEdge(false);
    final Vertex start = vertex("ga");

    for (final GraphTraverser traverser : new GraphTraverser[] {
        new BreadthFirstTraverser(Direction.OUT, new String[] { "LINK" }, 1, 3, true, true).withEdgesUnread(true),
        new DepthFirstTraverser(Direction.OUT, new String[] { "LINK" }, 1, 3, true, true).withEdgesUnread(true) }) {
      final Set<String> ends = new TreeSet<>();
      final Iterator<TraversalPath> paths = traverser.traversePaths(start);
      while (paths.hasNext())
        ends.add(paths.next().getEndVertex().getString("name"));
      assertThat(ends).as(traverser.getClass().getSimpleName()).containsExactly("gc");
    }
  }

  /** Nothing changes on a graph without a ghost: regular edges, a lightweight edge (no record to be a ghost of), and a cycle. */
  @Test
  void aCleanGraphIsWalkedAsBefore() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("G").set("name", "ga").save();
      final MutableVertex b = database.newVertex("G").set("name", "gb").save();
      final MutableVertex c = database.newVertex("G").set("name", "gc").save();
      final MutableVertex d = database.newVertex("G").set("name", "gd").save();
      a.newEdge("LINK", b).set("tag", "ab").save();
      a.newEdge("LINK", c).set("tag", "ac").save();
      c.newEdge("LINK", d, true, (Object[]) null).save();
      d.newEdge("LINK", a).set("tag", "da").save();
    });

    final long before = GhostEdgeReporter.getTotalSkipped();
    for (final String distinct : new String[] { "DISTINCT ", "" }) {
      assertThat(names(FROM_GA + "-[:LINK*1..3]->(b:G) RETURN " + distinct + "b.name", "b.name")).contains("gb", "gc", "gd");
      assertThat(names(FROM_GA + "-[:LINK*1..1]->(b:G) RETURN " + distinct + "b.name", "b.name")).containsExactly("gb", "gc");
      assertThat(names(FROM_GA + "-[:LINK*3..3]->(b:G) RETURN " + distinct + "b.name", "b.name")).containsExactly("ga");
    }
    assertThat(names(new BreadthFirstTraverser(Direction.OUT, new String[] { "LINK" }, 1, 3, false, true).traverse(vertex("ga"))))
        .containsExactly("gb", "gc", "gd");
    assertThat(GhostEdgeReporter.getTotalSkipped()).as("no ghost to report").isEqualTo(before);
  }

  /** The bucket is asked through the transaction, so the edges the open transaction wrote are live to the walk. */
  @Test
  void edgesOfTheOpenTransactionAreReached() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("G").set("name", "ga").save();
      final MutableVertex b = database.newVertex("G").set("name", "gb").save();
      final MutableVertex c = database.newVertex("G").set("name", "gc").save();
      a.newEdge("LINK", b).set("tag", "ab").save();
      b.newEdge("LINK", c).set("tag", "bc").save();

      assertThat(names(FROM_GA + "-[:LINK*1..3]->(x:G) RETURN DISTINCT x.name", "x.name")).containsExactly("gb", "gc");
      assertThat(names(FROM_GA + "-[:LINK*1..3]->(x:G) RETURN x.name", "x.name")).containsExactly("gb", "gc");
    });
  }

  /**
   * The graph of the issue: {@code ga -LINK-> gb} and {@code ga -LINK-> gc}, with the record of the first edge removed and both
   * adjacency entries left behind. With {@code liveRouteToB} there is also a live {@code gc -LINK-> gb}.
   */
  private void ghostEdge(final boolean liveRouteToB) {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("G").set("name", "ga").save();
      final MutableVertex b = database.newVertex("G").set("name", "gb").save();
      final MutableVertex c = database.newVertex("G").set("name", "gc").save();
      a.newEdge("LINK", b).set("tag", "ghost").save();
      a.newEdge("LINK", c).set("tag", "live-ac").save();
      if (liveRouteToB)
        c.newEdge("LINK", b).set("tag", "live-cb").save();
    });

    final RID ghost;
    try (final ResultSet rs = database.query("opencypher", "MATCH (:G {name: 'ga'})-[r:LINK {tag: 'ghost'}]->(:G {name: 'gb'}) RETURN r")) {
      ghost = ((Edge) rs.next().getProperty("r")).getIdentity();
    }
    // Only the edge record goes: the entries naming it in the adjacency lists of both ends stay behind
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, ghost));
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

  /** The distinct values of {@code column} the query returns. */
  private Set<String> names(final String query, final String column) {
    final Set<String> names = new TreeSet<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty(column));
    }
    return names;
  }
}
