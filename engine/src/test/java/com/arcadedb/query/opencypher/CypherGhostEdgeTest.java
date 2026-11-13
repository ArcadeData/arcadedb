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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.traversal.BreadthFirstTraverser;
import com.arcadedb.query.opencypher.traversal.DepthFirstTraverser;
import com.arcadedb.query.opencypher.traversal.GraphTraverser;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Cypher traversal over ghost edge entries (edge-list entries whose edge record is gone).
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherGhostEdgeTest {
  /**
   * Issue #10149: the variable-length walks that follow adjacency entries without reading the edge now ask the bucket whether the edge of
   * an entry still has its record. That question must not need a grant the walk never needed: the edge is not read and not handed out, only
   * the vertex the entry names is, so a user with no READ_RECORD on the edge type who can traverse an anonymous relationship before the
   * probe can traverse it after, and still has a ghost skipped and reported.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10149GhostProbeNeedsNoEdgeGrant {
    private static final String PATH = "target/databases/Issue10149GhostProbeNeedsNoEdgeGrant";

    private DatabaseFactory factory;
    private Database        database;

    @BeforeEach
    void setUp() {
      factory = new DatabaseFactory(PATH).setSecurity(db -> {
      });
      if (factory.exists())
        factory.open().drop();

      database = factory.create();
      // These walks skip a ghost only with the probe on, off by default since #10809
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_GHOST_EDGE_PROBE, true);
      database.getSchema().createVertexType("G");
      database.getSchema().createEdgeType("LINK");
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("G").set("name", "ga").save();
        final MutableVertex b = database.newVertex("G").set("name", "gb").save();
        final MutableVertex c = database.newVertex("G").set("name", "gc").save();
        final MutableVertex d = database.newVertex("G").set("name", "gd").save();
        a.newEdge("LINK", b).set("tag", "ghost").save();
        a.newEdge("LINK", c).set("tag", "live-ac").save();
        c.newEdge("LINK", d).set("tag", "live-cd").save();
      });

      // Only the edge record goes: the entries naming it in the adjacency lists of both ends stay behind
      final RID ghost;
      try (final ResultSet rs = database.query("opencypher", "MATCH (:G {name: 'ga'})-[r:LINK {tag: 'ghost'}]->(:G {name: 'gb'}) RETURN r")) {
        ghost = ((Edge) rs.next().getProperty("r")).getIdentity();
      }
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, ghost));
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen()) {
        DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
        database.drop();
      }
      factory.close();
    }

    /**
     * The walks the probe was added to, called the way the Cypher operators call them. The Cypher route of a query is the planner's
     * choice, and the path executor that loads the edge records needs the grant whatever the probe does, so the walks are driven directly.
     */
    @Test
    void aUserWithoutReadOnTheEdgeTypeTraversesUnreadWalksAndHasTheGhostSkippedAndReported() {
      final Vertex start = vertex("ga");
      bindUserWithoutReadOnEdges();
      final String[] types = { "LINK" };

      final long before = GhostEdgeReporter.getTotalSkipped();
      assertThat(names(new BreadthFirstTraverser(Direction.OUT, types, 1, 3, false, true).traverseDistinctEndpoints(start)))
          .as("distinct endpoints").containsExactly("gc", "gd");
      assertThat(names(new BreadthFirstTraverser(Direction.OUT, types, 1, 3, false, true).traverse(start))).as("BFS vertices")
          .containsExactly("gc", "gd");
      assertThat(names(new DepthFirstTraverser(Direction.OUT, types, 1, 3, false, true).traverse(start))).as("DFS vertices")
          .containsExactly("gc", "gd");
      assertThat(GhostEdgeReporter.getTotalSkipped()).as("the ghost is reported for a user who cannot read its type").isGreaterThan(before);

      // The path walks of a traverser told that nothing reads the relationships probe every entry
      for (final GraphTraverser traverser : new GraphTraverser[] {
          new BreadthFirstTraverser(Direction.OUT, types, 1, 3, true, true).withEdgesUnread(true),
          new DepthFirstTraverser(Direction.OUT, types, 1, 3, true, true).withEdgesUnread(true) }) {
        final long beforePaths = GhostEdgeReporter.getTotalSkipped();
        final Set<String> ends = new TreeSet<>();
        final Iterator<TraversalPath> paths = traverser.traversePaths(start);
        while (paths.hasNext())
          ends.add(paths.next().getEndVertex().getString("name"));
        assertThat(ends).as(traverser.getClass().getSimpleName()).containsExactly("gc", "gd");
        assertThat(GhostEdgeReporter.getTotalSkipped()).as(traverser.getClass().getSimpleName() + " reports the ghost")
            .isGreaterThan(beforePaths);
      }
    }

    /** The control: the user really has no grant on the edge records, so a query that reads one is refused. */
    @Test
    void readingTheEdgeIsStillRefusedForThatUser() {
      bindUserWithoutReadOnEdges();

      final Throwable thrown = catchThrowable(() -> names("MATCH (a:G {name: 'ga'})-[r:LINK*1..3]->(b:G) RETURN DISTINCT b.name AS name, size(r)"));

      assertThat(thrown).as("a walk that loads the edge needs the grant").isNotNull();
      assertThat(CauseChain.find(thrown, SecurityException.class)).as("got %s", thrown).isNotNull();
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

    private Set<String> names(final String query) {
      final Set<String> names = new TreeSet<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          names.add(rs.next().getProperty(query.contains("AS name") ? "name" : "b.name"));
      }
      return names;
    }

    private void bindUserWithoutReadOnEdges() {
      final Set<Integer> edgeFiles = new HashSet<>();
      for (final Bucket bucket : database.getSchema().getType("LINK").getBuckets(true))
        edgeFiles.add(bucket.getFileId());

      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
        @Override
        public String getName() {
          return "vertexReader";
        }

        @Override
        public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
          return false;
        }

        @Override
        public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
          return !(access == ACCESS.READ_RECORD && edgeFiles.contains(fileId));
        }

        @Override
        public long getResultSetLimit() {
          return -1L;
        }

        @Override
        public long getReadTimeout() {
          return -1L;
        }
      });
    }
  }

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
  @Nested
  class Issue10149VariableLengthHopGhostEdge extends TestHelper {
    private static final String FROM_GA = "MATCH (a:G {name: 'ga'})";

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // The fixture leaves dangling adjacency entries on purpose
      return false;
    }

    @Override
    protected void beginTest() {
      // These walks skip a ghost only with the probe on, off by default since #10809
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_GHOST_EDGE_PROBE, true);
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
        // The raw rows: the set of them would hide a ghost answered as a second row
        assertThat(rows(FROM_GA + "-[" + hop + "]->(b:G) RETURN b.name AS name", "name"))
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
        assertThat(rows(FROM_GA + "-[:LINK*1..3]->(b:G) RETURN " + distinct + "b.name", "b.name")).containsExactly("gb", "gc");
        assertThat(rows(FROM_GA + "-[:LINK*1..1]->(b:G) RETURN " + distinct + "b.name", "b.name")).containsExactly("gc");
        assertThat(rows(FROM_GA + "-[:LINK*2..2]->(b:G) RETURN " + distinct + "b.name", "b.name")).containsExactly("gb");
      }
    }

    /**
     * Two relationships to the same vertex, one of them a ghost: the vertex is reached through the live one, once, whichever of the two
     * entries the walk meets first. The newest entry of an adjacency list is the first one walked, so the ghost is created last to be met
     * first. Met first, the ghost is probed, skipped and reported without spending the vertex; met second, the vertex has been found
     * already and the ghost is not probed at all (the probe is paid once per vertex reached), so a walk that finds each vertex once
     * reports nothing.
     */
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void aLiveEntryToTheSameVertexReachesItWhicheverEntryTheWalkMeetsFirst(final boolean ghostMetFirst) {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("G").set("name", "ga").save();
        final MutableVertex b = database.newVertex("G").set("name", "gb").save();
        a.newEdge("LINK", b).set("tag", ghostMetFirst ? "live" : "ghost").save();
        a.newEdge("LINK", b).set("tag", ghostMetFirst ? "ghost" : "live").save();
      });
      deleteTheGhost();
      final Vertex start = vertex("ga");
      final String[] types = { "LINK" };

      // The raw rows, not their set: a walk that followed both entries answers the vertex twice
      for (final String distinct : new String[] { "DISTINCT ", "" })
        for (final String hop : new String[] { "*1..1", "*1..3", ":LINK*1..2" })
          assertThat(rows(FROM_GA + "-[" + hop + "]->(b:G) RETURN " + distinct + "b.name", "b.name")).as("%s%s", distinct, hop)
              .containsExactly("gb");

      // With the cycles detected each walk finds the vertex once: the ghost is probed only if it is met first
      final long expectedProbes = ghostMetFirst ? 1L : 0L;
      long before = GhostEdgeReporter.getTotalSkipped();
      assertThat(rows(new BreadthFirstTraverser(Direction.OUT, types, 1, 3, false, true).traverseDistinctEndpoints(start)))
          .as("distinct endpoints").containsExactly("gb");
      assertThat(GhostEdgeReporter.getTotalSkipped() - before).as("probes of the distinct-endpoint walk").isEqualTo(expectedProbes);

      before = GhostEdgeReporter.getTotalSkipped();
      assertThat(rows(new BreadthFirstTraverser(Direction.OUT, types, 1, 3, false, true).traverse(start))).as("BFS").containsExactly("gb");
      assertThat(GhostEdgeReporter.getTotalSkipped() - before).as("probes of the BFS vertex walk").isEqualTo(expectedProbes);

      before = GhostEdgeReporter.getTotalSkipped();
      assertThat(rows(new DepthFirstTraverser(Direction.OUT, types, 1, 3, false, true).traverse(start))).as("DFS").containsExactly("gb");
      assertThat(GhostEdgeReporter.getTotalSkipped() - before).as("probes of the DFS vertex walk").isEqualTo(expectedProbes);

      // Without cycle detection nothing is remembered, so every entry is probed, and the vertex is still answered once
      before = GhostEdgeReporter.getTotalSkipped();
      assertThat(rows(new BreadthFirstTraverser(Direction.OUT, types, 1, 3, false, false).traverse(start))).as("BFS, walk").containsExactly("gb");
      assertThat(rows(new DepthFirstTraverser(Direction.OUT, types, 1, 3, false, false).traverse(start))).as("DFS, walk").containsExactly("gb");
      assertThat(GhostEdgeReporter.getTotalSkipped() - before).as("one probe of the ghost per walk, whichever entry is met first").isEqualTo(2L);
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

      deleteTheGhost();
    }

    /** Only the edge record tagged "ghost" goes: the entries naming it in the adjacency lists of both ends stay behind. */
    private void deleteTheGhost() {
      final RID ghost;
      try (final ResultSet rs = database.query("opencypher", "MATCH (:G {name: 'ga'})-[r:LINK {tag: 'ghost'}]->(:G {name: 'gb'}) RETURN r")) {
        ghost = ((Edge) rs.next().getProperty("r")).getIdentity();
      }
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, ghost));
    }

    private Vertex vertex(final String name) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM G WHERE name = ?", name)) {
        return rs.next().getVertex().orElseThrow();
      }
    }

    /** The names of the vertices, one per vertex the walk answers, in name order: a vertex answered twice shows twice. */
    private static List<String> rows(final Iterator<Vertex> vertices) {
      final List<String> rows = new ArrayList<>();
      while (vertices.hasNext())
        rows.add(vertices.next().getString("name"));
      rows.sort(null);
      return rows;
    }

    /** The values of {@code column} the query returns, one per row, in value order: a row answered twice shows twice. */
    private List<String> rows(final String query, final String column) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          rows.add(rs.next().getProperty(column));
      }
      rows.sort(null);
      return rows;
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

  /**
   * Issue #10463, what is decided for the walks that answer from the adjacency entries and read no edge: the fixed-length anonymous hop
   * ({@code MATCH (a)-[:T]->(b)}), {@code getVertices()} and SQL {@code out()}/{@code in()}, and every operator that counts or joins the
   * entries (the degree of a vertex, the fused {@code count(*)} chains, the analytical views built from the adjacency). They follow a
   * dangling entry - one whose edge record is gone - because that is what answering from the entries means: probing every entry would
   * make the count of a vertex's edges a walk of its edge pages, and a cold graph would pay one edge page per entry on the commonest
   * expand path. Since #10809 the variable-length walks follow it too by default, and skip and report it (#10149) only with
   * {@code arcadedb.graph.ghostEdgeProbe} turned on, where the entry is probed once per vertex reached.
   * <p>
   * The anomaly itself is repaired, not worked around at every read: {@code CHECK DATABASE ... FIX} prunes the dangling entries, after
   * which every walk agrees. This pins that sequence, so the two halves of the contract cannot drift apart.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10463GhostEntriesAreRepairedNotProbed extends TestHelper {

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // The fixture leaves dangling adjacency entries on purpose, and the test repairs them
      return false;
    }

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("G");
      database.getSchema().createEdgeType("LINK");
      database.getSchema().buildEdgeType().withName("UNI").withBidirectional(false).create();
    }

    @Test
    void theWalksThatAnswerFromTheEntriesFollowTheGhostUntilTheDatabaseIsChecked() {
      ghostEdge("LINK");

      // answered from the entries: the ghost is followed, as the degree of the vertex counts it
      assertThat(names("MATCH (a:G {name: 'ga'})-[:LINK]->(b:G) RETURN b.name AS name")).containsExactly("gb", "gc");
      assertThat(names("SELECT out('LINK').name AS name FROM G WHERE name = 'ga'")).contains("gb", "gc");
      assertThat(degree("ga")).isEqualTo(2L);

      // #10809: by default a variable-length hop answers from the entries too, so its one-hop form agrees with the fixed-length hop
      assertThat(names("MATCH (a:G {name: 'ga'})-[:LINK*1..1]->(b:G) RETURN b.name AS name")).containsExactly("gb", "gc");

      // the walks that load the edge skip the ghost whatever the setting, from the exception they catch
      final long before = GhostEdgeReporter.getTotalSkipped();
      // the relationship is read: a variable nothing reads is an anonymous hop, which follows the entry
      assertThat(names("MATCH (a:G {name: 'ga'})-[r:LINK]->(b:G) RETURN b.name AS name, r.tag AS tag")).containsExactly("gc");
      assertThat(GhostEdgeReporter.getTotalSkipped()).as("the ghost is reported by the walks that skip it").isGreaterThan(before);

      // with the probe turned on, the variable-length hop skips and reports it as well
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_GHOST_EDGE_PROBE, true);
      final long beforeProbe = GhostEdgeReporter.getTotalSkipped();
      assertThat(names("MATCH (a:G {name: 'ga'})-[:LINK*1..1]->(b:G) RETURN b.name AS name")).containsExactly("gc");
      assertThat(GhostEdgeReporter.getTotalSkipped()).as("the probing walk reports the ghost").isGreaterThan(beforeProbe);
    }

    @Test
    void checkDatabaseFixPrunesTheDanglingEntriesAndEveryWalkAgrees() {
      ghostEdge("LINK");

      final Result report = fix();
      assertThat(((Number) report.getProperty("prunedDanglingEntries")).longValue()).as("one entry in each list of the edge")
          .isEqualTo(2L);

      for (final String hop : new String[] { "[:LINK]", "[:LINK*1..1]", "[:LINK*1..3]", "[r:LINK]" })
        assertThat(names("MATCH (a:G {name: 'ga'})-" + hop + "->(b:G) RETURN b.name AS name")).as(hop).containsExactly("gc");
      assertThat(names("SELECT out('LINK').name AS name FROM G WHERE name = 'ga'")).containsExactly("gc");
      assertThat(degree("ga")).isEqualTo(1L);
    }

    /** A unidirectional type has the entry in the outgoing list of its source only, and the repair finds that one too. */
    @Test
    void theEntryOfAUnidirectionalTypeIsPrunedToo() {
      ghostEdge("UNI");

      assertThat(names("MATCH (a:G {name: 'ga'})-[:UNI]->(b:G) RETURN b.name AS name")).containsExactly("gb", "gc");

      final Result report = fix();
      assertThat(((Number) report.getProperty("prunedDanglingEntries")).longValue()).isEqualTo(1L);

      for (final String hop : new String[] { "-[:UNI]->", "-[:UNI*1..1]->", "-[:UNI*1..3]-", "-[r:UNI]->" })
        assertThat(names("MATCH (a:G {name: 'ga'})" + hop + "(b:G) RETURN b.name AS name")).as(hop).containsExactly("gc");
    }

    /** The graph of the issue: {@code ga -T-> gb} and {@code ga -T-> gc}, with the record of the first edge removed. */
    private void ghostEdge(final String type) {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("G").set("name", "ga").save();
        final MutableVertex b = database.newVertex("G").set("name", "gb").save();
        final MutableVertex c = database.newVertex("G").set("name", "gc").save();
        a.newEdge(type, b).set("tag", "ghost").save();
        a.newEdge(type, c).set("tag", "live").save();
      });

      final RID ghost;
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (:G {name: 'ga'})-[r:" + type + " {tag: 'ghost'}]->(:G {name: 'gb'}) RETURN r")) {
        ghost = ((Edge) rs.next().getProperty("r")).getIdentity();
      }
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, ghost));
    }

    private Result fix() {
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX")) {
        return rs.next();
      }
    }

    /** What the vertex says its degree is, which counts the entries of its list. */
    private long degree(final String name) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM G WHERE name = ?", name)) {
        final Vertex vertex = rs.next().getVertex().orElseThrow();
        return vertex.countEdges(Vertex.DIRECTION.OUT, "LINK");
      }
    }

    /** The values of the first column of the query, one per row, in value order. */
    private List<String> names(final String query) {
      final List<String> names = new ArrayList<>();
      final String language = query.startsWith("SELECT") ? "sql" : "opencypher";
      try (final ResultSet rs = database.query(language, query)) {
        while (rs.hasNext()) {
          final Object value = rs.next().getProperty("name");
          if (value instanceof Iterable<?> many)
            for (final Object each : many)
              names.add(String.valueOf(each));
          else
            names.add(String.valueOf(value));
        }
      }
      names.sort(null);
      return names;
    }
  }

  /**
   * Issue #10463, the stored side of an undirected hop over a unidirectional edge type: the variable-length walks skip and report a
   * ghost (an adjacency entry whose edge record is gone, #10149), except here. An undirected hop over a unidirectional type is answered
   * by {@code IncomingEdgeLookup}, whose outgoing half is the stored list of the vertex read as vertices, with no edge RID to ask the
   * bucket about, so {@code MATCH (a)-[:U*1..3]-(b) RETURN DISTINCT b.name} named a vertex that
   * {@code MATCH (a)-[r:U*1..3]-(b) RETURN DISTINCT b.name, size(r)} did not, and reported nothing.
   * <p>
   * The entries of that list are answered with their edge RIDs now, and the walk probes them as it probes every other entry: once per
   * vertex it is about to reach.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10463UndirectedHopOverUnidirectionalGhost extends TestHelper {
    private static final String FROM_GA = "MATCH (a:G {name: 'ga'})";

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // The fixture leaves dangling adjacency entries on purpose
      return false;
    }

    @Override
    protected void beginTest() {
      // These walks skip a ghost only with the probe on, off by default since #10809
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_GHOST_EDGE_PROBE, true);
      database.getSchema().createVertexType("G");
      database.getSchema().buildEdgeType().withName("U").withBidirectional(false).create();
      database.getSchema().createEdgeType("B");
    }

    /** Every spelling of the undirected hop answers the vertices the live relationships lead to, whatever it returns. */
    @Test
    void everySpellingOfAnUndirectedHopAnswersTheSameVertices() {
      ghostEdge(false);

      for (final String hop : new String[] { ":U*1..3", ":U*1..1", ":U*1..", ":U*", "*1..3" }) {
        assertThat(rows(FROM_GA + "-[" + hop + "]-(b:G) RETURN DISTINCT b.name AS name")).as("DISTINCT, relationship unread: %s", hop)
            .containsExactly("gc");
        // The raw rows: the set of them would hide a ghost answered as a second row
        assertThat(rows(FROM_GA + "-[" + hop + "]-(b:G) RETURN b.name AS name")).as("not DISTINCT, relationship unread: %s", hop)
            .containsExactly("gc");
        assertThat(rows(FROM_GA + "-[" + hop + "]-(b:G) RETURN b.name AS name, count(*) AS c")).as("aggregated: %s", hop)
            .containsExactly("gc");
        assertThat(rows(FROM_GA + "-[r" + hop + "]-(b:G) RETURN DISTINCT b.name AS name, size(r) AS n")).as("relationship returned: %s",
            hop).containsExactly("gc");
        assertThat(rows("MATCH p = (a:G {name: 'ga'})-[" + hop + "]-(b:G) RETURN DISTINCT b.name AS name")).as("path returned: %s", hop)
            .containsExactly("gc");
      }
    }

    /** The lower bounds: 0 makes the start an end vertex, above 1 leaves the distinct-endpoint walk for the path walk. */
    @Test
    void theBoundsOfTheHopDoNotChangeTheAnswer() {
      ghostEdge(false);

      assertThat(rows(FROM_GA + "-[:U*0..3]-(b:G) RETURN DISTINCT b.name AS name")).containsExactly("ga", "gc");
      assertThat(rows(FROM_GA + "-[:U*0..3]-(b:G) RETURN b.name AS name")).containsExactly("ga", "gc");
      assertThat(rows(FROM_GA + "-[:U*2..3]-(b:G) RETURN DISTINCT b.name AS name")).isEmpty();
      assertThat(rows(FROM_GA + "-[:U*2..3]-(b:G) RETURN b.name AS name")).isEmpty();
    }

    /** The ghost is reported by every shape, not only by the one that loads the relationship. */
    @Test
    void everyShapeReportsTheGhost() {
      ghostEdge(false);

      for (final String query : new String[] {
          FROM_GA + "-[:U*1..3]-(b:G) RETURN DISTINCT b.name AS name",
          FROM_GA + "-[:U*1..3]-(b:G) RETURN b.name AS name",
          FROM_GA + "-[:U*0..3]-(b:G) RETURN DISTINCT b.name AS name",
          FROM_GA + "-[*1..3]-(b:G) RETURN DISTINCT b.name AS name",
          FROM_GA + "-[r:U*1..3]-(b:G) RETURN DISTINCT b.name AS name, size(r) AS n" }) {
        final long before = GhostEdgeReporter.getTotalSkipped();
        rows(query);
        assertThat(GhostEdgeReporter.getTotalSkipped()).as("the ghost is reported by: %s", query).isGreaterThan(before);
      }
    }

    /** A ghost does not spend the vertex it names: gb is also reachable through gc, one hop later than the ghost pretended. */
    @Test
    void aGhostDoesNotHideALiveRouteToTheSameVertex() {
      ghostEdge(true);

      for (final String distinct : new String[] { "DISTINCT ", "" }) {
        assertThat(rows(FROM_GA + "-[:U*1..3]-(b:G) RETURN " + distinct + "b.name AS name")).containsExactly("gb", "gc");
        assertThat(rows(FROM_GA + "-[:U*1..1]-(b:G) RETURN " + distinct + "b.name AS name")).containsExactly("gc");
        assertThat(rows(FROM_GA + "-[:U*2..2]-(b:G) RETURN " + distinct + "b.name AS name")).containsExactly("gb");
      }
    }

    /**
     * Two relationships to the same vertex, one of them a ghost: the vertex is reached through the live one, once, whichever of the two
     * entries the walk meets first (the newest entry of an adjacency list is walked first, so the ghost is created last to be met
     * first).
     */
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void aLiveEntryToTheSameVertexReachesItWhicheverEntryTheWalkMeetsFirst(final boolean ghostMetFirst) {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("G").set("name", "ga").save();
        final MutableVertex b = database.newVertex("G").set("name", "gb").save();
        a.newEdge("U", b).set("tag", ghostMetFirst ? "live" : "ghost").save();
        a.newEdge("U", b).set("tag", ghostMetFirst ? "ghost" : "live").save();
      });
      deleteTheGhost();

      // The raw rows, not their set: a walk that followed both entries answers the vertex twice
      for (final String distinct : new String[] { "DISTINCT ", "" })
        for (final String hop : new String[] { "*1..1", "*1..3", ":U*1..2" })
          assertThat(rows(FROM_GA + "-[" + hop + "]-(b:G) RETURN " + distinct + "b.name AS name")).as("%s%s", distinct, hop)
              .containsExactly("gb");
    }

    /**
     * Found alongside: an undirected path walk asks each edge for its endpoints to tell a self-loop, which loads the record of an edge
     * the adjacency entry did not carry the endpoints of, and a ghost failed the whole walk with a {@code RecordNotFoundException}
     * instead of being skipped and reported. Over a bidirectional type too, from either end of the ghost.
     */
    @Test
    void anUndirectedPathWalkOverABidirectionalTypeSkipsTheGhostInsteadOfFailing() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("G").set("name", "ga").save();
        final MutableVertex b = database.newVertex("G").set("name", "gb").save();
        final MutableVertex c = database.newVertex("G").set("name", "gc").save();
        a.newEdge("B", b).set("tag", "ghost").save();
        a.newEdge("B", c).set("tag", "live").save();
      });
      final RID ghost;
      try (final ResultSet rs = database.query("opencypher", "MATCH (:G {name: 'ga'})-[r:B {tag: 'ghost'}]->(:G {name: 'gb'}) RETURN r")) {
        ghost = ((Edge) rs.next().getProperty("r")).getIdentity();
      }
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, ghost));

      final long before = GhostEdgeReporter.getTotalSkipped();
      assertThat(rows("MATCH (a:G {name: 'ga'})-[r:B*1..3]-(b:G) RETURN b.name AS name")).containsExactly("gc");
      assertThat(rows("MATCH (a:G {name: 'gb'})-[r:B*1..3]-(b:G) RETURN b.name AS name")).isEmpty();
      assertThat(rows("MATCH p = (a:G {name: 'ga'})-[:B*1..3]-(b:G) RETURN b.name AS name")).containsExactly("gc");
      assertThat(GhostEdgeReporter.getTotalSkipped()).as("the ghost is reported").isGreaterThan(before);
    }

    /** Nothing changes on a graph without a ghost: regular edges, a cycle, and the edges of the open transaction. */
    @Test
    void aCleanGraphIsWalkedAsBefore() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("G").set("name", "ga").save();
        final MutableVertex b = database.newVertex("G").set("name", "gb").save();
        final MutableVertex c = database.newVertex("G").set("name", "gc").save();
        final MutableVertex d = database.newVertex("G").set("name", "gd").save();
        a.newEdge("U", b).set("tag", "ab").save();
        a.newEdge("U", c).set("tag", "ac").save();
        c.newEdge("U", d).set("tag", "cd").save();
        d.newEdge("U", a).set("tag", "da").save();
      });

      final long before = GhostEdgeReporter.getTotalSkipped();
      for (final String distinct : new String[] { "DISTINCT ", "" }) {
        assertThat(rows(FROM_GA + "-[:U*1..1]-(b:G) RETURN " + distinct + "b.name AS name")).containsExactly("gb", "gc", "gd");
        // the cycle ga, gc, gd, ga is three distinct relationships long: it ends in ga
        assertThat(rows(FROM_GA + "-[:U*1..3]-(b:G) RETURN DISTINCT b.name AS name")).containsExactly("ga", "gb", "gc", "gd");
      }
      assertThat(GhostEdgeReporter.getTotalSkipped()).as("no ghost to report").isEqualTo(before);

      // an edge the open transaction wrote is live to the walk
      database.transaction(() -> {
        database.newVertex("G").set("name", "ge").save();
        database.command("opencypher", "MATCH (a:G {name: 'ga'}), (e:G {name: 'ge'}) CREATE (a)-[:U {tag: 'ae'}]->(e)").close();
        assertThat(rows(FROM_GA + "-[:U*1..1]-(b:G) RETURN DISTINCT b.name AS name")).contains("ge");
      });
    }

    /**
     * The graph of the issue: {@code ga -U-> gb} and {@code ga -U-> gc}, with the record of the first edge removed and the entry naming it
     * left behind in the outgoing list of ga (a unidirectional type has no incoming entry). With {@code liveRouteToB} there is also a
     * live {@code gc -U-> gb}.
     */
    private void ghostEdge(final boolean liveRouteToB) {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("G").set("name", "ga").save();
        final MutableVertex b = database.newVertex("G").set("name", "gb").save();
        final MutableVertex c = database.newVertex("G").set("name", "gc").save();
        a.newEdge("U", b).set("tag", "ghost").save();
        a.newEdge("U", c).set("tag", "live-ac").save();
        if (liveRouteToB)
          c.newEdge("U", b).set("tag", "live-cb").save();
      });

      deleteTheGhost();
    }

    /** Only the edge record tagged "ghost" goes: the entry naming it in the adjacency list of its source stays behind. */
    private void deleteTheGhost() {
      final RID ghost;
      try (final ResultSet rs = database.query("opencypher", "MATCH (:G {name: 'ga'})-[r:U {tag: 'ghost'}]->(:G {name: 'gb'}) RETURN r")) {
        ghost = ((Edge) rs.next().getProperty("r")).getIdentity();
      }
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, ghost));
    }

    /** The values of the first column of the query, one per row, in value order: a row answered twice shows twice. */
    private List<String> rows(final String query) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          rows.add(String.valueOf(rs.next().<Object>getProperty("name")));
      }
      rows.sort(null);
      return rows;
    }
  }

  /**
   * Issue #10658: the ghost-edge probe of the walks that follow adjacency entries without loading the edge (#10148, #10149) costs one
   * edge-bucket page read per vertex reached, about a third of a variable-length hop that reaches many vertices.
   * {@link GlobalConfiguration#GRAPH_GHOST_EDGE_PROBE} governs it, and every one of those walks honours it: turned off, each follows
   * the ghost entry, as a fixed-length hop and SQL {@code out()} do, and reports nothing. Since #10809 off is the default: the probe
   * was paid by every database for the benefit of a corrupt one, and a database suspected to hold dangling entries turns it on.
   */
  @Nested
  class Issue10658GhostEdgeProbeSetting extends TestHelper {
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

    /**
     * Issue #10809: off by default, so a variable-length hop answers what the fixed-length hop of the same pattern answers, and
     * nothing is probed or reported.
     */
    @Test
    void theProbeIsOffByDefault() {
      assertThat(GlobalConfiguration.GRAPH_GHOST_EDGE_PROBE.getDefValue()).isEqualTo(false);
      assertThat(GhostEdgeReporter.isProbing(database)).isFalse();

      final long before = GhostEdgeReporter.getTotalSkipped();
      assertThat(rows(FROM_GA + "-[:LINK*1..1]->(b:G) RETURN DISTINCT b.name AS n"))
          .isEqualTo(rows(FROM_GA + "-[:LINK]->(b:G) RETURN DISTINCT b.name AS n")).containsExactly("gb", "gc");
      assertThat(rows(FROM_GA + "-[:LINK*1..3]->(b:G) RETURN DISTINCT b.name AS n")).containsExactly("gb", "gc");
      assertThat(rows("MATCH (a:G {name: 'ga'}) CALL path.subgraphNodes(a, {maxLevel: 3}) YIELD node RETURN node.name AS n"))
          .containsExactly("ga", "gb", "gc");
      assertThat(GhostEdgeReporter.getTotalSkipped()).as("nothing probed, so nothing reported").isEqualTo(before);
    }

    /** Turned on, the probing walks skip the ghost and report it. */
    @Test
    void turnedOnTheProbingWalksSkipAndReportTheGhost() {
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_GHOST_EDGE_PROBE, true);
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
}
