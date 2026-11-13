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
package com.arcadedb.query.opencypher.procedures.path;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code path.*} walks over adjacency entries whose edge or far endpoint record is gone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PathGhostEntriesTest {
  /**
   * Issue #10148: a ghost, an adjacency entry whose edge record is gone, is skipped by every path walk - but only the walks that
   * read the edge noticed it. {@code collectReachableComponent} (behind {@code path.subgraphNodes} and {@code path.subgraphAll})
   * identified a neighbour by the RID in the entry and never loaded the edge, so a node reachable only through the ghost was in the
   * component, while {@code path.spanningTree} and {@code path.expandConfig} left it out and {@code path.subgraphAll} did not return
   * the relationship that reached it.
   * <p>
   * The guarantee is the invariant over the family, not a cell patch: over one fixture holding a ghost, the procedures that walk the
   * same component return the same nodes, and every node {@code path.subgraphAll} returns is the start node or an endpoint of one of
   * the relationships it returns in the same row.
   */
  @Nested
  class GhostEdge extends TestHelper {
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

    @Test
    void theIssueReproduction() {
      ghostEdge(false);

      assertThat(subgraphAllNodes("ga", "{relationshipFilter: 'LINK>'}")).containsExactly("ga", "gc");
      assertThat(subgraphAllRelationships("ga", "{relationshipFilter: 'LINK>'}")).containsExactly("live-ac");
    }

    @Test
    void everyProcedureOfTheFamilyAgreesOnTheComponent() {
      ghostEdge(false);

      for (final String filter : new String[] { "{relationshipFilter: 'LINK>'}", "{relationshipFilter: 'LINK'}", "{}" }) {
        final String walk = filter.substring(0, filter.length() - 1) + (filter.length() > 2 ? ", " : "") + "minLevel: 0, maxLevel: 3}";
        final Set<String> expected = Set.of("ga", "gc");
        assertThat(subgraphNodes("ga", filter)).as("subgraphNodes %s", filter).isEqualTo(expected);
        assertThat(subgraphAllNodes("ga", filter)).as("subgraphAll %s", filter).isEqualTo(expected);
        assertThat(spanningTreeNodes("ga", filter)).as("spanningTree %s", filter).isEqualTo(expected);
        assertThat(expandConfigNodes("ga", walk)).as("expandConfig %s", walk).isEqualTo(expected);
        assertEveryNodeTouchesARelationship("ga", filter);
      }
    }

    @Test
    void theGhostIsNotFollowedFromEitherEnd() {
      ghostEdge(false);

      // gb is the target of the ghost: walking against the direction of the edge, the only way to ga is the ghost
      for (final String filter : new String[] { "{relationshipFilter: '<LINK'}", "{relationshipFilter: 'LINK'}", "{}" }) {
        assertThat(subgraphNodes("gb", filter)).as("subgraphNodes %s", filter).containsExactly("gb");
        assertThat(subgraphAllNodes("gb", filter)).as("subgraphAll %s", filter).containsExactly("gb");
        assertThat(spanningTreeNodes("gb", filter)).as("spanningTree %s", filter).containsExactly("gb");
        assertThat(subgraphAllRelationships("gb", filter)).as("relationships %s", filter).isEmpty();
      }
    }

    /**
     * A ghost does not spend the node it names: gb is also reachable through gc, and is found there, one level later than the ghost
     * pretended. With the walk bounded to one hop it is not found at all.
     */
    @Test
    void aGhostDoesNotHideALiveRouteToTheSameNode() {
      ghostEdge(true);

      for (final String filter : new String[] { "{relationshipFilter: 'LINK>'}", "{relationshipFilter: 'LINK'}", "{}" }) {
        final Set<String> all = Set.of("ga", "gb", "gc");
        assertThat(subgraphNodes("ga", filter)).as("subgraphNodes %s", filter).isEqualTo(all);
        assertThat(subgraphAllNodes("ga", filter)).as("subgraphAll %s", filter).isEqualTo(all);
        assertThat(spanningTreeNodes("ga", filter)).as("spanningTree %s", filter).isEqualTo(all);
        assertEveryNodeTouchesARelationship("ga", filter);
      }

      final String oneHop = "{relationshipFilter: 'LINK>', maxLevel: 1}";
      assertThat(subgraphNodes("ga", oneHop)).containsExactly("ga", "gc");
      assertThat(subgraphAllNodes("ga", oneHop)).containsExactly("ga", "gc");
      assertThat(spanningTreeNodes("ga", oneHop)).containsExactly("ga", "gc");
      assertThat(expandConfigNodes("ga", "{relationshipFilter: 'LINK>', minLevel: 0, maxLevel: 1}")).containsExactly("ga", "gc");

      final String twoHops = "{relationshipFilter: 'LINK>', maxLevel: 2}";
      assertThat(subgraphNodes("ga", twoHops)).containsExactly("ga", "gb", "gc");
      assertThat(subgraphAllRelationships("ga", twoHops)).containsExactly("live-ac", "live-cb");
    }

    /** The ghost is also the first entry of the list when it was written last: the order of the entries does not matter. */
    @Test
    void theOrderOfTheEntriesDoesNotMatter() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("G").set("name", "ga").save();
        final MutableVertex b = database.newVertex("G").set("name", "gb").save();
        final MutableVertex c = database.newVertex("G").set("name", "gc").save();
        a.newEdge("LINK", c).set("tag", "live-ac").save();
        a.newEdge("LINK", b).set("tag", "ghost").save();
      });
      deleteGhost();

      assertThat(subgraphNodes("ga", "{relationshipFilter: 'LINK>'}")).containsExactly("ga", "gc");
      assertThat(subgraphAllNodes("ga", "{relationshipFilter: 'LINK>'}")).containsExactly("ga", "gc");
      assertThat(spanningTreeNodes("ga", "{relationshipFilter: 'LINK>'}")).containsExactly("ga", "gc");
    }

    @Test
    void aCleanGraphIsWalkedAsBefore() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("G").set("name", "ga").save();
        final MutableVertex b = database.newVertex("G").set("name", "gb").save();
        final MutableVertex c = database.newVertex("G").set("name", "gc").save();
        a.newEdge("LINK", b).set("tag", "ab").save();
        a.newEdge("LINK", c).set("tag", "ac").save();
        // a lightweight edge has no record to be a ghost of: its adjacency entry is all there is, and the walk follows it
        final MutableVertex d = database.newVertex("G").set("name", "gd").save();
        c.newEdge("LINK", d, true, (Object[]) null).save();
      });

      final Set<String> all = Set.of("ga", "gb", "gc", "gd");
      assertThat(subgraphNodes("ga", "{relationshipFilter: 'LINK>'}")).isEqualTo(all);
      assertThat(subgraphAllNodes("ga", "{relationshipFilter: 'LINK>'}")).isEqualTo(all);
      assertThat(spanningTreeNodes("ga", "{relationshipFilter: 'LINK>'}")).isEqualTo(all);
      assertThat(expandConfigNodes("ga", "{relationshipFilter: 'LINK>', minLevel: 0, maxLevel: 3}")).isEqualTo(all);
    }

    /** The bucket is asked through the transaction, so the edges written by the open one are live to the walk. */
    @Test
    void edgesOfTheOpenTransactionAreReached() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("G").set("name", "ga").save();
        final MutableVertex b = database.newVertex("G").set("name", "gb").save();
        final MutableVertex c = database.newVertex("G").set("name", "gc").save();
        a.newEdge("LINK", b).set("tag", "ab").save();
        b.newEdge("LINK", c, true, (Object[]) null).save();

        final Set<String> all = Set.of("ga", "gb", "gc");
        assertThat(subgraphNodes("ga", "{relationshipFilter: 'LINK>'}")).isEqualTo(all);
        assertThat(subgraphAllNodes("ga", "{relationshipFilter: 'LINK>'}")).isEqualTo(all);
        assertThat(spanningTreeNodes("ga", "{relationshipFilter: 'LINK>'}")).isEqualTo(all);
      });
    }

    /** A ghost made by the open transaction is a ghost to the walk: the bucket is asked through the transaction, not the committed pages. */
    @Test
    void aGhostMadeInTheOpenTransactionIsSkipped() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("G").set("name", "ga").save();
        final MutableVertex b = database.newVertex("G").set("name", "gb").save();
        final MutableVertex c = database.newVertex("G").set("name", "gc").save();
        a.newEdge("LINK", b).set("tag", "ghost").save();
        a.newEdge("LINK", c).set("tag", "live-ac").save();
      });

      final RID ghost;
      try (final ResultSet rs = database.query("opencypher", "MATCH (:G {name: 'ga'})-[r:LINK {tag: 'ghost'}]->(:G {name: 'gb'}) RETURN r")) {
        ghost = ((Edge) rs.next().getProperty("r")).getIdentity();
      }

      database.transaction(() -> {
        assertThat(subgraphNodes("ga", "{relationshipFilter: 'LINK>'}")).containsExactly("ga", "gb", "gc");
        TestHelper.deleteRecordAtLowLevel(database, ghost);
        assertThat(subgraphNodes("ga", "{relationshipFilter: 'LINK>'}")).containsExactly("ga", "gc");
        assertThat(spanningTreeNodes("ga", "{relationshipFilter: 'LINK>'}")).containsExactly("ga", "gc");
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
      deleteGhost();
    }

    /** Only the edge record goes: the entries naming it in the adjacency lists of both ends stay behind. */
    private void deleteGhost() {
      final RID ghost;
      try (final ResultSet rs = database.query("opencypher", "MATCH (:G {name: 'ga'})-[r:LINK {tag: 'ghost'}]->(:G {name: 'gb'}) RETURN r")) {
        ghost = ((Edge) rs.next().getProperty("r")).getIdentity();
      }
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, ghost));
    }

    private void assertEveryNodeTouchesARelationship(final String start, final String config) {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:G {name: '" + start + "'}) CALL path.subgraphAll(a, " + config + ") YIELD nodes, relationships RETURN nodes, relationships")) {
        final Result row = rs.next();
        final Set<String> endpoints = new TreeSet<>();
        endpoints.add(start);
        for (final Object relationship : (List<?>) row.getProperty("relationships")) {
          final Edge edge = (Edge) relationship;
          endpoints.add(edge.getOutVertex().getString("name"));
          endpoints.add(edge.getInVertex().getString("name"));
        }
        for (final Object node : (List<?>) row.getProperty("nodes"))
          assertThat(endpoints).as("the node is neither the start nor an endpoint of a returned relationship, config %s", config)
              .contains(((Vertex) node).getString("name"));
      }
    }

    private Set<String> subgraphNodes(final String start, final String config) {
      final Set<String> names = new TreeSet<>();
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:G {name: '" + start + "'}) CALL path.subgraphNodes(a, " + config + ") YIELD node RETURN node.name AS n")) {
        while (rs.hasNext())
          names.add(rs.next().getProperty("n"));
      }
      return names;
    }

    private Set<String> subgraphAllNodes(final String start, final String config) {
      final Set<String> names = new TreeSet<>();
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:G {name: '" + start + "'}) CALL path.subgraphAll(a, " + config + ") YIELD nodes RETURN [n IN nodes | n.name] AS n")) {
        names.addAll(rs.next().<List<String>>getProperty("n"));
      }
      return names;
    }

    private List<String> subgraphAllRelationships(final String start, final String config) {
      final List<String> tags = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:G {name: '" + start + "'}) CALL path.subgraphAll(a, " + config
              + ") YIELD relationships RETURN [r IN relationships | r.tag] AS t")) {
        tags.addAll(rs.next().<List<String>>getProperty("t"));
      }
      tags.sort(null);
      return tags;
    }

    private Set<String> spanningTreeNodes(final String start, final String config) {
      return pathNodes("path.spanningTree", start, config);
    }

    private Set<String> expandConfigNodes(final String start, final String config) {
      return pathNodes("path.expandConfig", start, config);
    }

    /** The names of the nodes on the paths a procedure returns. */
    private Set<String> pathNodes(final String procedure, final String start, final String config) {
      final Set<String> names = new TreeSet<>();
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:G {name: '" + start + "'}) CALL " + procedure + "(a, " + config + ") YIELD path RETURN [n IN nodes(path) | n.name] AS n")) {
        while (rs.hasNext())
          names.addAll(rs.next().<List<String>>getProperty("n"));
      }
      return names;
    }
  }

  /**
   * A reachable-component walk must drop an adjacency entry whose far endpoint has no record WHOLE: neither the
   * missing vertex in {@code nodes}, nor the edge reaching it in {@code relationships}. Reporting the edge while
   * leaving its endpoint out just moves the failure to whoever reads the edge back.
   * <p>
   * The ghost is fabricated the way {@code AlgoGhostEdgeTest} and {@code CypherMergeSemanticsTest.Issue394} do it - the record is deleted
   * from its bucket directly, so the pointers that name it survive - except that here it is the VERTEX that goes,
   * not the edge.
   */
  @Nested
  class GhostEndpoint {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-path-subgraph-ghost-endpoint");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createVertexType("Other");
      database.getSchema().createEdgeType("LINK");

      // A -LINK-> B (kept) and A -LINK-> GHOST, twice, so the second reference exercises the known-ghost path
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Node").set("name", "A").save();
        final MutableVertex b = database.newVertex("Node").set("name", "B").save();
        final MutableVertex ghost = database.newVertex("Node").set("name", "GHOST").save();
        // A ghost of a type a labelFilter of 'Node' leaves out: its label is answered from the schema without reading
        // the record, so it is the one neighbour a filtered walk could report an edge to without ever noticing it is
        // not there
        final MutableVertex otherGhost = database.newVertex("Other").set("name", "OTHER_GHOST").save();
        a.newEdge("LINK", b, true, (Object[]) null).save();
        a.newEdge("LINK", ghost, true, (Object[]) null).save();
        b.newEdge("LINK", ghost, true, (Object[]) null).save();
        a.newEdge("LINK", otherGhost, true, (Object[]) null).save();
      });

      // Delete only the vertex record: the edges and the adjacency entries naming it stay behind
      final RID ghostRID = (RID) database.query("sql", "SELECT FROM Node WHERE name = 'GHOST'").next().getIdentity().get();
      final RID otherGhostRID = (RID) database.query("sql", "SELECT FROM Other WHERE name = 'OTHER_GHOST'").next().getIdentity()
          .get();
      database.transaction(() -> {
        TestHelper.deleteRecordAtLowLevel(database, ghostRID);
        TestHelper.deleteRecordAtLowLevel(database, otherGhostRID);
      });
    }

    @AfterEach
    void teardown() {
      if (database != null)
        database.drop();
    }

    @Test
    void subgraphAllDropsBothTheGhostVertexAndTheEdgesReachingIt() {
      final ResultSet rs = database.query("cypher", """
          MATCH (a:Node {name: 'A'})
          CALL path.subgraphall(a, {relationshipFilter: 'LINK'}) YIELD nodes, relationships
          RETURN nodes, relationships
          """);

      final Result result = rs.next();
      final List<?> nodes = result.getProperty("nodes");
      final List<?> relationships = result.getProperty("relationships");

      assertThat(nodes).hasSize(2);
      assertThat(relationships).hasSize(1);
    }

    /**
     * {@code path.expandConfig} defaults to a breadth-first walk, so its BFS branch is the one most callers hit. It
     * resolves the endpoint before marking the RID visited, so a ghost is reported every time it is met rather than
     * only the first time - and no path is ever built through it.
     */
    @Test
    void expandConfigDoesNotWalkThroughTheGhostVertex() {
      final ResultSet rs = database.query("cypher", """
          MATCH (a:Node {name: 'A'})
          CALL path.expandconfig(a, {relationshipFilter: 'LINK', minLevel: 1, maxLevel: 3}) YIELD path
          RETURN path
          """);

      // The live edge is reached and the two ghost edges are not, so exactly one path comes back - asserting only
      // that some path did would also pass on a walk that rejected every neighbour
      assertThat(pathEndpoints(rs)).containsExactly("A>B");
    }

    /**
     * The depth-first branch of {@code path.expandConfig}, which {@code bfs: false} selects, is a different walk from
     * the breadth-first one above and carries its own visited/ghost bookkeeping.
     */
    @Test
    void expandConfigDepthFirstDoesNotWalkThroughTheGhostVertex() {
      final ResultSet rs = database.query("cypher", """
          MATCH (a:Node {name: 'A'})
          CALL path.expandconfig(a, {relationshipFilter: 'LINK', minLevel: 1, maxLevel: 3, bfs: false}) YIELD path
          RETURN path
          """);

      assertThat(pathEndpoints(rs)).containsExactly("A>B");
    }

    /**
     * {@code path.expand} is the other depth-first walk, with its own copy of the same bookkeeping.
     */
    @Test
    void expandDoesNotWalkThroughTheGhostVertex() {
      final ResultSet rs = database.query("cypher", """
          MATCH (a:Node {name: 'A'})
          CALL path.expand(a, 'LINK', null, 1, 3) YIELD path
          RETURN path
          """);

      assertThat(pathEndpoints(rs)).containsExactly("A>B");
    }

    /**
     * {@code path.spanningTree} reaches the same endpoint from two different vertices in this fixture, so it also
     * exercises the second encounter with an endpoint already known to be missing.
     */
    @Test
    void spanningTreeDoesNotWalkThroughTheGhostVertex() {
      final ResultSet rs = database.query("cypher", """
          MATCH (a:Node {name: 'A'})
          CALL path.spanningtree(a, {relationshipFilter: 'LINK'}) YIELD path
          RETURN path
          """);

      // The root path is always returned, so the live neighbour has to be named explicitly: a walk that rejected
      // every neighbour, ghost or not, would still return the root
      assertThat(pathEndpoints(rs)).containsExactly("A", "A>B");
    }

    /** Every returned path as the {@code name}s of its nodes, joined - the whole answer, not just how much of it. */
    private static List<String> pathEndpoints(final ResultSet rs) {
      final List<String> paths = new ArrayList<>();
      while (rs.hasNext()) {
        final List<?> nodes = ((TraversalPath) rs.next().getProperty("path")).getVertices();
        paths.add(nodes.stream().map(node -> ((Vertex) node).getString("name")).collect(Collectors.joining(">")));
      }
      return paths;
    }

    /**
     * The label of a neighbour is answered from the schema, so a ghost the {@code labelFilter} excludes is the one
     * that could reach {@code relationships} without ever being read. It must not: an edge is reported only where its
     * endpoint is there, whatever the filter says about the endpoint's label.
     */
    @Test
    void subgraphAllDropsTheEdgeToALabelFilteredGhost() {
      final Result result = database.query("cypher", """
          MATCH (a:Node {name: 'A'})
          CALL path.subgraphall(a, {relationshipFilter: 'LINK', labelFilter: 'Node'}) YIELD nodes, relationships
          RETURN nodes, relationships
          """).next();

      assertThat((List<?>) result.getProperty("nodes")).hasSize(2);
      assertThat((List<?>) result.getProperty("relationships")).hasSize(1);
    }

    @Test
    void subgraphNodesDropsTheGhostVertex() {
      final ResultSet rs = database.query("cypher", """
          MATCH (a:Node {name: 'A'})
          CALL path.subgraphnodes(a, {relationshipFilter: 'LINK'}) YIELD node
          RETURN count(node) AS total
          """);

      assertThat(((Number) rs.next().getProperty("total")).intValue()).isEqualTo(2);
    }
  }
}
