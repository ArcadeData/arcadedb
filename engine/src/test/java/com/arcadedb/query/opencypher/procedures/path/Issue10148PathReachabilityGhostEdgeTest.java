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

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10148PathReachabilityGhostEdgeTest extends TestHelper {
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
