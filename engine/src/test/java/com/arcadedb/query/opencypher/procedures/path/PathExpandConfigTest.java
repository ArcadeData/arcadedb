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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.procedure.ProcedureRegistry;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.QueryHeapTracker;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the {@code path.expandConfig} Cypher procedure: levels, uniqueness, the bounds of a walk and the release of its stream.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PathExpandConfigTest {
  /**
   * {@code maxLevel} bounds the walk, and a {@code maxLevel} or {@code limit} above the int range reads as no bound (issue #5924).
   */
  @Nested
  class Levels {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-path-expandconfig");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("KNOWS");

      // Chain: A -KNOWS-> B -KNOWS-> C
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Node").set("name", "A").save();
        final MutableVertex b = database.newVertex("Node").set("name", "B").save();
        final MutableVertex c = database.newVertex("Node").set("name", "C").save();
        a.newEdge("KNOWS", b, true, (Object[]) null).save();
        b.newEdge("KNOWS", c, true, (Object[]) null).save();
      });
    }

    @AfterEach
    void teardown() {
      if (database != null)
        database.drop();
    }

    private int countPaths(final ResultSet rs) {
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      return count;
    }

    @Test
    void expandConfigRespectsMaxLevel() {
      // maxLevel:1 → only the start node (level 0) and its direct neighbour (level 1): 2 paths
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:Node {name:'A'}) CALL path.expandConfig(a, {relationshipFilter:'KNOWS', maxLevel: 1}) YIELD path RETURN path");
      assertThat(countPaths(rs)).isEqualTo(2);
    }

    @Test
    void expandConfigMaxLevelAboveIntRangeIsTreatedAsUnbounded() {
      // Issue #5924: maxLevel used to narrow via .intValue(), so a Long above Integer.MAX_VALUE
      // wrapped to a negative int and the "currentLevel <= maxLevel" loop guard failed on the very
      // first iteration, silently returning 0 paths instead of the whole unbounded expansion.
      final int unboundedCount;
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Node {name:'A'}) CALL path.expandConfig(a, {relationshipFilter:'KNOWS'}) YIELD path RETURN path")) {
        unboundedCount = countPaths(rs);
      }
      assertThat(unboundedCount).isEqualTo(3); // levels 0, 1, 2

      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Node {name:'A'}) CALL path.expandConfig(a, {relationshipFilter:'KNOWS', maxLevel: $maxLevel}) YIELD path RETURN path",
          Map.of("maxLevel", 2147483648L))) {
        assertThat(countPaths(rs)).isEqualTo(unboundedCount);
      }
    }

    @Test
    void expandConfigLimitAboveIntRangeIsTreatedAsUnbounded() {
      // Issue #5924: same narrowing bug on `limit` - a Long above Integer.MAX_VALUE used to wrap to a
      // negative int, and "allPaths.size() < limit" is false for every non-negative size, so the
      // expansion produced 0 paths instead of the whole unbounded result.
      final int unboundedCount;
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Node {name:'A'}) CALL path.expandConfig(a, {relationshipFilter:'KNOWS'}) YIELD path RETURN path")) {
        unboundedCount = countPaths(rs);
      }

      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Node {name:'A'}) CALL path.expandConfig(a, {relationshipFilter:'KNOWS', limit: $limit}) YIELD path RETURN path",
          Map.of("limit", 2147483648L))) {
        assertThat(countPaths(rs)).isEqualTo(unboundedCount);
      }
    }
  }

  /**
   * Issue #10049: {@code path.expandConfig} reads APOC's {@code uniqueness} option, with APOC's nine values and APOC's default
   * ({@code RELATIONSHIP_PATH}), instead of always walking node-global (breadth-first) or node-path (depth-first). Also
   * {@code optional}, and the negative {@code maxLevel} / {@code limit} that APOC reads as "no bound".
   * <p>
   * Every expected answer below was produced by Neo4j 5.26 with APOC 5.26.30 on the same graph, built by the same
   * {@code CREATE}. Three graphs are used:
   * <pre>
   *   U, with cycles, parallel relationships and a self-loop: it tells the trail semantics apart from the node ones
   *     (s)-[:R]-&gt;(a), (s)-[:R]-&gt;(b), (a)-[:R]-&gt;(c), (b)-[:R]-&gt;(c), (c)-[:R]-&gt;(d), (d)-[:R]-&gt;(s), (a)-[:R2]-&gt;(c), (d)-[:L]-&gt;(d)
   *   V, a layered diamond, where the answer of every uniqueness is the same whichever order the walk meets the relationships in
   *     (s)-[:R]-&gt;(a)-[:R]-&gt;(c)-[:R]-&gt;(d), (s)-[:R]-&gt;(b)-[:R]-&gt;(c)
   *   S/T/M, where a node is denied on its first arrival and welcome on a later one
   *     (s:S)-[:R]-&gt;(m:M), (s)-[:R]-&gt;(t:T), (t)-[:R]-&gt;(m)
   * </pre>
   * The answers that depend on the order a walk meets the relationships in (which of two routes to a node survives a
   * node-level or relationship-level uniqueness) are not compared path by path: the test checks what every order agrees on.
   */
  @Nested
  class Uniqueness {
    private static final String FROM_S = "MATCH (s:U {name: 's'}) ";
    private static final String FROM_D = "MATCH (s:U {name: 'd'}) ";
    private static final String FROM_V = "MATCH (s:V {name: 's'}) ";
    private static final String SEQUENCE_START = "MATCH (s:S {name: 's'}) ";

    private static final List<String> TRAILS_FROM_S_UP_TO_3 = List.of("s", "s-[R<]-d", "s-[R>]-a", "s-[R>]-b", "s-[R<]-d-[L>]-d",
        "s-[R<]-d-[R<]-c", "s-[R>]-a-[R>]-c", "s-[R>]-b-[R>]-c", "s-[R>]-a-[R2>]-c", "s-[R<]-d-[L>]-d-[R<]-c",
        "s-[R<]-d-[R<]-c-[R<]-a", "s-[R<]-d-[R<]-c-[R<]-b", "s-[R>]-a-[R>]-c-[R<]-b", "s-[R>]-a-[R>]-c-[R>]-d",
        "s-[R>]-b-[R>]-c-[R<]-a", "s-[R>]-b-[R>]-c-[R>]-d", "s-[R<]-d-[R<]-c-[R2<]-a", "s-[R>]-a-[R2>]-c-[R<]-a",
        "s-[R>]-a-[R2>]-c-[R<]-b", "s-[R>]-a-[R2>]-c-[R>]-d", "s-[R>]-a-[R>]-c-[R2<]-a", "s-[R>]-b-[R>]-c-[R2<]-a");

    private static final List<String> NODE_PATHS_FROM_S_UP_TO_3 = List.of("s", "s-[R<]-d", "s-[R>]-a", "s-[R>]-b", "s-[R<]-d-[R<]-c",
        "s-[R>]-a-[R>]-c", "s-[R>]-b-[R>]-c", "s-[R>]-a-[R2>]-c", "s-[R<]-d-[R<]-c-[R<]-a", "s-[R<]-d-[R<]-c-[R<]-b",
        "s-[R>]-a-[R>]-c-[R<]-b", "s-[R>]-a-[R>]-c-[R>]-d", "s-[R>]-b-[R>]-c-[R<]-a", "s-[R>]-b-[R>]-c-[R>]-d",
        "s-[R<]-d-[R<]-c-[R2<]-a", "s-[R>]-a-[R2>]-c-[R<]-b", "s-[R>]-a-[R2>]-c-[R>]-d", "s-[R>]-b-[R>]-c-[R2<]-a");

    // The trails with no restriction at all: the one extra path walks the relationship it arrived by after a self-loop
    private static final List<String> UNRESTRICTED_FROM_S_UP_TO_3 = unrestricted();

    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue10049-path-uniqueness");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.transaction(() -> {
        database.command("opencypher", """
            CREATE (s:U {name:'s'}), (a:U {name:'a'}), (b:U {name:'b'}), (c:U {name:'c'}), (d:U {name:'d'}),
                   (s)-[:R]->(a), (s)-[:R]->(b), (a)-[:R]->(c), (b)-[:R]->(c), (c)-[:R]->(d), (d)-[:R]->(s),
                   (a)-[:R2]->(c), (d)-[:L]->(d)
            """).close();
        database.command("opencypher", """
            CREATE (s:V {name:'s'}), (a:V {name:'a'}), (b:V {name:'b'}), (c:V {name:'c'}), (d:V {name:'d'}),
                   (s)-[:R]->(a), (s)-[:R]->(b), (a)-[:R]->(c), (b)-[:R]->(c), (c)-[:R]->(d)
            """).close();
        database.command("opencypher", """
            CREATE (s:S {name:'s'}), (m:M {name:'m'}), (t:T {name:'t'}), (s)-[:R]->(m), (s)-[:R]->(t), (t)-[:R]->(m)
            """).close();
      });
    }

    @AfterEach
    void teardown() {
      if (database != null)
        database.drop();
    }

    private static List<String> unrestricted() {
      final List<String> all = new ArrayList<>(TRAILS_FROM_S_UP_TO_3);
      all.add("s-[R<]-d-[L>]-d-[R>]-s");
      return all;
    }

    // ── The default is APOC's, RELATIONSHIP_PATH ──────────────────────────────

    @Test
    void theDefaultUniquenessIsRelationshipPath() {
      assertThat(fromS("{maxLevel: 3}")).containsExactlyInAnyOrderElementsOf(TRAILS_FROM_S_UP_TO_3);
      assertThat(fromS("{maxLevel: 3, uniqueness: 'RELATIONSHIP_PATH'}")).containsExactlyInAnyOrderElementsOf(TRAILS_FROM_S_UP_TO_3);
      // The walk order is not part of the answer: the depth-first walk finds the same trails
      assertThat(fromS("{maxLevel: 3, bfs: false}")).containsExactlyInAnyOrderElementsOf(TRAILS_FROM_S_UP_TO_3);
      assertThat(fromS("{maxLevel: 3, uniqueness: 'RELATIONSHIP_PATH', bfs: false}"))
          .containsExactlyInAnyOrderElementsOf(TRAILS_FROM_S_UP_TO_3);
    }

    @Test
    void relationshipPathFindsEveryTrailNotOnlyEveryNode() {
      // The reproduction of the issue: three more routes reach c (and d, and back) than a node-global walk would report
      assertThat(fromS("{maxLevel: 4}")).hasSize(42);
      assertThat(fromS("{maxLevel: 5, relationshipFilter: 'R>'}")).containsExactlyInAnyOrder("s", "s-[R>]-a", "s-[R>]-b",
          "s-[R>]-a-[R>]-c", "s-[R>]-b-[R>]-c", "s-[R>]-a-[R>]-c-[R>]-d", "s-[R>]-b-[R>]-c-[R>]-d",
          "s-[R>]-a-[R>]-c-[R>]-d-[R>]-s", "s-[R>]-b-[R>]-c-[R>]-d-[R>]-s", "s-[R>]-a-[R>]-c-[R>]-d-[R>]-s-[R>]-b",
          "s-[R>]-b-[R>]-c-[R>]-d-[R>]-s-[R>]-a");
    }

    @Test
    void relationshipPathNeverWalksBackAlongTheRelationshipItArrivedBy() {
      // Two hops from s: the paths a-then-back-to-s and d-then-back-to-s are not trails of the walk, whatever the uniqueness
      assertThat(fromS("{maxLevel: 2, uniqueness: 'NONE'}")).containsExactlyInAnyOrder("s", "s-[R<]-d", "s-[R>]-a", "s-[R>]-b",
          "s-[R<]-d-[L>]-d", "s-[R<]-d-[R<]-c", "s-[R>]-a-[R>]-c", "s-[R>]-b-[R>]-c", "s-[R>]-a-[R2>]-c");
    }

    // ── The nine values ───────────────────────────────────────────────────────

    @Test
    void nodePath() {
      assertThat(fromS("{maxLevel: 3, uniqueness: 'NODE_PATH'}")).containsExactlyInAnyOrderElementsOf(NODE_PATHS_FROM_S_UP_TO_3);
      assertThat(fromS("{maxLevel: 3, uniqueness: 'NODE_PATH', bfs: false}"))
          .containsExactlyInAnyOrderElementsOf(NODE_PATHS_FROM_S_UP_TO_3);
      // No node repeats, so the paths stop growing: there is nothing past depth 3 in this graph
      assertThat(fromS("{maxLevel: 4, uniqueness: 'NODE_PATH'}")).hasSize(18);
      assertThat(fromS("{maxLevel: 5, uniqueness: 'NODE_PATH', relationshipFilter: 'R>'}")).containsExactlyInAnyOrder("s",
          "s-[R>]-a", "s-[R>]-b", "s-[R>]-a-[R>]-c", "s-[R>]-b-[R>]-c", "s-[R>]-a-[R>]-c-[R>]-d", "s-[R>]-b-[R>]-c-[R>]-d");
    }

    @Test
    void none() {
      assertThat(fromS("{maxLevel: 3, uniqueness: 'NONE'}")).containsExactlyInAnyOrderElementsOf(UNRESTRICTED_FROM_S_UP_TO_3);
      assertThat(fromS("{maxLevel: 3, uniqueness: 'NONE', bfs: false}"))
          .containsExactlyInAnyOrderElementsOf(UNRESTRICTED_FROM_S_UP_TO_3);
    }

    @Test
    void nodeGlobalReportsEachNodeOnceAtItsShortestDistance() {
      assertThat(lengthAndEnd(FROM_S + "CALL path.expandConfig(s, {maxLevel: 3, uniqueness: 'NODE_GLOBAL'}) YIELD path RETURN path"))
          .containsExactly("0:s", "1:a", "1:b", "1:d", "2:c");
      // The depth-first walk reaches every node too, along whatever route it meets first
      assertThat(fromS("{maxLevel: 3, uniqueness: 'NODE_GLOBAL', bfs: false}")).hasSize(5);
    }

    @Test
    void nodeLevelKeepsAnotherVisitOfANodeAtAnotherLevel() {
      final List<String> paths = fromS("{maxLevel: 3, uniqueness: 'NODE_LEVEL'}");
      // d is reached at level 1 and again at level 2 through its self-loop: a node-global walk would refuse the second visit
      assertThat(paths).contains("s-[R<]-d", "s-[R<]-d-[L>]-d");
      // ... but never twice at the same level
      final Set<String> levelAndEnd = new HashSet<>();
      for (final String path : paths)
        assertThat(levelAndEnd.add(depthOf(path) + ":" + endOf(path))).as("%s repeats a node at its level", path).isTrue();
      // The first three levels are the same whichever of two routes to a node survives
      assertThat(levelAndEnd).contains("0:s", "1:a", "1:b", "1:d", "2:c", "2:d");
    }

    @Test
    void relationshipGlobalWalksEachRelationshipOnce() {
      // The start, and one path per relationship of the graph: all eight are within three hops, each walked once
      assertThat(fromS("{maxLevel: 3, uniqueness: 'RELATIONSHIP_GLOBAL'}")).hasSize(9);
      final List<RID> walked = lastRelationships(
          FROM_S + "CALL path.expandConfig(s, {maxLevel: 3, uniqueness: 'RELATIONSHIP_GLOBAL'}) YIELD path RETURN path");
      assertThat(walked).hasSize(8).doesNotHaveDuplicates();
    }

    @Test
    void relationshipLevelWalksEachRelationshipOncePerLevel() {
      assertThat(fromS("{maxLevel: 3, uniqueness: 'RELATIONSHIP_LEVEL'}")).hasSize(14);
    }

    @Test
    void theRecentValuesActAsTheGlobalOnesOnAGraphSmallerThanTheirWindow() {
      assertThat(fromS("{maxLevel: 3, uniqueness: 'NODE_RECENT'}")).hasSize(5);
      assertThat(fromS("{maxLevel: 3, uniqueness: 'RELATIONSHIP_RECENT'}")).hasSize(9);
    }

    /**
     * Layered diamond: every order of meeting the relationships gives the same answer, so every value, in both walk orders,
     * is compared with what APOC answered, as the multiset of (length, last node).
     */
    @Test
    void everyValueOnALayeredDiamond() {
      final List<String> everyRoute = List.of("0:s", "1:a", "1:b", "2:c", "2:c", "3:d", "3:d");
      final List<String> everyNodeOnce = List.of("0:s", "1:a", "1:b", "2:c", "3:d");
      final List<String> everyRelationshipOnce = List.of("0:s", "1:a", "1:b", "2:c", "2:c", "3:d");
      for (final boolean bfs : new boolean[] { true, false }) {
        assertThat(diamond("NONE", bfs)).as("NONE bfs=%s", bfs).containsExactlyElementsOf(everyRoute);
        assertThat(diamond("NODE_PATH", bfs)).as("NODE_PATH bfs=%s", bfs).containsExactlyElementsOf(everyRoute);
        assertThat(diamond("RELATIONSHIP_PATH", bfs)).as("RELATIONSHIP_PATH bfs=%s", bfs).containsExactlyElementsOf(everyRoute);
        assertThat(diamond("NODE_GLOBAL", bfs)).as("NODE_GLOBAL bfs=%s", bfs).containsExactlyElementsOf(everyNodeOnce);
        assertThat(diamond("NODE_LEVEL", bfs)).as("NODE_LEVEL bfs=%s", bfs).containsExactlyElementsOf(everyNodeOnce);
        assertThat(diamond("NODE_RECENT", bfs)).as("NODE_RECENT bfs=%s", bfs).containsExactlyElementsOf(everyNodeOnce);
        assertThat(diamond("RELATIONSHIP_GLOBAL", bfs)).as("RELATIONSHIP_GLOBAL bfs=%s", bfs)
            .containsExactlyElementsOf(everyRelationshipOnce);
        assertThat(diamond("RELATIONSHIP_LEVEL", bfs)).as("RELATIONSHIP_LEVEL bfs=%s", bfs)
            .containsExactlyElementsOf(everyRelationshipOnce);
        assertThat(diamond("RELATIONSHIP_RECENT", bfs)).as("RELATIONSHIP_RECENT bfs=%s", bfs)
            .containsExactlyElementsOf(everyRelationshipOnce);
      }
    }

    @Test
    void uniquenessWorksWithTheLevelBounds() {
      assertThat(lengthAndEnd(FROM_V + "CALL path.expandConfig(s, {relationshipFilter: 'R>', uniqueness: 'NODE_GLOBAL', minLevel: 2}) "
          + "YIELD path RETURN path")).containsExactly("2:c", "3:d");
      assertThat(lengthAndEnd(FROM_V + "CALL path.expandConfig(s, {relationshipFilter: 'R>', uniqueness: 'NODE_LEVEL', minLevel: 2, "
          + "maxLevel: 2}) YIELD path RETURN path")).containsExactly("2:c");
    }

    // ── A node denied on its first arrival is still consumed by a node-global uniqueness ──

    @Test
    void aNodeGlobalWalkConsumesANodeEvenWhenTheFilterTurnsItAway() {
      final String sequence = "{sequence: 'S,R>,T,R>,M,R>', maxLevel: 3";
      // m is first met one hop from s, where the sequence wants a T: turned away, and so consumed. Reached again two hops from s,
      // where the sequence wants an M, it is no longer a new node. APOC answers this way
      assertThat(pathsOf(SEQUENCE_START + "CALL path.expandConfig(s, " + sequence + ", uniqueness: 'NODE_GLOBAL'}) YIELD path RETURN path"))
          .containsExactlyInAnyOrder("s", "s-[R>]-t");
      assertThat(pathsOf(SEQUENCE_START + "CALL path.spanningTree(s, " + sequence + "}) YIELD path RETURN path"))
          .containsExactlyInAnyOrder("s", "s-[R>]-t");
      final List<String> nodes = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          SEQUENCE_START + "CALL path.subgraphNodes(s, " + sequence + "}) YIELD node RETURN node")) {
        while (rs.hasNext())
          nodes.add(((Vertex) rs.next().getProperty("node")).getString("name"));
      }
      assertThat(nodes).containsExactlyInAnyOrder("s", "t");
      // Walked as trails, the same graph reaches m through t
      assertThat(pathsOf(SEQUENCE_START + "CALL path.expandConfig(s, " + sequence + "}) YIELD path RETURN path"))
          .containsExactlyInAnyOrder("s", "s-[R>]-t", "s-[R>]-t-[R>]-m");
    }

    // ── Self-loops ────────────────────────────────────────────────────────────

    @Test
    void aSelfLoopIsOneRelationshipWhicheverWayItIsFollowed() {
      for (final String filter : new String[] { "'L'", "'L>'", "'<L'" })
        assertThat(fromD("{maxLevel: 2, relationshipFilter: " + filter + "}")).as(filter).containsExactly("d", "d-[L>]-d");
      // Not walked twice in a row: it is the relationship the walk arrived by
      assertThat(fromD("{maxLevel: 3, uniqueness: 'NONE', relationshipFilter: 'L'}")).containsExactly("d", "d-[L>]-d");
      assertThat(fromD("{maxLevel: 3, uniqueness: 'NODE_LEVEL', relationshipFilter: 'L'}")).containsExactly("d", "d-[L>]-d");
      assertThat(fromD("{maxLevel: 3, uniqueness: 'RELATIONSHIP_GLOBAL', relationshipFilter: 'L'}")).containsExactly("d", "d-[L>]-d");
      assertThat(fromD("{maxLevel: 3, uniqueness: 'RELATIONSHIP_LEVEL', relationshipFilter: 'L'}")).containsExactly("d", "d-[L>]-d");
      // A node-global walk has seen d already
      assertThat(fromD("{maxLevel: 3, uniqueness: 'NODE_GLOBAL', relationshipFilter: 'L'}")).containsExactly("d");
    }

    @Test
    void aSelfLoopIsNotReportedTwiceWhenBothDirectionsAreFollowed() {
      assertThat(fromD("{maxLevel: 2}")).containsExactlyInAnyOrder("d", "d-[L>]-d", "d-[R<]-c", "d-[R>]-s", "d-[L>]-d-[R<]-c",
          "d-[L>]-d-[R>]-s", "d-[R<]-c-[R<]-a", "d-[R<]-c-[R<]-b", "d-[R>]-s-[R>]-a", "d-[R>]-s-[R>]-b", "d-[R<]-c-[R2<]-a");
      assertThat(fromD("{maxLevel: 2, relationshipFilter: '>'}")).containsExactlyInAnyOrder("d", "d-[L>]-d", "d-[R>]-s",
          "d-[L>]-d-[R>]-s", "d-[R>]-s-[R>]-a", "d-[R>]-s-[R>]-b");
      assertThat(fromD("{maxLevel: 2, relationshipFilter: '<'}")).containsExactlyInAnyOrder("d", "d-[L>]-d", "d-[R<]-c",
          "d-[L>]-d-[R<]-c", "d-[R<]-c-[R<]-a", "d-[R<]-c-[R<]-b", "d-[R<]-c-[R2<]-a");
    }

    // ── Bounds ────────────────────────────────────────────────────────────────

    @Test
    void aNegativeMaxLevelOrLimitIsNoBound() {
      // APOC's own default for both is -1
      assertThat(fromD("{maxLevel: -1}")).hasSize(120);
      assertThat(fromD("{maxLevel: 2, limit: -1}")).hasSize(11);
      assertThat(fromD("{maxLevel: 3, limit: 3}")).hasSize(3).startsWith("d");
      assertThat(fromD("{maxLevel: 3, limit: 0}")).isEmpty();
      assertThat(fromD("{maxLevel: 0}")).containsExactly("d");
      assertThat(fromD("{maxLevel: 1, minLevel: 1}")).containsExactlyInAnyOrder("d-[L>]-d", "d-[R<]-c", "d-[R>]-s");
      assertThat(fromD("{maxLevel: 1, minLevel: -3}")).hasSize(4);
    }

    @Test
    void limitStopsTheWalkWithoutBuildingTheRest() {
      // A trail walk of this graph is exponential in the depth: the limit has to end the walk, not trim its answer
      assertThat(fromD("{maxLevel: 1000, limit: 5}")).hasSize(5);
    }

    // ── optional ──────────────────────────────────────────────────────────────

    @Test
    void optionalYieldsOneNullPathWhenTheWalkAnswersNothing() {
      assertThat(fromD("{minLevel: 9}")).isEmpty();
      try (final ResultSet rs = database.query("opencypher",
          FROM_D + "CALL path.expandConfig(s, {minLevel: 9, optional: true}) YIELD path RETURN path")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat((Object) rs.next().getProperty("path")).isNull();
        assertThat(rs.hasNext()).isFalse();
      }
      // A walk that answers something has no null row
      assertThat(fromD("{maxLevel: 2, optional: true}")).hasSize(11).doesNotContainNull();
    }

    @Test
    void optionalYieldsOneNullRowFromTheOtherProcedures() {
      try (final ResultSet rs = database.query("opencypher",
          FROM_D + "CALL path.spanningTree(s, {minLevel: 1, maxLevel: 0, optional: true}) YIELD path RETURN path")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat((Object) rs.next().getProperty("path")).isNull();
        assertThat(rs.hasNext()).isFalse();
      }
      try (final ResultSet rs = database.query("opencypher",
          FROM_D + "CALL path.subgraphNodes(s, {minLevel: 1, maxLevel: 0, optional: true}) YIELD node RETURN node")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat((Object) rs.next().getProperty("node")).isNull();
        assertThat(rs.hasNext()).isFalse();
      }
      // subgraphAll has lists to answer with, so it has no null row
      try (final ResultSet rs = database.query("opencypher",
          FROM_D + "CALL path.subgraphAll(s, {minLevel: 1, maxLevel: 0, optional: true}) YIELD nodes, relationships "
              + "RETURN nodes, relationships")) {
        final Result row = rs.next();
        assertThat((List<?>) row.getProperty("nodes")).isEmpty();
        assertThat((List<?>) row.getProperty("relationships")).isEmpty();
        assertThat(rs.hasNext()).isFalse();
      }
    }

    // ── path.expand ───────────────────────────────────────────────────────────

    @Test
    void thePositionalFormWalksTrailsBreadthFirstAsApocDoes() {
      final List<String> expected = new ArrayList<>(TRAILS_FROM_S_UP_TO_3);
      expected.remove("s");
      assertThat(pathsOf(FROM_S + "CALL path.expand(s, null, null, 1, 3) YIELD path RETURN path"))
          .containsExactlyInAnyOrderElementsOf(expected);
      assertThat(pathsOf(FROM_S + "CALL path.expand(s, 'R>', '', 0, 5) YIELD path RETURN path")).hasSize(11);
    }

    // ── The procedures that fix the uniqueness ────────────────────────────────

    @Test
    void spanningTreeAndTheSubgraphsAlwaysWalkNodeGlobal() {
      // APOC overwrites the option with NODE_GLOBAL in these three, so a value is accepted and changes nothing
      assertThat(pathsOf(FROM_S + "CALL path.spanningTree(s, {uniqueness: 'NONE'}) YIELD path RETURN path")).hasSize(5);
      assertThat(pathsOf(FROM_S + "CALL path.spanningTree(s, {uniqueness: 'RELATIONSHIP_PATH', maxLevel: 3}) YIELD path RETURN path"))
          .hasSize(5);
      try (final ResultSet rs = database.query("opencypher",
          FROM_S + "CALL path.subgraphNodes(s, {uniqueness: 'NONE'}) YIELD node RETURN count(node) AS n")) {
        assertThat((Number) rs.next().getProperty("n")).isEqualTo(5L);
      }
      try (final ResultSet rs = database.query("opencypher",
          FROM_S + "CALL path.subgraphAll(s, {uniqueness: 'NONE'}) YIELD nodes RETURN size(nodes) AS n")) {
        assertThat((Number) rs.next().getProperty("n")).isEqualTo(5L);
      }
    }

    // ── Refusals ──────────────────────────────────────────────────────────────

    @Test
    void theValueIsCaseInsensitive() {
      assertThat(fromS("{maxLevel: 3, uniqueness: 'node_path'}")).containsExactlyInAnyOrderElementsOf(NODE_PATHS_FROM_S_UP_TO_3);
      assertThat(fromS("{maxLevel: 3, uniqueness: 'Node_Path'}")).containsExactlyInAnyOrderElementsOf(NODE_PATHS_FROM_S_UP_TO_3);
    }

    @Test
    void anUnknownValueIsRefusedInEveryProcedureThatReadsIt() {
      // APOC falls back to its default for a value it does not know; an answer the caller did not ask for is worse than an error
      final String values = "NODE_GLOBAL, NODE_LEVEL, NODE_PATH, NODE_RECENT, RELATIONSHIP_GLOBAL, RELATIONSHIP_LEVEL, "
          + "RELATIONSHIP_PATH, RELATIONSHIP_RECENT, NONE";
      assertThatThrownBy(() -> fromS("{uniqueness: 'BOGUS'}"))
          .hasStackTraceContaining("path.expandconfig(): uniqueness 'BOGUS' is not one of " + values);
      assertThatThrownBy(() -> pathsOf(FROM_S + "CALL path.spanningTree(s, {uniqueness: 'BOGUS'}) YIELD path RETURN path"))
          .hasStackTraceContaining("path.spanningtree(): uniqueness 'BOGUS' is not one of " + values);
      assertThatThrownBy(() -> database.query("opencypher",
          FROM_S + "CALL path.subgraphNodes(s, {uniqueness: 'BOGUS'}) YIELD node RETURN node").hasNext())
          .hasStackTraceContaining("path.subgraphnodes(): uniqueness 'BOGUS' is not one of " + values);
      assertThatThrownBy(() -> database.query("opencypher",
          FROM_S + "CALL path.subgraphAll(s, {uniqueness: 'BOGUS'}) YIELD nodes RETURN nodes").hasNext())
          .hasStackTraceContaining("path.subgraphall(): uniqueness 'BOGUS' is not one of " + values);
      assertThatThrownBy(() -> fromS("{uniqueness: 5}")).hasStackTraceContaining("uniqueness must be a string, got Long");
    }

    // ── The window of the recent values ───────────────────────────────────────

    @Test
    void theRecentValuesRememberTheTenThousandMostRecentEntities() {
      final PathUniqueness.Admission nodes = PathUniqueness.NODE_RECENT.newAdmission(false, null);
      final RID first = new RID(1, 0);
      assertThat(nodes.admit(null, null, first, 1)).isTrue();
      // The first entity and 9999 more fill the window
      for (int i = 1; i < 10_000; i++)
        assertThat(nodes.admit(null, null, new RID(1, i), 1)).isTrue();
      assertThat(nodes.admit(null, null, first, 1)).as("first is the oldest of the 10000 entities").isFalse();
      // One more entity pushes the oldest out of the window
      assertThat(nodes.admit(null, null, new RID(1, 10_000), 1)).isTrue();
      assertThat(nodes.admit(null, null, first, 1)).as("first has left the window").isTrue();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private List<String> fromS(final String config) {
      return pathsOf(FROM_S + "CALL path.expandConfig(s, " + config + ") YIELD path RETURN path");
    }

    private List<String> fromD(final String config) {
      return pathsOf(FROM_D + "CALL path.expandConfig(s, " + config + ") YIELD path RETURN path");
    }

    /** Sorted "length:lastNode" of every path the walk of the layered diamond returns. */
    private List<String> diamond(final String uniqueness, final boolean bfs) {
      final List<String> ends = lengthAndEnd(FROM_V + "CALL path.expandConfig(s, {relationshipFilter: 'R>', uniqueness: '" + uniqueness
          + "', bfs: " + bfs + "}) YIELD path RETURN path");
      Collections.sort(ends);
      return ends;
    }

    private List<String> lengthAndEnd(final String query) {
      final List<String> ends = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final TraversalPath path = rs.next().getProperty("path");
          ends.add(path.length() + ":" + path.getEndVertex().getString("name"));
        }
      }
      Collections.sort(ends);
      return ends;
    }

    /** Each path as "s-[R>]-a-[R2<]-c": the direction is the one the walk crossed the relationship in. */
    private List<String> pathsOf(final String query) {
      final List<String> paths = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final TraversalPath path = rs.next().getProperty("path");
          if (path == null) {
            paths.add(null);
            continue;
          }
          final List<Vertex> vertices = path.getVertices();
          final List<Edge> edges = path.getEdges();
          final StringBuilder text = new StringBuilder(vertices.getFirst().getString("name"));
          for (int i = 0; i < edges.size(); i++) {
            final Edge edge = edges.get(i);
            final boolean forward = edge.getOut().equals(vertices.get(i).getIdentity());
            text.append("-[").append(edge.getTypeName()).append(forward ? ">" : "<").append("]-")
                .append(vertices.get(i + 1).getString("name"));
          }
          paths.add(text.toString());
        }
      }
      return paths;
    }

    private static int depthOf(final String path) {
      return path.split("]-", -1).length - 1;
    }

    private static String endOf(final String path) {
      return path.substring(path.lastIndexOf('-') + 1);
    }

    /** The identity of the relationship each non-trivial path ends with: the relationships a walk crossed, one entry per path. */
    private List<RID> lastRelationships(final String query) {
      final List<RID> relationships = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final TraversalPath path = rs.next().getProperty("path");
          if (path.length() > 0)
            relationships.add(path.getEdges().getLast().getIdentity());
        }
      }
      return relationships;
    }
  }

  /**
   * Issue #10049: with APOC's default uniqueness a walk returns every trail, and the trails of a graph with cycles or several
   * routes to a node are exponential in the depth. The walk is therefore a plain iterator: a {@code limit} ends it, the command
   * timeout ends it, and the branches it holds are charged to the query's heap, so a walk that cannot finish fails the query
   * instead of the server.
   * <p>
   * The graph is a chain of 40 diamonds, {@code j0 -> (top|bottom) -> j1 -> (top|bottom) -> j2 ...}: 2^40 trails from
   * {@code j0} to {@code j40}, which nothing enumerates.
   */
  @Nested
  class ExpansionLimits {
    private static final int DIAMONDS = 40;

    private static final String FROM_START = "MATCH (s:J {name: 'j0'}) ";

    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue10049-path-expansion-limits");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("J");
      database.getSchema().createVertexType("Z");
      database.getSchema().createEdgeType("E");
      database.transaction(() -> {
        MutableVertex junction = database.newVertex("J").set("name", "j0").save();
        for (int i = 0; i < DIAMONDS; i++) {
          final MutableVertex top = database.newVertex("J").set("name", "t" + i).save();
          final MutableVertex bottom = database.newVertex("J").set("name", "b" + i).save();
          final MutableVertex next = database.newVertex("J").set("name", "j" + (i + 1)).save();
          junction.newEdge("E", top).save();
          junction.newEdge("E", bottom).save();
          top.newEdge("E", next).save();
          bottom.newEdge("E", next).save();
          junction = next;
        }
        // A node no trail reaches, for a walk that is asked for paths ending there
        database.newVertex("Z").set("name", "unreachable").save();
      });
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
        database.drop();
      }
    }

    @Test
    @Timeout(120)
    void aLimitEndsAWalkWhoseAnswerNothingCouldEnumerate() {
      assertThat(count("{relationshipFilter: 'E>', limit: 5}")).isEqualTo(5);
      assertThat(count("{relationshipFilter: 'E>', limit: 5, bfs: false}")).isEqualTo(5);
      assertThat(count("{relationshipFilter: 'E>', limit: 5, uniqueness: 'NODE_PATH', bfs: false}")).isEqualTo(5);
    }

    @Test
    @Timeout(120)
    void theNodeGlobalWalkOfTheSameGraphIsNotExponential() {
      // Each of the 121 nodes once
      assertThat(count("{relationshipFilter: 'E>', uniqueness: 'NODE_GLOBAL'}")).isEqualTo(3 * DIAMONDS + 1);
    }

    @Test
    @Timeout(120)
    void theCommandTimeoutEndsAWalkThatFindsNoPath() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 500L);
      // Asked for the paths that end at a node no trail reaches, the depth-first walk meets 2^40 trails and returns none: it
      // spends its whole time inside one call, so only a check inside the walk can stop it
      assertThatThrownBy(() -> count("{relationshipFilter: 'E>', bfs: false, endNodes: [z]}", ", (z:Z {name: 'unreachable'})"))
          .satisfies(e -> assertThat(causeOfType(e)).as("got: %s", e).isNotNull());
    }

    @Test
    @Timeout(120)
    void aWalkTooWideForTheHeapFailsTheQueryNotTheServer() {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 1000L);
      // The frontier of the trail walk doubles with every diamond
      assertThatThrownBy(() -> count("{relationshipFilter: 'E>'}")).hasStackTraceContaining("in-heap path expansion")
          .hasStackTraceContaining("arcadedb.queryMaxHeapElementsAllowedPerOp");
      // The depth-first walk holds one branch per level of the path it is on, and the node-global one two nodes per level
      assertThat(count("{relationshipFilter: 'E>', uniqueness: 'NODE_GLOBAL'}")).isEqualTo(3 * DIAMONDS + 1);
      assertThat(count("{relationshipFilter: 'E>', limit: 10, bfs: false}")).isEqualTo(10);
    }

    @Test
    @Timeout(120)
    void closingTheStreamGivesBackTheHeapOfAWalkThatWasNotReadToTheEnd() {
      final long previous = GlobalConfiguration.QUERY_MAX_HEAP_RAM.getValueAsLong();
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L << 30);
      try {
        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        final Vertex start = database.query("sql", "SELECT FROM J WHERE name = 'j0'").next().getVertex().get();

        try (final Stream<Result> rows = new PathExpandConfig().execute(new Object[] { start, Map.of("relationshipFilter", "E>") }, null,
            context)) {
          final Iterator<Result> iterator = rows.iterator();
          // The frontier of the trail walk doubles with every diamond: a few hundred paths in, it holds a few hundred branches
          for (int i = 0; i < 1000; i++)
            iterator.next();
          assertThat(context.getQueryHeapTracker().getUsedBytes()).as("the walk holds its frontier").isGreaterThan(0L);
        }
        assertThat(context.getQueryHeapTracker().getUsedBytes()).as("closing the stream gave it back").isZero();
      } finally {
        GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(previous);
      }
    }

    private long count(final String config) {
      return count(config, "");
    }

    private long count(final String config, final String extraMatch) {
      try (final ResultSet rs = database.query("opencypher",
          FROM_START + extraMatch + " CALL path.expandConfig(s, " + config + ") YIELD path RETURN count(path) AS n")) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }

    private static Throwable causeOfType(final Throwable throwable) {
      for (Throwable cause = throwable; cause != null; cause = cause.getCause())
        if (cause instanceof TimeoutException)
          return cause;
      return null;
    }
  }

  /**
   * Issue #10150: a {@code CALL path.expandConfig(...) ... LIMIT n} kept its frontier charged to the shared query-heap budget after
   * the ResultSet was closed, because the release hangs on the procedure's {@link Stream#close()} and nothing in the engine closed the
   * stream: {@code CallStep} turned it into an iterator and let the stream object go. The guarantee is one level up from the
   * procedure: whatever stream a procedure answers, closing the ResultSet of the query closes it, so a procedure that registers an
   * {@code onClose} cannot leak by being called from Cypher.
   * <p>
   * The graph is a chain of 40 diamonds, {@code j0 -> (top|bottom) -> j1 -> ...}: 2^40 trails from {@code j0}, so a walk read to a
   * {@code LIMIT} is far from its end and holds a frontier of thousands of branches.
   * <p>
   * The walks read what their own query holds, from the {@link QueryHeapTracker} of the query, rather than the JVM-wide
   * {@link QueryHeapBudget} counter: that one also carries the reservations of the queries other tests of the same JVM abandoned
   * without closing them, which the {@code Cleaner} of the tracker gives back whenever the garbage collector finds them, so it can
   * drop by more than the walk holds while the walk is open. The tracker is the share of the budget the query holds: the walk
   * reserves from the budget through it, and gives it back through it. The walk is {@code path.expandConfig} itself, under a
   * name of its own that hands the test the tracker of the query that called it.
   */
  @Nested
  class StreamClosedByCall extends TestHelper {
    private static final int DIAMONDS = 40;

    private static final String WALK = TrackedExpandConfig.NAME + "(s, {relationshipFilter: 'E>'}) YIELD path";

    /** The tracker of every query that called the walk. */
    private final Set<QueryHeapTracker> trackers = ConcurrentHashMap.newKeySet();

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("J");
      database.getSchema().createEdgeType("E");
      database.transaction(() -> {
        MutableVertex junction = database.newVertex("J").set("name", "j0").save();
        for (int i = 0; i < DIAMONDS; i++) {
          final MutableVertex top = database.newVertex("J").set("name", "t" + i).save();
          final MutableVertex bottom = database.newVertex("J").set("name", "b" + i).save();
          final MutableVertex next = database.newVertex("J").set("name", "j" + (i + 1)).save();
          junction.newEdge("E", top).save();
          junction.newEdge("E", bottom).save();
          top.newEdge("E", next).save();
          bottom.newEdge("E", next).save();
          junction = next;
        }
      });
      // The budget is on: the frontier of a walk is charged to it. TestHelper puts the setting back after the test
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L << 30);
      CypherProcedureRegistry.registerOrReplace(new TrackedExpandConfig(trackers));
    }

    @Override
    protected void endTest() {
      unregister(TrackedExpandConfig.NAME);
    }

    @Test
    @Timeout(120)
    void theIssueReproduction() {
      try (final ResultSet resultSet = database.query("opencypher",
          "MATCH (s:J {name: 'j0'}) CALL " + WALK + " RETURN path LIMIT 20000")) {
        assertThat(drain(resultSet)).isEqualTo(20000);
        assertWalkHoldsItsFrontier();
      }
      assertWalkGaveItsFrontierBack();
    }

    @Test
    @Timeout(120)
    void aStandaloneCallGivesItBackToo() {
      final Vertex start = database.query("sql", "SELECT FROM J WHERE name = 'j0'").next().getVertex().get();
      try (final ResultSet resultSet = database.query("opencypher",
          "CALL " + TrackedExpandConfig.NAME + "($s, {relationshipFilter: 'E>'}) YIELD path RETURN path LIMIT 20000", Map.of("s", start))) {
        assertThat(drain(resultSet)).isEqualTo(20000);
        assertWalkHoldsItsFrontier();
      }
      assertWalkGaveItsFrontierBack();
    }

    @Test
    @Timeout(120)
    void aConsumerThatStopsBeforeAnyLimitGivesItBack() {
      try (final ResultSet resultSet = database.query("opencypher", "MATCH (s:J {name: 'j0'}) CALL " + WALK + " RETURN path")) {
        for (int i = 0; i < 20000; i++)
          resultSet.next();
        assertWalkHoldsItsFrontier();
      }
      assertWalkGaveItsFrontierBack();
    }

    @Test
    @Timeout(120)
    void everyWalkOfAChainedCallGivesItsFrontierBack() {
      try (final ResultSet resultSet = database.query("opencypher",
          "MATCH (s:J) WHERE s.name IN ['j0', 'j10', 'j20'] CALL " + WALK + " RETURN path LIMIT 20000")) {
        assertThat(drain(resultSet)).isEqualTo(20000);
        assertWalkHoldsItsFrontier();
      }
      // Every walk the chained call opened charged the same tracker, the one of the query: it holds nothing once they are all closed
      assertWalkGaveItsFrontierBack();
    }

    /** Whatever stream a procedure answers is closed with the ResultSet: the guarantee does not depend on the procedure. */
    @Test
    void theStreamOfAnyProcedureIsClosedWithTheResultSet() {
      final AtomicInteger closed = new AtomicInteger();
      final List<CypherProcedure> procedures = registerProcedures(closed);
      try {
        try (final ResultSet resultSet = database.query("opencypher", "CALL issue10150.endless() YIELD n RETURN n LIMIT 3")) {
          assertThat(drain(resultSet)).isEqualTo(3);
          assertThat(closed.get()).as("the stream is still open while the ResultSet is").isZero();
        }
        assertThat(closed.get()).isEqualTo(1);

        // The same when the stream is read to the end: closed once, by whoever gets there first
        closed.set(0);
        try (final ResultSet resultSet = database.query("opencypher", "CALL issue10150.finite() YIELD n RETURN n")) {
          assertThat(drain(resultSet)).isEqualTo(5);
        }
        assertThat(closed.get()).isEqualTo(1);

        // And one per invocation of a chained call: every input row runs the procedure and owns its stream
        closed.set(0);
        try (final ResultSet resultSet = database.query("opencypher",
            "UNWIND [1, 2, 3] AS i CALL issue10150.endless() YIELD n RETURN i, n LIMIT 4")) {
          assertThat(drain(resultSet)).isEqualTo(4);
        }
        assertThat(closed.get()).isEqualTo(3);
      } finally {
        unregister(procedures);
      }
    }

    @Test
    void aFailureWhileReadingTheStreamClosesItToo() {
      final AtomicInteger closed = new AtomicInteger();
      final List<CypherProcedure> procedures = registerProcedures(closed);
      try {
        assertThatThrownBy(() -> {
          try (final ResultSet resultSet = database.query("opencypher", "CALL issue10150.failing() YIELD n RETURN n")) {
            drain(resultSet);
          }
        }).hasStackTraceContaining("boom");
        assertThat(closed.get()).isEqualTo(1);
      } finally {
        unregister(procedures);
      }
    }

    /** The control: the walk really holds its frontier, past what a query may buffer before it reserves from the shared budget. */
    private void assertWalkHoldsItsFrontier() {
      assertThat(trackers).as("every walk of a query charges the tracker of that query").hasSize(1);
      final QueryHeapTracker tracker = trackers.iterator().next();
      assertThat(tracker.getReservedBytes()).as("the walk holds its frontier while the rows are read").isGreaterThan(0L);
    }

    private void assertWalkGaveItsFrontierBack() {
      assertThat(trackers).as("every walk of a query charges the tracker of that query").hasSize(1);
      final QueryHeapTracker tracker = trackers.iterator().next();
      assertThat(tracker.getUsedBytes()).as("closing the ResultSet gave the frontier back").isZero();
      assertThat(tracker.getReservedBytes()).as("and its share of the shared budget with it").isZero();
    }

    private static int drain(final ResultSet resultSet) {
      int rows = 0;
      while (resultSet.hasNext()) {
        resultSet.next();
        ++rows;
      }
      return rows;
    }

    private static List<CypherProcedure> registerProcedures(final AtomicInteger closed) {
      final List<CypherProcedure> procedures = List.of(
          new ClosingProcedure("issue10150.endless", closed, () -> Stream.iterate(0, n -> n + 1).map(StreamClosedByCall::row)),
          new ClosingProcedure("issue10150.finite", closed, () -> IntStream.range(0, 5).boxed().map(StreamClosedByCall::row)),
          new ClosingProcedure("issue10150.failing", closed, () -> Stream.iterate(0, n -> n + 1).map(n -> {
            if (n == 2)
              throw new IllegalStateException("boom");
            return row(n);
          })));
      for (final CypherProcedure procedure : procedures)
        CypherProcedureRegistry.registerOrReplace(procedure);
      return procedures;
    }

    private static void unregister(final List<CypherProcedure> procedures) {
      for (final CypherProcedure procedure : procedures)
        unregister(procedure.getName());
    }

    /** From both registries: registering a Cypher procedure also registers it in the unified one, which would keep it. */
    private static void unregister(final String name) {
      CypherProcedureRegistry.unregister(name);
      ProcedureRegistry.unregister(name);
    }

    private static Result row(final int n) {
      final ResultInternal row = new ResultInternal();
      row.setProperty("n", n);
      return row;
    }

    /**
     * {@code path.expandConfig} under a name of its own, recording the tracker of the query that calls it: the walk, its stream and
     * the {@code onClose} that gives its frontier back are those of the procedure.
     */
    private static final class TrackedExpandConfig extends PathExpandConfig {
      static final String NAME = "issue10150.expandConfig";

      private final Set<QueryHeapTracker> trackers;

      TrackedExpandConfig(final Set<QueryHeapTracker> trackers) {
        this.trackers = trackers;
      }

      @Override
      public String getName() {
        return NAME;
      }

      @Override
      public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
        trackers.add(context.getQueryHeapTracker());
        return super.execute(args, inputRow, context);
      }
    }

    /** A procedure whose stream registers an {@code onClose} that counts, as {@code AbstractPathProcedure.pathRows} does to give its heap back. */
    private static final class ClosingProcedure implements CypherProcedure {
      private final String               name;
      private final AtomicInteger        closed;
      private final Supplier<Stream<Result>> rows;

      ClosingProcedure(final String name, final AtomicInteger closed, final Supplier<Stream<Result>> rows) {
        this.name = name;
        this.closed = closed;
        this.rows = rows;
      }

      @Override
      public String getName() {
        return name;
      }

      @Override
      public int getMinArgs() {
        return 0;
      }

      @Override
      public int getMaxArgs() {
        return 0;
      }

      @Override
      public String getDescription() {
        return "Test procedure whose stream counts its close()";
      }

      @Override
      public List<String> getYieldFields() {
        return List.of("n");
      }

      @Override
      public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
        return rows.get().onClose(closed::incrementAndGet);
      }
    }
  }
}
