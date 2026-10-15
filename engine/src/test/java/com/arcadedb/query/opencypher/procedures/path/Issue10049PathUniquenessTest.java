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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10049PathUniquenessTest {
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
