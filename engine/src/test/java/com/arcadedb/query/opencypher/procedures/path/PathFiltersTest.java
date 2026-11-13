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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code relationshipFilter}, {@code labelFilter} and node-list options of the {@code path.*} procedures.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PathFiltersTest {
  /**
   * Issue #10025: the {@code path.*} procedures read {@code relationshipFilter} and {@code labelFilter} in APOC's syntax -
   * direction markers, relationship and label sequences, the label operators {@code + - / >}, {@code sequence},
   * {@code filterStartNode}, {@code beginSequenceAtStart} - and refuse a name that is not a type of the schema instead of
   * answering an empty result.
   * <p>
   * Every expected answer below was produced by Neo4j 5.26 with APOC 5.26.30 on the same graph, built by the same
   * {@code CREATE}: the graph is a tree, so APOC's default uniqueness and the node-global walks here agree on it.
   * <pre>
   *   (A:City)-[:ROAD]-&gt;(B:City)-[:ROAD]-&gt;(C:City)-[:RAIL]-&gt;(T:Town)-[:ROAD]-&gt;(P:Port:City)
   *   (X:Hub)-[:RAIL]-&gt;(B)
   * </pre>
   */
  @Nested
  class FilterSyntax {
    private static final String START = "MATCH (a:City {name: 'A'}) ";

    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue10025-path-filters");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.transaction(() -> database.command("opencypher", """
          CREATE (a:City {name:'A'}), (b:City {name:'B'}), (c:City {name:'C'}), (t:Town {name:'T'}), (x:Hub {name:'X'}),
                 (p:Port:City {name:'P'})
          CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), (c)-[:RAIL]->(t), (x)-[:RAIL]->(b), (t)-[:ROAD]->(p)
          """).close());
    }

    @AfterEach
    void teardown() {
      if (database != null)
        database.drop();
    }

    // ── The issue's reproduction ──────────────────────────────────────────────

    @Test
    void issueTableTwoHopPathsFromA() {
      assertThat(expandConfig("{relationshipFilter: 'ROAD', minLevel: 2, maxLevel: 2}")).containsExactly("A,B,C");
      assertThat(expandConfig("{relationshipFilter: 'ROAD>', minLevel: 2, maxLevel: 2}")).containsExactly("A,B,C");
      assertThat(expandConfig("{relationshipFilter: '<ROAD', minLevel: 2, maxLevel: 2}")).isEmpty();
      assertThat(expandConfig("{relationshipFilter: '>', minLevel: 2, maxLevel: 2}")).containsExactly("A,B,C");
    }

    @Test
    void unknownRelationshipTypeIsRefused() {
      assertThatThrownBy(() -> expandConfig("{relationshipFilter: 'ROADS', minLevel: 2, maxLevel: 2}"))
          .hasStackTraceContaining("path.expandconfig(): relationship type 'ROADS' does not exist");
      assertThatThrownBy(() -> expandConfig("{relationshipFilter: 'ROAD>|<RAILS'}"))
          .hasStackTraceContaining("relationship type 'RAILS' does not exist");
      assertThatThrownBy(() -> expandConfig("{relationshipFilter: 'City>'}"))
          .hasStackTraceContaining("'City' is not a relationship type, it is a vertex type");
      assertThatThrownBy(() -> paths(START + "CALL path.expand(a, 'ROADS', null, 1, 2) YIELD path RETURN path"))
          .hasStackTraceContaining("path.expand(): relationship type 'ROADS' does not exist");
      assertThatThrownBy(() -> nodes("CALL path.subgraphNodes(a, {relationshipFilter: 'ROADS'}) YIELD node RETURN node"))
          .hasStackTraceContaining("path.subgraphnodes(): relationship type 'ROADS' does not exist");
      assertThatThrownBy(() -> nodes("CALL path.subgraphAll(a, {relationshipFilter: 'ROADS'}) YIELD nodes UNWIND nodes AS node RETURN node"))
          .hasStackTraceContaining("path.subgraphall(): relationship type 'ROADS' does not exist");
      assertThatThrownBy(() -> paths(START + "CALL path.spanningTree(a, {relationshipFilter: 'ROADS'}) YIELD path RETURN path"))
          .hasStackTraceContaining("path.spanningtree(): relationship type 'ROADS' does not exist");
    }

    @Test
    void bothDirectionMarkersAreRefused() {
      assertThatThrownBy(() -> expandConfig("{relationshipFilter: '<ROAD>'}"))
          .hasStackTraceContaining("'<ROAD>' carries both direction markers: write 'ROAD' to follow it in both directions");
    }

    // ── relationshipFilter ────────────────────────────────────────────────────

    @Test
    void relationshipDirections() {
      assertThat(expandConfig("{relationshipFilter: 'ROAD>'}")).containsExactly("A", "A,B", "A,B,C");
      assertThat(expandConfig("{relationshipFilter: '<ROAD'}")).containsExactly("A");
      assertThat(expandConfig("{relationshipFilter: '>'}")).containsExactly("A", "A,B", "A,B,C", "A,B,C,T", "A,B,C,T,P");
      assertThat(expandConfig("{relationshipFilter: 'ROAD>|<RAIL'}")).containsExactlyInAnyOrder("A", "A,B", "A,B,C", "A,B,X");
      // The list form ArcadeDB accepted before is the same set of alternatives
      assertThat(expandConfig("{relationshipFilter: ['ROAD>', '<RAIL']}")).containsExactlyInAnyOrder("A", "A,B", "A,B,C", "A,B,X");
      // Empty alternatives are dropped, a blank filter follows everything
      assertThat(expandConfig("{relationshipFilter: 'ROAD>|', maxLevel: 1}")).containsExactly("A", "A,B");
      assertThat(expandConfig("{relationshipFilter: '', labelFilter: ''}")).hasSize(6);
    }

    @Test
    void relationshipSequence() {
      assertThat(expandConfig("{relationshipFilter: 'ROAD>,RAIL'}")).containsExactlyInAnyOrder("A", "A,B", "A,B,X");
      // beginSequenceAtStart only shifts the label sequence: the first hop still takes the first relationship filter
      assertThat(expandConfig("{relationshipFilter: 'ROAD>,RAIL', beginSequenceAtStart: false}"))
          .containsExactlyInAnyOrder("A", "A,B", "A,B,X");
      assertThat(expandConfig("{relationshipFilter: 'RAIL,ROAD>', beginSequenceAtStart: false}")).containsExactly("A");
      assertThat(expandConfig("{relationshipFilter: 'ROAD>,RAIL', bfs: false}")).containsExactlyInAnyOrder("A", "A,B", "A,B,X");
    }

    @Test
    void relationshipDirectionsInEveryProcedure() {
      assertThat(paths(START + "CALL path.expand(a, 'ROAD>', null, 1, 3) YIELD path RETURN path")).containsExactly("A,B", "A,B,C");
      assertThat(paths(START + "CALL path.expand(a, 'ROAD>,RAIL', '', 0, 3) YIELD path RETURN path"))
          .containsExactlyInAnyOrder("A", "A,B", "A,B,X");
      assertThat(expandConfig("{relationshipFilter: 'ROAD>', bfs: false}")).containsExactly("A", "A,B", "A,B,C");
      assertThat(paths(START + "CALL path.spanningTree(a, {relationshipFilter: 'ROAD>'}) YIELD path RETURN path"))
          .containsExactly("A", "A,B", "A,B,C");
      assertThat(paths(START + "CALL path.spanningTree(a, {relationshipFilter: 'ROAD>,RAIL'}) YIELD path RETURN path"))
          .containsExactly("A", "A,B", "A,B,X");
      assertThat(nodes("CALL path.subgraphNodes(a, {relationshipFilter: 'ROAD>'}) YIELD node RETURN node")).containsExactly("A", "B", "C");
      assertThat(nodes("CALL path.subgraphNodes(a, {relationshipFilter: 'ROAD>,RAIL'}) YIELD node RETURN node"))
          .containsExactly("A", "B", "X");
      // The YIELD nodes form walks adjacency entries rather than edges: the direction must hold there too
      assertThat(nodes("CALL path.subgraphAll(a, {relationshipFilter: 'ROAD>'}) YIELD nodes UNWIND nodes AS node RETURN node"))
          .containsExactly("A", "B", "C");
      assertThat(nodes("CALL path.subgraphAll(a, {relationshipFilter: '<RAIL|ROAD>'}) YIELD nodes UNWIND nodes AS node RETURN node"))
          .containsExactlyInAnyOrder("A", "B", "C", "X");
      assertThat(relationshipCount("{relationshipFilter: 'ROAD>'}")).isEqualTo(2);
    }

    // ── labelFilter ───────────────────────────────────────────────────────────

    @Test
    void labelOperators() {
      assertThat(expandConfig("{labelFilter: '+City'}")).containsExactly("A", "A,B", "A,B,C");
      assertThat(expandConfig("{labelFilter: 'City'}")).containsExactly("A", "A,B", "A,B,C");
      assertThat(expandConfig("{labelFilter: '-Hub'}")).containsExactly("A", "A,B", "A,B,C", "A,B,C,T", "A,B,C,T,P");
      assertThat(expandConfig("{labelFilter: '/Town'}")).containsExactly("A,B,C,T");
      assertThat(expandConfig("{labelFilter: '>Town'}")).containsExactly("A,B,C,T");
      assertThat(expandConfig("{labelFilter: '>City'}")).containsExactly("A,B", "A,B,C", "A,B,C,T,P");
      assertThat(expandConfig("{labelFilter: '+City|>Town'}")).containsExactly("A,B,C,T");
      assertThat(expandConfig("{labelFilter: '+City|/Town'}")).containsExactly("A,B,C,T");
      assertThat(expandConfig("{labelFilter: '+Port'}")).containsExactly("A");
      assertThat(expandConfig("{labelFilter: '-City'}")).containsExactly("A");
      assertThat(expandConfig("{labelFilter: '>Town|/Port'}")).containsExactly("A,B,C,T", "A,B,C,T,P");
      assertThat(expandConfig("{labelFilter: '/City'}")).containsExactly("A,B");
      // Each entry carries its own operator: 'Town' after '-Port' is allowed, not denied
      assertThat(expandConfig("{labelFilter: '+City|-Port|Town'}")).containsExactly("A", "A,B", "A,B,C", "A,B,C,T");
      assertThat(expandConfig("{labelFilter: '+City|Town'}")).containsExactly("A", "A,B", "A,B,C", "A,B,C,T", "A,B,C,T,P");
      assertThat(expandConfig("{labelFilter: '*'}")).containsExactlyInAnyOrder("A", "A,B", "A,B,X", "A,B,C", "A,B,C,T", "A,B,C,T,P");
    }

    @Test
    void aNodeWithSeveralLabelsCarriesEachOfThem() {
      // P is (:Port:City): before #10025 the filter compared the label with the type name and matched neither
      assertThat(expandConfig("{labelFilter: '>Port'}")).containsExactly("A,B,C,T,P");
      assertThat(expandConfig("{labelFilter: '>City:Port'}")).containsExactly("A,B,C,T,P");
      assertThat(expandConfig("{labelFilter: 'City:Port'}")).containsExactly("A");
      assertThat(expandConfig("{labelFilter: '-Town|City:Port'}")).containsExactly("A");
    }

    @Test
    void filterStartNode() {
      assertThat(expandConfig("{labelFilter: '/City', filterStartNode: true}")).containsExactly("A");
      assertThat(expandConfig("{labelFilter: '>City', filterStartNode: true}")).containsExactly("A", "A,B", "A,B,C", "A,B,C,T,P");
      assertThat(expandConfig("{labelFilter: '+Town', filterStartNode: true}")).isEmpty();
      assertThat(expandConfig("{labelFilter: '-City', filterStartNode: true, minLevel: 1}")).isEmpty();
      assertThat(expandConfig("{labelFilter: '/City', maxLevel: 0, filterStartNode: true}")).containsExactly("A");
    }

    @Test
    void minLevelAgainstTerminationAndEndNodes() {
      // A termination node closer than minLevel does not stop the walk
      assertThat(expandConfig("{labelFilter: '/City', minLevel: 2}")).containsExactly("A,B,C");
      assertThat(expandConfig("{labelFilter: '/City', minLevel: 2, maxLevel: 2}")).containsExactly("A,B,C");
      assertThat(expandConfig("{labelFilter: '/City', minLevel: 1}")).containsExactly("A,B");
      assertThat(expandConfig("{labelFilter: '/Town', minLevel: 4}")).isEmpty();
      assertThat(expandConfig("{labelFilter: '>Town', minLevel: 4}")).isEmpty();
      // A denied node is denied at every level
      assertThat(expandConfig("{labelFilter: '-City', minLevel: 2}")).isEmpty();
      assertThat(expandConfig("{labelFilter: '+Town', minLevel: 2}")).isEmpty();
    }

    @Test
    void labelSequence() {
      assertThat(expandConfig("{labelFilter: 'City,Town'}")).containsExactly("A");
      assertThat(expandConfig("{labelFilter: '*,City'}")).containsExactlyInAnyOrder("A", "A,B", "A,B,X", "A,B,C");
      assertThat(expandConfig("{labelFilter: 'City,*'}")).containsExactly("A", "A,B", "A,B,C", "A,B,C,T", "A,B,C,T,P");
      assertThat(expandConfig("{labelFilter: '*,City', beginSequenceAtStart: false}"))
          .containsExactly("A", "A,B", "A,B,C", "A,B,C,T", "A,B,C,T,P");
      assertThat(expandConfig("{labelFilter: '*,>City'}")).containsExactly("A,B");
      assertThat(expandConfig("{labelFilter: '>City,*'}")).containsExactly("A,B,C", "A,B,C,T,P");
      assertThat(expandConfig("{labelFilter: '-City,*'}")).containsExactlyInAnyOrder("A", "A,B", "A,B,X");
      assertThat(expandConfig("{labelFilter: '*,/Hub'}")).isEmpty();
      assertThat(paths(START + "CALL path.expand(a, '', 'City,*', 0, 4) YIELD path RETURN path"))
          .containsExactly("A", "A,B", "A,B,C", "A,B,C,T", "A,B,C,T,P");
    }

    @Test
    void sequenceConfiguration() {
      assertThat(expandConfig("{sequence: '*, ROAD>, City, RAIL'}")).containsExactlyInAnyOrder("A", "A,B", "A,B,X");
      assertThat(expandConfig("{sequence: 'City, ROAD>, *, RAIL'}")).containsExactly("A", "A,B");
      assertThat(expandConfig("{sequence: 'Town, ROAD>, *, RAIL'}")).containsExactly("A", "A,B");
      assertThat(expandConfig("{sequence: 'Town, ROAD>, *, RAIL', filterStartNode: true}")).isEmpty();
      assertThat(expandConfig("{sequence: 'ROAD>, *, RAIL, City', beginSequenceAtStart: false}")).containsExactly("A", "A,B");
      // sequence takes precedence over both filters
      assertThat(expandConfig("{relationshipFilter: 'ROAD>', labelFilter: 'Town', sequence: '*, >'}"))
          .containsExactly("A", "A,B", "A,B,C", "A,B,C,T", "A,B,C,T,P");
      assertThatThrownBy(() -> expandConfig("{sequence: 'City, ROAD>, Town'}"))
          .hasStackTraceContaining("must alternate label and relationship filters");
    }

    @Test
    void labelFiltersInEveryProcedure() {
      assertThat(paths(START + "CALL path.expand(a, null, '/Town', 0, 5) YIELD path RETURN path")).containsExactly("A,B,C,T");
      assertThat(paths(START + "CALL path.expand(a, null, '>Town', 0, 5) YIELD path RETURN path")).containsExactly("A,B,C,T");
      assertThat(expandConfig("{labelFilter: '/Town', bfs: false}")).containsExactly("A,B,C,T");
      assertThat(expandConfig("{labelFilter: '>City', bfs: false}")).containsExactlyInAnyOrder("A,B", "A,B,C", "A,B,C,T,P");
      assertThat(paths(START + "CALL path.spanningTree(a, {labelFilter: '>Town'}) YIELD path RETURN path")).containsExactly("A,B,C,T");

      assertThat(nodes("CALL path.subgraphNodes(a, {labelFilter: '>Town'}) YIELD node RETURN node")).containsExactly("T");
      assertThat(nodes("CALL path.subgraphNodes(a, {labelFilter: '/Town'}) YIELD node RETURN node")).containsExactly("T");
      assertThat(nodes("CALL path.subgraphNodes(a, {labelFilter: '-Town'}) YIELD node RETURN node")).containsExactlyInAnyOrder("A", "B", "X", "C");
      assertThat(nodes("CALL path.subgraphNodes(a, {labelFilter: '*,City'}) YIELD node RETURN node")).containsExactlyInAnyOrder("A", "B", "X", "C");
      assertThat(nodes("CALL path.subgraphNodes(a, {labelFilter: '>Town', filterStartNode: true}) YIELD node RETURN node"))
          .containsExactly("T");
      assertThat(nodes("CALL path.subgraphNodes(a, {labelFilter: '>City', minLevel: 1}) YIELD node RETURN node"))
          .containsExactly("B", "C", "P");
      assertThatThrownBy(() -> nodes("CALL path.subgraphNodes(a, {labelFilter: '-Town', minLevel: 2}) YIELD node RETURN node"))
          .hasStackTraceContaining("path.subgraphnodes(): minLevel can only be 0 or 1");
      assertThatThrownBy(() -> nodes("CALL path.subgraphNodes(a, {minLevel: 0.5}) YIELD node RETURN node"))
          .hasStackTraceContaining("path.subgraphnodes(): minLevel can only be 0 or 1, got 0.5");
      assertThat(nodes("CALL path.subgraphNodes(a, {relationshipFilter: 'ROAD>', minLevel: 1.0}) YIELD node RETURN node"))
          .containsExactly("B", "C");

      // subgraphAll reports the relationships between the nodes it reports, so a walked-through node takes its edges along
      assertThat(nodes("CALL path.subgraphAll(a, {labelFilter: '>Town'}) YIELD nodes UNWIND nodes AS node RETURN node"))
          .containsExactly("T");
      assertThat(relationshipCount("{labelFilter: '>Town'}")).isZero();
      assertThat(nodes("CALL path.subgraphAll(a, {labelFilter: '+City'}) YIELD nodes UNWIND nodes AS node RETURN node"))
          .containsExactly("A", "B", "C");
      assertThat(relationshipCount("{labelFilter: '+City'}")).isEqualTo(2);
    }

    @Test
    void unknownLabelsAndMalformedEntriesAreRefused() {
      assertThatThrownBy(() -> expandConfig("{labelFilter: 'Nope'}"))
          .hasStackTraceContaining("path.expandconfig(): node label 'Nope' does not exist");
      assertThatThrownBy(() -> expandConfig("{labelFilter: '+City|>Towns'}"))
          .hasStackTraceContaining("node label 'Towns' does not exist");
      assertThatThrownBy(() -> expandConfig("{labelFilter: 'ROAD'}"))
          .hasStackTraceContaining("'ROAD' is not a node label, it is an edge type");
      assertThatThrownBy(() -> expandConfig("{labelFilter: '+'}"))
          .hasStackTraceContaining("label filter entry '+' names no label");
      assertThatThrownBy(() -> paths(START + "CALL path.expand(a, null, 'Nope', 0, 2) YIELD path RETURN path"))
          .hasStackTraceContaining("path.expand(): node label 'Nope' does not exist");
    }

    @Test
    void configMustBeAMap() {
      assertThatThrownBy(() -> paths(START + "CALL path.expandConfig(a, 'ROAD>') YIELD path RETURN path"))
          .hasStackTraceContaining("path.expandconfig(): config must be a map, got String");
      assertThatThrownBy(() -> expandConfig("{filterStartNode: 'yes'}"))
          .hasStackTraceContaining("filterStartNode must be a boolean");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private List<String> expandConfig(final String config) {
      return paths(START + "CALL path.expandConfig(a, " + config + ") YIELD path RETURN path");
    }

    /** The node names of every path the query returns, each path as "A,B,C", in the order returned. */
    private List<String> paths(final String query) {
      final List<String> paths = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final TraversalPath path = rs.next().getProperty("path");
          final StringJoiner names = new StringJoiner(",");
          for (final Vertex node : path.getVertices())
            names.add(node.getString("name"));
          paths.add(names.toString());
        }
      }
      return paths;
    }

    private List<String> nodes(final String call) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", START + call)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          names.add(((Vertex) row.getProperty("node")).getString("name"));
        }
      }
      return names;
    }

    private int relationshipCount(final String config) {
      try (final ResultSet rs = database.query("opencypher",
          START + "CALL path.subgraphAll(a, " + config + ") YIELD relationships RETURN relationships")) {
        final List<?> relationships = rs.next().getProperty("relationships");
        for (final Object relationship : relationships)
          assertThat(relationship).isInstanceOf(Edge.class);
        return relationships.size();
      }
    }
  }

  /**
   * Issue #10049: {@code endNodes}, {@code terminatorNodes}, {@code allowlistNodes} / {@code whitelistNodes} and
   * {@code denylistNodes} / {@code blacklistNodes} of the {@code path.*} procedures name nodes where {@code >}, {@code /},
   * {@code +} and {@code -} of the label filter name labels, and they combine with the label filter: a node is an answer, and
   * the walk goes on past it, only if every filter says so.
   * <p>
   * Every expected answer below was produced by Neo4j 5.26 with APOC 5.26.30 on the same graph, built by the same
   * {@code CREATE}:
   * <pre>
   *   (A:City)-[:ROAD]-&gt;(B:City)-[:ROAD]-&gt;(C:City)-[:RAIL]-&gt;(T:Town)-[:ROAD]-&gt;(P:Port:City)
   *   (X:Hub)-[:RAIL]-&gt;(B)
   * </pre>
   */
  @Nested
  class NodeLists {
    private static final String NODES = "MATCH (a:City {name: 'A'}), (b:City {name: 'B'}), (c:City {name: 'C'}), (t:Town {name: 'T'}), "
        + "(x:Hub {name: 'X'}), (p:Port:City {name: 'P'}) ";

    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue10049-path-node-lists");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.transaction(() -> database.command("opencypher", """
          CREATE (a:City {name:'A'}), (b:City {name:'B'}), (c:City {name:'C'}), (t:Town {name:'T'}), (x:Hub {name:'X'}),
                 (p:Port:City {name:'P'})
          CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), (c)-[:RAIL]->(t), (x)-[:RAIL]->(b), (t)-[:ROAD]->(p)
          """).close());
    }

    @AfterEach
    void teardown() {
      if (database != null)
        database.drop();
    }

    // ── endNodes ──────────────────────────────────────────────────────────────

    @Test
    void endNodesOnlyEndThePathsThatStopAtThem() {
      assertThat(expand("{endNodes: [c]}")).containsExactly("A,B,C");
      assertThat(expand("{endNodes: [c, p]}")).containsExactlyInAnyOrder("A,B,C", "A,B,C,T,P");
      // The walk goes on past an end node: C is an answer, and so is the P behind it
      assertThat(expand("{endNodes: [p]}")).containsExactly("A,B,C,T,P");
      // The start node is not filtered unless asked to be, so it is not an end node of the answer either
      assertThat(expand("{endNodes: [a]}")).isEmpty();
      assertThat(expand("{endNodes: [a], filterStartNode: true}")).containsExactly("A");
      assertThat(expand("{endNodes: [b], minLevel: 2}")).isEmpty();
    }

    // ── terminatorNodes ───────────────────────────────────────────────────────

    @Test
    void terminatorNodesEndThePathAndStopTheWalk() {
      assertThat(expand("{terminatorNodes: [c]}")).containsExactly("A,B,C");
      assertThat(expand("{terminatorNodes: [b, p]}")).containsExactly("A,B");
      assertThat(expand("{terminatorNodes: [a]}")).isEmpty();
      assertThat(expand("{terminatorNodes: [a], filterStartNode: true}")).containsExactly("A");
      // A terminator closer than minLevel does not stop the walk, and is not an answer
      assertThat(expand("{terminatorNodes: [b], minLevel: 2}")).isEmpty();
    }

    @Test
    void endAndTerminatorNodesTogether() {
      assertThat(expand("{endNodes: [c], terminatorNodes: [t]}")).containsExactlyInAnyOrder("A,B,C", "A,B,C,T");
      // The terminator stops the walk before it reaches the end node
      assertThat(expand("{endNodes: [t], terminatorNodes: [b]}")).containsExactly("A,B");
      // A node that is both ends the path
      assertThat(expand("{terminatorNodes: [c], endNodes: [c]}")).containsExactly("A,B,C");
    }

    // ── allowlistNodes ────────────────────────────────────────────────────────

    @Test
    void allowlistNodesAreTheOnlyNodesAPathMayCross() {
      assertThat(expand("{allowlistNodes: [a, b, c]}")).containsExactlyInAnyOrder("A", "A,B", "A,B,C");
      // The start node is not filtered unless asked to be
      assertThat(expand("{allowlistNodes: [b, c]}")).containsExactlyInAnyOrder("A", "A,B", "A,B,C");
      assertThat(expand("{allowlistNodes: [b, c], filterStartNode: true}")).isEmpty();
      // The deprecated name
      assertThat(expand("{whitelistNodes: [a, b]}")).containsExactlyInAnyOrder("A", "A,B");
    }

    @Test
    void endAndTerminatorNodesAreAllowedWhateverTheAllowlistSays() {
      assertThat(expand("{endNodes: [c], allowlistNodes: [a, b]}")).containsExactly("A,B,C");
      assertThat(expand("{terminatorNodes: [c], allowlistNodes: [a, b]}")).containsExactly("A,B,C");
    }

    // ── denylistNodes ─────────────────────────────────────────────────────────

    @Test
    void denylistNodesAreNeverCrossed() {
      assertThat(expand("{denylistNodes: [c]}")).containsExactlyInAnyOrder("A", "A,B", "A,B,X");
      assertThat(expand("{blacklistNodes: [c]}")).containsExactlyInAnyOrder("A", "A,B", "A,B,X");
      // The start node is not filtered unless asked to be
      assertThat(expand("{denylistNodes: [a]}")).hasSize(6);
      assertThat(expand("{denylistNodes: [a], filterStartNode: true}")).isEmpty();
    }

    @Test
    void aDeniedNodeIsNotRescuedByTheEndOrTerminatorLists() {
      assertThat(expand("{endNodes: [c], denylistNodes: [c]}")).isEmpty();
      assertThat(expand("{terminatorNodes: [c], denylistNodes: [c]}")).isEmpty();
    }

    // ── With the label filter ─────────────────────────────────────────────────

    @Test
    void theNodeListsCombineWithTheLabelFilter() {
      assertThat(expand("{endNodes: [c], labelFilter: '-Town'}")).containsExactly("A,B,C");
      // Both want an end node, and not the same one
      assertThat(expand("{endNodes: [c], labelFilter: '>Port'}")).isEmpty();
      assertThat(expand("{terminatorNodes: [b], labelFilter: '/Town'}")).isEmpty();
      assertThat(expand("{allowlistNodes: [a, b, c], labelFilter: '-City'}")).containsExactly("A");
      assertThat(expand("{denylistNodes: [t], labelFilter: '+City'}")).containsExactlyInAnyOrder("A", "A,B", "A,B,C");
    }

    @Test
    void emptyListsFilterNothing() {
      assertThat(expand("{endNodes: [], terminatorNodes: [], allowlistNodes: [], denylistNodes: []}")).hasSize(6);
      assertThat(expand("{endNodes: null, whitelistNodes: null}")).hasSize(6);
    }

    @Test
    void aSingleNodeIsAListOfOne() {
      assertThat(expand("{endNodes: c}")).containsExactly("A,B,C");
    }

    // ── The other procedures ──────────────────────────────────────────────────

    @Test
    void spanningTreeReadsTheNodeLists() {
      assertThat(spanningTree("{endNodes: [c]}")).containsExactly("A,B,C");
      assertThat(spanningTree("{endNodes: [c, p]}")).containsExactlyInAnyOrder("A,B,C", "A,B,C,T,P");
      assertThat(spanningTree("{terminatorNodes: [b]}")).containsExactly("A,B");
      assertThat(spanningTree("{terminatorNodes: [b, p], minLevel: 1}")).containsExactly("A,B");
      assertThat(spanningTree("{allowlistNodes: [a, b, c]}")).containsExactlyInAnyOrder("A", "A,B", "A,B,C");
      assertThat(spanningTree("{allowlistNodes: [b, c], filterStartNode: true}")).isEmpty();
      assertThat(spanningTree("{denylistNodes: [c]}")).containsExactlyInAnyOrder("A", "A,B", "A,B,X");
      assertThat(spanningTree("{blacklistNodes: [t]}")).containsExactlyInAnyOrder("A", "A,B", "A,B,C", "A,B,X");
      assertThat(spanningTree("{whitelistNodes: [a, b, x]}")).containsExactlyInAnyOrder("A", "A,B", "A,B,X");
      assertThat(spanningTree("{endNodes: [c], terminatorNodes: [t]}")).containsExactlyInAnyOrder("A,B,C", "A,B,C,T");
      assertThat(spanningTree("{endNodes: [a]}")).isEmpty();
      assertThat(spanningTree("{endNodes: [a], filterStartNode: true}")).containsExactly("A");
      assertThat(spanningTree("{minLevel: 1, terminatorNodes: [b]}")).containsExactly("A,B");
      assertThat(spanningTree("{minLevel: 1, endNodes: [b, c]}")).containsExactlyInAnyOrder("A,B", "A,B,C");
    }

    @Test
    void subgraphNodesReadsTheNodeLists() {
      assertThat(subgraphNodes("{endNodes: [c]}")).containsExactly("C");
      assertThat(subgraphNodes("{endNodes: [c, p]}")).containsExactlyInAnyOrder("C", "P");
      assertThat(subgraphNodes("{terminatorNodes: [b]}")).containsExactly("B");
      assertThat(subgraphNodes("{terminatorNodes: [b, p], minLevel: 1}")).containsExactly("B");
      assertThat(subgraphNodes("{allowlistNodes: [a, b, c]}")).containsExactlyInAnyOrder("A", "B", "C");
      assertThat(subgraphNodes("{allowlistNodes: [b, c], filterStartNode: true}")).isEmpty();
      assertThat(subgraphNodes("{denylistNodes: [c]}")).containsExactlyInAnyOrder("A", "B", "X");
      assertThat(subgraphNodes("{denylistNodes: [a], filterStartNode: true}")).isEmpty();
      assertThat(subgraphNodes("{blacklistNodes: [t]}")).containsExactlyInAnyOrder("A", "B", "C", "X");
      assertThat(subgraphNodes("{whitelistNodes: [a, b, x]}")).containsExactlyInAnyOrder("A", "B", "X");
      assertThat(subgraphNodes("{endNodes: [c], terminatorNodes: [t]}")).containsExactlyInAnyOrder("C", "T");
      assertThat(subgraphNodes("{endNodes: [a]}")).isEmpty();
      assertThat(subgraphNodes("{endNodes: [a], filterStartNode: true}")).containsExactly("A");
      assertThat(subgraphNodes("{minLevel: 1, terminatorNodes: [b]}")).containsExactly("B");
      assertThat(subgraphNodes("{minLevel: 1, endNodes: [b, c]}")).containsExactlyInAnyOrder("B", "C");
    }

    @Test
    void subgraphAllReadsTheNodeListsAndAnswersTheRelationshipsBetweenTheNodesItKeeps() {
      try (final ResultSet rs = database.query("opencypher",
          NODES + "CALL path.subgraphAll(a, {endNodes: [b, c]}) YIELD nodes, relationships RETURN nodes, relationships")) {
        final Result row = rs.next();
        final List<?> nodes = row.getProperty("nodes");
        assertThat(nodes).hasSize(2);
        assertThat(((List<?>) row.getProperty("relationships"))).hasSize(1);
      }
    }

    // ── Refusals ──────────────────────────────────────────────────────────────

    @Test
    void aListOfSomethingElseThanNodesIsRefused() {
      assertThatThrownBy(() -> expand("{endNodes: [1]}")).hasStackTraceContaining("path.expandconfig(): endNodes must contain nodes, got Long");
      assertThatThrownBy(() -> expand("{allowlistNodes: 'C'}"))
          .hasStackTraceContaining("path.expandconfig(): allowlistNodes must be a list of nodes, got String");
      assertThatThrownBy(() -> expand("{denylistNodes: [c, 'C']}"))
          .hasStackTraceContaining("path.expandconfig(): denylistNodes must contain nodes, got String");
      assertThatThrownBy(() -> spanningTree("{terminatorNodes: [1]}"))
          .hasStackTraceContaining("path.spanningtree(): terminatorNodes must contain nodes, got Long");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private List<String> expand(final String config) {
      return paths(NODES + "CALL path.expandConfig(a, " + config + ") YIELD path RETURN path");
    }

    private List<String> spanningTree(final String config) {
      return paths(NODES + "CALL path.spanningTree(a, " + config + ") YIELD path RETURN path");
    }

    private List<String> subgraphNodes(final String config) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", NODES + "CALL path.subgraphNodes(a, " + config + ") YIELD node RETURN node")) {
        while (rs.hasNext())
          names.add(((Vertex) rs.next().getProperty("node")).getString("name"));
      }
      return names;
    }

    /** The node names of every path the query returns, each path as "A,B,C". */
    private List<String> paths(final String query) {
      final List<String> paths = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final TraversalPath path = rs.next().getProperty("path");
          final StringJoiner names = new StringJoiner(",");
          for (final Vertex node : path.getVertices())
            names.add(node.getString("name"));
          paths.add(names.toString());
        }
      }
      return paths;
    }
  }

  /**
   * Parser-level checks of {@link RelationshipFilter} and {@link LabelFilter} (issue #10025): what each malformed input is
   * refused with, how the empty pieces of a filter read, and that a type filter follows the subtypes of the type it names.
   */
  @Nested
  class Parsing {
    private static final String PROC = "path.test";

    private Database database;
    private Schema   schema;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-path-filter-parsing");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      schema = database.getSchema();
      schema.createVertexType("City");
      schema.createVertexType("Capital").addSuperType("City");
      schema.createEdgeType("ROAD");
      schema.createEdgeType("HIGHWAY").addSuperType("ROAD");
      schema.createEdgeType("RAIL");
    }

    @AfterEach
    void teardown() {
      if (database != null)
        database.drop();
    }

    @Test
    void relationshipFilterDirectionsAndEmptyPieces() {
      final RelationshipFilter filter = RelationshipFilter.parse(PROC, "ROAD>|<RAIL", schema);
      assertThat(filter.typesFor(0, Vertex.DIRECTION.OUT)).containsExactly("ROAD");
      assertThat(filter.typesFor(0, Vertex.DIRECTION.IN)).containsExactly("RAIL");
      assertThat(filter.isSymmetric(0)).isFalse();

      assertThat(RelationshipFilter.parse(PROC, "ROAD", schema).isSymmetric(0)).isTrue();
      assertThat(RelationshipFilter.parse(PROC, "<", schema).typesFor(0, Vertex.DIRECTION.OUT)).isNull();
      assertThat(RelationshipFilter.parse(PROC, "<", schema).typesFor(0, Vertex.DIRECTION.IN)).isEmpty();

      // Blank, a lone pipe and null all mean "everything"
      for (final Object blank : new Object[] { null, "", "  ", "|", List.of() })
        assertThat(RelationshipFilter.parse(PROC, blank, schema)).isSameAs(RelationshipFilter.ANY);

      // An empty hop inside a sequence follows everything on that hop
      final RelationshipFilter sequence = RelationshipFilter.parse(PROC, "ROAD>,,<RAIL", schema);
      assertThat(sequence.typesFor(0, Vertex.DIRECTION.IN)).isNull();
      assertThat(sequence.typesFor(1, Vertex.DIRECTION.OUT)).isEmpty();
      assertThat(sequence.typesFor(1, Vertex.DIRECTION.IN)).isEmpty();
      assertThat(sequence.typesFor(2, Vertex.DIRECTION.IN)).containsExactly("RAIL");
      assertThat(sequence.typesFor(3, Vertex.DIRECTION.OUT)).containsExactly("ROAD");
    }

    @Test
    void relationshipFilterListFormIsOneHopOfAlternatives() {
      final RelationshipFilter filter = RelationshipFilter.parse(PROC, List.of("ROAD>", "<RAIL"), schema);
      assertThat(filter.typesFor(0, Vertex.DIRECTION.OUT)).containsExactly("ROAD");
      assertThat(filter.typesFor(0, Vertex.DIRECTION.IN)).containsExactly("RAIL");
      // A comma inside an element is not a sequence there, so it reaches the schema check as part of a name
      assertThatThrownBy(() -> RelationshipFilter.parse(PROC, List.of("ROAD>,<RAIL"), schema))
          .hasMessageContaining("does not exist");
    }

    @Test
    void relationshipFilterRefusals() {
      assertThatThrownBy(() -> RelationshipFilter.parse(PROC, "<ROAD>", schema))
          .hasMessage("path.test(): relationship filter '<ROAD>' carries both direction markers: write 'ROAD' to follow it in both directions");
      assertThatThrownBy(() -> RelationshipFilter.parse(PROC, "<>", schema))
          .hasMessageContaining("leave the filter empty");
      assertThatThrownBy(() -> RelationshipFilter.parse(PROC, Map.of("a", 1), schema))
          .hasMessage("path.test(): relationshipFilter must be a string or a list of strings, got a map");
      assertThatThrownBy(() -> RelationshipFilter.parse(PROC, "ROADS>", schema))
          .hasMessage("path.test(): relationship type 'ROADS' does not exist");
      assertThatThrownBy(() -> RelationshipFilter.parse(PROC, "City", schema))
          .hasMessage("path.test(): 'City' is not a relationship type, it is a vertex type");
    }

    @Test
    void labelFilterRefusals() {
      assertThatThrownBy(() -> parseLabels("-"))
          .hasMessage("path.test(): label filter entry '-' names no label");
      assertThatThrownBy(() -> parseLabels("City:"))
          .hasMessage("path.test(): label filter entry 'City:' has an empty label");
      assertThatThrownBy(() -> parseLabels("City:*"))
          .hasMessage("path.test(): label filter entry 'City:*' uses '*' inside a compound label: '*' stands alone");
      assertThatThrownBy(() -> parseLabels(Map.of("a", 1)))
          .hasMessage("path.test(): labelFilter must be a string or a list of strings, got a map");
      assertThatThrownBy(() -> parseLabels("City>"))
          .hasMessageContaining("ends with a direction marker: a label filter's operator ('+', '-', '/' or '>') goes before the label");
      assertThatThrownBy(() -> parseLabels("Town"))
          .hasMessage("path.test(): node label 'Town' does not exist");
      assertThatThrownBy(() -> parseLabels("ROAD"))
          .hasMessage("path.test(): 'ROAD' is not a node label, it is an edge type");
    }

    @Test
    void labelFilterBlankPiecesMeanNoFilter() {
      for (final Object blank : new Object[] { null, "", " , ", "|", List.of() })
        assertThat(parseLabels(blank).isUnfiltered()).isTrue();
      assertThat(parseLabels("City").isUnfiltered()).isFalse();
    }

    /**
     * A type filter follows the subtypes of the type it names, for relationships as for labels: an ArcadeDB type hierarchy
     * has no counterpart in Neo4j, and a query over the parent type means the whole hierarchy, as it does in MATCH.
     */
    @Test
    void typeFiltersFollowSubtypes() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("City").set("name", "A").save();
        final MutableVertex b = database.newVertex("Capital").set("name", "B").save();
        final MutableVertex c = database.newVertex("City").set("name", "C").save();
        a.newEdge("HIGHWAY", b).save();
        b.newEdge("ROAD", c).save();
      });

      assertThat(names("CALL path.subgraphNodes(a, {relationshipFilter: 'ROAD>'}) YIELD node RETURN node")).containsExactly("A", "B", "C");
      assertThat(names("CALL path.subgraphNodes(a, {relationshipFilter: 'HIGHWAY>'}) YIELD node RETURN node")).containsExactly("A", "B");
      assertThat(names("CALL path.subgraphNodes(a, {labelFilter: '>City', minLevel: 1}) YIELD node RETURN node")).containsExactly("B", "C");
      assertThat(names("CALL path.subgraphNodes(a, {labelFilter: '/Capital'}) YIELD node RETURN node")).containsExactly("B");
    }

    private LabelFilter parseLabels(final Object filter) {
      return LabelFilter.parse(PROC, filter, schema, false, true, 0);
    }

    private List<String> names(final String call) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", "MATCH (a:City {name: 'A'}) " + call)) {
        while (rs.hasNext())
          names.add(((Vertex) rs.next().getProperty("node")).getString("name"));
      }
      return names;
    }
  }
}
