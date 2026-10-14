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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10049PathNodeListsTest {
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
