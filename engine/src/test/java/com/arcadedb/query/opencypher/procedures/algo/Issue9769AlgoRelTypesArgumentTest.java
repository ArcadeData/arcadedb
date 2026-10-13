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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression tests for issue #9769: {@code algo.mst} and {@code algo.apsp} took {@code (weightProperty, relTypes)} while
 * every other weighted {@code algo.*} procedure takes {@code (relTypes, weightProperty)}, and a call written in the
 * family's order walked an empty graph and answered no rows. Both now take the family order, and a relationship-type
 * argument naming no edge type is refused by every {@code algo.*} procedure instead of selecting nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9769AlgoRelTypesArgumentTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-9769-reltypes");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N").createProperty("k", Type.INTEGER);
    database.getSchema().createEdgeType("R").createProperty("w", Type.DOUBLE);
    database.getSchema().createEdgeType("S");

    // Barbell: triangle {0,1,2}, connector 2->3, triangle {3,4,5}. The connector is the heaviest edge of its triangle
    // neighbours so a weighted MST differs from an unweighted one
    database.transaction(() -> {
      final List<MutableVertex> v = new ArrayList<>();
      for (int i = 0; i < 6; i++)
        v.add(database.newVertex("N").set("k", i).save());
      final int[][] edges = { { 0, 1, 1 }, { 1, 2, 1 }, { 2, 0, 5 }, { 2, 3, 2 }, { 3, 4, 1 }, { 4, 5, 1 }, { 5, 3, 5 } };
      for (final int[] e : edges)
        v.get(e[0]).newEdge("R", v.get(e[1]), "w", (double) e[2]).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void mstTakesRelTypesFirst() {
    final List<Result> rows = rows("CALL algo.mst('R', 'w') YIELD source, target, weight, totalWeight RETURN weight, totalWeight");
    assertThat(rows).hasSize(5);
    assertThat(rows.getFirst().<Double>getProperty("totalWeight")).isCloseTo(6.0, within(1e-9));

    assertThat(rows("CALL algo.mst(null, 'w') YIELD totalWeight RETURN totalWeight").getFirst().<Double>getProperty("totalWeight"))
        .isCloseTo(6.0, within(1e-9));
    assertThat(rows("CALL algo.mst('R') YIELD source RETURN source")).hasSize(5);
    assertThat(rows("CALL algo.mst() YIELD source RETURN source")).hasSize(5);
  }

  @Test
  void mstInTheOldOrderIsRefusedNotAnsweredEmpty() {
    assertThatThrownBy(() -> rows("CALL algo.mst('w', 'R') YIELD source RETURN source"))
        .hasStackTraceContaining("algo.mst(): relationship type 'w' does not exist")
        // 'w' is declared on R, so the message points at the swapped order
        .hasStackTraceContaining("'w' is a property of the edge type 'R': the relationship types come first, then the weight property");
    assertThatThrownBy(() -> rows("CALL algo.mst('w') YIELD source RETURN source"))
        .hasStackTraceContaining("relationship type 'w' does not exist");
  }

  @Test
  void apspTakesRelTypesFirst() {
    // Weighted 0 -> 5: 0-2 (via 1, cost 2), 2-3 (2), 3-5 (via 4, cost 2) = 6
    final List<Result> rows = rows("CALL algo.apsp('R', 'w') YIELD source, target, distance "
        + "WITH source, target, distance WHERE source.k = 0 AND target.k = 5 RETURN distance");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Double>getProperty("distance")).isCloseTo(6.0, within(1e-9));

    assertThatThrownBy(() -> rows("CALL algo.apsp('w', 'R') YIELD distance RETURN distance"))
        .hasStackTraceContaining("algo.apsp(): relationship type 'w' does not exist");
  }

  @Test
  void oldOrderWithoutWeightIsRefusedToo() {
    // (null, 'R') used to mean "unit weights over R"; read in the new order R would be the weight property of every type
    assertThatThrownBy(() -> rows("CALL algo.apsp(null, 'R') YIELD distance RETURN distance"))
        .hasStackTraceContaining("algo.apsp(): 'R' is a relationship type, not a weight property");
    assertThatThrownBy(() -> rows("CALL algo.mst(null, 'R') YIELD source RETURN source"))
        .hasStackTraceContaining("algo.mst(): 'R' is a relationship type, not a weight property");
  }

  @Test
  void unknownRelTypeIsRefusedAcrossTheFamily() {
    // The ordinary typo, previously as silent as the swapped order
    assertThatThrownBy(() -> rows("CALL algo.bridges('RR') YIELD source RETURN source"))
        .hasStackTraceContaining("algo.bridges(): relationship type 'RR' does not exist");
    assertThatThrownBy(() -> rows("CALL algo.wcc('RR') YIELD node RETURN node"))
        .hasStackTraceContaining("relationship type 'RR' does not exist");
    // One bad name in a list is enough
    assertThatThrownBy(() -> rows("CALL algo.bridges(['R', 'RR']) YIELD source RETURN source"))
        .hasStackTraceContaining("relationship type 'RR' does not exist");
    assertThatThrownBy(() -> rows("CALL algo.bridges('R,RR') YIELD source RETURN source"))
        .hasStackTraceContaining("relationship type 'RR' does not exist");
    // A vertex type is not a relationship type
    assertThatThrownBy(() -> rows("CALL algo.bridges('N') YIELD source RETURN source"))
        .hasStackTraceContaining("'N' is not a relationship type, it is a vertex type");
    // A node-label filter is checked the same way, against the vertex types
    assertThatThrownBy(() -> rows("CALL algo.graphSummary(null, 'NOPE') YIELD nodeCount RETURN nodeCount"))
        .hasStackTraceContaining("algo.graphSummary(): node label 'NOPE' does not exist");
    assertThatThrownBy(() -> rows("CALL algo.graphSummary(null, 'R') YIELD nodeCount RETURN nodeCount"))
        .hasStackTraceContaining("'R' is not a node label, it is an edge type");
    assertThat(rows("CALL algo.graphSummary(null, 'N') YIELD nodeCount RETURN nodeCount").getFirst().<Number>getProperty("nodeCount")
        .intValue()).isEqualTo(6);
    // The types to skip are names too: a misspelt one would skip nothing
    assertThatThrownBy(() -> rows("MATCH (a:N {k: 0}), (b:N {k: 5}) "
        + "CALL algo.allSimplePaths(a, b, 'R', 5, {skipRelTypes: ['SS']}) YIELD path RETURN path"))
        .hasStackTraceContaining("algo.allsimplepaths(): relationship type 'SS' does not exist");
    assertThatThrownBy(() -> rows("MATCH (a:N {k: 0}), (b:N {k: 5}) "
        + "CALL algo.allSimplePaths(a, b, 'R', 5, {skipVertexTypes: 'NN'}) YIELD path RETURN path"))
        .hasStackTraceContaining("algo.allsimplepaths(): node label 'NN' does not exist");
    assertThatThrownBy(() -> rows("MATCH (a:N {k: 0}), (b:N {k: 5}) "
        + "CALL algo.allSimplePaths(a, b, 'R', 5, {skipVertexTypes: 'R'}) YIELD path RETURN path"))
        .hasStackTraceContaining("'R' is not a node label, it is an edge type");
    // The single-type procedures too
    assertThatThrownBy(() -> rows("MATCH (a:N {k: 0}), (b:N {k: 5}) CALL algo.dijkstra(a, b, 'RR', 'w') YIELD path RETURN path"))
        .hasStackTraceContaining("algo.dijkstra(): relationship type 'RR' does not exist");
  }

  @Test
  void existingAndAbsentFiltersStillWork() {
    assertThat(rows("CALL algo.bridges('R') YIELD source RETURN source")).hasSize(1);
    assertThat(rows("CALL algo.bridges(['R', 'S']) YIELD source RETURN source")).hasSize(1);
    // An existing type with no edges is a legitimate empty filter, not an error
    assertThat(rows("CALL algo.bridges('S') YIELD source RETURN source")).isEmpty();
    // A blank entry in a list is dropped like an empty part of 'R,,S'; a list of blanks means every type
    assertThat(rows("CALL algo.bridges(['R', '']) YIELD source RETURN source")).hasSize(1);
    assertThat(rows("CALL algo.bridges(['', ' ']) YIELD source RETURN source")).hasSize(1);
    // Blank and null mean every type
    assertThat(rows("CALL algo.bridges('') YIELD source RETURN source")).hasSize(1);
    assertThat(rows("CALL algo.bridges(null) YIELD source RETURN source")).hasSize(1);
  }

  private List<Result> rows(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }
}
