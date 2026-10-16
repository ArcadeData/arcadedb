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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10152: {@code collect(DISTINCT x)} over a variable-length hop answers the same SET of values whether the hop is walked path
 * by path or with a visited set, but its list carries the order the walk met the values in, which is depth first for the first and
 * breadth first for the second. Cypher promises no order for {@code collect} without an {@code ORDER BY} (the reference
 * documentation says so), so the shortcut stays - dropping it would bring back the walk over every path for the most common way to
 * ask "everything this reaches" - and what is pinned is what the shortcut does promise:
 * <ul>
 *   <li>the values, each once, are those of the path enumeration;</li>
 *   <li>a statement that reads the ORDER of the list is not one of the shapes the shortcut takes, so it keeps the enumeration order.</li>
 * </ul>
 * The differential fuzz of issue #10062 compares the lists of a {@code collect} as sets over random multigraphs.
 * <p>
 * The graph has four vertices 0..3 and the edges 0-&gt;1, 0-&gt;2, 2-&gt;0: the list was [2, 1, 0] without a named path and [2, 0, 1] with one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10152CollectDistinctEndpointsOrderTest extends TestHelper {
  private static final String ENDPOINTS = "DISTINCT ENDPOINTS";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V").createProperty("id", Type.INTEGER);
      database.getSchema().createEdgeType("E");
      final List<MutableVertex> vertices = new ArrayList<>();
      for (int i = 0; i < 4; i++)
        vertices.add(database.newVertex("V").set("id", i).save());
      vertices.get(0).newEdge("E", vertices.get(1)).save();
      vertices.get(0).newEdge("E", vertices.get(2)).save();
      vertices.get(2).newEdge("E", vertices.get(0)).save();
    });
  }

  @Test
  void theListHoldsTheValuesOfThePathEnumerationEachOnce() {
    final String walked = "MATCH (a:V {id: 0})-[:E*1..2]->(b:V) RETURN a.id AS s, collect(DISTINCT b.id) AS l";
    final String enumerated = "MATCH p = (a:V {id: 0})-[:E*1..2]->(b:V) RETURN a.id AS s, collect(DISTINCT b.id) AS l";
    assertThat(planOf(walked)).contains(ENDPOINTS);
    assertThat(planOf(enumerated)).doesNotContain(ENDPOINTS);

    final List<Object> fromWalk = list(walked);
    assertThat(fromWalk).containsExactlyInAnyOrder(0, 1, 2);
    assertThat(fromWalk).containsExactlyInAnyOrderElementsOf(list(enumerated));
  }

  @Test
  void aStatementThatReadsTheOrderOfTheListKeepsTheEnumerationOrder() {
    for (final String projection : new String[] { "head(collect(DISTINCT b.id))", "collect(DISTINCT b.id)[0]",
        "collect(DISTINCT b.id)[0..1]", "last(collect(DISTINCT b.id))" }) {
      final String walked = "MATCH (a:V {id: 0})-[:E*1..2]->(b:V) RETURN a.id AS s, " + projection + " AS x";
      final String enumerated = "MATCH p = (a:V {id: 0})-[:E*1..2]->(b:V) RETURN a.id AS s, " + projection + " AS x";
      assertThat(planOf(walked)).as(walked).doesNotContain(ENDPOINTS);
      assertThat(rows(walked)).as(walked).isEqualTo(rows(enumerated));
    }
  }

  private String planOf(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", "EXPLAIN " + query)) {
      return String.valueOf(resultSet.next().<Object>getProperty("executionPlan"));
    }
  }

  private List<Object> list(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      final Result row = resultSet.next();
      final List<Object> values = new ArrayList<>(row.<List<Object>>getProperty("l"));
      assertThat(resultSet.hasNext()).isFalse();
      return values;
    }
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final List<String> cells = new ArrayList<>();
        for (final String name : row.getPropertyNames())
          cells.add(name + "=" + row.<Object>getProperty(name));
        rows.add(String.join(",", cells));
      }
    }
    Collections.sort(rows);
    return rows;
  }
}
