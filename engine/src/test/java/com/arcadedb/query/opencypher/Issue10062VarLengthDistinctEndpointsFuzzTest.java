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
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10062, differential check: on many small random multigraphs (self loops, parallel edges, antiparallel edges, cycles, two
 * relationship types) every eligible variable-length shape must answer exactly what the path enumeration answers. The reference is
 * the same statement made ineligible by a named path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10062VarLengthDistinctEndpointsFuzzTest extends TestHelper {
  private static final int    VERTICES = 7;
  private static final int    GRAPHS   = 40;
  private static final String ENDPOINTS = "DISTINCT ENDPOINTS";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V").createProperty("id", Type.INTEGER);
      database.getSchema().createEdgeType("E");
      database.getSchema().createEdgeType("F");
    });
  }

  @Test
  void everyEligibleShapeAnswersWhatPathEnumerationAnswers() {
    final String[][] directions = { { "-", "->" }, { "<-", "-" }, { "-", "-" } };
    final String[] modes = { "", "WALK ", "ACYCLIC ", "TRAIL " };
    int compared = 0;
    int undirectedTrails = 0;
    for (int seed = 0; seed < GRAPHS; seed++) {
      buildGraph(seed);
      for (final String[] dir : directions)
        for (final String mode : modes)
          for (final int min : new int[] { 0, 1 })
            for (final int max : new int[] { 1, 2, 3, 4 })
              for (final String types : new String[] { ":E", ":E|F" }) {
                final String hop = dir[0] + "[" + types + "*" + min + ".." + max + "]" + dir[1];
                for (final String tail : new String[] {
                    "RETURN DISTINCT a.id AS s, b.id AS v ORDER BY s, v",
                    "RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s",
                    "RETURN a.id AS s, sum(DISTINCT b.id) AS t, min(b.id) AS lo, max(b.id) AS hi ORDER BY s",
                    "RETURN DISTINCT b.id AS v ORDER BY v" }) {
                  final String eligible = "MATCH " + mode + "(a:V)" + hop + "(b:V) " + tail;
                  final String reference = "MATCH p = " + mode + "(a:V)" + hop + "(b:V) " + tail;
                  if (!planOf(eligible).contains(ENDPOINTS))
                    continue;
                  compared++;
                  if (dir[0].equals("-") && dir[1].equals("-") && min == 1 && !mode.equals("WALK ") && !mode.equals("ACYCLIC "))
                    undirectedTrails++;
                  assertThat(rows(eligible)).as("seed=%d %s", seed, eligible).isEqualTo(rows(reference));
                }
              }
    }
    assertThat(compared).isGreaterThan(1000);
    // the undirected trails with a lower bound of 1 decide the start from a cycle search of their own: they must be among the shapes compared
    assertThat(undirectedTrails).isGreaterThan(200);
  }

  private void buildGraph(final int seed) {
    final Random random = new Random(seed);
    database.transaction(() -> {
      database.command("sql", "DELETE FROM V");
      final List<MutableVertex> vertices = new ArrayList<>();
      for (int i = 0; i < VERTICES; i++)
        vertices.add(database.newVertex("V").set("id", i).save());
      final int edges = VERTICES + random.nextInt(VERTICES * 2);
      for (int e = 0; e < edges; e++) {
        final MutableVertex from = vertices.get(random.nextInt(VERTICES));
        // a fifth of the edges are self loops, the rest may repeat or run both ways between a pair
        final MutableVertex to = random.nextInt(5) == 0 ? from : vertices.get(random.nextInt(VERTICES));
        from.newEdge(random.nextInt(3) == 0 ? "F" : "E", to).save();
      }
    });
  }

  private String planOf(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", "EXPLAIN " + query)) {
      return String.valueOf(resultSet.next().<Object>getProperty("executionPlan"));
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
    return rows;
  }
}
