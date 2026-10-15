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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10062: a variable-length hop whose consumer cannot tell two paths between the same pair of vertices apart reports each
 * end vertex once, found with a visited set, instead of walking every path. The answers must be exactly those of the path
 * enumeration, which the same query gives once a named path makes it ineligible.
 * <p>
 * The graph is a dependency-like DAG (each package depends on the four below it, so there are many paths onto few vertices)
 * with a cycle (3 to 30 and back down), a self loop, parallel edges, a second relationship type and a property on the edges.
 */
class Issue10062VarLengthDistinctEndpointsTest extends TestHelper {
  private static final int NODES = 40;
  private static final String ENDPOINTS = "DISTINCT ENDPOINTS";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Pkg").createProperty("id", Type.INTEGER);
      database.getSchema().getType("Pkg").createProperty("grp", Type.INTEGER);
      database.getSchema().createEdgeType("DEP");
      database.getSchema().createEdgeType("OTHER");
      final List<MutableVertex> pkgs = new ArrayList<>();
      for (int i = 0; i < NODES; i++)
        pkgs.add(database.newVertex("Pkg").set("id", i).set("grp", i % 3).save());
      for (int i = 1; i < NODES; i++)
        for (int j = Math.max(0, i - 4); j < i; j++)
          pkgs.get(i).newEdge("DEP", pkgs.get(j)).set("w", (i * 7 + j) % 3).save();
      for (int i = 6; i < NODES; i += 2)
        pkgs.get(i).newEdge("OTHER", pkgs.get(i - 5)).save();
      pkgs.get(3).newEdge("DEP", pkgs.get(30)).set("w", 1).save();   // closes a cycle: 3 -> 30 -> ... -> 3
      pkgs.get(12).newEdge("DEP", pkgs.get(12)).set("w", 0).save();  // a self loop
      pkgs.get(20).newEdge("DEP", pkgs.get(19)).set("w", 2).save();  // a parallel edge
    });
    database.transaction(() -> database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Pkg", "id"));
  }

  /** The ends of a hop written in the given direction, with and without the reading that makes the hop eligible. */
  @Test
  void answersMatchPathEnumerationInEveryDirectionAndRange() {
    for (final String[] dir : new String[][] { { "-", "->" }, { "<-", "-" }, { "-", "-" } })
      for (final int min : new int[] { 0, 1 })
        for (final int max : new int[] { 1, 2, 3, 5 })
          for (final int key : new int[] { 0, 3, 5, 12, 20, 30, 39 }) {
            final String hop = dir[0] + "[:DEP*" + min + ".." + max + "]" + dir[1];
            final String eligible = "MATCH (a:Pkg)" + hop + "(b:Pkg) WHERE a.id = $key RETURN DISTINCT b.id AS v ORDER BY v";
            final String reference = "MATCH p = (a:Pkg)" + hop + "(b:Pkg) WHERE a.id = $key RETURN DISTINCT b.id AS v ORDER BY v";
            assertThat(planOf(eligible)).as(eligible).contains(ENDPOINTS);
            assertThat(planOf(reference)).as(reference).doesNotContain(ENDPOINTS);
            assertThat(column(eligible, "v", Map.of("key", key))).as("%s key=%d", hop, key)
                .isEqualTo(column(reference, "v", Map.of("key", key)));
          }
  }

  @Test
  void answersMatchWhenTheNodesAreNotLabeledSoTheStepExecutorRunsIt() {
    for (final int min : new int[] { 0, 1 })
      for (final int max : new int[] { 2, 4 })
        for (final int key : new int[] { 0, 3, 12, 30, 39 }) {
          final String hop = "-[:DEP*" + min + ".." + max + "]->";
          final String eligible = "MATCH (a {id: $key})" + hop + "(b) RETURN DISTINCT b.id AS v ORDER BY v";
          final String reference = "MATCH p = (a {id: $key})" + hop + "(b) RETURN DISTINCT b.id AS v ORDER BY v";
          assertThat(planOf(eligible)).as(eligible).contains(ENDPOINTS);
          assertThat(column(eligible, "v", Map.of("key", key))).as("%s key=%d", hop, key)
              .isEqualTo(column(reference, "v", Map.of("key", key)));
        }
  }

  @Test
  void aCycleReportsTheStartVertexOnlyWhenTheRangeAllowsIt() {
    // 3 -> 30 -> 29 -> 28 -> 27 -> 26 -> ... -> 3 is a long cycle; a self loop makes 12 reach itself in one hop
    assertThat(column("MATCH (a:Pkg)-[:DEP*1..1]->(b:Pkg) WHERE a.id = 12 RETURN DISTINCT b.id AS v ORDER BY v", "v", Map.of()))
        .contains(12);
    assertThat(column("MATCH (a:Pkg)-[:DEP*1..1]->(b:Pkg) WHERE a.id = 5 RETURN DISTINCT b.id AS v ORDER BY v", "v", Map.of()))
        .doesNotContain(5);
    assertThat(column("MATCH (a:Pkg)-[:DEP*0..1]->(b:Pkg) WHERE a.id = 5 RETURN DISTINCT b.id AS v ORDER BY v", "v", Map.of()))
        .contains(5);
    assertThat(column("MATCH (a:Pkg)-[:DEP*1..30]->(b:Pkg) WHERE a.id = 3 RETURN DISTINCT b.id AS v ORDER BY v", "v", Map.of()))
        .contains(3);
  }

  @Test
  void countsOfDistinctValuesAndGroupingKeysAreUnchanged() {
    final String keys = "WHERE a.id IN [0, 3, 12, 20, 30, 39] ";
    for (final String hop : new String[] { "-[:DEP*1..4]->", "<-[:DEP*0..3]-", "-[:DEP|OTHER*0..3]-" }) {
      for (final String[] form : new String[][] {
          { "RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s", "c" },
          { "RETURN a.id AS s, count(DISTINCT b.grp) AS c ORDER BY s", "c" },
          { "RETURN count(DISTINCT b) AS c", "c" },
          { "RETURN DISTINCT a.id AS s, b.id AS v ORDER BY s, v", "v" },
          { "RETURN DISTINCT b.grp AS v ORDER BY v", "v" } }) {
        final String eligible = "MATCH (a:Pkg)" + hop + "(b:Pkg) " + keys + form[0];
        final String reference = "MATCH p = (a:Pkg)" + hop + "(b:Pkg) " + keys + form[0];
        assertThat(planOf(eligible)).as(eligible).contains(ENDPOINTS);
        assertThat(rows(eligible)).as(eligible).isEqualTo(rows(reference));
      }
    }
  }

  @Test
  void aggregatesThatIgnoreRepeatedValuesAreEligible() {
    final String keys = "WHERE a.id IN [0, 3, 12, 20, 30, 39] ";
    for (final String hop : new String[] { "-[:DEP*1..4]->", "<-[:DEP*0..3]-", "-[:DEP|OTHER*1..3]-" })
      for (final String form : new String[] {
          "RETURN a.id AS s, collect(DISTINCT b.id) AS c ORDER BY s",
          "RETURN a.id AS s, sum(DISTINCT b.id) AS c, avg(DISTINCT b.id) AS m ORDER BY s",
          "RETURN a.id AS s, min(b.id) AS lo, max(b.id) AS hi ORDER BY s",
          "RETURN a.id AS s, min(DISTINCT b.grp) AS lo, max(DISTINCT b.grp) AS hi, count(DISTINCT b) AS n ORDER BY s",
          "RETURN collect(DISTINCT b.id) AS c, count(DISTINCT a) AS n" }) {
        final String eligible = "MATCH (a:Pkg)" + hop + "(b:Pkg) " + keys + form;
        final String reference = "MATCH p = (a:Pkg)" + hop + "(b:Pkg) " + keys + form;
        assertThat(planOf(eligible)).as(eligible).contains(ENDPOINTS);
        assertThat(sortedRows(eligible)).as(eligible).isEqualTo(sortedRows(reference));
      }
    // an aggregate that counts or adds up the repeats must keep seeing them
    for (final String form : new String[] { "RETURN collect(b.id) AS c", "RETURN sum(b.id) AS c", "RETURN avg(b.id) AS c",
        "RETURN count(b) AS c", "RETURN collect(DISTINCT b.id + 1) AS c", "RETURN collect(DISTINCT b.id) AS c, count(b) AS n" })
      assertThat(planOf("MATCH (a:Pkg)-[:DEP*1..4]->(b:Pkg) WHERE a.id = 39 " + form)).as(form).doesNotContain(ENDPOINTS);
  }

  @Test
  void inlineEdgeFiltersAreAppliedToEveryHop() {
    for (final String hop : new String[] { "-[:DEP*1..4 {w: 1}]->", "-[:DEP*0..4 {w: 0}]-", "<-[:DEP|OTHER*0..3 {w: 2}]-" }) {
      final String eligible = "MATCH (a:Pkg)" + hop + "(b:Pkg) WHERE a.id IN [0, 3, 20, 39] RETURN DISTINCT a.id AS s, b.id AS v ORDER BY s, v";
      final String reference = "MATCH p = (a:Pkg)" + hop + "(b:Pkg) WHERE a.id IN [0, 3, 20, 39] RETURN DISTINCT a.id AS s, b.id AS v ORDER BY s, v";
      assertThat(planOf(eligible)).as(eligible).contains(ENDPOINTS);
      assertThat(rows(eligible)).as(eligible).isEqualTo(rows(reference));
    }
  }

  @Test
  void targetLabelAndPropertyFiltersAreAppliedToEveryEndVertex() {
    final String eligible = "MATCH (a:Pkg)-[:DEP*1..5]->(b:Pkg {grp: 1}) WHERE a.id = 39 RETURN DISTINCT b.id AS v ORDER BY v";
    final String reference = "MATCH p = (a:Pkg)-[:DEP*1..5]->(b:Pkg {grp: 1}) WHERE a.id = 39 RETURN DISTINCT b.id AS v ORDER BY v";
    assertThat(planOf(eligible)).contains(ENDPOINTS);
    assertThat(column(eligible, "v", Map.of())).isNotEmpty().isEqualTo(column(reference, "v", Map.of()));
    assertThat(column("MATCH (a:Pkg)-[:DEP*1..5]->(b:Missing) WHERE a.id = 39 RETURN DISTINCT b.id AS v", "v", Map.of())).isEmpty();
  }

  @Test
  void walkAndAcyclicPatternsGiveTheAnswersOfTheirPathEnumeration() {
    for (final String mode : new String[] { "WALK", "ACYCLIC", "TRAIL" })
      for (final String hop : new String[] { "-[:DEP*1..4]->", "<-[:DEP*1..4]-", "-[:DEP*0..3]-" })
        for (final int key : new int[] { 3, 12, 30 }) {
          // WALK needs an upper bound, which every hop here has
          final String eligible = "MATCH " + mode + " (a:Pkg)" + hop + "(b:Pkg) WHERE a.id = $key RETURN DISTINCT b.id AS v ORDER BY v";
          final String reference = "MATCH p = " + mode + " (a:Pkg)" + hop + "(b:Pkg) WHERE a.id = $key RETURN DISTINCT b.id AS v ORDER BY v";
          assertThat(planOf(eligible)).as(eligible).contains(ENDPOINTS);
          assertThat(column(eligible, "v", Map.of("key", key))).as("%s %s key=%d", mode, hop, key)
              .isEqualTo(column(reference, "v", Map.of("key", key)));
        }
    // an undirected WALK may go back along the relationship it came by, so a lower bound of 1 is decided by the visited set
    final String walk = "MATCH WALK (a:Pkg)-[:DEP*1..2]-(b:Pkg) WHERE a.id = $key RETURN DISTINCT b.id AS v ORDER BY v";
    final String walkPaths = "MATCH p = WALK (a:Pkg)-[:DEP*1..2]-(b:Pkg) WHERE a.id = $key RETURN DISTINCT b.id AS v ORDER BY v";
    assertThat(planOf(walk)).contains(ENDPOINTS);
    assertThat(column(walk, "v", Map.of("key", 0))).isEqualTo(column(walkPaths, "v", Map.of("key", 0))).contains(0);
  }

  @Test
  void aLimitStopsAfterTheRequestedDistinctEnds() {
    final String query = "MATCH (a:Pkg)-[:DEP*1..6]->(b:Pkg) WHERE a.id = 39 RETURN DISTINCT b.id AS v LIMIT 5";
    assertThat(planOf(query)).contains(ENDPOINTS);
    final List<Object> ends = column(query, "v", Map.of());
    assertThat(ends).hasSize(5).doesNotHaveDuplicates();
  }

  @Test
  void patternsThatCanTellPathsApartKeepEnumeratingThem() {
    final String[] ineligible = {
        // one row per path is the answer
        "MATCH (a:Pkg)-[:DEP*1..3]->(b:Pkg) WHERE a.id = 39 RETURN b.id AS v",
        "MATCH (a:Pkg)-[:DEP*1..3]->(b:Pkg) WHERE a.id = 39 RETURN count(b) AS v",
        "MATCH (a:Pkg)-[:DEP*1..3]->(b:Pkg) WHERE a.id = 39 RETURN count(*) AS v",
        "MATCH (a:Pkg)-[:DEP*1..3]->(b:Pkg) WHERE a.id = 39 RETURN b.id AS v, count(DISTINCT a) AS c, sum(b.id) AS s",
        // the path or the relationships are read
        "MATCH p = (a:Pkg)-[:DEP*1..3]->(b:Pkg) WHERE a.id = 39 RETURN DISTINCT b.id AS v",
        "MATCH (a:Pkg)-[r:DEP*1..3]->(b:Pkg) WHERE a.id = 39 RETURN DISTINCT b.id AS v, size(r) AS n",
        "MATCH (a:Pkg)-[r:DEP*1..3]->(b:Pkg) WHERE a.id = 39 AND size(r) > 1 RETURN DISTINCT b.id AS v",
        "MATCH (a:Pkg)-[r:DEP*1..3 WHERE r.w < 2]->(b:Pkg) WHERE a.id = 39 RETURN DISTINCT b.id AS v",
        // a lower bound above 1 needs trails, not reachability
        "MATCH (a:Pkg)-[:DEP*2..3]->(b:Pkg) WHERE a.id = 39 RETURN DISTINCT b.id AS v",
        // the projection reads something besides the two ends
        "MATCH (a:Pkg)-[:DEP*1..3]->(b:Pkg) WHERE a.id = 39 RETURN DISTINCT b.id + rand() AS v",
        "MATCH (a:Pkg)-[:DEP*1..3]->(b:Pkg) WHERE a.id = 39 RETURN DISTINCT b AS v, 1 AS one",
        // more than one MATCH, a WITH, an OPTIONAL MATCH or another pattern in the clause
        "MATCH (a:Pkg)-[:DEP*1..3]->(b:Pkg) WHERE a.id = 39 WITH b MATCH (b)-[:DEP]->(c) RETURN DISTINCT c.id AS v",
        "MATCH (a:Pkg)-[:DEP*1..3]->(b:Pkg) WHERE a.id = 39 WITH DISTINCT b RETURN b.id AS v",
        "OPTIONAL MATCH (a:Pkg)-[:DEP*1..3]->(b:Pkg) WHERE a.id = 39 RETURN DISTINCT b.id AS v",
        "MATCH (a:Pkg)-[:DEP*1..3]->(b:Pkg)-[:DEP]->(c:Pkg) WHERE a.id = 39 RETURN DISTINCT c.id AS v" };
    for (final String query : ineligible)
      assertThat(planOf(query)).as(query).doesNotContain(ENDPOINTS);
    // and the rows still repeat per path
    assertThat(column(ineligible[0], "v", Map.of()).size())
        .isGreaterThan(column("MATCH (a:Pkg)-[:DEP*1..3]->(b:Pkg) WHERE a.id = 39 RETURN DISTINCT b.id AS v", "v", Map.of()).size());
  }

  private String planOf(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(resultSet.hasNext()).isTrue();
      return String.valueOf(resultSet.next().<Object>getProperty("executionPlan"));
    }
  }

  private List<Object> column(final String query, final String name, final Map<String, Object> params) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query, params)) {
      while (resultSet.hasNext())
        values.add(resultSet.next().<Object>getProperty(name));
    }
    return values;
  }

  /** Like {@link #rows}, with the elements of every list value sorted: a collected list has no order the statement promised. */
  private List<String> sortedRows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final List<String> cells = new ArrayList<>();
        for (final String name : row.getPropertyNames()) {
          final Object value = row.getProperty(name);
          if (value instanceof List<?> list) {
            final List<String> elements = new ArrayList<>();
            for (final Object element : list)
              elements.add(String.valueOf(element));
            Collections.sort(elements);
            cells.add(name + "=" + elements);
          } else
            cells.add(name + "=" + value);
        }
        rows.add(String.join(",", cells));
      }
    }
    return rows;
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
