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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.InvalidPropertyTypeException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Variable-length relationship patterns: trail semantics, distinct endpoints, relationship property access and bounded exploration.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherVariableLengthTraversalTest {
  /**
   * Regression tests for <a href="https://github.com/ArcadeData/arcadedb/issues/6097">#6097</a>.
   * <p>
   * A hub-and-spoke graph with a modest branching factor (200 nodes per level, 3 levels) has only
   * 601 nodes and 80,200 edges, but {@code FANOUT^3 = 8,000,000} distinct length-3 paths between the
   * hub and the outermost level. Before the fix, {@code ExpandPathStep} always drove variable-length
   * patterns through a BFS traverser whose iterator materialized every one of those paths - across
   * every intermediate level - into a single in-memory list before the first row was even produced,
   * regardless of what the consuming clause actually needed. A plain {@code count(DISTINCT b)} (600
   * distinct destinations) or a {@code LIMIT 5} therefore paid for the full 8,000,000-path enumeration
   * anyway.
   */
  @Tag("slow")
  @Nested
  class Issue6097 extends TestHelper {
    private static final int FANOUT = 200;

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub6097");
        database.getSchema().createVertexType("L1_6097");
        database.getSchema().createVertexType("L2_6097");
        database.getSchema().createVertexType("L3_6097");
        database.getSchema().createEdgeType("R1_6097");
        database.getSchema().createEdgeType("R2_6097");
        database.getSchema().createEdgeType("R3_6097");

        final MutableVertex hub = database.newVertex("Hub6097").set("id", 0).save();

        final List<MutableVertex> l1 = new ArrayList<>(FANOUT);
        for (int i = 0; i < FANOUT; i++) {
          final MutableVertex v = database.newVertex("L1_6097").set("id", i).save();
          hub.newEdge("R1_6097", v).save();
          l1.add(v);
        }

        final List<MutableVertex> l2 = new ArrayList<>(FANOUT);
        for (int i = 0; i < FANOUT; i++)
          l2.add(database.newVertex("L2_6097").set("id", i).save());

        for (final MutableVertex a : l1)
          for (final MutableVertex b : l2)
            a.newEdge("R2_6097", b).save();

        final List<MutableVertex> l3 = new ArrayList<>(FANOUT);
        for (int i = 0; i < FANOUT; i++)
          l3.add(database.newVertex("L3_6097").set("id", i).save());

        for (final MutableVertex a : l2)
          for (final MutableVertex b : l3)
            a.newEdge("R3_6097", b).save();
      });
    }

    @Test
    void countDistinctOverVariableLengthPathDoesNotMaterializeEveryPath() {
      final ResultSet result = database.command("opencypher",
          "MATCH (:Hub6097)-[*1..3]->(b) RETURN count(DISTINCT b) AS c");

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      // Only the 3*FANOUT distinct destination nodes matter - not the FANOUT^3 distinct paths.
      assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(3L * FANOUT);
    }

    @Test
    void limitShortCircuitsInsteadOfExploringEveryPath() {
      final List<Object> targets = new ArrayList<>();

      // With a genuinely lazy (streaming) traversal, LIMIT stops expansion almost immediately - it
      // never needs to enumerate anywhere near the 8,000,000 length-3 paths that exist. Before the
      // fix this never returned in any reasonable time: the traverser built its full path list
      // before ExpandPathStep - let alone LIMIT - ever pulled the first row.
      assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
        final ResultSet result = database.command("opencypher",
            "MATCH (:Hub6097)-[*1..3]->(b) RETURN b LIMIT 5");
        while (result.hasNext())
          targets.add(result.next().getProperty("b"));
      });

      assertThat(targets).hasSize(5);
    }
  }

  /**
   * Issue #10062: an undirected trail pattern with a lower bound of 1 reports its start vertex as an end vertex only when a cycle
   * leads back to it, and walking back along the relationship it left by is not one. The visited-set traversal must decide that
   * like the path enumeration does, for every shape of cycle: none (a tree), a self loop, parallel edges, antiparallel edges, a
   * triangle, a long cycle, a cycle that does not pass through the start, and two branches that meet.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10062UndirectedTrailStart extends TestHelper {
    private static final String ENDPOINTS = "DISTINCT ENDPOINTS";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("N").createProperty("id", Type.INTEGER);
        database.getSchema().createEdgeType("L");
      });
    }

    @Test
    void aTreeNeverLeadsBackToTheStart() {
      graph(6, "0>1", "1>2", "1>3", "0>4", "4>5");
      for (int max = 1; max <= 6; max++)
        assertStartIsEnd(max, false);
      // walking back along the same relationship is allowed to a WALK
      assertThat(ends("MATCH WALK (a:N {id: 0})-[:L*1..2]-(b:N) RETURN DISTINCT b.id AS v ORDER BY v")).contains(0);
    }

    @Test
    void aSelfLoopIsAClosedTrailOfOneRelationship() {
      graph(3, "0>1", "0>0", "1>2");
      for (int max = 1; max <= 4; max++)
        assertStartIsEnd(max, true);
    }

    @Test
    void aSelfLoopElsewhereDoesNotBringTheStartBack() {
      graph(3, "0>1", "1>1", "1>2");
      for (int max = 1; max <= 5; max++)
        assertStartIsEnd(max, false);
    }

    @Test
    void parallelAndAntiparallelEdgesAreAClosedTrailOfTwo() {
      graph(3, "0>1", "0>1", "1>2");
      assertStartIsEnd(1, false);
      for (int max = 2; max <= 4; max++)
        assertStartIsEnd(max, true);
      graph(3, "0>1", "1>0", "1>2");
      assertStartIsEnd(1, false);
      for (int max = 2; max <= 4; max++)
        assertStartIsEnd(max, true);
    }

    @Test
    void parallelEdgesBetweenOtherVerticesDoNotBringTheStartBack() {
      graph(4, "0>1", "1>2", "1>2", "2>3");
      for (int max = 1; max <= 6; max++)
        assertStartIsEnd(max, false);
    }

    @Test
    void aTriangleNeedsThreeRelationships() {
      graph(3, "0>1", "1>2", "2>0");
      assertStartIsEnd(1, false);
      assertStartIsEnd(2, false);
      for (int max = 3; max <= 5; max++)
        assertStartIsEnd(max, true);
    }

    @Test
    void aLongCycleNeedsItsWholeLength() {
      graph(6, "0>1", "1>2", "2>3", "3>4", "4>5", "5>0");
      for (int max = 1; max <= 5; max++)
        assertStartIsEnd(max, false);
      assertStartIsEnd(6, true);
      assertStartIsEnd(7, true);
    }

    @Test
    void aCycleThatMissesTheStartDoesNotCount() {
      // 0 hangs off a triangle 1-2-3 by one relationship
      graph(4, "0>1", "1>2", "2>3", "3>1");
      for (int max = 1; max <= 6; max++)
        assertStartIsEnd(max, false);
    }

    @Test
    void twoBranchesThatMeetMakeACycleThroughTheStart() {
      // a diamond 0-1-3 and 0-2-3: the cycle is 4 long, found where the branches meet
      graph(4, "0>1", "0>2", "1>3", "2>3");
      for (int max = 1; max <= 3; max++)
        assertStartIsEnd(max, false);
      for (int max = 4; max <= 6; max++)
        assertStartIsEnd(max, true);
    }

    @Test
    void aCycleThroughTheStartBehindALongTail() {
      // a tail 0-1-2-3 leads to a diamond: the start is on no cycle, vertex 3 is
      graph(8, "0>1", "1>2", "2>3", "3>4", "3>5", "4>6", "5>6", "6>7");
      for (int max = 1; max <= 8; max++)
        assertStartIsEnd(max, false);
    }

    @Test
    void aStartOnTwoCyclesIsFoundThroughTheShorterOne() {
      // a triangle 0-1-2 and a square 0-3-4-5
      graph(6, "0>1", "1>2", "2>0", "0>3", "3>4", "4>5", "5>0");
      assertStartIsEnd(2, false);
      for (int max = 3; max <= 6; max++)
        assertStartIsEnd(max, true);
    }

    @Test
    void edgeFiltersDecideWhichCyclesExist() {
      // the triangle closes only through a relationship with w = 9
      database.transaction(() -> database.command("sql", "DELETE FROM N"));
      database.transaction(() -> {
        final List<MutableVertex> vertices = new ArrayList<>();
        for (int i = 0; i < 3; i++)
          vertices.add(database.newVertex("N").set("id", i).save());
        vertices.get(0).newEdge("L", vertices.get(1)).set("w", 1).save();
        vertices.get(1).newEdge("L", vertices.get(2)).set("w", 1).save();
        vertices.get(2).newEdge("L", vertices.get(0)).set("w", 9).save();
      });
      final String closed = "MATCH (a:N {id: 0})-[:L*1..3]-(b:N) RETURN DISTINCT b.id AS v ORDER BY v";
      final String open = "MATCH (a:N {id: 0})-[:L*1..3 {w: 1}]-(b:N) RETURN DISTINCT b.id AS v ORDER BY v";
      assertThat(planOf(open)).contains(ENDPOINTS);
      assertThat(ends(closed)).containsExactly(0, 1, 2);
      assertThat(ends(open)).containsExactly(1, 2);
    }

    /** The answer for the start vertex 0 and the given upper bound, from the visited set and from path enumeration. */
    private void assertStartIsEnd(final int max, final boolean expected) {
      final String eligible = "MATCH (a:N {id: 0})-[:L*1.." + max + "]-(b:N) RETURN DISTINCT b.id AS v ORDER BY v";
      final String reference = "MATCH p = (a:N {id: 0})-[:L*1.." + max + "]-(b:N) RETURN DISTINCT b.id AS v ORDER BY v";
      assertThat(planOf(eligible)).as(eligible).contains(ENDPOINTS);
      assertThat(planOf(reference)).as(reference).doesNotContain(ENDPOINTS);
      final List<Object> answer = ends(eligible);
      assertThat(answer).as(eligible).isEqualTo(ends(reference));
      assertThat(answer.contains(0)).as("%s: is the start its own end", eligible).isEqualTo(expected);
    }

    /** Replaces the graph with {@code vertices} vertices (id 0..n-1) and one L edge per {@code "from>to"} entry. */
    private void graph(final int vertices, final String... edges) {
      database.transaction(() -> {
        database.command("sql", "DELETE FROM N");
        final List<MutableVertex> created = new ArrayList<>();
        for (int i = 0; i < vertices; i++)
          created.add(database.newVertex("N").set("id", i).save());
        for (final String edge : edges) {
          final String[] ends = edge.split(">");
          created.get(Integer.parseInt(ends[0])).newEdge("L", created.get(Integer.parseInt(ends[1]))).save();
        }
      });
    }

    private String planOf(final String query) {
      try (final ResultSet resultSet = database.query("opencypher", "EXPLAIN " + query)) {
        return String.valueOf(resultSet.next().<Object>getProperty("executionPlan"));
      }
    }

    private List<Object> ends(final String query) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet resultSet = database.query("opencypher", query)) {
        while (resultSet.hasNext())
          values.add(resultSet.next().<Object>getProperty("v"));
      }
      return values;
    }
  }

  /**
   * Issue #10062, differential check: on many small random multigraphs (self loops, parallel edges, antiparallel edges, cycles, two
   * relationship types) every eligible variable-length shape must answer exactly what the path enumeration answers. The reference is
   * the same statement made ineligible by a named path.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10062VarLengthDistinctEndpointsFuzz extends TestHelper {
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
                      // the list of a collect holds the same values either way, in the order the walk met them (issue #10152)
                      "RETURN a.id AS s, collect(DISTINCT b.id) AS l ORDER BY s",
                      "RETURN a.id AS s, collect(DISTINCT b.id) AS l, count(DISTINCT b) AS c ORDER BY s",
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

    /** The rows as text, with the elements of every list value sorted: a collected list has no order the statement promised. */
    private List<String> rows(final String query) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet resultSet = database.query("opencypher", query)) {
        while (resultSet.hasNext()) {
          final Result row = resultSet.next();
          final List<String> cells = new ArrayList<>();
          for (final String name : row.getPropertyNames()) {
            final Object value = row.getProperty(name);
            if (value instanceof List<?> list) {
              final List<String> elements = new ArrayList<>(list.size());
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
  }

  /**
   * Issue #10062: a variable-length hop whose consumer cannot tell two paths between the same pair of vertices apart reports each
   * end vertex once, found with a visited set, instead of walking every path. The answers must be exactly those of the path
   * enumeration, which the same query gives once a named path makes it ineligible.
   * <p>
   * The graph is a dependency-like DAG (each package depends on the four below it, so there are many paths onto few vertices)
   * with a cycle (3 to 30 and back down), a self loop, parallel edges, a second relationship type and a property on the edges.
   */
  @Nested
  class Issue10062VarLengthDistinctEndpoints extends TestHelper {
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

  /**
   * Issue #10557: {@code count(DISTINCT b)} over a variable-length hop was fused into the walk on the cost-based path only (#10548). The
   * step-based path, taken for instance when the end node has no label, built one row per end vertex, filtered it for
   * {@code WHERE b <> a} and aggregated it. Its {@code ExpandPathStep} now counts while it walks, through a view or the records, and
   * answers what the rows answer. The oracle is the same statement with a named path, which enumerates paths and fuses nothing.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10557StepBasedDistinctEndpointCount extends TestHelper {
    private static final String VIEW        = "gav10557";
    private static final String STEP_WALK   = "EXPAND PATH";
    private static final String FUSED_COUNT = "DISTINCT ENDPOINT COUNT";
    private static final int    VERTICES    = 7;

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("V").createProperty("id", Type.INTEGER);
        database.getSchema().createVertexType("W").addSuperType("V");
        database.getSchema().createEdgeType("E");
        database.getSchema().createEdgeType("F");
      });
    }

    /** The issue's statement: PROFILE shows the fused count, and no aggregation, through the records and through a view. */
    @Test
    void theIssueStatementIsFusedOnTheStepBasedPath() {
      // 1 -> 2 -> 3 -> 4 -> 1 over E, and 3 -> 5 over F, which the walk must not follow
      database.transaction(() -> {
        final List<MutableVertex> vertices = new ArrayList<>();
        for (int i = 0; i <= 5; i++)
          vertices.add(database.newVertex(i == 4 ? "W" : "V").set("id", i).save());
        for (int i = 1; i <= 4; i++)
          vertices.get(i).newEdge("E", vertices.get(i == 4 ? 1 : i + 1)).save();
        vertices.get(3).newEdge("F", vertices.get(5)).save();
      });
      final String query = "MATCH (a:V {id: 1})-[:E*1..5]->(b) WHERE b <> a RETURN count(DISTINCT b) AS c";
      final String reference = query.replace("MATCH ", "MATCH p = ");

      assertThat(profileOf(query)).contains(FUSED_COUNT).contains(STEP_WALK).doesNotContainIgnoringCase("aggregat");
      assertThat(rows(query)).isEqualTo(rows(reference)).containsExactly("c=3");

      createView();
      assertThat(profileOf(query)).contains(FUSED_COUNT);
      assertThat(rows(query)).isEqualTo(rows(reference));
    }

    /**
     * Every shape the step-based executor plans with a distinct-endpoint {@code ExpandPathStep} and a lone {@code count(DISTINCT)}
     * is fused, and answers what the path enumeration answers: OUT, IN and BOTH, every path mode, lower bounds 0 and 1, the count of
     * either end, with and without {@code WHERE b <> a}, end node patterns the view cannot test (inline properties, a label
     * disjunction), with and without a view.
     */
    @Test
    void everyFusedShapeAnswersWhatPathEnumerationAnswers() {
      final String[][] directions = { { "-", "->" }, { "<-", "-" }, { "-", "-" } };
      final String[] modes = { "", "ACYCLIC ", "TRAIL " };
      final String[] tails = {
          "RETURN count(DISTINCT b) AS c",
          "WHERE b <> a RETURN count(DISTINCT b) AS c",
          "WHERE a <> b RETURN count(DISTINCT b) AS c",
          "RETURN count(DISTINCT a) AS c",
          "WHERE b <> a RETURN count(DISTINCT a) AS c" };
      final String[] ends = { "(b)", "(b {id: 3})", "(b:W|V)" };
      int fused = 0;
      int compared = 0;
      for (int seed = 0; seed < 3; seed++) {
        buildGraph(seed);
        for (final boolean withView : new boolean[] { false, true }) {
          if (withView)
            createView();
          for (final String[] dir : directions)
            for (final String mode : modes)
              for (final int min : new int[] { 0, 1 })
                for (final int max : new int[] { 1, 3 })
                  for (final String types : new String[] { ":E", "" })
                    for (final String start : new String[] { "(a:V)", "(a:V {id: " + (seed % VERTICES) + "})", "(a)" })
                      for (final String end : ends)
                        for (final String tail : tails) {
                          final String query = "MATCH " + mode + start + dir[0] + "[" + types + "*" + min + ".." + max + "]" + dir[1] + end
                              + " " + tail;
                          final String profile = profileOf(query);
                          if (profile.contains(FUSED_COUNT))
                            fused++;
                          else
                            assertThat(profile.contains(STEP_WALK) && profile.contains("DISTINCT ENDPOINTS"))
                                .as("a distinct-endpoint ExpandPathStep counted by an aggregation: %s%n%s", query, profile).isFalse();
                          assertThat(rows(query)).as("seed=%d view=%s %s", seed, withView, query)
                              .isEqualTo(rows(query.replace("MATCH ", "MATCH p = ")));
                          compared++;
                        }
          if (withView)
            dropView();
        }
      }
      assertThat(compared).isGreaterThan(3_000);
      // the step-based fusion is reached at all, or the comparison above proves nothing about it
      assertThat(fused).as("statements answered by the fused count of an ExpandPathStep").isGreaterThan(compared / 4);
    }

    /**
     * A target an earlier pattern of the clause bound, a second filter, another RETURN item, a count that is not DISTINCT and an
     * OPTIONAL MATCH: either fused and still the rows' answer, or not fused. Every one answers what the rows answer.
     */
    @Test
    void whatTheRowsDecideIsHonouredOrNotFused() {
      buildGraph(4);
      createView();
      final String[] fused = {
          "MATCH (a:V)-[r:E*1..3]->(b) RETURN count(DISTINCT b) AS c",
          "MATCH (a:V)-[:E*0..2]-(b {id: 2}) WHERE a <> b RETURN count(DISTINCT a) AS c" };
      // planned either way, depending on which end the planner anchors: b is bound by the first pattern before the walk reaches it
      final String[] either = {
          "MATCH (b:V)-[:F]->(c), (a:V)-[:E*1..3]->(b) RETURN count(DISTINCT b) AS c",
          "MATCH (b:V)-[:F]->(c), (a:V)-[:E*1..3]->(b) RETURN count(DISTINCT a) AS c" };
      for (final String query : either)
        assertThat(rows(query)).as(query).isEqualTo(rows(query.replace("(a:V)-", "p = (a:V)-")));

      // a subquery body is run once per outer row: each run counts its own walks, whatever the plan
      final String correlated =
          "MATCH (x:V) CALL { WITH x MATCH (x)-[:E*1..3]->(b) RETURN count(DISTINCT b) AS c } RETURN x.id AS s, c ORDER BY s";
      assertThat(rows(correlated)).isEqualTo(rows(correlated.replace("MATCH (x)-", "MATCH p = (x)-")))
          .anyMatch(row -> !row.endsWith("c=0"));
      final String[] notFused = {
          "MATCH (a:V)-[:E*1..3]->(b) WHERE b <> a AND b.id > 1 RETURN count(DISTINCT b) AS c",
          "MATCH (a:V)-[:E*1..3]->(b) WHERE b.id <> a.id RETURN count(DISTINCT b) AS c",
          "MATCH (a:V)-[:E*1..3]->(b) RETURN count(DISTINCT b) AS c, count(DISTINCT a) AS d",
          "MATCH (a:V)-[:E*1..3]->(b) RETURN count(b) AS c",
          "MATCH (a:V)-[:E*1..3]->(b) RETURN a.id AS s, count(DISTINCT b) AS c ORDER BY s",
          "OPTIONAL MATCH (a:V)-[:E*1..3]->(b) RETURN count(DISTINCT b) AS c" };
      for (final String query : fused) {
        assertThat(profileOf(query)).as(query).contains(FUSED_COUNT);
        assertThat(rows(query)).as(query).isEqualTo(rows(query.replace("(a:V)-", "p = (a:V)-")));
      }
      for (final String query : notFused) {
        assertThat(profileOf(query)).as(query).doesNotContain(FUSED_COUNT);
        assertThat(rows(query)).as(query).isEqualTo(rows(query.replace("(a:V)-", "p = (a:V)-")));
      }
    }

    private void buildGraph(final int seed) {
      final Random random = new Random(seed);
      database.transaction(() -> {
        database.command("sql", "DELETE FROM V");
        final List<MutableVertex> vertices = new ArrayList<>();
        for (int i = 0; i < VERTICES; i++)
          vertices.add(database.newVertex(i % 3 == 0 ? "W" : "V").set("id", i).save());
        final int edges = VERTICES + random.nextInt(VERTICES * 2);
        for (int e = 0; e < edges; e++) {
          final MutableVertex from = vertices.get(random.nextInt(VERTICES));
          // a fifth of the edges are self loops, the rest may repeat or run both ways between a pair
          final MutableVertex to = random.nextInt(5) == 0 ? from : vertices.get(random.nextInt(VERTICES));
          from.newEdge(random.nextInt(3) == 0 ? "F" : "E", to).save();
        }
      });
    }

    private void createView() {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + VIEW + " VERTEX TYPES (V, W) EDGE TYPES (E, F) UPDATE MODE SYNCHRONOUS");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, VIEW);
      final long deadline = System.currentTimeMillis() + 60_000;
      while (!view.isReady() && System.currentTimeMillis() < deadline)
        Thread.onSpinWait();
      assertThat(view.isReady()).isTrue();
    }

    private void dropView() {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW " + VIEW);
    }

    private String profileOf(final String query) {
      try (final ResultSet resultSet = database.query("opencypher", "PROFILE " + query)) {
        while (resultSet.hasNext())
          resultSet.next();
        return resultSet.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    private List<String> rows(final String query) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet resultSet = database.query("opencypher", query)) {
        while (resultSet.hasNext()) {
          final Result row = resultSet.next();
          final List<String> cells = new ArrayList<>();
          for (final String name : row.getPropertyNames())
            cells.add(name + "=" + row.getProperty(name));
          rows.add(String.join(",", cells));
        }
      }
      return rows;
    }
  }

  /**
   * Issue #11211: {@code r.weight} on the variable of a variable-length relationship - a list of relationships - failed
   * while the rows were serialized, as a plain {@code CommandExecutionException} the server answered with 500. Neo4j refuses
   * the query before it runs ({@code Type mismatch: expected Map, Node, Relationship, ... but was List<Relationship>}), and a
   * type error found only once a query runs is still the caller's ({@code Neo.ClientError.Statement.TypeError}).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue11211VarLengthRelationshipPropertyAccess extends TestHelper {

    @BeforeEach
    void createGraph() {
      database.transaction(() -> database.command("opencypher", "CREATE (:N {id: 1})-[:R {weight: 7}]->(:N {id: 2})"));
    }

    @Test
    void propertyAccessOnAVariableLengthRelationshipIsRefusedBeforeTheQueryRuns() {
      final Throwable thrown = refusal("MATCH (n:N)<-[r:R*1..3]-(m:N) RETURN r.weight AS w");

      assertThat(thrown).isInstanceOf(CommandParsingException.class);
      assertThat(thrown.getMessage()).contains("List<Relationship>").contains("'r.weight'");
      assertThat(ErrorCategory.of(thrown)).isEqualTo(ErrorCategory.PARSING);
    }

    @Test
    void theRefusalDoesNotDependOnWhatTheQueryMatches() {
      // Neo4j refuses it at compile time: a query that matches nothing is refused exactly the same way
      assertThat(refusal("MATCH (n:Nothing)<-[r:R*1..3]-(m:Nothing) RETURN r.weight AS w"))
          .isInstanceOf(CommandParsingException.class);
    }

    @Test
    void everyClauseThatReadsThePropertyIsRefused() {
      assertThat(refusal("MATCH (n:N)<-[r:R*]-(m:N) WHERE r.weight > 1 RETURN n")).isInstanceOf(CommandParsingException.class);
      assertThat(refusal("MATCH (n:N)<-[r:R*]-(m:N) RETURN n ORDER BY r.weight")).isInstanceOf(CommandParsingException.class);
      // the kind survives a projection that carries the variable, renamed or not
      assertThat(refusal("MATCH (n:N)<-[r:R*]-(m:N) WITH r RETURN r.weight AS w")).isInstanceOf(CommandParsingException.class);
      assertThat(refusal("MATCH (n:N)<-[r:R*]-(m:N) WITH r AS rels RETURN rels.weight AS w"))
          .isInstanceOf(CommandParsingException.class);
    }

    @Test
    void theListOfRelationshipsIsStillReadable() {
      assertThat(values("MATCH (n:N)<-[r:R*1..3]-(m:N) RETURN size(r) AS v")).containsExactly(1L);
      assertThat(values("MATCH (n:N)<-[r:R*1..3]-(m:N) RETURN [rel IN r | rel.weight] AS v")).containsExactly(List.of(7));
      assertThat(values("MATCH (n:N)<-[r:R*1..3]-(m:N) RETURN r[0].weight AS v")).containsExactly(7);
      assertThat(values("MATCH (n:N)<-[r:R*1..3]-(m:N) UNWIND r AS rel RETURN rel.weight AS v")).containsExactly(7);
      assertThat(values("MATCH (n:N)<-[r:R*1..3]-(m:N) RETURN last(r).weight AS v")).containsExactly(7);
      assertThat(values("MATCH (n:N)<-[r:R*1..3]-(m:N) WHERE all(rel IN r WHERE rel.weight > 1) RETURN size(r) AS v"))
          .containsExactly(1L);
      // the pattern's own inline predicate tests one relationship at a time, so there r.weight is a property read
      assertThat(values("MATCH (n:N)<-[r:R*1..3 WHERE r.weight > 1]-(m:N) RETURN size(r) AS v")).containsExactly(1L);
      // a fixed-length relationship is one relationship, and its property is read as before
      assertThat(values("MATCH (n:N)<-[r:R]-(m:N) RETURN r.weight AS v")).containsExactly(7);
      // a variable that names something else after a projection is not a list of relationships any more
      assertThat(values("MATCH (n:N)<-[r:R*1..3]-(m:N) WITH {weight: 3} AS r RETURN r.weight AS v")).containsExactly(3L);
    }

    @Test
    void aTypeErrorKnownOnlyAtRuntimeIsTheCallersNotTheServers() {
      // UNWIND says nothing about the kind of its variable, so the list is met only once the query runs
      final Throwable thrown = refusal("UNWIND [[1, 2]] AS l RETURN l.x AS v");

      assertThat(thrown).isInstanceOf(TypeMismatchException.class);
      // named with the language's type names, not with a Java class
      assertThat(thrown.getMessage()).contains("TypeError: Cannot access property 'x' on a LIST<ANY> value")
          .doesNotContain("ArrayList");
      assertThat(ErrorCategory.of(thrown)).isEqualTo(ErrorCategory.VALIDATION);
    }

    @Test
    void aChainedPropertyAccessOnAListIsTheSameTypeError() {
      final Throwable thrown = refusal("WITH {inner: [1, 2]} AS m RETURN m.inner.x AS v");
      assertThat(thrown).isInstanceOf(TypeMismatchException.class);
      assertThat(ErrorCategory.of(thrown)).isEqualTo(ErrorCategory.VALIDATION);
    }

    @Test
    void anIndexIntoAValueThatIsNoListIsTheSameTypeError() {
      final Throwable thrown = refusal("UNWIND [42] AS v RETURN v[0] AS x");
      assertThat(thrown).isInstanceOf(TypeMismatchException.class);
      assertThat(ErrorCategory.of(thrown)).isEqualTo(ErrorCategory.VALIDATION);

      assertThat(refusal("UNWIND [1.5] AS i RETURN [1, 2][i] AS x")).isInstanceOf(TypeMismatchException.class);
    }

    @Test
    void anUnstorableValueIsStillATypeErrorOfItsOwnKind() {
      final Throwable thrown = catchThrowable(
          () -> database.transaction(() -> database.command("opencypher", "CREATE (:N {m: {a: 1}})")));
      assertThat(thrown).isInstanceOf(InvalidPropertyTypeException.class).isInstanceOf(TypeMismatchException.class);
      assertThat(ErrorCategory.of(thrown)).isEqualTo(ErrorCategory.VALIDATION);
    }

    private Throwable refusal(final String cypher) {
      final Throwable thrown = catchThrowable(() -> values(cypher));
      assertThat(thrown).as("must be refused: %s", cypher).isNotNull();
      return thrown;
    }

    private List<Object> values(final String cypher) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          values.add(row.getProperty(row.getPropertyNames().iterator().next()));
        }
      }
      return values;
    }
  }
}
