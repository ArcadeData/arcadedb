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

package com.arcadedb.function.sql.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.sql.FunctionOptions;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.index.fulltext.MoreLikeThisConfig;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests of the graph path functions, grouped by feature.
 */
class ShortestPathOptionsTest {

  /**
   * Regression for #10439: shortestPath maxDepth bounds the number of edges.
   *
   * Regression test for issue #10439: {@code shortestPath()}'s {@code maxDepth} option counted vertices instead of edges,
   * so a path of exactly {@code maxDepth} edges was missed and {@code maxDepth: 1} could never find even a direct edge.
   * The option now bounds the number of edges, the same as Cypher's {@code *..n}.
   */
  @Nested
  class ShortestPathMaxDepth extends TestHelper {
    private static final int LENGTH = 8;

    /** A chain {@code 0 -> 1 -> ... -> LENGTH} plus a fan of dead-end leaves on vertex 0, so the two BFS fronts differ in size. */
    private void buildChain() {
      database.getSchema().createVertexType("N");
      database.getSchema().createEdgeType("L");
      database.transaction(() -> {
        for (int i = 0; i <= LENGTH; i++)
          database.command("sql", "INSERT INTO N SET id = ?", i);
        for (int i = 0; i < LENGTH; i++)
          database.command("sql", "CREATE EDGE L FROM (SELECT FROM N WHERE id = ?) TO (SELECT FROM N WHERE id = ?)", i, i + 1);
        for (int i = 0; i < 5; i++) {
          database.command("sql", "INSERT INTO N SET id = ?", 100 + i);
          database.command("sql", "CREATE EDGE L FROM (SELECT FROM N WHERE id = 0) TO (SELECT FROM N WHERE id = ?)", 100 + i);
        }
      });
    }

    private List<?> sqlPath(final int from, final int to, final String direction, final String options) {
      try (final ResultSet rs = database.query("sql",
          "SELECT shortestPath((SELECT FROM N WHERE id = ?), (SELECT FROM N WHERE id = ?), '" + direction + "', 'L', " + options
              + ") AS p", from, to)) {
        return rs.next().<List<?>>getProperty("p");
      }
    }

    private List<?> consolidatedPath(final int from, final int to, final int maxDepth) {
      try (final ResultSet rs = database.query("sql",
          "SELECT shortestPath((SELECT FROM N WHERE id = ?), (SELECT FROM N WHERE id = ?), {direction: 'OUT', edgeType: 'L', maxDepth: "
              + maxDepth + "}) AS p", from, to)) {
        return rs.next().<List<?>>getProperty("p");
      }
    }

    private void assertEdgeBound(final String label) {
      for (final String direction : new String[] { "OUT", "BOTH" })
        for (int to = 1; to <= LENGTH; to++) {
          assertThat(sqlPath(0, to, direction, "{maxDepth: " + to + "}")).as("%s %s: %d edges, maxDepth %d", label, direction, to, to)
              .hasSize(to + 1);
          assertThat(sqlPath(0, to, direction, "{maxDepth: " + (to - 1) + "}"))
              .as("%s %s: %d edges, maxDepth %d", label, direction, to, to - 1).isEmpty();
          // Reversed endpoints: the destination side has the smaller front, so the right-first branch runs
          assertThat(sqlPath(to, 0, "BOTH", "{maxDepth: " + to + "}")).as("%s reversed: %d edges, maxDepth %d", label, to, to)
              .hasSize(to + 1);
          assertThat(sqlPath(to, 0, "BOTH", "{maxDepth: " + (to - 1) + "}"))
              .as("%s reversed: %d edges, maxDepth %d", label, to, to - 1).isEmpty();
        }
    }

    @Test
    void maxDepthBoundsTheNumberOfEdges() {
      buildChain();
      // The reported repro: three edges, maxDepth 3
      assertThat(sqlPath(0, 3, "OUT", "{maxDepth: 3}")).hasSize(4);
      assertThat(sqlPath(0, 3, "OUT", "{maxDepth: 2}")).isEmpty();
      // A single edge used to be unreachable at any maxDepth below 2
      assertThat(sqlPath(0, 1, "OUT", "{maxDepth: 1}")).hasSize(2);
      assertThat(sqlPath(0, 1, "OUT", "{maxDepth: 0}")).isEmpty();
      assertEdgeBound("OLTP");
    }

    @Test
    void consolidatedOptionsMapUsesTheSameBound() {
      buildChain();
      for (int to = 1; to <= LENGTH; to++) {
        assertThat(consolidatedPath(0, to, to)).as("%d edges, maxDepth %d", to, to).hasSize(to + 1);
        assertThat(consolidatedPath(0, to, to - 1)).as("%d edges, maxDepth %d", to, to - 1).isEmpty();
      }
    }

    @Test
    void edgeModeUsesTheSameBound() {
      buildChain();
      for (int to = 1; to <= LENGTH; to++) {
        // edge: true returns vertices and edges interleaved
        assertThat(sqlPath(0, to, "OUT", "{maxDepth: " + to + ", edge: true}")).as("%d edges, maxDepth %d", to, to)
            .hasSize(2 * to + 1);
        assertThat(sqlPath(0, to, "OUT", "{maxDepth: " + (to - 1) + ", edge: true}")).as("%d edges, maxDepth %d", to, to - 1)
            .isEmpty();
      }
    }

    @Test
    void csrAcceleratedSearchUsesTheSameBound() {
      buildChain();
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("issue-10439-view").withVertexTypes("N")
          .withEdgeTypes("L").build();
      try {
        // Without this the loop below could pass on the OLTP path twice
        try (final ResultSet profile = database.command("sql",
            "PROFILE SELECT shortestPath((SELECT FROM N WHERE id = 0), (SELECT FROM N WHERE id = 3), 'OUT', 'L', {maxDepth: 3}) AS p")) {
          assertThat(profile.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("CSR-accelerated");
        }
        assertEdgeBound("CSR");
      } finally {
        view.drop();
      }
    }

    @Test
    void sameSourceAndDestinationIsAZeroEdgePath() {
      buildChain();
      assertThat(sqlPath(2, 2, "OUT", "{maxDepth: 0}")).hasSize(1);
    }

    @Test
    void cypherHopBoundsAgreeWithSqlMaxDepth() {
      buildChain();
      for (int to = 1; to <= LENGTH; to++) {
        assertThat(cypherPathLength(to, to)).as("Cypher *..%d over %d edges", to, to).isEqualTo(to + 1);
        assertThat(cypherPathLength(to, to - 1)).as("Cypher *..%d over %d edges", to - 1, to).isEqualTo(-1);
        assertThat(sqlPath(0, to, "OUT", "{maxDepth: " + to + "}")).hasSize(cypherPathLength(to, to));
      }
    }

    /** Number of vertices on the Cypher shortest path from 0 to {@code to} within {@code max} hops, or -1 for no row. */
    private int cypherPathLength(final int to, final int max) {
      if (max < 1)
        return -1;
      try (final ResultSet rs = database.query("opencypher",
          "MATCH p = shortestPath((a:N {id: 0})-[:L*.." + max + "]->(b:N {id: " + to + "})) RETURN [n IN nodes(p) | n.id] AS path")) {
        if (!rs.hasNext())
          return -1;
        final Result row = rs.next();
        final List<?> path = row.getProperty("path");
        return path == null ? -1 : path.size();
      }
    }
  }

  /**
   * Regression for #10788: path finder option bag rejects malformed options.
   *
   * Issue #10788: an option of the {@code astar()} / {@code dijkstra()} option bag that is not a value of its kind is refused with a
   * message naming the option, never read as another value: {@code maxDepth: 2.5} is not 2, {@code maxDepth: 'abc'} is not the
   * default, a negative {@code maxDepth} is not "no path", and {@code parallel: 'yes'} is not {@code false}. A value of the kind is
   * accepted at its full range: {@code maxDepth} is a long, so 3000000000 is a bound like any other.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PathFinderOptionBag extends TestHelper {

    private RID source;
    private RID target;

    @BeforeEach
    void buildChain() {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
      database.transaction(() -> {
        for (int i = 0; i <= 3; i++)
          database.command("sql", "CREATE VERTEX V SET nm = ?", "v" + i);
        for (int i = 0; i < 3; i++)
          database.command("sql", "CREATE EDGE E FROM (SELECT FROM V WHERE nm = ?) TO (SELECT FROM V WHERE nm = ?) SET w = 1.0", "v" + i,
              "v" + (i + 1));
      });
      source = rid("v0");
      target = rid("v3");
    }

    @ParameterizedTest
    @ValueSource(strings = { "astar", "dijkstra" })
    void aFractionalMaxDepthIsRefused(final String function) {
      assertRefused(function, "maxDepth: 2.5", "maxDepth");
      assertRefused(function, "maxDepth: '2.5'", "maxDepth");
    }

    @ParameterizedTest
    @ValueSource(strings = { "astar", "dijkstra" })
    void anUnparsableMaxDepthIsRefused(final String function) {
      assertRefused(function, "maxDepth: 'abc'", "maxDepth");
    }

    @ParameterizedTest
    @ValueSource(strings = { "astar", "dijkstra" })
    void aNegativeMaxDepthIsRefused(final String function) {
      assertRefused(function, "maxDepth: -1", "maxDepth");
    }

    @ParameterizedTest
    @ValueSource(strings = { "astar", "dijkstra" })
    void aMaxDepthBeyondTheIntRangeIsABoundLikeAnyOther(final String function) {
      assertThat(path(function, "maxDepth: 3000000000")).hasSize(4);
      assertThat(path(function, "maxDepth: '3000000000'")).hasSize(4);
      assertThat(path(function, "maxDepth: 3")).hasSize(4);
      assertThat(path(function, "maxDepth: 2")).isEmpty();
      assertThat(path(function, "maxDepth: 0")).isEmpty();
      // a whole number spelled as a decimal is that number
      assertThat(path(function, "maxDepth: 3.0")).hasSize(4);
    }

    @ParameterizedTest
    @ValueSource(strings = { "astar", "dijkstra" })
    void aBooleanOptionThatIsNotABooleanIsRefused(final String function) {
      assertRefused(function, "emptyIfMaxDepth: 'yes'", "emptyIfMaxDepth");
      assertRefused(function, "emptyIfMaxDepth: 1", "emptyIfMaxDepth");
      // the spellings of a boolean are accepted
      assertThat(path(function, "emptyIfMaxDepth: 'TRUE'")).hasSize(4);
      assertThat(path(function, "emptyIfMaxDepth: false")).hasSize(4);
    }

    @ParameterizedTest
    @ValueSource(strings = { "parallel: 'yes'", "tieBreaker: 'ture'", "dFactor: 'abc'" })
    void anAstarOnlyOptionOfTheWrongKindIsRefused(final String option) {
      assertRefused("astar", option, option.substring(0, option.indexOf(':')));
    }

    /** The option reader is shared by every function option bag: a boolean option of a non-graph function is held to the same rule. */
    @Test
    void aBooleanOptionOfAnotherFunctionIsHeldToTheSameRule() {
      assertThatThrownBy(() -> MoreLikeThisConfig.fromOptions(
          new FunctionOptions("search_index_more", Map.of("excludeSource", "yes"), MoreLikeThisConfig.OPTION_KEYS))).hasMessageContaining(
          "Option 'excludeSource'").hasMessageContaining("must be a boolean");
      assertThatThrownBy(() -> MoreLikeThisConfig.fromOptions(
          new FunctionOptions("search_index_more", Map.of("boostByScore", 1), MoreLikeThisConfig.OPTION_KEYS))).hasMessageContaining(
          "Option 'boostByScore'");
      assertThat(MoreLikeThisConfig.fromOptions(
          new FunctionOptions("search_index_more", Map.of("excludeSource", "FALSE"), MoreLikeThisConfig.OPTION_KEYS)).isExcludeSource()).isFalse();
    }

    /** shortestPath() reads maxDepth as an int: a null one is no bound, as for astar() and dijkstra(), and a negative one is refused. */
    @Test
    void aShortestPathMaxDepthIsNullForNoBoundAndNeverNegative() {
      assertThat(shortestPath("{maxDepth: null}")).hasSize(4);
      assertThat(shortestPath("{maxDepth: 3}")).hasSize(4);
      assertThat(shortestPath("{maxDepth: 2}")).isEmpty();
      assertThatThrownBy(() -> shortestPath("{maxDepth: -1}")).hasMessageContaining("Option 'maxDepth'").hasMessageContaining("negative");
      assertThatThrownBy(() -> shortestPath("{maxDepth: 2.5}")).hasMessageContaining("Option 'maxDepth'");
      assertThatThrownBy(() -> shortestPath("{edge: 'yes'}")).hasMessageContaining("Option 'edge'");
    }

    // ---- helpers

    private void assertRefused(final String function, final String option, final String optionName) {
      assertThatThrownBy(() -> path(function, option)).as("%s with %s", function, option)
          .hasMessageContaining("Option '" + optionName + "'").hasMessageContaining(function);
    }

    private List<?> path(final String function, final String option) {
      final String options = "{direction: 'OUT', edgeTypeNames: ['E']" + (function.equals("astar") ? ", vertexAxisNames: []" : "") + ", "
          + option + "}";
      try (final ResultSet resultSet = database.query("sql", "SELECT " + function + "(?, ?, 'w', " + options + ") AS p", source, target)) {
        return resultSet.next().getProperty("p");
      }
    }

    private List<?> shortestPath(final String options) {
      try (final ResultSet resultSet = database.query("sql", "SELECT shortestPath(?, ?, 'OUT', 'E', " + options + ") AS p", source, target)) {
        return resultSet.next().getProperty("p");
      }
    }

    private RID rid(final String name) {
      try (final ResultSet resultSet = database.query("sql", "SELECT FROM V WHERE nm = ?", name)) {
        return resultSet.next().getIdentity().orElseThrow();
      }
    }
  }

  /**
   * Regression for #8938: shortestPath trailing options map.
   *
   * Regression test for issue #8938: the trailing options map of {@code shortestPath()} (position 4) validated its keys
   * against the full option set, so {@code edgeTypeNames} passed validation there and was then silently dropped, and the
   * walk ran unrestricted.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ShortestPathTrailingOptions extends TestHelper {

    private void buildGraph() {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("Road");
      database.getSchema().createEdgeType("Rail");
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX V SET name = 'A'");
        database.command("sql", "CREATE VERTEX V SET name = 'B'");
        database.command("sql", "CREATE VERTEX V SET name = 'C'");
        database.command("sql", "CREATE EDGE Road FROM (SELECT FROM V WHERE name = 'A') TO (SELECT FROM V WHERE name = 'C')");
        database.command("sql", "CREATE EDGE Road FROM (SELECT FROM V WHERE name = 'C') TO (SELECT FROM V WHERE name = 'B')");
        database.command("sql", "CREATE EDGE Rail FROM (SELECT FROM V WHERE name = 'A') TO (SELECT FROM V WHERE name = 'B')");
      });
    }

    private int pathLength(final String args) {
      try (final ResultSet rs = database.query("sql",
          "SELECT shortestPath((SELECT FROM V WHERE name = 'A'), (SELECT FROM V WHERE name = 'B')" + args + ") AS p")) {
        return rs.next().<List<?>>getProperty("p").size();
      }
    }

    @Test
    void positionalAndConsolidatedFormsHonourTheEdgeTypes() {
      buildGraph();
      assertThat(pathLength(", 'OUT', ['Road']")).isEqualTo(3);
      assertThat(pathLength(", {direction:'OUT', edgeTypeNames:['Road']}")).isEqualTo(3);
    }

    @Test
    void aPathKnobInTheTrailingMapIsRefusedByName() {
      buildGraph();
      assertThatThrownBy(() -> pathLength(", 'OUT', null, {edgeTypeNames:['Road']}"))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("edgeTypeNames");
      assertThatThrownBy(() -> pathLength(", 'OUT', null, {direction:'IN'}"))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("direction");
    }

    @Test
    void theTrailingMapStillTakesMaxDepthAndEdge() {
      buildGraph();
      assertThat(pathLength(", 'OUT', ['Road'], {'maxDepth':5, 'edge':false}")).isEqualTo(3);
    }
  }
}
