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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * shortestPath and allShortestPaths.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherShortestPathTest {
  /**
   * Regression test for GitHub issue #5496.
   * <p>
   * {@code shortestPath()} in expression position, {@code RETURN shortestPath(...)}, computes between
   * two vertices that are already bound in the current row. When an endpoint was not bound it returned
   * {@code null}, which is indistinguishable from "no path exists", and when an endpoint carried no
   * variable at all it raised a bare {@code IllegalArgumentException}. Both now raise a
   * {@link CommandExecutionException} naming the offending endpoint and pointing at the {@code MATCH}
   * spelling.
   * <p>
   * The expression form deliberately does not search for an unbound endpoint. The {@code MATCH} form
   * does, but only because the planner binds the endpoint with a node scan first and then computes one
   * path per candidate, which multiplies rows. An expression yields a single value per row and cannot
   * reproduce that, so the two spellings answer different questions and the unsupported one fails loudly.
   * <p>
   * A variable bound to {@code null}, e.g. by a non-matching {@code OPTIONAL MATCH}, is a different
   * case: null propagates as it does everywhere else in Cypher, so it must not raise.
   */
  @Nested
  class Issue5496ShortestPathExpressionUnboundEndpoint {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-5496-shortestpath-expr-unbound").create();
      database.transaction(() -> {
        database.command("opencypher", "CREATE (a:A {v:1}), (b:A {v:10}), (d:A {v:2})");
        database.command("opencypher", "MATCH (a:A {v:1}), (b:A {v:10}), (d:A {v:2}) "
            + "CREATE (a)-[:E {tag:'ok'}]->(b), (b)-[:E {tag:'ok'}]->(d)");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    private List<Integer> pathValues(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).as("query returned no row: %s", query).isTrue();
        final List<Object> list = rs.next().getProperty("vs");
        final List<Integer> values = new ArrayList<>();
        if (list != null)
          for (final Object o : list)
            values.add(o == null ? null : ((Number) o).intValue());
        return values;
      }
    }

    /** Drains the result set so the expression is actually evaluated, then returns the rows. */
    private List<Result> rows(final String query) {
      final List<Result> all = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          all.add(rs.next());
      }
      return all;
    }

    // ---------------------------------------------------------------- unbound endpoints must raise

    @Test
    void unboundTargetVariableRaisesInsteadOfReturningNull() {
      assertThatThrownBy(() -> rows("MATCH (a:A {v:1}) RETURN shortestPath((a)-[*]->(x:A)) AS p"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("x")
          .hasMessageContaining("MATCH");
    }

    @Test
    void unboundSourceVariableRaisesInsteadOfReturningNull() {
      assertThatThrownBy(() -> rows("MATCH (d:A {v:2}) RETURN shortestPath((x:A)-[*]->(d)) AS p"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("x")
          .hasMessageContaining("MATCH");
    }

    @Test
    void anonymousTargetRaisesTheSameExceptionType() {
      assertThatThrownBy(() -> rows("MATCH (a:A {v:1}) RETURN shortestPath((a)-[*]->(:A)) AS p"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("MATCH");
    }

    @Test
    void anonymousSourceRaisesTheSameExceptionType() {
      assertThatThrownBy(() -> rows("MATCH (d:A {v:2}) RETURN shortestPath((:A)-[*]->(d)) AS p"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("MATCH");
    }

    @Test
    void allShortestPathsExpressionRaisesForAnUnboundEndpoint() {
      assertThatThrownBy(() -> rows("MATCH (a:A {v:1}) RETURN allShortestPaths((a)-[*]->(x:A)) AS p"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("MATCH");
    }

    @Test
    void theMessageNamesTheOffendingVariableAndTheWorkingSpelling() {
      assertThatThrownBy(() -> rows("MATCH (a:A {v:1}) RETURN shortestPath((a)-[*]->(x:A)) AS p"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("shortestPath")
          .hasMessageContaining("x")
          .hasMessageContaining("MATCH p = shortestPath");
    }

    // ---------------------------------------------------------------- null propagation must NOT raise

    @Test
    void anEndpointBoundToNullStillPropagatesAsNull() {
      // The OPTIONAL MATCH does not match, so z is bound to null. Cypher propagates null through the
      // expression rather than failing, and that must not be confused with an unbound variable.
      final List<Result> result = rows(
          "MATCH (a:A {v:1}) OPTIONAL MATCH (z:DoesNotExist) RETURN shortestPath((a)-[*]->(z)) AS p");
      assertThat(result).hasSize(1);
      assertThat((Object) result.get(0).getProperty("p")).isNull();
    }

    // ---------------------------------------------------------------- controls that must keep working

    @Test
    void boundEndpointsStillComputeThePath() {
      assertThat(pathValues("MATCH (a:A {v:1}), (d:A {v:2}) "
          + "RETURN [n IN nodes(shortestPath((a)-[*]->(d))) | n.v] AS vs")).containsExactly(1, 10, 2);
    }

    @Test
    void boundEndpointsWithAnInlineRelationshipWhereStillWork() {
      // Guards the #5481 edge-constraint path in the expression form.
      assertThat(pathValues("MATCH (a:A {v:1}), (d:A {v:2}) "
          + "RETURN [n IN nodes(shortestPath((a)-[r:E* WHERE r.tag = 'ok']->(d))) | n.v] AS vs"))
          .containsExactly(1, 10, 2);
    }

    @Test
    void boundEndpointsWithAnUnsatisfiableRelationshipWhereStillReturnNull() {
      // A genuine "no path" answer stays null: that is what the raised error must not be confused with.
      final List<Result> result = rows("MATCH (a:A {v:1}), (d:A {v:2}) "
          + "RETURN shortestPath((a)-[r:E* WHERE r.tag = 'nope']->(d)) AS p");
      assertThat(result).hasSize(1);
      assertThat((Object) result.get(0).getProperty("p")).isNull();
    }

    @Test
    void theMatchSpellingStillResolvesAnUnboundEndpoint() {
      // The MATCH form is unaffected: the planner binds x with a scan and emits one row per candidate.
      final List<Result> result = rows(
          "MATCH p = shortestPath((a:A {v:1})-[*]->(x:A)) RETURN x.v AS xv ORDER BY xv");
      assertThat(result).hasSize(3);
      final List<Integer> targets = new ArrayList<>();
      for (final Result r : result) {
        final Number n = r.getProperty("xv");
        targets.add(n == null ? null : n.intValue());
      }
      assertThat(targets).containsExactly(1, 2, 10);
    }
  }

  /**
   * Issue #9123: {@code MATCH p = allShortestPaths(...)} collected every path before it emitted the first row. With parallel
   * relationships the path count is the product of the parallel relationships per hop, so a LIMIT (or any consumer that stops
   * reading) had to wait for, and hold in memory, millions of paths. The paths are now produced on demand.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9123AllShortestPathsStreaming extends TestHelper {

    // 12 hops of 4 parallel relationships: 4^12 = 16.7M co-shortest paths between the two ends
    private static final int HOPS = 12;

    @Override
    public void beginTest() {
      database.getSchema().createVertexType("Chain");
      database.getSchema().createEdgeType("P");
      database.transaction(() -> {
        MutableVertex previous = database.newVertex("Chain").set("id", 0).save();
        for (int i = 1; i <= HOPS; i++) {
          final MutableVertex next = database.newVertex("Chain").set("id", i).save();
          for (int k = 0; k < 4; k++)
            previous.newEdge("P", next, "eid", i * 10 + k);
          previous = next;
        }
      });
    }

    private int rows(final String query) {
      int n = 0;
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final TraversalPath path = rs.next().getProperty("p");
          assertThat(path.length()).isEqualTo(HOPS);
          ++n;
        }
      }
      return n;
    }

    // @Timeout is a hang detector here: materializing 16.7M paths does not return in any reasonable time or fits no heap
    @Test
    @Timeout(60)
    void limitStopsTheEnumerationOfAnUnconstrainedPattern() {
      assertThat(rows("MATCH (a:Chain {id: 0}), (b:Chain {id: " + HOPS + "}) MATCH p = allShortestPaths((a)-[:P*]->(b)) RETURN p LIMIT 5"))
          .isEqualTo(5);
    }

    @Test
    @Timeout(60)
    void limitStopsTheEnumerationOfAConstrainedPattern() {
      assertThat(rows("MATCH (a:Chain {id: 0}), (b:Chain {id: " + HOPS
          + "}) MATCH p = allShortestPaths((a)-[r:P* WHERE r.eid > 0]->(b)) RETURN p LIMIT 5")).isEqualTo(5);
    }

    @Test
    @Timeout(60)
    void streamedPathsAreDistinctAndWalkTheChainInOrder() {
      final Set<List<Object>> seen = new HashSet<>();
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Chain {id: 0}), (b:Chain {id: " + HOPS + "}) MATCH p = allShortestPaths((a)-[:P*]->(b)) "
              + "RETURN [r IN relationships(p) | r.eid] AS eids LIMIT 2000")) {
        while (rs.hasNext()) {
          final List<Object> eids = List.copyOf(rs.next().<List<?>>getProperty("eids"));
          assertThat(eids).hasSize(HOPS);
          for (int i = 0; i < HOPS; i++)
            assertThat(((Number) eids.get(i)).intValue() / 10).as("hop %d", i).isEqualTo(i + 1);
          assertThat(seen.add(eids)).as("%s repeated", eids).isTrue();
        }
      }
      assertThat(seen).hasSize(2000);
    }

    // The enumeration is lazy: a write clause after the match must not see (or change) paths that are not resolved yet
    @Test
    @Timeout(60)
    void aWriteClauseAfterTheMatchDoesNotDisturbTheEnumeration() {
      database.getSchema().createVertexType("Small");
      database.getSchema().createEdgeType("S");
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Small").set("id", 0).save();
        final MutableVertex b = database.newVertex("Small").set("id", 1).save();
        for (int k = 0; k < 5; k++)
          a.newEdge("S", b, "eid", k);
      });
      database.transaction(() -> database.command("opencypher",
          "MATCH (a:Small {id: 0}), (b:Small {id: 1}) MATCH p = allShortestPaths((a)-[:S*]->(b)) "
              + "WITH relationships(p)[0] AS r DELETE r").close());
      try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r:S]->() RETURN count(r) AS c")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
      }
    }
  }
}
