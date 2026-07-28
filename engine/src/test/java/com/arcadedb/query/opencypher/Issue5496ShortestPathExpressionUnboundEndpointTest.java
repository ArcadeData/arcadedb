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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue5496ShortestPathExpressionUnboundEndpointTest {
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
