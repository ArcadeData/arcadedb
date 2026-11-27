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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11913: {@code shortestPath()} / {@code allShortestPaths()} whose endpoints are the same node answered the
 * zero-length path also when the pattern declares at least one hop ({@code [*]}, {@code [*1..]}, an unquantified
 * relationship), a path shorter than the pattern allows. The zero-length path now answers only a minimum of zero; any other
 * pattern has no row for that pair (a null / empty list in the expression form), whichever evaluator runs it.
 * <p>
 * Graph: 0 -> 1 -> 2 -> 0 (a cycle), 3 -> 3 (a self loop) and 3 -> 4; every relationship carries w = 1.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherShortestPathSameNodeIssue11913Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher", """
        UNWIND range(0, 4) AS i CREATE (:V {k: i})
        WITH count(*) AS nodes
        UNWIND [[0, 1], [1, 2], [2, 0], [3, 3], [3, 4]] AS e
        MATCH (a:V {k: e[0]}), (b:V {k: e[1]})
        CREATE (a)-[:E {w: 1}]->(b)"""));
  }

  @Test
  void aMinimumOfOneHasNoAnswerForTheSameNode() {
    assertThat(lengths("MATCH p = shortestPath((a:V {k: 3})-[:E*1..]->(b:V {k: 3})) RETURN length(p) AS l")).isEmpty();
    assertThat(lengths("MATCH (a:V {k: 3}) MATCH p = shortestPath((a)-[:E*]->(a)) RETURN length(p) AS l")).isEmpty();
    assertThat(lengths("MATCH p = shortestPath((a:V {k: 3})-[:E*1..]->(b:V)) WHERE b.k = 3 RETURN length(p) AS l")).isEmpty();
    assertThat(lengths("MATCH (a:V {k: 3}) MATCH p = shortestPath((a)-[:E]->(a)) RETURN length(p) AS l")).isEmpty();
    assertThat(lengths("MATCH (a:V {k: 0}) MATCH p = shortestPath((a)-[:E*]-(a)) RETURN length(p) AS l")).isEmpty();
    assertThat(lengths("MATCH p = shortestPath((a:V {k: 3})-[:E*2..]->(b:V {k: 3})) RETURN length(p) AS l")).isEmpty();
  }

  @Test
  void aMinimumOfZeroKeepsTheZeroLengthPath() {
    assertThat(lengths("MATCH p = shortestPath((a:V {k: 3})-[:E*0..]->(b:V {k: 3})) RETURN length(p) AS l")).containsExactly(0);
    assertThat(lengths("MATCH (a:V {k: 0}) MATCH p = shortestPath((a)-[:E*0..2]->(a)) RETURN length(p) AS l")).containsExactly(0);
    assertThat(lengths("MATCH p = allShortestPaths((a:V {k: 0})-[:E*0..]->(b:V {k: 0})) RETURN length(p) AS l"))
        .containsExactly(0);
  }

  @Test
  void allShortestPathsAppliesTheMinimum() {
    assertThat(lengths("MATCH p = allShortestPaths((a:V {k: 0})-[:E*1..]->(b:V {k: 0})) RETURN length(p) AS l")).isEmpty();
    assertThat(lengths("MATCH p = allShortestPaths((a:V {k: 3})-[:E*]->(b:V {k: 3})) RETURN length(p) AS l")).isEmpty();
  }

  /** An inline property map or WHERE routes both forms to the edge-aware BFS. */
  @Test
  void theEdgeAwareSearchAppliesTheMinimum() {
    assertThat(lengths("MATCH (a:V {k: 3}) MATCH p = shortestPath((a)-[:E* {w: 1}]->(a)) RETURN length(p) AS l")).isEmpty();
    assertThat(lengths("MATCH (a:V {k: 3}) MATCH p = shortestPath((a)-[r:E* WHERE r.w = 1]->(a)) RETURN length(p) AS l"))
        .isEmpty();
    assertThat(lengths("MATCH (a:V {k: 3}) MATCH p = allShortestPaths((a)-[:E* {w: 1}]->(a)) RETURN length(p) AS l")).isEmpty();
    assertThat(lengths("MATCH (a:V {k: 3}) MATCH p = shortestPath((a)-[:E*0.. {w: 1}]->(a)) RETURN length(p) AS l"))
        .containsExactly(0);
    assertThat(lengths("MATCH (a:V {k: 3}) MATCH p = allShortestPaths((a)-[:E*0.. {w: 1}]->(a)) RETURN length(p) AS l"))
        .containsExactly(0);
  }

  @Test
  void theExpressionFormAppliesTheMinimum() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:V {k: 3}) RETURN shortestPath((a)-[:E*]->(a)) AS p, allShortestPaths((a)-[:E*]->(a)) AS ps, "
            + "shortestPath((a)-[:E* {w: 1}]->(a)) AS pf, length(shortestPath((a)-[:E*0..]->(a))) AS l0")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("p")).isNull();
      assertThat(row.<List<?>>getProperty("ps")).isEmpty();
      assertThat(row.<Object>getProperty("pf")).isNull();
      assertThat(row.<Number>getProperty("l0").intValue()).isZero();
    }
  }

  /** Every pair of a cartesian product: the rows where both endpoints are the same node are the ones with no answer. */
  @Test
  void aCartesianProductDropsOnlyTheSameNodePairs() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:V), (b:V) MATCH p = shortestPath((a)-[:E*]->(b)) RETURN a.k AS a, b.k AS b, length(p) AS l")) {
      final List<Result> rows = rs.stream().toList();
      // 0, 1 and 2 reach each other (6 pairs), 3 reaches 4
      assertThat(rows).hasSize(7);
      for (final Result row : rows) {
        assertThat(row.<Number>getProperty("a").intValue()).isNotEqualTo(row.<Number>getProperty("b").intValue());
        assertThat(row.<Number>getProperty("l").intValue()).isPositive();
      }
    }
  }

  private List<Integer> lengths(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.stream().map(r -> r.<Number>getProperty("l").intValue()).toList();
    }
  }
}
