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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A DISTINCT aggregation over graph elements keys them by their address in a primitive set. The answers must be the ones the
 * general set gave: one per element however it is reached, apart from the values of other kinds in the same aggregation,
 * and per group.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherDistinctGraphElementTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("K");
    database.transaction(() -> {
      final MutableVertex[] p = new MutableVertex[5];
      for (int i = 0; i < p.length; i++)
        p[i] = database.newVertex("P").set("id", i, "team", i % 2 == 0 ? "even" : "odd").save();
      // A ring plus a chord: several paths reach the same vertex and the same edge
      for (int i = 0; i < p.length; i++)
        p[i].newEdge("K", p[(i + 1) % p.length], "w", i);
      p[0].newEdge("K", p[2], "w", 10);
    });
  }

  @Test
  void aVertexReachedByManyPathsCountsOnce() {
    assertThat(single("MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) RETURN count(DISTINCT c) AS n")).isEqualTo(5L);
    assertThat(single("MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) RETURN count(c) AS n")).isEqualTo(
        single("MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) RETURN count(*) AS n"));
  }

  @Test
  void anEdgeReachedFromBothEndsCountsOnce() {
    assertThat(single("MATCH (a:P)-[r:K]-(b:P) RETURN count(DISTINCT r) AS n")).isEqualTo(6L);
    assertThat(single("MATCH (a:P)-[r:K]-(b:P) RETURN count(r) AS n")).isEqualTo(12L);
  }

  @Test
  void elementsAndValuesOfOtherKindsShareOneAggregation() {
    // Two vertices twice each, two strings twice each, a number as integer and as float (one value for =), and nulls
    assertThat(single("MATCH (a:P {id: 0}), (b:P {id: 1}) UNWIND [a, b, 'x', a, 'y', 'x', b, 1, 1.0, null, 'y', null] AS v "
        + "RETURN count(DISTINCT v) AS n")).isEqualTo(5L);
    assertThat(single("MATCH (a:P {id: 0}), (b:P {id: 1}) UNWIND [a, b, a, b, a] AS v RETURN size(collect(DISTINCT v)) AS n"))
        .isEqualTo(2L);
  }

  @Test
  void eachGroupHasItsOwnSet() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:P)-[:K]-(b:P) RETURN a.team AS team, count(DISTINCT b) AS n ORDER BY team")) {
      final List<Result> rows = rs.stream().toList();
      assertThat(rows).hasSize(2);
      // even = {0, 2, 4}: neighbours 1, 4, 2 | 1, 3, 0 | 3, 0 -> {0, 1, 2, 3, 4}; odd = {1, 3}: 0, 2 | 2, 4 -> {0, 2, 4}
      assertThat(rows.get(0).<String>getProperty("team")).isEqualTo("even");
      assertThat(rows.get(0).<Number>getProperty("n").longValue()).isEqualTo(5L);
      assertThat(rows.get(1).<String>getProperty("team")).isEqualTo("odd");
      assertThat(rows.get(1).<Number>getProperty("n").longValue()).isEqualTo(3L);
    }
  }

  private long single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
