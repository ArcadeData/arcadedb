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

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #11910: algo.maxFlow must carry flow only along the relationship direction by default.
 */
class AlgoMaxFlowDirectionTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("S");
    database.getSchema().createEdgeType("PIPE");
  }

  private MutableVertex node(final String n) {
    return database.newVertex("S").set("n", n).save();
  }

  private void pipe(final MutableVertex from, final MutableVertex to, final double capacity) {
    from.newEdge("PIPE", to, new Object[] { "capacity", capacity }).save();
  }

  private double maxFlow(final String from, final String to, final String direction) {
    final String dirArg = direction == null ? "" : ", '" + direction + "'";
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:S {n: $from}), (t:S {n: $to}) CALL algo.maxFlow(s, t, 'PIPE', 'capacity'" + dirArg
            + ") YIELD maxFlow RETURN maxFlow", "from", from, "to", to)) {
      return rs.next().<Number>getProperty("maxFlow").doubleValue();
    }
  }

  @Test
  void singlePipeCarriesFlowOnlyAlongItsDirection() {
    database.transaction(() -> pipe(node("a"), node("b"), 5));

    assertThat(maxFlow("a", "b", null)).isEqualTo(5.0);
    assertThat(maxFlow("b", "a", null)).isEqualTo(0.0);
    assertThat(maxFlow("b", "a", "OUT")).isEqualTo(0.0);
    assertThat(maxFlow("b", "a", "IN")).isEqualTo(5.0);
    assertThat(maxFlow("a", "b", "IN")).isEqualTo(0.0);
    assertThat(maxFlow("b", "a", "BOTH")).isEqualTo(5.0);
    assertThat(maxFlow("a", "b", "BOTH")).isEqualTo(5.0);
  }

  @Test
  void directedNetworkDiffersFromUndirected() {
    // s->a(4), a->t(4), t->b(3), b->s(3). Directed s to t: 4. Directed t to s: 3. Undirected: 7.
    database.transaction(() -> {
      final MutableVertex s = node("s"), a = node("a"), b = node("b"), t = node("t");
      pipe(s, a, 4);
      pipe(a, t, 4);
      pipe(t, b, 3);
      pipe(b, s, 3);
    });

    assertThat(maxFlow("s", "t", null)).isEqualTo(4.0);
    assertThat(maxFlow("t", "s", null)).isEqualTo(3.0);
    assertThat(maxFlow("s", "t", "IN")).isEqualTo(3.0);
    assertThat(maxFlow("s", "t", "BOTH")).isEqualTo(7.0);
  }

  @Test
  void parallelPipesSumAndSelfLoopsAreIgnored() {
    database.transaction(() -> {
      final MutableVertex x = node("x"), y = node("y");
      pipe(x, y, 2);
      pipe(x, y, 3);
      pipe(x, x, 100);
      pipe(y, y, 100);
    });

    assertThat(maxFlow("x", "y", null)).isEqualTo(5.0);
    assertThat(maxFlow("y", "x", null)).isEqualTo(0.0);
    assertThat(maxFlow("y", "x", "BOTH")).isEqualTo(5.0);
  }

  @Test
  void unknownDirectionIsRefused() {
    database.transaction(() -> pipe(node("a"), node("b"), 5));

    assertThatThrownBy(() -> maxFlow("a", "b", "SIDEWAYS")).hasMessageContaining("direction");
  }
}
