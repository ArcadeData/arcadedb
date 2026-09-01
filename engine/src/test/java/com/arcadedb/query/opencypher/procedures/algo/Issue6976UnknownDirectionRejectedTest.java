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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6976: {@link GraphEngine#parseDirection(String)} used to silently coerce any
 * unrecognised direction string to {@code BOTH} instead of rejecting it, so a typo like {@code 'INCOMING'} was
 * answered with a plausible-looking - but wrong - result instead of an error. This helper backs ~23
 * {@code algo.*}/path procedures; {@code algo.bfs} is exercised end to end here as a representative caller.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6976UnknownDirectionRejectedTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue6976-direction");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("CONNECTS");

    // A -CONNECTS-> B -CONNECTS-> C
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      a.newEdge("CONNECTS", b, true, (Object[]) null).save();
      b.newEdge("CONNECTS", c, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void parseDirectionRejectsUnrecognisedValues() {
    for (final String bad : new String[] { "INCOMING", "OUTGOING", "nope", "in ", "" })
      assertThatThrownBy(() -> GraphEngine.parseDirection(bad))
          .as("direction '%s' must be rejected, not coerced to BOTH", bad)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("direction")
          .hasMessageContaining("OUT, IN or BOTH");
  }

  @Test
  void parseDirectionAcceptsValidValuesCaseInsensitively() {
    assertThat(GraphEngine.parseDirection("OUT")).isEqualTo(Vertex.DIRECTION.OUT);
    assertThat(GraphEngine.parseDirection("out")).isEqualTo(Vertex.DIRECTION.OUT);
    assertThat(GraphEngine.parseDirection("IN")).isEqualTo(Vertex.DIRECTION.IN);
    assertThat(GraphEngine.parseDirection("in")).isEqualTo(Vertex.DIRECTION.IN);
    assertThat(GraphEngine.parseDirection("BOTH")).isEqualTo(Vertex.DIRECTION.BOTH);
    assertThat(GraphEngine.parseDirection("both")).isEqualTo(Vertex.DIRECTION.BOTH);
  }

  @Test
  void parseDirectionDefaultsNullToBoth() {
    assertThat(GraphEngine.parseDirection(null)).isEqualTo(Vertex.DIRECTION.BOTH);
  }

  @Test
  void bfsRejectsAnUnrecognisedDirectionInsteadOfSilentlyTraversingBoth() {
    // Before the fix, 'INCOMING' silently became BOTH and algo.bfs returned all 3 nodes instead of erroring.
    assertThatThrownBy(() -> drain("CALL algo.bfs(start, null, 'INCOMING', 5) YIELD node RETURN node"))
        .as("'INCOMING' must be rejected, not coerced to BOTH")
        .hasStackTraceContaining("direction")
        .hasStackTraceContaining("OUT, IN or BOTH");

    // The three valid values, in either case, still work. Start node itself is never in the result, and A has
    // no incoming edges, so B and C (2 nodes) are reachable via OUT/BOTH, and none via IN.
    assertThat(drain("CALL algo.bfs(start, null, 'OUT', 5) YIELD node RETURN node")).isEqualTo(2);
    assertThat(drain("CALL algo.bfs(start, null, 'IN', 5) YIELD node RETURN node")).isEqualTo(0);
    assertThat(drain("CALL algo.bfs(start, null, 'both', 5) YIELD node RETURN node")).isEqualTo(2);
    // No direction arg at all still defaults to BOTH.
    assertThat(drain("CALL algo.bfs(start, null, null, 5) YIELD node RETURN node")).isEqualTo(2);
  }

  private int drain(final String query) {
    int count = 0;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (start:Node {name: 'A'}) " + query)) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }
    return count;
  }
}
