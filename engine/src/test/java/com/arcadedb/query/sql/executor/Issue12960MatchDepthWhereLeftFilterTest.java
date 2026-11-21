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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #12960. The {@code where:} of an alias that reads {@code $depth} belongs to the variable-depth hop the alias is the target
 * of. The filter a backwards walk of another hop applies to the node it arrives at is the merged filter of the alias, which kept that condition:
 * there {@code $depth} is not bound, {@code $depth >= 1} is {@code null >= 1}, and the pattern answered no rows.
 * <p>
 * The graph is 0 -A2-> 1 -A-> 2 (A2 extends A) of vertices {@code V(id)}.
 */
class Issue12960MatchDepthWhereLeftFilterTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("A");
    database.getSchema().createEdgeType("A2").addSuperType("A");
    database.transaction(() -> {
      final MutableVertex v0 = database.newVertex("V").set("id", 0).save();
      final MutableVertex v1 = database.newVertex("V").set("id", 1).save();
      final MutableVertex v2 = database.newVertex("V").set("id", 2).save();
      v0.newEdge("A2", v1).save();
      v1.newEdge("A", v2).save();
    });
  }

  @Test
  void aDepthWhereOnTheWhileTargetDoesNotFilterAClosingHopWalkedBackwards() {
    final String hop = "MATCH {type: V, as: a0}.in('A'){as: a2, where: ($depth >= 1), while: ($depth < 2)}";
    // the walk alone: a0 reaches a2 at depth 1 or 2
    assertThat(rows(hop + " RETURN a0.id AS a0, a2.id AS a2")).containsExactly("1,0", "2,0", "2,1");
    // closed by an A2 edge between the two ends: 0 and 1 are the only ones joined by it
    assertThat(rows(hop + ", {as: a2}-A2-{as: a0} RETURN a0.id AS a0, a2.id AS a2")).containsExactly("1,0");
  }

  @Test
  void theSameWithAMaxDepth() {
    assertThat(rows("MATCH {type: V, as: a0}.in('A'){as: a2, where: ($depth >= 1), maxDepth: 2}, {as: a2}-A2-{as: a0} "
        + "RETURN a0.id AS a0, a2.id AS a2")).containsExactly("1,0");
  }

  @Test
  void aTypedTargetAndAnAnyDirectionClosingHop() {
    assertThat(rows("MATCH {type: V, as: a0}.in('A'){as: a2, where: ($depth >= 1), while: ($depth < 2)}, {type: V, as: a2}.both(){as: a0} "
        + "RETURN a0.id AS a0, a2.id AS a2")).containsExactly("1,0", "2,1");
  }

  private List<String> rows(final String match) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", match)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder sb = new StringBuilder();
        for (final String name : row.getPropertyNames()) {
          if (!sb.isEmpty())
            sb.append(',');
          sb.append((Object) row.getProperty(name));
        }
        out.add(sb.toString());
      }
    }
    out.sort(null);
    return out;
  }
}
