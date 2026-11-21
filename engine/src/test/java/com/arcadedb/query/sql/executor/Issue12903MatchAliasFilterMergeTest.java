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
 * Regression test for #12903. An alias written in several patterns of one MATCH is one node: the {@code where:} of every occurrence applies to
 * it, whichever occurrence the traversal reaches it through. The planner merges the filters of an alias (it ANDs them in {@code addAliases})
 * and rebinds the merged filter to the occurrences, but the pattern the edges walk was built from copies made before the rebinding, so a node
 * bound by an occurrence that does not carry its {@code where:} was never filtered by it.
 * <p>
 * The graph is the triangle 0 -> 1 -> 2 -> 0 of vertices {@code V(id, c)} with {@code c = id}.
 */
class Issue12903MatchAliasFilterMergeTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      final MutableVertex[] v = new MutableVertex[3];
      for (int i = 0; i < 3; i++)
        v[i] = database.newVertex("V").set("id", i).set("c", i).save();
      for (int i = 0; i < 3; i++)
        v[i].newEdge("E", v[(i + 1) % 3]).save();
    });
  }

  @Test
  void aWhereOnTheOccurrenceTheTraversalDoesNotUseStillFilters() {
    // z is typed, so the walk starts at z and reaches y through the second pattern, whose {as: y} carries no where:
    assertThat(rows("MATCH {as: x}.out(){as: y, where: (c = 0)}, {type: V, as: z}.out(){as: y} RETURN x.id AS x, y.id AS y, z.id AS z"))
        .containsExactly("2,0,2");
  }

  @Test
  void aWhereOnALaterOccurrenceFiltersTheNodeBoundByAnEarlierOne() {
    assertThat(rows("MATCH {type: V, as: z}.out(){as: y}, {type: V, as: x}.out(){as: y, where: (c = 0)} RETURN x.id AS x, y.id AS y, z.id AS z"))
        .containsExactly("2,0,2");
  }

  @Test
  void aWhereOnABareNodePatternFiltersTheNode() {
    assertThat(rows("MATCH {type: V, as: z}.out(){as: y}, {as: y, where: (c = 0)} RETURN z.id AS z, y.id AS y"))
        .containsExactly("2,0");
    assertThat(rows("MATCH {as: y, where: (c = 0)}, {type: V, as: z}.out(){as: y} RETURN z.id AS z, y.id AS y"))
        .containsExactly("2,0");
  }

  @Test
  void aWhereOnTheClosingHopOfACycleFiltersTheNode() {
    // y is bound by the first hop and met again by the last one, which is the only occurrence carrying the where:
    assertThat(rows("MATCH {type: V, as: x}.out(){as: y}.out(){as: z}.out(){as: w}.out(){as: y, where: (c = 0)} "
        + "RETURN x.id AS x, y.id AS y, z.id AS z, w.id AS w")).containsExactly("2,0,1,2");
  }

  @Test
  void theWheresOfSeveralOccurrencesAreAnded() {
    // c = 0 on one occurrence and id > 0 on the other: no vertex satisfies both
    assertThat(rows("MATCH {as: x}.out(){as: y, where: (c = 0)}, {type: V, as: z}.out(){as: y, where: (id > 0)} "
        + "RETURN x.id AS x, y.id AS y, z.id AS z")).isEmpty();
    // c >= 1 and id < 2 leave the vertex 1
    assertThat(rows("MATCH {as: x}.out(){as: y, where: (c >= 1)}, {type: V, as: z}.out(){as: y, where: (id < 2)} "
        + "RETURN x.id AS x, y.id AS y, z.id AS z")).containsExactly("0,1,0");
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
