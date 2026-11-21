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
 * Regression test for #12902: a SQL MATCH that closes a cycle answered a handful of rows. {@code MatchEdgeTraverser.next()} answers null
 * for a candidate whose end point differs from the alias the row already holds, which is the normal outcome of a closing hop (it walks
 * every neighbour of its start and one of them is the bound alias). {@code MatchStep.fetchNext} skipped such a null for the first candidate
 * of a traverser but read it as the end of the data for the next ones, and the stream stopped for good when a batch opened on a second
 * non-closing candidate.
 */
class Issue12902MatchClosingHopTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("Knows");
  }

  /** Every ordered pair of the {@code n} vertices is joined by an edge: the directed triangles are the ordered triples of distinct vertices. */
  private void completeDigraph(final int n) {
    database.transaction(() -> {
      final MutableVertex[] v = new MutableVertex[n];
      for (int i = 0; i < n; i++)
        v[i] = database.newVertex("P").set("id", i).save();
      for (int i = 0; i < n; i++)
        for (int j = 0; j < n; j++)
          if (i != j)
            v[i].newEdge("Knows", v[j]).save();
    });
  }

  @Test
  void aTriangleOnACompleteDigraphIsAnsweredInFull() {
    final int n = 12;
    completeDigraph(n);
    assertThat(rows("MATCH {type: P, as: a}.out('Knows'){as: b}.out('Knows'){as: c}.out('Knows'){as: a} RETURN a.id AS a, b.id AS b, c.id AS c"))
        .hasSize(n * (n - 1) * (n - 2));
  }

  @Test
  void theCommaFormOfTheClosingHopIsAnsweredInFull() {
    final int n = 12;
    completeDigraph(n);
    assertThat(rows("MATCH {type: P, as: a}.out('Knows'){as: b}.out('Knows'){as: c}, {as: c}.out('Knows'){as: a} RETURN a.id AS a, b.id AS b, c.id AS c"))
        .hasSize(n * (n - 1) * (n - 2));
  }

  @Test
  void everyRowOfTheClosingHopIsDistinctAndValid() {
    final int n = 7;
    completeDigraph(n);
    final List<String> rows = rows(
        "MATCH {type: P, as: a}.out('Knows'){as: b}.out('Knows'){as: c}.out('Knows'){as: a} RETURN a.id AS a, b.id AS b, c.id AS c");
    assertThat(rows).hasSize(n * (n - 1) * (n - 2)).doesNotHaveDuplicates();
    for (final String row : rows) {
      final String[] ids = row.split(",");
      assertThat(ids[0]).isNotEqualTo(ids[1]);
      assertThat(ids[1]).isNotEqualTo(ids[2]);
      assertThat(ids[0]).isNotEqualTo(ids[2]);
    }
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
    return out;
  }
}
