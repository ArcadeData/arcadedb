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
 * Regression test for #12937. The {@code type:}, {@code bucket:} and {@code rid:} of an alias written on one occurrence apply to the node
 * whichever occurrence the walk binds it through. The planner merges them over the occurrences and a backwards walk already uses the merged
 * values, but a forward walk took them from the item it walked, so a node reached through a chained hop ({@code outE().inV()}, which the AST
 * builder flattens into one pattern edge per method) was accepted whatever its type, bucket or RID.
 * <p>
 * Graph: V(id=0), W(id=1) and V(id=2), with W extending V; 0 -A-> 1, 0 -A-> 2, 1 -A-> 0 and 2 -A-> 0.
 */
class Issue12937MatchMultiHopNodeOptionsTest extends TestHelper {
  private String wRid;
  private String wBucket;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
    database.getSchema().createVertexType("W").addSuperType("V");
    database.getSchema().createEdgeType("A");
    wBucket = database.getSchema().getType("W").getBuckets(false).getFirst().getName();
    database.transaction(() -> {
      final MutableVertex v0 = database.newVertex("V").set("id", 0).save();
      final MutableVertex w1 = database.newVertex("W").set("id", 1).save();
      final MutableVertex v2 = database.newVertex("V").set("id", 2).save();
      wRid = w1.getIdentity().toString();
      v0.newEdge("A", w1).save();
      v0.newEdge("A", v2).save();
      w1.newEdge("A", v0).save();
      v2.newEdge("A", v0).save();
    });
  }

  @Test
  void aTypeOnAnotherOccurrenceFiltersTheNodeOfAChainedHop() {
    assertThat(rows("MATCH {type: V, as: a, where: (id = 0)}.outE('A').inV(){as: b}, {as: b, type: W} RETURN a.id AS a, b.id AS b"))
        .containsExactly("0,1");
    assertThat(rows("MATCH {type: V, as: a, where: (id = 0)}.outE('A').inV(){as: b}, {type: W, as: b}.out('A'){as: c} "
        + "RETURN a.id AS a, b.id AS b, c.id AS c")).containsExactly("0,1,0");
  }

  @Test
  void theOtherChainedHopsAreFilteredToo() {
    assertThat(rows("MATCH {type: V, as: a, where: (id = 0)}.bothE('A').bothV(){as: b}, {as: b, type: W} RETURN a.id AS a, b.id AS b"))
        .containsExactly("0,1", "0,1");
    // inE().outV() reaches the vertices that point at 0; only 1 is a W
    assertThat(rows("MATCH {type: V, as: a, where: (id = 0)}.inE('A').outV(){as: b}, {as: b, type: W} RETURN a.id AS a, b.id AS b"))
        .containsExactly("0,1");
  }

  @Test
  void aBucketOnAnotherOccurrenceFiltersTheNodeOfAChainedHop() {
    assertThat(rows("MATCH {type: V, as: a, where: (id = 0)}.outE('A').inV(){as: b}, {as: b, bucket: " + wBucket
        + "} RETURN a.id AS a, b.id AS b")).containsExactly("0,1");
  }

  @Test
  void aRidOnAnotherOccurrenceFiltersTheNodeOfAChainedHop() {
    assertThat(rows("MATCH {type: V, as: a, where: (id = 0)}.outE('A').inV(){as: b}, {as: b, rid: " + wRid + "} RETURN a.id AS a, b.id AS b"))
        .containsExactly("0,1");
  }

  @Test
  void theOptionsWrittenOnTheWalkedItemStillApply() {
    assertThat(rows("MATCH {type: V, as: a, where: (id = 0)}.outE('A').inV(){as: b, type: W} RETURN a.id AS a, b.id AS b"))
        .containsExactly("0,1");
    assertThat(rows("MATCH {type: V, as: a, where: (id = 0)}.out('A'){as: b}, {as: b, type: W} RETURN a.id AS a, b.id AS b"))
        .containsExactly("0,1");
    // V is the base type: both vertices are Vs
    assertThat(rows("MATCH {type: V, as: a, where: (id = 0)}.outE('A').inV(){as: b}, {as: b, type: V} RETURN a.id AS a, b.id AS b"))
        .containsExactly("0,1", "0,2");
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
