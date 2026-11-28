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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
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
  private String v0Rid;
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
      v0Rid = v0.getIdentity().toString();
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

  @Test
  void theOptionsApplyToTheMultiItemSyntax() {
    assertThat(rows("MATCH {type: V, as: a, where: (id = 0)}.(outE('A').inV()){as: b}, {as: b, type: W} RETURN a.id AS a, b.id AS b"))
        .containsExactly("0,1");
  }

  @Test
  void anOptionalHopIsFilteredToo() {
    // the optional hop reaches 1 and 2 through two edges, two partial matches: only 1 is a W, so the other keeps its row with no b,
    // exactly as when the type is written on the walked item
    final List<String> merged = rows("MATCH {type: V, as: a, where: (id = 0)}.outE('A').inV(){as: b, optional: true}, {as: b, type: W} "
        + "RETURN a.id AS a, b.id AS b");
    assertThat(merged).containsExactly("0,1", "0,null");
    assertThat(merged).isEqualTo(
        rows("MATCH {type: V, as: a, where: (id = 0)}.outE('A').inV(){as: b, optional: true, type: W} RETURN a.id AS a, b.id AS b"));
  }

  @Test
  void aBackwardWalkStillTakesTheOptionsOfTheAliasItReaches() {
    // rooted at the RID of 0 (the cheapest alias), the planner walks the chained hop backwards to a: only W 1 points at 0
    assertThat(rows("MATCH {as: a}.outE('A').inV(){as: b}, {as: a, type: W}, {as: b, rid: " + v0Rid + "} RETURN a.id AS a, b.id AS b"))
        .containsExactly("1,0");
  }

  /**
   * With more than the planner's prefetch threshold of candidates on the far side, nothing rescues a forward walk: the plain {@code out()}
   * hop, the {@code while:} walk and the {@code .(...)} syntax have to take the type of the other occurrence themselves.
   */
  @Test
  void aForwardWalkNobodyPrefetchesForTakesTheTypeOfAnotherOccurrence() {
    database.getSchema().createEdgeType("B");
    database.transaction(() -> {
      final Vertex v0 = database.lookupByRID(new RID(v0Rid), true).asVertex();
      for (int i = 0; i < 150; i++) {
        v0.newEdge("B", database.newVertex("V").set("id", 1000 + i).save());
        v0.newEdge("B", database.newVertex("W").set("id", 2000 + i).save());
      }
    });

    final List<String> plain = rows("MATCH {as: a, rid: " + v0Rid + "}.out('B'){as: b}, {as: b, type: W} RETURN b.id AS b");
    assertThat(plain).hasSize(150);
    assertThat(plain).allMatch(id -> Integer.parseInt(id) >= 2000);

    final List<String> chained = rows("MATCH {as: a, rid: " + v0Rid + "}.outE('B').inV(){as: b}, {as: b, type: W} RETURN b.id AS b");
    assertThat(chained).isEqualTo(plain);

    final List<String> multiItem = rows("MATCH {as: a, rid: " + v0Rid + "}.(outE('B').inV()){as: b}, {as: b, type: W} RETURN b.id AS b");
    assertThat(multiItem).isEqualTo(plain);

    // depth 0 is 0 itself, a V: the type of the other occurrence decides which vertices of the walk bind b
    final List<String> recursive = rows(
        "MATCH {as: a, rid: " + v0Rid + "}.out('B'){as: b, while: ($depth < 1)}, {as: b, type: W} RETURN b.id AS b");
    assertThat(recursive).isEqualTo(plain);
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
