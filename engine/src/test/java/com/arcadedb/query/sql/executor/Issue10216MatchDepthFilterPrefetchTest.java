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
 * Issue #10216: a SQL MATCH node reached through a {@code while:} or {@code maxDepth:} item, with a {@code type:} and a
 * {@code where:} that reads {@code $depth}, matched nothing. The planner prefetched the node with its {@code where:} as a scan
 * filter, before any traversal, where {@code $depth} has no value. A prefetch of such a node is wrong even without
 * {@code $depth}: the recursive walk is restricted, at EVERY hop, to the prefetched candidates, so a path through a vertex that
 * is not itself a candidate (not of the end node's type) was cut short.
 * <p>
 * Graph: {@code A1 -R-> C2} (one hop) and {@code A1 -R-> X5 -R-> C3} (two hops through a vertex of another type).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10216MatchDepthFilterPrefetchTest extends TestHelper {

  @Override
  public void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE VERTEX TYPE C");
    database.command("sql", "CREATE VERTEX TYPE X");
    database.command("sql", "CREATE EDGE TYPE R");
    database.transaction(() -> {
      final MutableVertex a1 = database.newVertex("A").set("id", 1).save();
      final MutableVertex c2 = database.newVertex("C").set("id", 2).save();
      final MutableVertex c3 = database.newVertex("C").set("id", 3).save();
      final MutableVertex x5 = database.newVertex("X").set("id", 5).save();
      a1.newEdge("R", c2).save();
      a1.newEdge("R", x5).save();
      x5.newEdge("R", c3).save();
    });
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      rs.stream().forEach(r -> rows.add(r.<Object>getProperty("a") + "->" + r.<Object>getProperty("b")));
    }
    rows.sort(null);
    return rows;
  }

  /** {@code while:} is evaluated on the vertex the walk expands FROM: {@code $depth < 2} still expands X5, at depth 1. */
  @Test
  void aTypedNodeWithWhileAndADepthFilterMatches() {
    assertThat(rows("MATCH {type: A, as: a}.out('R'){type: C, as: b, while: ($depth < 1), where: ($depth >= 1)} "
        + "RETURN a.id AS a, b.id AS b")).containsExactly("1->2");
    assertThat(rows("MATCH {type: A, as: a}.out('R'){type: C, as: b, while: ($depth < 2), where: ($depth >= 1)} "
        + "RETURN a.id AS a, b.id AS b")).containsExactly("1->2", "1->3");
  }

  /** A {@code while:} that reads a property is not walked backwards: it holds on the vertices the forward walk expands from. */
  @Test
  void aWhileOnAPropertyIsWalkedFromItsSource() {
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.newVertex("A").set("id", 100 + i).save();
    });
    // Forward: A1 (id 1) and X5 (id 5) both pass, so C2 (depth 1) and C3 (depth 2) are reached. Walked back from C2 / C3 the
    // while: was evaluated on C2 / C3 (ids 2 and 3), which fail it, and only the zero-length path was tried
    assertThat(rows("MATCH {type: A, as: a}.out('R'){type: C, as: b, while: (id = 1 OR id = 5)} RETURN a.id AS a, b.id AS b"))
        .containsExactly("1->2", "1->3");
  }

  @Test
  void aTypedNodeWithMaxDepthAndADepthFilterMatches() {
    assertThat(rows("MATCH {type: A, as: a}.out('R'){type: C, as: b, maxDepth: 1, where: ($depth >= 1)} "
        + "RETURN a.id AS a, b.id AS b")).containsExactly("1->2");
    assertThat(rows("MATCH {type: A, as: a}.out('R'){type: C, as: b, maxDepth: 2, where: ($depth >= 1)} "
        + "RETURN a.id AS a, b.id AS b")).containsExactly("1->2", "1->3");
  }

  @Test
  void aDepthFilterSelectsTheHop() {
    assertThat(rows("MATCH {type: A, as: a}.out('R'){type: C, as: b, while: ($depth < 3), where: ($depth = 2)} "
        + "RETURN a.id AS a, b.id AS b")).containsExactly("1->3");
  }

  /** No {@code $depth} at all: the walk to C3 passes through X5, which is not a C and so was never a prefetched candidate. */
  @Test
  void aWalkThroughAVertexOfAnotherTypeReachesTheTypedEndNode() {
    assertThat(rows("MATCH {type: A, as: a}.out('R'){type: C, as: b, while: ($depth < 3)} RETURN a.id AS a, b.id AS b"))
        .containsExactly("1->2", "1->3");
    assertThat(rows("MATCH {type: A, as: a}.out('R'){type: C, as: b, maxDepth: 2} RETURN a.id AS a, b.id AS b"))
        .containsExactly("1->2", "1->3");
    assertThat(rows("MATCH {type: A, as: a}.out('R'){type: C, as: b, maxDepth: 2, where: (id = 3)} RETURN a.id AS a, b.id AS b"))
        .containsExactly("1->3");
  }

  /** The same patterns when the typed end node is the smaller set, so the planner is tempted to start from it. */
  @Test
  void theSameAnswersWhenTheStartNodeIsTheLargerType() {
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.newVertex("A").set("id", 100 + i).save();
    });

    assertThat(rows("MATCH {type: A, as: a}.out('R'){type: C, as: b, while: ($depth < 1), where: ($depth >= 1)} "
        + "RETURN a.id AS a, b.id AS b")).containsExactly("1->2");
    assertThat(rows("MATCH {type: A, as: a}.out('R'){type: C, as: b, maxDepth: 2, where: ($depth >= 1)} "
        + "RETURN a.id AS a, b.id AS b")).containsExactly("1->2", "1->3");
    assertThat(rows("MATCH {type: A, as: a}.out('R'){type: C, as: b, while: ($depth < 3)} RETURN a.id AS a, b.id AS b"))
        .containsExactly("1->2", "1->3");
    // $depth followed by a modifier is a read of $depth too
    assertThat(rows("MATCH {type: A, as: a}.out('R'){type: C, as: b, maxDepth: 2, where: ($depth.asInteger() >= 1)} "
        + "RETURN a.id AS a, b.id AS b")).containsExactly("1->2", "1->3");
  }

  /**
   * Both ends of a recursive edge are bound before it runs (b is reached through x first): the edge is checked from its source,
   * in its own direction. It used to be walked backwards whenever the scheduler reached it from its target.
   */
  @Test
  void aRecursiveEdgeBetweenTwoBoundNodesIsCheckedFromItsSource() {
    assertThat(rows("MATCH {type: A, as: a}.out('R'){as: x}.out('R'){type: C, as: b}, "
        + "{as: a}.out('R'){as: b, while: (id = 1 OR id = 5), where: ($depth = 2)} RETURN a.id AS a, b.id AS b"))
        .containsExactly("1->3");
    assertThat(rows("MATCH {type: A, as: a}.out('R'){as: x}.out('R'){type: C, as: b}, "
        + "{as: a}.out('R'){as: b, maxDepth: 1} RETURN a.id AS a, b.id AS b")).isEmpty();
  }

  /** The plan says why: the node reached through the recursive item is not prefetched. */
  @Test
  void theRecursiveEndNodeIsNotPrefetched() {
    try (final ResultSet rs = database.query("sql",
        "EXPLAIN MATCH {type: A, as: a}.out('R'){type: C, as: b, while: ($depth < 2), where: ($depth >= 1)} RETURN a, b")) {
      final String plan = rs.next().getProperty("executionPlanAsString");
      assertThat(plan).doesNotContain("PREFETCH b");
    }
  }
}
