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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5456: an undirected relationship pattern inside a pattern
 * comprehension emitted every self-loop twice, once through the outgoing adjacency list of the
 * vertex and once through the incoming one. Neo4j returns each relationship exactly once, and the
 * equivalent undirected {@code MATCH} already did after issue #5446 / PR #5447, so the pattern
 * comprehension evaluator was the last path missing the deduplication.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5456UndirectedPatternComprehensionSelfLoopTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (single:Person {id: 'single'}), (multi:Person {id: 'multi'}), "
        + "(head:Person {id: 'head'}), (tail:Person {id: 'tail'}), (far:Person {id: 'far'})");
    database.command("opencypher", "MATCH (a:Person {id: 'single'}) CREATE (a)-[:LOOP {rid: 1}]->(a)");
    database.command("opencypher", "MATCH (a:Person {id: 'multi'}) CREATE (a)-[:LOOP {rid: 1}]->(a), (a)-[:LOOP {rid: 2}]->(a)");
    // An ordinary relationship on a pair of loop-free nodes, so the plain undirected case stays covered.
    database.command("opencypher",
        "MATCH (h:Person {id: 'head'}), (t:Person {id: 'tail'}) CREATE (h)-[:LOOP {rid: 3}]->(t)");
    // A second relationship pointing at 'tail', so head..far is only reachable by changing direction.
    database.command("opencypher",
        "MATCH (f:Person {id: 'far'}), (t:Person {id: 'tail'}) CREATE (f)-[:LOOP {rid: 4}]->(t)");
  }

  /** The reporter's first witness: one self-loop must contribute exactly one list element. */
  @Test
  void undirectedPatternComprehensionEmitsSingleSelfLoopOnce() {
    assertThat(ids("MATCH (a:Person {id: 'single'}) RETURN [(a)-[r:LOOP]-(b) | r.rid] AS ids")).containsExactly(1);
  }

  /** The reporter's second witness: two distinct self-loops must stay distinct and not be doubled. */
  @Test
  void undirectedPatternComprehensionEmitsEveryDistinctSelfLoopOnce() {
    assertThat(ids("MATCH (a:Person {id: 'multi'}) RETURN [(a)-[r:LOOP]-(b) | r.rid] AS ids"))
        .containsExactlyInAnyOrder(1, 2);
  }

  /** size() over the comprehension is the cardinality the reporter cares about downstream. */
  @Test
  void sizeOfUndirectedPatternComprehensionCountsEachSelfLoopOnce() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {id: 'multi'}) RETURN size([(a)-[:LOOP]-(b) | b.id]) AS n");
    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(2);
    rs.close();
  }

  /** Control from the issue: the directed comprehension was always correct and must stay so. */
  @Test
  void directedPatternComprehensionIsUnaffected() {
    assertThat(ids("MATCH (a:Person {id: 'single'}) RETURN [(a)-[r:LOOP]->(b) | r.rid] AS ids")).containsExactly(1);
    assertThat(ids("MATCH (a:Person {id: 'multi'}) RETURN [(a)-[r:LOOP]->(b) | r.rid] AS ids"))
        .containsExactlyInAnyOrder(1, 2);
  }

  /** An undirected hop over a non-loop relationship must still be reached from either endpoint. */
  @Test
  void undirectedPatternComprehensionStillWalksBothDirectionsForNonLoopEdges() {
    assertThat(ids("MATCH (a:Person {id: 'head'}) RETURN [(a)-[r:LOOP]-(b) | b.id] AS ids")).containsExactly("tail");
    assertThat(ids("MATCH (a:Person {id: 'tail'}) RETURN [(a)-[r:LOOP]-(b) | b.id] AS ids"))
        .containsExactlyInAnyOrder("head", "far");
  }

  /** The variable-length form walks the same adjacency lists and must deduplicate identically. */
  @Test
  void undirectedVariableLengthPatternComprehensionEmitsEachSelfLoopOnce() {
    assertThat(ids("MATCH (a:Person {id: 'single'}) RETURN [(a)-[:LOOP*1..1]-(b) | b.id] AS ids"))
        .containsExactly("single");
    assertThat(ids("MATCH (a:Person {id: 'multi'}) RETURN [(a)-[:LOOP*1..1]-(b) | b.id] AS ids"))
        .containsExactly("multi", "multi");
  }

  /**
   * A single undirected walk also lets a variable-length hop change direction between steps:
   * {@code head -> tail <- far} is a valid undirected 2-hop path, which the previous
   * one-direction-per-pass traversal could never produce.
   */
  @Test
  void undirectedVariableLengthPatternComprehensionChangesDirectionBetweenHops() {
    assertThat(ids("MATCH (a:Person {id: 'head'}) RETURN [(a)-[:LOOP*1..2]-(b) | b.id] AS ids"))
        .containsExactlyInAnyOrder("tail", "far");
  }

  /** The undirected {@code MATCH} control the issue used to prove the comprehension disagreed. */
  @Test
  void undirectedMatchControlKeepsReturningOneRowPerRelationship() {
    final List<Object> rids = new ArrayList<>();
    final ResultSet rs = database.query("opencypher", "MATCH (a:Person {id: 'multi'})-[r:LOOP]-(b) RETURN r.rid AS rid");
    while (rs.hasNext())
      rids.add(rs.next().getProperty("rid"));
    rs.close();
    assertThat(rids).containsExactlyInAnyOrder(1, 2);
  }

  @SuppressWarnings("unchecked")
  private List<Object> ids(final String query) {
    final ResultSet rs = database.query("opencypher", query);
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    final Object value = row.getProperty(row.getPropertyNames().iterator().next());
    rs.close();
    assertThat(value).isInstanceOf(List.class);
    return (List<Object>) value;
  }
}
