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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10062: an undirected trail pattern with a lower bound of 1 reports its start vertex as an end vertex only when a cycle
 * leads back to it, and walking back along the relationship it left by is not one. The visited-set traversal must decide that
 * like the path enumeration does, for every shape of cycle: none (a tree), a self loop, parallel edges, antiparallel edges, a
 * triangle, a long cycle, a cycle that does not pass through the start, and two branches that meet.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10062UndirectedTrailStartTest extends TestHelper {
  private static final String ENDPOINTS = "DISTINCT ENDPOINTS";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("N").createProperty("id", Type.INTEGER);
      database.getSchema().createEdgeType("L");
    });
  }

  @Test
  void aTreeNeverLeadsBackToTheStart() {
    graph(6, "0>1", "1>2", "1>3", "0>4", "4>5");
    for (int max = 1; max <= 6; max++)
      assertStartIsEnd(max, false);
    // walking back along the same relationship is allowed to a WALK
    assertThat(ends("MATCH WALK (a:N {id: 0})-[:L*1..2]-(b:N) RETURN DISTINCT b.id AS v ORDER BY v")).contains(0);
  }

  @Test
  void aSelfLoopIsAClosedTrailOfOneRelationship() {
    graph(3, "0>1", "0>0", "1>2");
    for (int max = 1; max <= 4; max++)
      assertStartIsEnd(max, true);
  }

  @Test
  void aSelfLoopElsewhereDoesNotBringTheStartBack() {
    graph(3, "0>1", "1>1", "1>2");
    for (int max = 1; max <= 5; max++)
      assertStartIsEnd(max, false);
  }

  @Test
  void parallelAndAntiparallelEdgesAreAClosedTrailOfTwo() {
    graph(3, "0>1", "0>1", "1>2");
    assertStartIsEnd(1, false);
    for (int max = 2; max <= 4; max++)
      assertStartIsEnd(max, true);
    graph(3, "0>1", "1>0", "1>2");
    assertStartIsEnd(1, false);
    for (int max = 2; max <= 4; max++)
      assertStartIsEnd(max, true);
  }

  @Test
  void parallelEdgesBetweenOtherVerticesDoNotBringTheStartBack() {
    graph(4, "0>1", "1>2", "1>2", "2>3");
    for (int max = 1; max <= 6; max++)
      assertStartIsEnd(max, false);
  }

  @Test
  void aTriangleNeedsThreeRelationships() {
    graph(3, "0>1", "1>2", "2>0");
    assertStartIsEnd(1, false);
    assertStartIsEnd(2, false);
    for (int max = 3; max <= 5; max++)
      assertStartIsEnd(max, true);
  }

  @Test
  void aLongCycleNeedsItsWholeLength() {
    graph(6, "0>1", "1>2", "2>3", "3>4", "4>5", "5>0");
    for (int max = 1; max <= 5; max++)
      assertStartIsEnd(max, false);
    assertStartIsEnd(6, true);
    assertStartIsEnd(7, true);
  }

  @Test
  void aCycleThatMissesTheStartDoesNotCount() {
    // 0 hangs off a triangle 1-2-3 by one relationship
    graph(4, "0>1", "1>2", "2>3", "3>1");
    for (int max = 1; max <= 6; max++)
      assertStartIsEnd(max, false);
  }

  @Test
  void twoBranchesThatMeetMakeACycleThroughTheStart() {
    // a diamond 0-1-3 and 0-2-3: the cycle is 4 long, found where the branches meet
    graph(4, "0>1", "0>2", "1>3", "2>3");
    for (int max = 1; max <= 3; max++)
      assertStartIsEnd(max, false);
    for (int max = 4; max <= 6; max++)
      assertStartIsEnd(max, true);
  }

  @Test
  void aCycleThroughTheStartBehindALongTail() {
    // a tail 0-1-2-3 leads to a diamond: the start is on no cycle, vertex 3 is
    graph(8, "0>1", "1>2", "2>3", "3>4", "3>5", "4>6", "5>6", "6>7");
    for (int max = 1; max <= 8; max++)
      assertStartIsEnd(max, false);
  }

  @Test
  void aStartOnTwoCyclesIsFoundThroughTheShorterOne() {
    // a triangle 0-1-2 and a square 0-3-4-5
    graph(6, "0>1", "1>2", "2>0", "0>3", "3>4", "4>5", "5>0");
    assertStartIsEnd(2, false);
    for (int max = 3; max <= 6; max++)
      assertStartIsEnd(max, true);
  }

  @Test
  void edgeFiltersDecideWhichCyclesExist() {
    // the triangle closes only through a relationship with w = 9
    database.transaction(() -> database.command("sql", "DELETE FROM N"));
    database.transaction(() -> {
      final List<MutableVertex> vertices = new ArrayList<>();
      for (int i = 0; i < 3; i++)
        vertices.add(database.newVertex("N").set("id", i).save());
      vertices.get(0).newEdge("L", vertices.get(1)).set("w", 1).save();
      vertices.get(1).newEdge("L", vertices.get(2)).set("w", 1).save();
      vertices.get(2).newEdge("L", vertices.get(0)).set("w", 9).save();
    });
    final String closed = "MATCH (a:N {id: 0})-[:L*1..3]-(b:N) RETURN DISTINCT b.id AS v ORDER BY v";
    final String open = "MATCH (a:N {id: 0})-[:L*1..3 {w: 1}]-(b:N) RETURN DISTINCT b.id AS v ORDER BY v";
    assertThat(planOf(open)).contains(ENDPOINTS);
    assertThat(ends(closed)).containsExactly(0, 1, 2);
    assertThat(ends(open)).containsExactly(1, 2);
  }

  /** The answer for the start vertex 0 and the given upper bound, from the visited set and from path enumeration. */
  private void assertStartIsEnd(final int max, final boolean expected) {
    final String eligible = "MATCH (a:N {id: 0})-[:L*1.." + max + "]-(b:N) RETURN DISTINCT b.id AS v ORDER BY v";
    final String reference = "MATCH p = (a:N {id: 0})-[:L*1.." + max + "]-(b:N) RETURN DISTINCT b.id AS v ORDER BY v";
    assertThat(planOf(eligible)).as(eligible).contains(ENDPOINTS);
    assertThat(planOf(reference)).as(reference).doesNotContain(ENDPOINTS);
    final List<Object> answer = ends(eligible);
    assertThat(answer).as(eligible).isEqualTo(ends(reference));
    assertThat(answer.contains(0)).as("%s: is the start its own end", eligible).isEqualTo(expected);
  }

  /** Replaces the graph with {@code vertices} vertices (id 0..n-1) and one L edge per {@code "from>to"} entry. */
  private void graph(final int vertices, final String... edges) {
    database.transaction(() -> {
      database.command("sql", "DELETE FROM N");
      final List<MutableVertex> created = new ArrayList<>();
      for (int i = 0; i < vertices; i++)
        created.add(database.newVertex("N").set("id", i).save());
      for (final String edge : edges) {
        final String[] ends = edge.split(">");
        created.get(Integer.parseInt(ends[0])).newEdge("L", created.get(Integer.parseInt(ends[1]))).save();
      }
    });
  }

  private String planOf(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", "EXPLAIN " + query)) {
      return String.valueOf(resultSet.next().<Object>getProperty("executionPlan"));
    }
  }

  private List<Object> ends(final String query) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext())
        values.add(resultSet.next().<Object>getProperty("v"));
    }
    return values;
  }
}
