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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.PathMode;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.opencypher.traversal.VariableLengthPathTraverser;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #9983: the within-path relationship uniqueness of the variable length and quantified path
 * forms compared lightweight edges by RID, and two parallel lightweight edges of one type over the same ordered pair
 * share their RID, so a path walking one and then its twin was dropped as if it reused a relationship. #9573 fixed the
 * same defect for the fixed-length hop-by-hop pipeline only.
 * <p>
 * Every pattern form runs over the same fixture built twice, once with lightweight and once with record-backed
 * edges: record-backed edges were never affected, so the two must agree, and both must give the count enumerated by
 * hand (each stored edge being a relationship of its own).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9983VarLengthParallelLightEdgesTest extends TestHelper {

  private enum Fixture {
    /** two parallel K edges a->b */
    TWO_PARALLEL,
    /** three parallel K edges a->b */
    THREE_PARALLEL,
    /** two parallel K self loops on a */
    TWO_LOOPS
  }

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
  }

  static Stream<Arguments> patterns() {
    final List<Arguments> cases = new ArrayList<>();
    for (final boolean lightweight : new boolean[] { true, false }) {
      // a-e1-b, a-e2-b: a two hop trail walks one edge and comes back by the other, from either end, in either order
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K]-(y:P)-[:K]-(z:P) RETURN z", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K*2]-(z:P) RETURN z", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K*2..2]-(z:P) RETURN z", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K*1..2]-(z:P) RETURN z", 8);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K*]-(z:P) RETURN z", 8);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K*2]-(z:P) RETURN r", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH p = (x:P)-[:K*2]-(z:P) RETURN p", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K*2]-(z:P) WITH * RETURN z", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K*2]->(z:P) RETURN z", 0);
      // WALK skips the uniqueness check: every ordered pair of edges, the same one twice included
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH WALK (x:P)-[:K*2]-(z:P) RETURN z", 8);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)((u:P)-[:K]-(v:P)){2}(z:P) RETURN z", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)((u:P)-[:K]-(v:P)){1,2}(z:P) RETURN z", 8);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH ((u:P)-[:K]-(v:P)){2} RETURN v", 4);
      // pattern predicate and pattern comprehension evaluate the variable length pattern through their own traverser
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) WHERE (x)-[:K*2]-(x) RETURN x", 2);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) UNWIND [(x)-[:K*2]-(z) | z] AS z RETURN z", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) UNWIND [(x)-[:K]-(y)-[:K]-(z) | z] AS z RETURN z", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) WHERE (x)-[:K]-()-[:K]-(x) RETURN x", 2);
      // uniqueness spans the hops of the pattern, a fixed hop and a variable length one included
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) UNWIND [(x)-[:K]-(y)-[:K*1]-(z) | z] AS z RETURN z", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) UNWIND [(x)-[:K*1]-(y)-[:K*1]-(z) | z] AS z RETURN z", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) UNWIND [p = (x)-[:K]-()-[:K]-() | p] AS p RETURN p", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) WHERE (x)-[:K]-()-[:K*1]-(x) RETURN x", 2);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K]-(y:P)-[:K*1]-(z:P) RETURN z", 4);
      // relationship uniqueness across the patterns of one MATCH: the second pattern may take the twin of the first's
      // edge, never the edge itself
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]-(y:P), (y)-[:K*1]-(z:P) RETURN z", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]-(y:P), (y)-[r2:K*1]-(z:P) RETURN z", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K*1]-(y:P), (y)-[r2:K*1]-(z:P) RETURN z", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]-(y:P)((u:P)-[:K]-(v:P)){1}(z:P) RETURN z", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]-(y:P), (y)((u:P)-[:K]-(v:P)){1}(z:P) RETURN z", 4);
      // a relationship list bound by an earlier clause is matched as the whole path, from either end of the walk
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH ()-[r:K*2]-() WITH r MATCH (x:P)-[r*2]-(z:P) RETURN z", 8);

      // three copies: ordered sequences of distinct copies, alternating a and b, from either end
      add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[:K]-(y:P)-[:K]-(z:P) RETURN z", 12);
      add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[:K*2]-(z:P) RETURN z", 12);
      add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[:K*3]-(z:P) RETURN z", 12);
      add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[:K*4]-(z:P) RETURN z", 0);
      add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[:K*1..3]-(z:P) RETURN z", 30);
      add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)((u:P)-[:K]-(v:P)){3}(z:P) RETURN z", 12);
      add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)((u:P)-[:K]-(v:P)){1,3}(z:P) RETURN z", 30);
      add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)((u:P)-[:K]-(v:P)-[:K]-(w:P)){1}(z:P) RETURN z", 12);
      add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P) UNWIND [(x)-[:K]-()-[:K]-()-[:K]-(z) | z] AS z RETURN z", 12);
      add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P) UNWIND [(x)-[:K*3]-(z) | z] AS z RETURN z", 12);

      // two loops: (l1, l2) and (l2, l1)
      add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[:K]->(y:P)-[:K]->(z:P) RETURN z", 2);
      add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[:K*2]->(z:P) RETURN z", 2);
      add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[:K*1..2]->(z:P) RETURN z", 4);
      add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[:K*2]-(z:P) RETURN z", 2);
      add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[:K*1..2]-(z:P) RETURN z", 4);
      add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[:K]-(z:P) RETURN z", 2);
      add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P) UNWIND [(x)-[:K*2]-(z) | z] AS z RETURN z", 2);
      add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P) UNWIND [(x)-[:K]-(z) | z] AS z RETURN z", 2);
      add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[:K*3]->(z:P) RETURN z", 0);
      add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)((u:P)-[:K]->(v:P)){2}(z:P) RETURN z", 2);
    }
    return cases.stream();
  }

  private static void add(final List<Arguments> cases, final Fixture fixture, final boolean lightweight, final String query,
      final int expected) {
    cases.add(Arguments.of(fixture, lightweight, query, expected));
  }

  @ParameterizedTest(name = "{0} lightweight={1}: {2} -> {3}")
  @MethodSource("patterns")
  void everyPatternFormCountsParallelEdgesAsDistinctRelationships(final Fixture fixture, final boolean lightweight,
      final String query, final int expected) {
    build(fixture, lightweight);
    assertThat(rows(query)).as(query).isEqualTo(expected);
  }

  @Test
  void countPushDownAgreesWithTheRows() {
    build(Fixture.TWO_PARALLEL, true);
    assertThat(count("MATCH (x:P)-[:K*2]-(z:P) RETURN count(*) AS n")).isEqualTo(4L);
    assertThat(count("MATCH (x:P)-[:K*1..2]-(z:P) RETURN count(*) AS n")).isEqualTo(8L);
    assertThat(count("MATCH (x:P)((u:P)-[:K]-(v:P)){2}(z:P) RETURN count(*) AS n")).isEqualTo(4L);
  }

  @Test
  void aSingleLightEdgeIsStillNeverWalkedTwice() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("P").save();
      a.newLightEdge("K", b);
    });
    assertThat(rows("MATCH (x:P)-[:K*2]-(z:P) RETURN z")).isZero();
    assertThat(rows("MATCH (x:P)-[:K*1..3]-(z:P) RETURN z")).isEqualTo(2);
    assertThat(rows("MATCH (x:P)((u:P)-[:K]-(v:P)){2}(z:P) RETURN z")).isZero();
    assertThat(rows("MATCH (x:P)-[r1:K]-(y:P), (y)-[:K*1]-(z:P) RETURN z")).isZero();
    assertThat(rows("MATCH (x:P)-[r1:K]-(y:P)((u:P)-[:K]-(v:P)){1}(z:P) RETURN z")).isZero();
  }

  @Test
  void aLightEdgeParallelToARecordBackedOne() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("P").save();
      a.newLightEdge("K", b);
      a.newEdge("K", b);
    });
    assertThat(rows("MATCH (x:P)-[:K*2]-(z:P) RETURN z")).isEqualTo(4);
    assertThat(rows("MATCH (x:P)((u:P)-[:K]-(v:P)){2}(z:P) RETURN z")).isEqualTo(4);
  }

  @Test
  void twinsSurviveAReopen() {
    build(Fixture.THREE_PARALLEL, true);
    reopenDatabase();
    assertThat(rows("MATCH (x:P)-[:K*1..3]-(z:P) RETURN z")).isEqualTo(30);
    assertThat(rows("MATCH (x:P)((u:P)-[:K]-(v:P)){1,3}(z:P) RETURN z")).isEqualTo(30);
  }

  /**
   * The planner always picks the depth-first strategy (#6097), so the breadth-first one is driven directly: it runs the
   * same uniqueness check and must agree.
   */
  @Test
  void breadthAndDepthFirstTraversersAgree() {
    build(Fixture.THREE_PARALLEL, true);
    database.transaction(() -> {
      final Vertex a = database.query("opencypher", "MATCH (x:P {name:'a'}) RETURN x").next().getProperty("x");
      for (final boolean bfs : new boolean[] { true, false }) {
        // from a: 3 one-hop paths, 3*2 two-hop paths, 3*2*1 three-hop paths
        assertThat(paths(a, 1, 3, bfs)).as("bfs=" + bfs).isEqualTo(15);
        assertThat(paths(a, 3, 3, bfs)).as("bfs=" + bfs).isEqualTo(6);
      }
    });
  }

  private int paths(final Vertex start, final int minHops, final int maxHops, final boolean bfs) {
    final VariableLengthPathTraverser traverser = new VariableLengthPathTraverser(Direction.BOTH, new String[] { "K" }, null,
        minHops, maxHops, true, bfs, PathMode.TRAIL);
    int found = 0;
    for (final Iterator<TraversalPath> it = traverser.traversePaths(start); it.hasNext(); it.next())
      ++found;
    return found;
  }

  private void build(final Fixture fixture, final boolean lightweight) {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").set("name", "a").save();
      final MutableVertex b = database.newVertex("P").set("name", "b").save();
      final int copies = fixture == Fixture.THREE_PARALLEL ? 3 : 2;
      final MutableVertex to = fixture == Fixture.TWO_LOOPS ? a : b;
      for (int i = 0; i < copies; i++)
        if (lightweight)
          a.newLightEdge("K", to);
        else
          a.newEdge("K", to);
    });
  }

  private int rows(final String query) {
    int found = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        ++found;
      }
    }
    return found;
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
