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
import com.arcadedb.database.RID;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10255: a triangle written as one MATCH, with no WHERE or with the symmetry-breaking order on one property, ran
 * through the row pipeline, one row per wedge, about 50 times slower than {@code algo.triangleCount}. It is now answered by
 * {@code TriangleCountOp}, with the pattern's own semantics: a parallel relationship is a match of its own, the comparison
 * is on the property values (equal values drop the triangle, values of two types compare to null), and without a WHERE the
 * walks over self loops count too. The oracle is the same MATCH aggregated with {@code sum(1)}, which no count push-down
 * takes, so the row pipeline answers it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10255SingleMatchTriangleCountTest extends TestHelper {
  private static final String TRIANGLE = "MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a)";
  private static final String ORDERED  = TRIANGLE + " WHERE a.id < b.id AND b.id < c.id";
  private static final String PUSHED   = "COUNT TRIANGLES (CSR sorted intersection)";

  private RID[] persons;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE VERTEX TYPE Robot");
    database.command("sql", "CREATE EDGE TYPE KNOWS");
  }

  @Test
  void orderedTriangleIsPushedDownAndCountsEachTriangleOnce() throws InterruptedException {
    // a 4-clique plus a pendant edge: 4 triangles
    createPersons(5, i -> (long) i);
    for (final int[] e : new int[][] { { 0, 1 }, { 0, 2 }, { 0, 3 }, { 1, 2 }, { 1, 3 }, { 2, 3 }, { 3, 4 } })
      knows(e[0], e[1]);

    assertAllAgree(ORDERED, 4L);
    assertAllAgree(TRIANGLE, 24L);
    assertThat(explain(ORDERED + " RETURN count(*) AS n")).contains(PUSHED).contains("a.id < b.id < c.id");
  }

  @Test
  void everySpellingOfTheStrictOrderIsTaken() throws InterruptedException {
    createPersons(5, i -> (long) i);
    for (final int[] e : new int[][] { { 0, 1 }, { 0, 2 }, { 0, 3 }, { 1, 2 }, { 1, 3 }, { 2, 3 }, { 3, 4 } })
      knows(e[0], e[1]);

    for (final String where : new String[] { "b.id > a.id AND c.id > b.id", "c.id < a.id AND a.id < b.id",
        "a.id < b.id AND b.id < c.id AND a.id < c.id", "c.id > b.id AND a.id < b.id",
        "a.id < b.id AND a.id < b.id AND b.id < c.id" }) {
      final String query = TRIANGLE + " WHERE " + where;
      assertThat(explain(query + " RETURN count(*) AS n")).as(where).contains(PUSHED);
      assertAllAgree(query, 4L);
    }
  }

  @Test
  void parallelAndReciprocalRelationshipsAreMatchesOfTheirOwn() throws InterruptedException {
    createPersons(3, i -> (long) i);
    knows(0, 1);
    knows(0, 1);
    knows(1, 0);
    knows(1, 2);
    knows(2, 1);
    knows(2, 1);
    knows(2, 0);
    // 3 relationships between 0 and 1, 3 between 1 and 2, 1 between 2 and 0
    assertAllAgree(ORDERED, 9L);
    assertAllAgree(TRIANGLE, 6L * 9);
  }

  @Test
  void selfLoopWalksCountWithoutAWhere() throws InterruptedException {
    createPersons(3, i -> (long) i);
    knows(0, 1);
    knows(1, 2);
    knows(2, 0);
    knows(0, 0);
    knows(0, 0);
    knows(0, 1);
    knows(1, 1);
    knows(1, 1);
    knows(1, 1);
    // the row pipeline is the oracle of the walks over the loops: two positions on a looped vertex, or all three
    assertAllAgree(TRIANGLE, countWithRowPipeline(TRIANGLE));
    assertAllAgree(ORDERED, 2L);
  }

  @Test
  void equalValuesNullsAndMixedTypesDropTheTriangle() throws InterruptedException {
    // ties, a missing value, strings next to numbers and a NaN: none of them is ordered against the others
    final Object[] ids = { 1L, 1L, 2, null, "x", "y", 2.5d, Double.NaN, 3L, "z", true, false };
    createPersons(ids.length, i -> ids[i]);
    connectRandomly(new Random(11), 60, true);

    assertAllAgree(ORDERED, countWithRowPipeline(ORDERED));
    assertAllAgree(TRIANGLE, countWithRowPipeline(TRIANGLE));
  }

  @Test
  void valuesTheOperatorDoesNotRankAreComparedByTheWhere() throws InterruptedException {
    // dates are compared by the comparisons of the WHERE itself, triangle by triangle
    createPersons(12, i -> i % 5 == 4 ? null : LocalDate.of(2020, 1, 1 + i % 7));
    connectRandomly(new Random(5), 50, true);

    final long expected = countWithRowPipeline(ORDERED);
    assertThat(expected).isPositive();
    assertAllAgree(ORDERED, expected);
  }

  @Test
  void longsPastTwoToTheFiftyThirdNextToDoublesAreComparedByTheWhere() throws InterruptedException {
    // 2^53 + 1 and 2^53 as a double collapse in double precision: only the exact comparison of the WHERE tells them apart
    final long twoTo53 = 1L << 53;
    final Object[] ids = { twoTo53 + 1, twoTo53 + 2, (double) twoTo53, twoTo53 + 3, 1.5d, twoTo53 - 1 };
    createPersons(ids.length, i -> ids[i]);
    for (int x = 0; x < ids.length; x++)
      for (int y = x + 1; y < ids.length; y++)
        knows(x, y);

    final long expected = countWithRowPipeline(ORDERED);
    assertThat(expected).isEqualTo(20L);
    assertAllAgree(ORDERED, expected);
  }

  /**
   * A schemaless property whose first values are doubles gets a DOUBLE column. A FLOAT next to them used to be stored as its
   * binary double, which the comparison of the record (by its decimal form, #7609) does not see: {@code 0.1f} ties with
   * {@code 0.1} on the record and did not through the view.
   */
  @Test
  void viewColumnReadsBackWhatTheRecordHolds() throws InterruptedException {
    final Object[] ids = { 0.1d, 0.3d, 0.1f, 0.2d, 7, 0.25f };
    createPersons(ids.length, i -> ids[i]);
    for (int x = 0; x < ids.length; x++)
      for (int y = x + 1; y < ids.length; y++)
        knows(x, y);

    assertAllAgree(ORDERED, countWithRowPipeline(ORDERED));
  }

  @Test
  void unlabelledTriangleCountsEveryVertex() throws InterruptedException {
    createPersons(8, i -> (long) (i % 4));
    database.transaction(() -> {
      final RID robot = database.newVertex("Robot").set("id", 10L).save().getIdentity();
      robot.asVertex().newEdge("KNOWS", persons[0]);
      robot.asVertex().newEdge("KNOWS", persons[1]);
    });
    connectRandomly(new Random(3), 20, false);
    knows(0, 1);

    final String unlabelled = "MATCH (a)-[:KNOWS]-(b)-[:KNOWS]-(c)-[:KNOWS]-(a) WHERE a.id < b.id AND b.id < c.id";
    assertThat(explain(unlabelled + " RETURN count(*) AS n")).contains(PUSHED);
    assertAllAgree(unlabelled, countWithRowPipeline(unlabelled));
    final String walks = "MATCH (a)-[:KNOWS]-()-[:KNOWS]-()-[:KNOWS]-(a)";
    assertThat(explain(walks + " RETURN count(*) AS n")).contains(PUSHED);
    assertAllAgree(walks, countWithRowPipeline(walks));
  }

  @Test
  void shapesTheOperatorCannotExpressAreLeftToThePipeline() throws InterruptedException {
    createPersons(6, i -> (long) i);
    connectRandomly(new Random(9), 12, false);

    for (final String query : new String[] {
        // not a strict order of all three
        TRIANGLE + " WHERE a.id <= b.id AND b.id <= c.id",
        TRIANGLE + " WHERE a.id < b.id",
        TRIANGLE + " WHERE a.id < b.id AND a.id < c.id",
        TRIANGLE + " WHERE a.id < b.id AND b.id < c.id AND c.id < a.id",
        // two properties, or something else than a comparison
        TRIANGLE + " WHERE a.id < b.id AND b.name < c.name",
        TRIANGLE + " WHERE a.id < b.id OR b.id < c.id",
        // a predicate written inside a hop, which the operator could not apply
        "MATCH (a:Person)-[:KNOWS WHERE a.id > 1]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a) WHERE a.id < b.id AND b.id < c.id",
        "MATCH (a:Person)-[:KNOWS WHERE a.id > 1]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a)",
        // a label on only some positions, or a directed hop
        "MATCH (a:Person)-[:KNOWS]-(b)-[:KNOWS]-(c:Person)-[:KNOWS]-(a) WHERE a.id < b.id AND b.id < c.id",
        "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a) WHERE a.id < b.id AND b.id < c.id" }) {
      assertThat(explain(query + " RETURN count(*) AS n")).as(query).doesNotContain(PUSHED);
      assertThat(count(query + " RETURN count(*) AS n")).as(query).isEqualTo(countWithRowPipeline(query));
    }
  }

  /** The issue's own shape: a declared LONG id over a skewed random graph, large enough to take the parallel path. */
  @Test
  void largeGraphMatchesTheEdgeListAnswer() throws InterruptedException {
    database.command("sql", "CREATE PROPERTY Person.id LONG");
    final int n = 2_000;
    createPersons(n, i -> (long) i);
    final Random random = new Random(7);
    final Set<Long> seen = new HashSet<>();
    final List<int[]> edges = new ArrayList<>();
    while (edges.size() < 12_000) {
      final int x = random.nextInt(n), y = Math.min(random.nextInt(n), random.nextInt(n));
      if (x != y && seen.add(Math.min(x, y) * (long) n + Math.max(x, y)))
        edges.add(new int[] { x, y });
    }
    database.transaction(() -> {
      for (final int[] e : edges)
        persons[e[0]].asVertex().newEdge("KNOWS", persons[e[1]]);
    });

    final List<Set<Integer>> adjacency = new ArrayList<>();
    for (int i = 0; i < n; i++)
      adjacency.add(new HashSet<>());
    for (final int[] e : edges) {
      adjacency.get(e[0]).add(e[1]);
      adjacency.get(e[1]).add(e[0]);
    }
    long expected = 0;
    for (final int[] e : edges) {
      final int lo = Math.min(e[0], e[1]), hi = Math.max(e[0], e[1]);
      for (final int c : adjacency.get(lo))
        if (c > hi && adjacency.get(hi).contains(c))
          expected++;
    }
    assertThat(expected).isPositive();

    assertThat(count(ORDERED + " RETURN count(*) AS n")).as("no view").isEqualTo(expected);
    assertThat(count(TRIANGLE + " RETURN count(*) AS n")).as("no view, every walk").isEqualTo(6 * expected);
    createView("gav", "VERTEX TYPES (Person) EDGE TYPES (KNOWS) PROPERTIES (id) UPDATE MODE OFF");
    try {
      assertThat(explain(ORDERED + " RETURN count(*) AS n")).contains(PUSHED);
      assertThat(count(ORDERED + " RETURN count(*) AS n")).as("view").isEqualTo(expected);
      assertThat(count(TRIANGLE + " RETURN count(*) AS n")).as("view, every walk").isEqualTo(6 * expected);
      assertThat(count("CALL algo.triangleCount(['KNOWS']) YIELD triangles RETURN sum(triangles) / 3 AS n")).isEqualTo(expected);
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav");
    }
  }

  /** A SYNCHRONOUS view holds a commit in its overlay, where it hands out no neighbour view and the values may be overrides. */
  @Test
  void overlayCommitIsCounted() throws InterruptedException {
    createPersons(6, i -> (long) i);
    connectRandomly(new Random(21), 14, false);
    createView("overlay", "VERTEX TYPES (Person) EDGE TYPES (KNOWS) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");
    try {
      assertThat(count(ORDERED + " RETURN count(*) AS n")).isEqualTo(countWithRowPipeline(ORDERED));
      knows(0, 1);
      knows(1, 2);
      knows(2, 0);
      database.transaction(() -> persons[3].asVertex().modify().set("id", 100L).save());
      assertThat(explain(ORDERED + " RETURN count(*) AS n")).contains(PUSHED);
      assertThat(count(ORDERED + " RETURN count(*) AS n")).isEqualTo(countWithRowPipeline(ORDERED));
      assertThat(count(TRIANGLE + " RETURN count(*) AS n")).isEqualTo(countWithRowPipeline(TRIANGLE));
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW overlay");
    }
  }

  private void createPersons(final int n, final IntFunction<Object> id) {
    persons = new RID[n];
    database.transaction(() -> {
      for (int i = 0; i < n; i++) {
        final Object value = id.apply(i);
        persons[i] = (value == null ? database.newVertex("Person") : database.newVertex("Person").set("id", value)).save().getIdentity();
      }
    });
  }

  private void connectRandomly(final Random random, final int edges, final boolean withLoopsAndParallels) {
    database.transaction(() -> {
      for (int i = 0; i < edges; i++) {
        final int x = random.nextInt(persons.length);
        final int y = random.nextInt(persons.length);
        if (x == y && !withLoopsAndParallels)
          continue;
        persons[x].asVertex().newEdge("KNOWS", persons[y]);
      }
    });
  }

  private void knows(final int from, final int to) {
    database.transaction(() -> persons[from].asVertex().newEdge("KNOWS", persons[to]));
  }

  private void assertAllAgree(final String match, final long expected) throws InterruptedException {
    assertThat(countWithRowPipeline(match)).as("row pipeline").isEqualTo(expected);
    final String written = match + " RETURN count(*) AS n";
    assertThat(count(written)).as("no view").isEqualTo(expected);

    createView("narrow", "VERTEX TYPES (Person) EDGE TYPES (KNOWS) PROPERTIES (id) UPDATE MODE OFF");
    try {
      assertThat(count(written)).as("view with the property").isEqualTo(expected);
      // the view's column reads every value back as the record holds it, or leaves it to the record
      assertThat(countWithRowPipeline(match)).as("row pipeline over the view").isEqualTo(expected);
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW narrow");
    }

    createView("bare", "VERTEX TYPES (Person, Robot) EDGE TYPES (KNOWS) UPDATE MODE OFF");
    try {
      assertThat(count(written)).as("view without the property").isEqualTo(expected);
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW bare");
    }
  }

  private long countWithRowPipeline(final String match) {
    return count(match + " RETURN sum(1) AS n");
  }

  private void createView(final String name, final String definition) throws InterruptedException {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + name + " " + definition);
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, name);
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(view.isReady()).isTrue();
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
