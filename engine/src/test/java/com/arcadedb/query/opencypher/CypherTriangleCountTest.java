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

import org.junit.jupiter.api.Nested;
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
 * Triangle-shaped MATCH patterns: labels, directions, self loops and partitioned types.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherTriangleCountTest {
  /**
   * Issues #9387 and #9388: the country-partitioned triangle count push-down (LSQB Q3) walked the KNOWS hops as undirected whatever
   * direction the query wrote, and ignored the labels of the three persons (and of the chain nodes). The oracle is the same pattern
   * behind a WITH (the row pipeline), which is the Cypher answer.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9387TriangleLabelsAndDirection extends TestHelper {
    private static final String CHAINS = "MATCH (country:Country) MATCH (person1:Person)-[:IS_LOCATED_IN]->(city1:City)-[:IS_PART_OF]->(country) "
        + "MATCH (person2:Person)-[:IS_LOCATED_IN]->(city2:City)-[:IS_PART_OF]->(country) MATCH (person3:Person)-[:IS_LOCATED_IN]->(city3:City)-[:IS_PART_OF]->(country) ";
    private static final String UNDIRECTED = CHAINS + "MATCH (person1)-[:KNOWS]-(person2)-[:KNOWS]-(person3)-[:KNOWS]-(person1)";
    private static final String DIRECTED = CHAINS + "MATCH (person1)-[:KNOWS]->(person2)-[:KNOWS]->(person3)-[:KNOWS]->(person1)";
    private static final String VARS = "country, person1, person2, person3, city1, city2, city3";

    @Override
    protected void beginTest() {
      for (final String t : new String[] { "Person", "Company", "City", "Country" })
        database.command("sql", "CREATE VERTEX TYPE " + t);
      for (final String e : new String[] { "KNOWS", "IS_LOCATED_IN", "IS_PART_OF" })
        database.command("sql", "CREATE EDGE TYPE " + e);
    }

    private RID[] graph(final String thirdType) {
      final RID[] p = new RID[3];
      database.transaction(() -> {
        final RID country = database.newVertex("Country").save().getIdentity();
        final RID city = database.newVertex("City").save().getIdentity();
        city.asVertex().newEdge("IS_PART_OF", country);
        p[0] = database.newVertex("Person").save().getIdentity();
        p[1] = database.newVertex("Person").save().getIdentity();
        p[2] = database.newVertex(thirdType).save().getIdentity();
        for (final RID x : p)
          x.asVertex().newEdge("IS_LOCATED_IN", city);
      });
      return p;
    }

    private void knows(final RID from, final RID to) {
      database.transaction(() -> from.asVertex().newEdge("KNOWS", to));
    }

    @Test
    void triangleThroughNonPersonIsNotCounted() throws InterruptedException {
      final RID[] p = graph("Company");
      knows(p[0], p[1]);
      knows(p[1], p[2]);
      knows(p[2], p[0]);
      assertAllAgree(UNDIRECTED, 0L);
    }

    @Test
    void triangleOfPersonsIsCounted() throws InterruptedException {
      final RID[] p = graph("Person");
      knows(p[0], p[1]);
      knows(p[1], p[2]);
      knows(p[2], p[0]);
      assertAllAgree(UNDIRECTED, 6L);
    }

    @Test
    void directedPatternDoesNotCountUndirectedTriangle() throws InterruptedException {
      final RID[] p = graph("Person");
      knows(p[0], p[1]);
      knows(p[1], p[2]);
      knows(p[0], p[2]);
      assertAllAgree(DIRECTED, 0L);
    }

    @Test
    void directedCycleIsCounted() throws InterruptedException {
      final RID[] p = graph("Person");
      knows(p[0], p[1]);
      knows(p[1], p[2]);
      knows(p[2], p[0]);
      assertAllAgree(DIRECTED, 3L);
    }

    @Test
    void chainLabelsAreEnforced() throws InterruptedException {
      // the cities are not Company vertices: the partition chain has no match
      final RID[] p = graph("Person");
      knows(p[0], p[1]);
      knows(p[1], p[2]);
      knows(p[2], p[0]);
      final String wrongCity = UNDIRECTED.replace("(city1:City)", "(city1:Company)").replace("(city2:City)", "(city2:Company)")
          .replace("(city3:City)", "(city3:Company)");
      assertAllAgree(wrongCity, 0L);
    }

    @Test
    void anchorLabelIsEnforced() throws InterruptedException {
      final RID[] p = graph("Person");
      knows(p[0], p[1]);
      knows(p[1], p[2]);
      knows(p[2], p[0]);
      assertAllAgree(UNDIRECTED.replace("(country:Country)", "(country:Company)"), 0L);
    }

    @Test
    void subTypeOfTheLabelIsAccepted() throws InterruptedException {
      database.command("sql", "CREATE VERTEX TYPE Employee EXTENDS Person");
      final RID[] p = graph("Employee");
      knows(p[0], p[1]);
      knows(p[1], p[2]);
      knows(p[2], p[0]);
      assertAllAgree(UNDIRECTED, 6L);
    }

    @Test
    void ambiguousChainWithLabelsIsCountedExactly() throws InterruptedException {
      // a second city in the same country for one person: two paths to the country, one of them through a rejected label
      final RID[] p = graph("Person");
      database.transaction(() -> {
        final RID country = database.newVertex("Country").save().getIdentity();
        final RID other = database.newVertex("Company").save().getIdentity();
        other.asVertex().newEdge("IS_PART_OF", country);
        p[0].asVertex().newEdge("IS_LOCATED_IN", other);
      });
      knows(p[0], p[1]);
      knows(p[1], p[2]);
      knows(p[2], p[0]);
      assertAllAgree(UNDIRECTED, 6L);
    }

    @Test
    void labelOnSomeCycleVariablesOnlyIsEnforced() throws InterruptedException {
      final RID[] p = graph("Company");
      knows(p[0], p[1]);
      knows(p[1], p[2]);
      knows(p[2], p[0]);
      // person1 has no label: the Company can only be person1, in the two orders the other two persons take
      assertAllAgree(UNDIRECTED.replace("(person1:Person)-[:IS_LOCATED_IN]", "(person1)-[:IS_LOCATED_IN]"), 2L);
    }

    @Test
    void conflictingLabelsOnOneVariableDeclineThePushDown() throws InterruptedException {
      final RID[] p = graph("Person");
      knows(p[0], p[1]);
      knows(p[1], p[2]);
      knows(p[2], p[0]);
      // person1 is a Person in its chain and a Company in the cycle: nothing is both
      final String conflicting = UNDIRECTED.replace("MATCH (person1)-[:KNOWS]-", "MATCH (person1:Company)-[:KNOWS]-");
      assertAllAgree(conflicting, 0L);
    }

    @Test
    void labelOnTheChainEndOfOneChainOnlyIsEnforced() throws InterruptedException {
      final RID[] p = graph("Person");
      knows(p[0], p[1]);
      knows(p[1], p[2]);
      knows(p[2], p[0]);
      // only the second chain labels its end as a Company, which the country is not: the three chains share the anchor
      final String mixed = UNDIRECTED.replaceFirst("\\(city2:City\\)-\\[:IS_PART_OF\\]->\\(country\\)",
          "(city2:City)-[:IS_PART_OF]->(country:Company)");
      assertAllAgree(mixed, 0L);
    }

    private void assertAllAgree(final String match, final long expected) throws InterruptedException {
      assertThat(count(match + " WITH " + VARS + " RETURN count(*) AS n")).as("row pipeline").isEqualTo(expected);
      final String written = match + " RETURN count(*) AS n";
      assertThat(count(written)).as("no view").isEqualTo(expected);

      createView("narrow", "VERTEX TYPES (Person) EDGE TYPES (KNOWS) UPDATE MODE OFF");
      assertThat(count(written)).as("narrow view").isEqualTo(expected);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW narrow");

      createView("wide", "VERTEX TYPES (Person, Company, City, Country) EDGE TYPES (KNOWS, IS_LOCATED_IN, IS_PART_OF) UPDATE MODE OFF");
      assertThat(count(written)).as("wide view").isEqualTo(expected);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW wide");
    }

    private void createView(final String name, final String definition) throws InterruptedException {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + name + " " + definition);
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, name);
      final long deadline = System.currentTimeMillis() + 60_000;
      while (!view.isReady() && System.currentTimeMillis() < deadline)
        Thread.sleep(20);
      assertThat(view.isReady()).isTrue();
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }

  /**
   * Issue #9486: the partitioned triangle count push-down (LSQB q3) listed a self loop in both the out and the in adjacency of
   * its vertex, so every hop matched it twice, and nothing checked that the three relationships differ (relationship
   * uniqueness inside one MATCH clause). One loop answered 8 where the row pipeline answers 0. The oracle is the same text
   * behind a WITH, which runs the row pipeline.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9486PartitionedTriangleSelfLoop extends TestHelper {
    private static final String HEAD = "MATCH (t:Tag) MATCH (p1:P)-[:IN]->(t) MATCH (p2:P)-[:IN]->(t) MATCH (p3:P)-[:IN]->(t) ";
    private static final String TAIL = "MATCH (p1)-[:K]-(p2)-[:K]-(p3)-[:K]-(p1) ";
    private static final String VARS = "t, p1, p2, p3";

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE VERTEX TYPE Tag");
      database.command("sql", "CREATE EDGE TYPE K");
      database.command("sql", "CREATE EDGE TYPE IN");
    }

    @Test
    void onlySelfLoops() throws InterruptedException {
      for (int k = 1; k <= 4; k++) {
        reset();
        final int loops = k;
        database.transaction(() -> {
          final RID p = database.newVertex("P").set("id", 1).save().getIdentity();
          final RID tag = database.newVertex("Tag").set("id", 1).save().getIdentity();
          p.asVertex().newEdge("IN", tag);
          for (int i = 0; i < loops; i++)
            p.asVertex().newEdge("K", p);
        });
        final long expected = (long) k * (k - 1) * (k - 2);
        assertThat(count(HEAD + TAIL + "RETURN count(p1) AS n")).as(k + " loops, count(p1)").isEqualTo(expected);
        assertThat(count(HEAD + "WITH " + VARS + " " + TAIL + "RETURN count(*) AS n")).as(k + " loops, WITH").isEqualTo(expected);
        assertAllViewsAgree(expected);
      }
    }

    @Test
    void loopsNextToRealTrianglesAndParallelEdges() throws InterruptedException {
      final RID[] p = new RID[4];
      database.transaction(() -> {
        final RID tag = database.newVertex("Tag").set("id", 1).save().getIdentity();
        for (int i = 0; i < p.length; i++) {
          p[i] = database.newVertex("P").set("id", i).save().getIdentity();
          p[i].asVertex().newEdge("IN", tag);
        }
        // triangle 0-1-2, a loop on 0 and two on 1, a parallel edge 0-1 and a pendant 3
        p[0].asVertex().newEdge("K", p[1]);
        p[1].asVertex().newEdge("K", p[2]);
        p[2].asVertex().newEdge("K", p[0]);
        p[0].asVertex().newEdge("K", p[0]);
        p[1].asVertex().newEdge("K", p[1]);
        p[1].asVertex().newEdge("K", p[1]);
        p[0].asVertex().newEdge("K", p[1]);
        p[3].asVertex().newEdge("K", p[0]);
      });
      final long expected = count(HEAD + "WITH " + VARS + " " + TAIL + "RETURN count(*) AS n");
      assertThat(count(HEAD + TAIL + "RETURN count(p1) AS n")).isEqualTo(expected);
      assertAllViewsAgree(expected);
    }

    @Test
    void randomGraphsWithLoopsAgreeWithTheRowPipeline() throws InterruptedException {
      randomGraphs(false);
    }

    @Test
    void randomGraphsWithLoopsAndAmbiguousPartitionsAgreeWithTheRowPipeline() throws InterruptedException {
      randomGraphs(true);
    }

    private void randomGraphs(final boolean ambiguous) throws InterruptedException {
      final Random random = new Random(9486L);
      for (int seed = 0; seed < 12; seed++) {
        reset();
        final int persons = 3 + random.nextInt(4);
        final int edges = 4 + random.nextInt(10);
        final int loopBias = random.nextInt(3);
        database.transaction(() -> {
          final RID tag = database.newVertex("Tag").set("id", 1).save().getIdentity();
          final RID otherTag = database.newVertex("Tag").set("id", 2).save().getIdentity();
          final RID[] p = new RID[persons];
          for (int i = 0; i < persons; i++) {
            p[i] = database.newVertex("P").set("id", i).save().getIdentity();
            p[i].asVertex().newEdge("IN", tag);
            // a person in two tags makes the partition chain ambiguous, which takes the weighted path
            if (ambiguous && random.nextBoolean())
              p[i].asVertex().newEdge("IN", otherTag);
          }
          for (int e = 0; e < edges; e++) {
            final int from = random.nextInt(persons);
            final int to = random.nextInt(3) < loopBias ? from : random.nextInt(persons);
            p[from].asVertex().newEdge("K", p[to]);
          }
        });
        final long expected = count(HEAD + "WITH " + VARS + " " + TAIL + "RETURN count(*) AS n");
        assertThat(count(HEAD + TAIL + "RETURN count(*) AS n")).as("seed " + seed + " no view").isEqualTo(expected);
        if (seed < 4)
          assertAllViewsAgree(expected);
      }
    }

    private void reset() {
      database.transaction(() -> {
        database.command("sql", "DELETE FROM K");
        database.command("sql", "DELETE FROM P");
        database.command("sql", "DELETE FROM Tag");
      });
    }

    private void assertAllViewsAgree(final long expected) throws InterruptedException {
      assertThat(count(HEAD + TAIL + "RETURN count(*) AS n")).as("no view").isEqualTo(expected);

      createView("narrow", "VERTEX TYPES (P) EDGE TYPES (K) PROPERTIES (id) UPDATE MODE OFF");
      assertThat(count(HEAD + TAIL + "RETURN count(*) AS n")).as("narrow view").isEqualTo(expected);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW narrow");

      createView("wide", "VERTEX TYPES (P, Tag) EDGE TYPES (K, IN) PROPERTIES (id) UPDATE MODE OFF");
      assertThat(count(HEAD + TAIL + "RETURN count(*) AS n")).as("wide view").isEqualTo(expected);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW wide");
    }

    private void createView(final String name, final String definition) throws InterruptedException {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + name + " " + definition);
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, name);
      final long deadline = System.currentTimeMillis() + 60_000;
      while (!view.isReady() && System.currentTimeMillis() < deadline)
        Thread.sleep(20);
      assertThat(view.isReady()).isTrue();
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }

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
  @Nested
  class Issue10255SingleMatchTriangleCount extends TestHelper {
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
}
