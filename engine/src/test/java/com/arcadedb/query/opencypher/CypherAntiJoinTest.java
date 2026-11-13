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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Anti-join planning for inequality and NOT pattern predicates, including OPTIONAL MATCH anti-joins and anti-join chains.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherAntiJoinTest {
  /**
   * Regression test for issue #9203: LSQB Q9 written with {@code p1.id <> p3.id} (or {@code id(p1) <> id(p3)}) instead of
   * {@code p1 <> p3} declined the COUNT ANTI-JOIN CHAIN push-down and ran on the row pipeline, and the pattern predicate
   * {@code NOT (p1)-[:KNOWS]-(p3)} loaded every KNOWS edge record to compare its endpoint. A property inequality stands for the node
   * inequality only when the property is unique and never null over the nodes' type, so the push-down is taken then and only then.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9203AntiJoinPropertyInequality {
    private static final String DB_PATH = "./target/databases/issue9203";
    private static final String CHAIN   = "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) ";

    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      for (final String ddl : new String[] { "CREATE VERTEX TYPE Person", "CREATE PROPERTY Person.id LONG (mandatory true, notnull true)",
          "CREATE INDEX ON Person (id) UNIQUE", "CREATE PROPERTY Person.loose LONG", "CREATE INDEX ON Person (loose) UNIQUE",
          "CREATE PROPERTY Person.plain LONG (mandatory true, notnull true)", "CREATE VERTEX TYPE Tag", "CREATE EDGE TYPE KNOWS",
          "CREATE EDGE TYPE HAS_INTEREST" })
        database.command("sql", ddl);

      final Random r = new Random(11);
      database.begin();
      final RID[] p = new RID[60], t = new RID[40];
      for (int i = 0; i < p.length; i++) {
        final MutableVertex v = database.newVertex("Person").set("id", (long) i).set("loose", (long) i).set("plain", (long) (i % 7));
        v.save();
        p[i] = v.getIdentity();
      }
      for (int i = 0; i < t.length; i++) {
        final MutableVertex v = database.newVertex("Tag");
        v.save();
        t[i] = v.getIdentity();
      }
      final Set<Long> knows = new HashSet<>();
      while (knows.size() < 120) {
        final int a = r.nextInt(p.length), b = r.nextInt(p.length);
        if (a != b && knows.add(Math.min(a, b) * 1000L + Math.max(a, b)))
          p[Math.min(a, b)].asVertex().newEdge("KNOWS", p[Math.max(a, b)]).save();
      }
      for (final RID person : p) {
        final Set<Integer> mine = new HashSet<>();
        while (mine.size() < 5)
          mine.add(r.nextInt(t.length));
        for (final int k : mine)
          person.asVertex().newEdge("HAS_INTEREST", t[k]).save();
      }
      database.commit();
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }

    private boolean pushedDown(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("").contains("ANTI-JOIN");
      }
    }

    @Test
    void uniqueNotNullPropertyInequalityTakesThePushDown() {
      final String nodes = CHAIN + "WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3 RETURN count(*) AS n";
      final String property = CHAIN + "WHERE NOT (p1)-[:KNOWS]-(p3) AND p1.id <> p3.id RETURN count(*) AS n";
      final String function = CHAIN + "WHERE id(p1) <> id(p3) AND NOT (p1)-[:KNOWS]-(p3) RETURN count(*) AS n";

      assertThat(pushedDown(nodes)).isTrue();
      assertThat(pushedDown(property)).isTrue();
      assertThat(pushedDown(function)).isTrue();
      assertThat(count(nodes)).isPositive();
      assertThat(count(property)).isEqualTo(count(nodes));
      assertThat(count(function)).isEqualTo(count(nodes));
    }

    @Test
    void propertyThatDoesNotIdentifyTheNodeKeepsTheRowPipelineAndTheAnswer() {
      // plain is not unique; loose is unique but nullable: neither may stand for p1 <> p3
      for (final String property : new String[] { "plain", "loose" }) {
        final String query = CHAIN + "WHERE NOT (p1)-[:KNOWS]-(p3) AND p1." + property + " <> p3." + property + " RETURN count(*) AS n";
        assertThat(pushedDown(query)).as(property).isFalse();
      }
      final long expected = count(CHAIN + "WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3 RETURN count(*) AS n");
      assertThat(count(CHAIN + "WHERE NOT (p1)-[:KNOWS]-(p3) AND p1.loose <> p3.loose RETURN count(*) AS n")).isEqualTo(expected);
      // p1.plain <> p3.plain also drops the pairs that share a value, so it counts fewer rows
      assertThat(count(CHAIN + "WHERE NOT (p1)-[:KNOWS]-(p3) AND p1.plain <> p3.plain RETURN count(*) AS n")).isLessThan(expected);
    }

    @Test
    void patternPredicateAnswersFromTheSegmentLikeTheEdgeWalk() {
      // an inline property map keeps the edge-record path: both spellings must agree where the map matches every edge or none
      final String segment = "MATCH (a:Person), (b:Person) WHERE a.id < b.id AND NOT (a)-[:KNOWS]-(b) RETURN count(*) AS n";
      final String walked = "MATCH (a:Person), (b:Person) WHERE a.id < b.id AND NOT (a)-[:KNOWS {missing: 1}]-(b) RETURN count(*) AS n";
      final long pairs = 60L * 59 / 2;
      assertThat(count(segment)).isEqualTo(pairs - 120);
      assertThat(count(walked)).isEqualTo(pairs);
      assertThat(count("MATCH (a:Person), (b:Person) WHERE a.id < b.id AND (a)-[:KNOWS]->(b) RETURN count(*) AS n")).isEqualTo(120);
      assertThat(count("MATCH (a:Person), (b:Person) WHERE a.id < b.id AND (a)<-[:KNOWS]-(b) RETURN count(*) AS n")).isZero();
      assertThat(count("MATCH (a:Person), (b:Person) WHERE a.id < b.id AND (b)<-[:KNOWS]-(a) RETURN count(*) AS n")).isEqualTo(120);
      assertThat(count("MATCH (a:Person), (b:Person) WHERE a.id < b.id AND (a)-[:NoSuchType]-(b) RETURN count(*) AS n")).isZero();
      assertThat(count("MATCH (a:Person), (b:Person) WHERE a.id < b.id AND (a)-[:NoSuchType|KNOWS]-(b) RETURN count(*) AS n")).isEqualTo(120);
    }

    @Test
    void conjunctIsPlacedOnTheHopThatBindsItsVariables() {
      // p1.loose and p3.loose are bound after the second KNOWS hop: the filter belongs below the HAS_INTEREST hop, not above it
      final String query = CHAIN + "WHERE p1.id < 20 AND p1.loose <> p3.loose RETURN count(*) AS n";
      final String plan;
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        plan = rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
      }
      final int interestHop = plan.indexOf("HAS_INTEREST");
      final int filter = plan.indexOf("Filter [predicate=");
      assertThat(interestHop).isPositive();
      assertThat(filter).as(plan).isGreaterThan(interestHop);
      assertThat(count(query)).isPositive().isEqualTo(count(CHAIN + "WHERE p1.id < 20 AND p1 <> p3 RETURN count(*) AS n"));
      // a predicate that needs the last hop stays above it, and a mixed WHERE keeps both parts
      assertThat(count(CHAIN + "WHERE p1.id < 20 AND p1.loose <> p3.loose AND id(t) <> id(p1) AND p2.id > 10 RETURN count(*) AS n"))
          .isEqualTo(count(CHAIN + "WHERE p1.id < 20 AND p1 <> p3 AND p2.id > 10 RETURN count(*) AS n"));
    }

    @Test
    void whereIsStillAppliedOnShapesTheOptimizerMayNotPlan() {
      // OPTIONAL MATCH and a MATCH after WITH are not planned by the optimizer: their WHERE must keep filtering
      assertThat(count("MATCH (a:Person) WHERE a.id < 5 OPTIONAL MATCH (a)-[:KNOWS]-(b:Person) WHERE b.id >= 1000 RETURN count(*) AS n"))
          .isEqualTo(5);
      assertThat(count("MATCH (a:Person) WHERE a.id < 5 WITH a MATCH (a)-[:KNOWS]-(b:Person) WHERE b.id >= 1000 RETURN count(*) AS n")).isZero();
      // comma separated patterns with a WHERE spanning them, and a WHERE holding a pattern predicate
      assertThat(count("MATCH (a:Person), (b:Person) WHERE a.id < b.id AND b.id < 4 RETURN count(*) AS n")).isEqualTo(6);
      assertThat(count("MATCH (a:Person)-[:KNOWS]-(b:Person) WHERE a.id < b.id AND NOT (b)-[:KNOWS]-(a) RETURN count(*) AS n")).isZero();
      assertThat(count("MATCH (a:Person)-[:KNOWS]-(b:Person) WHERE a.id < b.id AND EXISTS { (b)-[:HAS_INTEREST]->(:Tag) } RETURN count(*) AS n"))
          .isEqualTo(count("MATCH (a:Person)-[:KNOWS]-(b:Person) WHERE a.id < b.id RETURN count(*) AS n"));
    }

    @Test
    void idOfARelationshipIsNotANodeInequality() {
      final String query = "MATCH (p1:Person)-[r:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person) WHERE NOT (p1)-[:KNOWS]-(p3) AND id(p1) <> id(r) RETURN count(*) AS n";
      assertThat(pushedDown(query)).isFalse();
      assertThat(count(query)).isEqualTo(count("MATCH (p1:Person)-[r:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person) WHERE NOT (p1)-[:KNOWS]-(p3) RETURN count(*) AS n"));
    }

    @Test
    void secondMatchAndVariableLengthHopKeepTheirWhere() {
      assertThat(count("MATCH (a:Person) WHERE a.id < 5 MATCH (a)-[:KNOWS]-(b:Person) WHERE a.id > 1000 RETURN count(*) AS n")).isZero();
      assertThat(count("MATCH (a:Person) WHERE a.id < 5 MATCH (a)-[:KNOWS]-(b:Person) WHERE b.id < a.id RETURN count(*) AS n"))
          .isEqualTo(count("MATCH (a:Person)-[:KNOWS]-(b:Person) WHERE a.id < 5 AND b.id < a.id RETURN count(*) AS n"));
      assertThat(count("MATCH (a:Person)-[:KNOWS*1..2]-(b:Person) WHERE a.id < 5 AND b.id >= 1000 RETURN count(*) AS n")).isZero();
      assertThat(count("MATCH (a:Person)-[:KNOWS*1..2]-(b:Person) WHERE a.id < 5 AND b.id < 5 RETURN count(*) AS n")).isPositive();
    }

    @Test
    void patternPredicateHonoursEdgeSubTypesAndSelfLoops() {
      database.command("sql", "CREATE EDGE TYPE BEST_FRIEND EXTENDS KNOWS");
      database.begin();
      final RID a = database.newVertex("Person").set("id", 1000L).set("loose", 1000L).set("plain", 1L).save().getIdentity();
      final RID b = database.newVertex("Person").set("id", 1001L).set("loose", 1001L).set("plain", 1L).save().getIdentity();
      a.asVertex().newEdge("BEST_FRIEND", b).save();
      a.asVertex().newEdge("KNOWS", a).save();
      database.commit();
      // the sub type is found through its parent, in both directions, and a self loop connects a vertex to itself
      assertThat(count("MATCH (a:Person {id: 1000}), (b:Person {id: 1001}) WHERE (a)-[:KNOWS]->(b) RETURN count(*) AS n")).isEqualTo(1);
      assertThat(count("MATCH (a:Person {id: 1000}), (b:Person {id: 1001}) WHERE (b)<-[:KNOWS]-(a) RETURN count(*) AS n")).isEqualTo(1);
      assertThat(count("MATCH (a:Person {id: 1000}), (b:Person {id: 1001}) WHERE (a)<-[:KNOWS]-(b) RETURN count(*) AS n")).isZero();
      assertThat(count("MATCH (a:Person {id: 1000}), (b:Person {id: 1001}) WHERE (a)-[:BEST_FRIEND]->(b) RETURN count(*) AS n")).isEqualTo(1);
      assertThat(count("MATCH (a:Person {id: 1000}) WHERE (a)-[:KNOWS]->(a) RETURN count(*) AS n")).isEqualTo(1);
      assertThat(count("MATCH (a:Person {id: 1001}) WHERE (a)-[:KNOWS]->(a) RETURN count(*) AS n")).isZero();
    }
  }

  /**
   * Regression tests for the COUNT ANTI-JOIN CHAIN push-down: issue #9277 (hop and anti-join neighbour maps reused across hops whose
   * target labels differ) and issue #9278 (a negated pattern carrying an edge property map, an edge variable, an inline WHERE or
   * end node labels/properties was pushed down with those parts dropped). Each query runs as written and with the same WHERE after a
   * WITH (row pipeline); the two must agree.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9277And9278AntiJoinChain {
    private static final String DB_PATH = "./target/databases/issue9277-9278";

    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    @Test
    void crossLabelChainReusesNoMap() {
      for (final String ddl : new String[] { "CREATE VERTEX TYPE Person", "CREATE VERTEX TYPE Employee", "CREATE VERTEX TYPE Tag",
          "CREATE EDGE TYPE KNOWS", "CREATE EDGE TYPE HAS_INTEREST" })
        database.command("sql", ddl);
      database.begin();
      final RID x0 = save(database.newVertex("Person").set("id", 0L)), x1 = save(database.newVertex("Person").set("id", 1L)),
          y0 = save(database.newVertex("Employee").set("id", 0L)), t = save(database.newVertex("Tag"));
      x0.asVertex().newEdge("KNOWS", x1);
      x1.asVertex().newEdge("KNOWS", y0);
      y0.asVertex().newEdge("HAS_INTEREST", t);
      database.commit();

      final String chain = "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Employee)-[:HAS_INTEREST]->(t:Tag) ";
      assertSameCount(chain, "p1, p2, p3, t", "NOT (p1)-[:KNOWS]-(p3)", 1);
      assertSameCount(chain, "p1, p2, p3, t", "NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3", 1);
      assertSameCount(chain, "p1, p2, p3, t", "NOT (p1)-[:KNOWS]-(p3) AND id(p1) <> id(p3)", 1);
      assertSameCount(chain, "p1, p2, p3, t", "NOT (p1)-[:KNOWS]-(p3) AND p1.id <> p3.id", 0);
      // no HAS_INTEREST hop
      assertSameCount("MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Employee) ", "p1, p2, p3", "NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3", 1);
    }

    @Test
    void edgePropertyMapOfNegatedPatternIsHonoured() {
      for (final String ddl : new String[] { "CREATE VERTEX TYPE P", "CREATE VERTEX TYPE Q", "CREATE EDGE TYPE K", "CREATE EDGE TYPE L" })
        database.command("sql", ddl);
      database.begin();
      final RID x = save(database.newVertex("P").set("id", 0L)), y = save(database.newVertex("P").set("id", 1L)),
          z = save(database.newVertex("P").set("id", 2L)), t = save(database.newVertex("Q").set("id", 0L));
      x.asVertex().newEdge("K", y).save();
      y.asVertex().newEdge("K", z).save();
      z.asVertex().newEdge("L", t).save();
      x.asVertex().newEdge("K", z).set("w", 0L).save();
      database.commit();

      final String chain = "MATCH (x:P)-[:K]->(y:P)-[:K]->(z:P)-[:L]->(t:Q) ";
      final String vars = "x, y, z, t";
      assertSameCount(chain, vars, "NOT (x)-[:K {w: 1}]->(z)", 1);
      assertSameCount(chain, vars, "NOT (x)-[:K {w: 0}]->(z)", 0);
      assertSameCount(chain, vars, "NOT (x)-[:K]->(z)", 0);
      assertSameCount(chain, vars, "NOT (x)-[:K {w: 1}]->(z) AND x <> z", 1);
      assertSameCount(chain, vars, "NOT (x)-[:K]->(z:Foo)", 1);
      assertSameCount(chain, vars, "NOT (x:P)-[:K]->(z:P)", 0);
      assertSameCount(chain, vars, "NOT (x:P)-[:K]->(z:Q)", 1);
      assertSameCount(chain, vars, "NOT (x:P:Foo)-[:K]->(z)", 1);
      assertSameCount(chain, vars, "NOT (x)-[:K]->(z {id: 99})", 1);
      assertSameCount(chain, vars, "NOT (x)-[:K]->(z {id: 2})", 0);
    }

    private void assertSameCount(final String chain, final String vars, final String where, final long expected) {
      final long asWritten = count(chain + "WHERE " + where + " RETURN count(*) AS n");
      final long afterWith = count(chain + "WITH " + vars + " WHERE " + where + " RETURN count(*) AS n");
      assertThat(afterWith).as("row pipeline: " + where).isEqualTo(expected);
      assertThat(asWritten).as("as written: " + where).isEqualTo(expected);
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }

    private static RID save(final MutableVertex v) {
      v.save();
      return v.getIdentity();
    }
  }

  /**
   * Regression tests for issue #9290: the COUNT ANTI-JOIN CHAIN push-down answered more than the row pipeline for chains with more than
   * two hops of one type, an inequality between other nodes than the negated pattern's, no inequality, or the negated pattern away from
   * the first node. Every shape must now agree with the row pipeline (the same WHERE after a WITH) and with the expected count, and the
   * shape the push-down is written for (LSQB Q9) must keep it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9290AntiJoinChainShapes {
    private static final String DB_PATH = "./target/databases/issue9290";

    private static final String P3 = "MATCH (p0:Person)-[:KNOWS]-(p1:Person)-[:KNOWS]-(p2:Person)-[:HAS_INTEREST]->(t:Tag) ";
    private static final String P4 = "MATCH (p0:Person)-[:KNOWS]-(p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person) ";

    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      for (final String ddl : new String[] { "CREATE VERTEX TYPE Person", "CREATE VERTEX TYPE Tag", "CREATE EDGE TYPE KNOWS",
          "CREATE EDGE TYPE HAS_INTEREST" })
        database.command("sql", ddl);
      database.begin();
      final RID[] p = new RID[4];
      for (int i = 0; i < 4; i++) {
        final MutableVertex v = database.newVertex("Person").set("id", (long) i);
        v.save();
        p[i] = v.getIdentity();
      }
      final MutableVertex tag = database.newVertex("Tag");
      tag.save();
      for (final int[] e : new int[][] { { 0, 1 }, { 1, 2 }, { 2, 3 }, { 0, 2 } })
        p[e[0]].asVertex().newEdge("KNOWS", p[e[1]]).save();
      for (int i = 0; i < 4; i++)
        p[i].asVertex().newEdge("HAS_INTEREST", tag.getIdentity()).save();
      database.commit();
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    @Test
    void controlLsqbQ9ShapeKeepsPushDown() {
      assertSame(P3, "p0, p1, p2, t", "NOT (p0)-[:KNOWS]-(p2) AND p0 <> p2", 4, true);
      assertSame(P3, "p0, p1, p2, t", "NOT (p2)-[:KNOWS]-(p0) AND p2 <> p0", 4, true);
    }

    @Test
    void unlabelledMiddleNodeWithFirstNodeAsSourceKeepsPushDown() {
      assertSame("MATCH (p0:Person)-[:KNOWS]-(p1)-[:KNOWS]-(p2:Person)-[:HAS_INTEREST]->(t:Tag) ", "p0, p1, p2, t",
          "NOT (p0)-[:KNOWS]-(p2) AND p0 <> p2", 4, true);
    }

    @Test
    void moreThanTwoHopsOfOneType() {
      assertSame(P4, "p0, p1, p2, p3", "NOT (p0)-[:KNOWS]-(p2) AND p0 <> p2", 2, false);
    }

    @Test
    void inequalityBetweenOtherNodes() {
      assertSame(P3, "p0, p1, p2, t", "NOT (p0)-[:KNOWS]-(p2) AND p1 <> p2", 4, false);
    }

    @Test
    void noInequality() {
      assertSame(P3, "p0, p1, p2, t", "NOT (p0)-[:KNOWS]-(p2)", 4, false);
    }

    @Test
    void negatedPatternAwayFromFirstNode() {
      assertSame(P4, "p0, p1, p2, p3", "NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3", 2, false);
    }

    @Test
    void unlabelledMiddleNodeWithFirstNodeAsTarget() {
      assertSame("MATCH (p0:Person)-[:KNOWS]-(p1)-[:KNOWS]-(p2:Person)-[:HAS_INTEREST]->(t:Tag) ", "p0, p1, p2, t",
          "NOT (p2)-[:KNOWS]-(p0) AND p2 <> p0", 4, false);
    }

    @Test
    void labelOfTheSuffixTargetFiltersTheEdgesItCounts() {
      database.command("sql", "CREATE VERTEX TYPE Other");
      database.transaction(() -> {
        final var other = database.newVertex("Other").save();
        for (final var row : database.query("sql", "SELECT FROM Person").stream().map(r -> r.getVertex().get()).toList())
          row.newEdge("HAS_INTEREST", other).save();
      });
      // every person now also has a HAS_INTEREST edge to a vertex that is not a Tag: the (t:Tag) label must not count it
      assertSame(P3, "p0, p1, p2, t", "NOT (p0)-[:KNOWS]-(p2) AND p0 <> p2", 4, true);
    }

    @Test
    void closingHopBackToTheFirstNodeIsNotPushedDown() {
      final String chain = "MATCH (p0:Person)-[:KNOWS]-(p1:Person)-[:KNOWS]-(p2:Person)-[:HAS_INTEREST]->(p0) ";
      final String where = "NOT (p0)-[:KNOWS]-(p2) AND p0 <> p2";
      assertThat(count(chain + "WHERE " + where + " RETURN count(*) AS n")).isEqualTo(count(chain + "WITH p0, p1, p2 WHERE " + where + " RETURN count(*) AS n"));
      assertNoPushDown(chain + "WHERE " + where + " RETURN count(*) AS n");
    }

    @Test
    void twoHopSuffixIsNotPushedDown() {
      final String chain = "MATCH (p0:Person)-[:KNOWS]-(p1:Person)-[:KNOWS]-(p2:Person)-[:HAS_INTEREST]->(t:Tag)<-[:HAS_INTEREST]-(q:Person) ";
      final String where = "NOT (p0)-[:KNOWS]-(p2) AND p0 <> p2";
      assertThat(count(chain + "WHERE " + where + " RETURN count(*) AS n"))
          .isEqualTo(count(chain + "WITH p0, p1, p2, t, q WHERE " + where + " RETURN count(*) AS n"));
      assertNoPushDown(chain + "WHERE " + where + " RETURN count(*) AS n");
    }

    @Test
    void suffixHopOfASubTypeOfAnEarlierHopIsNotPushedDown() {
      database.command("sql", "CREATE EDGE TYPE KNOWS_CHILD EXTENDS KNOWS");
      database.transaction(() -> {
        final var persons = database.query("sql", "SELECT FROM Person ORDER BY id").stream().map(r -> r.getVertex().get()).toList();
        persons.get(0).newEdge("KNOWS_CHILD", persons.get(3)).save();
      });
      final String chain = "MATCH (p0:Person)-[:KNOWS]-(p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS_CHILD]-(p3:Person) ";
      final String where = "NOT (p0)-[:KNOWS]-(p2) AND p0 <> p2";
      assertThat(count(chain + "WHERE " + where + " RETURN count(*) AS n"))
          .isEqualTo(count(chain + "WITH p0, p1, p2, p3 WHERE " + where + " RETURN count(*) AS n"));
      assertNoPushDown(chain + "WHERE " + where + " RETURN count(*) AS n");
    }

    private void assertNoPushDown(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        assertThat(rs.getExecutionPlan().map(x -> x.prettyPrint(0, 2)).orElse("")).doesNotContain("ANTI-JOIN");
      }
    }

    private void assertSame(final String chain, final String vars, final String where, final long expected, final boolean pushedDown) {
      final String text = chain + "WHERE " + where + " RETURN count(*) AS n";
      assertThat(count(text)).as("as written: " + text).isEqualTo(expected);
      assertThat(count(chain + "WITH " + vars + " WHERE " + where + " RETURN count(*) AS n")).as("row pipeline").isEqualTo(expected);
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + text)) {
        final String plan = rs.getExecutionPlan().map(x -> x.prettyPrint(0, 2)).orElse("");
        assertThat(plan.contains("ANTI-JOIN")).as("push-down in plan: " + plan).isEqualTo(pushedDown);
      }
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }

  /**
   * Issue #9598: LSQB Q8 written with the other common spelling of "no such edge", an OPTIONAL MATCH whose relationship is then
   * tested for null, ran row by row, 250x slower than the negated pattern the anti-join push-down answers. When the variables
   * the OPTIONAL MATCH introduces are read by nothing but that test, it is the anti-join, and it is now planned as one.
   * <p>
   * Each rewritten query is checked against an oracle the rewrite cannot apply to: the same query also counting the tested
   * variable in its RETURN, which keeps the OPTIONAL MATCH and runs it row by row. The second count of the oracle is 0, which
   * is what the test meant.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9598OptionalMatchAntiJoin extends TestHelper {
    private static final String ANTI_JOIN = "COUNT ANTI-JOIN CHAIN";

    private static final String CHAIN = "MATCH (tag1:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(tag2:Tag) ";
    private static final String Q8    = CHAIN + "WHERE NOT (comment)-[:HAS_TAG]->(tag1) AND tag1 <> tag2 RETURN count(*) AS n";
    private static final String Q8_OPTIONAL = CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) "
        + "WITH tag1, tag2, h WHERE tag1 <> tag2 AND h IS NULL RETURN count(*) AS n";

    @Override
    protected void beginTest() {
      for (final String ddl : new String[] { "CREATE VERTEX TYPE Tag", "CREATE VERTEX TYPE Message",
          "CREATE VERTEX TYPE Post EXTENDS Message", "CREATE VERTEX TYPE Comment EXTENDS Message", "CREATE EDGE TYPE HAS_TAG",
          "CREATE EDGE TYPE REPLY_OF" })
        database.command("sql", ddl);
    }

    @Test
    void theReportedQueryTakesTheAntiJoinPushDown() {
      // m has tags {a, b}; c1 replies to m with tags {b, c}; c2 replies to m with tags {a, b}
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Tag").set("name", "a").save();
        final MutableVertex b = database.newVertex("Tag").set("name", "b").save();
        final MutableVertex c = database.newVertex("Tag").set("name", "c").save();
        final MutableVertex m = database.newVertex("Post").save();
        final MutableVertex c1 = database.newVertex("Comment").save();
        final MutableVertex c2 = database.newVertex("Comment").save();
        m.newEdge("HAS_TAG", a);
        m.newEdge("HAS_TAG", b);
        c1.newEdge("HAS_TAG", b);
        c1.newEdge("HAS_TAG", c);
        c2.newEdge("HAS_TAG", a);
        c2.newEdge("HAS_TAG", b);
        c1.newEdge("REPLY_OF", m);
        c2.newEdge("REPLY_OF", m);
      });
      // c1: tag1 in {a} x tag2 in {b, c} = 2; c2: no tag of m it lacks
      assertThat(plan(Q8_OPTIONAL)).contains(ANTI_JOIN).doesNotContain("OPTIONAL MATCH");
      assertThat(count(Q8_OPTIONAL)).isEqualTo(2L);
      assertThat(count(Q8)).isEqualTo(2L);
      assertAgreesWithTheOptionalMatch(Q8_OPTIONAL, "h");
    }

    @Test
    void randomGraphsAgreeWithTheOptionalMatchOnTheVerticesAndOnAView() {
      for (long seed = 0; seed < 12; seed++) {
        populate(seed, 120);
        assertThat(plan(Q8_OPTIONAL)).contains(ANTI_JOIN);
        assertThat(count(Q8_OPTIONAL)).as("seed %s", seed).isEqualTo(count(Q8));
        assertAgreesWithTheOptionalMatch(Q8_OPTIONAL, "h");

        final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("q8").build();
        try {
          assertThat(count(Q8_OPTIONAL)).as("seed %s, on the view", seed).isEqualTo(count(Q8));
        } finally {
          view.drop();
        }
        database.drop();
        database = factory.create();
        beginTest();
      }
    }

    /** Every way of writing the test for null: a pass-through WITH *, a GQL FILTER, the pattern read from its other end. */
    @Test
    void otherSpellingsOfTheSameTestAreRewrittenToo() {
      populate(42, 150);
      final long expected = count(Q8);
      for (final String query : new String[] {
          CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH * WHERE tag1 <> tag2 AND h IS NULL RETURN count(*) AS n",
          CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) FILTER h IS NULL AND tag1 <> tag2 RETURN count(*) AS n",
          CHAIN + "OPTIONAL MATCH (tag1)<-[h:HAS_TAG]-(comment) WITH tag1, tag2, h WHERE h IS NULL AND tag1 <> tag2 RETURN count(*) AS n",
          CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag2, h, tag1 WHERE h IS NULL AND tag1 <> tag2 RETURN count(*) AS n" }) {
        assertThat(plan(query)).as("plan of %s", query).doesNotContain("OPTIONAL MATCH");
        assertThat(count(query)).as(query).isEqualTo(expected);
      }
      // the pair count, through a DISTINCT that keeps the WITH
      final String distinct = CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) "
          + "WITH DISTINCT tag1, tag2, h WHERE tag1 <> tag2 AND h IS NULL RETURN count(*) AS n";
      assertThat(plan(distinct)).doesNotContain("OPTIONAL MATCH");
      assertAgreesWithTheOptionalMatch(distinct, "h");
    }

    /** Not only counts: the rows the rewritten query returns are the rows the OPTIONAL MATCH kept. */
    @Test
    void theRowsAreTheRowsTheOptionalMatchKept() {
      populate(7, 100);
      final String rewritten = CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h "
          + "WHERE tag1 <> tag2 AND h IS NULL RETURN tag1.name AS a, tag2.name AS b ORDER BY a, b";
      final String oracle = CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h "
          + "WHERE tag1 <> tag2 AND h IS NULL RETURN tag1.name AS a, tag2.name AS b, h AS hh ORDER BY a, b";
      assertThat(plan(rewritten)).doesNotContain("OPTIONAL MATCH");
      assertThat(plan(oracle)).contains("OPTIONAL MATCH");
      assertThat(pairs(rewritten)).isNotEmpty().isEqualTo(pairs(oracle));
    }

    /** A new node tested for null, a longer optional pattern, and a variable-length one. */
    @Test
    void otherPatternShapesAreRewrittenAndAgree() {
      populate(11, 150);
      for (final String query : new String[] {
          // comments without a tag
          "MATCH (c:Comment) OPTIONAL MATCH (c)-[:HAS_TAG]->(t:Tag) WITH c, t WHERE t IS NULL RETURN count(*) AS n",
          // the tags of a comment that the message it replies to lacks, two hops away: an EXISTS subquery
          "MATCH (c:Comment)-[:HAS_TAG]->(t:Tag) OPTIONAL MATCH (c)-[:REPLY_OF]->(:Message)-[h:HAS_TAG]->(t) "
              + "WITH c, t, h WHERE h IS NULL RETURN count(*) AS n",
          // posts a comment does not reach in up to three replies
          "MATCH (c:Comment), (p:Post) OPTIONAL MATCH (c)-[h:REPLY_OF*1..3]->(p) WITH c, p, h WHERE h IS NULL RETURN count(*) AS n",
          // the pattern starting at the new node and ending at the bound one
          "MATCH (t:Tag) OPTIONAL MATCH (c:Comment)-[h:HAS_TAG]->(t) WITH t, h WHERE h IS NULL RETURN count(*) AS n",
          // variable-length hops inside a longer pattern, which runs from its rendered text: the bounds must survive it
          "MATCH (c:Comment)-[:HAS_TAG]->(t:Tag) OPTIONAL MATCH (c)-[:REPLY_OF*2]->(:Message)-[h:HAS_TAG]->(t) "
              + "WITH c, t, h WHERE h IS NULL RETURN count(*) AS n",
          "MATCH (c:Comment)-[:HAS_TAG]->(t:Tag) OPTIONAL MATCH (c)-[:REPLY_OF*..3]->(:Message)-[h:HAS_TAG]->(t) "
              + "WITH c, t, h WHERE h IS NULL RETURN count(*) AS n",
          "MATCH (c:Comment)-[:HAS_TAG]->(t:Tag) OPTIONAL MATCH (c)-[:REPLY_OF*]->(:Message)-[h:HAS_TAG]->(t) "
              + "WITH c, t, h WHERE h IS NULL RETURN count(*) AS n",
          // an undirected optional pattern, and a WITH that renames a shared variable it carries on
          "MATCH (c:Comment), (p:Post) OPTIONAL MATCH (c)-[h:REPLY_OF]-(p) WITH c, p, h WHERE h IS NULL RETURN count(*) AS n",
          "MATCH (c:Comment)-[:HAS_TAG]->(t:Tag) OPTIONAL MATCH (c)-[:REPLY_OF]->(:Message)-[h:HAS_TAG]->(t) "
              + "WITH c AS comment, t.name AS tag, h WHERE h IS NULL RETURN count(*) AS n",
          // a WITH that renames what the RETURN reads is not a pass-through: it stays, without the removed name
          CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1 AS first, tag2, h WHERE first <> tag2 AND h IS NULL "
              + "RETURN count(first) AS n",
          // the bound node in the middle of the pattern: neither end is bound, so it runs as a correlated EXISTS
          "MATCH (m:Message) OPTIONAL MATCH (:Comment)-[h:REPLY_OF]->(m)-[:HAS_TAG]->(:Tag) WITH m, h WHERE h IS NULL "
              + "RETURN count(*) AS n",
          // an open upper bound, rendered and parsed again
          "MATCH (c:Comment)-[:HAS_TAG]->(t:Tag) OPTIONAL MATCH (c)-[:REPLY_OF*2..]->(:Message)-[h:HAS_TAG]->(t) "
              + "WITH c, t, h WHERE h IS NULL RETURN count(*) AS n",
          // a label disjunction and alternative relationship types, rendered and parsed again
          "MATCH (c:Comment)-[:HAS_TAG]->(t:Tag) OPTIONAL MATCH (c)-[:REPLY_OF|HAS_TAG]->(:Post|Comment)-[h:HAS_TAG]->(t) "
              + "WITH c, t, h WHERE h IS NULL RETURN count(*) AS n",
          // one relationship type twice: the predicate binds two different relationships, as the OPTIONAL MATCH does
          "MATCH (c:Comment), (p:Post) OPTIONAL MATCH (c)-[:REPLY_OF]->(:Message)-[h:REPLY_OF]->(p) "
              + "WITH c, p, h WHERE h IS NULL RETURN count(*) AS n" }) {
        assertThat(plan(query)).as("plan of %s", query).doesNotContain("OPTIONAL MATCH");
        final String variable = query.contains("t IS NULL") ? "t" : "h";
        assertAgreesWithTheOptionalMatch(query, variable);
      }
    }

    /**
     * A pattern of more than one hop is evaluated by parsing its text again, so the names written in it must survive the trip:
     * a label with a space and a relationship type with a backtick in it.
     */
    @Test
    void namesThatNeedQuotingSurviveTheRenderedPattern() {
      database.getSchema().createVertexType("Odd Tag");
      database.getSchema().createEdgeType("HAS`TAG");
      database.transaction(() -> {
        final List<MutableVertex> tags = new ArrayList<>();
        for (int i = 0; i < 4; i++)
          tags.add(database.newVertex("Odd Tag").save());
        final MutableVertex post = database.newVertex("Post").save();
        post.newEdge("HAS`TAG", tags.get(0));
        post.newEdge("HAS`TAG", tags.get(1));
        for (int i = 0; i < 4; i++) {
          final MutableVertex comment = database.newVertex("Comment").save();
          comment.newEdge("REPLY_OF", post);
          comment.newEdge("HAS`TAG", tags.get(i));
        }
      });
      // the comments tagged with a tag their post does not have: the ones tagged 2 and 3
      final String query = "MATCH (c:Comment)-[:`HAS``TAG`]->(t:`Odd Tag`) "
          + "OPTIONAL MATCH (c)-[:REPLY_OF]->(:Message)-[h:`HAS``TAG`]->(t) WITH c, t, h WHERE h IS NULL RETURN count(*) AS n";
      assertThat(plan(query)).doesNotContain("OPTIONAL MATCH");
      assertThat(count(query)).isEqualTo(2L);
      assertAgreesWithTheOptionalMatch(query, "h");
    }

    /**
     * Shapes where dropping the OPTIONAL MATCH would change the answer stay as written: a variable read again, renamed, or
     * carried to a RETURN *, an endpoint that can be null, a WITH that aggregates or pages, a WHERE on the OPTIONAL MATCH, a
     * test for NOT NULL.
     */
    @Test
    void shapesThatAreNotOnlyATestForAbsenceStayAsWritten() {
      populate(13, 120);
      for (final String query : new String[] {
          CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h WHERE h IS NULL RETURN count(*) AS n, count(h) AS hs",
          CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h AS x WHERE x IS NULL RETURN count(*) AS n",
          CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, h, count(*) AS k WHERE h IS NULL RETURN sum(k) AS n",
          CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h LIMIT 1000000 WHERE h IS NULL RETURN count(*) AS n",
          CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WHERE tag1 <> tag2 WITH tag1, tag2, h WHERE h IS NULL RETURN count(*) AS n",
          CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h WHERE h IS NOT NULL RETURN count(*) AS n",
          // a property of the relationship tested for null: true for a relationship that has no such property, not an absence
          CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h WHERE h.weight IS NULL RETURN count(*) AS n",
          // the test only inside another expression: not a conjunct of its own
          CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h WHERE h IS NULL OR tag1 = tag2 RETURN count(*) AS n",
          CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h WHERE NOT (h IS NOT NULL) RETURN count(*) AS n",
          CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h WHERE coalesce(h, 0) = 0 RETURN count(*) AS n",
          // carried by a WITH from what an OPTIONAL MATCH bound, so still possibly null
          "MATCH (c:Comment) OPTIONAL MATCH (c)-[:REPLY_OF]->(p:Post) WITH c, p AS post OPTIONAL MATCH (post)-[h:HAS_TAG]->(:Tag) "
              + "WITH c, h WHERE h IS NULL RETURN count(*) AS n",
          // p is null for a comment that replies to a comment: OPTIONAL MATCH from a null node matches nothing
          "MATCH (c:Comment) OPTIONAL MATCH (c)-[:REPLY_OF]->(p:Post) OPTIONAL MATCH (p)-[h:HAS_TAG]->(:Tag) "
              + "WITH c, h WHERE h IS NULL RETURN count(*) AS n" }) {
        assertThat(plan(query)).as("plan of %s", query).contains("OPTIONAL MATCH");
        assertThat(count(query)).as(query).isEqualTo(count(query.replace("RETURN ", "WITH * SKIP 0 RETURN ")));
      }

      // carried by WITH * to a RETURN *: the column is part of the answer
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (c:Comment) OPTIONAL MATCH (c)-[h:HAS_TAG]->(:Tag) WITH * WHERE h IS NULL RETURN * LIMIT 1")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().getPropertyNames()).contains("c", "h");
      }

      // p is null for a comment that replies to a comment: such a comment is kept whatever the tags of other posts
      long expected = 0;
      for (final Iterator<Record> it = database.iterateType("Comment", true); it.hasNext(); ) {
        final Vertex comment = it.next().asVertex();
        boolean tagged = false;
        for (final Vertex parent : comment.getVertices(Vertex.DIRECTION.OUT, "REPLY_OF"))
          if (parent.getTypeName().equals("Post") && parent.getVertices(Vertex.DIRECTION.OUT, "HAS_TAG").iterator().hasNext())
            tagged = true;
        if (!tagged)
          ++expected;
      }
      assertThat(count("MATCH (c:Comment) OPTIONAL MATCH (c)-[:REPLY_OF]->(p:Post) OPTIONAL MATCH (p)-[h:HAS_TAG]->(:Tag) "
          + "WITH c, h WHERE h IS NULL RETURN count(*) AS n")).isEqualTo(expected);
    }

    /**
     * Relationship uniqueness is scoped to one MATCH clause: the OPTIONAL MATCH may bind the very relationship the MATCH before
     * it bound, and so may the pattern predicate. Here it can bind nothing else, so no row has it missing.
     */
    @Test
    void theOptionalMatchMayBindTheRelationshipAnEarlierMatchBound() {
      populate(29, 100);
      final String query = "MATCH (c:Comment)-[r:HAS_TAG]->(t:Tag) OPTIONAL MATCH (c)-[h:HAS_TAG]->(t) "
          + "WITH c, t, h WHERE h IS NULL RETURN count(*) AS n";
      assertThat(plan(query)).doesNotContain("OPTIONAL MATCH");
      assertThat(count(query)).isZero();
      assertAgreesWithTheOptionalMatch(query, "h");
    }

    /** An OPTIONAL MATCH after another one filters in its own place, since the WHERE of an OPTIONAL MATCH is part of it. */
    @Test
    void afterAnotherOptionalMatchTheTestBecomesAFilter() {
      populate(17, 120);
      final String query = "MATCH (c:Comment) OPTIONAL MATCH (c)-[:REPLY_OF]->(p:Post) OPTIONAL MATCH (c)-[h:HAS_TAG]->(:Tag) "
          + "WITH c, p, h WHERE h IS NULL RETURN count(*) AS n";
      final String plan = plan(query);
      assertThat(plan.indexOf("OPTIONAL MATCH")).isEqualTo(plan.lastIndexOf("OPTIONAL MATCH")).isNotNegative();
      assertAgreesWithTheOptionalMatch(query, "h");
    }

    @Test
    void eachBranchOfAUnionIsRewritten() {
      populate(19, 100);
      final String union = Q8_OPTIONAL + " UNION ALL " + Q8_OPTIONAL.replace("count(*)", "count(*) + 1");
      assertThat(plan(union)).contains("NOT ((comment)-[:HAS_TAG]->(tag1))").doesNotContain("OPTIONAL MATCH");
      final List<Long> values = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", union)) {
        while (rs.hasNext())
          values.add(((Number) rs.next().getProperty("n")).longValue());
      }
      final long expected = count(Q8);
      assertThat(values).containsExactlyInAnyOrder(expected, expected + 1);
    }

    /** A parameterised query is parsed and rewritten once, and each execution binds its own values. */
    @Test
    void aCachedRewriteBindsEachExecutionsParameters() {
      populate(41, 120);
      final String query = "MATCH (c:Comment)-[:HAS_TAG]->(t:Tag) WHERE t.name = $tag OPTIONAL MATCH (c)-[:REPLY_OF]->(m:Message) "
          + "OPTIONAL MATCH (m)-[h:HAS_TAG]->(t) WITH c, h WHERE h IS NULL RETURN count(*) AS n";
      for (final String tag : new String[] { "t0", "t3", "t7", "t0" }) {
        final long rewritten;
        try (final ResultSet rs = database.query("opencypher", query, "tag", tag)) {
          rewritten = ((Number) rs.next().getProperty("n")).longValue();
        }
        try (final ResultSet rs = database.query("opencypher", query.replace(" AS n", " AS n, count(h) AS tested"), "tag", tag)) {
          assertThat(rewritten).as("tag %s", tag).isEqualTo(((Number) rs.next().getProperty("n")).longValue());
        }
      }
    }

    /** Two OPTIONAL MATCHes each tested for absence are both rewritten. */
    @Test
    void twoAbsenceTestsInOneQueryAreBothRewritten() {
      populate(37, 120);
      final String query = "MATCH (c:Comment)-[:REPLY_OF]->(m:Message) OPTIONAL MATCH (c)-[h:HAS_TAG]->(:Tag) WITH c, m, h WHERE h IS NULL "
          + "OPTIONAL MATCH (m)-[k:HAS_TAG]->(:Tag) WITH c, k WHERE k IS NULL RETURN count(*) AS n";
      assertThat(plan(query)).doesNotContain("OPTIONAL MATCH");
      assertThat(count(query)).isEqualTo(
          count("MATCH (c:Comment)-[:REPLY_OF]->(m:Message) WHERE NOT (c)-[:HAS_TAG]->(:Tag) AND NOT (m)-[:HAS_TAG]->(:Tag) RETURN sum(1) AS n"));
    }

    /** A rewritten query profiles as it runs, and a pattern sharing no node with the rows is left as written. */
    @Test
    void profileAndAPatternSharingNoNode() {
      populate(31, 80);
      try (final ResultSet rs = database.query("opencypher", "PROFILE " + Q8_OPTIONAL)) {
        assertThat(((Number) rs.next().getProperty("n")).longValue()).isEqualTo(count(Q8));
        assertThat(rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("")).doesNotContain("OPTIONAL MATCH");
      }

      final String unrelated = "MATCH (c:Comment) OPTIONAL MATCH (x:Post)-[h:HAS_TAG]->(:Tag) WITH c, h WHERE h IS NULL "
          + "RETURN count(*) AS n";
      assertThat(plan(unrelated)).contains("OPTIONAL MATCH");
      assertAgreesWithTheOptionalMatch(unrelated, "h");

      // a UNION whose second branch reads the variable again: the first is rewritten, the second is not
      final String second = CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h WHERE h IS NULL "
          + "RETURN count(h) + count(*) AS n";
      final String plan = plan(Q8_OPTIONAL + " UNION ALL " + second);
      assertThat(plan.indexOf("OPTIONAL MATCH")).isEqualTo(plan.lastIndexOf("OPTIONAL MATCH")).isNotNegative();
      final List<Long> values = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", Q8_OPTIONAL + " UNION ALL " + second)) {
        while (rs.hasNext())
          values.add(((Number) rs.next().getProperty("n")).longValue());
      }
      assertThat(values).containsExactlyInAnyOrder(count(Q8), count(second));
    }

    /** A statement that writes is left as written: its clauses are not the ones the rewrite models. */
    @Test
    void aWritingStatementIsNotRewritten() {
      populate(23, 80);
      database.transaction(() -> database.command("opencypher",
          "MATCH (c:Comment) OPTIONAL MATCH (c)-[h:HAS_TAG]->(:Tag) WITH c, h WHERE h IS NULL SET c.untagged = true"));
      assertThat(count("MATCH (c:Comment) WHERE c.untagged = true RETURN count(*) AS n"))
          .isEqualTo(count("MATCH (c:Comment) WHERE NOT (c)-[:HAS_TAG]->(:Tag) RETURN count(*) AS n"));
    }

    /**
     * The query agrees with the same OPTIONAL MATCH run row by row: the oracle counts the tested variable too, which keeps the
     * OPTIONAL MATCH, and that count is 0.
     */
    private void assertAgreesWithTheOptionalMatch(final String query, final String variable) {
      final String oracle = query.replace(" AS n", " AS n, count(" + variable + ") AS tested");
      assertThat(plan(oracle)).as("plan of %s", oracle).contains("OPTIONAL MATCH");
      try (final ResultSet rs = database.query("opencypher", oracle)) {
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("tested")).longValue()).isZero();
        assertThat(count(query)).as(query).isEqualTo(((Number) row.getProperty("n")).longValue());
      }
    }

    /**
     * Tags, posts and comments replying to posts or to comments, with random tags, some comments untagged, some replying to
     * themselves.
     */
    private void populate(final long seed, final int messages) {
      final Random random = new Random(seed);
      database.transaction(() -> {
        final List<MutableVertex> tags = new ArrayList<>();
        for (int i = 0; i < 12; i++)
          tags.add(database.newVertex("Tag").set("name", "t" + i).save());
        final List<MutableVertex> all = new ArrayList<>();
        for (int i = 0; i < messages; i++) {
          final boolean post = i < messages / 3 || random.nextInt(4) == 0;
          final MutableVertex message = database.newVertex(post ? "Post" : "Comment").save();
          for (int t = random.nextInt(4); t > 0; t--)
            message.newEdge("HAS_TAG", tags.get(random.nextInt(tags.size())));
          if (!post) {
            final MutableVertex parent = random.nextInt(20) == 0 ? message : all.get(random.nextInt(all.size()));
            message.newEdge("REPLY_OF", parent);
          }
          all.add(message);
        }
      });
    }

    private List<String> pairs(final String query) {
      final List<String> pairs = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          pairs.add(row.getProperty("a") + "," + row.getProperty("b"));
        }
      }
      return pairs;
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().map(plan -> plan.prettyPrint(0, 2)).orElse("");
      }
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }

  /**
   * Issue #9608: a property predicate on a pattern node, {@code (:Message {kind: 'Comment'})}, made the LSQB Q2 (pair join) and
   * Q8 (anti-join) shapes fall back to the row pipeline, in every order and every spelling of the negation, while the label
   * spelling was a push-down. Both operators now apply a per-vertex predicate on top of the label of each position, the way the
   * chain and star operators do since issue #9595.
   * <p>
   * Every count is checked against the row pipeline, reached through a {@code sum(1)} that no push-down takes, with and
   * without a Graph Analytical View, since the operators have a CSR path and a record path.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9608PropertyPredicatePairAndAntiJoin extends TestHelper {
    private static final String Q2_LABELS     = "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
        + "(person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(person2)";
    private static final String Q2_PROPERTIES = "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
        + "(person1)<-[:HAS_CREATOR]-(comment:Message {kind: 'Comment'})-[:REPLY_OF]->(post:Message {kind: 'Post'})-[:HAS_CREATOR]->(person2)";
    private static final String Q2_REORDERED  = "MATCH (post:Message {kind: 'Post'})-[:HAS_CREATOR]->(person2:Person), "
        + "(comment:Message {kind: 'Comment'})-[:REPLY_OF]->(post), (comment)-[:HAS_CREATOR]->(person1:Person), (person2)-[:KNOWS]-(person1)";

    private static final String Q8_LABELS     = "MATCH (tag1:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(tag2:Tag)";
    private static final String Q8_PROPERTIES = "MATCH (tag1:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Message {kind: 'Comment'})-[:HAS_TAG]->(tag2:Tag)";
    private static final String Q8_NOT        = " WHERE NOT (comment)-[:HAS_TAG]->(tag1) AND tag1 <> tag2";
    private static final String Q8_OPTIONAL   = " OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h WHERE tag1 <> tag2 AND h IS NULL";

    private static final String Q9 = "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag)";
    private static final String Q9_WHERE = " WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3";

    @Override
    protected void beginTest() {
      for (final String ddl : new String[] { "CREATE VERTEX TYPE Person", "CREATE VERTEX TYPE Tag", "CREATE VERTEX TYPE Message",
          "CREATE VERTEX TYPE Post EXTENDS Message", "CREATE VERTEX TYPE Comment EXTENDS Message", "CREATE EDGE TYPE KNOWS",
          "CREATE EDGE TYPE HAS_CREATOR", "CREATE EDGE TYPE REPLY_OF", "CREATE EDGE TYPE HAS_TAG", "CREATE EDGE TYPE HAS_INTEREST" })
        database.command("sql", ddl);
    }

    @Test
    void theReportedQ2PropertyFormIsAPairJoin() {
      populate(1);
      assertQ2();
    }

    @Test
    void theReportedQ8PropertyFormsAreAnAntiJoin() {
      populate(2);
      assertQ8();
    }

    @Test
    void aViewAnswersThePropertyFormsAlike() {
      populate(3);
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("lsqb").build();
      try {
        assertQ2();
        assertQ8();
        assertQ9();
      } finally {
        view.drop();
      }
    }

    /** The same predicates written in WHERE rather than on the node take the same operators. */
    @Test
    void wherePredicatesOnOneNodeAreAppliedLikeInlineMaps() {
      populate(4);
      final String q2 = "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
          + "(person1)<-[:HAS_CREATOR]-(comment:Message)-[:REPLY_OF]->(post:Message)-[:HAS_CREATOR]->(person2) "
          + "WHERE comment.kind = 'Comment' AND post.kind = 'Post' AND person1.flag = true";
      assertThat(pushDown(q2 + " RETURN count(*) AS n")).contains("COUNT PAIR JOIN");
      assertThat(count(q2 + " RETURN count(*) AS n")).isEqualTo(count(q2 + " RETURN sum(1) AS n")).isPositive();

      final String q8 = "MATCH (tag1:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Message)-[:HAS_TAG]->(tag2:Tag) "
          + "WHERE comment.kind = 'Comment' AND NOT (comment)-[:HAS_TAG]->(tag1) AND tag1 <> tag2 AND message.lang = 'en'";
      assertThat(pushDown(q8 + " RETURN count(*) AS n")).contains("COUNT ANTI-JOIN CHAIN");
      assertThat(count(q8 + " RETURN count(*) AS n")).isEqualTo(count(q8 + " RETURN sum(1) AS n")).isPositive();
    }

    /** A predicate on every position of each shape, including a parameter, read through the record path and the CSR one. */
    @Test
    void aPredicateOnEveryPositionKeepsTheCount() {
      populate(5);
      assertQ9();
      final String q2 = "MATCH (person1:Person {flag: true})-[:KNOWS]-(person2:Person {flag: false}), "
          + "(person1)<-[:HAS_CREATOR]-(comment:Message {kind: $kind, lang: 'en'})-[:REPLY_OF]->(post:Message)-[:HAS_CREATOR]->(person2)";
      final Map<String, Object> parameters = Map.of("kind", "Comment");
      assertThat(pushDown(q2 + " RETURN count(*) AS n", parameters)).contains("COUNT PAIR JOIN");
      assertThat(count(q2 + " RETURN count(*) AS n", parameters)).isEqualTo(count(q2 + " RETURN sum(1) AS n", parameters));

      final String q8 = "MATCH (tag1:Tag {group: 1})<-[:HAS_TAG]-(message:Message {lang: 'en'})<-[:REPLY_OF]-"
          + "(comment:Message {kind: 'Comment'})-[:HAS_TAG]->(tag2:Tag {group: 0})";
      for (final String tail : new String[] { Q8_NOT, Q8_OPTIONAL }) {
        assertThat(pushDown(q8 + tail + " RETURN count(*) AS n")).contains("COUNT ANTI-JOIN CHAIN");
        assertThat(count(q8 + tail + " RETURN count(*) AS n")).as(tail).isEqualTo(count(q8 + Q8_NOT + " RETURN sum(1) AS n"));
      }
    }

    /**
     * A variable written at two positions keeps the properties of both: the pattern graph used to keep the labelled position
     * alone, which dropped a predicate written at the other one from every operator reading the joined pattern.
     */
    @Test
    void propertiesWrittenAtAnotherPositionOfTheVariableAreKept() {
      populate(6);
      for (final String match : new String[] {
          // a chain cut into parts, the predicate on the unlabelled position of comment
          "MATCH (c:Comment)-[:REPLY_OF]->(m:Message), (c {lang: 'en'})-[:HAS_TAG]->(t:Tag)",
          "MATCH (c {lang: 'en'})-[:REPLY_OF]->(m:Message), (c:Comment)-[:HAS_TAG]->(t:Tag)",
          // two different keys on the two positions
          "MATCH (c:Comment {kind: 'Comment'})-[:REPLY_OF]->(m:Message), (c {lang: 'en'})-[:HAS_TAG]->(t:Tag)",
          // the same key, two different values: nothing matches
          "MATCH (c:Comment {lang: 'it'})-[:REPLY_OF]->(m:Message), (c {lang: 'en'})-[:HAS_TAG]->(t:Tag)",
          // the Q2 cycle with the predicates on the probe's ends
          "MATCH (person1 {flag: true})-[:KNOWS]-(person2), "
              + "(person1:Person)<-[:HAS_CREATOR]-(comment:Message {kind: 'Comment'})-[:REPLY_OF]->(post:Message {kind: 'Post'})-[:HAS_CREATOR]->(person2:Person)",
          "MATCH (post:Message)-[:HAS_CREATOR]->(person2:Person), (comment:Message {kind: 'Comment'})-[:REPLY_OF]->(post {kind: 'Post'}), "
              + "(comment {lang: 'en'})-[:HAS_CREATOR]->(person1:Person), (person2 {flag: false})-[:KNOWS]-(person1)" })
        assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(count(match + " RETURN sum(1) AS n"));
    }

    /**
     * Two inline values prove two nodes different vertices only when no stored value can match both: a schema type on the
     * property, or a case-insensitive index answering the node seek, must not make the proof wrong.
     */
    @Test
    void typedAndCaseInsensitivelyIndexedPropertiesKeepTheCount() {
      database.command("sql", "CREATE PROPERTY Message.code STRING");
      database.command("sql", "CREATE PROPERTY Message.score INTEGER");
      database.command("sql", "CREATE INDEX ON Message (code COLLATE ci) NOTUNIQUE");
      populate(9);
      final Random random = new Random(9);
      final String[] codes = { "A", "a", "b" };
      database.transaction(() -> database.iterateType("Message", true).forEachRemaining(record -> record.asVertex().modify()
          .set("code", codes[random.nextInt(codes.length)]).set("score", random.nextInt(3)).save()));

      for (final String match : new String[] {
          "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
              + "(person1)<-[:HAS_CREATOR]-(comment:Message {code: 'A'})-[:REPLY_OF]->(post:Message {code: 'a'})-[:HAS_CREATOR]->(person2)",
          "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
              + "(person1)<-[:HAS_CREATOR]-(comment:Message {code: 'A'})-[:REPLY_OF]->(post:Message {code: 'b'})-[:HAS_CREATOR]->(person2)",
          "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
              + "(person1)<-[:HAS_CREATOR]-(comment:Message {score: 1})-[:REPLY_OF]->(post:Message {score: 2})-[:HAS_CREATOR]->(person2)",
          "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
              + "(person1)<-[:HAS_CREATOR]-(comment:Message {score: '1'})-[:REPLY_OF]->(post:Message {score: 1})-[:HAS_CREATOR]->(person2)",
          // a disjunction is no equality: it proves nothing about the two nodes
          "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
              + "(person1)<-[:HAS_CREATOR]-(comment:Message)-[:REPLY_OF]->(post:Message)-[:HAS_CREATOR]->(person2) "
              + "WHERE (comment.kind = 'Comment' OR comment.kind = 'Post') AND post.kind = 'Post'" })
        assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(count(match + " RETURN sum(1) AS n"));
    }

    /**
     * The proof that two hops bind different edges also decides which hops the row pipeline tracks for relationship
     * uniqueness. Rows, not counts: an inline map written on both nodes returns what the same predicates written in WHERE
     * return (which the proof does not read), whether the two values differ (no tracking needed) or are equal (the same edge
     * reached from both hops must still be refused).
     */
    @Test
    void theRowPipelineReturnsTheSameRowsWithInlineMaps() {
      populate(10);
      for (final String[] kinds : new String[][] { { "Comment", "Post" }, { "Comment", "Comment" } }) {
        final String inline = "MATCH (p:Person)<-[:HAS_CREATOR]-(a:Message {kind: '" + kinds[0] + "'}), "
            + "(b:Message {kind: '" + kinds[1] + "'})-[:HAS_CREATOR]->(p) RETURN id(a) AS a, id(b) AS b";
        final String where = "MATCH (p:Person)<-[:HAS_CREATOR]-(a:Message), (b:Message)-[:HAS_CREATOR]->(p) WHERE a.kind = '" + kinds[0]
            + "' AND b.kind = '" + kinds[1] + "' RETURN id(a) AS a, id(b) AS b";
        final List<String> expected = rows(where);
        assertThat(expected).isNotEmpty();
        assertThat(rows(inline)).as(inline).isEqualTo(expected);
        if (kinds[0].equals(kinds[1]))
          assertThat(expected).as("a vertex never pairs with itself through one edge").noneMatch(row -> row.split("\\|")[0].equals(row.split("\\|")[1]));
      }
    }

    private List<String> rows(final String query) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        rs.stream().forEach(row -> rows.add(row.getProperty("a") + "|" + row.getProperty("b")));
      }
      rows.sort(null);
      return rows;
    }

    /** An inline value contradicting a WHERE literal, a property some vertices lack, and a test for its absence. */
    @Test
    void conflictingAndMissingValuesKeepTheCount() {
      populate(11);
      database.transaction(() -> database.command("sql", "UPDATE Message REMOVE lang WHERE kind = 'Post'"));
      for (final String query : new String[] {
          Q2_PROPERTIES.replace("(comment:Message {kind: 'Comment'})", "(comment:Message {kind: 'Post'})") + " WHERE comment.kind = 'Comment'",
          Q2_PROPERTIES.replace("(post:Message {kind: 'Post'})", "(post:Message {kind: 'Post', lang: 'en'})"),
          Q2_PROPERTIES + " WHERE post.lang IS NULL AND comment.lang = 'en'",
          Q8_PROPERTIES.replace("(message:Message)", "(message:Message {lang: 'it'})") + Q8_NOT,
          Q8_PROPERTIES + Q8_NOT + " AND message.lang IS NULL" }) {
        final long expected = count(query + " RETURN sum(1) AS n");
        assertThat(count(query + " RETURN count(*) AS n")).as(query).isEqualTo(expected);
      }
      // the predicates that remain per vertex still take the push-downs
      assertThat(pushDown(Q2_PROPERTIES + " WHERE post.lang IS NULL RETURN count(*) AS n")).contains("COUNT PAIR JOIN");
      assertThat(pushDown(Q8_PROPERTIES + Q8_NOT + " AND message.lang IS NULL RETURN count(*) AS n")).contains("COUNT ANTI-JOIN CHAIN");
    }

    /** Predicates no per-vertex filter can express leave the shapes to the row pipeline, with the same count. */
    @Test
    void predicatesThatAreNotPerVertexStayExact() {
      populate(7);
      for (final String query : new String[] {
          // two variables in one conjunct
          Q2_PROPERTIES + " WHERE comment.lang = post.lang",
          // a random function
          Q2_PROPERTIES + " WHERE rand() < 2",
          Q8_PROPERTIES + " WHERE NOT (comment)-[:HAS_TAG]->(tag1) AND tag1 <> tag2 AND comment.lang = message.lang",
          // an inline value reading another node
          "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
              + "(person1)<-[:HAS_CREATOR]-(comment:Message {kind: 'Comment'})-[:REPLY_OF]->(post:Message {lang: comment.lang})-[:HAS_CREATOR]->(person2)" })
        assertThat(count(query + " RETURN count(*) AS n")).as(query).isEqualTo(count(query + " RETURN sum(1) AS n"));
    }

    /**
     * The pair join counts the build chain between the probe's two ends: a chain written past one of them, or naming a node
     * twice, is not that shape, and the hops it would leave out are kept by the row pipeline.
     */
    @Test
    void aBuildChainRunningPastTheProbeIsNotAPairJoin() {
      populate(8);
      for (final String match : new String[] {
          "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
              + "(person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(person2)-[:HAS_INTEREST]->(t:Tag)",
          "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
              + "(t:Tag)<-[:HAS_INTEREST]-(person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(person2)",
          "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
              + "(person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(comment)-[:HAS_CREATOR]->(person2)" })
        assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(count(match + " RETURN sum(1) AS n"));
    }

    private void assertQ2() {
      final long labels = count(Q2_LABELS + " RETURN count(*) AS n");
      assertThat(labels).isPositive();
      for (final String match : new String[] { Q2_PROPERTIES, Q2_REORDERED }) {
        assertThat(pushDown(match + " RETURN count(*) AS n")).as(match).contains("COUNT PAIR JOIN").contains("kind: Comment");
        assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(labels);
        assertThat(count(match + " RETURN sum(1) AS n")).as(match).isEqualTo(labels);
      }
    }

    private void assertQ8() {
      final long labels = count(Q8_LABELS + Q8_NOT + " RETURN count(*) AS n");
      assertThat(labels).isPositive();
      assertThat(count(Q8_PROPERTIES + Q8_NOT + " RETURN sum(1) AS n")).isEqualTo(labels);
      for (final String tail : new String[] { Q8_NOT, Q8_OPTIONAL }) {
        final String query = Q8_PROPERTIES + tail + " RETURN count(*) AS n";
        assertThat(pushDown(query)).as(tail).contains("COUNT ANTI-JOIN CHAIN").contains("kind: Comment");
        assertThat(count(query)).as(tail).isEqualTo(labels);
      }
    }

    /** LSQB Q9 with a predicate at each position: the two-hop algebraic count, the merge-scan and the recursive walk. */
    private void assertQ9() {
      for (final String query : new String[] {
          "MATCH (p1:Person {flag: true})-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag)" + Q9_WHERE,
          "MATCH (p1:Person)-[:KNOWS]-(p2:Person {flag: false})-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag)" + Q9_WHERE,
          "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person {flag: true})-[:HAS_INTEREST]->(t:Tag)" + Q9_WHERE,
          "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag {group: 1})" + Q9_WHERE,
          // an unlabelled position takes the recursive walk
          "MATCH (p1:Person {flag: true})-[:KNOWS]-(p2 {flag: false})-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t {group: 0})" + Q9_WHERE,
          Q9 + Q9_WHERE + " AND p2.flag = true AND t.group = 2" }) {
        assertThat(pushDown(query + " RETURN count(*) AS n")).as(query).contains("COUNT ANTI-JOIN CHAIN");
        assertThat(count(query + " RETURN count(*) AS n")).as(query).isEqualTo(count(query + " RETURN sum(1) AS n"));
      }
    }

    /**
     * People who know each other and have interests, posts and comments each with a creator, comments replying to posts or
     * comments, tags on the messages. Every message carries its kind, as in the LSQB load of the issue, and a language.
     */
    private void populate(final long seed) {
      final Random random = new Random(seed);
      database.transaction(() -> {
        final List<MutableVertex> persons = new ArrayList<>();
        final List<MutableVertex> tags = new ArrayList<>();
        final List<MutableVertex> messages = new ArrayList<>();
        for (int i = 0; i < 30; i++)
          persons.add(database.newVertex("Person").set("flag", random.nextBoolean()).save());
        for (int i = 0; i < 8; i++)
          tags.add(database.newVertex("Tag").set("group", i % 3).save());
        for (final MutableVertex person : persons) {
          for (int k = random.nextInt(4); k > 0; k--)
            person.newEdge("KNOWS", persons.get(random.nextInt(persons.size())));
          for (int k = random.nextInt(3); k > 0; k--)
            person.newEdge("HAS_INTEREST", tags.get(random.nextInt(tags.size())));
        }
        for (int i = 0; i < 300; i++) {
          final boolean post = i < 80;
          final MutableVertex message = database.newVertex(post ? "Post" : "Comment").set("kind", post ? "Post" : "Comment")
              .set("lang", random.nextBoolean() ? "en" : "it").save();
          message.newEdge("HAS_CREATOR", persons.get(random.nextInt(persons.size())));
          for (int t = random.nextInt(3); t > 0; t--)
            message.newEdge("HAS_TAG", tags.get(random.nextInt(tags.size())));
          if (!post)
            message.newEdge("REPLY_OF", messages.get(random.nextInt(messages.size())));
          messages.add(message);
        }
      });
    }

    private String pushDown(final String query) {
      return pushDown(query, Map.of());
    }

    /** The count push-down part of the plan, which has to be there. */
    private String pushDown(final String query, final Map<String, Object> parameters) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, parameters)) {
        final String plan = rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
        assertThat(plan).as("plan of %s", query).contains("Using Count Push-Down");
        final int start = plan.indexOf("+ COUNT");
        final int end = plan.indexOf("\nUsing ", start);
        return (end > 0 ? plan.substring(start, end) : plan.substring(start)).trim();
      }
    }

    private long count(final String query) {
      return count(query, Map.of());
    }

    private long count(final String query, final Map<String, Object> parameters) {
      try (final ResultSet rs = database.query("opencypher", query, parameters)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }
}
