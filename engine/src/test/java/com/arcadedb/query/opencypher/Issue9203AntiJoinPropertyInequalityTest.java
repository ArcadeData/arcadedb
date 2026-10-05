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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9203: LSQB Q9 written with {@code p1.id <> p3.id} (or {@code id(p1) <> id(p3)}) instead of
 * {@code p1 <> p3} declined the COUNT ANTI-JOIN CHAIN push-down and ran on the row pipeline, and the pattern predicate
 * {@code NOT (p1)-[:KNOWS]-(p3)} loaded every KNOWS edge record to compare its endpoint. A property inequality stands for the node
 * inequality only when the property is unique and never null over the nodes' type, so the push-down is taken then and only then.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9203AntiJoinPropertyInequalityTest {
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
