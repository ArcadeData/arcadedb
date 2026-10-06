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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The nine official LSQB queries (the exact texts the LDBC benchmark runs) must keep using their count push-down.
 * <p>
 * Why this exists: the Graphalytics/LSQB suite found two engine changes that silently made a push-down decline
 * (#6337 labelled star arms: Q4/Q7 OLAP 0.01 s to 5-12 s; #8426 non-adjacent overlap: Q5 0.2 s to ~3.5 s) while
 * every result stayed correct. {@code GAVEligibilityTest} mostly asserts "Cost-Based", which is also present when the
 * push-down is declined and the cost-based optimizer runs instead ("superseded by the count push-down" is printed when
 * both exist), so those regressions passed it. Here each query must report {@code Using Count Push-Down} and the
 * specific operator below.
 * <p>
 * Q8 (a NOT pattern next to an inequality) has no count push-down today and runs on the cost-based plan; it is left
 * out on purpose rather than pinned, so that adding a push-down for it does not break this test.
 */
class LsqbCountPushDownEligibilityTest {
  private Database database;

  private static final String Q1 =
      "MATCH (co:Country)<-[:IS_PART_OF]-(ci:City)<-[:IS_LOCATED_IN]-(p:Person)<-[:HAS_MEMBER]-(f:Forum)-[:CONTAINER_OF]->(po:Post)<-[:REPLY_OF]-(cm:Comment)-[:HAS_TAG]->(t:Tag)-[:HAS_TYPE]->(tc:TagClass) RETURN count(*) AS count";
  private static final String Q2 =
      "MATCH (p1:Person)-[:KNOWS]-(p2:Person), (p1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2) RETURN count(*) AS count";
  private static final String Q3 =
      "MATCH (co:Country) MATCH (p1:Person)-[:IS_LOCATED_IN]->(c1:City)-[:IS_PART_OF]->(co) MATCH (p2:Person)-[:IS_LOCATED_IN]->(c2:City)-[:IS_PART_OF]->(co) MATCH (p3:Person)-[:IS_LOCATED_IN]->(c3:City)-[:IS_PART_OF]->(co) MATCH (p1)-[:KNOWS]-(p2)-[:KNOWS]-(p3)-[:KNOWS]-(p1) RETURN count(*) AS count";
  private static final String Q4 =
      "MATCH (tg:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_CREATOR]->(cr:Person), (m)<-[:LIKES]-(lk:Person), (m)<-[:REPLY_OF]-(rp:Comment) RETURN count(*) AS count";
  private static final String Q5 =
      "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) WHERE t1 <> t2 RETURN count(*) AS count";
  private static final String Q6 =
      "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE p1 <> p3 RETURN count(*) AS count";
  private static final String Q7 =
      "MATCH (tg:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_CREATOR]->(cr:Person) OPTIONAL MATCH (m)<-[:LIKES]-(lk:Person) OPTIONAL MATCH (m)<-[:REPLY_OF]-(rp:Comment) RETURN count(*) AS count";
  private static final String Q8 =
      "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) WHERE NOT (c)-[:HAS_TAG]->(t1) AND t1 <> t2 RETURN count(*) AS count";
  private static final String Q9 =
      "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3 RETURN count(*) AS count";

  @BeforeEach
  void setup() {
    // Drop first rather than create outright: a run killed between create() and the @AfterEach below leaves the
    // directory behind, and create() refuses to overwrite an existing database - so the NEXT run failed here with
    // "Database ... already exists" and one kill cost two runs. The build removes target/databases before the test
    // phase for the same reason; this keeps the class self-healing when it is run straight from an IDE, which does
    // not go through the Maven lifecycle.
    database = TestHelper.dropDatabase("./target/databases/testopencypher-lsqb-pushdown-eligibility").create();

    // Create LSQB-like schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Message");
    database.getSchema().createVertexType("Comment").addSuperType("Message");
    database.getSchema().createVertexType("Post").addSuperType("Message");
    database.getSchema().createVertexType("Tag");
    database.getSchema().createVertexType("Country");
    database.getSchema().createVertexType("City");
    database.getSchema().createVertexType("Forum");
    database.getSchema().createVertexType("TagClass");

    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("HAS_CREATOR");
    database.getSchema().createEdgeType("REPLY_OF");
    database.getSchema().createEdgeType("HAS_TAG");
    database.getSchema().createEdgeType("IS_LOCATED_IN");
    database.getSchema().createEdgeType("IS_PART_OF");
    database.getSchema().createEdgeType("HAS_MEMBER");
    database.getSchema().createEdgeType("CONTAINER_OF");
    database.getSchema().createEdgeType("HAS_TYPE");
    database.getSchema().createEdgeType("LIKES");
    database.getSchema().createEdgeType("HAS_INTEREST");

    database.transaction(() -> {
      // Build a small LSQB-like graph
      database.command("opencypher", "CREATE (p1:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (p2:Person {name: 'Bob'})");
      database.command("opencypher", "CREATE (p3:Person {name: 'Charlie'})");
      database.command("opencypher", "CREATE (c:Comment {text: 'hello'})");
      database.command("opencypher", "CREATE (po:Post {text: 'world'})");
      database.command("opencypher", "CREATE (t1:Tag {name: 'Java'})");
      database.command("opencypher", "CREATE (t2:Tag {name: 'Python'})");
      database.command("opencypher", "CREATE (tc:TagClass {name: 'Programming'})");
      database.command("opencypher", "CREATE (country:Country {name: 'Italy'})");
      database.command("opencypher", "CREATE (city:City {name: 'Rome'})");
      database.command("opencypher", "CREATE (forum:Forum {name: 'Tech'})");

      // Edges
      database.command("opencypher",
          "MATCH (p1:Person {name: 'Alice'}), (p2:Person {name: 'Bob'}) CREATE (p1)-[:KNOWS]->(p2)");
      database.command("opencypher",
          "MATCH (p2:Person {name: 'Bob'}), (p3:Person {name: 'Charlie'}) CREATE (p2)-[:KNOWS]->(p3)");
      database.command("opencypher",
          "MATCH (c:Comment {text: 'hello'}), (p1:Person {name: 'Alice'}) CREATE (c)-[:HAS_CREATOR]->(p1)");
      database.command("opencypher",
          "MATCH (po:Post {text: 'world'}), (p2:Person {name: 'Bob'}) CREATE (po)-[:HAS_CREATOR]->(p2)");
      database.command("opencypher",
          "MATCH (c:Comment {text: 'hello'}), (po:Post {text: 'world'}) CREATE (c)-[:REPLY_OF]->(po)");
      database.command("opencypher",
          "MATCH (c:Comment {text: 'hello'}), (t1:Tag {name: 'Java'}) CREATE (c)-[:HAS_TAG]->(t1)");
      database.command("opencypher",
          "MATCH (po:Post {text: 'world'}), (t2:Tag {name: 'Python'}) CREATE (po)-[:HAS_TAG]->(t2)");
      database.command("opencypher",
          "MATCH (t1:Tag {name: 'Java'}), (tc:TagClass {name: 'Programming'}) CREATE (t1)-[:HAS_TYPE]->(tc)");
      database.command("opencypher",
          "MATCH (city:City {name: 'Rome'}), (country:Country {name: 'Italy'}) CREATE (city)-[:IS_PART_OF]->(country)");
      database.command("opencypher",
          "MATCH (p1:Person {name: 'Alice'}), (city:City {name: 'Rome'}) CREATE (p1)-[:IS_LOCATED_IN]->(city)");
      database.command("opencypher",
          "MATCH (forum:Forum {name: 'Tech'}), (p1:Person {name: 'Alice'}) CREATE (forum)-[:HAS_MEMBER]->(p1)");
      database.command("opencypher",
          "MATCH (forum:Forum {name: 'Tech'}), (po:Post {text: 'world'}) CREATE (forum)-[:CONTAINER_OF]->(po)");
      database.command("opencypher",
          "MATCH (p3:Person {name: 'Charlie'}), (t1:Tag {name: 'Java'}) CREATE (p3)-[:HAS_INTEREST]->(t1)");
      database.command("opencypher",
          "MATCH (p1:Person {name: 'Alice'}), (po:Post {text: 'world'}) CREATE (p1)-[:LIKES]->(po)");
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void q1UsesItsCountPushDown() {
    assertPushDown(Q1, "COUNT CHAIN PATHS");
  }

  @Test
  void q2UsesItsCountPushDown() {
    assertPushDown(Q2, "COUNT PAIR JOIN");
  }

  @Test
  void q3UsesItsCountPushDown() {
    assertPushDown(Q3, "COUNT TRIANGLES");
  }

  @Test
  void q4UsesItsCountPushDown() {
    assertPushDown(Q4, "COUNT STAR JOIN");
  }

  @Test
  void q5UsesItsCountPushDown() {
    assertPushDown(Q5, "COUNT CHAIN PATHS");
  }

  @Test
  void q6UsesItsCountPushDown() {
    assertPushDown(Q6, "COUNT CHAIN PATHS");
  }

  @Test
  void q7UsesItsCountPushDown() {
    assertPushDown(Q7, "COUNT STAR JOIN");
  }

  @Test
  void q9UsesItsCountPushDown() {
    assertPushDown(Q9, "COUNT ANTI-JOIN CHAIN");
  }

  private void assertPushDown(final String query, final String operator) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(plan).contains("Using Count Push-Down").contains(operator);
    }
  }
}
