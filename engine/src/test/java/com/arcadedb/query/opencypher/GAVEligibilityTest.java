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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for GAV/CSR eligibility improvements:
 * 1. Type-aware edge overlap checking (disjoint types unblock fast path after edge-tracking hops)
 * 2. Vertex-label-based disjointness for same-type hops with different source/target labels
 * 3. Optimizer enablement for aggregation queries with disjoint edge types
 */
class GAVEligibilityTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-gav-eligibility").create();

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

  // --- Change 2: Type-aware edge overlap (disjoint-type hops unblocked from cascade) ---

  @Test
  void disjointTypeHopUsesFastPathAfterEdgeTrackingHop() {
    // Pattern: same edge type twice (HAS_TAG) forces traditional path with count(*)
    // The REPLY_OF hop between them should use fast path (disjoint from HAS_TAG)
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) WHERE t1 <> t2 RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    // The REPLY_OF hop should use optimized/fast path, not standard path
    // (because REPLY_OF is disjoint from HAS_TAG in the result)
    // Count standard path occurrences - should be at most 2 (the two HAS_TAG hops)
    final long standardCount = planString.lines().filter(l -> l.contains("traversal: standard")).count();
    assertThat(standardCount).as("Only HAS_TAG hops should use standard path, not REPLY_OF")
        .isLessThanOrEqualTo(2);
    result.close();
  }

  @Test
  void disjointTypeCountCorrectWithCascadeUnblock() {
    // Verify the count is correct when disjoint-type hops use fast path
    final ResultSet result = database.query("opencypher",
        "MATCH (p1:Person)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2:Person) RETURN count(*) AS count");

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().getProperty("count");
    // Alice<-HAS_CREATOR-Comment-REPLY_OF->Post-HAS_CREATOR->Bob = 1 path
    assertThat(count).isEqualTo(1L);
    result.close();
  }

  // --- Change 1: Vertex-label disjointness for same edge type ---

  @Test
  void sameEdgeTypeDifferentSourceLabelsAllowsFastPath() {
    // HAS_CREATOR from Comment and from Post connect different source types.
    // Comment and Post are disjoint (siblings under Message), so edges are necessarily different.
    // With count(*), the query uses traditional path; all hops should use fast path.
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (p1:Person)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2:Person) RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    // With vertex-label disjointness, ALL hops should use fast path (no standard path)
    assertThat(planString).doesNotContain("traversal: standard");
    result.close();
  }

  @Test
  void sameEdgeTypeSameLabelsRequiresEdgeTracking() {
    // KNOWS between Person-Person: same type, same vertex labels → edge tracking required.
    // With count(*) and overlapping types, the optimizer is disabled → traditional path used.
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person) RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    // KNOWS hops must use standard path (same type, same vertex labels, BOTH direction)
    assertThat(planString).contains("traversal: standard");
    result.close();
  }

  @Test
  void sameEdgeTypeSameLabelsPreservesCorrectness() {
    // Verify edge uniqueness: (p1)-[:KNOWS]-(p2)-[:KNOWS]-(p3) should NOT traverse
    // the same edge twice. Alice→Bob→Charlie should work, but Alice→Bob→Alice should
    // be prevented by edge uniqueness (and p1<>p3 also filters it).
    final ResultSet result = database.query("opencypher",
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person) WHERE p1 <> p3 RETURN count(*) AS count");

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().getProperty("count");
    // Alice-Bob-Charlie and Charlie-Bob-Alice (BOTH direction) = 2 paths
    assertThat(count).isEqualTo(2L);
    result.close();
  }

  @Test
  void sameEdgeTypeWithInheritanceRequiresTracking() {
    // HAS_TAG from Message types: Comment extends Message and Post extends Message.
    // The HAS_TAG hops have overlapping source types (Message ⊃ Comment).
    // With count(*), overlapping types force traditional path. Edge tracking should be used.
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    // HAS_TAG hops should use standard path due to type hierarchy overlap
    assertThat(planString).contains("traversal: standard");
    result.close();
  }

  // --- Optimizer enablement for aggregation queries with disjoint edge types ---

  @Test
  void optimizerEnabledForCountWithDisjointTypes() {
    // Q1-like pattern: long chain with all disjoint edge types
    // Should use the cost-based optimizer even though RETURN has count(*)
    final ResultSet result = database.query("opencypher",
        "EXPLAIN MATCH (co:Country)<-[:IS_PART_OF]-(ci:City)<-[:IS_LOCATED_IN]-(p:Person)<-[:HAS_MEMBER]-(f:Forum)-[:CONTAINER_OF]->(po:Post) RETURN count(*) AS count");

    final String plan = result.getExecutionPlan().get().prettyPrint(0, 2);
    // Should use the cost-based optimizer, not traditional execution
    assertThat(plan).contains("Cost-Based");
    result.close();
  }

  @Test
  void optimizerDisabledForCountWithOverlappingTypes() {
    // Pattern with overlapping edge types → optimizer should NOT be used for count(*)
    final ResultSet result = database.query("opencypher",
        "EXPLAIN MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person) RETURN count(*) AS count");

    final String plan = result.getExecutionPlan().get().prettyPrint(0, 2);
    // Should use traditional execution due to overlapping KNOWS types
    assertThat(plan).contains("Traditional");
    result.close();
  }

  // --- Correctness tests for count(*) queries matching LSQB patterns ---

  @Test
  void q1LikeChainCountCorrect() {
    // Partial Q1 pattern
    final ResultSet result = database.query("opencypher",
        "MATCH (:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(:Person)<-[:HAS_MEMBER]-(:Forum)-[:CONTAINER_OF]->(:Post) RETURN count(*) AS count");

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().getProperty("count");
    // Italy<-Rome<-Alice<-Tech->world = 1 path
    assertThat(count).isEqualTo(1L);
    result.close();
  }

  @Test
  void q2LikePatternCorrect() {
    // Q2-like: Person knows Person + Comment-Post
    final ResultSet result = database.query("opencypher",
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person), (p1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2) RETURN count(*) AS count");

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().getProperty("count");
    // Alice-KNOWS-Bob, Alice<-HAS_CREATOR-Comment-REPLY_OF->Post-HAS_CREATOR->Bob = 1
    assertThat(count).isEqualTo(1L);
    result.close();
  }

  @Test
  void q5LikePatternCorrect() {
    // Q5-like: Message + reply with different tags
    final ResultSet result = database.query("opencypher",
        "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) WHERE t1 <> t2 RETURN count(*) AS count");

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().getProperty("count");
    // Post(Python)<-REPLY_OF-Comment(Java): t1=Python, t2=Java → t1<>t2 → 1 path
    assertThat(count).isEqualTo(1L);
    result.close();
  }

  @Test
  void q6LikePatternCorrect() {
    // Q6-like: 2-hop KNOWS + interest
    final ResultSet result = database.query("opencypher",
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE p1 <> p3 RETURN count(*) AS count");

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().getProperty("count");
    // Alice-KNOWS-Bob-KNOWS-Charlie-HAS_INTEREST->Java = 1 path
    assertThat(count).isEqualTo(1L);
    result.close();
  }
}
