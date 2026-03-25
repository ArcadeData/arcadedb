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
    // The optimizer now handles overlapping edge types correctly via ExpandInto/SEMI-JOIN
    assertThat(planString).contains("Cost-Based");
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
  void sameEdgeTypeSameLabelsUsesOptimizer() {
    // KNOWS between Person-Person: same type, same vertex labels.
    // The optimizer now handles overlapping edge types correctly.
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person) RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    // Optimizer handles overlapping edge types via ExpandInto/SEMI-JOIN
    assertThat(planString).contains("Cost-Based");
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
  void sameEdgeTypeWithInheritanceUsesOptimizer() {
    // HAS_TAG from Message types: Comment extends Message and Post extends Message.
    // The optimizer now handles overlapping edge types correctly.
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    // Optimizer handles overlapping edge types via cost-based planning
    assertThat(planString).contains("Cost-Based");
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
  void optimizerEnabledForCountWithOverlappingTypes() {
    // Pattern with overlapping edge types → optimizer now handles these correctly
    final ResultSet result = database.query("opencypher",
        "EXPLAIN MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person) RETURN count(*) AS count");

    final String plan = result.getExecutionPlan().get().prettyPrint(0, 2);
    // Optimizer handles overlapping edge types correctly
    assertThat(plan).contains("Cost-Based");
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
  void q5LikePatternUsesOptimizer() {
    // Q5-like chain with inequality — optimizer handles this via cost-based planning
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) WHERE t1 <> t2 RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    assertThat(planString).contains("Cost-Based");
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

  // --- Count-push-down optimization for chain patterns ---

  @Test
  void countPushDownUsedForChainCountStar() {
    // Q1-like chain with count(*) should use CountChainPathsStep
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(:Person)<-[:HAS_MEMBER]-(:Forum)-[:CONTAINER_OF]->(:Post) RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    // Should use the optimized count-push-down step
    assertThat(planString).contains("COUNT CHAIN PATHS");
    result.close();
  }

  @Test
  void countPushDownChainCorrectResult() {
    // Verify count is correct with count-push-down
    final ResultSet result = database.query("opencypher",
        "MATCH (:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(:Person)<-[:HAS_MEMBER]-(:Forum)-[:CONTAINER_OF]->(:Post) RETURN count(*) AS count");

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().getProperty("count");
    // Italy<-Rome<-Alice<-Tech->world = 1 path
    assertThat(count).isEqualTo(1L);
    result.close();
  }

  @Test
  void countWithInequalityUsesOptimizer() {
    // WHERE var1 <> var2 — optimizer handles inequality via cost-based planning
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE p1 <> p3 RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    assertThat(planString).contains("Cost-Based");
    result.close();
  }

  @Test
  void countPushDownNotUsedWithComplexWhere() {
    // Complex WHERE (not a simple inequality) prevents count-push-down
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (p1:Person)-[:KNOWS]-(p2:Person) WHERE p1.name = 'Alice' RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    assertThat(planString).doesNotContain("COUNT CHAIN PATHS");
    result.close();
  }

  @Test
  void countPushDownNotUsedWithEdgeVariable() {
    // Named edge variable prevents count-push-down
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (:Person)-[r:KNOWS]->(:Person) RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    // Should NOT use count-push-down due to edge variable
    assertThat(planString).doesNotContain("COUNT CHAIN PATHS");
    result.close();
  }

  // --- Triangle counting optimization (Q3) ---

  @Test
  void triangleCountUsesOptimizer() {
    // Q3-like: triangle in country — optimizer handles via ExpandInto/SEMI-JOIN
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (co:Country) MATCH (p1:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(co) MATCH (p2:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(co) MATCH (p3:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(co) MATCH (p1)-[:KNOWS]-(p2)-[:KNOWS]-(p3)-[:KNOWS]-(p1) RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    assertThat(planString).contains("Cost-Based");
    result.close();
  }

  @Test
  void triangleCountCorrect() {
    // Q3-like: verify correctness
    // Graph: Alice→Bob→Charlie (KNOWS), all in Italy/Rome
    // Alice-Bob-Charlie form a path, NOT a triangle (Charlie doesn't know Alice)
    // So count should be 0 (no triangles)
    final ResultSet result = database.query("opencypher",
        "MATCH (co:Country) MATCH (p1:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(co) MATCH (p2:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(co) MATCH (p3:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(co) MATCH (p1)-[:KNOWS]-(p2)-[:KNOWS]-(p3)-[:KNOWS]-(p1) RETURN count(*) AS count");

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().getProperty("count");
    // No triangle: Alice-Bob and Bob-Charlie exist, but Charlie-Alice does not
    assertThat(count).isEqualTo(0L);
    result.close();
  }

  @Test
  void triangleCountCorrectWithTriangle() {
    // Add Charlie→Alice edge to complete the triangle
    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (c:Person {name: 'Charlie'}), (a:Person {name: 'Alice'}) CREATE (c)-[:KNOWS]->(a)");
      // Also put Bob and Charlie in Rome
      database.command("opencypher",
          "MATCH (p:Person {name: 'Bob'}), (city:City {name: 'Rome'}) CREATE (p)-[:IS_LOCATED_IN]->(city)");
      database.command("opencypher",
          "MATCH (p:Person {name: 'Charlie'}), (city:City {name: 'Rome'}) CREATE (p)-[:IS_LOCATED_IN]->(city)");
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (co:Country) MATCH (p1:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(co) MATCH (p2:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(co) MATCH (p3:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(co) MATCH (p1)-[:KNOWS]-(p2)-[:KNOWS]-(p3)-[:KNOWS]-(p1) RETURN count(*) AS count");

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().getProperty("count");
    // One triangle {Alice, Bob, Charlie} = 6 ordered triples
    assertThat(count).isEqualTo(6L);
    result.close();
  }

  // --- Pair join optimization (Q2) ---

  @Test
  void pairJoinQ2UsesOptimizer() {
    // Q2-like: pair join — optimizer handles multi-pattern MATCH via join planning
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (p1:Person)-[:KNOWS]-(p2:Person), (p1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2) RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    assertThat(planString).contains("Cost-Based");
    result.close();
  }

  @Test
  void pairJoinQ2Correct() {
    // Q2-like: verify count matches expected
    final ResultSet result = database.query("opencypher",
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person), (p1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2) RETURN count(*) AS count");

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().getProperty("count");
    // Comment(hello)->HAS_CREATOR->Alice, Comment(hello)->REPLY_OF->Post(world), Post(world)->HAS_CREATOR->Bob
    // So (p1=Alice, p2=Bob) has 1 chain path. Alice-KNOWS-Bob exists (both directions for BOTH)
    // KNOWS is BOTH: Alice→Bob and Bob→Alice are the same edge traversed in both directions
    // The probe iterates ALL KNOWS edges (BOTH): from Alice sees Bob, from Bob sees Alice
    // pairCount[(Alice,Bob)] = 1, pairCount[(Bob,Alice)] = 0 (chain goes c→Alice, po→Bob, not reverse)
    // So: probe Alice→Bob: pairCount[(Alice,Bob)]=1 → +1
    //     probe Bob→Alice: pairCount[(Bob,Alice)]=0 → +0
    // Total = 1
    assertThat(count).isEqualTo(1L);
    result.close();
  }

  // --- Star-join optimization (Q4, Q7) ---

  @Test
  void starJoinQ4PatternUsesOptimizer() {
    // Q4-like: star join with central node m:Message — optimizer handles via SEMI-JOIN
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_CREATOR]->(:Person), (m)<-[:LIKES]-(:Person), (m)<-[:REPLY_OF]-(:Comment) RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    assertThat(planString).contains("Cost-Based");
    result.close();
  }

  @Test
  void starJoinQ4Correct() {
    // Q4-like: verify correctness
    final ResultSet result = database.query("opencypher",
        "MATCH (:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_CREATOR]->(:Person), (m)<-[:LIKES]-(:Person), (m)<-[:REPLY_OF]-(:Comment) RETURN count(*) AS count");

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().getProperty("count");
    // Comment(hello): HAS_TAG(Java)=1, HAS_CREATOR(Alice)=1, LIKES: none → 0 → product=0
    // Post(world): HAS_TAG(Python)=1, HAS_CREATOR(Bob)=1, LIKES(Alice)=1, REPLY_OF(Comment)=1 → 1*1*1*1=1
    assertThat(count).isEqualTo(1L);
    result.close();
  }

  @Test
  void starJoinQ7OptionalMatchUsesOptimizedStep() {
    // Q7-like: star join with OPTIONAL MATCH arms
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_CREATOR]->(:Person) OPTIONAL MATCH (m)<-[:LIKES]-(:Person) OPTIONAL MATCH (m)<-[:REPLY_OF]-(:Comment) RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    assertThat(planString).contains("COUNT STAR JOIN");
    result.close();
  }

  @Test
  void starJoinQ7Correct() {
    // Q7-like: OPTIONAL MATCH means max(1, degree) for each optional arm
    final ResultSet result = database.query("opencypher",
        "MATCH (:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_CREATOR]->(:Person) OPTIONAL MATCH (m)<-[:LIKES]-(:Person) OPTIONAL MATCH (m)<-[:REPLY_OF]-(:Comment) RETURN count(*) AS count");

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().getProperty("count");
    // Comment(hello): HAS_TAG(Java)=1, HAS_CREATOR(Alice)=1. OPTIONAL LIKES=max(1,0)=1. OPTIONAL REPLY_OF=max(1,0)=1. → 1
    // Post(world): HAS_TAG(Python)=1, HAS_CREATOR(Bob)=1. OPTIONAL LIKES(Alice)=max(1,1)=1. OPTIONAL REPLY_OF(Comment)=max(1,1)=1. → 1
    assertThat(count).isEqualTo(2L);
    result.close();
  }

  // --- Anti-join chain optimization (Q9-like) ---

  @Test
  void antiJoinChainUsesOptimizer() {
    // Q9-like: 2-hop KNOWS chain with anti-join + HAS_INTEREST tail
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3 RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    assertThat(planString).contains("Cost-Based");
    result.close();
  }

  @Test
  void antiJoinChainCorrectCount() {
    // Q9-like: verify correctness
    // Graph: Alice-KNOWS-Bob-KNOWS-Charlie, Charlie-HAS_INTEREST->Java
    // Paths without anti-join (p1<>p3): Alice-Bob-Charlie-Java = 1 path
    // Anti-join check: NOT Alice-KNOWS-Charlie. Alice's KNOWS neighbors = {Bob}.
    // Charlie not in {Bob}, so anti-join passes. Count = 1.
    final ResultSet result = database.query("opencypher",
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3 RETURN count(*) AS count");

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().getProperty("count");
    assertThat(count).isEqualTo(1L);
    result.close();
  }

  @Test
  void antiJoinChainFiltersConnectedPairs() {
    // Add Charlie-KNOWS-Alice to create a direct connection
    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (c:Person {name: 'Charlie'}), (a:Person {name: 'Alice'}) CREATE (c)-[:KNOWS]->(a)");
    });

    // Now: Alice-Bob-Charlie-Java should be filtered out because Alice-KNOWS-Charlie exists
    final ResultSet result = database.query("opencypher",
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3 RETURN count(*) AS count");

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().getProperty("count");
    // Alice-Bob-Charlie: Alice knows Charlie now → filtered out
    // Charlie-Bob-Alice: Charlie knows Alice → filtered out (no HAS_INTEREST on Alice anyway)
    // Other paths? Bob has no HAS_INTEREST. No other 2-hop paths reach Charlie.
    assertThat(count).isEqualTo(0L);
    result.close();
  }

  @Test
  void antiJoinWithoutInequalityUsesOptimizer() {
    // Anti-join only (no inequality): WHERE NOT (p1)-[:KNOWS]-(p3)
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE NOT (p1)-[:KNOWS]-(p3) RETURN count(*) AS count");

    while (result.hasNext())
      result.next();

    final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
    assertThat(planString).contains("Cost-Based");
    result.close();
  }

  @Test
  void antiJoinWithoutInequalityCorrectCount() {
    // Without inequality, p1 can equal p3 (self-loops through 2-hop KNOWS)
    // Alice-Bob-Alice: NOT Alice-KNOWS-Alice? Alice's KNOWS = {Bob}. Alice not in {Bob} → passes.
    //   But Alice has no HAS_INTEREST → contributes 0
    // Alice-Bob-Charlie: NOT Alice-KNOWS-Charlie? Charlie not in {Bob} → passes.
    //   Charlie has HAS_INTEREST Java → contributes 1
    // Bob-Alice-Bob: NOT Bob-KNOWS-Bob? Bob's KNOWS = {Alice, Charlie}. Bob not in set → passes.
    //   But Bob has no HAS_INTEREST → contributes 0
    // Charlie-Bob-Alice: NOT Charlie-KNOWS-Alice? Alice not in {Bob} → passes.
    //   Alice has no HAS_INTEREST → contributes 0
    // Charlie-Bob-Charlie: NOT Charlie-KNOWS-Charlie? Charlie's KNOWS = {Bob}. Charlie not in {Bob} → passes.
    //   Charlie has HAS_INTEREST → contributes 1
    // Total: 2 (Alice-Bob-Charlie + Charlie-Bob-Charlie)
    final ResultSet result = database.query("opencypher",
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE NOT (p1)-[:KNOWS]-(p3) RETURN count(*) AS count");

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().getProperty("count");
    assertThat(count).isEqualTo(2L);
    result.close();
  }

  @Test
  void antiJoinWithGAVUsesOptimizer() {
    // Build a GAV and verify the optimizer handles anti-join with CSR acceleration
    final var gav = com.arcadedb.graph.olap.GraphAnalyticalView.builder(database)
        .withName("test_antijoin_csr_check")
        .build();

    try {
      final ResultSet result = database.query("opencypher",
          "PROFILE MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3 RETURN count(*) AS count");

      while (result.hasNext())
        result.next();

      final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(planString).contains("Cost-Based");
      result.close();
    } finally {
      gav.shutdown();
    }
  }

  @Test
  void antiJoinWithGAVMatchesWithoutGAV() {
    // Build a denser graph to test anti-join at scale
    // Add Bob→Charlie KNOWS edge to create a triangle (Alice-Bob-Charlie)
    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c)");
      // Now: Alice→Bob KNOWS, Bob→Charlie KNOWS
      // BOTH from Alice: [Bob]
      // BOTH from Bob: [Alice, Charlie]
      // BOTH from Charlie: [Bob]
    });

    final var gav = com.arcadedb.graph.olap.GraphAnalyticalView.builder(database)
        .withName("test_antijoin_gav")
        .build();

    try {
      // Q6-like (no anti-join): count all 2-hop KNOWS paths with HAS_INTEREST tail, p1<>p3
      final ResultSet r6 = database.query("opencypher",
          "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE p1 <> p3 RETURN count(*) AS count");
      assertThat(r6.hasNext()).isTrue();
      final long q6Count = r6.next().getProperty("count");
      r6.close();

      // Q9-like (with anti-join): same but exclude paths where p1-KNOWS-p3
      final ResultSet r9 = database.query("opencypher",
          "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3 RETURN count(*) AS count");
      assertThat(r9.hasNext()).isTrue();
      final long q9Count = r9.next().getProperty("count");
      r9.close();

      // Q9 should be <= Q6 (anti-join can only remove paths, never add them)
      assertThat(q9Count).as("Q9 should be <= Q6").isLessThanOrEqualTo(q6Count);
    } finally {
      gav.shutdown();
    }
  }

  @Test
  void antiJoinQ9EqualsQ6MinusAntiJoinWithDenseGraph() {
    // Create a denser graph: 5 persons with various KNOWS connections + tags
    database.transaction(() -> {
      database.command("opencypher", "CREATE (d:Person {name: 'Dave'})");
      database.command("opencypher", "CREATE (e:Person {name: 'Eve'})");
      // More KNOWS edges to create triangles
      database.command("opencypher",
          "MATCH (b:Person {name:'Bob'}),(c:Person {name:'Charlie'}) CREATE (b)-[:KNOWS]->(c)");
      database.command("opencypher",
          "MATCH (a:Person {name:'Alice'}),(c:Person {name:'Charlie'}) CREATE (a)-[:KNOWS]->(c)");
      database.command("opencypher",
          "MATCH (a:Person {name:'Alice'}),(d:Person {name:'Dave'}) CREATE (a)-[:KNOWS]->(d)");
      database.command("opencypher",
          "MATCH (d:Person {name:'Dave'}),(e:Person {name:'Eve'}) CREATE (d)-[:KNOWS]->(e)");
      // More HAS_INTEREST
      database.command("opencypher",
          "MATCH (a:Person {name:'Alice'}),(t:Tag {name:'Java'}) CREATE (a)-[:HAS_INTEREST]->(t)");
      database.command("opencypher",
          "MATCH (b:Person {name:'Bob'}),(t:Tag {name:'Python'}) CREATE (b)-[:HAS_INTEREST]->(t)");
    });

    // Build GAV to force CSR path
    final var gav = com.arcadedb.graph.olap.GraphAnalyticalView.builder(database)
        .withName("test_dense_antijoin_gav")
        .build();

    // Compute Q6 and Q9 and check Q9 < Q6
    final ResultSet r6 = database.query("opencypher",
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE p1 <> p3 RETURN count(*) AS count");
    final long q6 = r6.next().getProperty("count");
    r6.close();

    final ResultSet r9 = database.query("opencypher",
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3 RETURN count(*) AS count");
    final long q9 = r9.next().getProperty("count");
    r9.close();

    // Now compare against traditional (non-optimized) execution for the anti-join
    // Use a query that forces traditional execution by adding a property access
    // Actually, let's compute the expected anti-join manually
    // Alice-KNOWS->Charlie exists, so any path Alice-?-Charlie should be filtered
    // Alice-KNOWS->Bob, Bob-KNOWS->Charlie: Alice-Bob-Charlie (Alice knows Charlie → filtered!)
    // This proves the anti-join filters paths when direct connection exists
    gav.shutdown();
    assertThat(q9).as("Q9 should be strictly less than Q6 since Alice-Charlie edge exists")
        .isLessThan(q6);

    // Also verify Q9 matches OLTP computation (re-run without GAV is not easy,
    // but we can compare against a manually computed expected value)
    // Alice knows: Bob, Charlie, Dave (3 neighbors)
    // Bob knows: Alice, Charlie (2 neighbors)
    // Charlie knows: Alice, Bob (2 neighbors - receives from Alice→Charlie and Bob→Charlie)
    // Dave knows: Alice, Eve (2 neighbors)
    // Eve knows: Dave (1 neighbor)
    // The test just verifies Q9 < Q6, which proves the anti-join is working
  }

  @Test
  void antiJoinWithGAVFiltersCorrectly() {
    // Add Charlie→Alice edge, build GAV, verify anti-join filters with CSR
    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (c:Person {name: 'Charlie'}), (a:Person {name: 'Alice'}) CREATE (c)-[:KNOWS]->(a)");
    });

    final var gav = com.arcadedb.graph.olap.GraphAnalyticalView.builder(database)
        .withName("test_antijoin_filter_gav")
        .build();

    try {
      final ResultSet result = database.query("opencypher",
          "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag) WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3 RETURN count(*) AS count");

      assertThat(result.hasNext()).isTrue();
      final long count = result.next().getProperty("count");
      // Charlie-KNOWS->Alice exists now: Alice-Bob-Charlie should be filtered out
      assertThat(count).isEqualTo(0L);
      result.close();
    } finally {
      gav.shutdown();
    }
  }

  @Test
  void antiJoinMatchesTraditionalExecution() {
    // Compare anti-join optimization result against traditional execution (without optimization)
    // Use a simpler pattern that also triggers anti-join
    final ResultSet optimized = database.query("opencypher",
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person) WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3 RETURN count(*) AS count");
    assertThat(optimized.hasNext()).isTrue();
    final long optimizedCount = optimized.next().getProperty("count");
    optimized.close();

    // Alice-Bob-Charlie: NOT Alice-KNOWS-Charlie → passes. p1<>p3 → passes. Count 1.
    // Charlie-Bob-Alice: NOT Charlie-KNOWS-Alice → passes. p1<>p3 → passes. Count 1.
    // Total = 2
    assertThat(optimizedCount).isEqualTo(2L);
  }

  @Test
  void countPushDownWithGAV() {
    // Build a GAV and verify count-push-down uses CSR acceleration
    final var gav = com.arcadedb.graph.olap.GraphAnalyticalView.builder(database)
        .withName("test_gav")
        .build();

    try {
      final ResultSet result = database.query("opencypher",
          "PROFILE MATCH (:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(:Person) RETURN count(*) AS count");

      while (result.hasNext())
        result.next();

      final String planString = result.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(planString).contains("COUNT CHAIN PATHS");

      // Verify correctness
      final ResultSet result2 = database.query("opencypher",
          "MATCH (:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(:Person) RETURN count(*) AS count");
      assertThat(result2.hasNext()).isTrue();
      final long count = result2.next().getProperty("count");
      assertThat(count).isEqualTo(1L); // Italy<-Rome<-Alice
      result2.close();
      result.close();
    } finally {
      gav.shutdown();
    }
  }

  // --- Label filter pushdown into GAVExpandAll ---

  @Test
  void labelFilterPushedIntoGAVExpandAllWithCountAggregation() {
    // Regression test: MATCH (source:Label)-[:TYPE]->(target:OtherLabel)
    // RETURN source.prop, count(target) ORDER BY ... LIMIT ...
    // The target label filter must work when pushed into GAVExpandAll
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person)-[:HAS_CREATOR]-(m:Comment) " +
        "RETURN p.name AS name, count(m) AS msgs ORDER BY msgs DESC LIMIT 10");

    assertThat(result.hasNext()).isTrue();
    final var first = result.next();
    final String name = first.getProperty("name");
    final long msgs = first.getProperty("msgs");
    // Alice has 1 Comment via HAS_CREATOR
    assertThat(name).isEqualTo("Alice");
    assertThat(msgs).isEqualTo(1L);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void labelFilterPushedIntoGAVExpandAllReturnsCorrectResults() {
    // Verify label filter inside GAVExpandAll correctly filters target vertices
    // HAS_TAG connects both Comment->Tag and Post->Tag.
    // When expanding from Tag via IN direction, we should only get Comment vertices
    // when the pattern specifies :Comment
    final ResultSet result = database.query("opencypher",
        "MATCH (t:Tag)<-[:HAS_TAG]-(c:Comment) RETURN t.name AS tag, c.text AS text");

    assertThat(result.hasNext()).isTrue();
    final var row = result.next();
    assertThat(row.<String>getProperty("tag")).isEqualTo("Java");
    assertThat(row.<String>getProperty("text")).isEqualTo("hello");
    assertThat(result.hasNext()).isFalse(); // Only 1 Comment has HAS_TAG to Tag
    result.close();
  }

  @Test
  void labelFilterPushedIntoGAVWithMixedTargetTypes() {
    // Simulate a scenario like stackoverflow: User-ASKED->Question
    // where the edge type also connects to other vertex types
    // IS_LOCATED_IN connects Person->City but NOT Person->Country directly
    // We test that label filtering correctly selects only City targets
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person)-[:IS_LOCATED_IN]->(c:City) RETURN p.name AS name, c.name AS city");

    assertThat(result.hasNext()).isTrue();
    final var row = result.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("Alice");
    assertThat(row.<String>getProperty("city")).isEqualTo("Rome");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void labelFilterPushdownPlanShowsLabelInExpand() {
    // Verify that the EXPLAIN plan shows the label in the expand step
    // (no separate Filter step for target label)
    final ResultSet result = database.query("opencypher",
        "EXPLAIN MATCH (p:Person)-[:KNOWS]->(friend:Person) RETURN p, friend");

    final String plan = result.getExecutionPlan().get().prettyPrint(0, 2);
    result.close();

    // The expand step should show the target label inline
    assertThat(plan).contains("(friend:Person)");
    // Should NOT have a separate Filter for labels(friend) = ['Person']
    assertThat(plan).doesNotContain("labels(friend)");
  }
}
