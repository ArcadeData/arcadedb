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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for GitHub issues #3758 and #3759: Cypher query result mismatches
 * on Stack Overflow graph workload. Tests all 10 OLAP queries from the issues
 * against a synthetic StackOverflow-like graph, both with and without GAV (CSR).
 * Also tests streaming aggregation with OPTIONAL MATCH + count chains to verify
 * correct handling of large cartesian products.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherStackOverflowWorkloadTest extends TestHelper {

  @BeforeEach
  void setupGraph() {
    database.getSchema().createVertexType("User");
    database.getSchema().createVertexType("Question");
    database.getSchema().createVertexType("Answer");
    database.getSchema().createVertexType("Tag");
    database.getSchema().createVertexType("Badge");
    database.getSchema().createVertexType("Comment");

    database.getSchema().createEdgeType("ASKED");
    database.getSchema().createEdgeType("ANSWERED");
    database.getSchema().createEdgeType("ACCEPTED_ANSWER");
    database.getSchema().createEdgeType("HAS_ANSWER");
    database.getSchema().createEdgeType("TAGGED_WITH");
    database.getSchema().createEdgeType("EARNED");
    database.getSchema().createEdgeType("COMMENTED_ON");
    database.getSchema().createEdgeType("COMMENTED_ON_ANSWER");

    database.transaction(() -> {
      // Users
      final MutableVertex alice = database.newVertex("User").set("Id", 1, "DisplayName", "Alice").save();
      final MutableVertex bob = database.newVertex("User").set("Id", 2, "DisplayName", "Bob").save();
      final MutableVertex charlie = database.newVertex("User").set("Id", 3, "DisplayName", "Charlie").save();

      // Tags
      final MutableVertex tagJava = database.newVertex("Tag").set("Id", 10, "TagName", "java").save();
      final MutableVertex tagDb = database.newVertex("Tag").set("Id", 11, "TagName", "database").save();

      // Badges
      final MutableVertex badgeStudent = database.newVertex("Badge").set("Name", "Student").save();
      final MutableVertex badgeTeacher = database.newVertex("Badge").set("Name", "Teacher").save();

      // Questions
      final MutableVertex q1 = database.newVertex("Question").set("Id", 100, "Score", 50).save();
      final MutableVertex q2 = database.newVertex("Question").set("Id", 101, "Score", 30).save();
      final MutableVertex q3 = database.newVertex("Question").set("Id", 102, "Score", 10).save();

      // Answers
      final MutableVertex a1 = database.newVertex("Answer").set("Id", 200).save();
      final MutableVertex a2 = database.newVertex("Answer").set("Id", 201).save();
      final MutableVertex a3 = database.newVertex("Answer").set("Id", 202).save();
      final MutableVertex a4 = database.newVertex("Answer").set("Id", 203).save();

      // Comments
      final MutableVertex c1 = database.newVertex("Comment").set("Id", 300).save();
      final MutableVertex c2 = database.newVertex("Comment").set("Id", 301).save();
      final MutableVertex c3 = database.newVertex("Comment").set("Id", 302).save();

      // ASKED: Alice asked q1, q2; Bob asked q3
      alice.newEdge("ASKED", q1, true, (Object[]) null).save();
      alice.newEdge("ASKED", q2, true, (Object[]) null).save();
      bob.newEdge("ASKED", q3, true, (Object[]) null).save();

      // ANSWERED: Bob answered a1, a2; Charlie answered a3, a4
      bob.newEdge("ANSWERED", a1, true, (Object[]) null).save();
      bob.newEdge("ANSWERED", a2, true, (Object[]) null).save();
      charlie.newEdge("ANSWERED", a3, true, (Object[]) null).save();
      charlie.newEdge("ANSWERED", a4, true, (Object[]) null).save();

      // HAS_ANSWER: q1 has a1, a3; q2 has a2; q3 has a4
      q1.newEdge("HAS_ANSWER", a1, true, (Object[]) null).save();
      q1.newEdge("HAS_ANSWER", a3, true, (Object[]) null).save();
      q2.newEdge("HAS_ANSWER", a2, true, (Object[]) null).save();
      q3.newEdge("HAS_ANSWER", a4, true, (Object[]) null).save();

      // ACCEPTED_ANSWER: q1 accepted a1; q2 accepted a2
      q1.newEdge("ACCEPTED_ANSWER", a1, true, (Object[]) null).save();
      q2.newEdge("ACCEPTED_ANSWER", a2, true, (Object[]) null).save();

      // TAGGED_WITH: q1 tagged java, database; q2 tagged java; q3 tagged database
      q1.newEdge("TAGGED_WITH", tagJava, true, (Object[]) null).save();
      q1.newEdge("TAGGED_WITH", tagDb, true, (Object[]) null).save();
      q2.newEdge("TAGGED_WITH", tagJava, true, (Object[]) null).save();
      q3.newEdge("TAGGED_WITH", tagDb, true, (Object[]) null).save();

      // EARNED: Alice earned Student, Teacher; Bob earned Student; Charlie earned Student
      alice.newEdge("EARNED", badgeStudent, true, (Object[]) null).save();
      alice.newEdge("EARNED", badgeTeacher, true, (Object[]) null).save();
      bob.newEdge("EARNED", badgeStudent, true, (Object[]) null).save();
      charlie.newEdge("EARNED", badgeStudent, true, (Object[]) null).save();

      // COMMENTED_ON (on question): c1 on q1, c2 on q1
      c1.newEdge("COMMENTED_ON", q1, true, (Object[]) null).save();
      c2.newEdge("COMMENTED_ON", q1, true, (Object[]) null).save();

      // COMMENTED_ON_ANSWER: c3 on a1
      c3.newEdge("COMMENTED_ON_ANSWER", a1, true, (Object[]) null).save();
    });
  }

  // Query 1: top_askers (PASSING in issue)
  @Test
  void query1_topAskers() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (u:User)-[:ASKED]->(q:Question) " +
            "RETURN u.Id AS user_id, u.DisplayName AS name, count(q) AS questions " +
            "ORDER BY questions DESC, user_id ASC")) {
      final List<Result> rows = collectResults(rs);
      assertThat(rows).hasSize(2);
      // Alice asked 2, Bob asked 1
      assertThat(((Number) rows.get(0).getProperty("questions")).longValue()).isEqualTo(2L);
      assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Alice");
      assertThat(((Number) rows.get(1).getProperty("questions")).longValue()).isEqualTo(1L);
      assertThat((String) rows.get(1).getProperty("name")).isEqualTo("Bob");
    }
  }

  // Query 2: top_answerers (PASSING in issue)
  @Test
  void query2_topAnswerers() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (u:User)-[:ANSWERED]->(a:Answer) " +
            "RETURN u.Id AS user_id, u.DisplayName AS name, count(a) AS answers " +
            "ORDER BY answers DESC, user_id ASC")) {
      final List<Result> rows = collectResults(rs);
      assertThat(rows).hasSize(2);
      // Bob answered 2, Charlie answered 2
      assertThat(((Number) rows.get(0).getProperty("answers")).longValue()).isEqualTo(2L);
      assertThat(((Number) rows.get(1).getProperty("answers")).longValue()).isEqualTo(2L);
    }
  }

  // Query 3: top_accepted_answerers (FAILING - returned 0 rows)
  @Test
  void query3_topAcceptedAnswerers() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question)-[:ACCEPTED_ANSWER]->(a:Answer) " +
            "MATCH (u:User)-[:ANSWERED]->(a) " +
            "WITH u, count(*) AS accepted " +
            "RETURN u.Id AS user_id, u.DisplayName AS name, accepted " +
            "ORDER BY accepted DESC, user_id ASC")) {
      final List<Result> rows = collectResults(rs);
      // Bob answered a1 (accepted by q1) and a2 (accepted by q2) => accepted=2
      assertThat(rows).isNotEmpty();
      assertThat(rows.size()).isGreaterThanOrEqualTo(1);
      // Bob should be the top accepted answerer with 2
      assertThat(((Number) rows.get(0).getProperty("accepted")).longValue()).isEqualTo(2L);
      assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Bob");
    }
  }

  // Query 4: top_tags_by_questions (FAILING - returned 0 rows)
  @Test
  void query4_topTagsByQuestions() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag) " +
            "RETURN t.Id AS tag_id, t.TagName AS tag, count(q) AS questions " +
            "ORDER BY questions DESC, tag_id ASC")) {
      final List<Result> rows = collectResults(rs);
      // java: q1, q2 => 2; database: q1, q3 => 2
      assertThat(rows).hasSize(2);
      assertThat(((Number) rows.get(0).getProperty("questions")).longValue()).isEqualTo(2L);
      assertThat(((Number) rows.get(1).getProperty("questions")).longValue()).isEqualTo(2L);
    }
  }

  // Query 5: tag_cooccurrence (PASSING in issue)
  @Test
  void query5_tagCooccurrence() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question)-[:TAGGED_WITH]->(t1:Tag) " +
            "MATCH (q)-[:TAGGED_WITH]->(t2:Tag) " +
            "WHERE t1.Id < t2.Id " +
            "RETURN t1.TagName AS tag1, t2.TagName AS tag2, count(*) AS cooccurs " +
            "ORDER BY cooccurs DESC, tag1 ASC, tag2 ASC")) {
      final List<Result> rows = collectResults(rs);
      // q1 is tagged with java(10) and database(11), so t1=java, t2=database
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("cooccurs")).longValue()).isEqualTo(1L);
    }
  }

  // Query 6: top_questions_by_score (PASSING in issue)
  @Test
  void query6_topQuestionsByScore() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question) " +
            "RETURN q.Id AS question_id, q.Score AS score " +
            "ORDER BY score DESC, question_id ASC")) {
      final List<Result> rows = collectResults(rs);
      assertThat(rows).hasSize(3);
      assertThat(((Number) rows.get(0).getProperty("score")).intValue()).isEqualTo(50);
      assertThat(((Number) rows.get(1).getProperty("score")).intValue()).isEqualTo(30);
      assertThat(((Number) rows.get(2).getProperty("score")).intValue()).isEqualTo(10);
    }
  }

  // Query 7: questions_with_most_answers (PASSING in issue)
  @Test
  void query7_questionsWithMostAnswers() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question)-[:HAS_ANSWER]->(a:Answer) " +
            "RETURN q.Id AS question_id, count(a) AS answers " +
            "ORDER BY answers DESC, question_id ASC")) {
      final List<Result> rows = collectResults(rs);
      assertThat(rows).hasSize(3);
      // q1 has 2 answers, q2 has 1, q3 has 1
      assertThat(((Number) rows.get(0).getProperty("answers")).longValue()).isEqualTo(2L);
      assertThat(((Number) rows.get(0).getProperty("question_id")).intValue()).isEqualTo(100);
    }
  }

  // Query 8: asker_answerer_pairs (FAILING - returned 0 rows)
  @Test
  void query8_askerAnswererPairs() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (asker:User)-[:ASKED]->(q:Question)-[:HAS_ANSWER]->(a:Answer)<-[:ANSWERED]-(answerer:User) " +
            "WHERE asker.Id <> answerer.Id " +
            "WITH asker, answerer, count(*) AS interactions " +
            "RETURN asker.Id AS asker_id, answerer.Id AS answerer_id, interactions " +
            "ORDER BY interactions DESC, asker_id ASC, answerer_id ASC")) {
      final List<Result> rows = collectResults(rs);
      // Alice asked q1 -> a1 (Bob), a3 (Charlie); Alice asked q2 -> a2 (Bob)
      // Bob asked q3 -> a4 (Charlie)
      // So: Alice-Bob: 2, Alice-Charlie: 1, Bob-Charlie: 1
      assertThat(rows).isNotEmpty();
      assertThat(rows.size()).isGreaterThanOrEqualTo(1);
      assertThat(((Number) rows.get(0).getProperty("interactions")).longValue()).isEqualTo(2L);
      assertThat(((Number) rows.get(0).getProperty("asker_id")).intValue()).isEqualTo(1); // Alice
      assertThat(((Number) rows.get(0).getProperty("answerer_id")).intValue()).isEqualTo(2); // Bob
    }
  }

  // Query 9: top_badges (FAILING - returned 0 rows) - uses anonymous node (:User)
  @Test
  void query9_topBadges() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (:User)-[:EARNED]->(b:Badge) " +
            "RETURN b.Name AS badge, count(*) AS earned " +
            "ORDER BY earned DESC, badge ASC")) {
      final List<Result> rows = collectResults(rs);
      // Student: 3 (Alice, Bob, Charlie); Teacher: 1 (Alice)
      assertThat(rows).hasSize(2);
      assertThat((String) rows.get(0).getProperty("badge")).isEqualTo("Student");
      assertThat(((Number) rows.get(0).getProperty("earned")).longValue()).isEqualTo(3L);
      assertThat((String) rows.get(1).getProperty("badge")).isEqualTo("Teacher");
      assertThat(((Number) rows.get(1).getProperty("earned")).longValue()).isEqualTo(1L);
    }
  }

  // Query 10: top_questions_by_total_comments (FAILING - wrong values, all 0)
  @Test
  void query10_topQuestionsByTotalComments() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (q:Question) " +
            "OPTIONAL MATCH (c1:Comment)-[:COMMENTED_ON]->(q) " +
            "WITH q, count(c1) AS direct_comments " +
            "OPTIONAL MATCH (q)-[:HAS_ANSWER]->(a:Answer) " +
            "OPTIONAL MATCH (c2:Comment)-[:COMMENTED_ON_ANSWER]->(a) " +
            "WITH q, direct_comments, count(c2) AS answer_comments " +
            "RETURN q.Id AS question_id, direct_comments + answer_comments AS total_comments " +
            "ORDER BY total_comments DESC, question_id ASC")) {
      final List<Result> rows = collectResults(rs);
      assertThat(rows).isNotEmpty();
      // q1 has 2 direct comments + 1 answer comment = 3
      assertThat(((Number) rows.get(0).getProperty("question_id")).intValue()).isEqualTo(100);
      assertThat(((Number) rows.get(0).getProperty("total_comments")).longValue()).isEqualTo(3L);
    }
  }

  // ========================= GAV-accelerated tests =========================
  // These test the same queries but with a Graph Analytical View (CSR) active.
  // The GAV enables fast-path operators (GAVExpandAll, GAVFusedChainOperator,
  // CountEdgesReturnStep with provider). These may use different code paths.

  @Nested
  class WithGAV {

    @BeforeEach
    void buildGAV() {
      // Build a GAV covering all edge types
      GraphAnalyticalView.builder(database)
          .withName("test_gav")
          .skipPersistence()
          .build();
    }

    @Test
    void gavQuery4_topTagsByQuestions() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag) " +
              "RETURN t.Id AS tag_id, t.TagName AS tag, count(q) AS questions " +
              "ORDER BY questions DESC, tag_id ASC")) {
        final List<Result> rows = collectResults(rs);
        assertThat(rows).hasSize(2);
        assertThat(((Number) rows.get(0).getProperty("questions")).longValue()).isEqualTo(2L);
        assertThat(((Number) rows.get(1).getProperty("questions")).longValue()).isEqualTo(2L);
      }
    }

    @Test
    void gavQuery9_topBadges() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (:User)-[:EARNED]->(b:Badge) " +
              "RETURN b.Name AS badge, count(*) AS earned " +
              "ORDER BY earned DESC, badge ASC")) {
        final List<Result> rows = collectResults(rs);
        assertThat(rows).hasSize(2);
        assertThat((String) rows.get(0).getProperty("badge")).isEqualTo("Student");
        assertThat(((Number) rows.get(0).getProperty("earned")).longValue()).isEqualTo(3L);
        assertThat((String) rows.get(1).getProperty("badge")).isEqualTo("Teacher");
        assertThat(((Number) rows.get(1).getProperty("earned")).longValue()).isEqualTo(1L);
      }
    }

    @Test
    void gavQuery3_topAcceptedAnswerers() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (q:Question)-[:ACCEPTED_ANSWER]->(a:Answer) " +
              "MATCH (u:User)-[:ANSWERED]->(a) " +
              "WITH u, count(*) AS accepted " +
              "RETURN u.Id AS user_id, u.DisplayName AS name, accepted " +
              "ORDER BY accepted DESC, user_id ASC")) {
        final List<Result> rows = collectResults(rs);
        assertThat(rows).isNotEmpty();
        assertThat(((Number) rows.get(0).getProperty("accepted")).longValue()).isEqualTo(2L);
        assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Bob");
      }
    }

    @Test
    void gavQuery8_askerAnswererPairs() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (asker:User)-[:ASKED]->(q:Question)-[:HAS_ANSWER]->(a:Answer)<-[:ANSWERED]-(answerer:User) " +
              "WHERE asker.Id <> answerer.Id " +
              "WITH asker, answerer, count(*) AS interactions " +
              "RETURN asker.Id AS asker_id, answerer.Id AS answerer_id, interactions " +
              "ORDER BY interactions DESC, asker_id ASC, answerer_id ASC")) {
        final List<Result> rows = collectResults(rs);
        assertThat(rows).isNotEmpty();
        assertThat(((Number) rows.get(0).getProperty("interactions")).longValue()).isEqualTo(2L);
        assertThat(((Number) rows.get(0).getProperty("asker_id")).intValue()).isEqualTo(1);
        assertThat(((Number) rows.get(0).getProperty("answerer_id")).intValue()).isEqualTo(2);
      }
    }

    @Test
    void gavQuery10_topQuestionsByTotalComments() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (q:Question) " +
              "OPTIONAL MATCH (c1:Comment)-[:COMMENTED_ON]->(q) " +
              "WITH q, count(c1) AS direct_comments " +
              "OPTIONAL MATCH (q)-[:HAS_ANSWER]->(a:Answer) " +
              "OPTIONAL MATCH (c2:Comment)-[:COMMENTED_ON_ANSWER]->(a) " +
              "WITH q, direct_comments, count(c2) AS answer_comments " +
              "RETURN q.Id AS question_id, direct_comments + answer_comments AS total_comments " +
              "ORDER BY total_comments DESC, question_id ASC")) {
        final List<Result> rows = collectResults(rs);
        assertThat(rows).isNotEmpty();
        assertThat(((Number) rows.get(0).getProperty("question_id")).intValue()).isEqualTo(100);
        assertThat(((Number) rows.get(0).getProperty("total_comments")).longValue()).isEqualTo(3L);
      }
    }

    // Isolation test: single-hop IN direction with GAV
    @Test
    void gavSingleHopInDirection() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Answer)<-[:ANSWERED]-(u:User) " +
              "RETURN a.Id AS answer_id, u.Id AS user_id")) {
        final List<Result> rows = collectResults(rs);
        // Bob answered a1(200), a2(201); Charlie answered a3(202), a4(203)
        assertThat(rows).hasSize(4);
      }
    }

    // Isolation test: 2-hop chain ending with IN direction
    @Test
    void gavTwoHopChainWithReverseDirection() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (q:Question)-[:HAS_ANSWER]->(a:Answer)<-[:ANSWERED]-(u:User) " +
              "RETURN q.Id AS question_id, u.Id AS user_id")) {
        final List<Result> rows = collectResults(rs);
        // q1(100)->a1<-Bob(2), q1(100)->a3<-Charlie(3), q2(101)->a2<-Bob(2), q3(102)->a4<-Charlie(3)
        assertThat(rows).hasSize(4);
      }
    }

    // Isolation: 3-hop chain without WHERE or aggregation
    @Test
    void gavThreeHopChainNoFilter() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (asker:User)-[:ASKED]->(q:Question)-[:HAS_ANSWER]->(a:Answer)<-[:ANSWERED]-(answerer:User) " +
              "RETURN asker.Id AS asker_id, answerer.Id AS answerer_id")) {
        final List<Result> rows = collectResults(rs);
        // Alice asked q1->a1(Bob),a3(Charlie), q2->a2(Bob); Bob asked q3->a4(Charlie)
        // Including self-answers: Alice-Bob(a1), Alice-Charlie(a3), Alice-Bob(a2), Bob-Charlie(a4) = 4 rows
        assertThat(rows).isNotEmpty();
      }
    }

    // Isolation: 3-hop chain WITH aggregation but NO WHERE
    @Test
    void gavThreeHopChainWithAggregation() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (asker:User)-[:ASKED]->(q:Question)-[:HAS_ANSWER]->(a:Answer)<-[:ANSWERED]-(answerer:User) " +
              "WITH asker, answerer, count(*) AS interactions " +
              "RETURN asker.Id AS asker_id, answerer.Id AS answerer_id, interactions " +
              "ORDER BY interactions DESC, asker_id ASC, answerer_id ASC")) {
        final List<Result> rows = collectResults(rs);
        assertThat(rows).isNotEmpty();
      }
    }

    // Isolation: 3-hop chain WITH WHERE but NO aggregation
    @Test
    void gavThreeHopChainWithFilterNoAgg() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (asker:User)-[:ASKED]->(q:Question)-[:HAS_ANSWER]->(a:Answer)<-[:ANSWERED]-(answerer:User) " +
              "WHERE asker.Id <> answerer.Id " +
              "RETURN asker.Id AS asker_id, answerer.Id AS answerer_id")) {
        final List<Result> rows = collectResults(rs);
        assertThat(rows).isNotEmpty();
      }
    }

    // Also test the passing queries to ensure GAV doesn't break them
    @Test
    void gavQuery1_topAskers() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (u:User)-[:ASKED]->(q:Question) " +
              "RETURN u.Id AS user_id, u.DisplayName AS name, count(q) AS questions " +
              "ORDER BY questions DESC, user_id ASC")) {
        final List<Result> rows = collectResults(rs);
        assertThat(rows).hasSize(2);
        assertThat(((Number) rows.get(0).getProperty("questions")).longValue()).isEqualTo(2L);
      }
    }

    @Test
    void gavQuery5_tagCooccurrence() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (q:Question)-[:TAGGED_WITH]->(t1:Tag) " +
              "MATCH (q)-[:TAGGED_WITH]->(t2:Tag) " +
              "WHERE t1.Id < t2.Id " +
              "RETURN t1.TagName AS tag1, t2.TagName AS tag2, count(*) AS cooccurs " +
              "ORDER BY cooccurs DESC, tag1 ASC, tag2 ASC")) {
        final List<Result> rows = collectResults(rs);
        assertThat(rows).hasSize(1);
        assertThat(((Number) rows.get(0).getProperty("cooccurs")).longValue()).isEqualTo(1L);
      }
    }

    @Test
    void gavQuery7_questionsWithMostAnswers() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (q:Question)-[:HAS_ANSWER]->(a:Answer) " +
              "RETURN q.Id AS question_id, count(a) AS answers " +
              "ORDER BY answers DESC, question_id ASC")) {
        final List<Result> rows = collectResults(rs);
        assertThat(rows).hasSize(3);
        assertThat(((Number) rows.get(0).getProperty("answers")).longValue()).isEqualTo(2L);
        assertThat(((Number) rows.get(0).getProperty("question_id")).intValue()).isEqualTo(100);
      }
    }
  }

  private List<Result> collectResults(final ResultSet rs) {
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    return results;
  }
}
