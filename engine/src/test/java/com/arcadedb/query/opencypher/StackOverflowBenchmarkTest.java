package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIf("databaseExists")
class StackOverflowBenchmarkTest {

  private static final String DB_PATH = "/tmp/stackoverflow-graph";
  private Database database;

  static boolean databaseExists() {
    return new File(DB_PATH).exists();
  }

  @BeforeAll
  void open() {
    database = new DatabaseFactory(DB_PATH).open();

    // Create GAV covering all benchmark edge types
    final long t = System.currentTimeMillis();
    GraphAnalyticalView.builder(database)
        .withName("bench_gav")
        .withEdgeTypes("ASKED", "ANSWERED", "HAS_ANSWER", "ACCEPTED_ANSWER",
            "TAGGED_WITH", "COMMENTED_ON", "COMMENTED_ON_ANSWER", "EARNED", "LINKED_TO")
        .skipPersistence()
        .build();
    System.out.printf("GAV built in %d ms%n", System.currentTimeMillis() - t);
  }

  @AfterAll
  void close() {
    if (database != null)
      database.close();
  }

  @Test @Order(1) void q1_topAskers()             { runQuery("Q1", Q1); }
  @Test @Order(2) void q2_topAnswerers()           { runQuery("Q2", Q2); }
  @Test @Order(3) void q3_acceptedAnswerPatterns() { runQuery("Q3", Q3); }
  @Test @Order(4) void q4_tagPopularity()          { runQuery("Q4", Q4); }
  @Test @Order(5) void q5_tagCooccurrence()        { runQuery("Q5", Q5); }
  @Test @Order(6) void q6_questionScores()         { runQuery("Q6", Q6); }
  @Test @Order(7) void q7_answerCounts()           { runQuery("Q7", Q7); }
  @Test @Order(8) void q8_userInteractionPairs()   { /* Skip for now - OOM on 4-hop pattern */ }
  @Test @Order(9) void q9_badgeDistribution()      { runQuery("Q9", Q9); }
  @Test @Order(10) void q10_commentVolumes()       { runQuery("Q10", Q10); }

  private void runQuery(final String label, final String query) {
    // Warm-up run
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        while (rs.hasNext())
          rs.next();
      }
    });

    // Timed run with PROFILE
    database.transaction(() -> {
      final long start = System.currentTimeMillis();
      try (final ResultSet rs = database.command("opencypher", "PROFILE " + query)) {
        int rows = 0;
        while (rs.hasNext()) {
          rs.next();
          rows++;
        }
        final long elapsed = System.currentTimeMillis() - start;
        final String plan = rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("(no plan)");
        final boolean usesGAV = plan.contains("GAV");
        System.out.printf("%-4s | %6d ms | %3d rows | GAV=%-5s%n", label, elapsed, rows, usesGAV);
        System.out.println(plan);
        System.out.println("---");
      }
    });
  }

  static final String Q1 = """
      MATCH (u:User)-[:ASKED]->(q:Question)
      RETURN u.Id AS user_id, u.DisplayName AS name, count(q) AS questions
      ORDER BY questions DESC, user_id ASC
      LIMIT 10
      """;

  static final String Q2 = """
      MATCH (u:User)-[:ANSWERED]->(a:Answer)
      RETURN u.Id AS user_id, u.DisplayName AS name, count(a) AS answers
      ORDER BY answers DESC, user_id ASC
      LIMIT 10
      """;

  static final String Q3 = """
      MATCH (q:Question)-[:ACCEPTED_ANSWER]->(a:Answer)
      MATCH (u:User)-[:ANSWERED]->(a)
      WITH u, count(*) AS accepted
      RETURN u.Id AS user_id, u.DisplayName AS name, accepted
      ORDER BY accepted DESC, user_id ASC
      LIMIT 10
      """;

  static final String Q4 = """
      MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag)
      RETURN t.Id AS tag_id, t.TagName AS tag, count(q) AS questions
      ORDER BY questions DESC, tag_id ASC
      LIMIT 10
      """;

  static final String Q5 = """
      MATCH (q:Question)-[:TAGGED_WITH]->(t1:Tag)
      MATCH (q)-[:TAGGED_WITH]->(t2:Tag)
      WHERE t1.Id < t2.Id
      RETURN t1.TagName AS tag1, t2.TagName AS tag2, count(*) AS cooccurs
      ORDER BY cooccurs DESC, tag1 ASC, tag2 ASC
      LIMIT 10
      """;

  static final String Q6 = """
      MATCH (q:Question)
      RETURN q.Id AS question_id, q.Score AS score
      ORDER BY score DESC, question_id ASC
      LIMIT 10
      """;

  static final String Q7 = """
      MATCH (q:Question)-[:HAS_ANSWER]->(a:Answer)
      RETURN q.Id AS question_id, count(a) AS answers
      ORDER BY answers DESC, question_id ASC
      LIMIT 10
      """;

  static final String Q8 = """
      MATCH (asker:User)-[:ASKED]->(q:Question)-[:HAS_ANSWER]->(a:Answer)<-[:ANSWERED]-(answerer:User)
      WHERE asker.Id <> answerer.Id
      WITH asker, answerer, count(*) AS interactions
      RETURN asker.Id AS asker_id, answerer.Id AS answerer_id, interactions
      ORDER BY interactions DESC, asker_id ASC, answerer_id ASC
      LIMIT 10
      """;

  static final String Q9 = """
      MATCH (:User)-[:EARNED]->(b:Badge)
      RETURN b.Name AS badge, count(*) AS earned
      ORDER BY earned DESC, badge ASC
      LIMIT 10
      """;

  static final String Q10 = """
      MATCH (q:Question)
      OPTIONAL MATCH (c1:Comment)-[:COMMENTED_ON]->(q)
      WITH q, count(c1) AS direct_comments
      OPTIONAL MATCH (q)-[:HAS_ANSWER]->(a:Answer)
      OPTIONAL MATCH (c2:Comment)-[:COMMENTED_ON_ANSWER]->(a)
      WITH q, direct_comments, count(c2) AS answer_comments
      RETURN q.Id AS question_id, direct_comments + answer_comments AS total_comments
      ORDER BY total_comments DESC, question_id ASC
      LIMIT 10
      """;
}
