package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test streaming aggregation in AggregationStep and GroupByAggregationStep.
 * Verifies that queries with OPTIONAL MATCH + aggregation can handle large
 * cartesian products without OOM by processing rows in batches.
 */
class StreamingAggregationTest {
  private static final String DB_PATH = "target/databases/StreamingAggregationTest";
  private Database db;

  @BeforeEach
  void setup() {
    final File dbDir = new File(DB_PATH);
    if (dbDir.exists()) {
      deleteDirectory(dbDir);
    }

    db = new DatabaseFactory(DB_PATH).create();

    // Create schema similar to stackoverflow
    db.transaction(() -> {
      final DocumentType question = db.getSchema().createVertexType("Question");
      question.createProperty("Id", Type.INTEGER);

      final DocumentType answer = db.getSchema().createVertexType("Answer");
      answer.createProperty("Id", Type.INTEGER);

      final DocumentType comment = db.getSchema().createVertexType("Comment");
      comment.createProperty("Id", Type.INTEGER);

      db.getSchema().createEdgeType("HAS_ANSWER");
      db.getSchema().createEdgeType("COMMENTED_ON");
      db.getSchema().createEdgeType("COMMENTED_ON_ANSWER");
    });

    // Create test data: 100 questions, each with 10 answers, each answer with 5 comments
    // This creates a potential cartesian product of 100 × 10 × 5 = 5,000 rows
    db.transaction(() -> {
      for (int q = 0; q < 100; q++) {
        final var question = db.newVertex("Question").set("Id", q);
        question.save();

        // Direct comments on question (0-3 per question)
        for (int dc = 0; dc < (q % 4); dc++) {
          final var directComment = db.newVertex("Comment").set("Id", q * 1000 + dc);
          directComment.save();
          final var edge = directComment.newEdge("COMMENTED_ON", question);
          edge.save();
        }

        // Answers for this question
        for (int a = 0; a < 10; a++) {
          final var answer = db.newVertex("Answer").set("Id", q * 100 + a);
          answer.save();
          final var answerEdge = question.newEdge("HAS_ANSWER", answer);
          answerEdge.save();

          // Comments on this answer
          for (int c = 0; c < 5; c++) {
            final var comment = db.newVertex("Comment").set("Id", q * 10000 + a * 100 + c);
            comment.save();
            final var commentEdge = comment.newEdge("COMMENTED_ON_ANSWER", answer);
            commentEdge.save();
          }
        }
      }
    });
  }

  @AfterEach
  void teardown() {
    if (db != null) {
      db.drop();
      db = null;
    }
  }

  @Test
  void streamingAggregationWithCartesianProduct() {
    // This query creates a cartesian product before the second aggregation:
    // 100 questions × 10 answers × 5 comments = 5,000 intermediate rows
    // With streaming, this should complete without buffering all 5K rows
    final String query = """
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

    try (final ResultSet resultSet = db.command("opencypher", query)) {
      int rowCount = 0;
      long prevTotal = Long.MAX_VALUE;

      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final long totalComments = ((Number) row.getProperty("total_comments")).longValue();

        // Verify ORDER BY DESC is working
        assertThat(totalComments).isLessThanOrEqualTo(prevTotal);
        prevTotal = totalComments;

        rowCount++;
      }

      // Should return exactly LIMIT 10 rows
      assertThat(rowCount).isEqualTo(10);
    }
  }

  @Test
  void groupByAggregationStreaming() {
    // Test GROUP BY with large result set
    // This groups 100 questions by (Id % 10), creating 10 groups
    // Each group aggregates over multiple answers and comments
    final String query = """
        MATCH (q:Question)
        OPTIONAL MATCH (q)-[:HAS_ANSWER]->(a:Answer)
        WITH q.Id % 10 AS group_key, count(a) AS answer_count
        RETURN group_key, answer_count
        ORDER BY answer_count DESC
        """;

    try (final ResultSet resultSet = db.command("opencypher", query)) {
      int rowCount = 0;
      long totalAnswers = 0;

      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final long answerCount = ((Number) row.getProperty("answer_count")).longValue();
        totalAnswers += answerCount;
        rowCount++;
      }

      // Should have 10 groups
      assertThat(rowCount).isEqualTo(10);

      // Total should be 100 questions × 10 answers = 1000
      assertThat(totalAnswers).isEqualTo(1000);
    }
  }

  @Test
  void simpleAggregationStreaming() {
    // Test simple aggregation (no GROUP BY) with cartesian product
    final String query = """
        MATCH (q:Question)
        OPTIONAL MATCH (q)-[:HAS_ANSWER]->(a:Answer)
        OPTIONAL MATCH (c:Comment)-[:COMMENTED_ON_ANSWER]->(a)
        RETURN count(c) AS total_comments
        """;

    try (final ResultSet resultSet = db.command("opencypher", query)) {
      assertThat(resultSet.hasNext()).isTrue();
      final Result row = resultSet.next();
      final long totalComments = ((Number) row.getProperty("total_comments")).longValue();

      // Should be 100 questions × 10 answers × 5 comments = 5000
      assertThat(totalComments).isEqualTo(5000);

      // Should return only 1 row (aggregation without grouping)
      assertThat(resultSet.hasNext()).isFalse();
    }
  }

  @Test
  void multipleAggregationsStreaming() {
    // Test multiple aggregations in same query
    final String query = """
        MATCH (q:Question)
        OPTIONAL MATCH (c1:Comment)-[:COMMENTED_ON]->(q)
        OPTIONAL MATCH (q)-[:HAS_ANSWER]->(a:Answer)
        WITH count(DISTINCT q) AS question_count, count(c1) AS direct_comment_count, count(a) AS answer_count
        RETURN question_count, direct_comment_count, answer_count
        """;

    try (final ResultSet resultSet = db.command("opencypher", query)) {
      assertThat(resultSet.hasNext()).isTrue();
      final Result row = resultSet.next();

      assertThat(((Number) row.getProperty("question_count")).longValue()).isEqualTo(100);
      // The two OPTIONAL MATCHes create a cartesian product: each comment row is multiplied
      // by the 10 answers per question. Direct comments without cartesian = 25 × (0+1+2+3) = 150,
      // but each comment row appears 10 times (once per answer), so count(c1) = 150 × 10 = 1500.
      assertThat(((Number) row.getProperty("direct_comment_count")).longValue()).isEqualTo(1500);
      // Similarly, answer rows are multiplied by comment rows per question:
      // q%4=0: 1×10=10, q%4=1: 1×10=10, q%4=2: 2×10=20, q%4=3: 3×10=30
      // 25 × (10+10+20+30) = 25 × 70 = 1750
      assertThat(((Number) row.getProperty("answer_count")).longValue()).isEqualTo(1750);
    }
  }

  private void deleteDirectory(final File directory) {
    if (directory.isDirectory()) {
      final File[] files = directory.listFiles();
      if (files != null) {
        for (final File file : files) {
          deleteDirectory(file);
        }
      }
    }
    directory.delete();
  }
}
