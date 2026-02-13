package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the problematic stackoverflow query with OptionalMatchStep streaming optimization.
 * This query previously caused OOM crashes due to cartesian product explosion.
 * With the streaming optimization (MAX_BUFFER_SIZE = 10,000), it should complete successfully.
 */
public class StackOverflowQueryTest {

  @Test
  public void testStackOverflowQueryWithStreamingOptimization() {
    final String dbPath = "../server/databases/stackoverflow_tiny_graph_olap_arcadedb";
    final File dbDir = new File(dbPath);

    if (!dbDir.exists()) {
      // System.out.println("Database not found at: " + dbPath);
      // System.out.println("Skipping test - database required for manual testing");
      return;
    }

    // System.out.println("========================================");
    // System.out.println("Testing OptionalMatch Streaming Optimization");
    // System.out.println("========================================");
    // System.out.println();
    // System.out.println("Database: " + dbPath);
    // System.out.println("Max buffer size: " + System.getProperty("arcadedb.optionalMatch.maxBufferSize", "10000"));
    // System.out.println();

    try (final Database db = new DatabaseFactory(dbPath).open()) {
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

      // System.out.println("Query:");
      // System.out.println(query);
      // System.out.println();
      // System.out.println("========================================");
      // System.out.println();

      final long startTime = System.currentTimeMillis();

      try (final ResultSet resultSet = db.command("opencypher", query)) {
        int rowCount = 0;
        // System.out.println("Results:");
        // System.out.println("-----------------------------------------");
        System.out.printf("%-15s | %-15s%n", "Question ID", "Total Comments");
        // System.out.println("-----------------------------------------");

        while (resultSet.hasNext()) {
          final Result row = resultSet.next();
          final Object questionId = row.getProperty("question_id");
          final Object totalComments = row.getProperty("total_comments");

          System.out.printf("%-15s | %-15s%n", questionId, totalComments);
          rowCount++;
        }

        // System.out.println("-----------------------------------------");
        // System.out.println();

        final long endTime = System.currentTimeMillis();
        final long duration = endTime - startTime;

        // System.out.println("========================================");
        // System.out.println("Success! Query completed without OOM");
        // System.out.println("========================================");
        // System.out.println();
        // System.out.println("Rows returned: " + rowCount);
        // System.out.println("Execution time: " + duration + " ms");
        // System.out.println();
        // System.out.println("Before optimization: This query would cause OOM crash");
        // System.out.println("After optimization: Bounded buffering (10K rows max per OPTIONAL MATCH)");
        // System.out.println("Memory usage: ~40MB (4 × 10K buffer) instead of 100GB+");

        // Verify we got some results
        assertThat(rowCount).isGreaterThan(0).isLessThanOrEqualTo(10);
      }
    } catch (OutOfMemoryError e) {
      System.err.println("========================================");
      System.err.println("FAILED: OOM Error occurred!");
      System.err.println("========================================");
      System.err.println("This indicates the streaming optimization is not working.");
      System.err.println("Expected: Bounded buffering should prevent OOM");
      throw e;
    }
  }

  @Test
  public void testIncrementalQuery1_JustFirstOptionalMatch() {
    final String dbPath = "../server/databases/stackoverflow_tiny_graph_olap_arcadedb";
    final File dbDir = new File(dbPath);

    if (!dbDir.exists()) {
      // System.out.println("Database not found - skipping test");
      return;
    }

    // System.out.println("========================================");
    // System.out.println("Test 1: Just first OPTIONAL MATCH");
    // System.out.println("========================================");

    try (final Database db = new DatabaseFactory(dbPath).open()) {
      final String query = """
          MATCH (q:Question)
          OPTIONAL MATCH (c1:Comment)-[:COMMENTED_ON]->(q)
          RETURN count(*) AS total
          """;

      // System.out.println("Query: " + query);
      final long start = System.currentTimeMillis();

      try (final ResultSet rs = db.command("opencypher", query)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          // System.out.println("Total rows: " + r.getProperty("total"));
        }
      }

      // System.out.println("Duration: " + (System.currentTimeMillis() - start) + " ms");
      // System.out.println("✅ Test 1 passed - no OOM\n");
    } catch (OutOfMemoryError e) {
      System.err.println("❌ Test 1 FAILED with OOM");
      throw e;
    }
  }

  @Test
  public void testIncrementalQuery2_WithAggregation() {
    final String dbPath = "../server/databases/stackoverflow_tiny_graph_olap_arcadedb";
    final File dbDir = new File(dbPath);

    if (!dbDir.exists()) {
      // System.out.println("Database not found - skipping test");
      return;
    }

    // System.out.println("========================================");
    // System.out.println("Test 2: OPTIONAL MATCH with aggregation");
    // System.out.println("========================================");

    try (final Database db = new DatabaseFactory(dbPath).open()) {
      final String query = """
          MATCH (q:Question)
          OPTIONAL MATCH (c1:Comment)-[:COMMENTED_ON]->(q)
          WITH q, count(c1) AS direct_comments
          RETURN count(*) AS total
          """;

      // System.out.println("Query: " + query);
      final long start = System.currentTimeMillis();

      try (final ResultSet rs = db.command("opencypher", query)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          // System.out.println("Total questions: " + r.getProperty("total"));
        }
      }

      // System.out.println("Duration: " + (System.currentTimeMillis() - start) + " ms");
      // System.out.println("✅ Test 2 passed - no OOM\n");
    } catch (OutOfMemoryError e) {
      System.err.println("❌ Test 2 FAILED with OOM - AggregationStep is the culprit!");
      throw e;
    }
  }

  @Test
  public void testIncrementalQuery3_TwoOptionalMatches() {
    final String dbPath = "../server/databases/stackoverflow_tiny_graph_olap_arcadedb";
    final File dbDir = new File(dbPath);

    if (!dbDir.exists()) {
      // System.out.println("Database not found - skipping test");
      return;
    }

    // System.out.println("========================================");
    // System.out.println("Test 3: Two OPTIONAL MATCH with aggregation");
    // System.out.println("========================================");

    try (final Database db = new DatabaseFactory(dbPath).open()) {
      final String query = """
          MATCH (q:Question)
          OPTIONAL MATCH (c1:Comment)-[:COMMENTED_ON]->(q)
          WITH q, count(c1) AS direct_comments
          OPTIONAL MATCH (q)-[:HAS_ANSWER]->(a:Answer)
          RETURN count(*) AS total
          """;

      // System.out.println("Query: " + query);
      final long start = System.currentTimeMillis();

      try (final ResultSet rs = db.command("opencypher", query)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          // System.out.println("Total: " + r.getProperty("total"));
        }
      }

      // System.out.println("Duration: " + (System.currentTimeMillis() - start) + " ms");
      // System.out.println("✅ Test 3 passed - no OOM\n");
    } catch (OutOfMemoryError e) {
      System.err.println("❌ Test 3 FAILED with OOM");
      throw e;
    }
  }

  @Test
  public void testIncrementalQuery4_ThreeOptionalMatches() {
    final String dbPath = "../server/databases/stackoverflow_tiny_graph_olap_arcadedb";
    final File dbDir = new File(dbPath);

    if (!dbDir.exists()) {
      // System.out.println("Database not found - skipping test");
      return;
    }

    // System.out.println("========================================");
    // System.out.println("Test 4: Three OPTIONAL MATCH - FULL QUERY");
    // System.out.println("========================================");

    try (final Database db = new DatabaseFactory(dbPath).open()) {
      final String query = """
          MATCH (q:Question)
          OPTIONAL MATCH (c1:Comment)-[:COMMENTED_ON]->(q)
          WITH q, count(c1) AS direct_comments
          OPTIONAL MATCH (q)-[:HAS_ANSWER]->(a:Answer)
          OPTIONAL MATCH (c2:Comment)-[:COMMENTED_ON_ANSWER]->(a)
          RETURN count(*) AS total
          """;

      // System.out.println("Query: " + query);
      final long start = System.currentTimeMillis();

      try (final ResultSet rs = db.command("opencypher", query)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          // System.out.println("Total: " + r.getProperty("total"));
        }
      }

      // System.out.println("Duration: " + (System.currentTimeMillis() - start) + " ms");
      // System.out.println("✅ Test 4 passed - no OOM\n");
    } catch (OutOfMemoryError e) {
      System.err.println("❌ Test 4 FAILED with OOM - This is the cartesian explosion!");
      throw e;
    }
  }

  @Test
  public void testIncrementalQuery5_FullQueryWithOrderBy() {
    final String dbPath = "../server/databases/stackoverflow_tiny_graph_olap_arcadedb";
    final File dbDir = new File(dbPath);

    if (!dbDir.exists()) {
      // System.out.println("Database not found - skipping test");
      return;
    }

    // System.out.println("========================================");
    // System.out.println("Test 5: FULL QUERY with ORDER BY and arithmetic");
    // System.out.println("========================================");

    try (final Database db = new DatabaseFactory(dbPath).open()) {
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

      // System.out.println("Query: " + query);
      final long start = System.currentTimeMillis();

      try (final ResultSet rs = db.command("opencypher", query)) {
        int count = 0;
        while (rs.hasNext()) {
          final Result r = rs.next();
          count++;
        }
        // System.out.println("Returned " + count + " rows");
      }

      // System.out.println("Duration: " + (System.currentTimeMillis() - start) + " ms");
      // System.out.println("✅ Test 5 passed - no OOM\n");
    } catch (OutOfMemoryError e) {
      System.err.println("❌ Test 5 FAILED with OOM - ORDER BY causes the problem!");
      throw e;
    }
  }

  @Test
  public void testStackOverflowQueryWithProfile() {
    final String dbPath = "../server/databases/stackoverflow_tiny_graph_olap_arcadedb";
    final File dbDir = new File(dbPath);

    if (!dbDir.exists()) {
      // System.out.println("Database not found at: " + dbPath);
      // System.out.println("Skipping test - database required for manual testing");
      return;
    }

    // System.out.println("========================================");
    // System.out.println("Testing with PROFILE to see execution plan");
    // System.out.println("========================================");
    // System.out.println();

    try (final Database db = new DatabaseFactory(dbPath).open()) {
      final String query = """
          PROFILE MATCH (q:Question)
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
        while (resultSet.hasNext()) {
          resultSet.next();
        }

        // Print execution plan
        // System.out.println();
        // System.out.println("Execution Plan:");
        // System.out.println("========================================");
        // System.out.println(resultSet.getExecutionPlan().get().prettyPrint(0, 2));
        // System.out.println("========================================");
        // System.out.println();
        // System.out.println("Look for 'OPTIONAL MATCH' steps in the plan above.");
        // System.out.println("The streaming optimization is active (bounded buffering).");
      }
    }
  }
}
