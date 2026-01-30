package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #1972: SQL LIMIT does not accept parameters (for dynamic limits)
 * <p>
 * This test verifies that LIMIT and SKIP clauses accept parameters and LET variables.
 * </p>
 */
class Issue1972LimitSkipParametersTest extends TestHelper {

  @Test
  void limitWithLetVariable() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc IF NOT EXISTS");
      database.command("sql", "DELETE FROM doc");

      // Insert test data
      for (int i = 0; i < 10; i++) {
        database.command("sql", "INSERT INTO doc SET num = " + i);
      }

      // Test from the GitHub issue - using LET variable in LIMIT
      String script = """
          LET $t = 5;
          SELECT FROM doc LIMIT $t;
          """;

      final ResultSet rs = database.command("sqlscript", script);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }
      rs.close();

      // Should return only 5 results
      assertThat(results).hasSize(5);
    });
  }

  @Test
  void skipWithLetVariable() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc2 IF NOT EXISTS");
      database.command("sql", "DELETE FROM doc2");

      // Insert test data
      for (int i = 0; i < 10; i++) {
        database.command("sql", "INSERT INTO doc2 SET num = " + i);
      }

      // Test SKIP with LET variable
      String script = """
          LET $s = 3;
          SELECT FROM doc2 SKIP $s;
          """;

      final ResultSet rs = database.command("sqlscript", script);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }
      rs.close();

      // Should return 10 - 3 = 7 results
      assertThat(results).hasSize(7);
    });
  }

  @Test
  void limitWithNamedParameter() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc3 IF NOT EXISTS");
      database.command("sql", "DELETE FROM doc3");

      // Insert test data
      for (int i = 0; i < 10; i++) {
        database.command("sql", "INSERT INTO doc3 SET num = " + i);
      }

      // Test with named parameter :limit
      Map<String, Object> params = new HashMap<>();
      params.put("limit", 3);

      final ResultSet rs = database.query("sql", "SELECT FROM doc3 LIMIT :limit", params);

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }
      rs.close();

      // Should return only 3 results
      assertThat(results).hasSize(3);
    });
  }

  @Test
  void skipWithNamedParameter() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc4 IF NOT EXISTS");
      database.command("sql", "DELETE FROM doc4");

      // Insert test data
      for (int i = 0; i < 10; i++) {
        database.command("sql", "INSERT INTO doc4 SET num = " + i);
      }

      // Test with named parameter :skip
      Map<String, Object> params = new HashMap<>();
      params.put("skip", 5);

      final ResultSet rs = database.query("sql", "SELECT FROM doc4 SKIP :skip", params);

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }
      rs.close();

      // Should return 10 - 5 = 5 results
      assertThat(results).hasSize(5);
    });
  }

  @Test
  void limitAndSkipWithParameters() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc5 IF NOT EXISTS");
      database.command("sql", "DELETE FROM doc5");

      // Insert test data (0-9)
      for (int i = 0; i < 10; i++) {
        database.command("sql", "INSERT INTO doc5 SET num = " + i);
      }

      // Test with both SKIP and LIMIT parameters
      Map<String, Object> params = new HashMap<>();
      params.put("skip", 2);
      params.put("limit", 5);

      final ResultSet rs = database.query("sql", "SELECT FROM doc5 SKIP :skip LIMIT :limit", params);

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }
      rs.close();

      // Should skip 2 and return 5 results
      assertThat(results).hasSize(5);
    });
  }

  @Test
  void limitAndSkipWithLetVariables() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc6 IF NOT EXISTS");
      database.command("sql", "DELETE FROM doc6");

      // Insert test data
      for (int i = 0; i < 10; i++) {
        database.command("sql", "INSERT INTO doc6 SET num = " + i);
      }

      // Test with both SKIP and LIMIT using LET variables
      String script = """
          LET $skip = 3;
          LET $limit = 4;
          SELECT FROM doc6 SKIP $skip LIMIT $limit;
          """;

      final ResultSet rs = database.command("sqlscript", script);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }
      rs.close();

      // Should skip 3 and return 4 results
      assertThat(results).hasSize(4);
    });
  }
}
