package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

public class ExpandParseTest extends TestHelper {

  @Test
  void testExpandParsing() {
    database.transaction(() -> {
      database.getSchema().createVertexType("TestType");
      database.newVertex("TestType").set("name", "test1").save();
    });

    // Test different function calls in SELECT
    String[] queries = {
        "select 1",
        "select expand(1)",
        "select expand(name)",  // Simple identifier
        "select expand(count(*))",  // Nested function
        "select expand(in())",  // Function with no args
        "select name.in() from TestType",  // Method call
        "select expand(name.in()) from TestType",  // Expand with method
        "select expand(in().include('name')) from TestType"
    };

    for (final String query : queries) {
      try {
        final ResultSet rs = database.query("sql", query);
        System.out.println("✓ " + query);
        rs.close();
      } catch (final Exception e) {
        System.out.println("✗ " + query + ": " + e.getMessage().split("\n")[0]);
      }
    }
  }
}
