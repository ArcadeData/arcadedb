package com.arcadedb.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Debug test for ORDER BY functionality.
 */
public class OrderByDebugTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/orderby-debug").create();
    database.getSchema().createVertexType("Person");

    database.transaction(() -> {
      database.newVertex("Person").set("name", "Alice").set("age", 30).save();
      database.newVertex("Person").set("name", "Bob").set("age", 25).save();
      database.newVertex("Person").set("name", "Charlie").set("age", 35).save();
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testDebugOrderBy() {
    System.out.println("=== Testing ORDER BY ===");

    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n.name ORDER BY n.age ASC");

    int count = 0;
    while (result.hasNext()) {
      final Result r = result.next();
      final String name = (String) r.getProperty("n.name");
      System.out.println("Result " + count + ": name=" + name);
      count++;
    }

    System.out.println("Total results: " + count);
  }

  @Test
  void testDebugOrderByFullVertex() {
    System.out.println("=== Testing ORDER BY with full vertex ===");

    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n ORDER BY n.age ASC");

    int count = 0;
    while (result.hasNext()) {
      final Result r = result.next();
      final Vertex v = (Vertex) r.getProperty("n");
      System.out.println("Result " + count + ": name=" + v.get("name") + ", age=" + v.get("age"));
      count++;
    }

    System.out.println("Total results: " + count);
  }
}
