package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for FOREACH clause in OpenCypher queries.
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/3328
 */
class OpenCypherForeachTest {
  private Database database;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    final String databasePath = "./target/databases/testopencypher-foreach-" + testInfo.getTestMethod().get().getName();
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void foreachCreateEdges_issue3328() {
    // Exact scenario from issue #3328
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (root:TestNode {name: 'Root'})\n" +
          "FOREACH (i IN [1, 2, 3] |\n" +
          "  CREATE (root)-[:HAS_ITEM]->(:Item {id: i})\n" +
          ")\n" +
          "WITH root\n" +
          "MATCH (root)-[:HAS_ITEM]->(item)\n" +
          "RETURN count(item) AS total");
    });

    // Verify: 3 items should have been created
    final ResultSet verify = database.query("opencypher",
        "MATCH (:TestNode)-[:HAS_ITEM]->(item:Item) RETURN count(item) AS total");
    assertThat(verify.hasNext()).isTrue();
    final Result r = verify.next();
    assertThat(((Number) r.getProperty("total")).longValue()).isEqualTo(3L);
  }

  @Test
  void foreachCreateNodes() {
    // Simple FOREACH creating standalone nodes
    database.transaction(() -> {
      database.command("opencypher",
          "FOREACH (name IN ['Alice', 'Bob', 'Charlie'] |\n" +
          "  CREATE (:Person {name: name})\n" +
          ")");
    });

    final ResultSet verify = database.query("opencypher",
        "MATCH (p:Person) RETURN p.name AS name ORDER BY name");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(3);
  }

  @Test
  void foreachWithMatchContext() {
    // Create initial data
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (:Person {name: 'Bob'})");
    });

    // Use FOREACH inside a query with MATCH context
    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (p:Person)\n" +
          "FOREACH (tag IN ['developer', 'tester'] |\n" +
          "  CREATE (p)-[:HAS_TAG]->(:Tag {value: tag})\n" +
          ")");
    });

    // Each person should have 2 tags = 4 total
    final ResultSet verify = database.query("opencypher",
        "MATCH (:Person)-[:HAS_TAG]->(t:Tag) RETURN count(t) AS total");
    assertThat(verify.hasNext()).isTrue();
    assertThat(((Number) verify.next().getProperty("total")).longValue()).isEqualTo(4L);
  }

  @Test
  void foreachPassesThroughInputRows() {
    // FOREACH should pass through input rows unchanged
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (root:Root {name: 'test'})\n" +
          "FOREACH (i IN [1, 2] |\n" +
          "  CREATE (:Child {id: i})\n" +
          ")\n" +
          "RETURN root.name AS name");

      assertThat(result.hasNext()).isTrue();
      assertThat((String) result.next().getProperty("name")).isEqualTo("test");
    });
  }

  @Test
  void foreachWithSetClause() {
    // Create initial data
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (:Item {id: 1, status: 'pending'})");
      database.command("opencypher",
          "CREATE (:Item {id: 2, status: 'pending'})");
      database.command("opencypher",
          "CREATE (:Item {id: 3, status: 'pending'})");
    });

    // Use FOREACH with SET to update nodes
    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (item:Item)\n" +
          "WITH collect(item) AS items\n" +
          "FOREACH (item IN items |\n" +
          "  SET item.status = 'done'\n" +
          ")");
    });

    // All items should be 'done'
    final ResultSet verify = database.query("opencypher",
        "MATCH (item:Item) WHERE item.status = 'done' RETURN count(item) AS total");
    assertThat(verify.hasNext()).isTrue();
    assertThat(((Number) verify.next().getProperty("total")).longValue()).isEqualTo(3L);
  }
}
