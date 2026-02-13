package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

;

/**
 * Tests for ORDER BY, SKIP, and LIMIT clauses in OpenCypher queries.
 */
public class OpenCypherOrderBySkipLimitTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-orderby").create();

    // Create schema
    database.getSchema().createVertexType("Person");

    // Create test data
    database.transaction(() -> {
      database.newVertex("Person").set("name", "Alice").set("age", 30).save();
      database.newVertex("Person").set("name", "Bob").set("age", 25).save();
      database.newVertex("Person").set("name", "Charlie").set("age", 35).save();
      database.newVertex("Person").set("name", "David").set("age", 28).save();
      database.newVertex("Person").set("name", "Eve").set("age", 32).save();
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
  void orderByAscending() {
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n.name ORDER BY n.age ASC");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    assertThat(results).hasSize(5);

    // Should be ordered by age ascending: Bob(25), David(28), Alice(30), Eve(32), Charlie(35)
    assertThat((String) results.get(0).getProperty("n.name")).isEqualTo("Bob");
    assertThat((String) results.get(1).getProperty("n.name")).isEqualTo("David");
    assertThat((String) results.get(2).getProperty("n.name")).isEqualTo("Alice");
    assertThat((String) results.get(3).getProperty("n.name")).isEqualTo("Eve");
    assertThat((String) results.get(4).getProperty("n.name")).isEqualTo("Charlie");
  }

  @Test
  void orderByDescending() {
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n.name ORDER BY n.age DESC");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    assertThat(results).hasSize(5);

    // Should be ordered by age descending: Charlie(35), Eve(32), Alice(30), David(28), Bob(25)
    assertThat((String) results.get(0).getProperty("n.name")).isEqualTo("Charlie");
    assertThat((String) results.get(1).getProperty("n.name")).isEqualTo("Eve");
    assertThat((String) results.get(2).getProperty("n.name")).isEqualTo("Alice");
    assertThat((String) results.get(3).getProperty("n.name")).isEqualTo("David");
    assertThat((String) results.get(4).getProperty("n.name")).isEqualTo("Bob");
  }

  @Test
  void orderByName() {
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n.name ORDER BY n.name ASC");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    assertThat(results).hasSize(5);

    // Should be ordered alphabetically: Alice, Bob, Charlie, David, Eve
    assertThat((String) results.get(0).getProperty("n.name")).isEqualTo("Alice");
    assertThat((String) results.get(1).getProperty("n.name")).isEqualTo("Bob");
    assertThat((String) results.get(2).getProperty("n.name")).isEqualTo("Charlie");
    assertThat((String) results.get(3).getProperty("n.name")).isEqualTo("David");
    assertThat((String) results.get(4).getProperty("n.name")).isEqualTo("Eve");
  }

  @Test
  void limitOnly() {
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n LIMIT 3");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    // Should return exactly 3 results
    assertThat(results).hasSize(3);

    for (final Result r : results) {
      final Object vertex = r.toElement();
      assertThat(vertex).isInstanceOf(Vertex.class);
    }
  }

  @Test
  void skipOnly() {
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n SKIP 2");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    // Should return 3 results (5 total - 2 skipped)
    assertThat(results).hasSize(3);

    for (final Result r : results) {
      final Object vertex = r.toElement();
      assertThat(vertex).isInstanceOf(Vertex.class);
    }
  }

  @Test
  void skipAndLimit() {
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n SKIP 1 LIMIT 2");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    // Should skip 1 and return 2 results
    assertThat(results).hasSize(2);

    for (final Result r : results) {
      final Object vertex = r.toElement();
      assertThat(vertex).isInstanceOf(Vertex.class);
    }
  }

  @Test
  void orderBySkipLimit() {
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Person) RETURN n.name ORDER BY n.age ASC SKIP 1 LIMIT 3");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    assertThat(results).hasSize(3);

    // Should be ordered by age, skip first (Bob), return next 3: David, Alice, Eve
    assertThat((String) results.get(0).getProperty("n.name")).isEqualTo("David");
    assertThat((String) results.get(1).getProperty("n.name")).isEqualTo("Alice");
    assertThat((String) results.get(2).getProperty("n.name")).isEqualTo("Eve");
  }

  @Test
  void orderByWithWhereFilter() {
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Person) WHERE n.age > 28 RETURN n.name ORDER BY n.age DESC");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    // Should return 3 people older than 28, ordered by age descending
    assertThat(results).hasSize(3);
    assertThat((String) results.get(0).getProperty("n.name")).isEqualTo("Charlie"); // 35
    assertThat((String) results.get(1).getProperty("n.name")).isEqualTo("Eve"); // 32
    assertThat((String) results.get(2).getProperty("n.name")).isEqualTo("Alice"); // 30
  }

  @Test
  void orderByWithNullsAscending() {
    // Issue #3335: In Cypher, null should be sorted to the end in ascending order
    final ResultSet result = database.query("opencypher", "UNWIND [3, 1, null, 2] AS val RETURN val ORDER BY val");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    assertThat(results).hasSize(4);

    // Ascending: non-null values first, then null at the end
    assertThat((Long) results.get(0).getProperty("val")).isEqualTo(1);
    assertThat((Long) results.get(1).getProperty("val")).isEqualTo(2);
    assertThat((Long) results.get(2).getProperty("val")).isEqualTo(3);
    assertThat((Object) results.get(3).getProperty("val")).isNull();
  }

  @Test
  void orderByWithNullsDescending() {
    // Issue #3335: In Cypher, null should be sorted to the beginning in descending order
    final ResultSet result = database.query("opencypher", "UNWIND [3, 1, null, 2] AS val RETURN val ORDER BY val DESC");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    assertThat(results).hasSize(4);

    // Descending: null first, then non-null values descending
    assertThat((Object) results.get(0).getProperty("val")).isNull();
    assertThat((Long) results.get(1).getProperty("val")).isEqualTo(3);
    assertThat((Long) results.get(2).getProperty("val")).isEqualTo(2);
    assertThat((Long) results.get(3).getProperty("val")).isEqualTo(1);
  }

  @Test
  void orderByWithMultipleNulls() {
    // Multiple nulls should be grouped together at the end in ascending order
    final ResultSet result = database.query("opencypher", "UNWIND [null, 3, null, 1, 2] AS val RETURN val ORDER BY val");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    assertThat(results).hasSize(5);

    assertThat((Long) results.get(0).getProperty("val")).isEqualTo(1);
    assertThat((Long) results.get(1).getProperty("val")).isEqualTo(2);
    assertThat((Long) results.get(2).getProperty("val")).isEqualTo(3);
    assertThat((Object) results.get(3).getProperty("val")).isNull();
    assertThat((Object) results.get(4).getProperty("val")).isNull();
  }

  @Test
  void limitLessThanResults() {
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n LIMIT 10");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    // Should return all 5 results (limit is greater than available)
    assertThat(results).hasSize(5);
  }

  @Test
  void skipMoreThanResults() {
    final ResultSet result = database.query("opencypher", "MATCH (n:Person) RETURN n SKIP 10");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    // Should return 0 results (skip is greater than available)
    assertThat(results).hasSize(0);
  }

  @Test
  void orderBySkipLimitWithManyNodes() {
    // Reproduces the Bolt queryWithSkipAndLimit scenario: 10 nodes, SKIP 5, LIMIT 5
    database.getSchema().createVertexType("SkipTest");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newVertex("SkipTest").set("idx", i).save();
      }
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (n:SkipTest) RETURN n.idx AS idx ORDER BY n.idx SKIP 5 LIMIT 5");

    assertThat((Object) result).isNotNull();
    final List<Result> results = collectResults(result);

    assertThat(results).hasSize(5);
    for (int i = 0; i < 5; i++) {
      assertThat(((Number) results.get(i).getProperty("idx")).longValue()).isEqualTo(i + 5);
    }
  }

  private List<Result> collectResults(final ResultSet resultSet) {
    final List<Result> results = new ArrayList<>();
    while (resultSet.hasNext()) {
      results.add(resultSet.next());
    }
    return results;
  }
}
