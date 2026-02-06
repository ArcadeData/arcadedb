package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub Issue #3316: OpenCypher DISTINCT query is not distinct.
 * <p>
 * RETURN DISTINCT returns duplicate results instead of unique ones.
 * count(DISTINCT n) counts all values instead of unique ones.
 */
class Issue3316Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-3316");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Animal");

    database.transaction(() -> {
      database.newVertex("Person").set("name", "Alice").save();
      database.newVertex("Person").set("name", "Bob").save();
      database.newVertex("Person").set("name", "Charlie").save();
      database.newVertex("Animal").set("name", "Dog").save();
      database.newVertex("Animal").set("name", "Cat").save();
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
  void returnDistinctLabels() {
    // Original issue: MATCH (n) RETURN DISTINCT labels(n) returns as many labels as records
    final ResultSet result = database.query("opencypher", "MATCH (n) RETURN DISTINCT labels(n) AS lab");
    final List<Object> labels = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      labels.add(row.getProperty("lab"));
    }
    // Should only return 2 distinct label sets: [Person] and [Animal]
    assertThat(labels).hasSize(2);
  }

  @Test
  void countDistinctWithUnwind() {
    // From comment: UNWIND [1, 1, 2, 3] AS n RETURN count(DISTINCT n) AS unique_count
    // Should return 3, was returning 4
    final ResultSet result = database.query("opencypher",
        "UNWIND [1, 1, 2, 3] AS n RETURN count(DISTINCT n) AS unique_count");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    final Object count = row.getProperty("unique_count");
    assertThat(((Number) count).longValue()).isEqualTo(3L);
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void returnDistinctSimpleValues() {
    // UNWIND with duplicates, RETURN DISTINCT should deduplicate
    final ResultSet result = database.query("opencypher",
        "UNWIND [1, 2, 2, 3, 3, 3] AS n RETURN DISTINCT n");

    final List<Object> values = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      values.add(row.getProperty("n"));
    }
    assertThat(values).hasSize(3);
  }

  @Test
  void returnDistinctWithMatchProperties() {
    // RETURN DISTINCT on vertex properties
    // Add duplicate name to test deduplication
    database.transaction(() -> {
      database.newVertex("Person").set("name", "Alice").save();
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (n:Person) RETURN DISTINCT n.name AS name");

    final List<String> names = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      names.add(row.getProperty("name"));
    }
    // Should be 3 distinct names: Alice, Bob, Charlie (not 4)
    assertThat(names).hasSize(3);
  }

  @Test
  void sumDistinct() {
    // sum(DISTINCT n) should sum only unique values
    final ResultSet result = database.query("opencypher",
        "UNWIND [1, 1, 2, 3] AS n RETURN sum(DISTINCT n) AS total");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    final Object total = row.getProperty("total");
    assertThat(((Number) total).longValue()).isEqualTo(6L);
    assertThat(result.hasNext()).isFalse();
  }
}
