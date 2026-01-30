package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test UPDATE MERGE and UPDATE CONTENT operations.
 */
class UpdateMergeTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/test-databases/UpdateMergeTest").create();
    final DocumentType type = database.getSchema().createDocumentType("V");

    database.transaction(() -> {
      // Create document with existing fields
      database.newDocument("V")
          .set("name", "John")
          .set("age", 30)
          .set("city", "New York")
          .save();

      // Create another document
      database.newDocument("V")
          .set("name", "Jane")
          .set("age", 25)
          .save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
    TestHelper.checkActiveDatabases();
  }

  @Test
  void updateMerge() {
    // UPDATE MERGE should merge new fields with existing ones
    database.transaction(() -> {
      ResultSet result = database.command("sql", "UPDATE V MERGE { \"status\": \"active\", \"email\": \"john@example.com\" } WHERE name = 'John'");
      assertThat(result.hasNext()).isTrue();
    });

    // Verify the update - original fields should still exist, plus new ones
    ResultSet result = database.query("sql", "SELECT name, age, city, status, email FROM V WHERE name = 'John'");
    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("John");
    assertThat(row.<Integer>getProperty("age")).isEqualTo(30);
    assertThat(row.<String>getProperty("city")).isEqualTo("New York");
    assertThat(row.<String>getProperty("status")).isEqualTo("active");
    assertThat(row.<String>getProperty("email")).isEqualTo("john@example.com");
  }

  @Test
  void updateContent() {
    // UPDATE CONTENT should replace entire document content
    database.transaction(() -> {
      ResultSet result = database.command("sql", "UPDATE V CONTENT { \"name\": \"Replaced\", \"status\": \"inactive\" } WHERE name = 'Jane'");
      assertThat(result.hasNext()).isTrue();
    });

    // Verify the update - old fields should be gone
    ResultSet result = database.query("sql", "SELECT name, status, age FROM V WHERE name = 'Replaced'");
    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("Replaced");
    assertThat(row.<String>getProperty("status")).isEqualTo("inactive");
    // Old field 'age' should not exist
    assertThat(row.<Integer>getProperty("age")).isNull();
  }
}
