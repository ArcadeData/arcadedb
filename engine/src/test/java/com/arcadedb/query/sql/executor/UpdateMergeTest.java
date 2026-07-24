package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
  void updateMergeWithNamedParameterMap() {
    // The JSON payload is supplied as a named parameter instead of being inlined in the query
    final Map<String, Object> payload = new HashMap<>();
    payload.put("status", "active");
    payload.put("email", "john@example.com");

    database.transaction(() -> {
      final ResultSet result = database.command("sql", "UPDATE V MERGE :payload WHERE name = 'John'", Map.of("payload", payload));
      assertThat(result.hasNext()).isTrue();
    });

    final ResultSet result = database.query("sql", "SELECT name, age, city, status, email FROM V WHERE name = 'John'");
    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("John");
    assertThat(row.<Integer>getProperty("age")).isEqualTo(30);
    assertThat(row.<String>getProperty("city")).isEqualTo("New York");
    assertThat(row.<String>getProperty("status")).isEqualTo("active");
    assertThat(row.<String>getProperty("email")).isEqualTo("john@example.com");
  }

  @Test
  void updateMergeWithPositionalParameterMap() {
    final Map<String, Object> payload = new HashMap<>();
    payload.put("status", "archived");

    database.transaction(() -> {
      final ResultSet result = database.command("sql", "UPDATE V MERGE ? WHERE name = 'Jane'", (Object) payload);
      assertThat(result.hasNext()).isTrue();
    });

    final ResultSet result = database.query("sql", "SELECT name, age, status FROM V WHERE name = 'Jane'");
    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<Integer>getProperty("age")).isEqualTo(25);
    assertThat(row.<String>getProperty("status")).isEqualTo("archived");
  }

  @Test
  void updateMergeWithParameterAndUpsertInsertsNewRecord() {
    final Map<String, Object> payload = new HashMap<>();
    payload.put("name_norm", "Aspirin");
    payload.put("grounded", true);

    database.getSchema().getType("V").createProperty("canonical_id", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "canonical_id");

    database.transaction(() -> {
      final ResultSet result = database.command("sql", "UPDATE V MERGE :payload UPSERT WHERE canonical_id = :id",
          Map.of("payload", payload, "id", "LOCAL_ENTITY:12345"));
      assertThat(result.hasNext()).isTrue();
    });

    final ResultSet result = database.query("sql", "SELECT canonical_id, name_norm, grounded FROM V WHERE canonical_id = 'LOCAL_ENTITY:12345'");
    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("canonical_id")).isEqualTo("LOCAL_ENTITY:12345");
    assertThat(row.<String>getProperty("name_norm")).isEqualTo("Aspirin");
    assertThat(row.<Boolean>getProperty("grounded")).isTrue();
  }

  @Test
  void updateMergeWithDocumentParameterDoesNotLeakMetadata() {
    database.transaction(() -> {
      final Document source = (Document) database.query("sql", "SELECT FROM V WHERE name = 'John'").next().getElement().get();

      final ResultSet result = database.command("sql", "UPDATE V MERGE :payload WHERE name = 'Jane'", Map.of("payload", source));
      assertThat(result.hasNext()).isTrue();
    });

    final ResultSet result = database.query("sql", "SELECT FROM V WHERE city = 'New York' AND age = 30");
    // both John and the merged Jane now match
    assertThat(result.stream().count()).isEqualTo(2);

    final Result jane = database.query("sql", "SELECT FROM V WHERE city = 'New York'").next();
    assertThat(jane.getPropertyNames()).doesNotContain("@rid", "@type", "@cat");
  }

  @Test
  void updateMergeWithSubQueryDoesNotLeakMetadata() {
    database.transaction(() -> {
      final ResultSet result = database.command("sql",
          "UPDATE V MERGE (SELECT city, age FROM V WHERE name = 'John') WHERE name = 'Jane'");
      assertThat(result.hasNext()).isTrue();
    });

    final ResultSet result = database.query("sql", "SELECT FROM V WHERE name = 'Jane'");
    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("city")).isEqualTo("New York");
    assertThat(row.<Integer>getProperty("age")).isEqualTo(30);
    assertThat(row.getPropertyNames()).doesNotContain("@rid", "@type", "@cat");
  }

  @Test
  void updateMergeWithNonStringKeyMapFails() {
    final Map<Object, Object> payload = new HashMap<>();
    payload.put(42, "answer");

    database.transaction(() -> {
      assertThatThrownBy(() -> database.command("sql", "UPDATE V MERGE :payload WHERE name = 'John'", Map.of("payload", payload)))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("string keys");
    });
  }

  @Test
  void updateMergeWithNonMapParameterFails() {
    database.transaction(() -> {
      assertThatThrownBy(() -> database.command("sql", "UPDATE V MERGE :payload WHERE name = 'John'", Map.of("payload", "not-a-map")))
          .isInstanceOf(CommandExecutionException.class);
    });
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
