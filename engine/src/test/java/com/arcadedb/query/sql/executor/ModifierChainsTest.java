package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test modifier chains in SQL expressions (e.g., field[0][1], obj.field.method()).
 */
class ModifierChainsTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/test-databases/ModifierChainsTest").create();
    final DocumentType type = database.getSchema().createDocumentType("V");

    database.transaction(() -> {
      // Create document with nested arrays
      database.newDocument("V")
          .set("matrix", List.of(
              List.of(1, 2, 3),
              List.of(4, 5, 6),
              List.of(7, 8, 9)
          ))
          .set("nested", Map.of(
              "level1", Map.of(
                  "level2", Map.of(
                      "value", 42
                  )
              )
          ))
          .save();

      // Create document with simple array
      database.newDocument("V")
          .set("array", List.of(10, 20, 30, 40, 50))
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
  void doubleArrayAccessor() {
    // Access matrix[0][1] - should get 2
    ResultSet result = database.command("sql", "SELECT matrix[0][1] as value FROM V WHERE matrix IS NOT NULL");
    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    assertThat(row.<Integer>getProperty("value")).isEqualTo(2);

    // Access matrix[1][2] - should get 6
    result = database.command("sql", "SELECT matrix[1][2] as value FROM V WHERE matrix IS NOT NULL");
    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat(row.<Integer>getProperty("value")).isEqualTo(6);

    // Access matrix[2][0] - should get 7
    result = database.command("sql", "SELECT matrix[2][0] as value FROM V WHERE matrix IS NOT NULL");
    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat(row.<Integer>getProperty("value")).isEqualTo(7);
  }

  @Test
  void nestedPropertyAccess() {
    // Access nested.level1.level2.value - should get 42
    ResultSet result = database.command("sql", "SELECT nested.level1.level2.value as value FROM V WHERE nested IS NOT NULL");
    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    assertThat(row.<Integer>getProperty("value")).isEqualTo(42);
  }

  @Test
  void arrayWithMethodChain() {
    // Access array[0] with size() method
    ResultSet result = database.command("sql", "SELECT array.size() as length FROM V WHERE array IS NOT NULL");
    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    assertThat(row.<Integer>getProperty("length")).isEqualTo(5);
  }
}
