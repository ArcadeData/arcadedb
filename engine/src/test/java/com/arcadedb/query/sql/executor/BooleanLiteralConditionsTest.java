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
 * Test Boolean literal conditions (TRUE, FALSE, NULL) in WHERE clauses.
 */
class BooleanLiteralConditionsTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/test-databases/BooleanLiteralConditionsTest").create();
    final DocumentType type = database.getSchema().createDocumentType("V");

    database.transaction(() -> {
      database.newDocument("V")
          .set("name", "John")
          .set("age", 30)
          .save();

      database.newDocument("V")
          .set("name", "Jane")
          .set("age", 25)
          .save();

      database.newDocument("V")
          .set("name", "Bob")
          .set("age", 35)
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
  void trueCondition() {
    // WHERE TRUE should return all records
    ResultSet result = database.query("sql", "SELECT name FROM V WHERE TRUE");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(3);
  }

  @Test
  void falseCondition() {
    // WHERE FALSE should return no records
    ResultSet result = database.query("sql", "SELECT name FROM V WHERE FALSE");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void nullCondition() {
    // WHERE NULL should return no records (NULL is falsy in boolean context)
    ResultSet result = database.query("sql", "SELECT name FROM V WHERE NULL");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void trueWithAnd() {
    // WHERE TRUE AND condition should be equivalent to just condition
    ResultSet result = database.query("sql", "SELECT name FROM V WHERE TRUE AND age > 25");

    int count = 0;
    while (result.hasNext()) {
      Result row = result.next();
      String name = row.getProperty("name");
      assertThat(name).isIn("John", "Bob");
      count++;
    }

    assertThat(count).isEqualTo(2);
  }

  @Test
  void falseWithOr() {
    // WHERE FALSE OR condition should be equivalent to just condition
    ResultSet result = database.query("sql", "SELECT name FROM V WHERE FALSE OR age = 25");

    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("Jane");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void complexBooleanExpression() {
    // WHERE (TRUE AND age > 30) OR (FALSE AND age < 20)
    // Should simplify to: age > 30
    ResultSet result = database.query("sql", "SELECT name FROM V WHERE (TRUE AND age > 30) OR (FALSE AND age < 20)");

    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("Bob");
    assertThat(result.hasNext()).isFalse();
  }
}
