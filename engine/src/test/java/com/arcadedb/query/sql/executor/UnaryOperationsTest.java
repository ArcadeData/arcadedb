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
 * Test unary operations in SQL expressions.
 */
class UnaryOperationsTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/test-databases/UnaryOperationsTest").create();
    final DocumentType type = database.getSchema().createDocumentType("V");
    database.transaction(() -> {
      database.newDocument("V").set("value", 10).set("negative", -5).save();
      database.newDocument("V").set("value", -20).set("negative", -15).save();
      database.newDocument("V").set("value", 0).set("negative", 0).save();
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
  void unaryMinus() {
    // Test unary minus on literal
    ResultSet result = database.command("sql", "SELECT -10 as result");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("result")).isEqualTo(-10);

    // Test unary minus on field
    result = database.command("sql", "SELECT -value as result FROM V WHERE value = 10");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("result")).isEqualTo(-10);

    // Test unary minus on negative field
    result = database.command("sql", "SELECT -value as result FROM V WHERE value = -20");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("result")).isEqualTo(20);

    // Test double unary minus with parentheses (--value would be a comment)
    result = database.command("sql", "SELECT -(-value) as result FROM V WHERE value = 10");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("result")).isEqualTo(10);
  }

  @Test
  void unaryPlus() {
    // Test unary plus on literal
    ResultSet result = database.command("sql", "SELECT +10 as result");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("result")).isEqualTo(10);

    // Test unary plus on field
    result = database.command("sql", "SELECT +value as result FROM V WHERE value = 10");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("result")).isEqualTo(10);

    // Test unary plus on negative field
    result = database.command("sql", "SELECT +value as result FROM V WHERE value = -20");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("result")).isEqualTo(-20);
  }

  @Test
  void unaryInExpression() {
    // Test unary minus in arithmetic expression
    ResultSet result = database.command("sql", "SELECT value + (-value) as result FROM V WHERE value = 10");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("result")).isEqualTo(0);

    // Test unary minus in WHERE clause
    result = database.command("sql", "SELECT value FROM V WHERE -value = -10");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("value")).isEqualTo(10);
  }

  @Test
  void unaryWithZero() {
    ResultSet result = database.command("sql", "SELECT -value as result FROM V WHERE value = 0");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("result")).isEqualTo(0);
  }
}
