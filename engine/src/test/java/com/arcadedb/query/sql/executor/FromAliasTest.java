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
 * Test FROM clause alias functionality.
 */
class FromAliasTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/test-databases/FromAliasTest").create();
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
  void simpleFromAlias() {
    // Test basic FROM alias with AS keyword, both unqualified and qualified by the alias (issue #7153)
    ResultSet result = database.query("sql", "SELECT name, age FROM V AS v1 WHERE age > 25");
    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("John");
    assertThat(row.<Integer>getProperty("age")).isEqualTo(30);
    assertThat(result.hasNext()).isFalse();

    result = database.query("sql", "SELECT v1.name AS name, v1.age AS age FROM V AS v1 WHERE v1.age > 25");
    row = result.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("John");
    assertThat(row.<Integer>getProperty("age")).isEqualTo(30);
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void fromAliasWithoutAS() {
    // Test FROM alias without AS keyword (AS is optional in SQL)
    ResultSet result = database.query("sql", "SELECT name, age FROM V v2 WHERE name = 'Jane'");
    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("Jane");
    assertThat(row.<Integer>getProperty("age")).isEqualTo(25);
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void subqueryAlias() {
    // Test subquery with alias
    ResultSet result = database.query("sql", "SELECT name, age FROM (SELECT name, age FROM V WHERE age > 20) AS sub WHERE name = 'John'");
    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("John");
    assertThat(row.<Integer>getProperty("age")).isEqualTo(30);
    assertThat(result.hasNext()).isFalse();
  }
}
