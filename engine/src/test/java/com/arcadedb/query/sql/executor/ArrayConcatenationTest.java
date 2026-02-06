package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test array concatenation expression (||).
 */
class ArrayConcatenationTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/test-databases/ArrayConcatenationTest").create();
    database.getSchema().createDocumentType("V");

    database.transaction(() -> {
      database.newDocument("V")
          .set("tags", List.of("java", "database"))
          .set("categories", List.of("tech", "software"))
          .save();

      database.newDocument("V")
          .set("tags", List.of("graph", "nosql"))
          .set("categories", List.of("data", "storage"))
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
  void arrayConcatTwoArrays() {
    // Concatenate two array fields
    ResultSet result = database.query("sql", "SELECT tags || categories as combined FROM V");

    assertThat(result.hasNext()).isTrue();
    Result row1 = result.next();
    List combined1 = row1.getProperty("combined");
    assertThat(combined1).hasSize(4);
    assertThat(combined1).containsExactly("java", "database", "tech", "software");

    assertThat(result.hasNext()).isTrue();
    Result row2 = result.next();
    List combined2 = row2.getProperty("combined");
    assertThat(combined2).hasSize(4);
    assertThat(combined2).containsExactly("graph", "nosql", "data", "storage");
  }

  @Test
  void arrayConcatArrayAndLiteral() {
    // Concatenate array with literal array
    ResultSet result = database.query("sql", "SELECT tags || ['extra'] as combined FROM V LIMIT 1");

    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    List combined = row.getProperty("combined");
    assertThat(combined).hasSize(3);
    assertThat(combined).containsExactly("java", "database", "extra");
  }

  @Test
  void arrayConcatLiteralArrays() {
    // Concatenate two literal arrays
    ResultSet result = database.query("sql", "SELECT ['a', 'b'] || ['c', 'd'] as combined");

    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    List combined = row.getProperty("combined");
    assertThat(combined).hasSize(4);
    assertThat(combined).containsExactly("a", "b", "c", "d");
  }

  @Test
  void arrayConcatChainedWithoutParentheses() {
    // Issue #3314: chained || without parentheses should work
    ResultSet result = database.query("sql", "SELECT [1,2] || [3,4] || [5,6] as combined");

    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    List combined = row.getProperty("combined");
    assertThat(combined).hasSize(6);
    assertThat(combined).containsExactly(1, 2, 3, 4, 5, 6);
  }

  @Test
  void arrayConcatChainedMultiple() {
    // Chain four arrays with ||
    ResultSet result = database.query("sql", "SELECT [1] || [2] || [3] || [4] as combined");

    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    List combined = row.getProperty("combined");
    assertThat(combined).hasSize(4);
    assertThat(combined).containsExactly(1, 2, 3, 4);
  }

  @Test
  void arrayConcatChainedWithFields() {
    // Chain field || literal || field
    ResultSet result = database.query("sql", "SELECT tags || ['middle'] || categories as combined FROM V LIMIT 1");

    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    List combined = row.getProperty("combined");
    assertThat(combined).hasSize(5);
    assertThat(combined).containsExactly("java", "database", "middle", "tech", "software");
  }
}
