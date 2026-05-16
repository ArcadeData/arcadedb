package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ExpandParseTest extends TestHelper {

  @Test
  void expandParsing() {
    database.transaction(() -> {
      database.getSchema().createVertexType("TestType");
      database.newVertex("TestType").set("name", "test1").save();
    });

    // Test different function calls in SELECT
    String[] queries = {
        "select 1",
        "select expand(1)",
        "select expand(name)",  // Simple identifier
        "select expand(count(*))",  // Nested function
        "select expand(in())",  // Function with no args
        "select name.in() from TestType",  // Method call
        "select expand(name.in()) from TestType",  // Expand with method
        "select expand(in().include('name')) from TestType"
    };

    for (final String query : queries) {
      try {
        final ResultSet rs = database.query("sql", query);
        //System.out.println("✓ " + query);
        rs.close();
      } catch (final Exception e) {
        //System.out.println("✗ " + query + ": " + e.getMessage().split("\n")[0]);
      }
    }
  }

  // Issue #2965: nested projection on an inline array of maps (without expand) selects the requested fields
  @Test
  void nestedProjectionWithoutExpand() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT [{'x':1,'y':2}]:{x} AS test");
      assertThat(result.hasNext()).isTrue();

      final var record = result.next();
      final Object testValue = record.getProperty("test");

      assertThat(testValue).isInstanceOf(List.class);

      @SuppressWarnings("unchecked")
      final List<Result> list = (List<Result>) testValue;
      assertThat(list).hasSize(1);
      assertThat(list.get(0).getPropertyNames().contains("x")).isTrue();
      assertThat(list.get(0).getPropertyNames().contains("y")).isFalse();
      assertThat((Object) list.get(0).getProperty("x")).isEqualTo(1);
    });
  }

  // Issue #2965: expand() on an inline array of maps without nested projection returns full records
  @Test
  void expandWithoutNestedProjection() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT expand([{'x':1,'y':2}]) AS test");
      assertThat(result.hasNext()).isTrue();

      final var record = result.next();

      assertThat((Object) record.getProperty("x")).isEqualTo(1);
      assertThat((Object) record.getProperty("y")).isEqualTo(2);
    });
  }

  // Issue #2965: expand() followed by nested projection limits the expanded record to the chosen fields
  @Test
  void expandWithNestedProjection() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql", "SELECT expand([{'x':1,'y':2}]):{x}");
      assertThat(result.hasNext()).isTrue();

      final var record = result.next();

      assertThat((Object) record.getProperty("x")).isEqualTo(1);
      assertThat(record.getPropertyNames().contains("y")).isFalse();
    });
  }

  // Issue #2965: expand() with nested projection over multiple maps preserves the per-record projection
  @Test
  void expandWithNestedProjectionMultipleRecords() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT expand([{'x':1,'y':2}, {'x':3,'y':4}, {'x':5,'y':6}]):{x}");

      int count = 0;
      while (result.hasNext()) {
        final var record = result.next();

        assertThat(record.getPropertyNames().contains("x")).isTrue();
        assertThat(record.getPropertyNames().contains("y")).isFalse();
        count++;
      }

      assertThat(count).isEqualTo(3);
    });
  }

  // Issue #2965: expand() with a multi-field nested projection keeps only the listed fields per row
  @Test
  void expandWithComplexNestedProjection() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT expand([{'a':1,'b':2,'c':3}, {'a':4,'b':5,'c':6}]):{a,c}");

      int count = 0;
      while (result.hasNext()) {
        final var record = result.next();

        assertThat(record.getPropertyNames().contains("a")).isTrue();
        assertThat(record.getPropertyNames().contains("c")).isTrue();
        assertThat(record.getPropertyNames().contains("b")).isFalse();
        count++;
      }

      assertThat(count).isEqualTo(2);
    });
  }
}
