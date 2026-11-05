package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ArrayConcatNestedProjectionTest extends TestHelper {

  @Test
  void arrayConcatWithNestedProjection() {
    database.transaction(() -> {
      // Test case from issue: SELECT list({"x":1}):{x} || []
      // This tests array concatenation with nested projection on an aggregate function. The map carries a key the
      // projection does NOT keep, or the filtered and the unfiltered answer would be the same map (issue #11306)
      final ResultSet rs = database.command("SQL", "SELECT list({'x':1,'y':2}):{x} || []");

      assertThat(rs.hasNext()).isTrue();
      final Result record = rs.next();

      // Get the result from the query
      final String propertyName = record.getPropertyNames().iterator().next();
      final Object result = record.getProperty(propertyName);

      // Expected: [{"x":1}]
      assertThat(result).isNotNull();
      assertThat(result).isInstanceOf(List.class);

      final List<?> resultList = (List<?>) result;
      assertThat(resultList).hasSize(1);

      // projected like the same aggregate without || projects it: into a Result holding only the kept key
      final Object firstElement = resultList.get(0);
      assertThat(firstElement).isInstanceOf(Result.class);
      assertThat(((Result) firstElement).toMap()).isEqualTo(Map.of("x", 1));
    });
  }
}
