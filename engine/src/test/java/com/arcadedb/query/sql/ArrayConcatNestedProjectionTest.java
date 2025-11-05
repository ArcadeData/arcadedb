package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class ArrayConcatNestedProjectionTest extends TestHelper {

  @Test
  public void testArrayConcatWithNestedProjection() {
    database.transaction(() -> {
      // Test case from issue: SELECT list({"x":1}):{x} || []
      final ResultSet rs = database.command("SQL", "SELECT list({'x':1}):{x} || []");
      
      assertThat(rs.hasNext()).isTrue();
      final Result record = rs.next();
      
      // Expected: [{"x":1}]
      final Object result = record.getProperty("list({'x':1}):{x} || []");
      System.out.println("Result: " + result);
      System.out.println("Result type: " + (result != null ? result.getClass() : "null"));
      
      assertThat(result).isInstanceOf(List.class);
      final List<?> resultList = (List<?>) result;
      assertThat(resultList).hasSize(1);
      
      final Object firstElement = resultList.get(0);
      assertThat(firstElement).isInstanceOf(Map.class);
      
      final Map<?, ?> map = (Map<?, ?>) firstElement;
      assertThat(map.get("x")).isEqualTo(1);
    });
  }
}
