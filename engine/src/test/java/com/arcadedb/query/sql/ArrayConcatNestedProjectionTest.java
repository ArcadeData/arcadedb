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
      // First test the nested projection alone
      try {
        ResultSet rs = database.command("SQL", "SELECT list({'x':1}):{x}");
        assertThat(rs.hasNext()).isTrue();
        Result record = rs.next();
        String propertyName = record.getPropertyNames().iterator().next();
        Object result = record.getProperty(propertyName);
        System.out.println("Nested projection alone - Property: '" + propertyName + "' = " + result);
      } catch (Exception e) {
        System.out.println("Error with nested projection alone: " + e.getMessage());
        e.printStackTrace();
      }
      
      // Test case from issue: SELECT list({"x":1}):{x} || []
      ResultSet rs = database.command("SQL", "SELECT list({'x':1}):{x} || []");
      
      assertThat(rs.hasNext()).isTrue();
      Result record = rs.next();
      
      System.out.println("All properties: " + record.getPropertyNames());
      for (String propName : record.getPropertyNames()) {
        System.out.println("Property '" + propName + "' = " + record.getProperty(propName));
      }
      
      // Get the first property (should be the only one)
      String propertyName = record.getPropertyNames().iterator().next();
      Object result = record.getProperty(propertyName);
      System.out.println("Result: " + result);
      System.out.println("Result type: " + (result != null ? result.getClass() : "null"));
      
      // Expected: [{"x":1}]
      assertThat(result).isNotNull();
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
