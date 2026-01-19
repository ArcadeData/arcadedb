package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class InsertReturnTest extends TestHelper {

  @Test
  void testInsertReturnThis() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TestDoc");

      ResultSet rs = database.command("sql", "INSERT INTO TestDoc SET name = 'test1' RETURN @this");
      System.out.println("Has next: " + rs.hasNext());

      assertThat(rs.hasNext()).as("INSERT...RETURN should return results").isTrue();

      var result = rs.next();
      assertThat(result).isNotNull();
      System.out.println("Result: " + result);
      System.out.println("Result class: " + result.getClass().getName());
      System.out.println("Properties: " + result.getPropertyNames());
      System.out.println("Identity: " + result.getIdentity());
      System.out.println("Element: " + result.getElement());
      if (result.getElement().isPresent()) {
        var doc = result.getElement().get();
        System.out.println("Element name property: " + doc.get("name"));
      }
      assertThat((String)result.getProperty("name")).isEqualTo("test1");

      rs.close();
    });
  }
}
