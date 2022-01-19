package com.arcadedb.query.java;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JavaFunctionsTest extends TestHelper {

  public static class Sum {
    public int sum(int a, int b) {
      return a + b;
    }

    public static int SUM(int a, int b) {
      return a + b;
    }
  }

  @Test
  public void testMethodParameterByPosition() {
    ResultSet result = database.command("java", "com.arcadedb.query.java.JavaFunctionsTest$Sum::sum", 3, 5);
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals(8, (Integer) result.next().getProperty("value"));
  }

  @Test
  public void testStaticMethodParameterByPosition() {
    ResultSet result = database.command("java", "com.arcadedb.query.java.JavaFunctionsTest$Sum::SUM", 3, 5);
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals(8, (Integer) result.next().getProperty("value"));
  }
}
