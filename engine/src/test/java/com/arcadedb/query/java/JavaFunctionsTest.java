package com.arcadedb.query.java;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
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
  public void testRegistration() {
    // TEST REGISTRATION HERE
    ((JavaQueryEngine) database.getQueryEngine("java")).registerClass("com.arcadedb.query.java.JavaFunctionsTest$Sum");
    ((JavaQueryEngine) database.getQueryEngine("java")).unregisterClass("com.arcadedb.query.java.JavaFunctionsTest$Sum");
    ((JavaQueryEngine) database.getQueryEngine("java")).registerClass("com.arcadedb.query.java.JavaFunctionsTest$Sum");
    ((JavaQueryEngine) database.getQueryEngine("java")).registerClass("com.arcadedb.query.java.JavaFunctionsTest$Sum");
  }

  @Test
  public void testSecurityError() {
    try {
      database.command("java", "com.arcadedb.query.java.JavaFunctionsTest$Sum::sum", 3, 5);
      Assertions.fail("it shouldn't be allowed to execute a method of a class that has not whitelisted before");
    } catch (CommandExecutionException e) {
      // EXPECTED
      Assertions.assertTrue(e.getCause() instanceof SecurityException);
    }
  }

  @Test
  public void testMethodNotFoundError() {
    try {
      ((JavaQueryEngine) database.getQueryEngine("java")).registerClass("com.arcadedb.query.java.JavaFunctionsTest$Sum");
      database.command("java", "com.arcadedb.query.java.JavaFunctionsTest$Sum::sum", 3, 5, 7);
      Assertions.fail("method does not exist");
    } catch (CommandExecutionException e) {
      // EXPECTED
      Assertions.assertTrue(e.getCause() instanceof NoSuchMethodException);
    }
  }

  @Test
  public void testMethodParameterByPosition() {
    // TEST REGISTRATION HERE
    ((JavaQueryEngine) database.getQueryEngine("java")).registerClass("com.arcadedb.query.java.JavaFunctionsTest$Sum");

    ResultSet result = database.command("java", "com.arcadedb.query.java.JavaFunctionsTest$Sum::sum", 3, 5);
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals(8, (Integer) result.next().getProperty("value"));
  }

  @Test
  public void testStaticMethodParameterByPosition() {
    ((JavaQueryEngine) database.getQueryEngine("java")).registerClass("com.arcadedb.query.java.JavaFunctionsTest$Sum");

    ResultSet result = database.command("java", "com.arcadedb.query.java.JavaFunctionsTest$Sum::SUM", 3, 5);
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals(8, (Integer) result.next().getProperty("value"));
  }
}
