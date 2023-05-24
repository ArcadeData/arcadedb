package com.arcadedb.query.java;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

class JavaMethods {
  public JavaMethods() {
  }

  public int sum(final int a, final int b) {
    return a + b;
  }

  public static int SUM(final int a, final int b) {
    return a + b;
  }

  public static void hello() {
    // EMPTY METHOD
  }
}

public class JavaQueryTest extends TestHelper {
  @Test
  public void testRegisteredMethod() {
    Assertions.assertEquals("java", database.getQueryEngine("java").getLanguage());

    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods::sum");

    final ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals(8, (Integer) result.next().getProperty("value"));
  }

  @Test
  public void testRegisteredMethods() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods::sum");
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods::SUM");

    ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals(8, (Integer) result.next().getProperty("value"));

    result = database.command("java", "com.arcadedb.query.java.JavaMethods::SUM", 5, 3);
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals(8, (Integer) result.next().getProperty("value"));

    database.getQueryEngine("java").unregisterFunctions();
  }

  @Test
  public void testRegisteredClass() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");

    ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals(8, (Integer) result.next().getProperty("value"));

    result = database.command("java", "com.arcadedb.query.java.JavaMethods::SUM", 5, 3);
    Assertions.assertTrue(result.hasNext());
    Assertions.assertEquals(8, (Integer) result.next().getProperty("value"));

    database.getQueryEngine("java").unregisterFunctions();
  }

  @Test
  public void testUnRegisteredMethod() {
    try {
      database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
      Assertions.fail();
    } catch (final CommandExecutionException e) {
      // EXPECTED
      Assertions.assertTrue(e.getCause() instanceof SecurityException);
    }
  }

  @Test
  public void testNotExistentMethod() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    try {
      database.command("java", "com.arcadedb.query.java.JavaMethods::totallyInvented", 5, 3);
      Assertions.fail();
    } catch (final CommandExecutionException e) {
      // EXPECTED
      Assertions.assertTrue(e.getCause() instanceof NoSuchMethodException);
    }
  }

  @Test
  public void testAnalyzeQuery() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("java").analyze("com.arcadedb.query.java.JavaMethods::totallyInvented");
    Assertions.assertFalse(analyzed.isDDL());
    Assertions.assertFalse(analyzed.isIdempotent());
  }

  @Test
  public void testUnsupportedMethods() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    try {
      database.query("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
      Assertions.fail();
    } catch (final UnsupportedOperationException e) {
      // EXPECTED
    }

    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    try {
      final HashMap map = new HashMap();
      map.put("name", 1);
      database.getQueryEngine("java").command("com.arcadedb.query.java.JavaMethods::hello", null, map);
      Assertions.fail();
    } catch (final UnsupportedOperationException e) {
      // EXPECTED
    }

    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    try {
      database.getQueryEngine("java").query("com.arcadedb.query.java.JavaMethods::sum", null, new HashMap<>());
      Assertions.fail();
    } catch (final UnsupportedOperationException e) {
      // EXPECTED
    }
  }
}
