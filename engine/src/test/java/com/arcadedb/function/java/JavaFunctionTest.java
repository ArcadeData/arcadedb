package com.arcadedb.function.java;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.*;

public class JavaFunctionTest extends TestHelper {

  public static class Sum {
    public int sum(int a, int b) {
      return a + b;
    }

    public static int SUM(int a, int b) {
      return a + b;
    }
  }

  @Test
  public void testRegistration()
      throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    // TEST REGISTRATION HERE
    registerClass();

    try {
      registerClass();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // EXPECTED
    }

    database.getSchema().unregisterFunctionLibrary("math");
    registerClass();
  }

  @Test
  public void testRegistrationByClassInstance()
      throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    // TEST REGISTRATION HERE
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class));

    try {
      database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class));
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // EXPECTED
    }

    database.getSchema().unregisterFunctionLibrary("math");
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class));
  }

  @Test
  public void testRegistrationSingleMethods()
      throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    // TEST REGISTRATION HERE
    database.getSchema()
        .registerFunctionLibrary(new JavaMethodFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class.getMethod("sum", Integer.TYPE, Integer.TYPE)));

    try {
      database.getSchema()
          .registerFunctionLibrary(new JavaMethodFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class.getMethod("sum", Integer.TYPE, Integer.TYPE)));
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // EXPECTED
    }

    database.getSchema().unregisterFunctionLibrary("math");
    database.getSchema()
        .registerFunctionLibrary(new JavaMethodFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class.getMethod("sum", Integer.TYPE, Integer.TYPE)));
  }

  @Test
  public void testFunctionNotFound() {
    Assertions.assertNull(database.getSchema().getFunction("math", "sum"));
  }

  @Test
  public void testMethodParameterByPosition()
      throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    // TEST REGISTRATION HERE
    registerClass();

    Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    Assertions.assertEquals(8, result);
  }

  @Test
  public void testStaticMethodParameterByPosition()
      throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    registerClass();

    Integer result = (Integer) database.getSchema().getFunction("math", "SUM").execute(3, 5);
    Assertions.assertEquals(8, result);
  }

  @Test
  public void testExecuteFromSQL()
      throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    registerClass();

    database.transaction(() -> {
      ResultSet rs = database.command("SQL", "SELECT `math.sum`(20,7) as sum");
      Assertions.assertTrue(rs.hasNext());
      Result record = rs.next();
      Assertions.assertNotNull(record);
      Assertions.assertFalse(record.getIdentity().isPresent());
      Assertions.assertEquals(27, ((Number) record.getProperty("sum")).intValue());
    });
  }

  private void registerClass() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("math", "com.arcadedb.function.java.JavaFunctionTest$Sum"));
  }
}
