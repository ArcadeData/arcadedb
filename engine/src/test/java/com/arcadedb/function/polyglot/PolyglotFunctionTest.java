package com.arcadedb.function.polyglot;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PolyglotFunctionTest extends TestHelper {
  @Test
  public void testEmbeddedFunction() {
    registerFunctions();
    Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    Assertions.assertEquals(8, result);
  }

  @Test
  public void testReuseSameQueryEngine() {
    registerFunctions();

    Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    Assertions.assertEquals(8, result);

    result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    Assertions.assertEquals(8, result);
  }

  @Test
  public void testRedefineFunction() {
    registerFunctions();

    Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(100, 50);
    Assertions.assertEquals(150, result);

    try {
      database.getSchema().getFunctionLibrary("math").registerFunction(new JavascriptFunctionDefinition("sum", "return a - b;", "a", "b"));
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // EXPECTED
    }

    database.getSchema().getFunctionLibrary("math").unregisterFunction("sum");
    database.getSchema().getFunctionLibrary("math").registerFunction(new JavascriptFunctionDefinition("sum", "return a - b;", "a", "b"));

    result = (Integer) database.getSchema().getFunction("math", "sum").execute(50, 100);
    Assertions.assertEquals(-50, result);
  }

  private void registerFunctions() {
    database.getSchema().registerFunctionLibrary(//
        new JavascriptFunctionLibraryDefinition(database, "math")//
            .registerFunction(new JavascriptFunctionDefinition("sum", "return a + b;", "a", "b")));
  }
}
