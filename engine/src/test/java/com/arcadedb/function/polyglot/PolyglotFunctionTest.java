package com.arcadedb.function.polyglot;

import com.arcadedb.TestHelper;
import com.arcadedb.function.FunctionExecutionException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class PolyglotFunctionTest extends TestHelper {
  @Test
  public void testEmbeddedFunction() {
    registerFunctions();
    final Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);
  }

  @Test
  public void testReuseSameQueryEngine() {
    registerFunctions();

    Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);

    result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);
  }

  @Test
  public void testRedefineFunction() {
    registerFunctions();

    Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(100, 50);
    assertThat(result).isEqualTo(150);

    try {
      database.getSchema().getFunctionLibrary("math").registerFunction(new JavascriptFunctionDefinition("sum", "return a - b;", "a", "b"));
      fail("");
    } catch (final IllegalArgumentException e) {
      // EXPECTED
    }

    database.getSchema().getFunctionLibrary("math").unregisterFunction("sum");
    database.getSchema().getFunctionLibrary("math").registerFunction(new JavascriptFunctionDefinition("sum", "return a - b;", "a", "b"));

    result = (Integer) database.getSchema().getFunction("math", "sum").execute(50, 100);
    assertThat(result).isEqualTo(-50);
  }

  @Test
  public void testNotFound() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    registerFunctions();
    try {
      database.getSchema().getFunction("math", "NOT_found").execute(3, 5);
      fail("");
    } catch (IllegalArgumentException e) {
      // EXPECTED
    }
  }

  @Test
  public void testExecutionError() {
    try {
      database.getSchema().registerFunctionLibrary(//
          new JavascriptFunctionLibraryDefinition(database, "math")//
              .registerFunction(new JavascriptFunctionDefinition("sum", "return a ++++ b;", "a", "b")));

      database.getSchema().getFunction("math", "sum").execute("invalid", 5);
      fail("");
    } catch (FunctionExecutionException e) {
      // EXPECTED
    }
  }

  private void registerFunctions() {
    database.getSchema().registerFunctionLibrary(//
        new JavascriptFunctionLibraryDefinition(database, "math")//
            .registerFunction(new JavascriptFunctionDefinition("sum", "return a + b;", "a", "b")));
  }
}
