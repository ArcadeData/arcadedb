package com.arcadedb.function.polyglot;

import com.arcadedb.TestHelper;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
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
      database.getSchema().getFunctionLibrary("math")
          .registerFunction(new JavascriptFunctionDefinition("sum", "return a - b;", "a", "b"));
      fail("");
    } catch (final IllegalArgumentException e) {
      // EXPECTED
    }

    database.getSchema().getFunctionLibrary("math").unregisterFunction("sum");
    database.getSchema().getFunctionLibrary("math")
        .registerFunction(new JavascriptFunctionDefinition("sum", "return a - b;", "a", "b"));

    result = (Integer) database.getSchema().getFunction("math", "sum").execute(50, 100);
    assertThat(result).isEqualTo(-50);
  }

  @Test
  public void testNotFound()
      throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException,
      IllegalAccessException {
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

  @Test
  void testJsonObjectAsInput() {

    database.command("sql", """
        DEFINE FUNCTION Test.objectComparison "return a.foo == 'bar'" PARAMETERS [a] LANGUAGE js;
        """);

    FunctionDefinition function = database.getSchema().getFunction("Test", "objectComparison");

    Boolean execute = (Boolean) function.execute("{\"foo\":\"bar\"}");

    assertThat(execute).isTrue();

    ResultSet resultSet = database.query("sql", """
        SELECT `Test.objectComparison`('{"foo":"bar"}') as matchRating;
        """);

    assertThat(resultSet.next().<Boolean>getProperty("matchRating")).isTrue();

  }

  @Test
  void testStringObjectAsInput() {

    database.command("sql", """
        DEFINE FUNCTION Test.lowercase "return a.toLowerCase()" PARAMETERS [a] LANGUAGE js;
        """);

    FunctionDefinition function = database.getSchema().getFunction("Test", "lowercase");

    //enclose in single quote
    String execute = (String) function.execute("'UPPERCASE'");

    assertThat(execute).isEqualTo("uppercase");

    // doubel quotes for SQL parser, then single quotes for JS
    ResultSet resultSet = database.query("sql", """
        SELECT `Test.lowercase`("'UPPERCASE'") as lowercase;
        """);

    assertThat(resultSet.next().<String>getProperty("lowercase")).isEqualTo("uppercase");

  }

  private void registerFunctions() {
    database.getSchema().registerFunctionLibrary(//
        new JavascriptFunctionLibraryDefinition(database, "math")//
            .registerFunction(new JavascriptFunctionDefinition("sum", "return a + b;", "a", "b")));
  }
}
