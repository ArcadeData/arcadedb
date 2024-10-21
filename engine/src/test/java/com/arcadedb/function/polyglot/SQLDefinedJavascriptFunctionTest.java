package com.arcadedb.function.polyglot;

import com.arcadedb.TestHelper;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class SQLDefinedJavascriptFunctionTest extends TestHelper {
  @Test
  public void testEmbeddedFunction() {
    registerFunctions();
    final Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);
  }

  @Test
  public void testCallFromSQL() {
    registerFunctions();
    final ResultSet result = database.command("sql", "select `math.sum`(?,?) as result", 3, 5);
    assertThat((Integer) result.next().getProperty("result")).isEqualTo(8);
  }

  @Test
  public void testReuseSameQueryEngine() {
    registerFunctions();

    Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);

    result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);

    result = (Integer) database.getSchema().getFunction("util", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);

    result = (Integer) database.getSchema().getFunction("util", "sum").execute(3, 5);
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

  private void registerFunctions() {
    database.command("sql", "define function math.sum \"return a + b\" parameters [a,b] language js");
    database.command("sql", "define function util.sum \"return a + b\" parameters [a,b] language js");

    final FunctionLibraryDefinition flib = database.getSchema().getFunctionLibrary("math");
    assertThat(flib).isNotNull();
  }
}
