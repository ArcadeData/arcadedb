package com.arcadedb.function.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class SQLDefinedSQLFunctionTest extends TestHelper {
  @Test
  public void testEmbeddedFunction() {
    registerFunctions();
    final Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);
  }

  @Test
  public void testCallFromSQLWithParams() {
    registerFunctions();
    final ResultSet result = database.command("sql", "select `math.sum`(?,?) as result", 3, 5);
    assertThat((Integer) result.next().getProperty("result")).isEqualTo(8);
  }

  @Test
  public void testCallFromSQLNoParams() {
    database.command("sql", "define function math.hello \"select 'hello'\" language sql");
    final ResultSet result = database.command("sql", "select `math.hello`() as result");
    assertThat(result.next().<String>getProperty("result")).isEqualTo("hello");
  }

  @Test
  public void errorTestCallFromSQLEmptyParams() {
    try {
      database.command("sql", "define function math.hello \"select 'hello'\" parameters [] language sql");
      fail("");
    } catch (CommandSQLParsingException e) {
      // EXPECTED
    }
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
      database.command("sql", "define function math.sum \"select :a + :b;\" parameters [a,b] language sql");
      fail("");
    } catch (final IllegalArgumentException e) {
      // EXPECTED
    }

    database.getSchema().getFunctionLibrary("math").unregisterFunction("sum");
    database.command("sql", "define function math.sum \"select :a + :b;\" parameters [a,b] language sql");

    result = (Integer) database.getSchema().getFunction("math", "sum").execute(-350, 150);
    assertThat(result).isEqualTo(-200);
  }

  private void registerFunctions() {
    database.command("sql", "define function math.sum \"select :a + :b;\" parameters [a,b] language sql");
    database.command("sql", "define function util.sum \"select :a + :b;\" parameters [a,b] language sql");

    final FunctionLibraryDefinition flib = database.getSchema().getFunctionLibrary("math");
    assertThat(flib).isNotNull();
  }
}
