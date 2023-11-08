package com.arcadedb.function.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.ParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SQLDefinedSQLFunctionTest extends TestHelper {
  @Test
  public void testEmbeddedFunction() {
    registerFunctions();
    final Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    Assertions.assertEquals(8, result);
  }

  @Test
  public void testCallFromSQLWithParams() {
    registerFunctions();
    final ResultSet result = database.command("sql", "select `math.sum`(?,?) as result", 3, 5);
    Assertions.assertEquals(8, (Integer) result.next().getProperty("result"));
  }

  @Test
  public void testCallFromSQLNoParams() {
    database.command("sql", "define function math.hello \"select 'hello'\" language sql");
    final ResultSet result = database.command("sql", "select `math.hello`() as result");
    Assertions.assertEquals("hello", result.next().getProperty("result"));
  }

  @Test
  public void errorTestCallFromSQLEmptyParams() {
    try {
      database.command("sql", "define function math.hello \"select 'hello'\" parameters [] language sql");
      Assertions.fail();
    } catch (CommandSQLParsingException e) {
      // EXPECTED
    }
  }

  @Test
  public void testReuseSameQueryEngine() {
    registerFunctions();

    Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    Assertions.assertEquals(8, result);

    result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    Assertions.assertEquals(8, result);

    result = (Integer) database.getSchema().getFunction("util", "sum").execute(3, 5);
    Assertions.assertEquals(8, result);

    result = (Integer) database.getSchema().getFunction("util", "sum").execute(3, 5);
    Assertions.assertEquals(8, result);
  }

  @Test
  public void testRedefineFunction() {
    registerFunctions();

    Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(100, 50);
    Assertions.assertEquals(150, result);

    try {
      database.command("sql", "define function math.sum \"select :a + :b;\" parameters [a,b] language sql");
      Assertions.fail();
    } catch (final IllegalArgumentException e) {
      // EXPECTED
    }

    database.getSchema().getFunctionLibrary("math").unregisterFunction("sum");
    database.command("sql", "define function math.sum \"select :a + :b;\" parameters [a,b] language sql");

    result = (Integer) database.getSchema().getFunction("math", "sum").execute(-350, 150);
    Assertions.assertEquals(-200, result);
  }

  private void registerFunctions() {
    database.command("sql", "define function math.sum \"select :a + :b;\" parameters [a,b] language sql");
    database.command("sql", "define function util.sum \"select :a + :b;\" parameters [a,b] language sql");

    final FunctionLibraryDefinition flib = database.getSchema().getFunctionLibrary("math");
    Assertions.assertNotNull(flib);
  }
}
