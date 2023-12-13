package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdatabase.com)
 */
public class ConsoleStatementExecutionTest extends TestHelper {

  @Test
  public void testError() {
    ResultSet result = database.command("sqlscript", "console.`error` 'foo bar'");
    Assertions.assertNotNull(result);
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals("error", item.getProperty("level"));
    Assertions.assertEquals("foo bar", item.getProperty("message"));
  }

  @Test
  public void testLog() {
    ResultSet result = database.command("sqlscript", "console.log 'foo bar'");
    Assertions.assertNotNull(result);
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertNotNull(item);
    Assertions.assertEquals("log", item.getProperty("level"));
    Assertions.assertEquals("foo bar", item.getProperty("message"));
  }

  @Test
  public void testInvalidLevel() {
    try {
      database.command("sqlscript", "console.bla 'foo bar'");
      Assertions.fail();
    } catch (CommandExecutionException x) {
      // EXPECTED
    } catch (Exception x2) {
      Assertions.fail();
    }
  }
}
