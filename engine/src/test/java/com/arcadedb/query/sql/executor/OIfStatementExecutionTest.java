package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OIfStatementExecutionTest extends TestHelper {

  @Test
  public void testPositive() {
    ResultSet results = database.command("sql", "if(1=1){ select 1 as a; }");
    Assertions.assertTrue(results.hasNext());
    Result result = results.next();
    assertThat((Integer) result.getProperty("a")).isEqualTo(1);
    Assertions.assertFalse(results.hasNext());
    results.close();
  }

  @Test
  public void testNegative() {
    ResultSet results = database.command("sql", "if(1=2){ select 1 as a; }");
    Assertions.assertFalse(results.hasNext());
    results.close();
  }

  @Test
  public void testIfReturn() {
    ResultSet results = database.command("sql", "if(1=1){ return 'yes'; }");
    Assertions.assertTrue(results.hasNext());
    Assertions.assertEquals("yes", results.next().getProperty("value"));
    Assertions.assertFalse(results.hasNext());
    results.close();
  }
}
