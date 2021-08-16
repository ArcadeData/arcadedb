package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.arcadedb.query.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OSleepStatementExecutionTest extends TestHelper {
  @Test
  public void testBasic() {
    long begin = System.currentTimeMillis();
    ResultSet result = database.command("sql", "sleep 1000");
    Assertions.assertTrue(System.currentTimeMillis() - begin >= 1000);
    printExecutionPlan(null, result);
    Assertions.assertNotNull(result);
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertEquals("sleep", item.getProperty("operation"));
    Assertions.assertFalse(result.hasNext());
  }
}
