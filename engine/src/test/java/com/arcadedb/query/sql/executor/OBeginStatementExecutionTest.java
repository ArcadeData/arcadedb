package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OBeginStatementExecutionTest {
  @Test
  public void testBegin() throws Exception {
    TestHelper.executeInNewDatabase("OCommitStatementExecutionTest", (db) -> {
      Assertions.assertTrue(db.getTransaction() == null || !db.getTransaction().isActive());
      ResultSet result = db.command("sql", "begin");
      //printExecutionPlan(null, result);
      Assertions.assertNotNull(result);
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertEquals("begin", item.getProperty("operation"));
      Assertions.assertFalse(result.hasNext());
      Assertions.assertFalse(db.getTransaction() == null || !db.getTransaction().isActive());
      db.commit();
    });
  }
}
