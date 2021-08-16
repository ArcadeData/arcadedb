package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OProfileStatementExecutionTest {
  @Test
  public void testProfile() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      db.getSchema().createDocumentType("testProfile");
      db.command("sql", "insert into testProfile set name ='foo'");
      db.command("sql", "insert into testProfile set name ='bar'");

      ResultSet result = db.query("sql", "PROFILE SELECT FROM testProfile WHERE name ='bar'");
      Assertions.assertTrue(result.getExecutionPlan().get().prettyPrint(0, 2).contains("μs"));

      result.close();
    });
  }
}
