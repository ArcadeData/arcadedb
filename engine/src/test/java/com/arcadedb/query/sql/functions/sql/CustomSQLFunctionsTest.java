package com.arcadedb.query.sql.functions.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CustomSQLFunctionsTest {

  @Test
  public void testRandom() throws Exception {
    TestHelper.executeInNewDatabase("testRandom", (db) -> {
      ResultSet result = db.query("sql", "select math_random() as random");
      Assertions.assertTrue((Double) result.next().getProperty("random") > 0);
    });
  }

  @Test
  public void testLog10() throws Exception {
    TestHelper.executeInNewDatabase("testRandom", (db) -> {
      ResultSet result = db.query("sql", "select math_log10(10000) as log10");
      Assertions.assertEquals(result.next().getProperty("log10"), 4.0, 0.0001);
    });
  }

  @Test
  public void testAbsInt() throws Exception {
    TestHelper.executeInNewDatabase("testRandom", (db) -> {
      ResultSet result = db.query("sql", "select math_abs(-5) as abs");
      Assertions.assertTrue((Integer) result.next().getProperty("abs") == 5);
    });
  }

  @Test
  public void testAbsDouble() throws Exception {
    TestHelper.executeInNewDatabase("testRandom", (db) -> {
      ResultSet result = db.query("sql", "select math_abs(-5.0d) as abs");
      Assertions.assertTrue((Double) result.next().getProperty("abs") == 5.0);
    });
  }

  @Test
  public void testAbsFloat() throws Exception {
    TestHelper.executeInNewDatabase("testRandom", (db) -> {
      ResultSet result = db.query("sql", "select math_abs(-5.0f) as abs");
      Assertions.assertTrue((Float) result.next().getProperty("abs") == 5.0);
    });
  }

  @Test
  public void testNonExistingFunction() throws Exception {
    TestHelper.executeInNewDatabase("testRandom", (db) -> {
      try {
        ResultSet result = db.query("sql", "select math_min('boom', 'boom') as boom");
        result.next();
        Assertions.fail("Expected QueryParsingException");
      } catch (QueryParsingException e) {
      }
    });
  }
}
