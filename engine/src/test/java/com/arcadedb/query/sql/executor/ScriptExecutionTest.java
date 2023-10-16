package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.EmbeddedDatabase;
import com.arcadedb.database.Identifiable;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdatabase.com)
 */
public class ScriptExecutionTest extends TestHelper {
  public class OSQLFunctionThrowCME extends SQLFunctionAbstract {
    public static final String NAME = "throwCME";

    /**
     * Get the date at construction to have the same date for all the iteration.
     */
    public OSQLFunctionThrowCME() {
      super(NAME);
    }

    public Object execute(Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams,
        CommandContext iContext) {
      throw new ConcurrentModificationException("" + iParams[0]);
    }

    public String getSyntax() {
      return "throwCME(message)";
    }

    @Override
    public Object getResult() {
      return null;
    }
  }

  @Test
  public void testTwoInserts() {
    String className = "testTwoInserts";
    database.getSchema().createDocumentType(className);
    database.transaction(() -> {
      database.execute("sqlscript",
          "INSERT INTO " + className + " SET name = 'foo';INSERT INTO " + className + " SET name = 'bar';");
    });
    ResultSet rs = database.query("sql", "SELECT count(*) as count from " + className);
    Assertions.assertEquals((Object) 2L, rs.next().getProperty("count"));
  }

  @Test
  public void testIf() {
    String className = "testIf";
    database.getSchema().createDocumentType(className);
    database.transaction(() -> {
      String script = "";
      script += "INSERT INTO " + className + " SET name = 'foo';";
      script += "LET $1 = SELECT count(*) as count FROM " + className + " WHERE name ='bar';";
      script += "IF($1.size() = 0 OR $1[0].count = 0){";
      script += "   INSERT INTO " + className + " SET name = 'bar';";
      script += "}";
      script += "LET $2 = SELECT count(*) as count FROM " + className + " WHERE name ='bar';";
      script += "IF($2.size() = 0 OR $2[0].count = 0){";
      script += "   INSERT INTO " + className + " SET name = 'bar';";
      script += "}";

      database.execute("sqlscript", script);
    });
    ResultSet rs = database.query("sql", "SELECT count(*) as count from " + className);
    Assertions.assertEquals((Object) 2L, rs.next().getProperty("count"));
  }

  @Test
  public void testReturnInIf() {
    String className = "testReturnInIf";
    database.getSchema().createDocumentType(className);

    database.transaction(() -> {
      String script = "";
      script += "INSERT INTO " + className + " SET name = 'foo';";
      script += "LET $1 = SELECT count(*) as count FROM " + className + " WHERE name ='foo';";
      script += "IF($1.size() = 0 OR $1[0].count = 0){";
      script += "   INSERT INTO " + className + " SET name = 'bar';";
      script += "   RETURN;";
      script += "}";
      script += "INSERT INTO " + className + " SET name = 'baz';";
      database.execute("sqlscript", script);
    });

    final ResultSet rs = database.query("sql", "SELECT count(*) as count from " + className);
    Assertions.assertEquals((Object) 2L, rs.next().getProperty("count"));
  }

  @Test
  public void testReturnInIf2() {
    String className = "testReturnInIf2";
    database.getSchema().createDocumentType(className);

    database.transaction(() -> {
      String script = "";
      script += "INSERT INTO " + className + " SET name = 'foo';";
      script += "LET $1 = SELECT count(*) as count FROM " + className + " WHERE name ='foo';";
      script += "IF($1.size() > 0 ){";
      script += "   RETURN 'OK';";
      script += "}";
      script += "RETURN 'FAIL';";
      ResultSet result = database.execute("sqlscript", script);

      Result item = result.next();

      Assertions.assertEquals("OK", item.getProperty("value"));
      result.close();
    });
  }

  @Test
  public void testReturnInIf3() {
    String className = "testReturnInIf3";
    database.getSchema().createDocumentType(className);

    database.transaction(() -> {
      String script = "";
      script += "INSERT INTO " + className + " SET name = 'foo';";
      script += "LET $1 = SELECT count(*) as count FROM " + className + " WHERE name ='foo';";
      script += "IF($1.size() = 0 ){";
      script += "   RETURN 'FAIL';";
      script += "}";
      script += "RETURN 'OK';";
      ResultSet result = database.execute("sqlscript", script);

      Result item = result.next();

      Assertions.assertEquals("OK", item.getProperty("value"));
      result.close();
    });
  }

  @Test
  public void testLazyExecutionPlanning() {
    database.transaction(() -> {
      String script = "";
      script += "LET $1 = SELECT FROM (select from schema:types) where name = 'nonExistingClass';";
      script += "IF($1.size() > 0) {";
      script += "   SELECT FROM nonExistingClass;";
      script += "   RETURN 'FAIL';";
      script += "}";
      script += "RETURN 'OK';";
      ResultSet result = database.execute("sqlscript", script);

      Result item = result.next();

      Assertions.assertEquals("OK", item.getProperty("value"));
      result.close();
    });
  }

  @Test
  public void testCommitRetry() {
    String className = "testCommitRetry";
    database.getSchema().createDocumentType(className);

    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(defineThrowCME());

    database.transaction(() -> {
      String script = "";
      script += "LET $retries = 0;";
      script += "BEGIN;";
      script += "INSERT INTO " + className + " set attempt = $retries;";
      script += "LET $retries = $retries + 1;";
      script += "IF($retries < 5) {";
      script += "  SELECT throwCME(#-1:-1, 1, 1, 1);";
      script += "}";
      script += "COMMIT RETRY 10;";
      database.execute("sqlscript", script);
    });

    ResultSet result = database.query("sql", "select from " + className);
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertEquals(4, (int) item.getProperty("attempt"));
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCommitRetryMultiThreadsSQLIncrement() throws IOException {
    String className = "testCommitRetryMTSQLIncrement";
    database.getSchema().createDocumentType(className);

    // AVOID RETRY, EXPECTING TO MISS SOME UPDATES
    database.transaction(() -> {
      database.newDocument(className).set("id", 0).save();
    });

    final int TOTAL = 1000;
    for (int i = 0; i < TOTAL; i++) {
      String script = "";
      script += "LET $retries = 0;";
      script += "BEGIN;";
      script += "UPDATE " + className + " set attempt = attempt + 1 WHERE id = 0;";
      script += "LET $retries = $retries + 1;";
      script += "COMMIT;";
      database.async().command("sqlscript", script, null);
    }

    database.async().waitCompletion();

    ResultSet result = database.query("sql", "select from " + className + " where id = 0");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    //Assertions.assertTrue((Integer) item.getProperty("attempt") < TOTAL);
    Assertions.assertFalse(result.hasNext());
    result.close();

    // USE RETRY, EXPECTING NO MISS OF UPDATES
    database.transaction(() -> {
      database.newDocument(className).set("id", 1).save();
    });

    //database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);

    for (int i = 0; i < TOTAL; i++) {
      String script = "";
      script += "LET $retries = 0;";
      script += "BEGIN;";
      script += "UPDATE " + className + " set attempt += 1 WHERE id = 1;";
      script += "LET $retries = $retries + 1;";
      script += "COMMIT RETRY 100;";
      database.async().command("sqlscript", script, null);
    }

    database.async().waitCompletion();

    ImmutablePage page = ((EmbeddedDatabase) database).getPageManager().getImmutablePage(new PageId(2, 0),
        ((PaginatedComponentFile) ((EmbeddedDatabase) database).getFileManager().getFile(2)).getPageSize(), false, false);

    Assertions.assertEquals(TOTAL + 1, page.getVersion(), "Page v." + page.getVersion());
  }

  @Test
  public void testCommitRetryMultiThreadsSQLIncrementRepeatableRead() throws IOException {
    String className = "testCommitRetryMTSQLIncrement";
    database.getSchema().createDocumentType(className);

    // AVOID RETRY, EXPECTING TO MISS SOME UPDATES
    database.transaction(() -> {
      database.newDocument(className).set("id", 0).save();
    });

    final int TOTAL = 10_000;
    for (int i = 0; i < TOTAL; i++) {
      String script = "";
      script += "LET $retries = 0;";
      script += "BEGIN;";
      script += "UPDATE " + className + " set attempt = attempt + 1 WHERE id = 0;";
      script += "LET $retries = $retries + 1;";
      script += "COMMIT;";
      database.async().command("sqlscript", script, null);
    }

    database.async().waitCompletion();

    ResultSet result = database.query("sql", "select from " + className + " where id = 0");
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertTrue((Integer) item.getProperty("attempt") < TOTAL);
    Assertions.assertFalse(result.hasNext());
    result.close();

    // USE RETRY, EXPECTING NO MISS OF UPDATES
    database.transaction(() -> {
      database.newDocument(className).set("id", 1).save();
    });

    database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);

    for (int i = 0; i < TOTAL; i++) {
      String script = "";
      script += "LET $retries = 0;";
      script += "BEGIN;";
      script += "UPDATE " + className + " set attempt += 1 WHERE id = 1;";
      script += "LET $retries = $retries + 1;";
      script += "COMMIT RETRY 100;";
      database.async().command("sqlscript", script, null);
    }

    database.async().waitCompletion();

    ImmutablePage page = ((EmbeddedDatabase) database).getPageManager().getImmutablePage(new PageId(2, 0),
        ((PaginatedComponentFile) ((EmbeddedDatabase) database).getFileManager().getFile(2)).getPageSize(), false, false);

    Assertions.assertEquals(TOTAL + 1, page.getVersion(), "Page v." + page.getVersion());

    result = database.query("sql", "select from " + className + " where id = 1");
    Assertions.assertTrue(result.hasNext());
    item = result.next();
    Assertions.assertEquals(TOTAL, (Integer) item.getProperty("attempt"));
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCommitRetryWithFailure() {
    String className = "testCommitRetryWithFailure";
    database.getSchema().createDocumentType(className);

    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(defineThrowCME());

    database.transaction(() -> {
      String script = "";
      script += "LET $retries = 0;";
      script += "BEGIN;";
      script += "INSERT INTO " + className + " set attempt = $retries;";
      script += "LET $retries = $retries + 1;";
      script += "SELECT throwCME(#-1:-1, 1, 1, 1);";
      script += "COMMIT RETRY 10;";
      try {
        database.execute("sqlscript", script);
      } catch (ConcurrentModificationException x) {
      }

      ResultSet result = database.query("sql", "select from " + className);
      Assertions.assertFalse(result.hasNext());
      result.close();
    });
  }

  @Test
  public void testCommitRetryWithFailureAndContinue() {
    String className = "testCommitRetryWithFailureAndContinue";
    database.getSchema().createDocumentType(className);

    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(defineThrowCME());

    database.transaction(() -> {
      String script = "";
      script += "LET $retries = 0;";
      script += "BEGIN;";
      script += "INSERT INTO " + className + " set attempt = $retries;";
      script += "LET $retries = $retries + 1;";
      script += "SELECT throwCME(#-1:-1, 1, 1, 1);";
      script += "COMMIT RETRY 10 ELSE CONTINUE;";
      script += "INSERT INTO " + className + " set name = 'foo';";

      database.execute("sqlscript", script);

      ResultSet result = database.query("sql", "select from " + className);
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertEquals("foo", item.getProperty("name"));
      Assertions.assertFalse(result.hasNext());
      result.close();
    });
  }

  @Test
  public void testCommitRetryWithFailureScriptAndContinue() {
    String className = "testCommitRetryWithFailureScriptAndContinue";
    database.getSchema().createDocumentType(className);

    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(defineThrowCME());

    database.transaction(() -> {
      String script = "";
      script += "LET $retries = 0;";
      script += "BEGIN;";
      script += "INSERT INTO " + className + " set attempt = $retries;";
      script += "LET $retries = $retries + 1;";
      script += "SELECT throwCME(#-1:-1, 1, 1, 1);";
      script += "COMMIT RETRY 10 ELSE {";
      script += "INSERT INTO " + className + " set name = 'foo';";
      script += "} AND CONTINUE;";

      database.execute("sqlscript", script);
    });

    ResultSet result = database.query("sql", "select from " + className);
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertEquals("foo", item.getProperty("name"));
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCommitRetryWithFailureScriptAndFail() {
    String className = "testCommitRetryWithFailureScriptAndFail";
    database.getSchema().createDocumentType(className);

    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(defineThrowCME());

    database.transaction(() -> {
      String script = "";
      script += "LET $retries = 0;";
      script += "BEGIN;";
      script += "INSERT INTO " + className + " set attempt = $retries;";
      script += "LET $retries = $retries + 1;";
      script += "SELECT throwCME(#-1:-1, 1, 1, 1);";
      script += "COMMIT RETRY 10 ELSE {";
      script += "INSERT INTO " + className + " set name = 'foo';";
      script += "} AND FAIL;";

      try {
        database.execute("sqlscript", script);
        Assertions.fail();
      } catch (ConcurrentModificationException e) {
      }
    });

    ResultSet result = database.query("sql", "select from " + className);
    Assertions.assertTrue(result.hasNext());
    Result item = result.next();
    Assertions.assertEquals("foo", item.getProperty("name"));
    Assertions.assertFalse(result.hasNext());
    result.close();
  }

  private SQLFunction defineThrowCME() {
    return new OSQLFunctionThrowCME();
  }

  @Test
  public void testCommitRetryWithFailureScriptAndFail2() {
    String className = "testCommitRetryWithFailureScriptAndFail2";
    database.getSchema().createDocumentType(className);

    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(defineThrowCME());

    database.transaction(() -> {
      String script = "";
      script += "LET $retries = 0;";
      script += "BEGIN;";
      script += "INSERT INTO " + className + " set attempt = $retries;";
      script += "LET $retries = $retries + 1;";
      script += "SELECT throwCME(#-1:-1, 1, 1, 1);";
      script += "COMMIT RETRY 10 ELSE {";
      script += "INSERT INTO " + className + " set name = 'foo';";
      script += "}";

      try {
        database.execute("sqlscript", script);
        Assertions.fail();
      } catch (ConcurrentModificationException e) {

      }

      ResultSet result = database.query("sql", "select from " + className);
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Assertions.assertEquals("foo", item.getProperty("name"));
      Assertions.assertFalse(result.hasNext());
      result.close();
    });
  }

  @Test
  public void testFunctionAsStatement() {
    database.transaction(() -> {
      String script = "";
      script += "sqrt(64);";

      try {
        database.command("sql", script);
        Assertions.fail();
      } catch (CommandSQLParsingException e) {
      }

      ResultSet rs = database.execute("sqlscript", script);
      Assertions.assertTrue(rs.hasNext());
      Result item = rs.next();
      Assertions.assertEquals(8, (Integer) item.getProperty("result"));
      Assertions.assertFalse(rs.hasNext());

      rs.close();
    });
  }

  @Test
  public void testAssignOnEdgeCreate() {
    database.transaction(() -> {
      String script = "";
      script += "create vertex type V if not exists;\n";
      script += "create edge type E if not exists;\n";
      script += "create edge type IndirectEdge if not exists extends E;\n";

      script += "insert into V set name = 'a', PrimaryName = 'foo1';\n";
      script += "insert into V set name = 'b', PrimaryName = 'foo2';\n";
      script += "insert into V set name = 'c', PrimaryName = 'foo3';\n";
      script += "insert into V set name = 'd', PrimaryName = 'foo4';\n";

      script += "create edge E from (select from V where name = 'a') to (select from V where name = 'b');\n";
      script += "create edge E from (select from V where name = 'c') to (select from V where name = 'd');\n";
      database.execute("sql", script).close();
    });

    String script = "begin;\n";
    script += "LET SourceDataset = SELECT expand(out()) from V where name = 'a';\n";
    script += "LET TarDataset = SELECT expand(out()) from V where name = 'c';\n";
    script += "IF ($SourceDataset[0] != $TarDataset[0])\n";
    script += "{\n";
    script += "CREATE EDGE IndirectEdge FROM $SourceDataset To $TarDataset SET Source = $SourceDataset[0].PrimaryName;\n";
    script += "};\n";
    script += "commit retry 10;\n";

    database.execute("sql", script).close();

    try (ResultSet rs = database.query("sql", "select from IndirectEdge")) {
      Assertions.assertTrue(rs.hasNext());
      Assertions.assertEquals("foo2", rs.next().getProperty("Source"));
      Assertions.assertFalse(rs.hasNext());
    }
  }
}
