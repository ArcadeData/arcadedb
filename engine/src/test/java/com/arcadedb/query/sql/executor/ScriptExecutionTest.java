package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.Identifiable;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdatabase.com)
 */
public class ScriptExecutionTest extends TestHelper {
  public static class SQLFunctionThrowCME extends SQLFunctionAbstract {
    public static final String NAME = "throwCME";

    /**
     * Get the date at construction to have the same date for all the iteration.
     */
    public SQLFunctionThrowCME() {
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
      database.command("sqlscript",
          "INSERT INTO " + className + " SET name = 'foo';INSERT INTO " + className + " SET name = 'bar';");
    });
    ResultSet rs = database.query("sql", "SELECT count(*) as count from " + className);
    assertThat(rs.next().<Long>getProperty("count")).isEqualTo((Object) 2L);
  }

  @Test
  public void testIf() {
    String className = "testIf";
    database.getSchema().createDocumentType(className);
    database.transaction(() -> {
      String script = """
              INSERT INTO %s SET name = 'foo';
              LET $1 = SELECT count(*) as count FROM %s WHERE name ='bar';
              IF($1.size() = 0 OR $1[0].count = 0){
                  INSERT INTO %s SET name = 'bar';
              }
              LET $2 = SELECT count(*) as count FROM %s WHERE name ='bar';
              IF($2.size() = 0 OR $2[0].count = 0){
                  INSERT INTO %s SET name = 'bar';
              }
          """.formatted(className, className, className, className, className);
      database.command("sqlscript", script);
    });
    ResultSet rs = database.query("sql", "SELECT count(*) as count from " + className);
    assertThat(rs.next().<Long>getProperty("count")).isEqualTo(2L);
  }

  @Test
  public void testReturnInIf() {
    String className = "testReturnInIf";
    database.getSchema().createDocumentType(className);

    database.transaction(() -> {
      String script = """
              INSERT INTO %s SET name = 'foo';
              LET $1 = SELECT count(*) as count FROM %s WHERE name ='foo';
              IF($1.size() = 0 OR $1[0].count = 0){
                  INSERT INTO %s SET name = 'bar';
                  RETURN;
              }
              INSERT INTO %s SET name = 'baz';
          """.formatted(className, className, className, className);
      database.command("sqlscript", script);
    });

    final ResultSet rs = database.query("sql", "SELECT count(*) as count from " + className);
    assertThat(rs.next().<Long>getProperty("count")).isEqualTo(2L);
  }

  @Test
  public void testReturnInIf2() {
    String className = "testReturnInIf2";
    database.getSchema().createDocumentType(className);

    database.transaction(() -> {
      String script = """
              INSERT INTO %s SET name = 'foo';
              LET $1 = SELECT count(*) as count FROM %s WHERE name ='foo';
              IF($1.size() > 0 ){
                  RETURN 'OK';
              }
              RETURN 'FAIL';
          """.formatted(className, className);
      ResultSet result = database.command("sqlscript", script);

      Result item = result.next();

      assertThat(item.<String>getProperty("value")).isEqualTo("OK");
      result.close();
    });
  }

  @Test
  public void testReturnInIf3() {
    String className = "testReturnInIf3";
    database.getSchema().createDocumentType(className);

    database.transaction(() -> {
      String script = """
              INSERT INTO %s SET name = 'foo';
              LET $1 = SELECT count(*) as count FROM %s WHERE name ='foo';
              IF($1.size() = 0 ){
                  RETURN 'FAIL';
              }
              RETURN 'OK';
          """.formatted(className, className);
      ResultSet result = database.command("sqlscript", script);

      Result item = result.next();

      assertThat(item.<String>getProperty("value")).isEqualTo("OK");
      result.close();
    });
  }

  @Test
  public void testLazyExecutionPlanning() {
    database.transaction(() -> {
      String script = """
              LET $1 = SELECT FROM (select from schema:types) where name = 'nonExistingClass';
              IF($1.size() > 0) {
                  SELECT FROM nonExistingClass;
                  RETURN 'FAIL';
              }
              RETURN 'OK';
          """;
      ResultSet result = database.command("sqlscript", script);

      Result item = result.next();

      assertThat(item.<String>getProperty("value")).isEqualTo("OK");
      result.close();
    });
  }

  @Test
  public void testCommitRetry() {
    String className = "testCommitRetry";
    database.getSchema().createDocumentType(className);

    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(defineThrowCME());

    database.transaction(() -> {
      String script = """
              LET $retries = 0;
              BEGIN;
              INSERT INTO %s set attempt = $retries;
              LET $retries = $retries + 1;
              IF($retries < 5) {
                  SELECT throwCME(#-1:-1, 1, 1, 1);
              }
              COMMIT RETRY 10;
          """.formatted(className);
      database.command("sqlscript", script);
    });

    ResultSet result = database.query("sql", "select from " + className);
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat((int) item.getProperty("attempt")).isEqualTo(4);
    assertThat(result.hasNext()).isFalse();
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
    String script = """
            LET $retries = 0;
            BEGIN;
            UPDATE %s set attempt = attempt + 1 WHERE id = 0;
            LET $retries = $retries + 1;
            COMMIT;
        """.formatted(className);
    for (int i = 0; i < TOTAL; i++) {
      database.async().command("sqlscript", script, null);
    }

    database.async().waitCompletion();

    ResultSet result = database.query("sql", "select from " + className + " where id = 0");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    //Assertions.assertThat((Integer).isTrue() item.getProperty("attempt") < TOTAL);
    assertThat(result.hasNext()).isFalse();
    result.close();

    // USE RETRY, EXPECTING NO MISS OF UPDATES
    database.transaction(() -> {
      database.newDocument(className).set("id", 1).save();
    });

    //database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);

    script = """
            LET $retries = 0;
            BEGIN;
            UPDATE %s set attempt += 1 WHERE id = 1;
            LET $retries = $retries + 1;
            COMMIT RETRY 100;
        """.formatted(className);
    for (int i = 0; i < TOTAL; i++) {
      database.async().command("sqlscript", script, null);
    }

    database.async().waitCompletion();

    ImmutablePage page = ((LocalDatabase) database).getPageManager().getImmutablePage(new PageId(2, 0),
        ((PaginatedComponentFile) ((LocalDatabase) database).getFileManager().getFile(2)).getPageSize(), false, false);

    assertThat(page.getVersion()).as("Page v." + page.getVersion()).isEqualTo(TOTAL + 1);
  }

  @Test
  public void testCommitRetryMultiThreadsSQLIncrementRepeatableRead() throws IOException {
    String className = "testCommitRetryMTSQLIncrement";
    database.getSchema().createDocumentType(className);

    // AVOID RETRY, EXPECTING TO MISS SOME UPDATES
    database.transaction(() -> {
      database.newDocument(className).set("id", 0).save();
    });

    String script = """
        LET $retries = 0;
        BEGIN;
        UPDATE %s set attempt = attempt + 1 WHERE id = 0;
        LET $retries = $retries + 1;
        COMMIT;
        """.formatted(className);
    ;
    final int TOTAL = 10_000;
    for (int i = 0; i < TOTAL; i++) {
      database.async().command("sqlscript", script, null);
    }

    database.async().waitCompletion();

    ResultSet result = database.query("sql", "select from " + className + " where id = 0");
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item.<Integer>getProperty("attempt")).as("Found attempts = " + item.getProperty("attempt")).isLessThan(TOTAL);
    assertThat(result.hasNext()).isFalse();
    result.close();

    // USE RETRY, EXPECTING NO MISS OF UPDATES
    database.transaction(() -> {
      database.newDocument(className).set("id", 1).save();
    });

    database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);

    script = """
        LET $retries = 0;
        BEGIN;
        UPDATE %s set attempt += 1 WHERE id = 1;
        LET $retries = $retries + 1;
        COMMIT RETRY 100;
        """.formatted(className);
    ;
    for (int i = 0; i < TOTAL; i++) {
      database.async().command("sqlscript", script, null);
    }

    database.async().waitCompletion();

    ImmutablePage page = ((LocalDatabase) database).getPageManager().getImmutablePage(new PageId(2, 0),
        ((PaginatedComponentFile) ((LocalDatabase) database).getFileManager().getFile(2)).getPageSize(), false, false);

    assertThat(page.getVersion()).as("Page v." + page.getVersion()).isEqualTo(TOTAL + 1);

    result = database.query("sql", "select from " + className + " where id = 1");
    assertThat(result.hasNext()).isTrue();
    item = result.next();
    assertThat(item.<Integer>getProperty("attempt")).isEqualTo(TOTAL);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  public void testCommitRetryWithFailure() {
    String className = "testCommitRetryWithFailure";
    database.getSchema().createDocumentType(className);

    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(defineThrowCME());

    database.transaction(() -> {
      String script = """
          LET $retries = 0;
          BEGIN;
          INSERT INTO %s set attempt = $retries;
          LET $retries = $retries + 1;
          SELECT throwCME(#-1:-1, 1, 1, 1);
          COMMIT RETRY 10;
          """.formatted(className);
      ;
      try {
        database.command("sqlscript", script);
      } catch (ConcurrentModificationException x) {
      }

      ResultSet result = database.query("sql", "select from " + className);
      assertThat(result.hasNext()).isFalse();
      result.close();
    });
  }

  @Test
  public void testCommitRetryWithFailureAndContinue() {
    String className = "testCommitRetryWithFailureAndContinue";
    database.getSchema().createDocumentType(className);

    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(defineThrowCME());

    database.transaction(() -> {
      String script = """
          LET $retries = 0;
          BEGIN;
          INSERT INTO %s set attempt = $retries;
          LET $retries = $retries + 1;
          SELECT throwCME(#-1:-1, 1, 1, 1);
          COMMIT RETRY 10 ELSE CONTINUE;
          INSERT INTO %s set name = 'foo';
          """.formatted(className, className);
      ;

      database.command("sqlscript", script);

      ResultSet result = database.query("sql", "select from " + className);
      assertThat(result.hasNext()).isTrue();
      Result item = result.next();
      assertThat(item.<String>getProperty("name")).isEqualTo("foo");
      assertThat(result.hasNext()).isFalse();
      result.close();
    });
  }

  @Test
  public void testCommitRetryWithFailureScriptAndContinue() {
    String className = "testCommitRetryWithFailureScriptAndContinue";
    database.getSchema().createDocumentType(className);

    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(defineThrowCME());

    database.transaction(() -> {
      String script = """
          LET $retries = 0;
          BEGIN;
          INSERT INTO %s set attempt = $retries;
          LET $retries = $retries + 1;
          SELECT throwCME(#-1:-1, 1, 1, 1);
          COMMIT RETRY 10 ELSE {
          INSERT INTO %s set name = 'foo';
          }
          AND CONTINUE;
          """.formatted(className, className);

      database.command("sqlscript", script);
    });

    ResultSet result = database.query("sql", "select from " + className);
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("foo");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  public void testCommitRetryWithFailureScriptAndFail() {
    String className = "testCommitRetryWithFailureScriptAndFail";
    database.getSchema().createDocumentType(className);

    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(defineThrowCME());

    database.transaction(() -> {
      String script = """
              LET $retries = 0;
              BEGIN;
              INSERT INTO %s set attempt = $retries;
              LET $retries = $retries + 1;
              SELECT throwCME(#-1:-1, 1, 1, 1);
              COMMIT RETRY 10 ELSE {
                  INSERT INTO %s set name = 'foo';
              } AND FAIL;
          """.formatted(className, className);

      try {
        database.command("sqlscript", script);
        Assertions.fail();
      } catch (ConcurrentModificationException e) {
      }
    });

    ResultSet result = database.query("sql", "select from " + className);
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("foo");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  public void testCommitRetryWithFailureScriptAndFail2() {
    String className = "testCommitRetryWithFailureScriptAndFail2";
    database.getSchema().createDocumentType(className);

    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(defineThrowCME());

    database.transaction(() -> {
      String script = """
              LET $retries = 0;
              BEGIN;
              INSERT INTO %s set attempt = $retries;
              LET $retries = $retries + 1;
              SELECT throwCME(#-1:-1, 1, 1, 1);
              COMMIT RETRY 10 ELSE {
                  INSERT INTO %s set name = 'foo';
              }
          """.formatted(className, className);

      try {
        database.command("sqlscript", script);
        Assertions.fail();
      } catch (ConcurrentModificationException e) {

      }

      ResultSet result = database.query("sql", "select from " + className);
      assertThat(result.hasNext()).isTrue();
      Result item = result.next();
      assertThat(item.<String>getProperty("name")).isEqualTo("foo");
      assertThat(result.hasNext()).isFalse();
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

      ResultSet rs = database.command("sqlscript", script);
      assertThat(rs.hasNext()).isTrue();
      Result item = rs.next();
      assertThat((Integer) item.getProperty("result")).isEqualTo(8);
      assertThat(rs.hasNext()).isFalse();

      rs.close();
    });
  }

  @Test
  public void testAssignOnEdgeCreate() {
    database.transaction(() -> {
      String script = """
              create vertex type V if not exists;
              create edge type E if not exists;
              create edge type IndirectEdge if not exists extends E;

              insert into V set name = 'a', PrimaryName = 'foo1';
              insert into V set name = 'b', PrimaryName = 'foo2';
              insert into V set name = 'c', PrimaryName = 'foo3';
              insert into V set name = 'd', PrimaryName = 'foo4';

              create edge E from (select from V where name = 'a') to (select from V where name = 'b');
              create edge E from (select from V where name = 'c') to (select from V where name = 'd');
          """;
      database.command("sqlscript", script).close();
    });

    String script = """
            begin;
            LET SourceDataset = SELECT expand(out()) from V where name = 'a';
            LET TarDataset = SELECT expand(out()) from V where name = 'c';
            IF ($SourceDataset[0] != $TarDataset[0]) {
                CREATE EDGE IndirectEdge FROM $SourceDataset To $TarDataset SET Source = $SourceDataset[0].PrimaryName;
            }
            commit retry 10;
        """;

    database.command("sqlscript", script).close();

    try (ResultSet rs = database.query("sql", "select from IndirectEdge")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("Source")).isEqualTo("foo2");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Override
  protected void beginTest() {
    database.async().setParallelLevel(PARALLEL_LEVEL);
  }

  private SQLFunction defineThrowCME() {
    return new SQLFunctionThrowCME();
  }
}
