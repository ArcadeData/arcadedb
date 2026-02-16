/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.function.sql.SQLFunctionAbstract;
  import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdatabase.com)
 */
class ScriptExecutionTest extends TestHelper {
  public static class SQLFunctionThrowCME extends SQLFunctionAbstract {
    public static final String NAME = "throwCME";

    /**
     * Get the date at construction to have the same date for all the iteration.
     */
    public SQLFunctionThrowCME() {
      super(NAME);
    }

    public Object execute(Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
        CommandContext context) {
      throw new ConcurrentModificationException("" + params[0]);
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
  void twoInserts() {
    String typeName = "testTwoInserts";
    database.getSchema().createDocumentType(typeName);
    database.transaction(() -> {
      database.command("sqlscript",
          "INSERT INTO " + typeName + " SET name = 'foo';INSERT INTO " + typeName + " SET name = 'bar';");
    });
    ResultSet rs = database.query("sql", "SELECT count(*) as count from " + typeName);
    assertThat(rs.next().<Long>getProperty("count")).isEqualTo((Object) 2L);
  }

  @Test
  void testIf() {
    String typeName = "testIf";
    database.getSchema().createDocumentType(typeName);
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
          """.formatted(typeName, typeName, typeName, typeName, typeName);
      database.command("sqlscript", script);
    });
    ResultSet rs = database.query("sql", "SELECT count(*) as count from " + typeName);
    assertThat(rs.next().<Long>getProperty("count")).isEqualTo(2L);
  }

  @Test
  void returnInIf() {
    String typeName = "testReturnInIf";
    database.getSchema().createDocumentType(typeName);

    database.transaction(() -> {
      String script = """
              INSERT INTO %s SET name = 'foo';
              LET $1 = SELECT count(*) as count FROM %s WHERE name ='foo';
              IF($1.size() = 0 OR $1[0].count = 0){
                  INSERT INTO %s SET name = 'bar';
                  RETURN;
              }
              INSERT INTO %s SET name = 'baz';
          """.formatted(typeName, typeName, typeName, typeName);
      database.command("sqlscript", script);
    });

    final ResultSet rs = database.query("sql", "SELECT count(*) as count from " + typeName);
    assertThat(rs.next().<Long>getProperty("count")).isEqualTo(2L);
  }

  @Test
  void returnInIf2() {
    String typeName = "testReturnInIf2";
    database.getSchema().createDocumentType(typeName);

    database.transaction(() -> {
      String script = """
              INSERT INTO %s SET name = 'foo';
              LET $1 = SELECT count(*) as count FROM %s WHERE name ='foo';
              IF($1.size() > 0 ){
                  RETURN 'OK';
              }
              RETURN 'FAIL';
          """.formatted(typeName, typeName);
      ResultSet result = database.command("sqlscript", script);

      Result item = result.next();

      assertThat(item.<String>getProperty("value")).isEqualTo("OK");
      result.close();
    });
  }

  @Test
  void returnInIf3() {
    String typeName = "testReturnInIf3";
    database.getSchema().createDocumentType(typeName);

    database.transaction(() -> {
      String script = """
              INSERT INTO %s SET name = 'foo';
              LET $1 = SELECT count(*) as count FROM %s WHERE name ='foo';
              IF($1.size() = 0 ){
                  RETURN 'FAIL';
              }
              RETURN 'OK';
          """.formatted(typeName, typeName);
      ResultSet result = database.command("sqlscript", script);

      Result item = result.next();

      assertThat(item.<String>getProperty("value")).isEqualTo("OK");
      result.close();
    });
  }

  @Test
  void lazyExecutionPlanning() {
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
  void commitRetry() {
    String typeName = "testCommitRetry";
    database.getSchema().createDocumentType(typeName);

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
          """.formatted(typeName);
      database.command("sqlscript", script);
    });

    ResultSet result = database.query("sql", "select from " + typeName);
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat((int) item.getProperty("attempt")).isEqualTo(4);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void commitRetryMultiThreadsSQLIncrement() throws Exception {
    String typeName = "testCommitRetryMTSQLIncrement";
    database.getSchema().createDocumentType(typeName, 8);

    // AVOID RETRY, EXPECTING TO MISS SOME UPDATES
    database.transaction(() -> {
      database.newDocument(typeName).set("id", 0).save();
    });

    final int TOTAL = 1000;
    String script = """
            LET $retries = 0;
            BEGIN;
            UPDATE %s set attempt = attempt + 1 WHERE id = 0;
            LET $retries = $retries + 1;
            COMMIT;
        """.formatted(typeName);
    for (int i = 0; i < TOTAL; i++) {
      database.async().command("sqlscript", script, null);
    }

    database.async().waitCompletion();

    ResultSet result = database.query("sql", "select from " + typeName + " where id = 0");
    Result item = result.next();
    result.close();

    // USE RETRY, EXPECTING NO MISS OF UPDATES
    database.transaction(() -> {
      database.newDocument(typeName).set("id", 1).save();
    });

    //database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);

    script = """
            LET $retries = 0;
            BEGIN;
            UPDATE %s set attempt += 1 WHERE id = 1;
            LET $retries = $retries + 1;
            COMMIT RETRY 100;
        """.formatted(typeName);
    for (int i = 0; i < TOTAL; i++) {
      database.async().command("sqlscript", script, null);
    }

    database.async().waitCompletion();

    ImmutablePage page = ((LocalDatabase) database).getPageManager().getImmutablePage(new PageId(database, 2, 0),
        ((PaginatedComponentFile) ((LocalDatabase) database).getFileManager().getFile(2)).getPageSize(), false, false);

    assertThat(page.getVersion()).as("Page v." + page.getVersion()).isEqualTo(TOTAL + 1);
  }

  @Test
  void commitRetryMultiThreadsSQLIncrementRepeatableRead() throws Exception {
    String typeName = "testCommitRetryMTSQLIncrement";
    database.getSchema()
        .buildDocumentType()
        .withName(typeName)
        .withTotalBuckets(Runtime.getRuntime().availableProcessors())
        .create();

    // AVOID RETRY, EXPECTING TO MISS SOME UPDATES
    database.transaction(() -> {
      database.newDocument(typeName).set("id", 0).save();
    });

    String script = """
        LET $retries = 0;
        BEGIN;
        UPDATE %s set attempt = attempt + 1 WHERE id = 0;
        LET $retries = $retries + 1;
        COMMIT;
        """.formatted(typeName);
    ;
    final int TOTAL = 10_000;
    for (int i = 0; i < TOTAL; i++) {
      database.async().command("sqlscript", script, null);
    }

    database.async().waitCompletion();

    ResultSet result = database.query("sql", "select from " + typeName + " where id = 0");
    Result item = result.next();
    assertThat(item.<Integer>getProperty("attempt")).as("Found attempts = " + item.getProperty("attempt")).isLessThan(TOTAL);
    result.close();

    // USE RETRY, EXPECTING NO MISS OF UPDATES
    database.transaction(() -> {
      database.newDocument(typeName).set("id", 1).save();
    });

    database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);

    script = """
        LET $retries = 0;
        BEGIN;
        UPDATE %s set attempt += 1 WHERE id = 1;
        LET $retries = $retries + 1;
        COMMIT RETRY 100;
        """.formatted(typeName);
    ;
    for (int i = 0; i < TOTAL; i++) {
      database.async().command("sqlscript", script, null);
    }

    database.async().waitCompletion();

    ImmutablePage page = ((LocalDatabase) database).getPageManager().getImmutablePage(new PageId(database, 2, 0),
        ((PaginatedComponentFile) ((LocalDatabase) database).getFileManager().getFile(2)).getPageSize(), false, false);

    assertThat(page.getVersion()).as("Page v." + page.getVersion()).isEqualTo(TOTAL + 1);

    result = database.query("sql", "select from " + typeName + " where id = 1");
    item = result.next();
    assertThat(item.<Integer>getProperty("attempt")).isEqualTo(TOTAL);
    result.close();
  }

  @Test
  void commitRetryWithFailure() {
    String typeName = "testCommitRetryWithFailure";
    database.getSchema().createDocumentType(typeName);

    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(defineThrowCME());

    database.transaction(() -> {
      String script = """
          LET $retries = 0;
          BEGIN;
          INSERT INTO %s set attempt = $retries;
          LET $retries = $retries + 1;
          SELECT throwCME(#-1:-1, 1, 1, 1);
          COMMIT RETRY 10;
          """.formatted(typeName);

      try {
        database.command("sqlscript", script);
      } catch (ConcurrentModificationException x) {
      }

      ResultSet result = database.query("sql", "select from " + typeName);
      assertThat(result.hasNext()).isFalse();
      result.close();
    });
  }

  @Test
  void commitRetryWithFailureAndContinue() {
    String typeName = "testCommitRetryWithFailureAndContinue";
    database.getSchema().createDocumentType(typeName);

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
          """.formatted(typeName, typeName);
      ;

      database.command("sqlscript", script);

      ResultSet result = database.query("sql", "select from " + typeName);
      Result item = result.next();
      assertThat(item.<String>getProperty("name")).isEqualTo("foo");
      result.close();
    });
  }

  @Test
  void commitRetryWithFailureScriptAndContinue() {
    String typeName = "testCommitRetryWithFailureScriptAndContinue";
    database.getSchema().createDocumentType(typeName);

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
          """.formatted(typeName, typeName);

      database.command("sqlscript", script);
    });

    ResultSet result = database.query("sql", "select from " + typeName);
    Result item = result.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("foo");
    result.close();
  }

  @Test
  void commitRetryWithFailureScriptAndFail() {
    String typeName = "testCommitRetryWithFailureScriptAndFail";
    database.getSchema().createDocumentType(typeName);

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
          """.formatted(typeName, typeName);

      assertThatThrownBy(() -> database.command("sqlscript", script))
          .isInstanceOf(ConcurrentModificationException.class);
    });

    ResultSet result = database.query("sql", "select from " + typeName);
    Result item = result.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("foo");
    result.close();
  }

  @Test
  void commitRetryWithFailureScriptAndFail2() {
    String typeName = "testCommitRetryWithFailureScriptAndFail2";
    database.getSchema().createDocumentType(typeName);

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
          """.formatted(typeName, typeName);

      assertThatThrownBy(() -> database.command("sqlscript", script))
          .isInstanceOf(ConcurrentModificationException.class);

      ResultSet result = database.query("sql", "select from " + typeName);
      Result item = result.next();
      assertThat(item.<String>getProperty("name")).isEqualTo("foo");
      result.close();
    });
  }

  @Test
  void functionAsStatement() {
    database.transaction(() -> {
      String script = "sqrt(64);";

      assertThatThrownBy(() -> database.command("sql", script))
          .isInstanceOf(CommandSQLParsingException.class);

      ResultSet rs = database.command("sqlscript", script);
      Result item = rs.next();
      assertThat((Integer) item.getProperty("result")).isEqualTo(8);

      rs.close();
    });
  }

  @Test
  void assignOnEdgeCreate() {
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
      assertThat(rs.next().<String>getProperty("Source")).isEqualTo("foo2");
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
