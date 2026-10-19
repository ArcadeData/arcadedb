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
package com.arcadedb.query;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10379: the guard that stops the caller of a failed multi-row statement from committing half of it (#10194) lived in
 * {@code SQLQueryEngine} alone, so the same statement written in openCypher or run as an SQL script, inside the caller's own
 * transaction, failed after its first row and the caller's commit published that row. The rule belongs to the transaction, not to
 * a language: every engine answers it now.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10379FailedStatementInCallerTransactionTest extends TestHelper {
  /** Every engine of this module that runs a statement writing records, each with the same failing multi-row update. */
  private static final String[][] MULTI_ROW_UPDATE = {
      { "sql", "UPDATE T SET v = 1" },
      { "opencypher", "MATCH (n:T) SET n.v = 1" },
      { "sqlscript", "UPDATE T SET v = 1;" } };

  /** The same engines, each with an update that is refused on its first and only row, before it wrote anything. */
  private static final String[][] SINGLE_ROW_REFUSAL = {
      { "sql", "UPDATE T SET v = 'abc' WHERE n = 0" },
      { "opencypher", "MATCH (n:T) WHERE n.n = 0 SET n.v = 'abc'" },
      { "sqlscript", "UPDATE T SET v = 'abc' WHERE n = 0;" } };

  @BeforeEach
  void createData() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE PROPERTY T.n INTEGER");
    database.command("sql", "CREATE PROPERTY T.v INTEGER");
    database.command("sql", "CREATE INDEX ON T (v) UNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newVertex("T").set("n", i).set("v", 100 + i).save();
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "sql", "opencypher", "sqlscript" })
  void aCaughtHalfAppliedStatementCannotBeCommitted(final String language) {
    final AtomicReference<Throwable> caught = new AtomicReference<>();

    assertThatThrownBy(() -> database.transaction(() -> {
      try {
        database.command(language, statement(MULTI_ROW_UPDATE, language)).close();
      } catch (final RuntimeException e) {
        caught.set(e);
      }
    }, false, 0)).as(language).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");

    assertThat(caught.get()).as(language + " failed").isNotNull();
    assertThat(values()).as(language + " published").containsExactly(100, 101, 102);
  }

  @ParameterizedTest
  @ValueSource(strings = { "sql", "opencypher", "sqlscript" })
  void theBeginAndCommitOfTheCallerRefuseTheSameWay(final String language) {
    // the reproducer of the report, with the caller driving begin() and commit() itself
    database.begin();
    try {
      database.command(language, statement(MULTI_ROW_UPDATE, language)).close();
    } catch (final RuntimeException ignored) {
      // an application retrying on a duplicate key catches it and goes on
    }

    assertThatThrownBy(() -> database.commit()).as(language).isInstanceOf(TransactionException.class)
        .hasMessageContaining("cannot be committed");
    database.rollback();
    assertThat(values()).as(language + " published").containsExactly(100, 101, 102);
  }

  @ParameterizedTest
  @ValueSource(strings = { "sql", "opencypher", "sqlscript" })
  void aStatementRefusedBeforeItWroteAnythingLeavesTheTransactionUsable(final String language) {
    final AtomicReference<Throwable> caught = new AtomicReference<>();

    database.transaction(() -> {
      try {
        database.command(language, statement(SINGLE_ROW_REFUSAL, language)).close();
      } catch (final RuntimeException e) {
        caught.set(e);
      }
      database.command("sql", "UPDATE T SET v = 500 WHERE n = 2").close();
    }, false, 0);

    assertThat(caught.get()).as(language + " failed").isNotNull();
    assertThat(values()).as(language).containsExactly(100, 101, 500);
  }

  @ParameterizedTest
  @ValueSource(strings = { "sql", "opencypher", "sqlscript" })
  void aStatementThatCannotBeParsedLeavesTheTransactionUsable(final String language) {
    database.transaction(() -> {
      database.command("sql", "UPDATE T SET v = 500 WHERE n = 2").close();
      assertThatThrownBy(() -> database.command(language, "THIS IS NOT A STATEMENT (").close()).as(language)
          .isInstanceOf(RuntimeException.class);
    }, false, 0);

    assertThat(values()).as(language).containsExactly(100, 101, 500);
  }

  @ParameterizedTest
  @ValueSource(strings = { "sql", "opencypher", "sqlscript" })
  void aStatementThatSucceedsCommitsNormally(final String language) {
    final String[][] update = {
        { "sql", "UPDATE T SET v = v + 1000" },
        { "opencypher", "MATCH (n:T) SET n.v = n.v + 1000" },
        { "sqlscript", "UPDATE T SET v = v + 1000;" } };

    database.transaction(() -> database.command(language, statement(update, language)).close(), false, 0);

    assertThat(values()).as(language).containsExactly(1100, 1101, 1102);
  }

  @Test
  void aFailedReadInsideATransactionWithEarlierWritesStaysUsable() {
    // the writes before the statement are the caller's own, completed before it started: they are not "part of" a statement that failed
    database.transaction(() -> {
      database.command("sql", "UPDATE T SET v = 500 WHERE n = 2").close();
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (n:T RETURN n").close()).isInstanceOf(RuntimeException.class);
      database.command("opencypher", "MATCH (n:T) WHERE n.n = 1 SET n.v = 600").close();
    }, false, 0);

    assertThat(values()).containsExactly(100, 600, 500);
  }

  @Test
  void aStatementWhoseOnlyWritesWereTakenBackByItsExistenceCheckLeavesTheTransactionUsable() {
    database.command("opencypher", "CREATE CONSTRAINT req_org_exists IF NOT EXISTS FOR (n:Req) REQUIRE n.orgId IS NOT NULL");

    database.transaction(() -> {
      // the record is created, updated and taken back by the statement itself: nothing of it stands (#7945), so nothing is half applied
      assertThatThrownBy(() -> database.command("opencypher", "CREATE (n:Req {id: 'a'}) SET n.note = 'x'").close())
          .isInstanceOf(RuntimeException.class);
      assertThatThrownBy(() -> database.command("opencypher", "CREATE (n:Req {id: 'b'})").close())
          .isInstanceOf(RuntimeException.class);
      database.command("sql", "UPDATE T SET v = 500 WHERE n = 2").close();
    }, false, 0);

    assertThat(values()).containsExactly(100, 101, 500);
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM Req")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
    }
  }

  @Test
  void aStatementThatTookBackItsProvisionalRecordsButKeptAnotherWriteStillRefusesTheCommit() {
    database.command("opencypher", "CREATE CONSTRAINT req_org_exists IF NOT EXISTS FOR (n:Req) REQUIRE n.orgId IS NOT NULL");

    assertThatThrownBy(() -> database.transaction(() -> {
      try {
        // the update of an existing record stands when the existence check of the created one fails at the end of the statement
        database.command("opencypher", "MATCH (n:T) WHERE n.n = 0 SET n.v = 900 CREATE (:Req {id: 'c'})").close();
      } catch (final RuntimeException ignored) {
        // caught by the caller
      }
    }, false, 0)).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");

    assertThat(values()).containsExactly(100, 101, 102);
  }

  @Test
  void aProvisionalRecordCreatedAndUpdatedByTheStatementDoesNotHideAWriteThatStays() {
    database.command("opencypher", "CREATE CONSTRAINT req_org_exists IF NOT EXISTS FOR (n:Req) REQUIRE n.orgId IS NOT NULL");

    assertThatThrownBy(() -> database.transaction(() -> {
      try {
        // the Req is created, updated while provisional and taken back; the update of the T stays and is what the commit must refuse
        database.command("opencypher", "MATCH (n:T) WHERE n.n = 0 SET n.v = 900 CREATE (m:Req {id: 'd'}) SET m.note = 'x'").close();
      } catch (final RuntimeException ignored) {
        // caught by the caller
      }
    }, false, 0)).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");

    assertThat(values()).containsExactly(100, 101, 102);
  }

  @Test
  void anUpdateVetoedOnAProvisionalRecordIsNotCountedSoItCannotCancelAWriteThatStays() {
    database.command("opencypher", "CREATE CONSTRAINT req_org_exists IF NOT EXISTS FOR (n:Req) REQUIRE n.orgId IS NOT NULL");
    // the listener refuses the update of the provisional record: the write is not made, so it is not counted either
    database.getSchema().getType("Req").getEvents().registerListener((BeforeRecordUpdateListener) record -> false);

    assertThatThrownBy(() -> database.transaction(() -> {
      try {
        database.command("opencypher", "MATCH (n:T) WHERE n.n = 0 SET n.v = 900 CREATE (m:Req {id: 'f'}) WITH m SET m.note = 'x'").close();
      } catch (final RuntimeException ignored) {
        // caught by the caller
      }
    }, false, 0)).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");

    assertThat(values()).containsExactly(100, 101, 102);
  }

  /**
   * Every public {@code command()} overload of the database carries the check: a new overload that forgets it fails here. The
   * {@code query()} overloads of SQL and openCypher refuse a write outright, so they are covered where a query can still write, by
   * the Gremlin test.
   */
  @ParameterizedTest
  @ValueSource(ints = { 0, 1, 2, 3, 4 })
  void everyCommandOverloadRefusesTheCommit(final int overload) {
    final AtomicReference<Throwable> caught = new AtomicReference<>();

    assertThatThrownBy(() -> database.transaction(() -> {
      try {
        runThroughOverload(overload).close();
      } catch (final RuntimeException e) {
        caught.set(e);
      }
    }, false, 0)).as("overload " + overload).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");

    assertThat(caught.get()).as("overload " + overload + " failed").isNotNull();
    assertThat(values()).as("overload " + overload).containsExactly(100, 101, 102);
  }

  @ParameterizedTest
  @ValueSource(strings = { "sql", "opencypher" })
  void queryRefusesAWriteStatement(final String language) {
    final String statement = language.equals("sql") ? "UPDATE T SET v = 1" : "MATCH (n:T) SET n.v = 1";
    assertThatThrownBy(() -> database.query(language, statement)).as(language).isInstanceOf(QueryNotIdempotentException.class);
    assertThat(values()).as(language).containsExactly(100, 101, 102);
  }

  private ResultSet runThroughOverload(final int overload) {
    final ContextConfiguration configuration = new ContextConfiguration();
    final String update = "UPDATE T SET v = 1";
    return switch (overload) {
      case 0 -> database.command("sql", update);
      case 1 -> database.command("sql", update, new Object[] {});
      case 2 -> database.command("sql", update, configuration, new Object[] {});
      case 3 -> database.command("sql", update, Map.<String, Object>of());
      case 4 -> database.command("sql", update, configuration, Map.<String, Object>of());
      default -> throw new IllegalArgumentException("No overload " + overload);
    };
  }

  @Test
  void aFailureAfterTheCallersOwnEarlierWritesIsRefusedToo() {
    assertThatThrownBy(() -> database.transaction(() -> {
      // the caller's own write, made before the statement starts, is not what the statement is judged by
      database.command("sql", "UPDATE T SET v = 500 WHERE n = 2").close();
      try {
        database.command("opencypher", "MATCH (n:T) WHERE n.n < 2 SET n.v = 1").close();
      } catch (final RuntimeException ignored) {
        // caught by the caller
      }
    }, false, 0)).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");

    assertThat(values()).containsExactly(100, 101, 102);
  }

  @Test
  void takingBackAProvisionalRecordGivesBackExactlyTheWritesItCounted() {
    database.command("opencypher", "CREATE CONSTRAINT req_org_exists IF NOT EXISTS FOR (n:Req) REQUIRE n.orgId IS NOT NULL");

    database.begin();
    final long before = ((DatabaseInternal) database).getTransaction().getCompletedRecordWrites();
    // created, updated twice while provisional, then deleted by the existence check: the counter is where it was
    assertThatThrownBy(() -> database.command("opencypher", "CREATE (n:Req {id: 'h'}) WITH n SET n.a = 1 WITH n SET n.b = 2").close())
        .isInstanceOf(RuntimeException.class);
    assertThat(((DatabaseInternal) database).getTransaction().getCompletedRecordWrites()).isEqualTo(before);

    // and a statement that really fails after writing is still refused afterwards
    assertThatThrownBy(() -> database.command("opencypher", "MATCH (n:T) SET n.v = 1").close()).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
    database.rollback();

    assertThat(values()).containsExactly(100, 101, 102);
  }

  @Test
  void twoUpdatesInARowOnAProvisionalRecordAreBothGivenBack() {
    database.command("opencypher", "CREATE CONSTRAINT req_org_exists IF NOT EXISTS FOR (n:Req) REQUIRE n.orgId IS NOT NULL");

    database.transaction(() -> {
      assertThatThrownBy(() -> database.command("opencypher", "CREATE (n:Req {id: 'g'}) WITH n SET n.a = 1 WITH n SET n.b = 2").close())
          .isInstanceOf(RuntimeException.class);
      database.command("sql", "UPDATE T SET v = 500 WHERE n = 2").close();
    }, false, 0);

    assertThat(values()).containsExactly(100, 101, 500);
  }

  @Test
  void aScriptWithAWriteOutsideItsRetryBlockDoesNotHandleItsOwnFailures() {
    final QueryEngine script = database.getQueryEngine("sqlscript");
    assertThat(script.handlesItsOwnFailures("UPDATE T SET v = 1; BEGIN; UPDATE T SET v = 2; COMMIT RETRY 3;")).isFalse();
    assertThat(script.handlesItsOwnFailures("BEGIN; UPDATE T SET v = 2; COMMIT RETRY 3; UPDATE T SET v = 1;")).isFalse();
    assertThat(script.handlesItsOwnFailures("LET $a = 1; BEGIN; UPDATE T SET v = 2; COMMIT RETRY 3;")).isTrue();
    assertThat(script.handlesItsOwnFailures("BEGIN; UPDATE T SET v = 2; COMMIT RETRY 3 ELSE { UPDATE T SET v = 3; }")).isTrue();
  }

  @Test
  void aTopLevelWriteBesideARetryBlockIsRefusedWhenTheScriptFails() {
    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(new SQLFunctionThrowCME());
    // row 0 is written outside the block, which stays in the caller's transaction; the block then fails on every attempt
    final String script = "UPDATE T SET v = 1 WHERE n = 0; BEGIN; SELECT throwCME(#-1:-1, 1, 1, 1); COMMIT RETRY 2;";

    final AtomicReference<Throwable> caught = new AtomicReference<>();
    assertThatThrownBy(() -> database.transaction(() -> {
      try {
        database.command("sqlscript", script).close();
      } catch (final RuntimeException e) {
        caught.set(e);
      }
    }, false, 0)).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");

    assertThat(caught.get()).as("the script failed").isInstanceOf(ConcurrentModificationException.class);
    assertThat(values()).containsExactly(100, 101, 102);
  }

  @Test
  void aScriptWhoseWritesAreAllInTheRetryBlockLeavesTheTransactionUsable() {
    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(new SQLFunctionThrowCME());
    final String script = "BEGIN; UPDATE T SET v = 1 WHERE n = 0; SELECT throwCME(#-1:-1, 1, 1, 1); COMMIT RETRY 2;";

    database.transaction(() -> {
      assertThatThrownBy(() -> database.command("sqlscript", script).close()).isInstanceOf(ConcurrentModificationException.class);
      database.command("sql", "UPDATE T SET v = 500 WHERE n = 2").close();
    }, false, 0);

    assertThat(values()).containsExactly(100, 101, 500);
  }

  /**
   * The check runs at every call, so a statement a function runs through the database, whose failure the outer statement catches and
   * carries on from, marks the caller's transaction like any other: its first rows are in the transaction and nothing takes them back.
   */
  @Test
  void aNestedStatementThatFailedAfterWritingAndWasCaughtStillRefusesTheCommit() {
    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(new SQLFunctionNestedUpdate());

    assertThatThrownBy(() -> database.transaction(() -> {
      // the outer statement succeeds: its function caught the failure of the multi-row update it ran
      try (final ResultSet rs = database.command("sql", "SELECT nestedUpdate() AS r")) {
        // the projection runs when the row is read
        assertThat(rs.hasNext()).isTrue();
        rs.next();
      }
    }, false, 0)).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");

    assertThat(values()).containsExactly(100, 101, 102);
  }

  /** Runs a multi-row update through the database, which fails on its second row, and catches the failure. */
  public static class SQLFunctionNestedUpdate extends SQLFunctionAbstract {
    SQLFunctionNestedUpdate() {
      super("nestedUpdate");
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
        final CommandContext context) {
      try {
        context.getDatabase().command("sql", "UPDATE T SET v = 1").close();
      } catch (final RuntimeException ignored) {
        // carried on from by the function, as an application retrying on a duplicate key would
      }
      return null;
    }

    @Override
    public String getSyntax() {
      return "nestedUpdate()";
    }

    @Override
    public Object getResult() {
      return null;
    }
  }

  /** Fails with a ConcurrentModificationException, which {@code COMMIT RETRY} answers by rolling the attempt back and trying again. */
  public static class SQLFunctionThrowCME extends SQLFunctionAbstract {
    SQLFunctionThrowCME() {
      super("throwCME");
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
        final CommandContext context) {
      throw new ConcurrentModificationException("" + params[0]);
    }

    @Override
    public String getSyntax() {
      return "throwCME(message)";
    }

    @Override
    public Object getResult() {
      return null;
    }
  }

  /** The write engines run a statement to the end inside command(), so the failure of a row is raised there and not while the rows are read. */
  @ParameterizedTest
  @ValueSource(strings = { "sql", "opencypher" })
  void aFailureWhileTheResultIsReadIsStillRefused(final String language) {
    final String statement = language.equals("sql") ? "UPDATE T SET v = 1 RETURN AFTER" : "MATCH (n:T) SET n.v = 1 RETURN n";

    assertThatThrownBy(() -> database.transaction(() -> {
      try (final ResultSet rs = database.command(language, statement)) {
        while (rs.hasNext())
          rs.next();
      } catch (final RuntimeException ignored) {
        // caught by the caller
      }
    }, false, 0)).as(language).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");

    assertThat(values()).as(language).containsExactly(100, 101, 102);
  }

  @Test
  void onlyAScriptWithARetryBlockHandlesItsOwnFailures() {
    final QueryEngine script = database.getQueryEngine("sqlscript");
    assertThat(script.handlesItsOwnFailures("BEGIN; UPDATE T SET v = 1; COMMIT RETRY 3;")).isTrue();
    assertThat(script.handlesItsOwnFailures("UPDATE T SET v = 1;")).isFalse();
    assertThat(script.handlesItsOwnFailures("BEGIN; UPDATE T SET v = 1; COMMIT;")).isFalse();
    assertThat(script.handlesItsOwnFailures("THIS IS NOT A SCRIPT (")).isFalse();
    assertThat(database.getQueryEngine("sql").handlesItsOwnFailures("UPDATE T SET v = 1")).isFalse();
    assertThat(database.getQueryEngine("opencypher").handlesItsOwnFailures("MATCH (n:T) SET n.v = 1")).isFalse();
  }

  private static String statement(final String[][] statements, final String language) {
    for (final String[] s : statements)
      if (s[0].equals(language))
        return s[1];
    throw new AssertionError("No statement for " + language);
  }

  private List<Integer> values() {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT v FROM T ORDER BY n")) {
      while (rs.hasNext())
        out.add(rs.next().<Number>getProperty("v").intValue());
    }
    return out;
  }
}
