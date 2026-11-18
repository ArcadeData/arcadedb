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
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Consolidated regression tests, one nested class per original issue.
 */
class StatementGuardTest {
  /**
   * Regression for #10379: a failed statement never leaves half of itself in the caller transaction, in every language.
   * <p>
   * Issue #10379: the guard that stops the caller of a failed multi-row statement from committing half of it (#10194) lived in
   * {@code SQLQueryEngine} alone, so the same statement written in openCypher or run as an SQL script, inside the caller's own
   * transaction, failed after its first row and the caller's commit published that row. The rule belongs to the transaction, not to
   * a language: every engine answers it now.
   * <p>
   * Since statement savepoints (#10346) the answer is no longer to refuse the commit: the failed statement is taken back to its
   * savepoint, in every language, and the transaction goes on. The commit is still refused where the engine cannot take a statement back
   * exactly (see {@code StatementSavepoint}), which {@link #aFailureAfterTheCallersOwnEarlierWritesIsRefusedToo} pins.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FailedStatementInCallerTransaction extends TestHelper {
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
    void aCaughtHalfAppliedStatementIsTakenBackAndTheTransactionGoesOn(final String language) {
      final AtomicReference<Throwable> caught = new AtomicReference<>();

      database.transaction(() -> {
        try {
          database.command(language, statement(MULTI_ROW_UPDATE, language)).close();
        } catch (final RuntimeException e) {
          caught.set(e);
        }
        // the rows the statement wrote before its error are gone from the transaction too
        assertThat(values()).as(language + " read in the transaction").containsExactly(100, 101, 102);
        database.command("sql", "UPDATE T SET v = 500 WHERE n = 2").close();
      }, false, 0);

      assertThat(caught.get()).as(language + " failed").isNotNull();
      assertThat(values()).as(language + " published").containsExactly(100, 101, 500);
    }

    @ParameterizedTest
    @ValueSource(strings = { "sql", "opencypher", "sqlscript" })
    void theBeginAndCommitOfTheCallerWorkTheSameWay(final String language) {
      // the reproducer of the report, with the caller driving begin() and commit() itself
      database.begin();
      try {
        database.command(language, statement(MULTI_ROW_UPDATE, language)).close();
      } catch (final RuntimeException ignored) {
        // an application retrying on a duplicate key catches it and goes on
      }

      database.commit();
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
    void aStatementThatTookBackItsProvisionalRecordsIsTakenBackWithTheWriteThatWouldStand() {
      database.command("opencypher", "CREATE CONSTRAINT req_org_exists IF NOT EXISTS FOR (n:Req) REQUIRE n.orgId IS NOT NULL");

      database.transaction(() -> {
        try {
          // the update of an existing record would stand when the existence check of the created one fails at the end of the statement
          database.command("opencypher", "MATCH (n:T) WHERE n.n = 0 SET n.v = 900 CREATE (:Req {id: 'c'})").close();
        } catch (final RuntimeException ignored) {
          // caught by the caller
        }
      }, false, 0);

      assertThat(values()).containsExactly(100, 101, 102);
    }

    @Test
    void aProvisionalRecordCreatedAndUpdatedByTheStatementIsTakenBackWithTheWriteThatWouldStand() {
      database.command("opencypher", "CREATE CONSTRAINT req_org_exists IF NOT EXISTS FOR (n:Req) REQUIRE n.orgId IS NOT NULL");

      database.transaction(() -> {
        try {
          // the Req is created, updated while provisional and taken back; the update of the T would stand
          database.command("opencypher", "MATCH (n:T) WHERE n.n = 0 SET n.v = 900 CREATE (m:Req {id: 'd'}) SET m.note = 'x'").close();
        } catch (final RuntimeException ignored) {
          // caught by the caller
        }
      }, false, 0);

      assertThat(values()).containsExactly(100, 101, 102);
    }

    @Test
    void anUpdateVetoedOnAProvisionalRecordIsTakenBackWithTheWriteThatWouldStand() {
      database.command("opencypher", "CREATE CONSTRAINT req_org_exists IF NOT EXISTS FOR (n:Req) REQUIRE n.orgId IS NOT NULL");
      // the listener refuses the update of the provisional record: the write is not made, so it is not counted either
      database.getSchema().getType("Req").getEvents().registerListener((BeforeRecordUpdateListener) record -> false);

      database.transaction(() -> {
        try {
          database.command("opencypher", "MATCH (n:T) WHERE n.n = 0 SET n.v = 900 CREATE (m:Req {id: 'f'}) WITH m SET m.note = 'x'").close();
        } catch (final RuntimeException ignored) {
          // caught by the caller
        }
      }, false, 0);

      assertThat(values()).containsExactly(100, 101, 102);
    }

    /**
     * Every public {@code command()} overload of the database carries the check: a new overload that forgets it fails here. The
     * {@code query()} overloads of SQL and openCypher refuse a write outright, so they are covered where a query can still write, by
     * the Gremlin test.
     */
    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3, 4 })
    void everyCommandOverloadTakesTheStatementBack(final int overload) {
      final AtomicReference<Throwable> caught = new AtomicReference<>();

      database.transaction(() -> {
        try {
          runThroughOverload(overload).close();
        } catch (final RuntimeException e) {
          caught.set(e);
        }
      }, false, 0);

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

    /**
     * The statement reads the record the caller updated before it started, which is not written yet: the object the transaction holds
     * for it is the only copy of that update. The statement is taken back to the content the record had when it began, so the update
     * of the caller stands and nothing of the statement does.
     */
    @Test
    void aFailureAfterTheCallersOwnEarlierWritesIsTakenBackAndTheEarlierWritesStand() {
      database.transaction(() -> {
        // the caller's own write, made before the statement starts, is not what the statement is judged by
        database.command("sql", "UPDATE T SET v = 500 WHERE n = 2").close();
        try {
          database.command("opencypher", "MATCH (n:T) WHERE n.n < 2 SET n.v = 1").close();
        } catch (final RuntimeException ignored) {
          // caught by the caller
        }
      }, false, 0);

      assertThat(values()).containsExactly(100, 101, 500);
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

      // and a statement that really fails after writing is taken back, whole
      assertThatThrownBy(() -> database.command("opencypher", "MATCH (n:T) SET n.v = 1").close()).isInstanceOf(RuntimeException.class);
      assertThat(((DatabaseInternal) database).getTransaction().getCompletedRecordWrites()).isEqualTo(before);
      database.commit();

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
    void aTopLevelWriteBesideARetryBlockIsTakenBackWhenTheScriptFails() {
      ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(new SQLFunctionThrowCME());
      // row 0 is written outside the block, which stays in the caller's transaction; the block then fails on every attempt
      final String script = "UPDATE T SET v = 1 WHERE n = 0; BEGIN; SELECT throwCME(#-1:-1, 1, 1, 1); COMMIT RETRY 2;";

      final AtomicReference<Throwable> caught = new AtomicReference<>();
      database.transaction(() -> {
        try {
          database.command("sqlscript", script).close();
        } catch (final RuntimeException e) {
          caught.set(e);
        }
      }, false, 0);

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
     * The check runs at every call. The projection of a SELECT runs when the row is read, after {@code command()} returned: the
     * statement a function runs there through the database is a statement of the caller's transaction like any other, and it is taken
     * back on its own when it fails, whatever the function does with the failure.
     */
    @Test
    void aStatementAFunctionRunsWhileTheResultIsReadIsTakenBackOnItsOwn() {
      ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(new SQLFunctionNestedUpdate());

      database.transaction(() -> {
        // the outer statement succeeds: its function caught the failure of the multi-row update it ran
        try (final ResultSet rs = database.command("sql", "SELECT nestedUpdate() AS r")) {
          // the projection runs when the row is read
          assertThat(rs.hasNext()).isTrue();
          rs.next();
        }
      }, false, 0);

      assertThat(values()).containsExactly(100, 101, 102);
    }

    /**
     * A statement run by another one that failed after writing, which the outer statement swallowed, leaves its half in the transaction
     * with nothing to take it back: it is part of the outer statement, which succeeded.
     */
    @Test
    void aNestedStatementThatFailedAfterWritingAndWasSwallowedStillRefusesTheCommit() {
      ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(new SQLFunctionNestedUpdate());

      assertThatThrownBy(() -> database.transaction(() -> {
        // the function runs while the statement is: an UPDATE ... SET calls it for every row it writes
        database.command("sql", "UPDATE T SET v = v + 1000 WHERE n = 0 AND nestedUpdate() IS NULL").close();
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
    void aFailureWhileTheResultIsReadIsStillTakenBack(final String language) {
      final String statement = language.equals("sql") ? "UPDATE T SET v = 1 RETURN AFTER" : "MATCH (n:T) SET n.v = 1 RETURN n";

      database.transaction(() -> {
        try (final ResultSet rs = database.command(language, statement)) {
          while (rs.hasNext())
            rs.next();
        } catch (final RuntimeException ignored) {
          // caught by the caller
        }
      }, false, 0);

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

  /**
   * Regression for #10482: gaps in the guard against committing half of a failed statement.
   * <p>
   * Issue #10482: two gaps in the guard that stops a caller from committing half of a failed statement (#10194, #10379). Since
   * statement savepoints (#10346) a statement is taken back whole when the engine can, so the tests of the second gap check that the
   * transaction commits with no trace of the statement, and the ones of the first gap, where the engine cannot (the instance an earlier
   * statement queued is the only copy of what it did), still check that the commit is refused.
   * <ol>
   *   <li>a refused UPDATE of a record the same transaction already updated left the refused values on the instance the transaction
   *   had queued for the commit, and the guard saw no completed write, so the commit stored them;</li>
   *   <li>a statement that took its own provisional records back (a mandatory property, #7945) still refused the commit when the
   *   records had edges, because only the creation, the delete and the updates of the record itself were given back.</li>
   * </ol>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class StatementGuardGaps extends TestHelper {
    /** Raised by the before-update listener when {@link #refuse} is set. */
    private volatile boolean refuse;
    private volatile boolean vetoInsteadOfThrowing;

    @BeforeEach
    void createData() {
      database.command("sql", "CREATE VERTEX TYPE U");
      database.command("sql", "CREATE PROPERTY U.u INTEGER");
      database.command("sql", "CREATE PROPERTY U.w INTEGER");
      database.command("sql", "ALTER PROPERTY U.w MAX 5");
      database.command("sql", "CREATE INDEX ON U (u) UNIQUE");
      database.command("sql", "CREATE VERTEX TYPE T");
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE R");
      database.command("sql", "CREATE PROPERTY V.p STRING");
      database.command("sql", "ALTER PROPERTY V.p MANDATORY true");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO U SET u = 1, w = 0, s = 's'").close();
        database.command("sql", "INSERT INTO U SET u = 2, w = 0, s = 's'").close();
        database.command("sql", "CREATE VERTEX T SET k = 1").close();
        database.command("sql", "CREATE VERTEX T SET k = 2").close();
      });
      database.getEvents().registerListener((BeforeRecordUpdateListener) record -> {
        if (!refuse)
          return true;
        if (vetoInsteadOfThrowing)
          return false;
        throw new IllegalStateException("VETO");
      });
    }

    // ---- 1. a refused UPDATE of a record the transaction already updated ----

    static Stream<Arguments> refusedSecondUpdates() {
      return Stream.of( //
          Arguments.of("sql", "UPDATE U SET s = 'A' WHERE u = 1", "UPDATE U SET w = 99 WHERE u = 1", false), //
          Arguments.of("opencypher", "MATCH (m:U {u: 1}) SET m.s = 'A'", "MATCH (m:U {u: 1}) SET m.w = 99", false), //
          Arguments.of("sql", "UPDATE U SET s = 'A' WHERE u = 1", "UPDATE U SET s = 'B' WHERE u = 1", true), //
          Arguments.of("opencypher", "MATCH (m:U {u: 1}) SET m.s = 'A'", "MATCH (m:U {u: 1}) SET m.s = 'B'", true));
    }

    /**
     * The refused value must never reach the disk. The refusal comes after the statement set it on the object the transaction queued for
     * its commit: the statement is taken back to the content that object held before it, so the earlier update stands and the refused
     * value is gone (#10346; the commit was refused instead before).
     */
    @ParameterizedTest(name = "{0}: {2}")
    @MethodSource("refusedSecondUpdates")
    void aRefusedUpdateOfARecordAlreadyUpdatedByTheTransactionKeepsTheEarlierUpdateOnly(final String language, final String first,
        final String second, final boolean byListener) {
      database.begin();
      database.command(language, first).close();
      refuse = byListener;
      try {
        assertRefusedWith(() -> database.command(language, second).close(), ValidationException.class, IllegalStateException.class);
      } finally {
        refuse = false;
      }

      database.commit();

      assertThat(rowOf(1)).isEqualTo("w=0,s=A");
      assertDatabaseIsConsistent();
    }

    /** A listener that answers false skips the record: the update it skipped must not be committed either, whatever the language. */
    @ParameterizedTest
    @ValueSource(strings = { "sql", "opencypher" })
    void aVetoedUpdateOfARecordAlreadyUpdatedByTheTransactionIsNotCommitted(final String language) {
      final String first = language.equals("sql") ? "UPDATE U SET s = 'A' WHERE u = 1" : "MATCH (m:U {u: 1}) SET m.s = 'A'";
      final String second = language.equals("sql") ? "UPDATE U SET s = 'B' WHERE u = 1" : "MATCH (m:U {u: 1}) SET m.s = 'B'";

      database.begin();
      database.command(language, first).close();
      refuse = true;
      vetoInsteadOfThrowing = true;
      try {
        database.command(language, second).close();
      } finally {
        refuse = false;
        vetoInsteadOfThrowing = false;
      }

      assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
      database.rollback();

      assertThat(rowOf(1)).isEqualTo("w=0,s=s");
    }

    /** The same statements on a record the transaction has not touched leave nothing behind, so the transaction stays usable. */
    @ParameterizedTest(name = "{0}: {2}")
    @MethodSource("refusedSecondUpdates")
    void aRefusedUpdateOfAnotherRecordLeavesTheTransactionUsable(final String language, final String first, final String second,
        final boolean byListener) {
      final String firstOnRecord2 = first.replace("1", "2");

      database.begin();
      database.command(language, firstOnRecord2).close();
      refuse = byListener;
      try {
        assertRefusedWith(() -> database.command(language, second).close(), ValidationException.class, IllegalStateException.class);
      } finally {
        refuse = false;
      }
      database.commit();

      assertThat(rowOf(1)).isEqualTo("w=0,s=s");
      assertThat(rowOf(2)).isEqualTo("w=0,s=A");
    }

    // ---- 2. a statement that took its own records back ----

    static Stream<String> statementsThatTakeTheirRecordsBack() {
      return Stream.of( //
          "CREATE (:V {k: 1})", //
          "MATCH (m:T {k: 1}) CREATE (m)-[:R {w: 1}]->(:V {k: 2})", //
          "CREATE (v:V {k: 3})-[:R {w: 1}]->(v)", //
          "MATCH (m:T {k: 1}) CREATE (:V {k: 4})-[:R {w: 1}]->(m)", //
          "CREATE (a:V {k: 5})-[:R {w: 1}]->(b:V {k: 6})-[:R {w: 2}]->(a)", //
          "MATCH (m:T {k: 1}), (n:T {k: 2}) CREATE (m)-[:R {w: 1}]->(:V {k: 7})-[:R {w: 2}]->(n)");
    }

    @ParameterizedTest
    @MethodSource("statementsThatTakeTheirRecordsBack")
    void aStatementWhoseRecordsWereAllTakenBackLeavesTheTransactionCommittable(final String statement) {
      database.begin();
      database.command("sql", "UPDATE U SET s = 'A' WHERE u = 1").close();
      assertRefusedWith(() -> database.command("opencypher", statement).close(), ValidationException.class);

      database.commit();

      assertThat(rowOf(1)).isEqualTo("w=0,s=A");
      assertThat(count("V")).isZero();
      assertThat(count("R")).isZero();
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
        assertThat(rs.next().<Number>getProperty("totalErrors").longValue()).isZero();
      }
    }

    /**
     * The statement is taken back to its savepoint (#10346), so what it left beside the records it took back itself goes too. Before
     * savepoints the take-back gave back only what the statement did to those records, and the transaction was refused its commit.
     */
    @Test
    void anEdgeBetweenTwoExistingRecordsIsTakenBackWithTheRestOfTheStatement() {
      database.begin();
      database.command("sql", "UPDATE U SET s = 'A' WHERE u = 1").close();
      // the edge between the two T would stand when the V created next to it is taken back at the end of the statement
      assertRefusedWith(() -> database.command("opencypher",
          "MATCH (m:T {k: 1}), (n:T {k: 2}) CREATE (m)-[:R {w: 1}]->(n) CREATE (:V {k: 8})").close(), ValidationException.class);

      database.commit();

      assertThat(count("R")).isZero();
      assertThat(count("V")).isZero();
      assertThat(rowOf(1)).isEqualTo("w=0,s=A");
      assertDatabaseIsConsistent();
    }

    /**
     * A lightweight edge is an entry in the edge lists of its two vertices and no record: the one between two existing vertices would
     * stand when the provisional record is taken back. The edge lists are pages, and come back with the statement.
     */
    @Test
    void aLightweightEdgeBetweenTwoExistingRecordsIsTakenBackWithTheRestOfTheStatement() {
      database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
      // the two vertices have an edge list already, so the lightweight edge only appends to it: updates that assign no property
      database.command("opencypher", "MATCH (m:T {k: 1}), (n:T {k: 2}) CREATE (m)-[:R {w: 0}]->(n)").close();

      database.begin();
      database.command("sql", "UPDATE U SET s = 'A' WHERE u = 1").close();
      assertRefusedWith(() -> database.command("opencypher",
          "CREATE (v:V {k: 10}) WITH v MATCH (m:T {k: 1}), (n:T {k: 2}) CREATE (m)-[:L]->(n)").close(), ValidationException.class);

      database.commit();

      assertThat(rowOf(1)).isEqualTo("w=0,s=A");
      try (final ResultSet rs = database.query("opencypher", "MATCH (:T)-[r:L]->(:T) RETURN count(r) AS c")) {
        assertThat(rs.next().<Long>getProperty("c")).isZero();
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH (:T)-[r:R]->(:T) RETURN count(r) AS c")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
      }
      assertDatabaseIsConsistent();
    }

    /**
     * A statement that writes more than the savepoint journals cannot be taken back exactly, so it is not taken back at all: the commit is
     * refused, as it was before savepoints, rather than risk giving back part of what it wrote.
     */
    @Test
    void aStatementThatWritesPastTheLimitOfTheSavepointKeepsTheCommitRefused() {
      database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES, 100);
      try {
        database.begin();
        database.command("sql", "UPDATE U SET s = 'A' WHERE u = 1").close();
        // 5,000 edges are more entries than the savepoint holds; all of them are taken back with the vertex they leave from and arrive at
        assertRefusedWith(() -> database.command("opencypher",
            "CREATE (v:V {k: 11}) WITH v UNWIND range(1, 5000) AS i CREATE (v)-[:R {w: i}]->(v)").close(), ValidationException.class);

        assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
        database.rollback();
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES,
            GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES.getDefValue());
      }

      assertThat(rowOf(1)).isEqualTo("w=0,s=s");
      assertThat(count("V")).isZero();
      assertThat(count("R")).isZero();
    }

    /** The same statement within the limit is taken back whole: 5,000 edges, their segments, their vertex. */
    @Test
    void aStatementThatWritesThousandsOfRecordsIsTakenBackWhole() {
      database.begin();
      database.command("sql", "UPDATE U SET s = 'A' WHERE u = 1").close();
      assertRefusedWith(() -> database.command("opencypher",
          "CREATE (v:V {k: 11}) WITH v UNWIND range(1, 5000) AS i CREATE (v)-[:R {w: i}]->(v)").close(), ValidationException.class);

      database.commit();

      assertThat(rowOf(1)).isEqualTo("w=0,s=A");
      assertThat(count("V")).isZero();
      assertThat(count("R")).isZero();
      assertDatabaseIsConsistent();
    }

    /** The property is written while the provisional record is pending: it goes with the statement. */
    @Test
    void aPropertyWrittenWhileTheProvisionalRecordIsPendingIsTakenBack() {
      database.begin();
      assertRefusedWith(() -> database.command("opencypher",
          "CREATE (v:V {k: 15}) WITH v MATCH (m:T {k: 1}) SET m.note = 'x' CREATE (m)-[:R {w: 1}]->(v)").close(), ValidationException.class);

      database.commit();

      try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM T WHERE note = 'x'")) {
        assertThat(rs.next().<Long>getProperty("c")).isZero();
      }
      assertThat(count("V")).isZero();
      assertThat(count("R")).isZero();
      assertDatabaseIsConsistent();
    }

    /**
     * The edge between two existing vertices that have no edge list yet makes the statement create their edge segments, while the
     * provisional record is pending: the segments, the edge and the vertex records pointing at them all go with the statement.
     */
    @Test
    void anEdgeSegmentCreatedBesideTheProvisionalRecordIsTakenBack() {
      database.begin();
      assertRefusedWith(() -> database.command("opencypher",
          "CREATE (v:V {k: 17}) WITH v MATCH (m:T {k: 1}), (n:T {k: 2}) CREATE (m)-[:R {w: 1}]->(n)").close(), ValidationException.class);

      database.commit();

      assertThat(count("V")).isZero();
      assertThat(count("R")).isZero();
      // THE VERTICES CAN TAKE AN EDGE AGAIN: THE POINTERS TO A SEGMENT THAT NO LONGER EXISTS ARE GONE
      database.transaction(() -> database.command("opencypher", "MATCH (m:T {k: 1}), (n:T {k: 2}) CREATE (m)-[:R {w: 2}]->(n)").close());
      assertThat(count("R")).isEqualTo(1L);
      assertDatabaseIsConsistent();
    }

    /** A record the statement deletes is given back with the rest of the statement. */
    @Test
    void aDeleteOfAnExistingRecordBesideTheTakenBackRecordIsTakenBack() {
      database.begin();
      assertRefusedWith(() -> database.command("opencypher", "CREATE (v:V {k: 16}) WITH v MATCH (m:T {k: 2}) DELETE m").close(),
          ValidationException.class);

      database.commit();

      try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM T WHERE k = 2")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
      }
      assertThat(count("V")).isZero();
      assertDatabaseIsConsistent();
    }

    /** Every conservative path of the old take-back together: a lightweight edge and a property stand beside a regular edge. */
    @Test
    void aMixedStatementWhoseWritesPartlyWouldStandIsTakenBackWhole() {
      database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");

      database.begin();
      assertRefusedWith(() -> database.command("opencypher",
          "CREATE (v:V {k: 12}) WITH v MATCH (m:T {k: 1}), (n:T {k: 2}) SET m.note = 'x' CREATE (m)-[:L]->(n) CREATE (m)-[:R {w: 1}]->(v)")
          .close(), ValidationException.class);

      database.commit();

      assertThat(count("V")).isZero();
      assertThat(count("R")).isZero();
      try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM T WHERE note = 'x'")) {
        assertThat(rs.next().<Long>getProperty("c")).isZero();
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH (:T)-[r:L]->(:T) RETURN count(r) AS c")) {
        assertThat(rs.next().<Long>getProperty("c")).isZero();
      }
      assertDatabaseIsConsistent();
    }

    /** A statement after one that was taken back journals as if nothing had happened. */
    @Test
    void aStatementAfterOneThatWasTakenBackIsJournaledAgain() {
      database.begin();
      assertRefusedWith(() -> database.command("opencypher",
          "CREATE (v:V {k: 13}) WITH v UNWIND range(1, 5000) AS i CREATE (v)-[:R {w: i}]->(v)").close(), ValidationException.class);
      database.rollback();

      database.begin();
      database.command("sql", "UPDATE U SET s = 'A' WHERE u = 1").close();
      assertRefusedWith(() -> database.command("opencypher", "CREATE (v:V {k: 14})-[:R {w: 1}]->(v)").close(), ValidationException.class);
      database.commit();

      assertThat(rowOf(1)).isEqualTo("w=0,s=A");
      assertThat(count("V")).isZero();
      assertThat(count("R")).isZero();
    }

    @Test
    void aPropertyWrittenOnAnExistingRecordBesideTheTakenBackEdgeIsTakenBack() {
      database.begin();
      assertRefusedWith(() -> database.command("opencypher",
          "MATCH (m:T {k: 1}) SET m.note = 'x' CREATE (m)-[:R {w: 1}]->(:V {k: 9})").close(), ValidationException.class);

      database.commit();

      try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM T WHERE note = 'x'")) {
        assertThat(rs.next().<Long>getProperty("c")).isZero();
      }
      assertDatabaseIsConsistent();
    }

    private void assertDatabaseIsConsistent() {
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
        assertThat(rs.next().<Number>getProperty("totalErrors").longValue()).isZero();
      }
    }

    // ---- helpers ----

    /** The statement fails, and what it fails with - under whatever its language wraps it in - is one of {@code causes}. */
    private static void assertRefusedWith(final ThrowingCallable statement, final Class<?>... causes) {
      final Throwable thrown = catchThrowable(statement);
      assertThat(thrown).as("the statement failed").isNotNull();
      Throwable root = thrown;
      while (root.getCause() != null && root.getCause() != root)
        root = root.getCause();
      assertThat(root).isInstanceOfAny(causes);
    }

    private String rowOf(final int u) {
      try (final ResultSet rs = database.query("sql", "SELECT w, s FROM U WHERE u = " + u)) {
        final var row = rs.next();
        return "w=" + row.getProperty("w") + ",s=" + row.getProperty("s");
      }
    }

    private long count(final String type) {
      try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM " + type)) {
        return rs.next().<Long>getProperty("c");
      }
    }
  }

  /**
   * Regression for #11024: a property write is classified as such on both arms of the update.
   * <p>
   * Issue #11024: a statement that takes its provisional records back gives back the writes it counted beside them only when they
   * assigned no property, and decides that from {@code MutableDocument.arePropertiesAssigned()}. The asynchronous (write-now) arm of
   * the update wrote the page first, which clears the flag on the caller's instance, and asked it after: a property write was
   * classified property-less, given back, and the commit the property should have refused was allowed. Both arms are driven here
   * with the savepoint budget at one entry, so the statement cannot be taken back and the counter is the only guard, as in
   * {@code StatementGuardTest.StatementGuardGaps.aStatementThatWritesPastTheLimitOfTheSavepointKeepsTheCommitRefused}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PropertyWriteClassification extends TestHelper {
    private AfterRecordCreateListener listener;

    @BeforeEach
    void createData() {
      database.command("sql", "CREATE VERTEX TYPE T");
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE R");
      database.command("sql", "CREATE PROPERTY V.p STRING");
      database.command("sql", "ALTER PROPERTY V.p MANDATORY true");
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX T SET k = 1").close();
        database.command("sql", "CREATE VERTEX T SET k = 2").close();
      });
      database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES, 1);
    }

    @AfterEach
    void restore() {
      if (listener != null)
        database.getEvents().unregisterListener(listener);
      database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES,
          GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES.getDefValue());
    }

    /** The deferred arm: the property is set on an existing record while the provisional one is pending. */
    @Test
    void aPropertyWrittenWhileTheProvisionalRecordIsPendingStillRefusesTheCommit() {
      database.begin();
      assertStatementFails(() -> database.command("opencypher",
          "CREATE (v:V {k: 15}) WITH v MATCH (m:T {k: 1}) SET m.note = 'x' CREATE (m)-[:R {w: 1}]->(v)").close());

      assertCommitRefusedAndNothingStands();
    }

    /**
     * The write-now arm, the one {@code DatabaseAsyncUpdateRecord} takes: a property written to another record while the provisional
     * one is pending. With {@code discardRecordAfter} false the page write hands the instance its new buffer, which clears the flag;
     * with it true the flag survives. Both must refuse the commit.
     */
    @ParameterizedTest(name = "discardRecordAfter={0}")
    @ValueSource(booleans = { false, true })
    void aPropertyWrittenNowWhileTheProvisionalRecordIsPendingStillRefusesTheCommit(final boolean discardRecordAfter) {
      final boolean[] fired = new boolean[1];
      listener = record -> {
        if (fired[0] || !record.asDocument().getTypeName().equals("V"))
          return;
        fired[0] = true;
        try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE k = 1")) {
          final MutableDocument other = rs.next().getElement().orElseThrow().modify();
          other.set("note", "x");
          assertThat(((DatabaseInternal) database).updateRecordWithEventsNoLock(other, discardRecordAfter)).isTrue();
        }
      };
      database.getEvents().registerListener(listener);

      database.begin();
      // V.p is mandatory and never given: the scope deletes the provisional vertex and the statement fails
      assertStatementFails(() -> database.command("opencypher", "CREATE (v:V {k: 15}) RETURN v").close());
      assertThat(fired[0]).as("the listener wrote the property while the vertex was provisional").isTrue();

      assertCommitRefusedAndNothingStands();
    }

    /**
     * A list changed in place assigns no property, so {@code arePropertiesAssigned()} stays false, but the update changes the record's
     * content all the same: it is no edge-list write, and must keep the commit refused on both arms (issue #11024 review).
     */
    @ParameterizedTest(name = "writeNow={0}")
    @ValueSource(booleans = { false, true })
    void aListChangedInPlaceWhileTheProvisionalRecordIsPendingStillRefusesTheCommit(final boolean writeNow) {
      database.transaction(() -> database.command("sql", "UPDATE T SET tags = ['a'] WHERE k = 1").close());

      final boolean[] fired = new boolean[1];
      listener = record -> {
        if (fired[0] || !record.asDocument().getTypeName().equals("V"))
          return;
        fired[0] = true;
        try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE k = 1")) {
          final MutableDocument other = rs.next().getElement().orElseThrow().modify();
          final List<Object> tags = other.getList("tags");
          tags.add("x");
          assertThat(other.arePropertiesAssigned()).as("an in-place change assigns no property").isFalse();
          if (writeNow)
            assertThat(((DatabaseInternal) database).updateRecordWithEventsNoLock(other, false)).isTrue();
          else
            other.save();
        }
      };
      database.getEvents().registerListener(listener);

      database.begin();
      assertStatementFails(() -> database.command("opencypher", "CREATE (v:V {k: 15}) RETURN v").close());
      assertThat(fired[0]).isTrue();

      assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
      if (database.isTransactionActive())
        database.rollback();
      try (final ResultSet rs = database.query("sql", "SELECT tags FROM T WHERE k = 1")) {
        assertThat(rs.next().<List<Object>>getProperty("tags")).containsExactly("a");
      }
    }

    /** The control of the write-now arm: an update that assigns no property is still given back with the statement. */
    @Test
    void aStatementWhoseOnlyWritesWereTakenBackStillCommits() {
      database.begin();
      assertStatementFails(() -> database.command("opencypher", "CREATE (v:V {k: 15}) RETURN v").close());
      database.commit();

      assertThat(count("V")).isZero();
    }

    private void assertCommitRefusedAndNothingStands() {
      assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
      if (database.isTransactionActive())
        database.rollback();

      try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM T WHERE note = 'x'")) {
        assertThat(rs.next().<Long>getProperty("c")).isZero();
      }
      assertThat(count("V")).isZero();
      assertThat(count("R")).isZero();
    }

    private static void assertStatementFails(final Runnable statement) {
      assertThat(catchThrowable(statement::run)).as("the statement failed").isNotNull();
    }

    private long count(final String type) {
      try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM " + type)) {
        return rs.next().<Long>getProperty("c");
      }
    }
  }
}
