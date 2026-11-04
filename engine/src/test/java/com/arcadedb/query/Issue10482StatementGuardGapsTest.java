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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
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
class Issue10482StatementGuardGapsTest extends TestHelper {
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
