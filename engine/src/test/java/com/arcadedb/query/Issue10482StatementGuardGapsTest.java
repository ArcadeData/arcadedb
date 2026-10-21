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
 * Issue #10482: two gaps in the guard that stops a caller from committing half of a failed statement (#10194, #10379).
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

  /** The refused value must never reach the disk, and the commit that would have published it is refused instead. */
  @ParameterizedTest(name = "{0}: {2}")
  @MethodSource("refusedSecondUpdates")
  void aRefusedUpdateOfARecordAlreadyUpdatedByTheTransactionIsNotCommitted(final String language, final String first,
      final String second, final boolean byListener) {
    database.begin();
    database.command(language, first).close();
    refuse = byListener;
    try {
      assertRefusedWith(() -> database.command(language, second).close(), ValidationException.class, IllegalStateException.class);
    } finally {
      refuse = false;
    }

    assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
    database.rollback();

    assertThat(rowOf(1)).isEqualTo("w=0,s=s");
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

  /** The take-back gives back what the statement did to the records it took back, and nothing else. */
  @Test
  void anEdgeBetweenTwoExistingRecordsThatStandsStillRefusesTheCommit() {
    database.begin();
    database.command("sql", "UPDATE U SET s = 'A' WHERE u = 1").close();
    // the edge between the two T stays when the V created next to it is taken back at the end of the statement
    assertRefusedWith(() -> database.command("opencypher",
        "MATCH (m:T {k: 1}), (n:T {k: 2}) CREATE (m)-[:R {w: 1}]->(n) CREATE (:V {k: 8})").close(), ValidationException.class);

    assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
    database.rollback();

    assertThat(count("R")).isZero();
    assertThat(rowOf(1)).isEqualTo("w=0,s=s");
  }

  /**
   * A lightweight edge is an entry in the edge lists of its two vertices and no record: the one between two existing vertices stands
   * when the provisional record is taken back, and the writes counted for the vertices are what refuses the commit.
   */
  @Test
  void aLightweightEdgeBetweenTwoExistingRecordsThatStandsStillRefusesTheCommit() {
    database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
    // the two vertices have an edge list already, so the lightweight edge only appends to it: updates that assign no property
    database.command("opencypher", "MATCH (m:T {k: 1}), (n:T {k: 2}) CREATE (m)-[:R {w: 0}]->(n)").close();

    database.begin();
    database.command("sql", "UPDATE U SET s = 'A' WHERE u = 1").close();
    assertRefusedWith(() -> database.command("opencypher",
        "CREATE (v:V {k: 10}) WITH v MATCH (m:T {k: 1}), (n:T {k: 2}) CREATE (m)-[:L]->(n)").close(), ValidationException.class);

    assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
    database.rollback();

    assertThat(rowOf(1)).isEqualTo("w=0,s=s");
    try (final ResultSet rs = database.query("opencypher", "MATCH (:T)-[r:L]->(:T) RETURN count(r) AS c")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
    }
  }

  /**
   * The journal follows a bounded number of records. A statement that writes more beside its provisional record than it holds gives it
   * up and gives back nothing but what the provisional record itself counted, which keeps the commit refused rather than risk giving
   * back a write that stands.
   */
  @Test
  void aStatementThatWritesPastTheLimitOfTheJournalKeepsTheCommitRefused() {
    database.begin();
    database.command("sql", "UPDATE U SET s = 'A' WHERE u = 1").close();
    // 5,000 edges is more records than the journal follows; all of them are taken back with the vertex they leave from and arrive at
    assertRefusedWith(() -> database.command("opencypher",
        "CREATE (v:V {k: 11}) WITH v UNWIND range(1, 5000) AS i CREATE (v)-[:R {w: i}]->(v)").close(), ValidationException.class);

    assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
    database.rollback();

    assertThat(rowOf(1)).isEqualTo("w=0,s=s");
    assertThat(count("V")).isZero();
    assertThat(count("R")).isZero();
  }

  /**
   * The property is written while the provisional record is pending, so the journal sees the write: it is a property assigned, not the
   * maintenance of an edge list, and it stays when the edge that was taken back is gone.
   */
  @Test
  void aPropertyWrittenWhileTheProvisionalRecordIsPendingStillRefusesTheCommit() {
    database.begin();
    assertRefusedWith(() -> database.command("opencypher",
        "CREATE (v:V {k: 15}) WITH v MATCH (m:T {k: 1}) SET m.note = 'x' CREATE (m)-[:R {w: 1}]->(v)").close(), ValidationException.class);

    assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
    database.rollback();

    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM T WHERE note = 'x'")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
    }
    assertThat(count("V")).isZero();
    assertThat(count("R")).isZero();
  }

  /**
   * The edge between two existing vertices that have no edge list yet makes the statement create their edge segments, while the
   * provisional record is pending: they hold the edge that stands, so they are not empty and nothing of them is given back.
   */
  @Test
  void aStandingEdgeSegmentCreatedBesideTheProvisionalRecordIsNeverGivenBack() {
    database.begin();
    assertRefusedWith(() -> database.command("opencypher",
        "CREATE (v:V {k: 17}) WITH v MATCH (m:T {k: 1}), (n:T {k: 2}) CREATE (m)-[:R {w: 1}]->(n)").close(), ValidationException.class);

    assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
    database.rollback();

    assertThat(count("V")).isZero();
    assertThat(count("R")).isZero();
  }

  /** A record the statement deletes is a write that stands, whatever happens to the provisional record created beside it. */
  @Test
  void aDeleteOfAnExistingRecordBesideTheTakenBackRecordStillRefusesTheCommit() {
    database.begin();
    assertRefusedWith(() -> database.command("opencypher", "CREATE (v:V {k: 16}) WITH v MATCH (m:T {k: 2}) DELETE m").close(),
        ValidationException.class);

    assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
    database.rollback();

    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM T WHERE k = 2")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
    }
    assertThat(count("V")).isZero();
  }

  /** The conservative paths together: a lightweight edge and a property on existing vertices stand beside a regular edge that is taken back. */
  @Test
  void aMixedStatementWhoseWritesPartlyStandStillRefusesTheCommit() {
    database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");

    database.begin();
    assertRefusedWith(() -> database.command("opencypher",
        "CREATE (v:V {k: 12}) WITH v MATCH (m:T {k: 1}), (n:T {k: 2}) SET m.note = 'x' CREATE (m)-[:L]->(n) CREATE (m)-[:R {w: 1}]->(v)")
        .close(), ValidationException.class);

    assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
    database.rollback();

    assertThat(count("V")).isZero();
    assertThat(count("R")).isZero();
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM T WHERE note = 'x'")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
    }
  }

  /** What a statement that crossed the limit of the journal left in the scope is gone with it: the next statement journals again. */
  @Test
  void aStatementAfterOneThatCrossedTheLimitOfTheJournalIsJournaledAgain() {
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
  void aPropertyWrittenOnAnExistingRecordBesideTheTakenBackEdgeStillRefusesTheCommit() {
    database.begin();
    assertRefusedWith(() -> database.command("opencypher",
        "MATCH (m:T {k: 1}) SET m.note = 'x' CREATE (m)-[:R {w: 1}]->(:V {k: 9})").close(), ValidationException.class);

    assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
    database.rollback();

    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM T WHERE note = 'x'")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
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
