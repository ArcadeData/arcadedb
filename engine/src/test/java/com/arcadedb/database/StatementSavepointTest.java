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
package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.IntHashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10346: a statement that fails inside a transaction the caller keeps is taken back on its own, and the transaction goes on
 * as if the statement had never run, instead of being refused its commit. What the statement left in the transaction has to be
 * gone from every place the transaction is read: the records, the indexes, the counters and the pages.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StatementSavepointTest extends TestHelper {

  @BeforeEach
  void createType() {
    database.command("sql", "CREATE DOCUMENT TYPE T").close();
    database.command("sql", "CREATE PROPERTY T.id LONG").close();
    database.command("sql", "CREATE PROPERTY T.k INTEGER").close();
    database.command("sql", "CREATE INDEX ON T (k) UNIQUE").close();
    database.command("sql", "CREATE INDEX ON T (id) NOTUNIQUE").close();
  }

  private List<Long> ids() {
    final List<Long> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM T ORDER BY id")) {
      while (rs.hasNext())
        ids.add(rs.next().<Number>getProperty("id").longValue());
    }
    return ids;
  }

  private long countStar() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM T")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private List<Long> idsByIndexedId(final long id) {
    final List<Long> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM T WHERE id = ?", id)) {
      while (rs.hasNext())
        ids.add(rs.next().<Number>getProperty("id").longValue());
    }
    return ids;
  }

  private long idByUniqueK(final int k) {
    try (final ResultSet rs = database.query("sql", "SELECT id FROM T WHERE k = ?", k)) {
      return rs.hasNext() ? rs.next().<Number>getProperty("id").longValue() : -1L;
    }
  }

  private void assertDatabaseIsConsistent() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result result = rs.next();
      assertThat(result.<Number>getProperty("totalErrors").intValue()).as("CHECK DATABASE: %s", result).isZero();
    }
  }

  @Test
  void aMultiRowInsertThatFailsMidwayIsTakenBackAndTheTransactionGoesOn() {
    database.begin();
    database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO T (id, k) VALUES (2, 2), (3, 3), (4, 1)").close()).isInstanceOf(
        DuplicatedKeyException.class);

    // THE ROWS THE FAILED STATEMENT WROTE BEFORE ITS ERROR ARE GONE, FROM THE RECORDS AND FROM BOTH INDEXES
    assertThat(ids()).containsExactly(1L);
    assertThat(countStar()).isEqualTo(1L);
    assertThat(idByUniqueK(2)).isEqualTo(-1L);
    assertThat(idByUniqueK(3)).isEqualTo(-1L);
    assertThat(idsByIndexedId(2)).isEmpty();

    // AND THE TRANSACTION GOES ON, REUSING THE KEYS THE FAILED STATEMENT HAD CLAIMED
    database.command("sql", "INSERT INTO T (id, k) VALUES (2, 2), (3, 3)").close();
    database.commit();

    assertThat(ids()).containsExactly(1L, 2L, 3L);
    assertThat(countStar()).isEqualTo(3L);
    assertThat(idByUniqueK(3)).isEqualTo(3L);
    assertDatabaseIsConsistent();
  }

  @Test
  void aTransactionWhoseStatementWasTakenBackCommitsWhatIsLeft() {
    database.begin();
    database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
    try {
      database.command("sql", "INSERT INTO T (id, k) VALUES (2, 2), (3, 1)").close();
    } catch (final DuplicatedKeyException e) {
      // CARRIED ON
    }
    database.commit();

    assertThat(ids()).containsExactly(1L);
    assertDatabaseIsConsistent();
  }

  @Test
  void theLimitOfTheJournalFallsBackToRefusingTheCommit() {
    database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES, 4);
    try {
      database.begin();
      try {
        database.command("sql", "INSERT INTO T (id, k) VALUES (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 2)").close();
      } catch (final DuplicatedKeyException e) {
        // CARRIED ON
      }
      assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
      database.rollback();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES,
          GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES.getDefValue());
    }
    assertThat(ids()).isEmpty();
  }

  @Test
  void aStatementWhoseIndexLaneWasDroppedSaysWhyItCannotBeTakenBack() {
    final DatabaseInternal db = (DatabaseInternal) database;
    database.begin();
    final TransactionContext tx = db.getTransaction();
    final StatementSavepoint savepoint = tx.beginStatement();
    assertThat(savepoint).isNotNull();

    database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
    // A LANE THE STATEMENT QUEUED TO IS DROPPED UNDER IT: THE JOURNAL NO LONGER DESCRIBES THE LANES
    tx.getIndexChanges().removeIndex(database.getSchema().getIndexByName("T[k]").getName());

    assertThat(tx.rollbackStatement(savepoint)).isFalse();
    assertThat(savepoint.getUnrestorableReason()).contains("index lane");
    tx.endStatement(savepoint);
    database.rollback();
  }

  @Test
  void theScanForIncomingEdgesOfAStatementTakenBackIsTakenBackToo() {
    createGraphSchema();
    database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL").close();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO V (id) VALUES (1), (2)").close();
      database.command("sql", "CREATE EDGE U FROM (SELECT FROM V WHERE id = 1) TO (SELECT FROM V WHERE id = 2)").close();
    });

    // THE SECOND VERTEX THE STATEMENT DELETES IS REFUSED, WHICHEVER IT IS
    final AtomicInteger deletes = new AtomicInteger();
    database.getSchema().getType("V").getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      if (deletes.incrementAndGet() == 2)
        throw new IllegalStateException("refused");
      return true;
    });

    final DatabaseInternal db = (DatabaseInternal) database;
    final int vertexBucket = database.getSchema().getType("V").getBuckets(false).get(0).getFileId();
    database.begin();
    // THE DELETE OF THE FIRST VERTEX SCANS FOR THE UNIDIRECTIONAL EDGES ENDING IN IT, THEN THE SECOND ONE IS REFUSED
    assertThatThrownBy(() -> database.command("sql", "DELETE VERTEX FROM V WHERE id IN [1, 2]").close()).hasStackTraceContaining("refused");
    assertThat(deletes.get()).as("the first vertex was deleted, the second one refused").isEqualTo(2);
    assertThat(ids("V")).containsExactly(1L, 2L);
    // SOMETHING TO COMMIT: A TRANSACTION WITH NOTHING TO WRITE IS NOT CHECKED AT ALL
    database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();

    // ANOTHER TRANSACTION PUBLISHES AN EDGE ENDING IN THE BUCKET: THE DELETE THE TRANSACTION NO LONGER HOLDS HAS NOTHING TO BE REFUSED FOR
    final IntHashSet buckets = new IntHashSet();
    buckets.add(vertexBucket);
    db.getTransactionManager().unidirectionalEdgesCommitted(buckets);

    database.commit();
    assertThat(ids("V")).containsExactly(1L, 2L);
  }

  private List<Long> ids(final String type) {
    final List<Long> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM " + type + " ORDER BY id")) {
      while (rs.hasNext())
        ids.add(rs.next().<Number>getProperty("id").longValue());
    }
    return ids;
  }

  @Test
  void savepointsCanBeDisabled() {
    database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES, 0);
    try {
      database.begin();
      try {
        database.command("sql", "INSERT INTO T (id, k) VALUES (2, 2), (3, 2)").close();
      } catch (final DuplicatedKeyException e) {
        // CARRIED ON
      }
      assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
      database.rollback();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES,
          GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES.getDefValue());
    }
  }

  @Test
  void aDeleteWhoseAfterListenerFailsIsTakenBack() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
      database.command("sql", "INSERT INTO T SET id = 2, k = 2").close();
    });
    database.getSchema().getType("T").getEvents().registerListener((AfterRecordDeleteListener) record -> {
      throw new IllegalStateException("cascade failed");
    });

    database.begin();
    assertThatThrownBy(() -> database.command("sql", "DELETE FROM T WHERE id = 1").close()).hasStackTraceContaining("cascade failed");
    // THE DELETE WAS APPLIED BEFORE ITS LISTENER FAILED, AND IS GIVEN BACK
    assertThat(ids()).containsExactly(1L, 2L);
    database.commit();
    assertThat(ids()).containsExactly(1L, 2L);
  }

  @Test
  void aDeleteWhoseAfterListenerFailsAndCannotBeTakenBackRefusesTheCommit() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
      database.command("sql", "INSERT INTO T SET id = 2, k = 2").close();
    });
    database.getSchema().getType("T").getEvents().registerListener((AfterRecordDeleteListener) record -> {
      throw new IllegalStateException("cascade failed");
    });

    // THE JOURNAL HOLDS ONE ENTRY: THE DELETE CHANGES MORE THAN THAT, SO IT CANNOT BE GIVEN BACK
    database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES, 1);
    try {
      database.begin();
      assertThatThrownBy(() -> database.command("sql", "DELETE FROM T WHERE id = 1").close()).hasStackTraceContaining("cascade failed");
      // THE LISTENER FAILED AFTER THE ROW WAS DELETED, BEFORE THE DELETE WAS COUNTED AS COMPLETED: A COMMIT WOULD PUBLISH IT
      assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
      database.rollback();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES,
          GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES.getDefValue());
    }
    assertThat(ids()).containsExactly(1L, 2L);
  }

  @Test
  void aVertexAndAnEdgeAnEarlierStatementUpdatedComeBackAsThatStatementLeftThem() {
    database.command("sql", "CREATE VERTEX TYPE VV").close();
    database.command("sql", "CREATE EDGE TYPE EE").close();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO VV (id, name) VALUES (1, 'x'), (2, 'x'), (3, 'x')").close();
      database.command("sql", "CREATE EDGE EE FROM (SELECT FROM VV WHERE id = 1) TO (SELECT FROM VV WHERE id IN [2, 3]) SET label = 'x'")
          .close();
    });

    // THE SECOND UPDATE OF A STATEMENT IS REFUSED ONCE THE LISTENER IS ARMED
    final AtomicInteger updates = new AtomicInteger();
    final AtomicInteger refuseAt = new AtomicInteger(-1);
    final BeforeRecordUpdateListener refuseTheNth = record -> {
      if (updates.incrementAndGet() == refuseAt.get())
        throw new IllegalStateException("refused");
      return true;
    };
    database.getSchema().getType("VV").getEvents().registerListener(refuseTheNth);
    database.getSchema().getType("EE").getEvents().registerListener(refuseTheNth);

    database.begin();
    // THE EARLIER STATEMENTS QUEUE THE VERTICES AND THE EDGES FOR THE COMMIT WITH THEIR NEW CONTENT
    database.command("sql", "UPDATE VV SET name = 'a'").close();
    database.command("sql", "UPDATE EE SET label = 'a'").close();

    updates.set(0);
    refuseAt.set(2);
    assertThatThrownBy(() -> database.command("sql", "UPDATE VV SET name = 'b'").close()).hasStackTraceContaining("refused");
    updates.set(0);
    assertThatThrownBy(() -> database.command("sql", "UPDATE EE SET label = 'b'").close()).hasStackTraceContaining("refused");
    refuseAt.set(-1);

    assertThat(valuesOf("VV", "name")).containsExactly("a", "a", "a");
    assertThat(valuesOf("EE", "label")).containsExactly("a", "a");
    database.commit();
    assertThat(valuesOf("VV", "name")).containsExactly("a", "a", "a");
    assertThat(valuesOf("EE", "label")).containsExactly("a", "a");
  }

  private List<String> valuesOf(final String type, final String property) {
    final List<String> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT " + property + " FROM " + type)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(property));
    }
    return values;
  }

  @Test
  void aStatementThatOverwroteMoreGranulesThanTheBudgetIsRefusedBeforeAnyPageIsRestored() {
    database.command("sql", "CREATE DOCUMENT TYPE W BUCKETS 2").close();
    database.command("sql", "CREATE PROPERTY W.id LONG").close();
    database.command("sql", "CREATE PROPERTY W.payload STRING").close();
    database.command("sql", "CREATE INDEX ON W (id) UNIQUE").close();
    final String big = "x".repeat(3000);

    database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES, 8);
    try {
      database.begin();
      // THE TWO BUCKETS HOLD A PAGE THE TRANSACTION MODIFIED
      database.command("sql", "INSERT INTO W (id, payload) VALUES (1, 'a'), (2, 'a')").close();
      // THE ROWS GO TO BOTH BUCKETS: THE BUDGET RUNS OUT ON THE SECOND PAGE, WHILE THE LOG OF THE FIRST HOLDS GRANULES
      assertThatThrownBy(() -> database.command("sql",
          "INSERT INTO W (id, payload) VALUES (3, '" + big + "'), (4, '" + big + "'), (5, '" + big + "'), (6, '" + big + "'), (1, 'clash')")
          .close()).isInstanceOf(DuplicatedKeyException.class);
      // THE REASON IS THE BUDGET, NOT A RESTORE THAT STOPPED HALFWAY: NO PAGE WAS TOUCHED
      assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class)
          .hasMessageContaining("could not be taken back").hasMessageContaining("granules");
      database.rollback();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES,
          GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES.getDefValue());
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM W")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  @Test
  void aCommitAfterAStatementWasTakenBackSurvivesACrash() {
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
      database.command("sql", "INSERT INTO T SET id = 2, k = 2").close();
    });

    database.begin();
    database.command("sql", "INSERT INTO T SET id = 3, k = 3").close();
    // THE FIRST ROW IS WRITTEN TO THE PAGE THE TRANSACTION HOLDS AND TAKEN BACK, THE ROWS AFTER IT GO ON TOP
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO T (id, k) VALUES (4, 4), (5, 4)").close()).isInstanceOf(
        DuplicatedKeyException.class);
    database.command("sql", "INSERT INTO T SET id = 6, k = 6").close();
    database.command("sql", "UPDATE T SET k = k + 100 WHERE id = 1").close();
    database.commit();

    // NOTHING IS FLUSHED: THE WAL IS WHAT BRINGS THE TRANSACTION BACK
    ((DatabaseInternal) database).kill();
    database.close();
    final AtomicBoolean notClosed = new AtomicBoolean();
    factory.registerCallback(DatabaseInternal.CALLBACK_EVENT.DB_NOT_CLOSED, () -> {
      notClosed.set(true);
      return null;
    });
    database = factory.open();
    assertThat(notClosed.get()).isTrue();

    assertThat(ids()).containsExactly(1L, 2L, 3L, 6L);
    assertThat(idsByIndexedId(4)).isEmpty();
    assertThat(idsByIndexedId(5)).isEmpty();
    try (final ResultSet rs = database.query("sql", "SELECT k FROM T WHERE id = 1")) {
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(101);
    }
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      assertThat(rs.next().<Number>getProperty("totalErrors").intValue()).isZero();
    }
  }

  @Test
  void aMultiRowUpdateThatFailsMidwayIsTakenBack() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
      database.command("sql", "INSERT INTO T SET id = 2, k = 2").close();
      database.command("sql", "INSERT INTO T SET id = 3, k = 5").close();
    });

    database.begin();
    assertThatThrownBy(() -> database.command("sql", "UPDATE T SET k = 5 WHERE id < 3").close()).isInstanceOf(
        DuplicatedKeyException.class);

    assertThat(idByUniqueK(1)).isEqualTo(1L);
    assertThat(idByUniqueK(2)).isEqualTo(2L);
    assertThat(idByUniqueK(5)).isEqualTo(3L);

    // THE TRANSACTION GOES ON: A STATEMENT THAT CAN SUCCEED NOW DOES
    database.command("sql", "UPDATE T SET k = 7 WHERE id = 1").close();
    database.commit();

    assertThat(idByUniqueK(7)).isEqualTo(1L);
    assertThat(idByUniqueK(1)).isEqualTo(-1L);
    assertThat(idByUniqueK(2)).isEqualTo(2L);
    assertThat(idByUniqueK(5)).isEqualTo(3L);
    assertDatabaseIsConsistent();
  }

  @Test
  void aMultiRowDeleteThatFailsMidwayIsTakenBack() {
    database.transaction(() -> {
      for (int i = 1; i <= 5; i++)
        database.command("sql", "INSERT INTO T SET id = ?, k = ?", i, i).close();
    });

    database.getSchema().getType("T").getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      if (((Document) record).getLong("id") == 4L)
        throw new IllegalStateException("refused");
      return true;
    });

    database.begin();
    assertThatThrownBy(() -> database.command("sql", "DELETE FROM T").close()).hasStackTraceContaining("refused");

    assertThat(ids()).containsExactly(1L, 2L, 3L, 4L, 5L);
    assertThat(countStar()).isEqualTo(5L);
    assertThat(idByUniqueK(1)).isEqualTo(1L);
    assertThat(idByUniqueK(3)).isEqualTo(3L);

    database.commit();

    assertThat(ids()).containsExactly(1L, 2L, 3L, 4L, 5L);
    assertThat(countStar()).isEqualTo(5L);
    assertDatabaseIsConsistent();
  }

  @Test
  void thePagesAnEarlierStatementWroteComeBackByteForByte() {
    database.begin();
    // THE FIRST STATEMENT CREATES THE PAGE; THE SECOND ONE WRITES TO A PAGE THE TRANSACTION ALREADY HOLDS
    database.command("sql", "INSERT INTO T (id, k) VALUES (1, 1), (2, 2), (3, 3)").close();

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO T (id, k) VALUES (4, 4), (5, 5), (6, 2)").close()).isInstanceOf(
        DuplicatedKeyException.class);

    assertThat(ids()).containsExactly(1L, 2L, 3L);
    database.command("sql", "INSERT INTO T (id, k) VALUES (4, 4), (5, 5)").close();
    database.commit();

    reopenDatabase();
    assertThat(ids()).containsExactly(1L, 2L, 3L, 4L, 5L);
    assertThat(countStar()).isEqualTo(5L);
    assertDatabaseIsConsistent();
  }

  @Test
  void aStatementThatFilledSeveralPagesLeavesNoneOfThemBehind() {
    database.begin();
    database.command("sql", "INSERT INTO T SET id = 0, k = 0").close();

    final StringBuilder statement = new StringBuilder("INSERT INTO T (id, k) VALUES ");
    for (int i = 1; i <= 3000; i++)
      statement.append(i > 1 ? ", " : "").append('(').append(i).append(", ").append(i).append(')');
    // THE LAST ROW CLAIMS A KEY THE FIRST STATEMENT TOOK
    statement.append(", (9999, 0)");

    assertThatThrownBy(() -> database.command("sql", statement.toString()).close()).isInstanceOf(DuplicatedKeyException.class);

    assertThat(ids()).containsExactly(0L);
    assertThat(countStar()).isEqualTo(1L);

    database.command("sql", "INSERT INTO T (id, k) VALUES (1, 1), (2, 2)").close();
    database.commit();

    reopenDatabase();
    assertThat(ids()).containsExactly(0L, 1L, 2L);
    assertThat(countStar()).isEqualTo(3L);
    assertDatabaseIsConsistent();
  }

  @Test
  void aRecordAnEarlierStatementCreatedAndThisOneChangedIsGivenBack() {
    database.begin();
    database.command("sql", "INSERT INTO T (id, k) VALUES (1, 1), (2, 2)").close();

    assertThatThrownBy(() -> database.command("sql", "UPDATE T SET k = 2 WHERE id <= 2").close()).isInstanceOf(
        DuplicatedKeyException.class);

    assertThat(idByUniqueK(1)).isEqualTo(1L);
    assertThat(idByUniqueK(2)).isEqualTo(2L);
    try (final ResultSet rs = database.query("sql", "SELECT k FROM T ORDER BY id")) {
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(1);
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(2);
    }
    database.commit();

    try (final ResultSet rs = database.query("sql", "SELECT k FROM T ORDER BY id")) {
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(1);
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(2);
    }
    assertDatabaseIsConsistent();
  }

  @Test
  void aStatementThatChangesAgainWhatAnEarlierOneUpdatedGivesBackTheEarlierUpdate() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T (id, k) VALUES (1, 1), (2, 2), (3, 3)").close();
    });

    database.begin();
    // UPDATED, NOT WRITTEN YET: THE OBJECT THE NEXT STATEMENT FINDS IS THE ONLY COPY OF WHAT THIS ONE DID
    database.command("sql", "UPDATE T SET k = 10 WHERE id = 1").close();
    assertThatThrownBy(() -> database.command("sql", "UPDATE T SET k = 3 WHERE id <= 2").close()).isInstanceOf(
        DuplicatedKeyException.class);

    // THE FAILED STATEMENT IS GONE, THE EARLIER ONE STANDS
    assertThat(idByUniqueK(10)).isEqualTo(1L);
    assertThat(idByUniqueK(1)).isEqualTo(-1L);
    assertThat(idByUniqueK(2)).isEqualTo(2L);
    assertThat(idByUniqueK(3)).isEqualTo(3L);
    database.commit();

    assertThat(idByUniqueK(10)).isEqualTo(1L);
    assertThat(idByUniqueK(2)).isEqualTo(2L);
    assertThat(idByUniqueK(3)).isEqualTo(3L);
    assertDatabaseIsConsistent();
  }

  @Test
  void aStatementThatCannotKeepWhatAnEarlierOneUpdatedCannotBeTakenBackAndRefusesTheCommit() {
    // A record with an EXTERNAL property keeps that value in a bucket of its own, which a snapshot of the record does not hold
    database.command("sql", "CREATE PROPERTY T.blob STRING").close();
    database.command("sql", "ALTER PROPERTY T.blob EXTERNAL true").close();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T (id, k, blob) VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c')").close();
    });

    database.begin();
    database.command("sql", "UPDATE T SET k = 10 WHERE id = 1").close();
    try {
      database.command("sql", "UPDATE T SET k = 7 WHERE id <= 2").close();
    } catch (final DuplicatedKeyException e) {
      // CARRIED ON
    }
    assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
    database.rollback();

    assertThat(idByUniqueK(1)).isEqualTo(1L);
  }

  @Test
  void aStatementThatCannotBeTakenBackAndFailsBeforeItCompletedAWriteStillRefusesTheCommit() {
    database.command("sql", "CREATE PROPERTY T.blob STRING").close();
    database.command("sql", "ALTER PROPERTY T.blob EXTERNAL true").close();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T (id, k, blob) VALUES (1, 1, 'a'), (2, 2, 'b')").close();
    });

    database.begin();
    // THE EARLIER STATEMENT QUEUES THE FIRST ROW FOR THE COMMIT, WITH ITS NEW CONTENT
    database.command("sql", "UPDATE T SET k = 10 WHERE id = 1").close();
    final RID first;
    try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE id = 1")) {
      first = rs.next().getIdentity().get();
    }

    // THE NEXT STATEMENT IS REFUSED FOR ANOTHER ROW, AFTER A LISTENER CHANGED THE OBJECT QUEUED FOR THE FIRST ONE (NO WRITE COMPLETED)
    database.getSchema().getType("T").getEvents().registerListener((BeforeRecordUpdateListener) record -> {
      if (((Document) record).getLong("id") == 2L) {
        database.lookupByRID(first, true).asDocument().modify().set("k", 99);
        throw new IllegalStateException("refused");
      }
      return true;
    });
    assertThatThrownBy(() -> database.command("sql", "UPDATE T SET k = 20 WHERE id = 2").close()).hasStackTraceContaining("refused");

    // A RECORD WITH AN EXTERNAL PROPERTY CANNOT BE PUT BACK, SO THE COMMIT WOULD STORE WHAT THE LISTENER LEFT ON IT
    assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed")
        .hasMessageContaining("could not be taken back");
    database.rollback();

    assertThat(idByUniqueK(1)).isEqualTo(1L);
  }

  @Test
  void aStatementThatCannotBeTakenBackForAnotherReasonAndReadsAQueuedRecordRefusesTheCommit() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T (id, k) VALUES (1, 1), (2, 2)").close();
    });

    database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES, 2);
    try {
      database.begin();
      // THREE RECORDS THE TRANSACTION WROTE AND LEFT CLEAN: A STATEMENT THAT READS THEM ALL IS PAST THE BUDGET OF ITS SAVEPOINT
      final List<RID> clean = new ArrayList<>();
      for (int i = 10; i < 13; i++)
        clean.add(database.newDocument("T").set("id", (long) i).set("k", i).save().getIdentity());
      // THE EARLIER STATEMENT QUEUES THE FIRST ROW FOR THE COMMIT
      database.command("sql", "UPDATE T SET k = 100 WHERE id = 1").close();
      final RID first;
      try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE id = 1")) {
        first = rs.next().getIdentity().get();
      }

      // THE LISTENER READS THE CLEAN RECORDS (THE SAVEPOINT GIVES UP), THEN CHANGES THE QUEUED OBJECT OF THE FIRST ROW, THEN REFUSES
      database.getSchema().getType("T").getEvents().registerListener((BeforeRecordUpdateListener) record -> {
        if (((Document) record).getLong("id") == 2L) {
          for (final RID rid : clean)
            database.lookupByRID(rid, true);
          database.lookupByRID(first, true).asDocument().modify().set("k", 99);
          throw new IllegalStateException("refused");
        }
        return true;
      });
      assertThatThrownBy(() -> database.command("sql", "UPDATE T SET k = 20 WHERE id = 2").close()).hasStackTraceContaining("refused");

      assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed")
          .hasMessageContaining("could not be taken back");
      database.rollback();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES,
          GlobalConfiguration.TX_STATEMENT_SAVEPOINT_MAX_ENTRIES.getDefValue());
    }
    assertThat(idByUniqueK(1)).isEqualTo(1L);
  }

  @Test
  void aCallbackAFailedStatementRegisteredForTheCommitIsTakenBackWithIt() {
    final AtomicInteger published = new AtomicInteger();
    database.getSchema().getType("T").getEvents().registerListener((BeforeRecordCreateListener) record -> {
      if (((Document) record).getLong("id") == 5L) {
        // WORK FOR THE COMMIT, REGISTERED BEFORE THE RECORD IS WRITTEN, AND THEN THE CREATE IS REFUSED
        ((DatabaseInternal) database).getTransaction().addAfterCommitCallback(published::incrementAndGet);
        throw new IllegalStateException("refused");
      }
      return true;
    });

    database.begin();
    database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO T SET id = 5, k = 5").close()).hasStackTraceContaining("refused");
    database.command("sql", "INSERT INTO T SET id = 2, k = 2").close();
    database.commit();

    assertThat(published.get()).as("the callback of the statement that failed").isZero();
    assertThat(ids()).containsExactly(1L, 2L);
  }

  @Test
  void aStatementRunByAnotherIsTakenBackWithIt() {
    database.command("sql", "CREATE DOCUMENT TYPE Log").close();
    database.command("sql", "CREATE PROPERTY Log.n INTEGER").close();
    database.command("sql", "CREATE INDEX ON Log (n) UNIQUE").close();

    // The listener runs a statement of its own in the transaction of the one that fired it, and that one fails midway
    database.getSchema().getType("T").getEvents().registerListener((AfterRecordCreateListener) record -> {
      if (((Document) record).getLong("id") == 2L)
        database.command("sql", "INSERT INTO Log (n) VALUES (1), (2), (2)").close();
    });

    database.begin();
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO T (id, k) VALUES (1, 1), (2, 2)").close()).hasStackTraceContaining(
        "Duplicated key");

    assertThat(ids()).isEmpty();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Log")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }

    database.commit();
    assertThat(ids()).isEmpty();
  }

  @Test
  void aFailureOfAnotherStatementTheOuterOneSwallowedRefusesTheCommit() {
    database.command("sql", "CREATE DOCUMENT TYPE Log").close();
    database.command("sql", "CREATE PROPERTY Log.n INTEGER").close();
    database.command("sql", "CREATE INDEX ON Log (n) UNIQUE").close();

    database.getSchema().getType("T").getEvents().registerListener((AfterRecordCreateListener) record -> {
      try {
        database.command("sql", "INSERT INTO Log (n) VALUES (1), (2), (2)").close();
      } catch (final DuplicatedKeyException e) {
        // SWALLOWED: THE OUTER STATEMENT SUCCEEDS WITH HALF OF THE INNER ONE IN THE TRANSACTION
      }
    });

    database.begin();
    database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
    assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
    database.rollback();
  }

  @Test
  void aStatementThatOwnsItsTransactionIsRolledBackWholeAsBefore() {
    // WITH AN AUTO-TRANSACTION THE STATEMENT BEGINS ITS OWN TRANSACTION, AND PAYS FOR NO SAVEPOINT: THE WHOLE TRANSACTION GOES
    database.setAutoTransaction(true);
    try {
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO T (id, k) VALUES (1, 1), (2, 2), (3, 1)").close()).isInstanceOf(
          DuplicatedKeyException.class);
    } finally {
      database.setAutoTransaction(false);
    }

    assertThat(ids()).isEmpty();
  }

  @Test
  void manyFailedStatementsInOneTransactionLeaveItConsistent() {
    database.begin();
    database.command("sql", "INSERT INTO T SET id = 100, k = 100").close();
    for (int round = 0; round < 25; round++) {
      final int base = round * 10;
      assertThatThrownBy(() -> database.command("sql",
          "INSERT INTO T (id, k) VALUES (" + (base + 1) + ", " + (base + 1) + "), (" + (base + 2) + ", " + (base + 2) + "), ("
              + (base + 3) + ", 100)").close()).isInstanceOf(DuplicatedKeyException.class);
      database.command("sql", "INSERT INTO T SET id = " + (1000 + round) + ", k = " + (1000 + round)).close();
    }
    database.commit();

    assertThat(countStar()).isEqualTo(26L);
    assertThat(ids()).hasSize(26).contains(100L, 1000L, 1024L).doesNotContain(1L, 2L);
    assertDatabaseIsConsistent();
  }

  // ----------------------------------------------------------------------------------------------------------------------------
  // GRAPH
  // ----------------------------------------------------------------------------------------------------------------------------

  private void createGraphSchema() {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.command("sql", "CREATE PROPERTY V.id LONG").close();
    database.command("sql", "CREATE INDEX ON V (id) UNIQUE").close();
    database.command("sql", "CREATE EDGE TYPE E").close();
    database.command("sql", "CREATE PROPERTY E.w INTEGER").close();
    database.command("sql", "CREATE INDEX ON E (w) UNIQUE").close();
  }

  private String degrees() {
    final StringBuilder result = new StringBuilder();
    try (final ResultSet rs = database.query("sql", "SELECT id, outE().size() AS o, inE().size() AS i FROM V ORDER BY id")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        result.append(row.<Object>getProperty("id")).append(':').append(row.<Object>getProperty("o")).append('/')
            .append(row.<Object>getProperty("i")).append(' ');
      }
    }
    return result.toString().trim();
  }

  private void failingEdgeStatement(final int w) {
    assertThatThrownBy(() -> database.command("sql",
        "CREATE EDGE E FROM (SELECT FROM V WHERE id = 1) TO (SELECT FROM V WHERE id IN [2, 3]) SET w = " + w).close()).isInstanceOf(
        DuplicatedKeyException.class);
  }

  @Test
  void edgesOfAFailedStatementBetweenCommittedVerticesAreTakenBack() {
    createGraphSchema();
    database.transaction(() -> database.command("sql", "INSERT INTO V (id) VALUES (1), (2), (3)").close());

    database.begin();
    failingEdgeStatement(10);
    assertThat(degrees()).isEqualTo("1:0/0 2:0/0 3:0/0");
    database.commit();
    assertThat(degrees()).isEqualTo("1:0/0 2:0/0 3:0/0");
    assertDatabaseIsConsistent();
  }

  @Test
  void edgesOfAFailedStatementAppendedToAnExistingEdgeListAreTakenBack() {
    createGraphSchema();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO V (id) VALUES (1), (2), (3)").close();
      database.command("sql", "CREATE EDGE E FROM (SELECT FROM V WHERE id = 1) TO (SELECT FROM V WHERE id = 2) SET w = 1").close();
    });

    database.begin();
    failingEdgeStatement(10);
    assertThat(degrees()).isEqualTo("1:1/0 2:0/1 3:0/0");
    database.commit();
    assertThat(degrees()).isEqualTo("1:1/0 2:0/1 3:0/0");
    assertDatabaseIsConsistent();
  }

  @Test
  void edgesOfAFailedStatementBetweenVerticesTheTransactionCreatedAreTakenBack() {
    createGraphSchema();
    database.begin();
    database.command("sql", "INSERT INTO V (id) VALUES (1), (2), (3)").close();
    failingEdgeStatement(10);
    assertThat(degrees()).isEqualTo("1:0/0 2:0/0 3:0/0");
    database.command("sql", "CREATE EDGE E FROM (SELECT FROM V WHERE id = 1) TO (SELECT FROM V WHERE id = 3) SET w = 11").close();
    database.commit();
    assertThat(degrees()).isEqualTo("1:1/0 2:0/0 3:0/1");
    assertDatabaseIsConsistent();
  }

  @Test
  void edgesOfAFailedStatementAfterAnEarlierStatementAddedEdgesAreTakenBack() {
    createGraphSchema();
    database.transaction(() -> database.command("sql", "INSERT INTO V (id) VALUES (1), (2), (3)").close());

    database.begin();
    database.command("sql", "CREATE EDGE E FROM (SELECT FROM V WHERE id = 1) TO (SELECT FROM V WHERE id = 2) SET w = 1").close();
    failingEdgeStatement(10);
    assertThat(degrees()).isEqualTo("1:1/0 2:0/1 3:0/0");
    database.commit();
    assertThat(degrees()).isEqualTo("1:1/0 2:0/1 3:0/0");
    assertDatabaseIsConsistent();
  }

  // ----------------------------------------------------------------------------------------------------------------------------
  // WHAT AN EARLIER STATEMENT LEFT QUEUED, AND THE OTHER STATE OF THE TRANSACTION
  // ----------------------------------------------------------------------------------------------------------------------------

  @Test
  void aListAnEarlierStatementSetIsGivenBackWhenALaterOneChangesItAndFails() {
    database.command("sql", "CREATE PROPERTY T.tags LIST").close();
    database.transaction(() -> database.command("sql", "INSERT INTO T (id, k) VALUES (1, 1), (2, 2), (3, 3)").close());

    database.begin();
    database.command("sql", "UPDATE T SET tags = ['a', 'b'] WHERE id = 1").close();
    assertThatThrownBy(() -> database.command("sql", "UPDATE T SET tags = ['z'], k = 7 WHERE id <= 2").close()).isInstanceOf(
        DuplicatedKeyException.class);

    try (final ResultSet rs = database.query("sql", "SELECT tags, k FROM T WHERE id = 1")) {
      final Result row = rs.next();
      assertThat(row.<List<String>>getProperty("tags")).containsExactly("a", "b");
      assertThat(row.<Number>getProperty("k").intValue()).isEqualTo(1);
    }
    database.commit();

    try (final ResultSet rs = database.query("sql", "SELECT tags, k FROM T WHERE id = 1")) {
      final Result row = rs.next();
      assertThat(row.<List<String>>getProperty("tags")).containsExactly("a", "b");
      assertThat(row.<Number>getProperty("k").intValue()).isEqualTo(1);
    }
    try (final ResultSet rs = database.query("sql", "SELECT tags FROM T WHERE id = 2")) {
      assertThat(rs.next().<Object>getProperty("tags")).isNull();
    }
    assertDatabaseIsConsistent();
  }

  @Test
  void aRepeatableReadTransactionTakesAFailedStatementBackToo() {
    database.transaction(() -> database.command("sql", "INSERT INTO T (id, k) VALUES (1, 1), (2, 2)").close());

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    assertThat(ids()).containsExactly(1L, 2L);
    database.command("sql", "UPDATE T SET k = 5 WHERE id = 1").close();
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO T (id, k) VALUES (3, 3), (4, 4), (5, 5)").close()).isInstanceOf(
        DuplicatedKeyException.class);
    assertThat(ids()).containsExactly(1L, 2L);
    assertThat(idByUniqueK(5)).isEqualTo(1L);
    database.command("sql", "INSERT INTO T (id, k) VALUES (3, 3)").close();
    database.commit();

    assertThat(ids()).containsExactly(1L, 2L, 3L);
    assertDatabaseIsConsistent();
  }

  @Test
  void aVertexDeleteThatFailsMidwayGivesBackTheVerticesAndTheirEdges() {
    createGraphSchema();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO V (id) VALUES (1), (2), (3), (4)").close();
      database.command("sql", "CREATE EDGE E FROM (SELECT FROM V WHERE id = 1) TO (SELECT FROM V WHERE id = 2) SET w = 1").close();
      database.command("sql", "CREATE EDGE E FROM (SELECT FROM V WHERE id = 3) TO (SELECT FROM V WHERE id = 4) SET w = 2").close();
      database.command("sql", "CREATE EDGE E FROM (SELECT FROM V WHERE id = 2) TO (SELECT FROM V WHERE id = 3) SET w = 3").close();
    });

    database.getSchema().getType("V").getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      if (((Document) record).getLong("id") == 3L)
        throw new IllegalStateException("refused");
      return true;
    });

    database.begin();
    assertThatThrownBy(() -> database.command("sql", "DELETE VERTEX FROM V WHERE id IN [1, 2, 3, 4]").close()).hasStackTraceContaining(
        "refused");

    assertThat(degrees()).isEqualTo("1:1/0 2:1/1 3:1/1 4:0/1");
    database.commit();

    assertThat(degrees()).isEqualTo("1:1/0 2:1/1 3:1/1 4:0/1");
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM E")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(3L);
    }
    assertDatabaseIsConsistent();
  }

  @Test
  void theCorpusCountersOfAFullTextIndexDoNotCountWhatAFailedStatementWrote() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc").close();
    database.command("sql", "CREATE PROPERTY Doc.id LONG").close();
    database.command("sql", "CREATE PROPERTY Doc.body STRING").close();
    database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE").close();
    database.command("sql", "CREATE INDEX ON Doc (body) FULL_TEXT").close();

    database.begin();
    database.command("sql", "INSERT INTO Doc (id, body) VALUES (1, 'the quick brown fox')").close();
    assertThatThrownBy(() -> database.command("sql",
        "INSERT INTO Doc (id, body) VALUES (2, 'jumps over the lazy dog'), (3, 'and runs away'), (1, 'a clash')").close()).isInstanceOf(
        DuplicatedKeyException.class);
    database.command("sql", "INSERT INTO Doc (id, body) VALUES (4, 'a different story')").close();
    database.commit();

    assertThat(totalDocsOfFullTextIndexes()).isEqualTo(2L);
    try (final ResultSet rs = database.query("sql", "SELECT id FROM Doc WHERE SEARCH_INDEX('Doc[body]', 'lazy') = true")) {
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT id FROM Doc WHERE SEARCH_INDEX('Doc[body]', 'fox') = true")) {
      assertThat(rs.next().<Number>getProperty("id").longValue()).isEqualTo(1L);
    }
  }

  private long totalDocsOfFullTextIndexes() {
    long total = 0;
    for (final Index index : database.getSchema().getIndexes())
      if (index instanceof LSMTreeFullTextIndex fullText && fullText.getFullTextMetadata() != null)
        total += fullText.getFullTextMetadata().getTotalDocs();
    return total;
  }

  @Test
  void aVectorIndexDoesNotSearchTheRowsOfAFailedStatement() {
    database.command("sql", "CREATE DOCUMENT TYPE VD").close();
    database.command("sql", "CREATE PROPERTY VD.id LONG").close();
    database.command("sql", "CREATE PROPERTY VD.v ARRAY_OF_FLOATS").close();
    database.command("sql", "CREATE INDEX ON VD (id) UNIQUE").close();
    database.command("sql", "CREATE INDEX ON VD (v) LSM_VECTOR METADATA { \"dimensions\": 4, \"similarity\": \"COSINE\" }").close();

    database.begin();
    database.command("sql", "INSERT INTO VD (id, v) VALUES (1, [1.0, 0.0, 0.0, 0.0])").close();
    assertThatThrownBy(() -> database.command("sql",
        "INSERT INTO VD (id, v) VALUES (2, [0.0, 1.0, 0.0, 0.0]), (3, [0.0, 0.0, 1.0, 0.0]), (1, [0.0, 0.0, 0.0, 1.0])").close())
        .isInstanceOf(DuplicatedKeyException.class);

    assertThat(neighbors()).containsExactly(1L);
    database.command("sql", "INSERT INTO VD (id, v) VALUES (4, [0.0, 0.0, 0.0, 1.0])").close();
    assertThat(neighbors()).containsExactlyInAnyOrder(1L, 4L);
    database.commit();

    assertThat(neighbors()).containsExactlyInAnyOrder(1L, 4L);
    reopenDatabase();
    assertThat(neighbors()).containsExactlyInAnyOrder(1L, 4L);
    assertDatabaseIsConsistent();
  }

  private List<Long> neighbors() {
    final List<Long> found = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM (SELECT expand(vectorNeighbors('VD[v]', [1.0, 0.0, 0.0, 0.0], 10)))")) {
      while (rs.hasNext())
        found.add(rs.next().<Number>getProperty("id").longValue());
    }
    return found;
  }

  @Test
  void aMapAnEarlierStatementSetIsGivenBackAfterTwoFailedStatementsInARow() {
    database.command("sql", "CREATE PROPERTY T.meta MAP").close();
    database.transaction(() -> database.command("sql", "INSERT INTO T (id, k) VALUES (1, 1), (2, 2), (3, 3)").close());

    database.begin();
    database.command("sql", "UPDATE T SET meta = {'a': 1}, k = 10 WHERE id = 1").close();
    for (int round = 0; round < 2; round++) {
      final int value = round;
      assertThatThrownBy(() -> database.command("sql", "UPDATE T SET meta = {'b': " + value + "}, k = 7 WHERE id <= 2").close())
          .isInstanceOf(DuplicatedKeyException.class);
      try (final ResultSet rs = database.query("sql", "SELECT meta, k FROM T WHERE id = 1")) {
        final Result row = rs.next();
        assertThat(row.<java.util.Map<String, Object>>getProperty("meta")).containsOnlyKeys("a");
        assertThat(row.<Number>getProperty("k").intValue()).isEqualTo(10);
      }
    }
    database.commit();

    assertThat(idByUniqueK(10)).isEqualTo(1L);
    assertThat(idByUniqueK(2)).isEqualTo(2L);
    assertDatabaseIsConsistent();
  }

  @Test
  void aKeyFreedAndTakenAgainByEarlierStatementsStaysWithItsHolderWhenALaterStatementFails() {
    database.transaction(() -> database.command("sql", "INSERT INTO T (id, k) VALUES (1, 1), (2, 2)").close());

    database.begin();
    // THE KEY 1 IS FREED, THEN TAKEN BY ANOTHER RECORD: THE INDEX LANE HOLDS A REPLACEMENT OF THE COMMITTED HOLDER
    database.command("sql", "DELETE FROM T WHERE id = 1").close();
    database.command("sql", "INSERT INTO T (id, k) VALUES (9, 1)").close();
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO T (id, k) VALUES (10, 10), (11, 1)").close()).isInstanceOf(
        DuplicatedKeyException.class);

    assertThat(idByUniqueK(1)).isEqualTo(9L);
    assertThat(idByUniqueK(10)).isEqualTo(-1L);
    assertThat(ids()).containsExactly(2L, 9L);
    database.commit();

    assertThat(idByUniqueK(1)).isEqualTo(9L);
    assertThat(ids()).containsExactly(2L, 9L);
    assertDatabaseIsConsistent();
  }

  @Test
  void aRecordDeletedByAnEarlierStatementStaysDeletedWhenALaterStatementFails() {
    database.transaction(() -> database.command("sql", "INSERT INTO T (id, k) VALUES (1, 1), (2, 2), (3, 3)").close());

    database.begin();
    database.command("sql", "DELETE FROM T WHERE id = 1").close();
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO T (id, k) VALUES (4, 4), (5, 4)").close()).isInstanceOf(
        DuplicatedKeyException.class);

    assertThat(ids()).containsExactly(2L, 3L);
    assertThat(countStar()).isEqualTo(2L);
    database.commit();

    assertThat(ids()).containsExactly(2L, 3L);
    assertThat(countStar()).isEqualTo(2L);
    assertDatabaseIsConsistent();
  }

  @Test
  void aCypherMergeThatFailsMidwayIsTakenBack() {
    database.command("sql", "CREATE VERTEX TYPE P").close();
    database.command("sql", "CREATE PROPERTY P.id LONG").close();
    database.command("sql", "CREATE INDEX ON P (id) UNIQUE").close();
    database.command("sql", "CREATE EDGE TYPE KNOWS").close();

    database.begin();
    database.command("opencypher", "CREATE (:P {id: 1})").close();
    assertThatThrownBy(() -> database.command("opencypher",
        "UNWIND [2, 3, 1] AS i CREATE (a:P {id: i}) WITH a MATCH (b:P {id: 1}) WHERE a <> b CREATE (a)-[:KNOWS]->(b)").close())
        .hasStackTraceContaining("Duplicated key");

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:P) RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
    database.commit();
    assertDatabaseIsConsistent();
  }
}
