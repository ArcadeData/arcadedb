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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.engine.PageManager;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Atomicity of SQL write statements: what a failed, retried, raced or concurrently read statement leaves behind.
 */
class DmlAtomicityTest extends TestHelper {

  // ---- #8188: COMMIT RETRY does not replay a committed half ----
  //
  // Regression for issue #8188: the SQL {@code COMMIT RETRY n} clause is the fourth retry loop of the shape issue
  // #7916 named - it rolls the attempt back and re-runs the caller's whole block - and it was the one the
  // enumeration missed. A block containing a statement with an EXPLICIT batch boundary
  // ({@code UPDATE/DELETE/MOVE VERTEX ... BATCH n}) commits and re-begins mid-execution, so a rollback cannot take
  // that half back and the replay applies it a SECOND time, once per remaining attempt, while the script can still
  // report success.
  // <p>
  // The other three loops ({@code LocalDatabase.transaction}, {@code DatabaseAsyncTransaction.executeTransaction},
  // {@code RemoteDatabase.transaction} over HTTP and gRPC) already consult
  // {@code TransactionContext.isPartiallyCommitted}; this pins that {@code RetryStep} does too.

  private static final String ROWS    = "Issue8188Row";
  private static final String TRIGGER = "Issue8188Trigger";
  private static final int    SIZE    = 25;

  /** Counts the attempts by counting the times the failing statement was reached, and fails it every time. */
  private final AtomicInteger              attempts = new AtomicInteger();
  private final BeforeRecordUpdateListener conflict = record -> {
    if (isTrigger(record)) {
      attempts.incrementAndGet();
      throw new ConcurrentModificationException("simulated conflict after the batch boundary");
    }
    return true;
  };

  private static boolean isTrigger(final Record record) {
    return record instanceof Document document && TRIGGER.equals(document.getTypeName());
  }

  private void createCommitRetryRows() {
    database.getSchema().createDocumentType(ROWS).createProperty("seq", Type.INTEGER);
    database.getSchema().createDocumentType(TRIGGER).createProperty("seq", Type.INTEGER);

    database.transaction(() -> {
      for (int i = 0; i < SIZE; i++)
        database.newDocument(ROWS).set("seq", 0).save();
      database.newDocument(TRIGGER).set("seq", 0).save();
    });

    database.getEvents().registerListener(conflict);
  }

  /** Runs after every test of this class: a no-op for those that never registered the COMMIT RETRY listener. */
  @AfterEach
  void unregisterListener() {
    database.getEvents().unregisterListener(conflict);
  }

  /**
   * Regression test for issue #8188.
   * <p>
   * The defect itself. With a BATCH boundary crossed before the conflict, the block must be executed ONCE: the
   * rows the batch published are incremented exactly once and the conflict reaches the caller.
   */
  @Test
  void aCommitRetryBlockThatCommittedPartOfItsWorkIsNotReplayed() {
    createCommitRetryRows();
    assertThatThrownBy(() -> database.command("sqlscript", """
        BEGIN;
        UPDATE %s SET seq = seq + 1 BATCH 10;
        UPDATE %s SET seq = seq + 1;
        COMMIT RETRY 3;
        """.formatted(ROWS, TRIGGER))).isInstanceOf(NeedRetryException.class);

    assertThat(attempts.get()).as("the block must be run ONCE: its durable half cannot be replayed").isEqualTo(1);

    // The 20 rows the batch published are incremented exactly once; the remaining 5 were rolled back.
    assertThat(countRowsWithSeq(1)).isEqualTo(20);
    assertThat(countRowsWithSeq(0)).isEqualTo(5);
    assertThat(countRowsWithSeq(2)).as("no row may be incremented twice").isZero();
  }

  /**
   * Regression test for issue #8188.
   * <p>
   * The same block, run under a caller transaction - which is what every server path does, {@code BEGIN} inside
   * the script then opening a NESTED transaction. The commit the batch boundary publishes happens on that nested
   * context, which is popped straight after, so the guard only works if a nested commit is reported to the
   * context that survives it.
   */
  @Test
  void theGuardHoldsWhenTheScriptRunsUnderACallerTransaction() {
    createCommitRetryRows();
    assertThatThrownBy(() -> database.transaction(() -> database.command("sqlscript", """
        BEGIN;
        UPDATE %s SET seq = seq + 1 BATCH 10;
        UPDATE %s SET seq = seq + 1;
        COMMIT RETRY 3;
        """.formatted(ROWS, TRIGGER)), true, 1)).isInstanceOf(NeedRetryException.class);

    assertThat(attempts.get()).as("the block must be run ONCE: its durable half cannot be replayed").isEqualTo(1);
    assertThat(countRowsWithSeq(1)).isEqualTo(20);
    assertThat(countRowsWithSeq(2)).as("no row may be incremented twice").isZero();
  }

  /**
   * Regression test for issue #8188.
   * <p>
   * The guard must not disturb the case it sits next to: a block that publishes NOTHING before failing is still
   * retried the full number of attempts, because the rollback really did take everything back.
   */
  @Test
  void aCommitRetryBlockThatCommittedNothingIsStillRetried() {
    createCommitRetryRows();
    assertThatThrownBy(() -> database.command("sqlscript", """
        BEGIN;
        UPDATE %s SET seq = seq + 1;
        UPDATE %s SET seq = seq + 1;
        COMMIT RETRY 3;
        """.formatted(ROWS, TRIGGER))).isInstanceOf(NeedRetryException.class);

    assertThat(attempts.get()).as("nothing was durable, so all the attempts are still available").isEqualTo(3);
    assertThat(countRowsWithSeq(0)).as("and the rollback took every attempt's work back").isEqualTo(SIZE);
  }

  /**
   * Regression test for issue #8188.
   * <p>
   * And a block that publishes nothing and succeeds on a later attempt still succeeds: the guard reads the commit
   * counter of the transaction the attempt ran in, so an earlier attempt's rollback cannot make a later one look
   * partially committed.
   */
  @Test
  void aCommitRetryBlockThatSucceedsOnALaterAttemptStillSucceeds() {
    createCommitRetryRows();
    final AtomicInteger runs = new AtomicInteger();
    final BeforeRecordUpdateListener transientConflict = record -> {
      if (isTrigger(record) && runs.incrementAndGet() < 3)
        throw new ConcurrentModificationException("simulated transient conflict");
      return true;
    };

    database.getEvents().unregisterListener(conflict);
    database.getEvents().registerListener(transientConflict);
    try {
      database.command("sqlscript", """
          BEGIN;
          UPDATE %s SET seq = seq + 1;
          UPDATE %s SET seq = seq + 1;
          COMMIT RETRY 5;
          """.formatted(ROWS, TRIGGER));
    } finally {
      database.getEvents().unregisterListener(transientConflict);
      database.getEvents().registerListener(conflict);
    }

    assertThat(runs.get()).isEqualTo(3);
    assertThat(countRowsWithSeq(1)).as("the successful attempt's work is the only one that stands").isEqualTo(SIZE);
  }

  private long countRowsWithSeq(final int seq) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + ROWS + " WHERE seq = ?", seq)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  // ---- #8859: INSERT ON DUPLICATE KEY SKIP racing index DDL ----
  //
  // Issue #8859 (follow-up of #8855): an {@code INSERT ... ON DUPLICATE KEY SKIP} racing a {@code CREATE INDEX} or a
  // {@code DROP INDEX} on the same type used to fail with an IndexException, because {@code SaveElementStep} asked
  // {@code TypeIndex.isUnique()} of an index with no sub-index yet, or one already invalidated.

  private static final int WRITERS = 4;
  private static final int ROUNDS  = 120;
  private static final int RETRIES = 50;

  /** Regression test for issue #8859. */
  @Test
  void isUniqueIfPresentAnswersFalseForAnIndexThatWentAwayInsteadOfFailing() {
    final DocumentType type = database.getSchema().createDocumentType("Product");
    type.createProperty("sku", Type.STRING);
    final TypeIndex unique = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "sku");
    assertThat(unique.isUniqueIfPresent()).isTrue();

    unique.drop();

    assertThat(unique.isUniqueIfPresent()).as("dropped: no index, so no uniqueness to enforce").isFalse();
  }

  /** Regression test for issue #8859. */
  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void insertOnDuplicateKeySkipKeepsWorkingWhileTheUniqueIndexIsCreatedAndDropped() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Product");
    database.command("sql", "CREATE PROPERTY Product.sku STRING");

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong inserted = new AtomicLong();
    final Map<String, AtomicLong> failures = new ConcurrentHashMap<>();
    final List<Thread> writers = new ArrayList<>();
    for (int t = 0; t < WRITERS; t++) {
      final int writer = t;
      final Thread thread = new Thread(() -> {
        long sequence = 0;
        while (!stop.get()) {
          try {
            // a key no other writer and no earlier round ever uses: nothing can clash, so every failure is a race
            final String sku = "w" + writer + "-" + sequence++;
            // A CREATE INDEX that lands on a type this transaction is writing to refuses it with a retryable
            // ConcurrentModificationException (LocalSchema.reconcileIndexesRegisteredSince, #10903), the documented outcome of DDL
            // racing a writer, so the transaction is retried as that message asks. Any other failure still fails the test.
            database.transaction(() -> database.command("sql",
                "INSERT INTO Product CONTENT [ {\"sku\":\"" + sku + "\"}, {\"sku\":\"" + sku + "-b\"} ] ON DUPLICATE KEY SKIP")
                .close(), false, RETRIES);
            inserted.addAndGet(2);
          } catch (final Throwable e) {
            failures.computeIfAbsent(describe(e), k -> new AtomicLong()).incrementAndGet();
          }
        }
      });
      writers.add(thread);
      thread.start();
    }

    try {
      for (int i = 0; i < ROUNDS; i++) {
        database.command("sql", "CREATE INDEX ON Product (sku) UNIQUE");
        database.command("sql", "DROP INDEX `Product[sku]`");
      }
    } finally {
      stop.set(true);
      for (final Thread thread : writers)
        thread.join();
    }

    assertThat(inserted.get()).isGreaterThan(0);
    assertThat(failures).as("inserts that succeeded: " + inserted.get()).isEmpty();
  }

  /** The exception and the engine frames it came from, so a failure names the code that raced. */
  private static String describe(final Throwable e) {
    final StringBuilder text = new StringBuilder(e.getClass().getSimpleName()).append(": ")
        .append(String.valueOf(e.getMessage()).split("\n")[0]);
    int frames = 0;
    for (final StackTraceElement frame : e.getStackTrace())
      if (frame.getClassName().startsWith("com.arcadedb") && frames++ < 6)
        text.append("\n   at ").append(frame.getClassName().substring(frame.getClassName().lastIndexOf('.') + 1)).append('.')
            .append(frame.getMethodName()).append(':').append(frame.getLineNumber());
    return text.toString();
  }

  // ---- #9369: an index lookup never sees a torn commit ----
  //
  // Issue #9369: a reader running while a transaction deletes a record and creates another with the same indexed value must see
  // that value in every answer, since every committed state holds it. Before the fix an index lookup could observe the commit half
  // published (the index page of one commit with the record page of the other) and lose the row, while a scan never did.

  private static final int FIRST_ID = 200;
  private static final int IDS      = 50;

  private void createTornCommitType() {
    database.command("sql", "CREATE VERTEX TYPE Product");
    database.command("sql", "CREATE PROPERTY Product.pid INTEGER");
    database.command("sql", "CREATE PROPERTY Product.views INTEGER");
  }

  /** Regression test for issue #9369. */
  @Test
  @Tag("slow")
  void uniqueHashLookupNeverLosesARowOfAnAtomicReplace() throws Exception {
    createTornCommitType();
    run("UNIQUE_HASH");
  }

  /** Regression test for issue #9369. */
  @Test
  @Tag("slow")
  void uniqueLookupNeverLosesARowOfAnAtomicReplace() throws Exception {
    createTornCommitType();
    run("UNIQUE");
  }

  /** Regression test for issue #9369. */
  @Test
  @Tag("slow")
  void notUniqueLookupNeverLosesARowOfAnAtomicReplace() throws Exception {
    createTornCommitType();
    run("NOTUNIQUE");
  }

  /** Regression test for issue #9369. */
  @Test
  void publicationSequenceMovesTwiceForACommitAndIsEvenAtRest() {
    createTornCommitType();
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final long before = pageManager.getPublicationSequence();
    assertThat(before & 1).isZero();
    database.transaction(() -> database.command("sql", "CREATE VERTEX Product SET pid = 1, views = 1").close());
    final long after = pageManager.getPublicationSequence();
    assertThat(after & 1).isZero();
    assertThat(after).isGreaterThan(before);
  }

  /** Regression test for issue #9369. */
  @Test
  void theSamePointLookupRunTwiceServesTheSameRows() {
    createTornCommitType();
    database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "CREATE VERTEX Product SET pid = :p, views = 1", Map.of("p", (long) i)).close();
    });
    final List<Long> ids = List.of(1L, 3L, 5L);
    for (int run = 0; run < 3; run++) {
      final List<Integer> pids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT pid FROM Product WHERE pid IN :ids ORDER BY pid", Map.of("ids", ids))) {
        rs.forEachRemaining(r -> pids.add(r.getProperty("pid")));
      }
      assertThat(pids).as("run " + run).containsExactly(1, 3, 5);
    }
  }

  /** Regression test for issue #9369. */
  @Test
  void repeatableReadKeepsItsSnapshotWhenNoCommitOverlapsTheLookup() {
    createTornCommitType();
    database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
    database.transaction(() -> database.command("sql", "CREATE VERTEX Product SET pid = 1, views = 1").close());
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      try (final ResultSet rs = database.query("sql", "SELECT views FROM Product WHERE pid = 1")) {
        assertThat(rs.next().<Integer>getProperty("views")).isEqualTo(1);
      }
      // another thread commits a change of the record the transaction has already read
      final Thread writer = new Thread(() -> database.transaction(() -> database.command("sql", "UPDATE Product SET views = 2 WHERE pid = 1").close()));
      writer.start();
      try {
        writer.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      try (final ResultSet rs = database.query("sql", "SELECT views FROM Product WHERE pid = 1")) {
        assertThat(rs.next().<Integer>getProperty("views")).as("the snapshot the transaction pinned").isEqualTo(1);
      }
    } finally {
      database.rollback();
    }
  }

  /** Regression test for issue #9369. */
  @Test
  void largePointLookupIsServedInFull() {
    createTornCommitType();
    database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
    final int count = 3000;
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.command("sql", "CREATE VERTEX Product SET pid = :p, views = 1", Map.of("p", (long) i)).close();
    });
    final List<Long> ids = new ArrayList<>();
    for (long i = 0; i < count; i++)
      ids.add(i);
    int rows = 0;
    try (final ResultSet rs = database.query("sql", "SELECT pid FROM Product WHERE pid IN :ids", Map.of("ids", ids))) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    assertThat(rows).isEqualTo(count);
  }

  private void run(final String indexKind) throws Exception {
    database.command("sql", "CREATE INDEX ON Product (pid) " + indexKind);
    database.transaction(() -> {
      for (int i = 0; i < 1000; i++)
        database.command("sql", "CREATE VERTEX Product SET pid = :p, views = 100", Map.of("p", (long) i)).close();
    });
    final List<Long> ids = new ArrayList<>();
    for (long i = FIRST_ID; i < FIRST_ID + IDS; i++)
      ids.add(i);

    final String[] modes = { "outside any transaction", "READ_COMMITTED transaction", "REPEATABLE_READ transaction" };
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong[] reads = { new AtomicLong(), new AtomicLong(), new AtomicLong() };
    final AtomicLong[] missing = { new AtomicLong(), new AtomicLong(), new AtomicLong() };
    final AtomicReference<Throwable>[] readerFailures = new AtomicReference[] { new AtomicReference<Throwable>(), new AtomicReference<Throwable>(),
        new AtomicReference<Throwable>() };
    final Thread[] readers = new Thread[3];
    for (int m = 0; m < 3; m++) {
      final int mode = m;
      readers[m] = new Thread(() -> {
        try {
          while (!stop.get()) {
            if (mode == 2)
              database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
            if (mode != 0)
              database.begin();
            int rows = 0;
            try (final ResultSet rs = database.query("sql", "SELECT pid FROM Product WHERE pid IN :ids", Map.of("ids", ids))) {
              while (rs.hasNext()) {
                rs.next();
                rows++;
              }
            } finally {
              if (mode != 0)
                database.rollback();
            }
            reads[mode].incrementAndGet();
            if (rows != IDS)
              missing[mode].incrementAndGet();
          }
        } catch (final Throwable e) {
          // a crashed reader must fail the test, not leave its counters at zero
          readerFailures[mode].set(e);
        }
      }, "reader-" + mode);
      readers[m].setDaemon(true);
      readers[m].start();
    }

    final long deadline = System.currentTimeMillis() + 8_000;
    long commits = 0;
    while (System.currentTimeMillis() < deadline) {
      final long x = FIRST_ID + commits % IDS;
      database.begin();
      database.command("sql", "DELETE FROM Product WHERE pid = :p", Map.of("p", x)).close();
      database.command("sql", "CREATE VERTEX Product SET pid = :p, views = 100", Map.of("p", x)).close();
      database.commit();
      commits++;
    }
    stop.set(true);
    for (final Thread t : readers)
      t.join();

    for (int m = 0; m < 3; m++)
      assertThat(readerFailures[m].get()).as(modes[m] + " reader failed").isNull();

    final StringBuilder report = new StringBuilder();
    for (int m = 0; m < 3; m++)
      if (missing[m].get() > 0)
        report.append(indexKind).append(", ").append(modes[m]).append(": ").append(missing[m]).append(" of ").append(reads[m])
            .append(" reads lost a row; ");
    assertThat(report.toString()).isEmpty();
  }

  // ---- #10194: a failed statement keeps no partial rows ----
  //
  // #10194, third point: a multi-row statement that failed midway inside a transaction left the rows it had written before the
  // error, and a caller that caught the error and committed published them: half a statement. The fix of #10194 refused the commit of
  // such a transaction, there being no savepoint to take the statement back to. Statement savepoints (#10346) take it back: the rows
  // the statement wrote before its error are gone, and the transaction goes on. The commit is still refused where the engine cannot take
  // a statement back exactly, which is pinned by the tests of #10379 and #10482.

  private void createUniqueKeyType() {
    database.command("sql", "CREATE DOCUMENT TYPE T").close();
    database.command("sql", "CREATE PROPERTY T.id LONG").close();
    database.command("sql", "CREATE PROPERTY T.k INTEGER").close();
    database.command("sql", "CREATE INDEX ON T (k) UNIQUE").close();
  }

  private long rows() {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM T")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  /** Runs the statement inside a transaction, catching its error there and committing, as the issue's reproducer does. */
  private Throwable runCaughtAndCommit(final String statement) {
    final AtomicReference<Throwable> caught = new AtomicReference<>();
    database.transaction(() -> {
      try {
        database.command("sql", statement).close();
      } catch (final RuntimeException e) {
        caught.set(e);
      }
    }, false, 0);
    return caught.get();
  }

  /** Regression test for issue #10194. */
  @Test
  void aCaughtMultiRowInsertIsTakenBackAndTheTransactionCommits() {
    createUniqueKeyType();
    final AtomicReference<Throwable> caught = new AtomicReference<>();

    database.transaction(() -> {
      try {
        database.command("sql", "INSERT INTO T (id, k) VALUES (30, 23), (31, 23)").close();
      } catch (final DuplicatedKeyException e) {
        caught.set(e);
      }
      database.command("sql", "INSERT INTO T SET id = 5, k = 5").close();
    }, false, 0);

    assertThat(caught.get()).isInstanceOf(DuplicatedKeyException.class);
    assertThat(rows()).isEqualTo(1);
  }

  /** Regression test for issue #10194. */
  @Test
  void aSingleRowRefusalLeavesTheTransactionUsable() {
    createUniqueKeyType();
    final AtomicReference<Throwable> caught = new AtomicReference<>();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 2, k = 2").close();
      database.command("sql", "INSERT INTO T SET id = 3, k = 3").close();
      try {
        // a duplicate of a key this transaction already claimed is refused by the statement itself, with its record retracted
        database.command("sql", "INSERT INTO T SET id = 4, k = 3").close();
      } catch (final DuplicatedKeyException e) {
        caught.set(e);
      }
      database.command("sql", "INSERT INTO T SET id = 5, k = 5").close();
    });

    assertThat(caught.get()).isInstanceOf(DuplicatedKeyException.class);
    assertThat(rows()).isEqualTo(3);
  }

  /** Regression test for issue #10194. */
  @Test
  void aCaughtMultiRowUpdateIsTakenBackAndTheTransactionCommits() {
    createUniqueKeyType();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
      database.command("sql", "INSERT INTO T SET id = 2, k = 2").close();
      database.command("sql", "INSERT INTO T SET id = 3, k = 5").close();
    });

    assertThat(runCaughtAndCommit("UPDATE T SET k = 5 WHERE id < 3")).isInstanceOf(DuplicatedKeyException.class);

    try (final ResultSet rs = database.query("sql", "SELECT k FROM T ORDER BY id")) {
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(1);
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(2);
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(5);
    }
  }

  /** Regression test for issue #10194. */
  @Test
  void aStatementThatFailedBeforeItsFirstWriteLeavesTheTransactionUsable() {
    createUniqueKeyType();
    final AtomicReference<Throwable> caught = new AtomicReference<>();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
      try {
        // its first row is the refused one: the statement wrote nothing
        database.command("sql", "INSERT INTO T (id, k) VALUES (2, 1), (3, 3)").close();
      } catch (final DuplicatedKeyException e) {
        caught.set(e);
      }
    });

    assertThat(caught.get()).isInstanceOf(DuplicatedKeyException.class);
    assertThat(rows()).isEqualTo(1);
  }

  /** Regression test for issue #10194. */
  @Test
  void anUncaughtFailureStillRollsBackEverythingAsBefore() {
    createUniqueKeyType();
    assertThatThrownBy(() -> database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
      database.command("sql", "INSERT INTO T (id, k) VALUES (30, 23), (31, 23)").close();
    })).isInstanceOf(DuplicatedKeyException.class);

    assertThat(rows()).isZero();
  }

  /** Regression test for issue #10194. */
  @Test
  void aWholeStatementThatSucceedsIsUnaffected() {
    createUniqueKeyType();
    database.transaction(() -> database.command("sql", "INSERT INTO T (id, k) VALUES (1, 1), (2, 2), (3, 3)").close());

    assertThat(rows()).isEqualTo(3);
  }

  /** Regression test for issue #10194. */
  @Test
  void aStatementThatChangedWhatAnEarlierOneUpdatedIsTakenBackAndTheEarlierUpdateStands() {
    createUniqueKeyType();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
      database.command("sql", "INSERT INTO T SET id = 2, k = 2").close();
    });

    database.transaction(() -> {
      // UPDATED, NOT WRITTEN YET: THE NEXT STATEMENT FINDS THE ONLY COPY OF WHAT THIS ONE DID, AND CHANGES IT
      database.command("sql", "UPDATE T SET k = 10 WHERE id = 1").close();
      try {
        database.command("sql", "UPDATE T SET k = 7 WHERE id <= 2").close();
      } catch (final DuplicatedKeyException e) {
        // carried on, as the caller of the reproducer does
      }
      database.command("sql", "INSERT INTO T SET id = 5, k = 5").close();
    }, false, 0);

    assertThat(rows()).isEqualTo(3);
    try (final ResultSet rs = database.query("sql", "SELECT k FROM T ORDER BY id")) {
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(10);
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(2);
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(5);
    }
  }

  /** Regression test for issue #10194. */
  @Test
  void aWriteStatementFailsInsideCommandAndNotWhileTheResultIsRead() {
    createUniqueKeyType();
    database.command("sql", "CREATE DOCUMENT TYPE Src").close();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Src SET id = 1, k = 7").close();
      database.command("sql", "INSERT INTO Src SET id = 2, k = 7").close();
    });

    // INSERT ... FROM SELECT runs to the end inside command(): the error is raised there, where the guard sees it, and the
    // result set that comes back holds what the statement already did
    final AtomicReference<Throwable> caught = new AtomicReference<>();
    database.transaction(() -> {
      try {
        database.command("sql", "INSERT INTO T FROM SELECT id, k FROM Src ORDER BY id").close();
      } catch (final DuplicatedKeyException e) {
        caught.set(e);
      }
    }, false, 0);

    assertThat(caught.get()).isInstanceOf(DuplicatedKeyException.class);
    assertThat(rows()).isZero();
  }

  /** Regression test for issue #10194. */
  @Test
  void aWriteRunThroughAnalyzeIsTakenBackLikeCommand() {
    createUniqueKeyType();
    // analyze().execute() is the entry the MCP command tool runs writes through, and it takes no CommandContext
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("INSERT INTO T (id, k) VALUES (30, 23), (31, 23)");
    assertThat(analyzed.isIdempotent()).isFalse();

    final AtomicReference<Throwable> caught = new AtomicReference<>();
    database.transaction(() -> {
      try {
        analyzed.execute(Map.of()).close();
      } catch (final DuplicatedKeyException e) {
        caught.set(e);
      }
    }, false, 0);

    assertThat(caught.get()).isInstanceOf(DuplicatedKeyException.class);
    assertThat(rows()).isZero();
  }

  /** Regression test for issue #10194. */
  @Test
  void aReadOnlyStatementNeverBlocksTheCommit() {
    createUniqueKeyType();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
      assertThatThrownBy(() -> database.query("sql", "SELECT FROM NoSuchType")).isInstanceOf(RuntimeException.class);
    });

    assertThat(rows()).isEqualTo(1);
  }
}
