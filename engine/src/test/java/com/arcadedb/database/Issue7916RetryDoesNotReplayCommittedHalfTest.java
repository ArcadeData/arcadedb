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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #7916: {@code db.transaction(txBlock, joinCurrentTx, attempts, ...)} rolls back and re-runs
 * the whole block on a {@code NeedRetryException}. A rollback can only take back what is still buffered, so a block
 * containing a statement that COMMITS MID-EXECUTION - {@code UPDATE/DELETE/MOVE VERTEX ... BATCH n}, or DDL such as
 * {@code TRUNCATE TYPE} and {@code REBUILD INDEX} - has its durable half applied a SECOND time by the retry, and
 * {@code ok.call()} then reports clean success. {@code BatchStep} re-begins straight after its commit, so on return
 * the database looks exactly as it did going in and the loop cannot tell from {@code isTransactionActive()} that
 * anything happened.
 * <p>
 * #7667 gave {@code TransactionContext} the commit counter that detects this and wired it into the async executor's
 * shared batch; the two block-level retry loops did not consult it. The fix makes them refuse the retry and
 * propagate the conflict to the caller, who is the only one who knows how to compensate for the half that stands.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7916RetryDoesNotReplayCommittedHalfTest extends TestHelper {

  private static final String ROWS = "Issue7916Row";
  private static final int    SIZE = 25;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(ROWS).createProperty("seq", Type.INTEGER);

    database.transaction(() -> {
      for (int i = 0; i < SIZE; i++)
        database.newDocument(ROWS).set("seq", 0).save();
    });
  }

  /**
   * The defect itself: with a BATCH boundary crossed before the conflict, the retry used to double-apply the rows
   * already published. Now the block is not re-run at all, so every row is incremented exactly once, and the
   * conflict reaches the caller instead of being swallowed into a clean return.
   */
  @Test
  void aBlockThatCommittedPartOfItsWorkIsNotReplayed() {
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicInteger okCalls = new AtomicInteger();

    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      // Commits every 10 of the 25 rows: 20 are durable by the time the statement ends.
      database.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10");
      // ... and then the block fails, the way a genuine MVCC conflict on a later statement would.
      throw new ConcurrentModificationException("simulated conflict after the batch boundary");
    }, true, 3, okCalls::incrementAndGet, null)).isInstanceOf(NeedRetryException.class);

    assertThat(attempts.get()).as("the block must be run ONCE: its durable half cannot be replayed").isEqualTo(1);
    assertThat(okCalls.get()).as("and a block that failed must not report success").isZero();

    // The 20 rows the batch published are incremented exactly once; the remaining 5 were rolled back.
    assertThat(countRowsWithSeq(1)).isEqualTo(20);
    assertThat(countRowsWithSeq(0)).isEqualTo(5);
    assertThat(countRowsWithSeq(2)).as("no row may be incremented twice").isZero();
  }

  /**
   * The issue also named {@code TRUNCATE TYPE} and {@code REBUILD TYPE} as mid-statement committers. They are NOT,
   * at this HEAD: both suppress their batching when a caller transaction is active (issue #6220), so they join the
   * caller's unit and a rollback really does put the records back. The retry therefore stays available, and this
   * pins that - a future change that made either of them commit under a caller transaction would trip the guard
   * here and be caught as the behaviour change it is.
   * <p>
   * {@code BATCH n} is deliberately the exception: the clause IS the user asking for intermediate commits.
   */
  @Test
  void truncateInsideACallerTransactionJoinsItSoTheRetryStaysAvailable() {
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      database.command("sql", "TRUNCATE TYPE " + ROWS);
      throw new ConcurrentModificationException("simulated conflict after the truncate");
    }, true, 3, null, null)).isInstanceOf(NeedRetryException.class);

    assertThat(attempts.get()).as("nothing was published, so every attempt is still available").isEqualTo(3);
    assertThat(countRowsWithSeq(0)).as("and the rollback put every row back").isEqualTo(SIZE);
  }

  /**
   * {@code DELETE ... BATCH n} goes through the same {@code BatchStep}, so it gets the same refusal.
   */
  @Test
  void aBatchedDeleteThatCommittedPartOfItsWorkIsNotReplayed() {
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      database.command("sql", "DELETE FROM " + ROWS + " BATCH 10");
      throw new ConcurrentModificationException("simulated conflict after the batch boundary");
    }, true, 3, null, null)).isInstanceOf(NeedRetryException.class);

    assertThat(attempts.get()).isEqualTo(1);
    // 20 rows are durably gone, the last 5 deletes were rolled back.
    assertThat(countRowsWithSeq(0)).isEqualTo(5);
  }

  /**
   * The guard must not disturb the ordinary case it sits next to: a block that publishes NOTHING before failing is
   * still retried the full number of attempts, because the rollback really did take everything back.
   */
  @Test
  void aBlockThatCommittedNothingIsStillRetried() {
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      database.newDocument(ROWS).set("seq", 99).save();
      throw new ConcurrentModificationException("simulated conflict with nothing published");
    }, true, 3, null, null)).isInstanceOf(NeedRetryException.class);

    assertThat(attempts.get()).as("nothing was durable, so all the attempts are still available").isEqualTo(3);
    assertThat(countRowsWithSeq(99)).as("and the rollback took every attempt's work back").isZero();
  }

  /**
   * And a block that succeeds on a later attempt still succeeds: the guard reads the commit counter of the
   * transaction the attempt ran in, not of the whole database, so an earlier attempt's rollback cannot make a
   * later one look partially committed.
   */
  @Test
  void aBlockThatSucceedsOnRetryStillSucceeds() {
    final AtomicInteger attempts = new AtomicInteger();

    database.transaction(() -> {
      if (attempts.incrementAndGet() < 3)
        throw new ConcurrentModificationException("simulated transient conflict");
      database.newDocument(ROWS).set("seq", 42).save();
    }, true, 5, null, null);

    assertThat(attempts.get()).isEqualTo(3);
    assertThat(countRowsWithSeq(42)).isEqualTo(1);
  }

  /**
   * The second loop with the same hazard, and the one {@code commitBatch()}'s javadoc cites as the precedent for
   * the async retry contract: {@code DatabaseAsyncTransaction.executeTransaction}.
   */
  @Test
  void theAsyncRetryLoopRefusesToReplayACommittedHalf() throws Exception {
    final AtomicInteger      attempts = new AtomicInteger();
    final CountDownLatch     finished = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    database.async().transaction(() -> {
      attempts.incrementAndGet();
      database.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10");
      throw new ConcurrentModificationException("simulated conflict after the batch boundary");
    }, 3, null, e -> {
      failure.set(e);
      finished.countDown();
    });

    assertThat(finished.await(60, TimeUnit.SECONDS)).as("the async task reported back").isTrue();
    database.async().waitCompletion();

    assertThat(attempts.get()).as("the block must be run ONCE: its durable half cannot be replayed").isEqualTo(1);
    assertThat(failure.get()).isInstanceOf(NeedRetryException.class);

    assertThat(countRowsWithSeq(1)).isEqualTo(20);
    assertThat(countRowsWithSeq(0)).isEqualTo(5);
    assertThat(countRowsWithSeq(2)).as("no row may be incremented twice").isZero();
  }

  private long countRowsWithSeq(final int seq) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + ROWS + " WHERE seq = ?", seq)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
