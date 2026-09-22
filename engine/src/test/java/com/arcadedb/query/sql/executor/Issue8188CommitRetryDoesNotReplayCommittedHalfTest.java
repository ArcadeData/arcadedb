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
import com.arcadedb.database.Record;
import com.arcadedb.database.Document;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #8188: the SQL {@code COMMIT RETRY n} clause is the fourth retry loop of the shape issue
 * #7916 named - it rolls the attempt back and re-runs the caller's whole block - and it was the one the
 * enumeration missed. A block containing a statement with an EXPLICIT batch boundary
 * ({@code UPDATE/DELETE/MOVE VERTEX ... BATCH n}) commits and re-begins mid-execution, so a rollback cannot take
 * that half back and the replay applies it a SECOND time, once per remaining attempt, while the script can still
 * report success.
 * <p>
 * The other three loops ({@code LocalDatabase.transaction}, {@code DatabaseAsyncTransaction.executeTransaction},
 * {@code RemoteDatabase.transaction} over HTTP and gRPC) already consult
 * {@code TransactionContext.isPartiallyCommitted}; this pins that {@code RetryStep} does too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8188CommitRetryDoesNotReplayCommittedHalfTest extends TestHelper {

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

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(ROWS).createProperty("seq", Type.INTEGER);
    database.getSchema().createDocumentType(TRIGGER).createProperty("seq", Type.INTEGER);

    database.transaction(() -> {
      for (int i = 0; i < SIZE; i++)
        database.newDocument(ROWS).set("seq", 0).save();
      database.newDocument(TRIGGER).set("seq", 0).save();
    });

    database.getEvents().registerListener(conflict);
  }

  @AfterEach
  void unregisterListener() {
    database.getEvents().unregisterListener(conflict);
  }

  /**
   * The defect itself. With a BATCH boundary crossed before the conflict, the block must be executed ONCE: the
   * rows the batch published are incremented exactly once and the conflict reaches the caller.
   */
  @Test
  void aCommitRetryBlockThatCommittedPartOfItsWorkIsNotReplayed() {
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
   * The same block, run under a caller transaction - which is what every server path does, {@code BEGIN} inside
   * the script then opening a NESTED transaction. The commit the batch boundary publishes happens on that nested
   * context, which is popped straight after, so the guard only works if a nested commit is reported to the
   * context that survives it.
   */
  @Test
  void theGuardHoldsWhenTheScriptRunsUnderACallerTransaction() {
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
   * The guard must not disturb the case it sits next to: a block that publishes NOTHING before failing is still
   * retried the full number of attempts, because the rollback really did take everything back.
   */
  @Test
  void aCommitRetryBlockThatCommittedNothingIsStillRetried() {
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
   * And a block that publishes nothing and succeeds on a later attempt still succeeds: the guard reads the commit
   * counter of the transaction the attempt ran in, so an earlier attempt's rollback cannot make a later one look
   * partially committed.
   */
  @Test
  void aCommitRetryBlockThatSucceedsOnALaterAttemptStillSucceeds() {
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
}
