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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.TransactionException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10416: commit phase 2 turned an {@link InterruptedException} into a {@link TransactionException} without restoring the
 * thread's interrupt flag, so the code after a failed commit no longer saw that the thread had been cancelled.
 * <p>
 * The interrupt is delivered where phase 2 really waits: for room in the page-flush queue ({@code reserveQueueSlot}, served
 * by {@code PageManager.publishPages}). The test takes every slot of this database's budget first, so the committer parks
 * there, and interrupts it once the flush thread counts it as waiting. No timing is involved: the wait only ends by the
 * interrupt.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10416CommitPhase2InterruptFlagTest extends TestHelper {
  private static final String TYPE                  = "Doc10416";
  /** Bounds only waits expected to succeed, so a generous value cannot turn a passing run red (#6260). */
  private static final long   ASSERTION_TIMEOUT_SEC = 60;

  @Test
  void anInterruptDuringPhase2IsStillVisibleAfterTheTransactionException() throws Exception {
    // THE FIRST DOCUMENT REGISTERS THE PROPERTY NAME IN THE DICTIONARY: A NEW NAME IS A NESTED COMMIT OF ITS OWN, WHICH WOULD
    // TAKE THE INTERRUPT BEFORE THE COMMIT UNDER TEST
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE);
      database.newDocument(TYPE).set("id", 0).save();
    });

    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManagerFlushThread flush = db.getPageManager().getFlushThread();

    // THE BATCHES OF THE SCHEMA COMMIT GIVE THEIR SLOTS BACK WHEN THE FLUSH THREAD POLLS THEM: ONLY THEN IS EVERY SLOT TAKEN HERE
    // ONE NO OTHER COMMIT CAN FREE
    final long drainDeadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC);
    while (flush.slotsUsedBy(db) > 0 && System.currentTimeMillis() < drainDeadline)
      Thread.sleep(5);
    assertThat(flush.slotsUsedBy(db)).isZero();

    int reserved = 0;
    while (flush.tryReserveQueueSlot(db))
      ++reserved;
    assertThat(reserved).isGreaterThan(0);

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicBoolean interruptedAfterTheFailure = new AtomicBoolean();
    try {
      final long waitsBefore = flush.queueSlotWaits.get();

      final Thread committer = new Thread(() -> {
        database.begin();
        database.newDocument(TYPE).set("id", 1).save();
        try {
          database.commit();
        } catch (final Throwable e) {
          failure.set(e);
        }
        interruptedAfterTheFailure.set(Thread.currentThread().isInterrupted());
      }, "issue10416-committer");
      committer.start();

      final long parkDeadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC);
      while (flush.queueSlotWaits.get() == waitsBefore && committer.isAlive() && System.currentTimeMillis() < parkDeadline)
        Thread.sleep(5);
      assertThat(flush.queueSlotWaits.get()).as("the committer must be parked waiting for a flush-queue slot")
          .isGreaterThan(waitsBefore);

      committer.interrupt();
      committer.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
      assertThat(committer.isAlive()).isFalse();
    } finally {
      for (int i = 0; i < reserved; i++)
        flush.releaseQueueReservation(db);
    }

    // THE INTERRUPT ARRIVED PAST THE WAL APPEND, SO THE DATABASE IS FENCED: REOPENED BEFORE ANY ASSERTION, SO A RED RUN DOES NOT
    // ALSO LEAVE A FENCED DATABASE TO THE TEARDOWN
    final boolean fenced = ((LocalDatabase) db.getEmbedded()).isFencedForRecovery();
    reopenDatabase();

    // THE SHAPE restoreInterruptIfCause RELIES ON: THE InterruptedException IS THE DIRECT CAUSE OF THE TransactionException,
    // NOT WRAPPED AGAIN ON ITS WAY OUT OF commit()
    assertThat(failure.get()).isInstanceOf(TransactionException.class);
    assertThat(failure.get().getCause()).isInstanceOf(InterruptedException.class);
    assertThat(interruptedAfterTheFailure.get()).as("the cancellation must survive the failed commit").isTrue();

    // THE REOPEN REPLAYED THE TRANSACTION FROM THE WAL
    assertThat(fenced).isTrue();
    assertThat(database.countType(TYPE, false)).isEqualTo(2L);
  }
}
