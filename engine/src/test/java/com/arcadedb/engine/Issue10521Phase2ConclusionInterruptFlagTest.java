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
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.IndexReplayConclusion;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10521: the rule of #10416 is "no thread concludes a failed phase 2 with the interrupt flag set", because the rollback reloads
 * records and the dictionary from the files, and a channel read by a thread with the flag up is closed under it
 * ({@code ClosedByInterruptException}). The implementation only restored a flag that the {@link InterruptedException} had
 * consumed; the WAL write sets the flag itself before it throws, so the conclusion ran with it up.
 * <p>
 * The flag is read where the conclusion runs, on the transaction's own thread: an {@link IndexReplayConclusion} registered on the
 * transaction is called by the rollback ({@code undoIndexReplay}) and by the reset of every other conclusion
 * ({@code publishIndexReplay}), both inside {@code concludePhase2}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10521Phase2ConclusionInterruptFlagTest extends TestHelper {
  private static final String TYPE                  = "Doc10521";
  /** Bounds only waits expected to succeed, so a generous value cannot turn a passing run red (#6260). */
  private static final long   ASSERTION_TIMEOUT_SEC = 60;

  /** Records the interrupt flag of the thread that runs the conclusion. */
  private static final class FlagProbe implements IndexReplayConclusion {
    final AtomicBoolean undone               = new AtomicBoolean();
    final AtomicBoolean published            = new AtomicBoolean();
    final AtomicBoolean flagSetDuringUndo    = new AtomicBoolean();
    final AtomicBoolean flagSetDuringPublish = new AtomicBoolean();

    @Override
    public void undoIndexReplay() {
      flagSetDuringUndo.set(Thread.currentThread().isInterrupted());
      undone.set(true);
    }

    @Override
    public void publishIndexReplay() {
      flagSetDuringPublish.set(Thread.currentThread().isInterrupted());
      published.set(true);
    }
  }

  private IndexInternal prepareSchema() {
    // THE FIRST DOCUMENT REGISTERS THE PROPERTY NAME IN THE DICTIONARY: A NEW NAME IS A NESTED COMMIT OF ITS OWN. THE INDEX IS ON A
    // PROPERTY NO TEST WRITES, SO IT NEVER REGISTERS A CONCLUSION OF ITS OWN
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE).createProperty("k", Integer.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE, "k");
      database.newDocument(TYPE).set("v", 1).save();
    });
    return (IndexInternal) database.getSchema().getIndexByName(TYPE + "[k]");
  }

  @Test
  void aCommitCancelledInTheWalWriteConcludesWithTheFlagCleared() throws Exception {
    final IndexInternal index = prepareSchema();
    final DatabaseInternal db = (DatabaseInternal) database;
    final TransactionManager tm = db.getTransactionManager();

    // A RETIRED FILE, NOT AN EMPTY SLOT: AN EMPTY SLOT IS GIVEN A FRESH FILE BY THE FIRST COMMIT THAT NEEDS IT (#10234), A RETIRED ONE
    // REFUSES THE WRITE, SO THE COMMIT SLEEPS IN THE RETRY LOOP OF writeTransactionToWAL UNTIL IT IS INTERRUPTED
    final Field poolField = TransactionManager.class.getDeclaredField("activeWALFilePool");
    poolField.setAccessible(true);
    final WALFile[] original = (WALFile[]) poolField.get(tm);
    final WALFile retired = new WALFile(db.getDatabasePath() + File.separator + "issue10521-test.wal");
    retired.setActive(false);

    final FlagProbe probe = new FlagProbe();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicBoolean interruptedAfterTheFailure = new AtomicBoolean();
    poolField.set(tm, new WALFile[] { retired });
    try {
      final Thread committer = new Thread(() -> {
        database.begin();
        database.newDocument(TYPE).set("v", 2).save();
        db.getTransaction().addIndexReplayConclusion(index, probe);
        try {
          database.commit();
        } catch (final Throwable e) {
          failure.set(e);
        }
        interruptedAfterTheFailure.set(Thread.currentThread().isInterrupted());
      }, "issue10521-committer");
      committer.start();

      final long parkDeadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC);
      while (!isSleepingInTheWalWrite(committer) && committer.isAlive() && System.currentTimeMillis() < parkDeadline)
        Thread.sleep(5);
      assertThat(isSleepingInTheWalWrite(committer)).as("the committer must be sleeping in the retry loop of the WAL write").isTrue();

      committer.interrupt();
      committer.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
      assertThat(committer.isAlive()).isFalse();
    } finally {
      poolField.set(tm, original);
      retired.drop();
    }

    // THE SHAPE THE FIX HAS TO COVER: THE WAL WRITE RAISES THE INTERRUPT AS A TransactionException AFTER SETTING THE FLAG ITSELF
    assertThat(failure.get()).isInstanceOf(TransactionException.class);
    assertThat(failure.get().getCause()).isInstanceOf(InterruptedException.class);
    assertThat(probe.undone.get()).as("the failure happened before anything was durable, so it is rolled back").isTrue();
    assertThat(probe.flagSetDuringUndo.get()).as("the rollback must not read files with the interrupt flag up").isFalse();
    assertThat(interruptedAfterTheFailure.get()).as("the cancellation must survive the failed commit").isTrue();
    assertThat(database.countType(TYPE, false)).isEqualTo(1L);
  }

  @Test
  void concludingAFailedPhase2ClearsTheFlagAndHandsItBack() {
    final IndexInternal index = prepareSchema();
    final DatabaseInternal db = (DatabaseInternal) database;

    database.begin();
    database.newDocument(TYPE).set("v", 2).save();
    final TransactionContext tx = db.getTransaction();
    final FlagProbe probe = new FlagProbe();
    tx.addIndexReplayConclusion(index, probe);
    tx.commit1stPhase(true);

    Thread.currentThread().interrupt();
    try {
      tx.concludeFailedPhase2(new TransactionException("phase 2 failed"));
      assertThat(Thread.currentThread().isInterrupted()).as("the flag of the caller is handed back").isTrue();
    } finally {
      Thread.interrupted();
    }

    assertThat(probe.undone.get()).isTrue();
    assertThat(probe.flagSetDuringUndo.get()).as("the rollback must not read files with the interrupt flag up").isFalse();
    assertThat(database.countType(TYPE, false)).isEqualTo(1L);
  }

  @Test
  void completingACommitClearsTheFlagAndHandsItBack() {
    final IndexInternal index = prepareSchema();
    final DatabaseInternal db = (DatabaseInternal) database;

    database.begin();
    database.newDocument(TYPE).set("v", 3).save();
    final TransactionContext tx = db.getTransaction();
    final FlagProbe probe = new FlagProbe();
    tx.addIndexReplayConclusion(index, probe);
    final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
    tx.publishCommittedPages(phase1);

    Thread.currentThread().interrupt();
    try {
      tx.completeCommit();
      assertThat(Thread.currentThread().isInterrupted()).as("the flag of the caller is handed back").isTrue();
    } finally {
      Thread.interrupted();
    }

    assertThat(probe.published.get()).isTrue();
    assertThat(probe.flagSetDuringPublish.get()).as("the conclusion must not touch files with the interrupt flag up").isFalse();
    assertThat(database.countType(TYPE, false)).isEqualTo(2L);
  }

  @Test
  void aFlagThatIsNotSetStaysNotSet() {
    final IndexInternal index = prepareSchema();
    final DatabaseInternal db = (DatabaseInternal) database;

    database.begin();
    database.newDocument(TYPE).set("v", 2).save();
    final TransactionContext tx = db.getTransaction();
    tx.addIndexReplayConclusion(index, new FlagProbe());
    tx.commit1stPhase(true);

    tx.concludeFailedPhase2(new TransactionException("phase 2 failed"));

    assertThat(Thread.currentThread().isInterrupted()).as("the conclusion must not invent a cancellation").isFalse();
    assertThat(database.countType(TYPE, false)).isEqualTo(1L);
  }

  private static boolean isSleepingInTheWalWrite(final Thread thread) {
    return thread.getState() == Thread.State.TIMED_WAITING && Arrays.stream(thread.getStackTrace())
        .anyMatch(e -> e.getMethodName().equals("writeTransactionToWAL"));
  }
}
