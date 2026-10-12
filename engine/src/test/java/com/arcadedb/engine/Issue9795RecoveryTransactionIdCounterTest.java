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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9795: after a WAL replay the transaction counter must be past EVERY transaction id that was ever handed out,
 * because each of them may be durable. Recovery used to set it from the transaction it replayed LAST, which is the
 * highest only when every WAL file holds its transactions in id order - and none does: an id is allocated at the end
 * of {@code commit1stPhase} and the record is appended in phase 2, so two commits that share no file can append in
 * the opposite order to their ids, into one WAL file whenever their thread ids hash to the same slot (always, with
 * {@code arcadedb.txWalFiles=1}). It also threw away the marker persisted by a runtime WAL rotation whenever that
 * marker was higher than what the surviving WAL files held.
 * <p>
 * Both tests pin the claim, not the shape: they build the WAL out of order through the public commit phases, crash
 * without a clean close and assert the recovered counter equals the highest id handed out before the crash.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9795RecoveryTransactionIdCounterTest extends TestHelper {
  private static final String TYPE_A = "Issue9795A";
  private static final String TYPE_B = "Issue9795B";

  private Object previousWalFiles;

  @BeforeEach
  void singleWalFile() {
    // ONE WAL FILE: THE TWO RECORDS ARE CERTAIN TO SHARE IT. WITH THE DEFAULT POOL THE SAME HAPPENS ON A HASH COLLISION
    previousWalFiles = GlobalConfiguration.TX_WAL_FILES.getValue();
    GlobalConfiguration.TX_WAL_FILES.setValue(1);
    database.close();
    database = factory.open();

    database.transaction(() -> {
      database.getSchema().getOrCreateDocumentType(TYPE_A);
      database.getSchema().getOrCreateDocumentType(TYPE_B);
    });
    // THE PROPERTY NAME IS IN THE DICTIONARY BEFORE THE PAIR, SO THE TWO TRANSACTIONS BELOW SHARE NO FILE AND NO LOCK
    database.transaction(() -> {
      database.newDocument(TYPE_A).set("k", 0).save();
      database.newDocument(TYPE_B).set("k", 0).save();
    });
  }

  @AfterEach
  void restoreWalFiles() {
    GlobalConfiguration.TX_WAL_FILES.setValue(previousWalFiles);
  }

  @Test
  void recoveryOfAnOutOfOrderWalSetsTheCounterPastTheHighestReplayedId() throws Exception {
    final long before = lastTransactionId();

    // A ALLOCATES before+1, B ALLOCATES AND APPENDS before+2, THEN A APPENDS: THE FILE HOLDS [before+2, before+1]
    commitInterleaved(() -> database.transaction(() -> database.newDocument(TYPE_B).set("k", 1).save()));

    final long highest = before + 2;
    assertThat(lastTransactionId()).isEqualTo(highest);

    crashAndReopen();

    assertThat(database.countType(TYPE_A, false)).as("A's transaction was replayed").isEqualTo(2);
    assertThat(database.countType(TYPE_B, false)).as("B's transaction was replayed").isEqualTo(2);
    assertThat(lastTransactionId())
        .as("the counter must be past every replayed transaction, not past the one replayed last")
        .isEqualTo(highest);

    database.transaction(() -> database.newDocument(TYPE_A).set("k", 2).save());
    assertThat(lastTransactionId()).as("the next commit must get a fresh id, not one already durable")
        .isEqualTo(highest + 1);
  }

  @Test
  void recoveryNeverMovesTheCounterBelowTheMarkerARotationPersisted() throws Exception {
    final long before = lastTransactionId();

    // B COMMITS before+2 WHILE A HOLDS before+1, THEN A RUNTIME ROTATION DROPS B'S WAL FILE AND PERSISTS THE MARKER AT
    // before+2: A'S RECORD LANDS ALONE IN THE NEW FILE, SO THE REPLAY SEES ONLY before+1
    commitInterleaved(() -> {
      database.transaction(() -> database.newDocument(TYPE_B).set("k", 1).save());
      try {
        ((DatabaseInternal) database).getTransactionManager().rotateAndDropWALForTesting();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });

    final long highest = before + 2;
    assertThat(lastTransactionId()).isEqualTo(highest);

    crashAndReopen();

    assertThat(database.countType(TYPE_A, false)).isEqualTo(2);
    assertThat(database.countType(TYPE_B, false)).isEqualTo(2);
    assertThat(lastTransactionId())
        .as("the replay must not overwrite a higher persisted marker: B's id is durable even though its WAL is gone")
        .isEqualTo(highest);

    database.transaction(() -> database.newDocument(TYPE_A).set("k", 2).save());
    assertThat(lastTransactionId()).isEqualTo(highest + 1);
  }

  /**
   * Runs phase 1 of an insert into {@link #TYPE_A} on another thread (allocating its id), then {@code betweenPhases}
   * on this thread, then A's phase 2 (appending its WAL record).
   */
  private void commitInterleaved(final Runnable betweenPhases) throws Exception {
    final CountDownLatch phase1Done = new CountDownLatch(1);
    final CountDownLatch resumeA = new CountDownLatch(1);

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<?> a = executor.submit(() -> {
      database.begin();
      database.newDocument(TYPE_A).set("k", 1).save();
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      phase1Done.countDown();
      try {
        assertThat(resumeA.await(30, TimeUnit.SECONDS)).isTrue();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      tx.commit2ndPhase(phase1);
    });

    try {
      assertThat(phase1Done.await(30, TimeUnit.SECONDS)).isTrue();
      try {
        betweenPhases.run();
      } finally {
        resumeA.countDown();
      }
      a.get(30, TimeUnit.SECONDS);
    } finally {
      // ALSO ON A TIMEOUT OR A FAILED CALLBACK: NEVER LEAVE A'S THREAD PARKED OR ALIVE PAST THE TEST
      resumeA.countDown();
      executor.shutdownNow();
    }
  }

  private long lastTransactionId() {
    return ((DatabaseInternal) database).getTransactionManager().getLastTransactionId();
  }

  private void crashAndReopen() {
    ((DatabaseInternal) database).kill();
    final File[] walFiles = new File(getDatabasePath()).listFiles((dir, name) -> name.endsWith(".wal"));
    assertThat(walFiles).as("the crash must leave the WAL for the next open to replay").isNotEmpty();
    // UNREGISTERS THE KILLED INSTANCE ONLY: THE LOCK FILE AND THE WAL STAY, SO THE OPEN BELOW RUNS RECOVERY
    database.close();
    database = factory.open();
  }
}
