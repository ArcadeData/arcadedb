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
import org.junit.jupiter.api.Test;

import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12022: the #11746 branch that writes the WAL record of a replicated commit without forcing it
 * ({@code TransactionContext.publishReplicatedCommittedPages}) was pinned only by an HA integration test that ran nightly. This
 * drives the phased commit an HA leader runs - 1st phase, publication, completion - on a plain database with
 * {@code txWalFlush} 1, through both publications: the replicated one must force no record, the local one (the control, same
 * path, same thread, same transaction context) one per commit. Reverting the branch makes the first assertion fail.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ReplicatedPublicationWalFlushTest extends TestHelper {
  private static final int COMMITS = 20;

  @Test
  void aReplicatedPublicationWritesItsWalRecordWithoutForcingIt() throws Exception {
    database.getSchema().createDocumentType("Replicated");
    // the first record of the type, out of the measure: what it creates beside the record is saved by an internal commit
    database.transaction(() -> database.newDocument("Replicated").set("k", -1).save());

    // read when a thread's transaction context is created: the commits run on a thread that has none yet
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_GROUP_COMMIT, false);
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 1);
    try {
      onAFreshThread(() -> {
        database.begin();
        try {
          assertThat(transaction().getWALFlush()).as("the setting this test is about must be in force")
              .isEqualTo(WALFile.FlushType.YES_NOMETADATA);
        } finally {
          database.rollback();
        }

        final long forcedBefore = stat("forcedWALWrites");
        final long bytesBefore = stat("bytesWritten");
        commitInPhases(TransactionContext::publishReplicatedCommittedPages);
        assertThat(stat("bytesWritten")).as("a replicated publication still writes the WAL record of each commit")
            .isGreaterThan(bytesBefore);
        assertThat(stat("forcedWALWrites")).as("a replicated publication forces no WAL record").isEqualTo(forcedBefore);

        // the control: the local publication on the same context forces every record, so the flag did not outlive the
        // replicated publications and the statistic does move on this path
        final long forcedLocal = stat("forcedWALWrites");
        commitInPhases(TransactionContext::publishCommittedPages);
        assertThat(stat("forcedWALWrites") - forcedLocal).as("a local publication forces the WAL record of each commit")
            .isGreaterThanOrEqualTo(COMMITS);
      });
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 0);
      database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_GROUP_COMMIT, true);
    }

    assertThat(database.countType("Replicated", false)).isEqualTo(2L * COMMITS + 1);
  }

  /** The commit {@code RaftReplicatedDatabase} runs on the leader, with the publication the test names. */
  private void commitInPhases(final BiConsumer<TransactionContext, TransactionContext.TransactionPhase1> publication) {
    for (int i = 0; i < COMMITS; i++) {
      final int k = i;
      database.begin();
      database.newDocument("Replicated").set("k", k).save();
      ((DatabaseInternal) database).executeInReadLock(() -> {
        final TransactionContext tx = transaction();
        final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
        publication.accept(tx, phase1);
        tx.completeCommit();
        return null;
      });
    }
  }

  private TransactionContext transaction() {
    return ((DatabaseInternal) database).getTransaction();
  }

  private long stat(final String name) {
    return (Long) ((DatabaseInternal) database).getTransactionManager().getStats().get(name);
  }

  private static void onAFreshThread(final Runnable body) throws Exception {
    final Throwable[] failure = new Throwable[1];
    final Thread thread = new Thread(() -> {
      try {
        body.run();
      } catch (final Throwable t) {
        failure[0] = t;
      }
    });
    thread.start();
    thread.join();
    assertThat(failure[0]).isNull();
  }
}
