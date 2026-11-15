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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11921 review: a commit published outside the log takes the validation lock of the ledger of the state machine
 * current at its first version check. An in-place Ratis restart replaces the state machine, and with it the ledger, so a
 * publication in flight across the swap was not ordered against the validations of the new state machine: one validated
 * between the publication's check and its publication could reserve the version it published. The restart now waits for
 * the publications in flight before it builds the new state machine, and a publication started after it runs on the new one.
 */
class Issue11921RestartWaitsForPublicationIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void anInPlaceRestartDoesNotReplaceTheStateMachineUnderAPublicationInFlight() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final RaftHAServer raft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final ArcadeStateMachine before = raft.getStateMachine();
    final DatabaseInternal db = (DatabaseInternal) getServerDatabase(leaderIndex, getDatabaseName());

    final CountDownLatch publishing = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicReference<Throwable> publicationFailure = new AtomicReference<>();
    final Thread publication = new Thread(() -> {
      try {
        before.publishOutsideTheLog(db, () -> {
          publishing.countDown();
          return release.await(60, TimeUnit.SECONDS);
        });
      } catch (final Throwable t) {
        publicationFailure.set(t);
      }
    }, "issue11921-publication");
    publication.start();
    assertThat(publishing.await(10, TimeUnit.SECONDS)).isTrue();

    final Thread restart = new Thread(raft::restartRatisIfNeeded, "issue11921-restart");
    restart.start();

    // The close of the old Ratis server takes milliseconds in-process: a restart that does not wait swaps well within this
    boolean replacedDuringPublication = false;
    final long deadline = System.currentTimeMillis() + 5_000;
    while (System.currentTimeMillis() < deadline && !replacedDuringPublication) {
      replacedDuringPublication = raft.getStateMachine() != before;
      Thread.sleep(20);
    }
    LogManager.instance().log(this, Level.INFO, "TEST: state machine replaced while the publication was in flight=%s",
        replacedDuringPublication);

    release.countDown();
    publication.join(30_000);
    restart.join(60_000);

    assertThat(replacedDuringPublication)
        .as("the restart replaced the state machine, and its ledger, while a publication was in flight").isFalse();
    assertThat(publicationFailure.get()).isNull();
    assertThat(raft.getStateMachine()).as("the restart goes on once the publication returned").isNotSameAs(before);

    waitForAllServers();
    assertClusterConsistency();
  }
}
