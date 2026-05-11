/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression IT for issue #4147: once the cluster has committed at least one Raft entry, restarts
 * must NOT re-engage the bootstrap election. The gate in {@link BootstrapElection#runIfEligible()}
 * is keyed on the Raft commit index being 0 at the time of leader change, so any subsequent
 * leader-change callback (including the one fired after a restart catches up) must return a
 * non-COMMITTED outcome and the previously-committed baseline must stay unchanged.
 */
class RaftBootstrapDoesNotEngageOnRestartIT extends BaseRaftHATest {

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void restartDoesNotReengageBootstrap() {
    final String dbName = getDatabaseName();

    // Wait for the initial bootstrap entry to commit and apply on every peer.
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(250, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          for (int i = 0; i < getServerCount(); i++) {
            final RaftHAPlugin plugin = getRaftPlugin(i);
            assertThat(plugin).as("server %d Raft plugin", i).isNotNull();
            assertThat(plugin.getRaftHAServer().getStateMachine().getBootstrapBaseline(dbName))
                .as("server %d should have applied the initial bootstrap baseline", i)
                .isNotNull();
          }
        });

    final var originalBaseline = getRaftPlugin(0).getRaftHAServer()
        .getStateMachine().getBootstrapBaseline(dbName);
    final long originalLastTxId = originalBaseline.lastTxId();
    final String originalFingerprint = originalBaseline.fingerprint();

    // Restart a non-leader peer with persistent Raft storage. Leader remains stable.
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int restartIndex = (leaderIndex + 1) % getServerCount();
    restartServer(restartIndex);

    // After restart, the bootstrap baseline must survive log replay unchanged.
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(250, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          final var afterRestart = getRaftPlugin(restartIndex).getRaftHAServer()
              .getStateMachine().getBootstrapBaseline(dbName);
          assertThat(afterRestart).as("baseline must be re-applied from Raft log on restart").isNotNull();
          assertThat(afterRestart.lastTxId()).isEqualTo(originalLastTxId);
          assertThat(afterRestart.fingerprint()).isEqualTo(originalFingerprint);
        });

    // Forcing the protocol on the current leader must NOT commit a new baseline: the gate is
    // commitIndex > 0 which has been true since first formation. We accept either
    // SKIPPED_NOT_FIRST_FORMATION (the normal outcome) or NOT_LEADER (test-flake window during a
    // post-restart election shuffle); both prove the protocol declined to commit.
    final int currentLeaderIndex = findLeaderIndex();
    assertThat(currentLeaderIndex).isGreaterThanOrEqualTo(0);
    final BootstrapElection.Outcome outcome = getRaftPlugin(currentLeaderIndex)
        .getRaftHAServer().runBootstrapIfEligible();
    assertThat(outcome).isIn(
        BootstrapElection.Outcome.SKIPPED_NOT_FIRST_FORMATION,
        BootstrapElection.Outcome.NOT_LEADER);

    // And nothing changed: same baseline visible everywhere.
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin == null)
        continue;
      final var baseline = plugin.getRaftHAServer().getStateMachine().getBootstrapBaseline(dbName);
      assertThat(baseline)
          .as("server %d baseline must persist after restart + manual re-run", i)
          .isNotNull();
      assertThat(baseline.lastTxId()).isEqualTo(originalLastTxId);
      assertThat(baseline.fingerprint()).isEqualTo(originalFingerprint);
    }
  }
}
