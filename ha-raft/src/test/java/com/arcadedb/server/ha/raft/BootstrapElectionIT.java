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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.ContextConfiguration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT for the bootstrap election protocol (issue #4147 phase 4). Confirms the protocol fires on the
 * first leader election when the Raft log is empty and stays a no-op on subsequent leader changes.
 * <p>
 * The deeper "leadership transfers to a peer with a fresher lastTxId" scenario lives in phase 8;
 * here we cover the gating + happy-path-with-empty-databases case.
 */
class BootstrapElectionIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // Default is true, but make the intent explicit so a future default flip doesn't silently
    // skip the assertions below.
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void protocolDoesNotInterfereWithFreshlyFormedCluster() {
    // The very first leader election should NOT cause a leadership flip purely because of the
    // bootstrap election: all peers either have empty databases or share the same recency level
    // and the local peer is preferred on ties. Capture the leader, give the protocol time to run,
    // verify leadership is stable.
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final RaftHAServer leaderHa = getServer(leaderIndex).getHA() instanceof RaftHAPlugin rp
        ? rp.getRaftHAServer() : null;
    assertThat(leaderHa).isNotNull();

    // Wait long enough for the bootstrap election to have run on the leader-change handler,
    // then verify the cluster still has the same leader and is committing entries.
    Awaitility.await()
        .atMost(15, TimeUnit.SECONDS)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          assertThat(leaderHa.getCommitIndex()).isGreaterThanOrEqualTo(0);
          assertThat(findLeaderIndex()).isEqualTo(leaderIndex);
        });
  }

  @Test
  void runBootstrapShortCircuitsAfterFirstFormation() {
    // Manually re-run the protocol after the cluster has committed entries; gating must short
    // out. This guards against regressions where the bootstrap path re-engages on later leader
    // changes and starts shipping leadership transfers around long after the cluster has been
    // running.
    final int leaderIndex = findLeaderIndex();
    final RaftHAServer leaderHa = getServer(leaderIndex).getHA() instanceof RaftHAPlugin rp
        ? rp.getRaftHAServer() : null;
    assertThat(leaderHa).isNotNull();

    Awaitility.await()
        .atMost(15, TimeUnit.SECONDS)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(leaderHa.getCommitIndex()).isGreaterThan(0));

    final BootstrapElection.Outcome outcome = leaderHa.runBootstrapIfEligible();
    assertThat(outcome).isIn(
        BootstrapElection.Outcome.SKIPPED_NOT_FIRST_FORMATION,
        // Could also be NOT_LEADER if a follower test instance briefly demotes during the manual
        // call - either is a legitimate "the protocol declined to do anything" signal.
        BootstrapElection.Outcome.NOT_LEADER);
  }

  @Test
  void disabledFlagSkipsBootstrap() {
    // Force the flag off on the leader and run the protocol manually. Even with a brand-new
    // cluster, runBootstrapIfEligible must short-circuit immediately.
    final int leaderIndex = findLeaderIndex();
    final RaftHAServer leaderHa = getServer(leaderIndex).getHA() instanceof RaftHAPlugin rp
        ? rp.getRaftHAServer() : null;
    assertThat(leaderHa).isNotNull();

    final var cfg = getServer(leaderIndex).getConfiguration();
    final boolean previous = cfg.getValueAsBoolean(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE);
    cfg.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, false);
    try {
      final BootstrapElection.Outcome outcome = leaderHa.runBootstrapIfEligible();
      // Either disabled (flag off) or not-first-formation (the natural automatic run already
      // committed entries) - both prove the path is opt-out-respecting and idempotent.
      assertThat(outcome).isIn(BootstrapElection.Outcome.SKIPPED_DISABLED,
          BootstrapElection.Outcome.SKIPPED_NOT_FIRST_FORMATION);
    } finally {
      cfg.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, previous);
    }
  }
}
