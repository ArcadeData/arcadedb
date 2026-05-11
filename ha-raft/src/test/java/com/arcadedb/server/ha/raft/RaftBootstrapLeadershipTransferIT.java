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
import com.arcadedb.server.ArcadeDBServer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression IT for issue #4147 leadership-transfer step: if the elected bootstrap source is NOT
 * the current Raft leader, the protocol must transfer leadership to the source before committing
 * {@code BOOTSTRAP_FINGERPRINT_ENTRY}. Pre-stages a higher {@code lastTxId} on the last peer so the
 * source is unambiguously not the initial leader candidate.
 */
class RaftBootstrapLeadershipTransferIT extends BaseRaftHATest {

  private static final long[] PRE_STAGED_LAST_TX_IDS = { 100L, 100L, 500L };
  private static final int    EXPECTED_LEADER_INDEX  = 2;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    super.onBeforeStarting(server);
    final String name = server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int idx = Integer.parseInt(name.substring(name.lastIndexOf('_') + 1));
    final String dbDir = server.getConfiguration()
        .getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY)
        + File.separator + getDatabaseName();
    final File lastTxIdFile = new File(dbDir, "last-tx-id.bin");
    try (final DataOutputStream out = new DataOutputStream(new FileOutputStream(lastTxIdFile))) {
      out.writeLong(PRE_STAGED_LAST_TX_IDS[idx]);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to pre-stage last-tx-id.bin for server " + idx, e);
    }
  }

  @Test
  void leadershipTransfersToTheFresherPeerBeforeBootstrapCommit() {
    final String dbName = getDatabaseName();

    // Wait for the bootstrap protocol to commit the entry on every peer. The protocol runs:
    //   1) initial leader collects state -> picks server 2 as source (highest lastTxId)
    //   2) leadership transfers to server 2
    //   3) server 2 re-runs the protocol on its own term and commits BOOTSTRAP_FINGERPRINT_ENTRY
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(250, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          for (int i = 0; i < getServerCount(); i++) {
            final RaftHAPlugin plugin = getRaftPlugin(i);
            assertThat(plugin).as("server %d Raft plugin", i).isNotNull();
            assertThat(plugin.getRaftHAServer().getStateMachine().getBootstrapBaseline(dbName))
                .as("server %d baseline", i).isNotNull();
          }
        });

    // After the protocol settles, the Raft leader must be the bootstrap source (server 2). If the
    // initial election picked a different leader, the protocol transferred leadership; if it picked
    // server 2 directly, no transfer was needed but the assertion still holds.
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(250, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(findLeaderIndex())
            .as("bootstrap source (server %d) must hold leadership after the protocol", EXPECTED_LEADER_INDEX)
            .isEqualTo(EXPECTED_LEADER_INDEX));

    // And the committed baseline's lastTxId must come from server 2 (>= its pre-staged value,
    // strictly greater than every other peer's value).
    final var baseline = getRaftPlugin(EXPECTED_LEADER_INDEX).getRaftHAServer()
        .getStateMachine().getBootstrapBaseline(dbName);
    assertThat(baseline.lastTxId())
        .as("baseline must come from server %d (lastTxId >= %d)",
            EXPECTED_LEADER_INDEX, PRE_STAGED_LAST_TX_IDS[EXPECTED_LEADER_INDEX])
        .isGreaterThanOrEqualTo(PRE_STAGED_LAST_TX_IDS[EXPECTED_LEADER_INDEX]);
  }
}
