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
 * Regression IT for issue #4147 source-picking: when peers report different {@code lastTxId} values
 * at first cluster formation, the bootstrap protocol must elect the peer with the highest one as
 * the source.
 * <p>
 * The fixture exploits that {@code last-tx-id.bin} is read on database open and is NOT included in
 * the {@link com.arcadedb.database.BootstrapFingerprint} surface. Writing different long values per
 * server's {@code last-tx-id.bin} before the server starts varies only the recency signal that the
 * bootstrap protocol uses to pick the source; the fingerprints stay identical so the source's
 * choice is unambiguous and falls back to the {@code lastTxId} tiebreak.
 */
class RaftBootstrapPicksHighestLastTxIdIT extends BaseRaftHATest {

  // Pre-staged lastTxId per server. Server 2 has the highest and must be picked as the source.
  private static final long[] PRE_STAGED_LAST_TX_IDS = { 100L, 200L, 300L };

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
  void highestLastTxIdPeerBecomesBootstrapSource() {
    final String dbName = getDatabaseName();

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

    // Every peer must agree on the same committed baseline (single Raft entry, deterministic apply).
    final var refBaseline = getRaftPlugin(0).getRaftHAServer().getStateMachine().getBootstrapBaseline(dbName);
    for (int i = 1; i < getServerCount(); i++) {
      final var baseline = getRaftPlugin(i).getRaftHAServer().getStateMachine().getBootstrapBaseline(dbName);
      assertThat(baseline.lastTxId()).as("server %d baseline lastTxId agreement", i)
          .isEqualTo(refBaseline.lastTxId());
      assertThat(baseline.fingerprint()).as("server %d baseline fingerprint agreement", i)
          .isEqualTo(refBaseline.fingerprint());
    }

    // The baseline's lastTxId must be at least the highest pre-staged value, proving the protocol
    // picked the right peer. It MUST exceed every other pre-staged value, otherwise the source
    // picking is wrong.
    final long highest = PRE_STAGED_LAST_TX_IDS[2];
    final long secondHighest = PRE_STAGED_LAST_TX_IDS[1];
    assertThat(refBaseline.lastTxId())
        .as("baseline lastTxId must be >= server 2's pre-staged value (the highest)")
        .isGreaterThanOrEqualTo(highest);
    assertThat(refBaseline.lastTxId())
        .as("baseline lastTxId must be > server 1's pre-staged value (proves source != server 1)")
        .isGreaterThan(secondHighest);
  }
}
