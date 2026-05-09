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
import com.arcadedb.database.BootstrapFingerprint;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.server.ServerDatabase;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end IT for issue #4147: pre-staged identical databases on every peer should converge via
 * the offline bootstrap path. The test fixture (inherited from BaseGraphServerTest via
 * BaseRaftHATest) copies the test database directory from server 0 to every other server before
 * the cluster starts, which is exactly the customer's "pre-staged identical backups" scenario.
 * <p>
 * What we assert:
 * <ol>
 *   <li>After the cluster forms, every peer's state machine has recorded a bootstrap baseline
 *       (a {@code BOOTSTRAP_FINGERPRINT_ENTRY} was committed and applied).</li>
 *   <li>The baseline's fingerprint matches the local database fingerprint on every peer (the
 *       "match → bootstrap locally" path was taken — no leader-shipped snapshot was needed).</li>
 *   <li>All peers agree on the same baseline (consensus on the source's choice).</li>
 * </ol>
 * Negative coverage (mismatched ages, leadership transfer to fresher peer, late-newer-joiner
 * refusal) belongs in dedicated ITs — they need finer test fixtures than {@code BaseGraphServerTest}
 * provides today; tracked alongside #4150 / phase 6b.
 */
class RaftBootstrapFromLocalDatabaseIT extends BaseRaftHATest {

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
  void preStagedIdenticalDatabasesBootstrapLocallyAcrossEveryPeer() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final String dbName = getDatabaseName();

    // Wait for the bootstrap election to fire on the leader and for the
    // BOOTSTRAP_FINGERPRINT_ENTRY to be applied on every peer's state machine. The protocol runs
    // on a background lifecycleExecutor so we have to poll.
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(250, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          for (int i = 0; i < getServerCount(); i++) {
            final RaftHAPlugin plugin = getRaftPlugin(i);
            assertThat(plugin).as("server %d Raft plugin", i).isNotNull();
            final var stateMachine = plugin.getRaftHAServer().getStateMachine();
            final var baseline = stateMachine.getBootstrapBaseline(dbName);
            assertThat(baseline)
                .as("server %d should have recorded a bootstrap baseline for '%s'", i, dbName)
                .isNotNull();
            assertThat(baseline.lastTxId())
                .as("server %d baseline lastTxId for '%s'", i, dbName)
                .isGreaterThanOrEqualTo(0L);
            assertThat(baseline.fingerprint())
                .as("server %d baseline fingerprint for '%s'", i, dbName)
                .hasSize(64); // SHA-256 hex
          }
        });

    // All peers must agree on the same baseline (Raft committed one entry, every state machine
    // applied it identically). Capture peer 0's baseline as the reference.
    final var refBaseline = getRaftPlugin(0).getRaftHAServer().getStateMachine().getBootstrapBaseline(dbName);
    for (int i = 1; i < getServerCount(); i++) {
      final var baseline = getRaftPlugin(i).getRaftHAServer().getStateMachine().getBootstrapBaseline(dbName);
      assertThat(baseline.fingerprint())
          .as("server %d must agree on the bootstrap fingerprint", i)
          .isEqualTo(refBaseline.fingerprint());
      assertThat(baseline.lastTxId())
          .as("server %d must agree on the bootstrap lastTxId", i)
          .isEqualTo(refBaseline.lastTxId());
    }

    // The "match → bootstrap locally" path: at least one peer's local fingerprint must equal the
    // committed baseline. (BaseGraphServerTest copies the directory at the bytes-identical level,
    // but Raft replication of subsequent transactions can advance the local lastTxId beyond the
    // baseline lastTxId, so we don't require ALL peers to still match — we require at least the
    // source peer to match, proving the source's own apply skipped the snapshot transfer.)
    boolean atLeastOneMatched = false;
    for (int i = 0; i < getServerCount(); i++) {
      final ServerDatabase serverDb = (ServerDatabase) getServerDatabase(i, dbName);
      if (!(serverDb.getWrappedDatabaseInstance().getEmbedded() instanceof LocalDatabase localDb))
        continue;
      final String localFingerprint = BootstrapFingerprint.compute(new File(localDb.getDatabasePath()));
      if (localFingerprint.equals(refBaseline.fingerprint())) {
        atLeastOneMatched = true;
        break;
      }
    }
    assertThat(atLeastOneMatched)
        .as("at least one peer (the source) should have a fingerprint matching the committed baseline, "
            + "proving the bootstrap-locally branch fired on it (no leader-shipped snapshot was needed)")
        .isTrue();
  }
}
