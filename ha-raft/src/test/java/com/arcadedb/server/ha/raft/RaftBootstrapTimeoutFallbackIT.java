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
 * Regression IT for issue #4147 timeout fallback: when {@code HA_SERVER_LIST} advertises a peer
 * that never comes up, the bootstrap leader must time out its {@code POST /api/v1/cluster/bootstrap-state}
 * RPC after {@code HA_BOOTSTRAP_TIMEOUT_MS} and proceed with the state from the responsive
 * majority. The committed baseline must still be applied on every responsive peer.
 * <p>
 * The fixture advertises 3 peers in {@code HA_SERVER_LIST} but only starts 2. The third address
 * points at a localhost port nobody listens on, so the bootstrap-state RPC connect attempt fails
 * fast (refused) - which exercises the same fall-through code path as a slow timeout, just
 * faster.
 */
class RaftBootstrapTimeoutFallbackIT extends BaseRaftHATest {

  private static final int  UNREACHABLE_RAFT_PORT = 12434;
  private static final int  UNREACHABLE_HTTP_PORT = 12480;
  private static final long SHORT_BOOTSTRAP_TIMEOUT_MS = 5_000L;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_TIMEOUT_MS, SHORT_BOOTSTRAP_TIMEOUT_MS);
  }

  @Override
  protected int getServerCount() {
    // Only 2 actual processes; the third advertised address is unreachable.
    return 2;
  }

  @Override
  protected String getServerAddresses() {
    // Append a never-running third peer so the bootstrap leader has a peer to time out on.
    return super.getServerAddresses() + ",localhost:" + UNREACHABLE_RAFT_PORT + ":" + UNREACHABLE_HTTP_PORT;
  }

  @Test
  void bootstrapProceedsDespiteOneUnreachablePeer() {
    final String dbName = getDatabaseName();

    // The protocol must still commit BOOTSTRAP_FINGERPRINT_ENTRY on the 2 responsive peers despite
    // the third HA_SERVER_LIST entry being a black hole.
    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          for (int i = 0; i < getServerCount(); i++) {
            final RaftHAPlugin plugin = getRaftPlugin(i);
            assertThat(plugin).as("server %d Raft plugin", i).isNotNull();
            assertThat(plugin.getRaftHAServer().getStateMachine().getBootstrapBaseline(dbName))
                .as("server %d should still commit a baseline despite an unreachable peer", i)
                .isNotNull();
          }
        });

    // Responsive peers must agree on the baseline.
    final var b0 = getRaftPlugin(0).getRaftHAServer().getStateMachine().getBootstrapBaseline(dbName);
    final var b1 = getRaftPlugin(1).getRaftHAServer().getStateMachine().getBootstrapBaseline(dbName);
    assertThat(b0.lastTxId()).isEqualTo(b1.lastTxId());
    assertThat(b0.fingerprint()).isEqualTo(b1.fingerprint());
  }
}
