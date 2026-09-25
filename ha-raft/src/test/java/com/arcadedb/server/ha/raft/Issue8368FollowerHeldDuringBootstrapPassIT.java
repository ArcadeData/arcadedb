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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8368 end to end: a follower is held out of the Service from the moment the leader's first-formation
 * bootstrap pass probes it, not only once the committed baseline reaches its apply thread - and the hold is released
 * on every node once the pass has settled.
 * <p>
 * The window is made long enough to observe with the fixture of {@code RaftBootstrapTimeoutFallbackIT}: a third
 * advertised peer that never comes up, so the leader keeps collecting states for the whole bootstrap timeout after
 * the responsive follower has answered. Before this change that follower reported ready throughout, since nothing
 * local said a pass was running.
 * <p>
 * Observed from the setup gate, which is the only place the window is still open: {@link BaseRaftHATest} waits for the
 * bootstrap to settle before a test body runs.
 */
class Issue8368FollowerHeldDuringBootstrapPassIT extends BaseRaftHATest {

  private static final long BOOTSTRAP_TIMEOUT_MS = 8_000L;

  // [http of server 0, http of server 1, raft of the unreachable peer, http of the unreachable peer]
  private int[]            ports;
  private volatile boolean followerHeldDuringThePass;

  private synchronized int[] ports() {
    if (ports == null)
      ports = allocateFixturePorts(getServerCount() + 2);
    return ports;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE, true);
    config.setValue(GlobalConfiguration.HA_BOOTSTRAP_TIMEOUT_MS, BOOTSTRAP_TIMEOUT_MS);
    // Each server binds exactly the HTTP port the server list advertises for it. The default is the 2480-2489 range
    // with a best-effort hint of 2480 + index in the list, patched only after startup: on a machine where something
    // else holds one of those ports, the pass that runs right after the election probes a stranger instead of the
    // follower, and the window this test observes never opens.
    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(ports()[index]));
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected String getServerAddresses() {
    final int[] p = ports();
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getServerCount(); i++)
      sb.append("localhost:").append(raftPort(i)).append(":").append(p[i]).append(',');
    // A third peer on free ports nobody binds: the probe to it is refused and retried until the bootstrap timeout.
    return sb.append("localhost:").append(p[getServerCount()]).append(":").append(p[getServerCount() + 1]).toString();
  }

  @Override
  protected void waitForClusterBootstrapToSettle() {
    final long deadline = System.currentTimeMillis() + BOOTSTRAP_TIMEOUT_MS * 3;
    while (!followerHeldDuringThePass && System.currentTimeMillis() < deadline) {
      for (int i = 0; i < getServerCount(); i++) {
        final RaftHAPlugin plugin = getRaftPlugin(i);
        if (plugin == null || plugin.getRaftHAServer() == null || plugin.getRaftHAServer().isLeader())
          continue;
        final ArcadeStateMachine stateMachine = plugin.getRaftHAServer().getStateMachine();
        final String reason = plugin.getBootstrapWindowReason();
        if (stateMachine != null && stateMachine.getBootstrapBaseline(getDatabaseName()) == null && reason != null
            && reason.contains("deciding which copy"))
          followerHeldDuringThePass = true;
      }
      try {
        Thread.sleep(50);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    super.waitForClusterBootstrapToSettle();
  }

  @Test
  void theFollowerIsHeldFromTheProbeAndReleasedOnceThePassSettles() {
    assertThat(followerHeldDuringThePass)
        .as("the follower reported itself unfit to serve while the pass was collecting, before any baseline reached it")
        .isTrue();

    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      assertThat(plugin.getRaftHAServer().getStateMachine().getBootstrapBaseline(getDatabaseName()))
          .as("server %d applied the baseline", i).isNotNull();
      assertThat(plugin.getRaftHAServer().getStateMachine().isBootstrapPassPending(getDatabaseName()))
          .as("server %d released the hold once the pass settled", i).isFalse();
      assertThat(plugin.getBootstrapWindowReason()).as("server %d is back in the Service", i).isNull();
    }
  }
}
