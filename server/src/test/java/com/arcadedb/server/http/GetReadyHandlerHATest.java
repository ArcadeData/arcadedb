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
package com.arcadedb.server.http;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.http.handler.GetReadyHandler;
import com.arcadedb.server.security.ServerSecurityUser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies how the {@code /ready} probe gates on HA state. Instead of mocking, this test boots a real
 * single-node {@link ArcadeDBServer} (the {@code server} module does not depend on {@code arcadedb-ha-raft},
 * so {@link ArcadeDBServer#getHA()} is naturally {@code null}) and drives {@link GetReadyHandler} through its
 * real {@link HttpServer}. The HA election status and consensus readiness signal are supplied by a hand-written
 * {@link FakeHAPlugin} injected via {@link ArcadeDBServer#setHA(HAServerPlugin)}, which lets each test pin those
 * states deterministically without a live Raft cluster.
 */
class GetReadyHandlerHATest extends StaticBaseServerTest {

  private ArcadeDBServer server;

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);

    server = new ArcadeDBServer(config);
    server.start();
    assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);
    // No arcadedb-ha-raft on the classpath -> no HA plugin auto-registered.
    assertThat(server.getHA()).isNull();
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      if (server != null && server.isStarted())
        server.stop();
    } finally {
      super.endTest();
    }
  }

  private GetReadyHandler handler() {
    return new GetReadyHandler(server.getHttpServer());
  }

  private ExecutionResponse executeReady() throws Exception {
    return handler().execute(null, (ServerSecurityUser) null, null);
  }

  @Test
  void flagOffReturns204RegardlessOfHA() throws Exception {
    server.getConfiguration().setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, false);
    // HA still electing, but the flag is off so it must be ignored.
    server.setHA(new FakeHAPlugin(HAServerPlugin.ELECTION_STATUS.VOTING_FOR_ME, null));

    assertThat(executeReady().getCode()).isEqualTo(204);
  }

  @Test
  void flagOnAndElectionDoneReturns204() throws Exception {
    server.getConfiguration().setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
    server.getConfiguration().setValue(GlobalConfiguration.HA_ENABLED, true);
    server.setHA(new FakeHAPlugin(HAServerPlugin.ELECTION_STATUS.DONE, null));

    assertThat(executeReady().getCode()).isEqualTo(204);
  }

  @Test
  void flagOnAndElectionInProgressReturns503() throws Exception {
    server.getConfiguration().setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
    server.getConfiguration().setValue(GlobalConfiguration.HA_ENABLED, true);
    server.setHA(new FakeHAPlugin(HAServerPlugin.ELECTION_STATUS.VOTING_FOR_ME, null));

    assertThat(executeReady().getCode()).isEqualTo(503);
  }

  @Test
  void flagOnButHaDisabledReturns204() throws Exception {
    server.getConfiguration().setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
    // HA not enabled: single-node deployment, the readiness flag has no effect.
    server.getConfiguration().setValue(GlobalConfiguration.HA_ENABLED, false);

    assertThat(executeReady().getCode()).isEqualTo(204);
  }

  @Test
  void flagOnHaEnabledButPluginAbsentReturns503() throws Exception {
    server.getConfiguration().setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
    // HA requested but no HA plugin registered (e.g. arcadedb-ha-raft missing): the node runs
    // standalone, which does not satisfy the operator's "require HA for readiness" intent.
    server.getConfiguration().setValue(GlobalConfiguration.HA_ENABLED, true);
    // getHA() is already null on a server module boot; do not inject a plugin.

    assertThat(executeReady().getCode()).isEqualTo(503);
  }

  @Test
  void flagOnElectionDoneButNotCaughtUpReturns503() throws Exception {
    server.getConfiguration().setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
    server.getConfiguration().setValue(GlobalConfiguration.HA_ENABLED, true);
    // A leader is known, but this node is not yet in the committed config / not caught up.
    server.setHA(new FakeHAPlugin(HAServerPlugin.ELECTION_STATUS.DONE, HAServerPlugin.READINESS_SIGNAL.NOT_READY));

    assertThat(executeReady().getCode()).isEqualTo(503);
  }

  @Test
  void flagOnElectionDoneAndCaughtUpReturns204() throws Exception {
    server.getConfiguration().setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
    server.getConfiguration().setValue(GlobalConfiguration.HA_ENABLED, true);
    server.setHA(new FakeHAPlugin(HAServerPlugin.ELECTION_STATUS.DONE, HAServerPlugin.READINESS_SIGNAL.READY));

    assertThat(executeReady().getCode()).isEqualTo(204);
  }

  @Test
  void flagOnElectionDoneAndNoConsensusSignalReturns204() throws Exception {
    // An HA implementation that provides no consensus readiness signal returns null from
    // getReadinessSignal: the probe applies no additional gating beyond the election status.
    server.getConfiguration().setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
    server.getConfiguration().setValue(GlobalConfiguration.HA_ENABLED, true);
    server.setHA(new FakeHAPlugin(HAServerPlugin.ELECTION_STATUS.DONE, null));

    assertThat(executeReady().getCode()).isEqualTo(204);
  }

  /**
   * Minimal {@link HAServerPlugin} that exposes a fixed election status and consensus readiness signal.
   * Only the methods consulted by {@link GetReadyHandler} carry meaningful values; the rest are inert.
   */
  private static final class FakeHAPlugin implements HAServerPlugin {
    private final ELECTION_STATUS  electionStatus;
    private final READINESS_SIGNAL readinessSignal;

    private FakeHAPlugin(final ELECTION_STATUS electionStatus, final READINESS_SIGNAL readinessSignal) {
      this.electionStatus = electionStatus;
      this.readinessSignal = readinessSignal;
    }

    @Override
    public ELECTION_STATUS getElectionStatus() {
      return electionStatus;
    }

    @Override
    public READINESS_SIGNAL getReadinessSignal(final long maxLagEntries) {
      return readinessSignal;
    }

    @Override
    public void startService() {
    }

    @Override
    public boolean isLeader() {
      return false;
    }

    @Override
    public String getLeaderName() {
      return null;
    }

    @Override
    public String getClusterName() {
      return "test";
    }

    @Override
    public Map<String, Object> getStats() {
      return Map.of();
    }

    @Override
    public int getConfiguredServers() {
      return 1;
    }

    @Override
    public String getLeaderAddress() {
      return null;
    }

    @Override
    public String getReplicaAddresses() {
      return "";
    }

    @Override
    public void shutdownRemoteServer(final String serverName) {
    }

    @Override
    public void disconnectCluster() {
    }
  }
}
