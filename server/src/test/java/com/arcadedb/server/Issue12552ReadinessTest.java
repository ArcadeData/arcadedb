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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #12552: {@code /api/v1/ready} kept answering 204 on a node whose replication had stopped
 * with nothing recovering it, and whose Bolt listener thread had died of an {@code OutOfMemoryError}. Both are reported
 * whether or not {@code arcadedb.server.readinessRequiresHA} is on: the node is not behind, it has stopped.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12552ReadinessTest {

  // Regression for #12552
  @Test
  void aStoppedReplicationIsNotReadyWithoutReadinessRequiresHA() {
    final String reason = new ServerControlPlane(onlineServerWith(new FailingHAPlugin(
        "Raft state machine cannot apply entries: the Ratis StateMachineUpdater thread 'n2-StateMachineUpdater' has terminated"),
        haConfiguration())).notReadyReason();

    assertThat(reason).isNotNull().contains("StateMachineUpdater");
  }

  // Regression for #12552
  @Test
  void aHealthyReplicationStaysReady() {
    assertThat(new ServerControlPlane(onlineServerWith(new FailingHAPlugin(null), haConfiguration())).notReadyReason())
        .isNull();
  }

  // Regression for #12552: the Bolt listener died while the rest of the server stayed up
  @Test
  void aDeadPluginListenerIsNotReady() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create(new ContextConfiguration());
    server.online();
    server.plugins(listenerPlugin(null), listenerPlugin("Bolt listener thread 'n2 BOLT listening at 0.0.0.0:7687' has terminated"));

    assertThat(new ServerControlPlane(server).notReadyReason()).isNotNull().contains("BOLT listening");
  }

  // Regression for #12552
  @Test
  void livePluginListenersStayReady() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create(new ContextConfiguration());
    server.online();
    server.plugins(listenerPlugin(null));

    assertThat(new ServerControlPlane(server).notReadyReason()).isNull();
  }

  // Regression for #12552: a plugin that throws while asked does not turn the probe into a 500
  @Test
  void aListenerSignalThatCannotBeReadDoesNotFailTheProbe() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create(new ContextConfiguration());
    server.online();
    server.plugins(new ServerPlugin() {
      @Override
      public void startService() {
      }

      @Override
      public String getListenerFailure() {
        throw new IllegalStateException("not initialized");
      }
    });

    assertThat(new ServerControlPlane(server).notReadyReason()).isNull();
  }

  private static ServerPlugin listenerPlugin(final String failure) {
    return new ServerPlugin() {
      @Override
      public void startService() {
      }

      @Override
      public String getListenerFailure() {
        return failure;
      }
    };
  }

  private static ArcadeDBServer onlineServerWith(final HAServerPlugin ha, final ContextConfiguration configuration) {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create(configuration);
    server.online();
    server.setHA(ha);
    return server;
  }

  private static ContextConfiguration haConfiguration() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
    configuration.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, false);
    return configuration;
  }

  /** An HA layer that is healthy in every respect but, optionally, a stopped replication. */
  private static final class FailingHAPlugin implements HAServerPlugin {
    private final String replicationFailure;

    private FailingHAPlugin(final String replicationFailure) {
      this.replicationFailure = replicationFailure;
    }

    @Override
    public String getReplicationFailureReason() {
      return replicationFailure;
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
      return "leader";
    }

    @Override
    public ELECTION_STATUS getElectionStatus() {
      return ELECTION_STATUS.DONE;
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
      return 3;
    }

    @Override
    public String getLeaderAddress() {
      return "leader:2480";
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
