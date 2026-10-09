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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.ServerControlPlane.OperationNotAvailableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8077: the seed report of {@code connect cluster} moved down into {@code HAServerPlugin}, so the embedded
 * {@code connectCluster} API can report it too, and {@code ServerControlPlane.connectCluster} now consumes the
 * plugin's report instead of running its own seed request. That keeps one seed request per {@code connect cluster}
 * (issue #7834), and the default keeps an implementation that predates the new method on the seed this verb always
 * ran for it - which {@code Issue7532ConnectClusterReportsSeedFailureTest} pins from the reporting side.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8077ConnectClusterConsumesPluginSeedReportTest {

  private static final String PEER_ADDRESS = "db2:2435";

  private FakeArcadeDBServer server;
  private FakeServerSecurity security;

  @BeforeEach
  void setUp() {
    server = FakeArcadeDBServer.create();
    server.returns("getConfiguration", new ContextConfiguration());
    security = FakeServerSecurity.create();
    security.returns("seedSecurityStateClusterWide", List.of());
    server.security(security);
  }

  /** The plugin's report is the verb's report, and nothing seeds a second time - neither the leader nor locally. */
  @Test
  void aPluginThatReportsItsOwnSeedIsNotSeededAgain() {
    final RecordingHAPlugin ha = new RecordingHAPlugin(Optional.of(List.of("groups")));
    server.setHA(ha);

    final ServerControlPlane.ConnectClusterResult result = new ServerControlPlane(server).connectCluster(PEER_ADDRESS);

    assertThat(result.failedSeeds()).containsExactly("groups");
    assertThat(ha.steps).as("one seed request per connect cluster (issue #7834)")
        .containsExactly("join+report " + PEER_ADDRESS);
    assertThat(security.calls("seedSecurityStateClusterWide")).isEmpty();
  }

  /** A clean report from the plugin is a clean join, still with no second seed. */
  @Test
  void aCleanPluginReportIsACleanJoin() {
    final RecordingHAPlugin ha = new RecordingHAPlugin(Optional.of(List.of()));
    server.setHA(ha);

    assertThat(new ServerControlPlane(server).connectCluster(PEER_ADDRESS).hasFailedSeeds()).isFalse();
    assertThat(ha.steps).containsExactly("join+report " + PEER_ADDRESS);
    assertThat(security.calls("seedSecurityStateClusterWide")).isEmpty();
  }

  /**
   * An implementation that leaves the seed to its caller (the interface default) still gets the leader-side seed
   * request it implements, exactly once and after the join.
   */
  @Test
  void aPluginThatLeavesTheSeedToItsCallerIsAskedForTheLeaderSeed() {
    final RecordingHAPlugin ha = new RecordingHAPlugin(Optional.empty());
    ha.leaderSeed = Optional.of(List.of("users"));
    server.setHA(ha);

    assertThat(new ServerControlPlane(server).connectCluster(PEER_ADDRESS).failedSeeds()).containsExactly("users");
    assertThat(ha.steps).containsExactly("join+report " + PEER_ADDRESS, "seed " + PEER_ADDRESS);
    assertThat(security.calls("seedSecurityStateClusterWide")).isEmpty();
  }

  /** The interface default joins through connectCluster and reports nothing of its own. */
  @Test
  void theInterfaceDefaultJoinsAndLeavesTheSeedToItsCaller() {
    final DefaultHAPlugin ha = new DefaultHAPlugin();

    assertThat(ha.connectClusterAndReportSeed(PEER_ADDRESS)).isEmpty();
    assertThat(ha.steps).containsExactly("join " + PEER_ADDRESS);
  }

  /** An implementation with no runtime membership is still the precondition refusal it always was. */
  @Test
  void anImplementationWithoutRuntimeMembershipIsStillRefusedAsAPrecondition() {
    final HAServerPlugin ha = new BaseHAPlugin() {
    };
    server.setHA(ha);

    assertThatThrownBy(() -> new ServerControlPlane(server).connectCluster(PEER_ADDRESS))
        .isInstanceOf(OperationNotAvailableException.class)
        .hasMessageContaining(PEER_ADDRESS);
  }

  /** The precondition mapping also covers a plugin that overrides the reporting form and refuses the join. */
  @Test
  void aReportingPluginThatCannotChangeMembershipIsRefusedAsAPrecondition() {
    final HAServerPlugin ha = new BaseHAPlugin() {
      @Override
      public Optional<List<String>> connectClusterAndReportSeed(final String serverAddress) {
        throw new UnsupportedOperationException("no runtime membership here");
      }
    };
    server.setHA(ha);

    assertThatThrownBy(() -> new ServerControlPlane(server).connectCluster(PEER_ADDRESS))
        .isInstanceOf(OperationNotAvailableException.class)
        .hasMessageContaining(PEER_ADDRESS)
        .hasMessageContaining("no runtime membership here");
    assertThat(security.calls("seedSecurityStateClusterWide")).isEmpty();
  }

  /** Overrides the reporting form, as the Raft implementation does. */
  private static class RecordingHAPlugin extends BaseHAPlugin {
    final List<String>           steps      = new ArrayList<>();
    final Optional<List<String>> report;
    Optional<List<String>>       leaderSeed = Optional.of(List.of());

    RecordingHAPlugin(final Optional<List<String>> report) {
      this.report = report;
    }

    @Override
    public Optional<List<String>> connectClusterAndReportSeed(final String serverAddress) {
      steps.add("join+report " + serverAddress);
      return report;
    }

    @Override
    public Optional<List<String>> seedSecurityStateForAdmission(final String admittedPeer) {
      steps.add("seed " + admittedPeer);
      return leaderSeed;
    }
  }

  /** Overrides only the void form, as an implementation written before issue #8077 does. */
  private static class DefaultHAPlugin extends BaseHAPlugin {
    final List<String> steps = new ArrayList<>();

    @Override
    public void connectCluster(final String serverAddress) {
      steps.add("join " + serverAddress);
    }
  }

  private abstract static class BaseHAPlugin implements HAServerPlugin {
    @Override
    public ELECTION_STATUS getElectionStatus() {
      return ELECTION_STATUS.DONE;
    }

    @Override
    public void startService() {
    }

    @Override
    public boolean isLeader() {
      return true;
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
      return Collections.emptyMap();
    }

    @Override
    public int getConfiguredServers() {
      return 2;
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
