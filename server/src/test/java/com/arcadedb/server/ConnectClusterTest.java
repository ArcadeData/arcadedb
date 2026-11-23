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
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ServerControlPlane.ConnectClusterResult;
import com.arcadedb.server.ServerControlPlane.OperationNotAvailableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The transport-independent {@code connect cluster} of {@code ServerControlPlane}: its outcomes, and the seed report the HA
 * plugin hands it.
 */
class ConnectClusterTest {
  // Regression: #7401 each outcome of connect cluster, pinned once rather than once per transport
  /**
   * Issue #7401: the transport-independent half of {@code connect cluster}, driven directly so each of
   * its outcomes is pinned once rather than once per transport.
   * <p>
   * Like {@code GetReadyHandlerHATest} this boots a real single-node {@link ArcadeDBServer} - the
   * {@code server} module does not depend on {@code arcadedb-ha-raft}, so {@link ArcadeDBServer#getHA()}
   * is naturally {@code null} - and supplies HA behaviour through a hand-written plugin injected with
   * {@link ArcadeDBServer#setHA(HAServerPlugin)}. No mocking framework, and no Raft cluster needed to
   * decide what this layer does with an address.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class Outcomes extends StaticBaseServerTest {
    private static final String PEER_ADDRESS = "db2:2435";

    private ArcadeDBServer      server;
    private ServerControlPlane  controlPlane;

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
      assertThat(server.getHA()).isNull();

      controlPlane = new ServerControlPlane(server);
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

    /**
     * The address reaches the HA implementation byte for byte. Nothing else in this class would notice a
     * layer that trimmed, lowercased or rebuilt it, and an address the joining peer does not answer to is
     * a configuration entry for a process that never appears.
     */
    @Test
    void theAddressReachesTheHaImplementationUnmodified() {
      final RecordingHAPlugin ha = new RecordingHAPlugin();
      server.setHA(ha);

      controlPlane.connectCluster("frankfurt@DB2.example.com:2435:2481");

      assertThat(ha.connectedAddress).isEqualTo("frankfurt@DB2.example.com:2435:2481");
    }

    /**
     * The users seed of {@code PostAddPeerHandler} runs for this verb too, and only after the join.
     * {@code server-users.jsonl} lives outside the database directory, so a snapshot install does not
     * carry it and a peer joined without the seed runs with a stale user set until the next cluster-wide
     * user change. The ordering is asserted because a seed sent before the peer is a member reaches a
     * cluster the peer is not in yet.
     */
    @Test
    void aSuccessfulJoinSeedsTheUsersFileAfterwards() {
      final RecordingHAPlugin ha = new RecordingHAPlugin();
      server.setHA(ha);

      controlPlane.connectCluster(PEER_ADDRESS);

      assertThat(ha.seededUsers).as("users payload").contains("root");
      assertThat(ha.seedFollowedTheJoin).as("seed must follow the membership change").isTrue();
    }

    /**
     * The seed is best-effort and the join is not: the peer is a committed member by the time the seed
     * runs, so a seed failure must not be reported as a failed join - the caller would retry a join that
     * already happened.
     */
    @Test
    void aFailingUsersSeedDoesNotFailTheJoin() {
      final RecordingHAPlugin ha = new RecordingHAPlugin() {
        @Override
        public void replicateSecurityUsers(final String usersJsonArray) {
          throw new IllegalStateException("no leader to replicate to");
        }
      };
      server.setHA(ha);

      controlPlane.connectCluster(PEER_ADDRESS);

      assertThat(ha.connectedAddress).isEqualTo(PEER_ADDRESS);
    }

    /**
     * A blank address is the caller's mistake, not a precondition of this server: an
     * {@link IllegalArgumentException} is HTTP 400 and gRPC {@code INVALID_ARGUMENT}, while the
     * {@link OperationNotAvailableException} below is HTTP 500 and gRPC {@code FAILED_PRECONDITION}.
     * A bare {@code connect cluster} arrives here as {@code ""} from {@code extractTarget}.
     */
    @Test
    void aBlankAddressIsRejectedBeforeHaIsConsulted() {
      final RecordingHAPlugin ha = new RecordingHAPlugin();
      server.setHA(ha);

      assertThatThrownBy(() -> controlPlane.connectCluster(""))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("requires the address");

      assertThat(ha.connectedAddress).as("HA must not be asked to join nothing").isNull();
    }

    /**
     * Without HA the refusal still names the address. That is the property #7400's tests pin from both
     * transports, and it is the only thing that makes the argument observable from outside when the
     * command cannot run - so it survives the verb becoming a real join.
     */
    @Test
    void withoutHaTheRefusalNamesTheAddress() {
      assertThatThrownBy(() -> controlPlane.connectCluster(PEER_ADDRESS))
          .isInstanceOf(OperationNotAvailableException.class)
          .hasMessageContaining(PEER_ADDRESS)
          .hasMessageContaining("High Availability module enabled");
    }

    /**
     * An HA implementation that cannot change membership at runtime keeps
     * {@code HAServerPlugin.connectCluster}'s default, which raises {@link UnsupportedOperationException}.
     * That has to arrive as a precondition failure and not as an internal error: gRPC maps an unchecked
     * exception it does not recognise to {@code INTERNAL}, which would tell an operator the server broke
     * rather than that this HA stack does not do joins.
     */
    @Test
    void anHaImplementationWithoutDynamicMembershipRefusesAsAPrecondition() {
      server.setHA(new RecordingHAPlugin(false));

      assertThatThrownBy(() -> controlPlane.connectCluster(PEER_ADDRESS))
          .isInstanceOf(OperationNotAvailableException.class)
          .hasMessageContaining(PEER_ADDRESS)
          .hasMessageContaining("Dynamic membership not supported");
    }

    /**
     * A failure raised by a working HA implementation is not converted: only the interface default's
     * {@link UnsupportedOperationException} becomes a precondition failure. A join that was attempted and
     * failed is a different report from one that was never possible.
     */
    @Test
    void aFailedJoinIsNotDisguisedAsAnUnavailableOperation() {
      server.setHA(new RecordingHAPlugin() {
        @Override
        public void connectCluster(final String serverAddress) {
          throw new ServerException("Raft HA server not started");
        }
      });

      assertThatThrownBy(() -> controlPlane.connectCluster(PEER_ADDRESS))
          .isInstanceOf(ServerException.class)
          .isNotInstanceOf(OperationNotAvailableException.class);
    }

    /**
     * An {@link HAServerPlugin} that records what the control plane asked it to do. Everything the
     * readiness probe and the cluster views need is inert; only the cluster-join pair carries behaviour.
     */
    private static class RecordingHAPlugin implements HAServerPlugin {
      private final boolean dynamicMembership;
      private       String  connectedAddress    = null;
      private       String  seededUsers         = null;
      private       boolean seedFollowedTheJoin = false;

      private RecordingHAPlugin() {
        this(true);
      }

      private RecordingHAPlugin(final boolean dynamicMembership) {
        this.dynamicMembership = dynamicMembership;
      }

      @Override
      public void connectCluster(final String serverAddress) {
        if (!dynamicMembership)
          // The interface default, invoked rather than restated, so this test cannot pass against a
          // default whose message or type has changed underneath it.
          HAServerPlugin.super.connectCluster(serverAddress);
        connectedAddress = serverAddress;
      }

      @Override
      public ELECTION_STATUS getElectionStatus() {
        return ELECTION_STATUS.DONE;
      }

      @Override
      public void replicateSecurityUsers(final String usersJsonArray) {
        seededUsers = usersJsonArray;
        seedFollowedTheJoin = connectedAddress != null;
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
        return Collections.emptyMap();
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

  // Regression: #7532 the two admission verbs agree about a residual seed failure
  /**
   * Issue #7532, absorbing #7550: the two admission verbs no longer disagree about a residual seed failure.
   * <p>
   * {@code POST /api/v1/cluster/peer} has answered 503 and named the documents since issue #7521. {@code connect
   * cluster} - the same membership change and the same seed, reached from HTTP {@code POST /api/v1/server} and
   * from the gRPC {@code ConnectCluster} RPC - returned {@code void} and left the identical failure in a SEVERE
   * log line, so the same condition was a hard failure through one verb and invisible to automation through the
   * other.
   * <p>
   * It is reported, not thrown, and the class that follows pins why: {@code
   * ConnectClusterTest.Outcomes.aFailingUsersSeedDoesNotFailTheJoin} requires the join to stand,
   * because by the time the seed runs the peer is a committed member and a caller retrying a failed join would be
   * retrying something that already happened. A return value says "the join happened AND part of the follow-up did
   * not"; an exception could only say the first half wrongly.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class ReportsSeedFailure extends StaticBaseServerTest {
    private static final String PEER_ADDRESS = "db2:2435";

    private ArcadeDBServer     server;
    private ServerControlPlane controlPlane;

    @BeforeEach
    @Override
    public void beginTest() {
      super.beginTest();

      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
      config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
      config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);
      // No retry budget: the seeds that fail here fail deterministically, and a budget would only add sleep.
      config.setValue(GlobalConfiguration.HA_SECURITY_SEED_RETRY_TIMEOUT, 0L);

      server = new ArcadeDBServer(config);
      server.start();
      assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);

      controlPlane = new ServerControlPlane(server);
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

    /** A clean join reports nothing to act on, and the address it joined. */
    @Test
    void aCleanJoinReportsNoFailedSeeds() {
      server.setHA(new SeedingHAPlugin());

      final ConnectClusterResult result = controlPlane.connectCluster(PEER_ADDRESS);

      assertThat(result.hasFailedSeeds()).isFalse();
      assertThat(result.failedSeeds()).isEmpty();
      assertThat(result.serverAddress()).isEqualTo(PEER_ADDRESS);
    }

    /**
     * The defect: a document the seed could not land came back as nothing at all. It now names the document, and
     * only the one that failed - the other two committed and must not be reported as outstanding.
     */
    @Test
    void aDocumentThatCouldNotBeSeededIsNamedInTheResult() {
      server.setHA(new SeedingHAPlugin() {
        @Override
        public void replicateSecurityApiTokens(final String apiTokensJson) {
          throw new IllegalStateException("no quorum to replicate to");
        }
      });

      final ConnectClusterResult result = controlPlane.connectCluster(PEER_ADDRESS);

      assertThat(result.hasFailedSeeds()).isTrue();
      assertThat(result.failedSeeds()).containsExactly("API tokens");
      assertThat(result.errorMessage()).contains(PEER_ADDRESS, "API tokens");
      assertThat(result.detailMessage()).as("the operator needs the remediation, not only the diagnosis")
          .contains("connect cluster");
    }

    /** Every document failing reports every document, in the order the seed reports them. */
    @Test
    void allThreeDocumentsAreNamedWhenNoneOfThemLand() {
      server.setHA(new SeedingHAPlugin() {
        @Override
        public void replicateSecurityUsers(final String usersJsonArray) {
          throw new IllegalStateException("no quorum");
        }

        @Override
        public void replicateSecurityGroups(final String groupsJson) {
          throw new IllegalStateException("no quorum");
        }

        @Override
        public void replicateSecurityApiTokens(final String apiTokensJson) {
          throw new IllegalStateException("no quorum");
        }
      });

      assertThat(controlPlane.connectCluster(PEER_ADDRESS).failedSeeds())
          .containsExactly("users", "groups", "API tokens");
    }

    /**
     * The join still stands. This is the same property
     * {@code ConnectClusterTest.Outcomes.aFailingUsersSeedDoesNotFailTheJoin} pins, restated
     * from the reporting side: the verb must report the residual failure without ever claiming the membership
     * change did not happen.
     */
    @Test
    void aFailedSeedIsReportedWithoutUndoingOrFailingTheJoin() {
      final SeedingHAPlugin ha = new SeedingHAPlugin() {
        @Override
        public void replicateSecurityUsers(final String usersJsonArray) {
          throw new IllegalStateException("no leader to replicate to");
        }
      };
      server.setHA(ha);

      final ConnectClusterResult result = controlPlane.connectCluster(PEER_ADDRESS);

      assertThat(ha.connectedAddress).as("the peer is a committed member").isEqualTo(PEER_ADDRESS);
      assertThat(result.failedSeeds()).containsExactly("users");
    }

    /**
     * A seed that could not run AT ALL reports all three rather than none. The empty list would have been read as
     * "joined, everything seeded" from the one path where nothing was seeded, which is the silence this issue
     * exists to remove.
     */
    @Test
    void aSeedThatCouldNotRunAtAllReportsEveryDocument() {
      // Driven against a mocked server because the condition is one a running one cannot reach: the failure the
      // production catch is there for is a security store that is not installed, which a started ArcadeDBServer
      // always has. seedSecurityStateClusterWide collects its per-document failures itself, so what is exercised
      // here is everything around them.
      final FakeArcadeDBServer unstartedServer = FakeArcadeDBServer.create();
      unstartedServer.setHA(new SeedingHAPlugin());
      unstartedServer.returns("getConfiguration", new ContextConfiguration());
      final FakeServerSecurity security = FakeServerSecurity.create();
      security.fails("seedSecurityStateClusterWide", new IllegalStateException("security store not installed"));
      unstartedServer.security(security);

      assertThat(new ServerControlPlane(unstartedServer).connectCluster(PEER_ADDRESS).failedSeeds())
          .containsExactly("users", "groups", "API tokens");
    }

    /** The result is immutable: a caller cannot edit the list a transport is about to report. */
    @Test
    void theReportedListCannotBeMutatedByItsCaller() {
      assertThat(new ConnectClusterResult(PEER_ADDRESS, new ArrayList<>(List.of("users"))).failedSeeds())
          .isUnmodifiable();
    }

    /**
     * An {@link HAServerPlugin} whose cluster-join pair records and whose three security replications succeed,
     * so a subclass overriding one of them isolates exactly that document's failure.
     */
    private static class SeedingHAPlugin implements HAServerPlugin {
      private String connectedAddress = null;

      @Override
      public void connectCluster(final String serverAddress) {
        connectedAddress = serverAddress;
      }

      @Override
      public void replicateSecurityUsers(final String usersJsonArray) {
      }

      @Override
      public void replicateSecurityGroups(final String groupsJson) {
      }

      @Override
      public void replicateSecurityApiTokens(final String apiTokensJson) {
      }

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

  // Regression: #8077 connect cluster consumes the seed report of the HA plugin
  /**
   * Issue #8077: the seed report of {@code connect cluster} moved down into {@code HAServerPlugin}, so the embedded
   * {@code connectCluster} API can report it too, and {@code ServerControlPlane.connectCluster} now consumes the
   * plugin's report instead of running its own seed request. That keeps one seed request per {@code connect cluster}
   * (issue #7834), and the default keeps an implementation that predates the new method on the seed this verb always
   * ran for it - which {@code ConnectClusterTest.ReportsSeedFailure} pins from the reporting side.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class ConsumesPluginSeedReport {
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
}
