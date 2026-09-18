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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.arcadedb.server.security.ServerSecurity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
 * Issue7401ServerControlPlaneConnectClusterTest.aFailingUsersSeedDoesNotFailTheJoin} requires the join to stand,
 * because by the time the seed runs the peer is a committed member and a caller retrying a failed join would be
 * retrying something that already happened. A return value says "the join happened AND part of the follow-up did
 * not"; an exception could only say the first half wrongly.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7532ConnectClusterReportsSeedFailureTest extends StaticBaseServerTest {

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
   * {@code Issue7401ServerControlPlaneConnectClusterTest.aFailingUsersSeedDoesNotFailTheJoin} pins, restated
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
    final ArcadeDBServer unstartedServer = mock(ArcadeDBServer.class);
    when(unstartedServer.getHA()).thenReturn(new SeedingHAPlugin());
    when(unstartedServer.getConfiguration()).thenReturn(new ContextConfiguration());
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.seedSecurityStateClusterWide(anyLong()))
        .thenThrow(new IllegalStateException("security store not installed"));
    when(unstartedServer.getSecurity()).thenReturn(security);

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
