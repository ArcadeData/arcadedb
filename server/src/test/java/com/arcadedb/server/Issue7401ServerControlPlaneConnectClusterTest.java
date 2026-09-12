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
import com.arcadedb.server.ServerControlPlane.OperationNotAvailableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue7401ServerControlPlaneConnectClusterTest extends StaticBaseServerTest {

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
