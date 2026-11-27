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
package com.arcadedb.bolt;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ServerPlugin;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issues #12551 and #12552: a node of a cluster that cannot take traffic must not answer the Bolt
 * ROUTE request with a table, least of all one that names itself as the writer.
 * <p>
 * During startup the Bolt listener accepts connections before the HA plugin registers itself, so {@code getHA()} was
 * {@code null} and ROUTE took the single-node branch: WRITE = this node. A driver that refreshed its table there sent its
 * writes to a node that had not caught up. A node out of the Service (its replication stopped, a listener dead) kept
 * describing the cluster too. Such a node now refuses ROUTE with a transient error, which a driver does not treat as
 * fatal to discovery: it asks the next router.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12551RouteReadinessTest {

  private static final String SELF = "localhost:-1"; // the probe's socket is unconnected

  // Regression for #12551: the case of the report
  @Test
  void aClusterNodeWhoseHaPluginIsNotRegisteredYetRefusesRoute() throws Exception {
    final FakeArcadeDBServer server = clusterServer();
    server.status(ArcadeDBServer.STATUS.ONLINE);

    final BoltHandlerProbe.RouteReply reply = BoltHandlerProbe.route(server);

    assertThat(reply.signature()).as("no routing table, least of all one naming this node as the writer")
        .isEqualTo(BoltMessage.FAILURE);
    assertThat(reply.metadata().get("code")).isEqualTo(BoltErrorCodes.DATABASE_UNAVAILABLE_ERROR);
    assertThat(reply.stateAfter()).isEqualTo("FAILED");
  }

  // Regression for #12551
  @Test
  void aClusterNodeStillStartingRefusesRouteEvenWithItsHaPlugin() throws Exception {
    final FakeArcadeDBServer server = clusterServer();
    server.status(ArcadeDBServer.STATUS.STARTING);
    server.setHA(new FakeHAPlugin(new HAServerPlugin.RoutingTable(HAServerPlugin.ROUTING_PROTOCOL.BOLT, SELF, List.of()),
        null));

    final BoltHandlerProbe.RouteReply reply = BoltHandlerProbe.route(server);

    assertThat(reply.signature()).isEqualTo(BoltMessage.FAILURE);
    assertThat(String.valueOf(reply.metadata().get("message"))).contains("Server not started yet");
  }

  // Regression for #12552: a node whose replication stopped is out of the Service, and not a router either
  @Test
  void aClusterNodeWhoseReplicationStoppedRefusesRoute() throws Exception {
    final FakeArcadeDBServer server = clusterServer();
    server.status(ArcadeDBServer.STATUS.ONLINE);
    server.setHA(new FakeHAPlugin(new HAServerPlugin.RoutingTable(HAServerPlugin.ROUTING_PROTOCOL.BOLT, "leader:7687",
        List.of(SELF)), "Raft state machine cannot apply entries: the Ratis StateMachineUpdater thread 'x' has terminated"));

    final BoltHandlerProbe.RouteReply reply = BoltHandlerProbe.route(server);

    assertThat(reply.signature()).isEqualTo(BoltMessage.FAILURE);
    assertThat(String.valueOf(reply.metadata().get("message"))).contains("StateMachineUpdater");
  }

  // Regression for #12552: a dead listener thread (here another plugin's) takes the node out of the Service
  @Test
  void aClusterNodeWithADeadListenerRefusesRoute() throws Exception {
    final FakeArcadeDBServer server = clusterServer();
    server.status(ArcadeDBServer.STATUS.ONLINE);
    server.setHA(new FakeHAPlugin(new HAServerPlugin.RoutingTable(HAServerPlugin.ROUTING_PROTOCOL.BOLT, "leader:7687",
        List.of(SELF)), null));
    server.plugins(new ServerPlugin() {
      @Override
      public void startService() {
      }

      @Override
      public String getListenerFailure() {
        return "Postgres listener thread 'x' has terminated";
      }
    });

    final BoltHandlerProbe.RouteReply reply = BoltHandlerProbe.route(server);

    assertThat(reply.signature()).isEqualTo(BoltMessage.FAILURE);
    assertThat(String.valueOf(reply.metadata().get("message"))).contains("Postgres listener");
  }

  // Regression for #12551: the healthy cluster node still answers the cluster's table
  @Test
  void aReadyClusterNodeAnswersTheClusterTable() throws Exception {
    final FakeArcadeDBServer server = clusterServer();
    server.status(ArcadeDBServer.STATUS.ONLINE);
    server.setHA(new FakeHAPlugin(new HAServerPlugin.RoutingTable(HAServerPlugin.ROUTING_PROTOCOL.BOLT, "leader:7687",
        List.of("follower:7687")), null));

    final BoltHandlerProbe.RouteReply reply = BoltHandlerProbe.route(server);

    assertThat(reply.signature()).isEqualTo(BoltMessage.SUCCESS);
    @SuppressWarnings("unchecked")
    final Map<String, Object> rt = (Map<String, Object>) reply.metadata().get("rt");
    assertThat(BoltRouteTestSupport.addressesForRole(rt, "WRITE")).containsExactly("leader:7687");
    assertThat(BoltRouteTestSupport.addressesForRole(rt, "READ")).containsExactly("follower:7687");
  }

  // Regression for #12551: a standalone server is its clients' only router, and stays the writer
  @Test
  void aStandaloneServerStillNamesItselfTheWriter() throws Exception {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create(new ContextConfiguration());
    BoltHandlerProbe.withPassThroughSecurity(server);
    server.status(ArcadeDBServer.STATUS.ONLINE);

    final BoltHandlerProbe.RouteReply reply = BoltHandlerProbe.route(server);

    assertThat(reply.signature()).isEqualTo(BoltMessage.SUCCESS);
    @SuppressWarnings("unchecked")
    final Map<String, Object> rt = (Map<String, Object>) reply.metadata().get("rt");
    assertThat(BoltRouteTestSupport.addressesForRole(rt, "WRITE")).hasSize(1);
  }

  private static FakeArcadeDBServer clusterServer() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
    final FakeArcadeDBServer server = FakeArcadeDBServer.create(configuration);
    BoltHandlerProbe.withPassThroughSecurity(server);
    return server;
  }

  /** An HA layer that answers a fixed routing table and, optionally, a stopped replication. */
  private static final class FakeHAPlugin implements HAServerPlugin {
    private final RoutingTable table;
    private final String       replicationFailure;

    private FakeHAPlugin(final RoutingTable table, final String replicationFailure) {
      this.table = table;
      this.replicationFailure = replicationFailure;
    }

    @Override
    public RoutingTable getRoutingTable(final ROUTING_PROTOCOL protocol) {
      return table;
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
