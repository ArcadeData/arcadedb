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
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.http.HttpServer;

import io.undertow.server.handlers.PathHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;

/**
 * ServerPlugin implementation that bootstraps the Raft-based HA subsystem.
 * Discovered via Java ServiceLoader when {@code HA_ENABLED=true}.
 */
public class RaftHAPlugin implements HAServerPlugin {

  private ArcadeDBServer       server;
  private ContextConfiguration configuration;
  private RaftHAServer         raftHAServer;

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.configuration = configuration;
  }

  @Override
  public PluginInstallationPriority getInstallationPriority() {
    return PluginInstallationPriority.AFTER_HTTP_ON;
  }

  @Override
  public void startService() {
    if (!isRaftEnabled()) {
      HALog.log(this, HALog.TRACE, "Raft HA plugin not activated (HA not enabled)");
      return;
    }

    validateConfiguration();

    try {
      raftHAServer = new RaftHAServer(server, configuration);
      raftHAServer.getStateMachine().setRaftHAServer(raftHAServer);
      raftHAServer.start();

      // Register the database wrapper so the server wraps databases with RaftReplicatedDatabase
      server.setDatabaseWrapper(db -> new RaftReplicatedDatabase(server, db, raftHAServer));

      // Re-wrap any databases that were already loaded before this plugin started
      server.rewrapDatabases();

      // Register this plugin as the HA implementation on the server
      server.setHA(this);

      LogManager.instance().log(this, Level.INFO, "Raft HA plugin started successfully");
    } catch (final IOException e) {
      throw new RuntimeException("Failed to start Raft HA server", e);
    }
  }

  @Override
  public void stopService() {
    if (raftHAServer != null) {
      // K8s auto-leave: gracefully remove self from cluster on shutdown
      if (configuration != null && configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S)) {
        try {
          raftHAServer.leaveCluster();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING,
              "K8s auto-leave failed (best-effort): %s", e.getMessage());
        }
      }
      raftHAServer.stop();
      raftHAServer = null;
    }
    // Clear the wrapper so databases loaded during restart don't capture a stale/null raftHAServer.
    // startService() will set a fresh wrapper and call rewrapDatabases().
    if (server != null)
      server.setDatabaseWrapper(null);
  }

  public RaftHAServer getRaftHAServer() {
    return raftHAServer;
  }

  @Override
  public void replicateSecurityUsers(final String usersJsonArray) {
    if (raftHAServer == null)
      throw new TransactionException("Raft HA server not started");

    try {
      raftHAServer.getTransactionBroker().replicateSecurityUsers(usersJsonArray);
    } catch (final TransactionException e) {
      throw e;
    } catch (final Exception e) {
      throw new TransactionException("Error sending security-users entry via Raft", e);
    }
    LogManager.instance().log(this, Level.INFO, "Security users entry committed via Raft");
  }

  @Override
  public void registerAPI(final HttpServer httpServer, final PathHandler routes) {
    // Always register the endpoint - it returns 503 when Raft is not yet started.
    // Note: registerAPI is called before configure()/startService() for AFTER_HTTP_ON plugins,
    // so isRaftEnabled() cannot be checked here.
    routes.addExactPath("/api/v1/cluster", new GetClusterHandler(httpServer, this));
    LogManager.instance().log(this, Level.INFO, "Raft cluster status endpoint registered at /api/v1/cluster");
    routes.addPrefixPath("/api/v1/ha/snapshot/", new SnapshotHttpHandler(httpServer));
    LogManager.instance().log(this, Level.INFO, "Raft snapshot endpoint registered at /api/v1/ha/snapshot/{database}");
    routes.addExactPath("/api/v1/cluster/peer", new PostAddPeerHandler(httpServer, this));
    routes.addPrefixPath("/api/v1/cluster/peer/", new DeletePeerHandler(httpServer, this));
    routes.addExactPath("/api/v1/cluster/leader", new PostTransferLeaderHandler(httpServer, this));
    routes.addExactPath("/api/v1/cluster/stepdown", new PostStepDownHandler(httpServer, this));
    routes.addExactPath("/api/v1/cluster/leave", new PostLeaveHandler(httpServer, this));
    routes.addPrefixPath("/api/v1/cluster/verify/", new PostVerifyDatabaseHandler(httpServer, this));
    LogManager.instance().log(this, Level.INFO, "Raft cluster management endpoints registered");
  }

  @Override
  public boolean isLeader() {
    return raftHAServer != null && raftHAServer.isLeader();
  }

  @Override
  public String getLeaderName() {
    return raftHAServer != null ? raftHAServer.getLeaderName() : null;
  }

  @Override
  public HAServerPlugin.ELECTION_STATUS getElectionStatus() {
    if (raftHAServer == null)
      return ELECTION_STATUS.DONE;
    return raftHAServer.getLeaderId() != null ? ELECTION_STATUS.DONE : ELECTION_STATUS.VOTING_FOR_ME;
  }

  @Override
  public String getClusterToken() {
    return raftHAServer != null ? raftHAServer.getClusterToken() : null;
  }

  @Override
  public String getClusterName() {
    return raftHAServer != null ? raftHAServer.getClusterName() : null;
  }

  @Override
  public java.util.Map<String, Object> getStats() {
    return raftHAServer != null ? raftHAServer.getStats() : java.util.Collections.emptyMap();
  }

  @Override
  public int getConfiguredServers() {
    return raftHAServer != null ? raftHAServer.getConfiguredServers() : 1;
  }

  @Override
  public String getLeaderAddress() {
    return raftHAServer != null ? raftHAServer.getLeaderHttpAddress() : null;
  }

  @Override
  public String getReplicaAddresses() {
    return raftHAServer != null ? raftHAServer.getReplicaAddresses() : "";
  }

  @Override
  public void shutdownRemoteServer(final String serverName) {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");

    String targetAddr = null;
    for (final var peer : raftHAServer.getRaftGroup().getPeers()) {
      final String httpAddr = raftHAServer.getHttpAddresses().get(peer.getId());
      if (httpAddr != null && (peer.getId().toString().contains(serverName) || httpAddr.contains(serverName))) {
        targetAddr = httpAddr;
        break;
      }
    }
    if (targetAddr == null)
      throw new com.arcadedb.server.ServerException("Cannot find server '" + serverName + "' in the cluster");

    try {
      final java.net.HttpURLConnection conn = (java.net.HttpURLConnection)
          new java.net.URL("http://" + targetAddr + "/api/v1/server").openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");

      final String token = raftHAServer.getClusterToken();
      if (token != null && !token.isEmpty())
        conn.setRequestProperty("Authorization", "Bearer " + token);

      conn.getOutputStream().write("{\"command\":\"shutdown\"}".getBytes(StandardCharsets.UTF_8));
      conn.getResponseCode();
      conn.disconnect();
    } catch (final java.io.IOException e) {
      throw new RuntimeException("Failed to shutdown remote server '" + serverName + "'", e);
    }
  }

  @Override
  public void disconnectCluster() {
    if (raftHAServer != null)
      raftHAServer.stop();
  }

  @Override
  public void addPeer(final String peerId, final String address) {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");
    raftHAServer.addPeer(peerId, address);
  }

  @Override
  public void removePeer(final String peerId) {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");
    raftHAServer.removePeer(peerId);
  }

  @Override
  public void transferLeadership(final String targetPeerId, final long timeoutMs) {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");
    raftHAServer.transferLeadership(targetPeerId, timeoutMs);
  }

  @Override
  public void stepDown() {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");
    raftHAServer.stepDown();
  }

  @Override
  public void leaveCluster() {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");
    raftHAServer.leaveCluster();
  }

  private boolean isRaftEnabled() {
    return configuration != null
        && configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED);
  }

  private void validateConfiguration() {
    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    if (serverList == null || serverList.isEmpty())
      throw new RuntimeException("HA_SERVER_LIST must be configured for Raft HA");

    // Validate quorum early - will throw ConfigurationException for invalid values
    final Quorum quorum = Quorum.parse(configuration.getValueAsString(GlobalConfiguration.HA_QUORUM));

    final int serverCount = serverList.split(",").length;
    if (quorum == Quorum.ALL && serverCount > 3)
      LogManager.instance().log(this, Level.WARNING,
          "HA_QUORUM=ALL with %d nodes: every node must acknowledge writes. A single slow node will throttle the cluster.",
          serverCount);
  }
}
