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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;

import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Manages the Apache Ratis RaftServer instance for ArcadeDB high availability.
 * Handles peer list parsing, server/client lifecycle, and leadership queries.
 */
public class RaftHAServer {

  private final ArcadeDBServer      arcadeServer;
  private final ContextConfiguration configuration;
  private final ArcadeStateMachine  stateMachine;
  private final ClusterMonitor      clusterMonitor;
  private final RaftGroup           raftGroup;
  private final RaftPeerId          localPeerId;

  private RaftServer raftServer;
  private RaftClient raftClient;

  public RaftHAServer(final ArcadeDBServer arcadeServer, final ContextConfiguration configuration) {
    this.arcadeServer = arcadeServer;
    this.configuration = configuration;

    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    final String clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    final int raftPort = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_PORT);
    final long lagWarningThreshold = configuration.getValueAsLong(GlobalConfiguration.HA_REPLICATION_LAG_WARNING);

    final List<RaftPeer> peers = parsePeerList(serverList, raftPort);
    final String serverName = arcadeServer.getServerName();

    this.localPeerId = findLocalPeerId(peers, serverName, arcadeServer);
    this.raftGroup = RaftGroup.valueOf(
        RaftGroupId.valueOf(UUID.nameUUIDFromBytes(clusterName.getBytes())),
        peers);

    this.stateMachine = new ArcadeStateMachine();
    this.stateMachine.setServer(arcadeServer);

    this.clusterMonitor = new ClusterMonitor(lagWarningThreshold);

    LogManager.instance().log(this, Level.INFO,
        "RaftHAServer configured: cluster='%s', localPeer='%s', peers=%d",
        clusterName, localPeerId, peers.size());
  }

  /**
   * Parses a comma-separated server list (host:httpPort) into RaftPeer objects.
   * Each peer is assigned a unique Raft port: peer-0 gets baseRaftPort, peer-1 gets baseRaftPort+1, etc.
   */
  static List<RaftPeer> parsePeerList(final String serverList, final int baseRaftPort) {
    final String[] entries = serverList.split(",");
    final List<RaftPeer> peers = new ArrayList<>(entries.length);

    for (int i = 0; i < entries.length; i++) {
      final String entry = entries[i].trim();
      final String host = entry.substring(0, entry.indexOf(':'));
      final RaftPeer peer = RaftPeer.newBuilder()
          .setId("peer-" + i)
          .setAddress(host + ":" + (baseRaftPort + i))
          .build();
      peers.add(peer);
    }

    return Collections.unmodifiableList(peers);
  }

  /**
   * Determines the local peer ID by parsing the numeric suffix from the server name.
   * For example, "ArcadeDB_0" maps to index 0, which corresponds to "peer-0".
   */
  static RaftPeerId findLocalPeerId(final List<RaftPeer> peers, final String serverName,
      final ArcadeDBServer server) {
    final int underscoreIdx = serverName.lastIndexOf('_');
    if (underscoreIdx < 0 || underscoreIdx == serverName.length() - 1)
      throw new IllegalArgumentException("Cannot parse server index from server name: " + serverName);

    final int index = Integer.parseInt(serverName.substring(underscoreIdx + 1));
    if (index < 0 || index >= peers.size())
      throw new IllegalArgumentException(
          "Server index " + index + " from name '" + serverName + "' is out of range [0, " + peers.size() + ")");

    return peers.get(index).getId();
  }

  /**
   * Creates and starts the Ratis RaftServer and RaftClient.
   */
  public void start() throws IOException {
    final RaftProperties properties = new RaftProperties();

    // Use the port assigned to this specific peer (baseRaftPort + peerIndex)
    final int localPeerIndex = Integer.parseInt(localPeerId.toString().substring("peer-".length()));
    final int baseRaftPort = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_PORT);
    final int localRaftPort = baseRaftPort + localPeerIndex;
    GrpcConfigKeys.Server.setPort(properties, localRaftPort);

    // Configure Raft RPC timeouts for cluster stability
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(2, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(5, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(10, TimeUnit.SECONDS));

    final File storageDir = new File(arcadeServer.getRootPath() + File.separator + "raft-storage-" + localPeerId);
    // Clean existing Raft storage to avoid FORMAT conflicts on restart
    if (storageDir.exists())
      deleteRecursive(storageDir);
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    raftServer = RaftServer.newBuilder()
        .setServerId(localPeerId)
        .setGroup(raftGroup)
        .setStateMachine(stateMachine)
        .setProperties(properties)
        .setParameters(new Parameters())
        .build();

    raftServer.start();

    raftClient = RaftClient.newBuilder()
        .setRaftGroup(raftGroup)
        .setProperties(properties)
        .setParameters(new Parameters())
        .build();

    LogManager.instance().log(this, Level.INFO, "RaftHAServer started: peerId='%s'", localPeerId);
  }

  /**
   * Stops the Raft client and server, releasing all resources.
   */
  public void stop() {
    if (raftClient != null) {
      try {
        raftClient.close();
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING, "Error closing RaftClient", e);
      }
      raftClient = null;
    }

    if (raftServer != null) {
      try {
        raftServer.close();
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING, "Error closing RaftServer", e);
      }
      raftServer = null;
    }

    LogManager.instance().log(this, Level.INFO, "RaftHAServer stopped");
  }

  /**
   * Returns true if this server is the current Raft leader.
   */
  public boolean isLeader() {
    if (raftServer == null)
      return false;

    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getInfo().isLeader();
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Error checking leader status", e);
      return false;
    }
  }

  /**
   * Returns the peer ID of the current Raft leader, or null if unknown.
   */
  public RaftPeerId getLeaderId() {
    if (raftServer == null)
      return null;

    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getInfo().getLeaderId();
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Error getting leader ID", e);
      return null;
    }
  }

  public RaftClient getClient() {
    return raftClient;
  }

  public ArcadeStateMachine getStateMachine() {
    return stateMachine;
  }

  public ClusterMonitor getClusterMonitor() {
    return clusterMonitor;
  }

  public RaftGroup getRaftGroup() {
    return raftGroup;
  }

  public RaftPeerId getLocalPeerId() {
    return localPeerId;
  }

  private static void deleteRecursive(final File file) {
    if (file.isDirectory()) {
      final File[] children = file.listFiles();
      if (children != null)
        for (final File child : children)
          deleteRecursive(child);
    }
    file.delete();
  }
}
