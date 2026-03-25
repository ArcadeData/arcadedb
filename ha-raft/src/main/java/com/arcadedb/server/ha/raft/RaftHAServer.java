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
import org.apache.ratis.server.storage.RaftStorage;

import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Manages the Apache Ratis RaftServer instance for ArcadeDB high availability.
 * Handles peer list parsing, server/client lifecycle, and leadership queries.
 */
public class RaftHAServer {

  /**
   * Result of parsing the HA server list. Contains the Raft peers (with raft addresses) and
   * a map from peer ID to HTTP address for replica-to-leader HTTP command forwarding.
   * The {@code httpAddresses} map is empty when no httpPort is specified in the server list.
   */
  public record ParsedPeerList(List<RaftPeer> peers, Map<RaftPeerId, String> httpAddresses) {
  }

  private final ArcadeDBServer             arcadeServer;
  private final ContextConfiguration       configuration;
  private final ArcadeStateMachine         stateMachine;
  private final ClusterMonitor             clusterMonitor;
  private final RaftGroup                  raftGroup;
  private final RaftPeerId                 localPeerId;
  private final Map<RaftPeerId, String>    httpAddresses;
  private final Map<RaftPeerId, String>    peerDisplayNames;

  private RaftServer                raftServer;
  private RaftClient                raftClient;
  private ScheduledExecutorService  lagMonitorExecutor;

  public RaftHAServer(final ArcadeDBServer arcadeServer, final ContextConfiguration configuration) {
    this.arcadeServer = arcadeServer;
    this.configuration = configuration;

    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    final String clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    final long lagWarningThreshold = configuration.getValueAsLong(GlobalConfiguration.HA_REPLICATION_LAG_WARNING);
    final int raftPort = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_PORT);

    final ParsedPeerList parsed = parsePeerList(serverList, raftPort);
    final List<RaftPeer> peers = parsed.peers();
    final String serverName = arcadeServer.getServerName();

    this.httpAddresses = parsed.httpAddresses();
    this.localPeerId = findLocalPeerId(peers, serverName, arcadeServer);
    this.raftGroup = RaftGroup.valueOf(
        RaftGroupId.valueOf(UUID.nameUUIDFromBytes(clusterName.getBytes())),
        peers);

    // Build human-readable display names: "ServerName_N (host:httpPort)"
    final String prefix = serverName.substring(0, serverName.lastIndexOf('_'));
    final Map<RaftPeerId, String> displayNames = new HashMap<>(peers.size());
    for (int i = 0; i < peers.size(); i++) {
      final RaftPeerId peerId = peers.get(i).getId();
      final String nodeName = prefix + "_" + i;
      final String httpAddr = this.httpAddresses.get(peerId);
      displayNames.put(peerId, httpAddr != null ? nodeName + " (" + httpAddr + ")" : nodeName);
    }
    this.peerDisplayNames = Collections.unmodifiableMap(displayNames);

    this.stateMachine = new ArcadeStateMachine();
    this.stateMachine.setServer(arcadeServer);

    this.clusterMonitor = new ClusterMonitor(lagWarningThreshold);

    LogManager.instance().log(this, Level.INFO,
        "RaftHAServer configured: cluster='%s', localPeer='%s', peers=%d",
        clusterName, localPeerId, peers.size());
  }

  /**
   * Parses a comma-separated server list into a {@link ParsedPeerList}.
   * <p>
   * Each entry supports the following formats:
   * <ul>
   *   <li>{@code host:raftPort:httpPort:priority} — explicit Raft port, HTTP port, and leader-election priority</li>
   *   <li>{@code host:raftPort:httpPort} — explicit Raft and HTTP ports, priority defaults to 0</li>
   *   <li>{@code host:raftPort} — explicit Raft port, no HTTP address stored, priority defaults to 0</li>
   *   <li>{@code host} — Raft port defaults to {@code defaultPort}, no HTTP address, priority defaults to 0</li>
   * </ul>
   * The {@code httpAddresses} map in the result is populated only for entries with 3 or 4 parts;
   * it is keyed by the {@link RaftPeerId} of each peer.
   * <p>
   * Priority is used for Raft leader election: the node with the highest priority is preferred as leader.
   * This is a soft preference — if the preferred leader is unavailable, another node will take over.
   */
  static ParsedPeerList parsePeerList(final String serverList, final int defaultPort) {
    final String[] entries = serverList.split(",");
    final List<RaftPeer> peers = new ArrayList<>(entries.length);
    final Map<RaftPeerId, String> httpAddresses = new HashMap<>(entries.length);

    for (int i = 0; i < entries.length; i++) {
      final String entry = entries[i].trim();
      final String[] parts = entry.split(":");

      final String raftAddress;
      String httpAddress = null;
      int priority = 0;

      if (parts.length == 4) {
        // host:raftPort:httpPort:priority
        raftAddress = parts[0] + ":" + parts[1];
        httpAddress = parts[0] + ":" + parts[2];
        priority = Integer.parseInt(parts[3]);
      } else if (parts.length == 3) {
        // host:raftPort:httpPort
        raftAddress = parts[0] + ":" + parts[1];
        httpAddress = parts[0] + ":" + parts[2];
      } else if (parts.length == 2) {
        // host:raftPort
        raftAddress = entry;
      } else {
        // host only - use default Raft port
        raftAddress = entry + ":" + defaultPort;
      }

      final RaftPeer peer = RaftPeer.newBuilder()
          .setId("peer-" + i)
          .setAddress(raftAddress)
          .setPriority(priority)
          .build();
      peers.add(peer);

      if (httpAddress != null)
        httpAddresses.put(peer.getId(), httpAddress);
    }

    return new ParsedPeerList(Collections.unmodifiableList(peers), Collections.unmodifiableMap(httpAddresses));
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
   * Returns a human-readable display name for a peer, e.g. "ArcadeDB_0 (localhost:2480)".
   * Falls back to the raw peer ID string if the peer is unknown.
   */
  public String getPeerDisplayName(final RaftPeerId peerId) {
    final String name = peerDisplayNames.get(peerId);
    return name != null ? name : peerId.toString();
  }

  /**
   * Creates and starts the Ratis RaftServer and RaftClient.
   */
  public void start() throws IOException {
    // Suppress verbose Ratis internal logs — operators see ArcadeDB-level cluster events instead
    java.util.logging.Logger.getLogger("org.apache.ratis").setLevel(java.util.logging.Level.WARNING);

    final RaftProperties properties = new RaftProperties();

    // Extract the Raft port from this peer's address in the server list
    final String localAddress = raftGroup.getPeer(localPeerId).getAddress();
    final int localRaftPort = Integer.parseInt(localAddress.substring(localAddress.lastIndexOf(':') + 1));
    GrpcConfigKeys.Server.setPort(properties, localRaftPort);

    // Configure Raft RPC timeouts for cluster stability
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(2, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(5, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(10, TimeUnit.SECONDS));

    final long snapshotThreshold = configuration.getValueAsLong(GlobalConfiguration.HA_RAFT_SNAPSHOT_THRESHOLD);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, snapshotThreshold);
    RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true);

    final File storageDir = new File(arcadeServer.getRootPath() + File.separator + "raft-storage-" + localPeerId);
    // Only delete existing Raft storage when persistence is not requested.
    // Persistent mode (HA_RAFT_PERSIST_STORAGE=true) is used in tests that restart nodes
    // within a single test run, so the Raft log survives across stop/start calls.
    final boolean persistStorage = configuration.getValueAsBoolean(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE);
    if (storageDir.exists() && !persistStorage)
      deleteRecursive(storageDir);
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    initClusterToken(configuration, storageDir);

    // When persistent storage is requested and the storage directory already has data,
    // use RECOVER mode so Ratis loads the existing Raft log instead of trying to format
    // (which would fail if the group directory already exists).
    final File[] storageDirs = storageDir.listFiles(f -> f.isDirectory() && !f.getName().equals("lost+found"));
    final boolean hasExistingStorage = persistStorage && storageDir.exists()
        && storageDirs != null && storageDirs.length > 0;
    final RaftStorage.StartupOption startupOption = hasExistingStorage
        ? RaftStorage.StartupOption.RECOVER
        : RaftStorage.StartupOption.FORMAT;

    raftServer = RaftServer.newBuilder()
        .setServerId(localPeerId)
        .setGroup(raftGroup)
        .setStateMachine(stateMachine)
        .setProperties(properties)
        .setParameters(new Parameters())
        .setOption(startupOption)
        .build();

    raftServer.start();

    raftClient = RaftClient.newBuilder()
        .setRaftGroup(raftGroup)
        .setProperties(properties)
        .setParameters(new Parameters())
        .build();

    LogManager.instance().log(this, Level.INFO, "Raft cluster joined: %d nodes %s", peerDisplayNames.size(), peerDisplayNames.values());
  }

  /**
   * Stops the Raft client and server, releasing all resources.
   */
  public void stop() {
    stopLagMonitor();
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

  /**
   * Returns the HTTP address (host:port) of the current Raft leader, or {@code null} if the leader
   * is unknown or its HTTP address was not configured in the server list.
   */
  public String getLeaderHttpAddress() {
    final RaftPeerId leaderId = getLeaderId();
    if (leaderId == null)
      return null;
    return httpAddresses.get(leaderId);
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

  /**
   * Initialises the cluster token used for inter-node request forwarding.
   * <ul>
   *   <li>If {@code HA_CLUSTER_TOKEN} is already set in config, nothing changes.</li>
   *   <li>If the token file exists in {@code storageDir}, its value is loaded into config.</li>
   *   <li>Otherwise a new UUID is generated, written to the file, and set in config.</li>
   * </ul>
   */
  static void initClusterToken(final ContextConfiguration configuration, final File storageDir) throws IOException {
    final String configured = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
    if (configured != null && !configured.isBlank())
      return;

    final File tokenFile = new File(storageDir, "cluster-token.txt");
    if (tokenFile.exists()) {
      final String persisted = Files.readString(tokenFile.toPath()).trim();
      configuration.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, persisted);
      return;
    }

    storageDir.mkdirs();
    final String newToken = UUID.randomUUID().toString();
    Files.writeString(tokenFile.toPath(), newToken);
    configuration.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, newToken);
    LogManager.instance().log(RaftHAServer.class, Level.INFO,
        "Generated new cluster token (saved to %s)", tokenFile.getAbsolutePath());
  }

  /**
   * Starts a periodic task that updates the {@link ClusterMonitor} with the leader's commit index.
   * Called when this node becomes the Raft leader.
   */
  void startLagMonitor() {
    if (lagMonitorExecutor != null)
      return;
    lagMonitorExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "arcadedb-raft-lag-monitor");
      t.setDaemon(true);
      return t;
    });
    lagMonitorExecutor.scheduleAtFixedRate(this::checkReplicaLag, 5, 5, TimeUnit.SECONDS);
  }

  /**
   * Stops the periodic lag monitoring task. Called when this node loses leadership.
   */
  void stopLagMonitor() {
    if (lagMonitorExecutor != null) {
      lagMonitorExecutor.shutdownNow();
      lagMonitorExecutor = null;
    }
  }

  private void checkReplicaLag() {
    try {
      final var division = raftServer.getDivision(raftGroup.getGroupId());
      final var info = division.getInfo();
      if (!info.isLeader())
        return;
      final long commitIndex = info.getLastAppliedIndex();
      clusterMonitor.updateLeaderCommitIndex(commitIndex);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Error checking replica lag", e);
    }
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
