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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.HostUtil;
import com.arcadedb.network.binary.ChannelBinaryClient;
import com.arcadedb.network.binary.ConnectionException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ReplicationCallback;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.ha.message.ErrorResponse;
import com.arcadedb.server.ha.message.HACommand;
import com.arcadedb.server.ha.message.HAMessageFactory;
import com.arcadedb.server.ha.message.UpdateClusterConfiguration;
import com.arcadedb.server.ha.network.DefaultServerSocketFactory;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.utility.DateUtils;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.RecordTableFormatter;
import com.arcadedb.utility.TableFormatter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class HAServer implements ServerPlugin {
  public static final String                                         DEFAULT_PORT                      = HostUtil.HA_DEFAULT_PORT;
  private volatile    int                                            configuredServers                 = 1;
  private volatile    ElectionStatus                                 electionStatus                    = HAServer.ElectionStatus.DONE;
  private final       HAMessageFactory                               messageFactory;
  private final       ArcadeDBServer                                 server;
  private final       ContextConfiguration                           configuration;
  private final       String                                         clusterName;
  private final       long                                           startedOn;
  // Use server name (alias) as stable identity for replica connections
  // This prevents identity changes when ServerInfo network addresses are updated
  private final       Map<String, Leader2ReplicaNetworkExecutor>     replicaConnections                = new ConcurrentHashMap<>();
  private final       Map<String, ServerInfo>                        serverInfoByName                  = new ConcurrentHashMap<>();
  private final       Map<String, String>                            replicaHTTPAddresses              = new ConcurrentHashMap<>();
  private final       AtomicReference<Replica2LeaderNetworkExecutor> leaderConnection                  = new AtomicReference<>();
  private final       AtomicLong                                     lastDistributedOperationNumber    = new AtomicLong(-1);
  private final       AtomicLong                                     lastForwardOperationNumber        = new AtomicLong(0);
  private final       Map<Long, QuorumMessage>                       messagesWaitingForQuorum          = new ConcurrentHashMap<>(
      1024);
  private final       Map<Long, ForwardedMessage>                    forwardMessagesWaitingForResponse = new ConcurrentHashMap<>(
      1024);
  private final       Object                                         sendingLock                       = new Object();

  //  private final       Set<ServerInfo>                                serverAddressList                 = new HashSet<>();
  private          HACluster             cluster;
  private final    ServerRole            serverRole;
  protected final  String                replicationPath;
  private          LeaderNetworkListener listener;
  private          long                  lastConfigurationOutputHash = 0;
  private          ServerInfo            serverAddress;
  //  private         String                replicasHTTPAddresses;
  private          boolean               started;
  private          Thread                electionThread;
  private          ReplicationLogFile    replicationLogFile;
  protected        Pair<Long, String>    lastElectionVote;
  private final    LeaderFence           leaderFence;
  private volatile long                  lastElectionStartTime       = 0;
  private          ConsistencyMonitor    consistencyMonitor;

  public record ServerInfo(String host, int port, String alias, String actualHost, Integer actualPort) {

    /**
     * Constructor for configured-only address (backwards compatible).
     * This is used when we only have the configured/stable address (e.g., from server list).
     */
    public ServerInfo(String host, int port, String alias) {
      this(host, port, alias, null, null);
    }

    public static ServerInfo fromString(String address) {
      final String[] parts = HostUtil.parseHostAddress(address, DEFAULT_PORT);
      return new ServerInfo(parts[0], Integer.parseInt(parts[1]), parts[2]);
    }

    /**
     * Gets the host to use for network connections.
     * Always returns the configured address (proxy address in Docker scenarios).
     */
    public String getConnectHost() {
      return host;
    }

    /**
     * Gets the port to use for network connections.
     * Always returns the configured port (proxy port in Docker scenarios).
     */
    public int getConnectPort() {
      return port;
    }

    /**
     * Gets the actual address for informational/debugging purposes.
     * Returns null if no actual address is tracked.
     */
    public String getActualAddress() {
      if (actualHost != null && actualPort != null && actualPort > 0) {
        return actualHost + ":" + actualPort;
      }
      return null;
    }

    /**
     * Creates a new ServerInfo with updated actual address while preserving configured address.
     */
    public ServerInfo withActualAddress(String actualHost, int actualPort) {
      return new ServerInfo(this.host, this.port, this.alias, actualHost, actualPort);
    }

    @Override
    public String toString() {
      return "{%s}%s:%d".formatted(alias, host, port);
    }

    /**
     * Returns a detailed string representation showing both configured and actual addresses.
     */
    public String toDetailedString() {
      final String actual = getActualAddress();
      if (actual != null && !actual.equals(host + ":" + port)) {
        return "{%s}%s:%d (actual: %s)".formatted(alias, host, port, actual);
      }
      return toString();
    }
  }

  public static class HACluster {

    public final Set<ServerInfo> servers;

    public HACluster(Set<ServerInfo> servers) {
      this.servers = servers;
    }

    public Set<ServerInfo> getServers() {
      return servers;
    }

    public int clusterSize() {
      return servers.size();
    }

    @Override
    public String toString() {
      return "HACluster{" +
          "servers=" + servers.stream().map(ServerInfo::toString).collect(Collectors.joining(",")) +
          '}';
    }

    public Optional<ServerInfo> findByAlias(String serverAlias) {
      for (ServerInfo server : servers) {
        if (server.alias.equals(serverAlias)) {
          LogManager.instance().log(this, Level.FINE, "find by alias %s - Found server %s", serverAlias, server);
          return Optional.of(server);
        }
      }

      // Not finding by alias is normal when servers have duplicate aliases (e.g., all "localhost")
      // Caller should use fallback matching strategies
      LogManager.instance().log(this, Level.FINE, "Server with alias '%s' not found in cluster %s", serverAlias, servers);
      return Optional.empty();
    }

    /**
     * Finds a server by host and port combination.
     * This is useful when we need to match a server by its network address.
     *
     * @param host the hostname or IP address
     * @param port the port number
     *
     * @return Optional containing the ServerInfo if found, empty otherwise
     */
    public Optional<ServerInfo> findByHostAndPort(String host, int port) {
      for (ServerInfo server : servers) {
        if (server.host.equals(host) && server.port == port) {
          return Optional.of(server);
        }
      }
      return Optional.empty();
    }

  }

  public enum Quorum {
    NONE, ONE, TWO, THREE, MAJORITY, ALL;

    public int quorum(int numberOfServers) {
      return switch (this) {
        case NONE -> 0;
        case ONE -> 1;
        case TWO -> 2;
        case THREE -> 3;
        case MAJORITY -> numberOfServers / 2 + 1;
        case ALL -> numberOfServers;
      };
    }
  }

  public enum ElectionStatus {
    DONE, VOTING_FOR_ME, VOTING_FOR_OTHERS, LEADER_WAITING_FOR_QUORUM
  }

  public enum ServerRole {
    ANY, REPLICA
  }

  private static class QuorumMessage {
    public final long           sentOn = System.currentTimeMillis();
    public final CountDownLatch semaphore;
    public       List<Object>   payloads;

    public QuorumMessage(final CountDownLatch quorumSemaphore) {
      this.semaphore = quorumSemaphore;
    }
  }

  private static class ForwardedMessage {
    public final CountDownLatch semaphore;
    public       ErrorResponse  error;
    public       Object         result;

    public ForwardedMessage() {
      this.semaphore = new CountDownLatch(1);
    }
  }

  public HAServer(final ArcadeDBServer server, final ContextConfiguration configuration) {
    if (!configuration.getValueAsBoolean(GlobalConfiguration.TX_WAL))
      throw new ConfigurationException("Cannot start HA service without using WAL. Please enable the TX_WAL setting");

    this.server = server;
    this.messageFactory = new HAMessageFactory(server);
    this.configuration = configuration;
    this.clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    this.startedOn = System.currentTimeMillis();
    this.replicationPath = server.getRootPath() + "/replication";
    this.serverRole = ServerRole.valueOf(
        configuration.getValueAsString(GlobalConfiguration.HA_SERVER_ROLE).toUpperCase(Locale.ENGLISH));
    this.leaderFence = new LeaderFence(server.getServerName());
  }

  @Override
  public void startService() {
    if (started)
      return;

    waitForHttpServerConnection();

    started = true;
    initializeReplicationLogFile();

    listener = new LeaderNetworkListener(this, new DefaultServerSocketFactory(),
        configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST),
        configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS));

    // Determine the server's alias and configured address from server list
    final Pair<String, ServerInfo> aliasAndConfigured = determineServerAliasAndAddress(listener.getPort());
    final String configuredAlias = aliasAndConfigured.getFirst();
    final ServerInfo configuredInfo = aliasAndConfigured.getSecond();

    // Use configured address if found (for ToxiProxy/Docker scenarios), otherwise fall back to actual address
    if (configuredInfo != null) {
      // Track both configured (for communication) and actual (for info/debugging)
      serverAddress = new ServerInfo(
          configuredInfo.host(),           // Configured proxy address (stable)
          configuredInfo.port(),            // Configured proxy port (stable)
          configuredAlias,
          server.getHostAddress(),          // Actual Docker address (volatile)
          listener.getPort()                // Actual port
      );
      LogManager.instance().log(this, Level.INFO, "Starting HA service: configured=%s, actual=%s:%d",
          serverAddress, server.getHostAddress(), listener.getPort());
    } else {
      // No proxy configuration, use actual address only
      serverAddress = new ServerInfo(server.getHostAddress(), listener.getPort(), configuredAlias);
      LogManager.instance().log(this, Level.INFO, "Starting HA service on %s", serverAddress);
    }

    configureCluster();

    // Start consistency monitor if enabled
    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_CONSISTENCY_CHECK_ENABLED)) {
      consistencyMonitor = new ConsistencyMonitor(this);
      consistencyMonitor.start();
      LogManager.instance().log(this, Level.INFO, "Consistency monitor started");
    }

    if (leaderConnection.get() == null && serverRole != ServerRole.REPLICA) {
      startElection(false);
    }
  }

  private void waitForHttpServerConnection() {
    final long timeout = configuration.getValueAsLong(GlobalConfiguration.HA_HTTP_STARTUP_TIMEOUT);
    final long startTime = System.currentTimeMillis();

    while (!server.getHttpServer().isConnected()) {
      if (System.currentTimeMillis() - startTime > timeout) {
        throw new ServerException(
            "Timeout waiting for HTTP server to start after " + timeout + "ms. Check HTTP server configuration.");
      }
      CodeUtils.sleep(200);
    }
  }

  private void initializeReplicationLogFile() {
    final String fileName = replicationPath + "/replication_" + server.getServerName() + ".rlog";
    try {
      replicationLogFile = new ReplicationLogFile(fileName);
      lastDistributedOperationNumber.set(replicationLogFile.getLastMessageNumber());
      if (lastDistributedOperationNumber.get() > -1) {
        LogManager.instance().log(this, Level.FINE, "Found an existent replication log. Starting messages from %d",
            lastDistributedOperationNumber.get());
      }
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on creating replication file '%s' for remote server '%s'", fileName,
          server.getServerName());
      stopService();
      throw new ReplicationLogException("Error on creating replication file '" + fileName + "'", e);
    }
  }

  private void configureCluster() {
    final String cfgServerList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST).trim();
    if (!cfgServerList.isEmpty()) {
      final String[] serverEntries = cfgServerList.split(",");

      configuredServers = serverEntries.length;

      LogManager.instance()
          .log(this, Level.FINE, "Connecting to servers %s (cluster=%s configuredServers=%d)", cfgServerList, clusterName,
              configuredServers);
      checkAllOrNoneAreLocalhosts(serverEntries);

      cluster = new HACluster(parseServerList(cfgServerList));
      for (final ServerInfo serverEntry : cluster.servers) {

        if (!isCurrentServer(serverEntry) && connectToLeader(serverEntry, null)) {
          break;
        }
      }
    }
  }

  protected boolean isCurrentServer(final ServerInfo serverEntry) {
    if (serverAddress.equals(serverEntry))
      return true;

//    final String[] localServerParts = HostUtil.parseHostAddress(serverAddress, DEFAULT_PORT);

    try {
//      final String[] serverParts = HostUtil.parseHostAddress(serverEntry, DEFAULT_PORT);
      if (serverAddress.host.equals(serverEntry.host) && serverAddress.port == serverEntry.port)
        return true;

      final InetAddress localhostAddress = InetAddress.getLocalHost();

      if (localhostAddress.getHostAddress().equals(serverEntry.host) && serverAddress.host.equals(serverEntry.host))
        return true;

      if (localhostAddress.getHostName().equals(serverEntry.host) && serverAddress.port == serverEntry.port)
        return true;

    } catch (final UnknownHostException e) {
      // IGNORE THIS EXCEPTION AND RETURN FALSE
    }
    return false;
  }

  @Override
  public void stopService() {
    started = false;

    // Stop consistency monitor if running
    if (consistencyMonitor != null) {
      consistencyMonitor.shutdown();
      try {
        consistencyMonitor.join(5000); // Wait up to 5 seconds for clean shutdown
      } catch (final InterruptedException e) {
        LogManager.instance().log(this, Level.WARNING, "Interrupted while waiting for consistency monitor shutdown");
        Thread.currentThread().interrupt();
      }
      consistencyMonitor = null;
    }

    if (listener != null)
      listener.close();

    final Replica2LeaderNetworkExecutor lc = leaderConnection.get();
    if (lc != null) {
      lc.close();
      leaderConnection.set(null);
    }

    if (!replicaConnections.isEmpty()) {
      for (final Leader2ReplicaNetworkExecutor r : replicaConnections.values()) {
        r.close();
      }
      replicaConnections.clear();
    }

    if (replicationLogFile != null)
      replicationLogFile.close();
  }

  public void startElection(final boolean waitForCompletion) {
    synchronized (this) {
      // Check for election cooldown to prevent election storms
      if (isInElectionCooldown()) {
        final long remaining = getRemainingCooldown();
        LogManager.instance().log(this, Level.INFO,
            "Election requested but in cooldown period. %dms remaining", remaining);
        return;
      }

      if (electionThread == null) {
        // Record the election start time for cooldown tracking
        lastElectionStartTime = System.currentTimeMillis();

        electionThread = new Thread(this::startElection, getServerName() + " election");
        electionThread.start();
        if (waitForCompletion) {
          try {
            electionThread.join(60 * 1_000);
          } catch (InterruptedException e) {
            LogManager.instance().log(this, Level.SEVERE, "Timeout on election process");
            // IGNORE IT
          }
        }
      }
    }
  }

  private boolean checkForExistentLeaderConnection(final long electionTurn) {
    final Replica2LeaderNetworkExecutor lc = leaderConnection.get();
    if (lc != null) {
      // I AM A REPLICA, NO LEADER ELECTION IS NEEDED
      LogManager.instance()
          .log(this, Level.INFO, "Abort election process, a Leader (%s) has been already found (turn=%d)", lc.getRemoteServerName(),
              electionTurn);
      return true;
    }
    return false;
  }

  private void sendNewLeadershipToOtherNodes() {
    lastDistributedOperationNumber.set(replicationLogFile.getLastMessageNumber());

    // Create a new epoch for this leadership term.
    // The epoch number is based on the election turn which is monotonically increasing.
    final LeaderEpoch newEpoch = LeaderEpoch.create(lastElectionVote.getFirst(), getServerName());
    leaderFence.becomeLeader(newEpoch);

    // Ensure all databases are accessible before completing leadership transition
    // This prevents DatabaseIsClosedException during leader failover
    ensureDatabasesAccessible();

    setElectionStatus(ElectionStatus.LEADER_WAITING_FOR_QUORUM);

    LogManager.instance()
        .log(this, Level.INFO, "Contacting all the servers for the new leadership (turn=%d epoch=%s)...",
            lastElectionVote.getFirst(), newEpoch);

    for (final ServerInfo serverAddress : cluster.servers) {
      if (isCurrentServer(serverAddress))
        // SKIP LOCAL SERVER
        continue;

      try {
//        final String[] parts = HostUtil.parseHostAddress(serverAddress, DEFAULT_PORT);

        LogManager.instance().log(this, Level.INFO, "- Sending new Leader to server '%s'...", serverAddress);

        final ChannelBinaryClient channel = createNetworkConnection(serverAddress, ReplicationProtocol.COMMAND_ELECTION_COMPLETED);
        channel.writeLong(lastElectionVote.getFirst());
        channel.flush();

      } catch (final Exception e) {
        LogManager.instance().log(this, Level.INFO, "Error contacting server %s for election", serverAddress);
      }
    }
  }

  /**
   * Get all replica connections for health monitoring.
   * Returns a defensive copy to prevent external modification.
   *
   * @return map of replica name to network executor
   */
  public Map<String, Leader2ReplicaNetworkExecutor> getReplicaConnections() {
    if (!isLeader()) {
      return java.util.Collections.emptyMap();
    }
    // Return defensive copy
    return new HashMap<>(replicaConnections);
  }

  /**
   * Gets a replica connection by server name (alias).
   * This is the primary method for accessing replica connections.
   *
   * @param replicaName the server name (alias)
   *
   * @return the replica network executor, or null if not found
   */
  public Leader2ReplicaNetworkExecutor getReplica(final String replicaName) {
    return replicaConnections.get(replicaName);
  }

  /**
   * Gets a replica connection by ServerInfo.
   * This method extracts the alias from ServerInfo and delegates to getReplica(String).
   *
   * @param replicaInfo the ServerInfo identifying the replica server
   *
   * @return the replica network executor, or null if not found
   */
  public Leader2ReplicaNetworkExecutor getReplica(final ServerInfo replicaInfo) {
    return getReplica(replicaInfo.alias());
  }

  /**
   * Gets ServerInfo for a replica by server name.
   *
   * @param replicaName the server name (alias)
   *
   * @return the ServerInfo, or null if not found
   */
  public ServerInfo getReplicaServerInfo(final String replicaName) {
    return serverInfoByName.get(replicaName);
  }

  public void disconnectAllReplicas() {
    final List<Leader2ReplicaNetworkExecutor> replicas = new ArrayList<>(replicaConnections.values());
    replicaConnections.clear();
    serverInfoByName.clear();

    for (Leader2ReplicaNetworkExecutor replica : replicas) {
      try {
        replica.close();
        setReplicaStatus(replica.getRemoteServerName().alias(), false);
      } catch (Exception e) {
        // IGNORE IT
      }
    }
    configuredServers = 1;
  }

  public void setReplicaStatus(final String remoteServerName, final boolean online) {
    final Leader2ReplicaNetworkExecutor c = replicaConnections.get(remoteServerName);
    if (c == null) {
      LogManager.instance().log(this, Level.SEVERE,
          "Replica '%s' was not registered. Available replicas: %s",
          remoteServerName, replicaConnections.keySet());
      return;
    }

    final Leader2ReplicaNetworkExecutor.STATUS oldStatus = c.getStatus();
    final Leader2ReplicaNetworkExecutor.STATUS newStatus = online ?
        Leader2ReplicaNetworkExecutor.STATUS.ONLINE :
        Leader2ReplicaNetworkExecutor.STATUS.OFFLINE;
    c.setStatus(newStatus);

    LogManager.instance().log(this, Level.INFO,
        "Replica '%s' status changed: %s -> %s (online replicas now: %d)",
        remoteServerName, oldStatus, newStatus, getOnlineReplicas());

    try {
      server.lifecycleEvent(online ? ReplicationCallback.Type.REPLICA_ONLINE : ReplicationCallback.Type.REPLICA_OFFLINE,
          remoteServerName);
    } catch (final Exception e) {
      // IGNORE IT
    }

    if (electionStatus == ElectionStatus.LEADER_WAITING_FOR_QUORUM) {
      if (getOnlineServers() >= configuredServers / 2 + 1)
        // ELECTION COMPLETED
        setElectionStatus(ElectionStatus.DONE);
    }
  }

  /**
   * Overload for backward compatibility with ServerInfo.
   */
  public void setReplicaStatus(final ServerInfo remoteServerInfo, final boolean online) {
    setReplicaStatus(remoteServerInfo.alias(), online);
  }

  public void receivedResponse(final ServerInfo remoteServerName, final long messageNumber, final Object payload) {
    final long receivedOn = System.currentTimeMillis();

    final QuorumMessage msg = messagesWaitingForQuorum.get(messageNumber);
    if (msg == null)
      // QUORUM ALREADY REACHED OR TIMEOUT
      return;

    if (payload != null) {
      synchronized (msg) {
        if (msg.payloads == null)
          msg.payloads = new ArrayList<>();
        msg.payloads.add(payload);
      }
    }

    msg.semaphore.countDown();

    // UPDATE LATENCY
    final Leader2ReplicaNetworkExecutor c = replicaConnections.get(remoteServerName);
    if (c != null)
      c.updateStats(msg.sentOn, receivedOn);
  }

  public void receivedResponseFromForward(final long messageNumber, final Object result, final ErrorResponse error) {
    final ForwardedMessage msg = forwardMessagesWaitingForResponse.get(messageNumber);
    if (msg == null)
      // QUORUM ALREADY REACHED OR TIMEOUT
      return;

    LogManager.instance().log(this, Level.FINE, "Forwarded message %d has been executed", messageNumber);

    msg.result = result;
    msg.error = error;
    msg.semaphore.countDown();
  }

  public ReplicationLogFile getReplicationLogFile() {
    return replicationLogFile;
  }

  public ArcadeDBServer getServer() {
    return server;
  }

  public boolean isLeader() {
    return leaderConnection.get() == null;
  }

  public String getLeaderName() {
    return leaderConnection.get() == null ? getServerName() : leaderConnection.get().getRemoteServerName();
  }

  public Replica2LeaderNetworkExecutor getLeader() {
    return leaderConnection.get();
  }

  public String getServerName() {
    return server.getServerName();
  }

  public String getClusterName() {
    return clusterName;
  }

  /**
   * Gets the leader fence used for split-brain prevention.
   *
   * @return The LeaderFence instance
   */
  public LeaderFence getLeaderFence() {
    return leaderFence;
  }

  /**
   * Gets the current leader epoch, or null if not set.
   *
   * @return The current LeaderEpoch or null
   */
  public LeaderEpoch getCurrentEpoch() {
    return leaderFence.getCurrentEpoch();
  }

  public void registerIncomingConnection(final ServerInfo replicaServerInfo, final Leader2ReplicaNetworkExecutor connection) {
    // Use server name (alias) as stable identity
    final String serverName = replicaServerInfo.alias();

    // Register connection using server name as key
    final Leader2ReplicaNetworkExecutor previousConnection = replicaConnections.put(serverName, connection);
    if (previousConnection != null && previousConnection != connection) {
      // MERGE CONNECTIONS
      connection.mergeFrom(previousConnection);
    }

    // Update or merge ServerInfo, preserving configured addresses if known
    serverInfoByName.compute(serverName, (name, existingInfo) -> {
      if (existingInfo == null) {
        // First time seeing this server - check cluster for configured address
        if (cluster != null) {
          final ServerInfo configuredInfo = cluster.findByAlias(serverName).orElse(null);
          if (configuredInfo != null && !configuredInfo.host().equals(replicaServerInfo.host())) {
            // Preserve configured address, track actual address
            LogManager.instance().log(this, Level.FINE,
                "Preserving configured address for %s: configured=%s:%d, actual=%s:%d",
                serverName, configuredInfo.host(), configuredInfo.port(),
                replicaServerInfo.host(), replicaServerInfo.port());
            return new ServerInfo(
                configuredInfo.host(),      // Keep configured proxy address
                configuredInfo.port(),       // Keep configured proxy port
                serverName,
                replicaServerInfo.host(),    // Store actual connection address
                replicaServerInfo.port()     // Store actual connection port
            );
          }
        }
        return replicaServerInfo;
      } else {
        // Update existing info: preserve configured address, update actual address if changed
        if (replicaServerInfo.host().equals(existingInfo.host()) &&
            replicaServerInfo.port() == existingInfo.port()) {
          // Connection from configured address - keep as is
          return existingInfo;
        } else {
          // Connection from different address - track as actual address
          LogManager.instance().log(this, Level.FINE,
              "Updating actual address for %s: was=%s:%d, now=%s:%d",
              serverName,
              existingInfo.actualHost(), existingInfo.actualPort(),
              replicaServerInfo.host(), replicaServerInfo.port());
          return new ServerInfo(
              existingInfo.host(),          // Keep configured address
              existingInfo.port(),
              serverName,
              replicaServerInfo.host(),     // Update actual address
              replicaServerInfo.port()
          );
        }
      }
    });

    final int totReplicas = replicaConnections.size();
    if (1 + totReplicas > configuredServers)
      // UPDATE SERVER COUNT
      configuredServers = 1 + totReplicas;

    // Build the actual cluster membership: leader + all connected replicas
    final Set<ServerInfo> currentMembers = new HashSet<>();
    currentMembers.add(serverAddress); // Add self (the leader) with configured address

    // Add all replicas from serverInfoByName (which has stable, merged ServerInfo)
    currentMembers.addAll(serverInfoByName.values());

    // Update the cluster to reflect actual membership with preserved configured addresses
    cluster = new HACluster(currentMembers);

    sendCommandToReplicasNoLog(new UpdateClusterConfiguration(cluster));

    printClusterConfiguration();
  }

  public ElectionStatus getElectionStatus() {
    return electionStatus;
  }

  protected void setElectionStatus(final ElectionStatus status) {
    LogManager.instance().log(this, Level.INFO, "Change election status from %s to %s", this.electionStatus, status);
    this.electionStatus = status;
  }

  /**
   * Checks if the server is currently in an election cooldown period.
   * This prevents election storms where nodes rapidly trigger elections.
   *
   * @return true if in cooldown period, false otherwise
   */
  public boolean isInElectionCooldown() {
    final long cooldownMs = configuration.getValueAsLong(GlobalConfiguration.HA_ELECTION_COOLDOWN);
    final long elapsed = System.currentTimeMillis() - lastElectionStartTime;
    return elapsed < cooldownMs;
  }

  /**
   * Gets the remaining cooldown time in milliseconds.
   *
   * @return Remaining cooldown time, or 0 if not in cooldown
   */
  public long getRemainingCooldown() {
    final long cooldownMs = configuration.getValueAsLong(GlobalConfiguration.HA_ELECTION_COOLDOWN);
    final long elapsed = System.currentTimeMillis() - lastElectionStartTime;
    return Math.max(0, cooldownMs - elapsed);
  }

  public HAMessageFactory getMessageFactory() {
    return messageFactory;
  }

  public Set<ServerInfo> parseServerList(final String serverList) {
    final Set<ServerInfo> servers = new HashSet<>();
    if (serverList != null && !serverList.isEmpty()) {
      final String[] serverEntries = serverList.split(",");

      for (String entry : serverEntries) {
        final String[] parts = HostUtil.parseHostAddress(entry, DEFAULT_PORT);
        servers.add(new ServerInfo(parts[0], Integer.parseInt(parts[1]), parts[2]));
      }
    }
    return servers;
  }

  /**
   * Determines the server's alias by finding itself in the configured server list.
   * This ensures the server uses the same alias as configured in the cluster,
   * rather than just using the SERVER_NAME configuration.
   *
   * @param actualPort The actual port the server is listening on
   *
   * @return The alias from the server list, or SERVER_NAME as fallback
   */
  private String determineServerAlias(final int actualPort) {
    return determineServerAliasAndAddress(actualPort).getFirst();
  }

  /**
   * Determines the server's alias and configured address by finding itself in the server list.
   * This ensures we use the configured proxy address rather than the Docker container address.
   * In Docker/ToxiProxy scenarios, the configured address (e.g., proxy:8666) is stable across
   * container restarts, while the actual Docker address (e.g., 81014e8c51c1:2424) changes.
   *
   * @param actualPort The actual port the server is listening on
   *
   * @return Pair of (alias, configured ServerInfo) or (alias, null) if not found in server list
   */
  private Pair<String, ServerInfo> determineServerAliasAndAddress(final int actualPort) {
    final String cfgServerList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST).trim();
    if (cfgServerList.isEmpty()) {
      // No server list configured, use SERVER_NAME
      return new Pair<>(server.getServerName(), null);
    }

    final Set<ServerInfo> configuredServers = parseServerList(cfgServerList);
    final String currentHost = server.getHostAddress();

    // Try to find ourselves in the configured server list
    ServerInfo matchedServer = null;

    // First, try matching by host:port
    for (ServerInfo serverInfo : configuredServers) {
      if (isMatchingServer(serverInfo, currentHost, actualPort)) {
        matchedServer = serverInfo;
        break;
      }
    }

    // Fallback: If not found by host:port, try matching by SERVER_NAME in alias
    if (matchedServer == null) {
      final String serverName = server.getServerName();
      for (ServerInfo serverInfo : configuredServers) {
        if (serverInfo.alias().equals(serverName)) {
          matchedServer = serverInfo;
          LogManager.instance().log(this, Level.INFO,
              "Matched server '%s' by SERVER_NAME (alias) - configured=%s:%d, actual=%s:%d",
              serverName, serverInfo.host(), serverInfo.port(), currentHost, actualPort);
          break;
        }
      }
    }

    if (matchedServer != null) {
      // Check if the alias is unique in the server list
      if (isAliasUnique(matchedServer.alias(), configuredServers)) {
        LogManager.instance().log(this, Level.INFO,
            "Found unique server alias '%s' with configured address %s:%d for actual %s:%d",
            matchedServer.alias(), matchedServer.host(), matchedServer.port(), currentHost, actualPort);
        return new Pair<>(matchedServer.alias(), matchedServer);
      } else {
        // Alias is not unique (e.g., all servers have "localhost" as alias)
        // Use SERVER_NAME to ensure uniqueness, but still return the configured address
        LogManager.instance().log(this, Level.WARNING,
            "Server alias '%s' from server list is not unique, using SERVER_NAME '%s' instead",
            matchedServer.alias(), server.getServerName());
        return new Pair<>(server.getServerName(), matchedServer);
      }
    }

    // Fallback: not found in server list, use SERVER_NAME and no configured address
    LogManager.instance().log(this, Level.WARNING,
        "Could not find %s:%d in configured server list %s, using SERVER_NAME '%s' as alias",
        currentHost, actualPort, cfgServerList, server.getServerName());
    return new Pair<>(server.getServerName(), null);
  }

  /**
   * Checks if an alias is unique in the server list.
   */
  private boolean isAliasUnique(final String alias, final Set<ServerInfo> servers) {
    long count = servers.stream().filter(s -> s.alias().equals(alias)).count();
    return count == 1;
  }

  /**
   * Checks if a ServerInfo matches the current server's host and port.
   * Handles localhost variants and network aliases.
   */
  private boolean isMatchingServer(final ServerInfo serverInfo, final String currentHost, final int actualPort) {
    // Port must match
    if (serverInfo.port() != actualPort) {
      return false;
    }

    // Exact host match
    if (serverInfo.host().equals(currentHost)) {
      return true;
    }

    // Handle localhost variants
    final boolean currentIsLocalhost = isLocalhostVariant(currentHost);
    final boolean configuredIsLocalhost = isLocalhostVariant(serverInfo.host());

    return currentIsLocalhost && configuredIsLocalhost;
  }

  /**
   * Checks if a host string represents localhost.
   */
  private boolean isLocalhostVariant(final String host) {
    return host.equals("localhost") ||
        host.equals("127.0.0.1") ||
        host.equals("0.0.0.0") ||
        host.equals("::1");
  }

  /**
   * Resolves a server alias to the actual server information.
   * This method is used to resolve alias placeholders (e.g., {arcade2}proxy:8667)
   * to actual server addresses in Docker/K8s environments.
   *
   * @param serverInfo The server info potentially containing an alias to resolve
   *
   * @return The resolved ServerInfo with actual host/port, or the original if alias is empty or not found
   */
  public ServerInfo resolveAlias(final ServerInfo serverInfo) {
    if (serverInfo.alias().isEmpty()) {
      return serverInfo;
    }

    return cluster.findByAlias(serverInfo.alias())
        .orElse(serverInfo);
  }

  /**
   * Updates the cluster configuration with a new cluster received from the leader.
   * This method merges the received cluster with the current cluster knowledge,
   * preserving configured addresses and our own server address.
   * In Docker/ToxiProxy scenarios, the received cluster may have internal Docker addresses,
   * but we want to preserve the stable configured proxy addresses.
   *
   * @param receivedCluster the new cluster configuration received from the leader
   */
  public void setServerAddresses(final HACluster receivedCluster) {
    if (receivedCluster == null) {
      LogManager.instance().log(this, Level.WARNING, "Received null cluster configuration, ignoring update");
      return;
    }

    LogManager.instance().log(this, Level.INFO, "Updating cluster configuration: current=%s, received=%s",
        cluster, receivedCluster);

    // Build merged cluster: prefer configured addresses from receivedCluster,
    // but always preserve our own serverAddress configuration
    final Set<ServerInfo> mergedServers = new HashSet<>();

    for (ServerInfo received : receivedCluster.getServers()) {
      if (isCurrentServer(received)) {
        // Always use our own configured address for self (don't let others overwrite it)
        mergedServers.add(serverAddress);
        LogManager.instance().log(this, Level.FINE,
            "Preserving own server address: %s (received: %s)", serverAddress, received);
      } else {
        // For other servers, use the received configured address
        // The received address from the leader should already be the configured one
        mergedServers.add(received);
      }
    }

    // Check if cluster membership has changed
    final boolean clusterChanged = cluster == null ||
        !cluster.getServers().equals(mergedServers);

    if (clusterChanged) {
      LogManager.instance().log(this, Level.INFO, "Cluster membership changed from %d to %d servers",
          cluster != null ? cluster.clusterSize() : 0, mergedServers.size());

      // Log new servers
      if (cluster != null) {
        for (ServerInfo server : mergedServers) {
          final boolean wasPresent = cluster.getServers().stream()
              .anyMatch(s -> s.alias().equals(server.alias()));
          if (!wasPresent) {
            LogManager.instance().log(this, Level.INFO, "New server joined cluster: %s", server);
          }
        }

        // Log removed servers
        for (ServerInfo server : cluster.getServers()) {
          final boolean stillPresent = mergedServers.stream()
              .anyMatch(s -> s.alias().equals(server.alias()));
          if (!stillPresent) {
            LogManager.instance().log(this, Level.INFO, "Server left cluster: %s", server);
          }
        }
      }
    } else {
      LogManager.instance().log(this, Level.FINE, "Cluster membership unchanged");
    }

    // Update cluster configuration with merged servers
    this.cluster = new HACluster(mergedServers);
    this.configuredServers = cluster.clusterSize();

    LogManager.instance().log(this, Level.INFO, "Cluster configuration updated: %d servers configured",
        configuredServers);

    // Print cluster topology (also on replicas, not just on leader)
    printClusterConfiguration();
  }

  /**
   * Forward a command to the leader server. This occurs with transactions and DDL commands. If the timeout is 0, then the request is asynchronous and the
   * response is a Resultset containing `{"operation", "forwarded to the leader"}`
   *
   * @param command HACommand to forward
   * @param timeout Timeout in milliseconds. 0 for asynchronous commands
   *
   * @return the result from the command if synchronous, otherwise a result set containing `{"operation", "forwarded to the leader"}`
   */
  public Object forwardCommandToLeader(final HACommand command, final long timeout) {
    LogManager.instance().setContext(getServerName());

    final Binary buffer = new Binary();

    final String leaderName = getLeaderName();

    final long opNumber = this.lastForwardOperationNumber.decrementAndGet();

    LogManager.instance().log(this, Level.FINE, "Forwarding request %d (%s) to Leader server '%s'", opNumber, command, leaderName);

    // REGISTER THE REQUEST TO WAIT FOR
    final ForwardedMessage forwardedMessage = new ForwardedMessage();

    if (leaderConnection.get() == null)
      throw new ReplicationException("Leader not available");

    forwardMessagesWaitingForResponse.put(opNumber, forwardedMessage);
    try {
      leaderConnection.get().sendCommandToLeader(buffer, command, opNumber);
      if (timeout > 0) {
        try {
          if (forwardedMessage.semaphore.await(timeout, TimeUnit.MILLISECONDS)) {

            if (forwardedMessage.error != null) {
              // EXCEPTION
              if (forwardedMessage.error.exceptionClass.equals(ConcurrentModificationException.class.getName()))
                throw new ConcurrentModificationException(forwardedMessage.error.exceptionMessage);
              else if (forwardedMessage.error.exceptionClass.equals(TransactionException.class.getName()))
                throw new TransactionException(forwardedMessage.error.exceptionMessage);
              else if (forwardedMessage.error.exceptionClass.equals(QuorumNotReachedException.class.getName()))
                throw new QuorumNotReachedException(forwardedMessage.error.exceptionMessage);

              LogManager.instance()
                  .log(this, Level.WARNING, "Unexpected error received from forwarding a transaction to the Leader");
              throw new ReplicationException("Unexpected error received from forwarding a transaction to the Leader");
            }

          } else {
            throw new TimeoutException("Error on forwarding transaction to the Leader server");
          }

        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new ReplicationException(
              "No response received from the Leader for request " + opNumber + " because the thread was interrupted");
        }
      } else
        forwardedMessage.result = new InternalResultSet(new ResultInternal(CollectionUtils.singletonMap("operation", "forwarded to the leader")));

    } catch (final IOException | TimeoutException e) {
      LogManager.instance().log(this, Level.SEVERE, "Leader server '%s' does not respond, starting election...", leaderName);
      startElection(false);
    } finally {
      forwardMessagesWaitingForResponse.remove(opNumber);
    }

    return forwardedMessage.result;
  }

  public void sendCommandToReplicasNoLog(final HACommand command) {
    // Check if this leader has been fenced (a newer leader exists)
    leaderFence.checkFenced();
    checkCurrentNodeIsTheLeader();

    final Binary buffer = new Binary();

    // SEND THE REQUEST TO ALL THE REPLICAS
    final List<Leader2ReplicaNetworkExecutor> replicas = new ArrayList<>(replicaConnections.values());

    // ASSURE THE TX ARE WRITTEN IN SEQUENCE INTO THE LOGFILE
    synchronized (sendingLock) {
      messageFactory.serializeCommand(command, buffer, -1);

      LogManager.instance().log(this, Level.INFO, "Sending request (%s) to %s", -1, command, replicas);

      for (final Leader2ReplicaNetworkExecutor replicaConnection : replicas) {
        // STARTING FROM THE SECOND SERVER, COPY THE BUFFER
        try {
          replicaConnection.enqueueMessage(-1, buffer.slice(0));
        } catch (final ReplicationException e) {
          // REMOVE THE REPLICA
          LogManager.instance().log(this, Level.SEVERE, "Replica '%s' does not respond, setting it as OFFLINE",
              replicaConnection.getRemoteServerName());
          setReplicaStatus(replicaConnection.getRemoteServerName(), false);
        }
      }
    }
  }

  public List<Object> sendCommandToReplicasWithQuorum(final HACommand command, final int quorum, final long timeout) {
    // Check if this leader has been fenced (a newer leader exists)
    leaderFence.checkFenced();
    checkCurrentNodeIsTheLeader();

    if (quorum > getOnlineServers()) {
      // THE ONLY SMART THING TO DO HERE IS TO THROW AN EXCEPTION. IF THE SERVER WAITS THE ELECTION
      // IS COMPLETED, IT COULD CAUSE A DEADLOCK BECAUSE LOCKS COULD BE ACQUIRED IN CASE OF TX
      throw new QuorumNotReachedException(
          "Quorum " + quorum + " not reached because only " + getOnlineServers() + " server(s) are online");
//      waitAndRetryDuringElection(quorum);
//      checkCurrentNodeIsTheLeader();
    }

    final Binary buffer = new Binary();

    long opNumber = -1;
    QuorumMessage quorumMessage = null;
    List<Object> responsePayloads = null;

    try {
      while (true) {
        int sent = 0;

        // ASSURE THE TX ARE WRITTEN IN SEQUENCE INTO THE LOGFILE
        synchronized (sendingLock) {
          if (opNumber == -1)
            opNumber = this.lastDistributedOperationNumber.incrementAndGet();

          buffer.clear();
          messageFactory.serializeCommand(command, buffer, opNumber);

          // WAL SEMANTICS: Write to replication log BEFORE sending to replicas.
          // This ensures that if the leader crashes after getting quorum acknowledgment
          // but before the log write, we don't lose the committed transaction.
          replicationLogFile.appendMessage(new ReplicationMessage(opNumber, buffer));

          if (quorum > 1) {
            // REGISTER THE REQUEST TO WAIT FOR THE QUORUM
            quorumMessage = new QuorumMessage(new CountDownLatch(quorum - 1));
            messagesWaitingForQuorum.put(opNumber, quorumMessage);
          }

          // SEND THE REQUEST TO ALL THE REPLICAS
          final List<Leader2ReplicaNetworkExecutor> replicas = new ArrayList<>(replicaConnections.values());

          LogManager.instance()
              .log(this, Level.FINE, "Sending request %d '%s' to %s (quorum=%d)", opNumber, command, replicas, quorum);

          for (final Leader2ReplicaNetworkExecutor replicaConnection : replicas) {
            try {

              if (replicaConnection.enqueueMessage(opNumber, buffer.slice(0)))
                ++sent;
              else {
                if (quorumMessage != null)
                  quorumMessage.semaphore.countDown();
              }

            } catch (final ReplicationException e) {
              LogManager.instance().log(this, Level.SEVERE, "Error on replicating message %d to replica '%s' (error=%s)", opNumber,
                  replicaConnection.getRemoteServerName(), e);

              // REMOVE THE REPLICA AND EXCLUDE IT FROM THE QUORUM
              if (quorumMessage != null)
                quorumMessage.semaphore.countDown();
            }
          }
        }

        if (sent < quorum - 1) {
          checkCurrentNodeIsTheLeader();
          LogManager.instance()
              .log(this, Level.WARNING, "Quorum " + quorum + " not reached because only " + (sent + 1) + " server(s) are online");
          throw new QuorumNotReachedException(
              "Quorum " + quorum + " not reached because only " + (sent + 1) + " server(s) are online");
        }

        if (quorumMessage != null) {
          try {
            if (!quorumMessage.semaphore.await(timeout, TimeUnit.MILLISECONDS)) {

              checkCurrentNodeIsTheLeader();

              if (quorum > 1 + getOnlineReplicas())
                if (waitAndRetryDuringElection(quorum))
                  continue;

              checkCurrentNodeIsTheLeader();

              LogManager.instance()
                  .log(this, Level.WARNING, "Timeout waiting for quorum (%d) to be reached for request %d", quorum, opNumber);
              throw new QuorumNotReachedException(
                  "Timeout waiting for quorum (" + quorum + ") to be reached for request " + opNumber);
            }

          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QuorumNotReachedException(
                "Quorum not reached for request " + opNumber + " because the thread was interrupted");
          }
        }

        // Message was already written to log before sending (WAL semantics)
        // OK - quorum reached
        break;

      }
    } finally {
      // REQUEST IS OVER, REMOVE FROM THE QUORUM MAP
      if (quorumMessage != null) {
        responsePayloads = quorumMessage.payloads;
        messagesWaitingForQuorum.remove(opNumber);
      }
    }

    return responsePayloads;
  }

  public int getMessagesInQueue() {
    int total = 0;
    for (Leader2ReplicaNetworkExecutor r : replicaConnections.values())
      total += r.getMessagesInQueue();

    return total;
  }

  /**
   * Stores the HTTP address of a replica server.
   * This is used by clients to redirect HTTP requests to available replicas.
   *
   * @param serverInfo  the ServerInfo of the replica
   * @param httpAddress the HTTP address (host:port) of the replica
   */
  public void setReplicaHTTPAddress(final ServerInfo serverInfo, final String httpAddress) {
    replicaHTTPAddresses.put(serverInfo.alias(), httpAddress);
    LogManager.instance().log(this, Level.FINE, "Stored HTTP address for replica %s: %s", serverInfo.alias(), httpAddress);
  }

  public void setReplicaHTTPAddress(final String serverName, final String httpAddress) {
    replicaHTTPAddresses.put(serverName, httpAddress);
    LogManager.instance().log(this, Level.FINE, "Stored HTTP address for replica %s: %s", serverName, httpAddress);
  }

  /**
   * Returns a comma-separated list of HTTP addresses of all replica servers.
   * This is used by clients to discover available HTTP endpoints for load balancing and failover.
   *
   * @return comma-separated list of replica HTTP addresses, or empty string if no replicas
   */
  public String getReplicaServersHTTPAddressesList() {
    final StringBuilder list = new StringBuilder();
    for (final Map.Entry<String, String> entry : replicaHTTPAddresses.entrySet()) {
      final String addr = entry.getValue();
      LogManager.instance().log(this, Level.FINE, "Replica http %s", addr);
      if (addr == null)
        // HTTP ADDRESS NOT AVAILABLE YET
        continue;

      if (list.length() > 0)
        list.append(",");
      list.append(addr);
    }
    return list.toString();
  }

  /**
   * Removes a server from the cluster by ServerInfo.
   * This is the primary method for removing servers, using the ServerInfo key directly.
   *
   * @param serverInfo the ServerInfo identifying the server to remove
   */
  /**
   * Removes a server from the cluster by name (alias).
   *
   * @param remoteServerName the server name (alias)
   */
  public void removeServer(final String remoteServerName) {
    final Leader2ReplicaNetworkExecutor c = replicaConnections.remove(remoteServerName);
    if (c != null) {
      LogManager.instance()
          .log(this, Level.SEVERE, "Replica '%s' seems not active, removing it from the cluster", remoteServerName);
      c.close();
    }

    // Also remove the ServerInfo and HTTP address mapping
    serverInfoByName.remove(remoteServerName);
    replicaHTTPAddresses.remove(remoteServerName);

    configuredServers = 1 + replicaConnections.size();
  }

  /**
   * Removes a server from the cluster by ServerInfo.
   * Overload for backward compatibility.
   *
   * @param serverInfo the ServerInfo identifying the server
   */
  public void removeServer(final ServerInfo serverInfo) {
    removeServer(serverInfo.alias());
  }

  public int getOnlineServers() {
    return 1 + getOnlineReplicas();
  }

  public int getOnlineReplicas() {
    int total = 0;
    for (final Leader2ReplicaNetworkExecutor c : replicaConnections.values()) {
      if (c.getStatus() == Leader2ReplicaNetworkExecutor.STATUS.ONLINE)
        total++;
    }
    return total;
  }

  /**
   * Logs a summary of all replica statuses for debugging purposes.
   */
  public void logReplicaStatusSummary() {
    LogManager.instance().log(this, Level.INFO,
        "=== Replica Status Summary (total: %d, online: %d) ===",
        replicaConnections.size(), getOnlineReplicas());

    for (final var entry : replicaConnections.entrySet()) {
      LogManager.instance().log(this, Level.INFO,
          "  %s: %s", entry.getKey(), entry.getValue().getStatus());
    }
  }

  /**
   * Returns a map of replica names to their current status.
   * Only available on leader servers.
   */
  public Map<String, Leader2ReplicaNetworkExecutor.STATUS> getReplicaStatuses() {
    final Map<String, Leader2ReplicaNetworkExecutor.STATUS> statuses = new HashMap<>();
    for (final var entry : replicaConnections.entrySet()) {
      statuses.put(entry.getKey(), entry.getValue().getStatus());
    }
    return statuses;
  }

  public int getConfiguredServers() {
    return configuredServers;
  }

//  public Set<ServerInfo> getServerAddressList() {

  /// /    final StringBuilder list = new StringBuilder();
  /// /    for (final ServerInfo s : serverAddressList) {
  /// /      if (list.length() > 0)
  /// /        list.append(',');
  /// /      list.append(s.host);
  /// /    }
  /// /    return list.toString();
//    return serverAddressList;
//  }
  public void printClusterConfiguration() {
    final StringBuilder buffer = new StringBuilder("NEW CLUSTER CONFIGURATION\n");
    final TableFormatter table = new TableFormatter((text, args) -> buffer.append(text.formatted(args)));

    final List<RecordTableFormatter.TableRecordRow> list = new ArrayList<>();
    final boolean amILeader = isLeader();
    final Replica2LeaderNetworkExecutor leaderConn = leaderConnection.get();

    if (amILeader) {
      // LEADER VIEW: Show self as leader, then all replicas
      ResultInternal line = new ResultInternal();
      list.add(new RecordTableFormatter.TableRecordRow(line));

      Date date = new Date(startedOn);
      String dateFormatted = startedOn > 0 ?
          DateUtils.areSameDay(date, new Date()) ?
              DateUtils.format(date, "HH:mm:ss") :
              DateUtils.format(date, "yyyy-MM-dd HH:mm:ss") :
          "";

      line.setProperty("SERVER", getServerName());
      line.setProperty("HOST:PORT", getServerAddress().toDetailedString());
      line.setProperty("ROLE", "Leader");
      line.setProperty("STATUS", "ONLINE");
      line.setProperty("JOINED ON", dateFormatted);
      line.setProperty("LEFT ON", "");
      line.setProperty("THROUGHPUT", "");
      line.setProperty("LATENCY", "");

      for (final Map.Entry<String, Leader2ReplicaNetworkExecutor> entry : replicaConnections.entrySet()) {
        final String serverName = entry.getKey();
        final Leader2ReplicaNetworkExecutor c = entry.getValue();
        final ServerInfo replicaInfo = serverInfoByName.get(serverName);

        line = new ResultInternal();
        list.add(new RecordTableFormatter.TableRecordRow(line));

        final Leader2ReplicaNetworkExecutor.STATUS status = c.getStatus();

        line.setProperty("SERVER", replicaInfo.alias());
        line.setProperty("HOST:PORT", replicaInfo.toDetailedString());
        line.setProperty("ROLE", "Replica");
        line.setProperty("STATUS", status);

        date = new Date(c.getJoinedOn());
        dateFormatted = c.getJoinedOn() > 0 ?
            DateUtils.areSameDay(date, new Date()) ?
                DateUtils.format(date, "HH:mm:ss") :
                DateUtils.format(date, "yyyy-MM-dd HH:mm:ss") :
            "";

        line.setProperty("JOINED ON", dateFormatted);

        date = new Date(c.getLeftOn());
        dateFormatted = c.getLeftOn() > 0 ?
            DateUtils.areSameDay(date, new Date()) ?
                DateUtils.format(date, "HH:mm:ss") :
                DateUtils.format(date, "yyyy-MM-dd HH:mm:ss") :
            "";

        line.setProperty("LEFT ON", dateFormatted);
        line.setProperty("THROUGHPUT", c.getThroughputStats());
        line.setProperty("LATENCY", c.getLatencyStats());
      }
    } else {
      // REPLICA VIEW: Show all servers from cluster, marking leader and self
      if (cluster != null) {
        for (final ServerInfo serverInfo : cluster.getServers()) {
          ResultInternal line = new ResultInternal();
          list.add(new RecordTableFormatter.TableRecordRow(line));

          final boolean isThisServer = isCurrentServer(serverInfo);
          final boolean isLeaderServer = leaderConn != null &&
              serverInfo.alias().equals(leaderConn.getRemoteServerName());

          line.setProperty("SERVER", serverInfo.alias());
          line.setProperty("HOST:PORT", serverInfo.toDetailedString());

          if (isLeaderServer) {
            line.setProperty("ROLE", "Leader");
          } else {
            line.setProperty("ROLE", "Replica");
          }

          line.setProperty("STATUS", "ONLINE");

          if (isThisServer) {
            Date date = new Date(startedOn);
            String dateFormatted = startedOn > 0 ?
                DateUtils.areSameDay(date, new Date()) ?
                    DateUtils.format(date, "HH:mm:ss") :
                    DateUtils.format(date, "yyyy-MM-dd HH:mm:ss") :
                "";
            line.setProperty("JOINED ON", dateFormatted);
          } else {
            line.setProperty("JOINED ON", "");
          }

          line.setProperty("LEFT ON", "");
          line.setProperty("THROUGHPUT", "");
          line.setProperty("LATENCY", "");
        }
      }
    }

    table.writeRows(list, -1);

    final String output = buffer.toString();

    int hash = 7;
    for (int i = 0; i < output.length(); i++)
      hash = hash * 31 + output.charAt(i);

    if (lastConfigurationOutputHash == hash)
      // NO CHANGES, AVOID PRINTING CFG
      return;

    lastConfigurationOutputHash = hash;

    LogManager.instance().log(this, Level.INFO, output + "\n");
  }

  public JSONObject getStats() {
    final String dateTimeFormat = GlobalConfiguration.DATE_TIME_FORMAT.getValueAsString();

    final JSONObject result = new JSONObject().setDateTimeFormat(dateTimeFormat)
        .setDateFormat(GlobalConfiguration.DATE_FORMAT.getValueAsString());

    final JSONObject current = new JSONObject().setDateTimeFormat(dateTimeFormat)
        .setDateFormat(GlobalConfiguration.DATE_FORMAT.getValueAsString());
    current.put("name", getServerName());
    current.put("address", getServerAddress());
    current.put("role", isLeader() ? "Leader" : "Replica");
    current.put("status", "ONLINE");

    Date date = new Date(startedOn);
    String dateFormatted = DateUtils.areSameDay(date, new Date()) ?
        DateUtils.format(date, "HH:mm:ss") :
        DateUtils.format(date, "yyyy-MM-dd HH:mm:ss");

    current.put("joinedOn", dateFormatted);

    result.put("current", current);

    if (isLeader()) {
      final JSONArray replicas = new JSONArray();

      for (final Leader2ReplicaNetworkExecutor c : replicaConnections.values()) {
        final Leader2ReplicaNetworkExecutor.STATUS status = c.getStatus();

        final JSONObject replica = new JSONObject().setDateFormat(dateTimeFormat);
        replicas.put(replica);

        replica.put("name", c.getRemoteServerName());
        replica.put("address", c.getRemoteServerAddress());
        replica.put("role", "Replica");
        replica.put("status", status);

        date = new Date(c.getJoinedOn());
        dateFormatted = c.getJoinedOn() > 0 ?
            DateUtils.areSameDay(date, new Date()) ?
                DateUtils.format(date, "HH:mm:ss") :
                DateUtils.format(date, "yyyy-MM-dd HH:mm:ss") :
            "";

        replica.put("joinedOn", dateFormatted);

        date = new Date(c.getLeftOn());
        dateFormatted = c.getLeftOn() > 0 ?
            DateUtils.areSameDay(date, new Date()) ?
                DateUtils.format(date, "HH:mm:ss") :
                DateUtils.format(date, "yyyy-MM-dd HH:mm:ss") :
            "";

        replica.put("leftOn", dateFormatted);
        replica.put("throughput", c.getThroughputStats());
        replica.put("latency", c.getLatencyStats());
      }

      result.put("replicas", replicas);
    }

    return result;
  }

  public ServerInfo getServerAddress() {
    return serverAddress;
  }

  @Override
  public String toString() {
    return getServerName();
  }

  public void resendMessagesToReplica(final long fromMessageNumber, final String replicaName) {
    // SEND THE REQUEST TO ALL THE REPLICAS
    final Leader2ReplicaNetworkExecutor replica = replicaConnections.get(replicaName);

    if (replica == null)
      throw new ReplicationException(
          "Server '" + getServerName() + "' cannot sync replica '" + replicaName + "' because it is offline");

    final long fromPositionInLog = replicationLogFile.findMessagePosition(fromMessageNumber);

    final AtomicInteger totalSentMessages = new AtomicInteger();

    long min = -1, max = -1;

    synchronized (sendingLock) {

      for (long pos = fromPositionInLog; pos < replicationLogFile.getSize(); ) {
        final Pair<ReplicationMessage, Long> entry = replicationLogFile.getMessage(pos);

        // STARTING FROM THE SECOND SERVER, COPY THE BUFFER
        try {
          LogManager.instance()
              .log(this, Level.FINE, "Resending message (%s) to replica '%s'...", entry.getFirst(), replica.getRemoteServerName());

          if (min == -1)
            min = entry.getFirst().messageNumber;
          max = entry.getFirst().messageNumber;

          replica.sendMessage(entry.getFirst().payload);

          totalSentMessages.incrementAndGet();

          pos = entry.getSecond();

        } catch (final Exception e) {
          // REMOVE THE REPLICA
          LogManager.instance().log(this, Level.SEVERE, "Replica '%s' does not respond, setting it as OFFLINE (error=%s)",
              replica.getRemoteServerName(), e.toString());
          setReplicaStatus(replica.getRemoteServerName(), false);
          throw new ReplicationException("Cannot resend messages to replica '" + replicaName + "'", e);
        }
      }
    }

    LogManager.instance()
        .log(this, Level.INFO, "Recovering completed. Sent %d message(s) to replica '%s' (%d-%d)", totalSentMessages.get(),
            replicaName, min, max);
  }

  public void resendMessagesToReplica(final long fromMessageNumber, final ServerInfo replicaInfo) {
    resendMessagesToReplica(fromMessageNumber, replicaInfo.alias());
  }

  public boolean connectToLeader(final ServerInfo serverEntry, final Callable<Void, Exception> errorCallback) {
    try {

      connectToLeader(serverEntry);

      // OK, CONNECTED
      return true;

    } catch (final ServerIsNotTheLeaderException e) {
      final String leaderAddress = e.getLeaderAddress();
      LogManager.instance()
          .log(this, Level.INFO, "Remote server %s is not the Leader, connecting to %s", serverEntry, leaderAddress);

      final String[] leader = HostUtil.parseHostAddress(leaderAddress, DEFAULT_PORT);

      ServerInfo server1 = new ServerInfo(leader[0], Integer.parseInt(leader[1]), leader[2]);

      // Resolve alias if present (fix for issue #2945)
      server1 = resolveAlias(server1);

      connectToLeader(server1);

      // OK, CONNECTED
      return true;

    } catch (final Exception e) {
      //[HAServer] <arcade2> Error connecting to the remote Leader server {proxy}proxy:8666 (error=com.arcadedb.network.binary.ConnectionException: Error on connecting to server '{proxy}proxy:8666' (cause=java.lang.IllegalArgumentException: Invalid host proxy:8667{arcade3}proxy:8668))
      LogManager.instance().log(this, Level.INFO, "Error connecting to the remote Leader server %s ", e, serverEntry);
      if (errorCallback != null)
        errorCallback.call(e);
    }
    return false;
  }

  /**
   * Connects to a remote server. The connection succeed only if the remote server is the leader.
   */
  private synchronized void connectToLeader(ServerInfo server) {
    LogManager.instance().log(this, Level.INFO, "Connecting to leader server %s", server);

    // If we were the leader, we must step down (fence ourselves) to prevent split-brain
    if (isLeader()) {
      leaderFence.stepDown("Connecting to new leader: " + server);

      // Ensure databases remain accessible after stepping down from leader role
      // This prevents DatabaseIsClosedException when transitioning from leader to replica
      ensureDatabasesAccessible();
    }

    final Replica2LeaderNetworkExecutor lc = leaderConnection.get();

    // Check if we're already connected/connecting to the same server (by host:port)
    // This prevents duplicate connection attempts in 2-server clusters where
    // connectToLeader() can be called from multiple places (initial startup + ELECTION_COMPLETED)
    // Note: We compare by host:port, not by equals(), because the alias might differ
    // With synchronized method, second thread will wait and see the connection already established
    if (lc != null && lc.isAlive()) {
      final ServerInfo currentLeader = lc.getLeader();
      if (currentLeader.host().equals(server.host()) && currentLeader.port() == server.port()) {
        LogManager.instance().log(this, Level.INFO,
            "Already connected/connecting to leader %s (host:port %s:%d), skipping duplicate request",
            server, server.host(), server.port());
        return;
      }
    }

    if (lc != null) {
      // CLOSE ANY LEADER CONNECTION STILL OPEN
      // kill() waits for any in-progress connection attempt to complete before proceeding
      lc.kill();
      leaderConnection.set(null);
    }

    // KILL ANY ACTIVE REPLICA CONNECTION
    for (final Leader2ReplicaNetworkExecutor r : replicaConnections.values())
      r.close();
    replicaConnections.clear();

    leaderConnection.set(new Replica2LeaderNetworkExecutor(this, server));
    LogManager.instance().log(this, Level.INFO, "DIAGNOSTIC: About to call startup() on Replica2LeaderNetworkExecutor");
    leaderConnection.get().startup();
    LogManager.instance().log(this, Level.INFO, "DIAGNOSTIC: startup() returned successfully");

    // START SEPARATE THREAD TO EXECUTE LEADER'S REQUESTS
    LogManager.instance().log(this, Level.INFO, "DIAGNOSTIC: About to call start() to begin run() thread");
    leaderConnection.get().start();
    LogManager.instance().log(this, Level.INFO, "DIAGNOSTIC: start() called, run() thread should now be running");
  }

  protected ChannelBinaryClient createNetworkConnection(ServerInfo dest, final short commandId)
      throws IOException {
    try {
      server.lifecycleEvent(ReplicationCallback.Type.NETWORK_CONNECTION, dest);
    } catch (final Exception e) {
      throw new ConnectionException(dest.toString(), e);
    }

    LogManager.instance()
        .log(this, Level.INFO, "Creating client connection to  '%s' ", dest);

    final ChannelBinaryClient channel = new ChannelBinaryClient(dest.host, dest.port, this.configuration);

    final String clusterName = this.configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);

    LogManager.instance()
        .log(this, Level.INFO, "Creating client connection - sending server name '%s' server address '%s'", getServerName(),
            getServerAddress());
    // SEND SERVER INFO
    channel.writeLong(ReplicationProtocol.MAGIC_NUMBER);
    channel.writeShort(ReplicationProtocol.PROTOCOL_VERSION);
    channel.writeString(clusterName);
    channel.writeString(getServerName());
    channel.writeString(getServerAddress().toString());
    channel.writeString(server.getHttpServer().getListeningAddress());
    // Send actual address if different from configured (for Docker/proxy scenarios)
    final String actualAddr = serverAddress.getActualAddress();
    channel.writeString(actualAddr != null ? actualAddr : "");

    channel.writeShort(commandId);
    return channel;
  }

  private boolean waitAndRetryDuringElection(final int quorum) {
    if (electionStatus == ElectionStatus.DONE)
      // BLOCK HERE THE REQUEST, THE QUORUM CANNOT BE REACHED AT PRIORI
      throw new QuorumNotReachedException(
          "Quorum " + quorum + " not reached because only " + getOnlineServers() + " server(s) are online");

    LogManager.instance()
        .log(this, Level.INFO, "Waiting during election (quorum=%d onlineReplicas=%d)", quorum, getOnlineReplicas());

    for (int retry = 0; retry < 10 && electionStatus != ElectionStatus.DONE; ++retry) {
      try {
        Thread.sleep(500);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    LogManager.instance()
        .log(this, Level.INFO, "Waiting is over (electionStatus=%s quorum=%d onlineReplicas=%d)", electionStatus, quorum,
            getOnlineReplicas());

    return electionStatus == ElectionStatus.DONE;
  }

  private void checkCurrentNodeIsTheLeader() {
    if (!isLeader())
      throw new ServerIsNotTheLeaderException("Cannot execute command", getLeader().getRemoteServerName());
  }

  private static void checkAllOrNoneAreLocalhosts(String[] serverEntries) {
    int localHostServers = 0;
    for (final String serverEntry : serverEntries) {
      if (serverEntry.startsWith("localhost") || serverEntry.startsWith("127.0.0.1"))
        ++localHostServers;
    }

    if (localHostServers > 0 && localHostServers < serverEntries.length)
      throw new ServerException(
          "Found a localhost (127.0.0.1) in the server list among non-localhost servers. Please fix the server list configuration.");
  }

  private void startElection() {
    try {
      if (electionStatus == ElectionStatus.VOTING_FOR_ME)
        // ELECTION ALREADY RUNNING
        return;

      setElectionStatus(ElectionStatus.VOTING_FOR_ME);

      final long lastReplicationMessage = replicationLogFile.getLastMessageNumber();

      long electionTurn = lastElectionVote == null ? 1 : lastElectionVote.getFirst() + 1;

      final Replica2LeaderNetworkExecutor lc = leaderConnection.get();
      if (lc != null) {
        // CLOSE ANY LEADER CONNECTION STILL OPEN
        lc.close();
        leaderConnection.set(null);
      }

      // TODO: IF A LEADER START THE ELECTION, SHOULD IT CLOSE THE EXISTENT CONNECTIONS TO THE REPLICAS?

      for (int retry = 0; !checkForExistentLeaderConnection(electionTurn) && started; ++retry) {
        final int majorityOfVotes = (configuredServers / 2) + 1;

        int totalVotes = 1;

        lastElectionVote = new Pair<>(electionTurn, getServerName());

        LogManager.instance().log(this, Level.INFO,
            "Starting election of local server asking for votes from %s (turn=%d retry=%d lastReplicationMessage=%d configuredServers=%d majorityOfVotes=%d)",
            cluster.servers, electionTurn, retry, lastReplicationMessage, configuredServers, majorityOfVotes);

        final HashMap<String, Integer> otherLeaders = new HashMap<>();

        boolean electionAborted = false;

//        final HashSet<ServerInfo> serverAddressListCopy = new HashSet<>(serverAddressList);

        for (final ServerInfo aServer : cluster.servers) {
          if (isCurrentServer(aServer))
            // SKIP LOCAL SERVER
            continue;

          try {

            final ChannelBinaryClient channel = createNetworkConnection(aServer, ReplicationProtocol.COMMAND_VOTE_FOR_ME);
            channel.writeLong(electionTurn);
            channel.writeLong(lastReplicationMessage);
            channel.flush();

            final byte vote = channel.readByte();

            if (vote == 0) {
              // RECEIVED VOTE
              ++totalVotes;
              LogManager.instance()
                  .log(this, Level.INFO, "Received the vote from server %s (turn=%d totalVotes=%d majority=%d)", aServer,
                      electionTurn, totalVotes, majorityOfVotes);

            } else {
              final String otherLeaderName = channel.readString();
              LogManager.instance().log(this, Level.INFO,
                  "Did not receive the vote from server %s (turn=%d totalVotes=%d majority=%d itsLeader=%s)", aServer,
                  electionTurn, totalVotes, majorityOfVotes, otherLeaderName);

              if (!otherLeaderName.isEmpty()) {
                final Integer counter = otherLeaders.get(otherLeaderName);
                otherLeaders.put(otherLeaderName, counter == null ? 1 : counter + 1);
              }

              if (vote == 1) {
                // NO VOTE, IT ALREADY VOTED FOR SOMEBODY ELSE
                LogManager.instance().log(this, Level.INFO,
                    "Did not receive the vote from server %s (turn=%d totalVotes=%d majority=%d itsLeader=%s)", aServer,
                    electionTurn, totalVotes, majorityOfVotes, otherLeaderName);

              } else if (vote == 2) {
                // NO VOTE, THE OTHER NODE HAS A HIGHER LSN, IT WILL START THE ELECTION
                electionAborted = true;
                LogManager.instance().log(this, Level.INFO,
                    "Aborting election because server %s has a higher LSN (turn=%d lastReplicationMessage=%d totalVotes=%d majority=%d)",
                    aServer, electionTurn, lastReplicationMessage, totalVotes, majorityOfVotes);
              }
            }

            channel.close();
          } catch (final Exception e) {
            LogManager.instance()
                .log(this, Level.INFO, "Error contacting server %s for election: %s", aServer, e.getMessage());
          }
        }

        if (checkForExistentLeaderConnection(electionTurn))
          break;

        if (!electionAborted && totalVotes >= majorityOfVotes) {
          LogManager.instance()
              .log(this, Level.INFO, "Current server elected as new $ANSI{green Leader} (turn=%d totalVotes=%d majority=%d)",
                  electionTurn, totalVotes, majorityOfVotes);
          sendNewLeadershipToOtherNodes();
          break;
        }

        if (!otherLeaders.isEmpty()) {
          // TRY TO CONNECT TO THE EXISTENT LEADER
          LogManager.instance()
              .log(this, Level.INFO, "Other leaders found %s (turn=%d totalVotes=%d majority=%d)", otherLeaders, electionTurn,
                  totalVotes, majorityOfVotes);
          for (final Map.Entry<String, Integer> entry : otherLeaders.entrySet()) {
            if (entry.getValue() >= majorityOfVotes) {
              LogManager.instance()
                  .log(this, Level.INFO, "Trying to connect to the existing leader '%s' (turn=%d totalVotes=%d majority=%d)",
                      entry.getKey(), electionTurn, entry.getValue(), majorityOfVotes);

              // Resolve the server info - try by alias first, then parse as address
              ServerInfo serverInfo = cluster.findByAlias(entry.getKey()).orElse(null);
              if (serverInfo == null) {
                // Try to parse as host:port string
                try {
                  serverInfo = ServerInfo.fromString(entry.getKey());
                } catch (final Exception e) {
                  LogManager.instance()
                      .log(this, Level.WARNING, "Could not resolve leader address '%s', skipping", entry.getKey());
                  continue;
                }
              }

              if (!isCurrentServer(serverInfo) && connectToLeader(serverInfo, null))
                break;
            }
          }
        }

        if (checkForExistentLeaderConnection(electionTurn))
          break;

        try {
          long timeout = 1000 + ThreadLocalRandom.current().nextInt(1000);
          if (electionAborted)
            timeout *= 3;

          LogManager.instance()
              .log(this, Level.INFO, "Not able to be elected as Leader, waiting %dms and retry (turn=%d totalVotes=%d majority=%d)",
                  timeout, electionTurn, totalVotes, majorityOfVotes);
          Thread.sleep(timeout);

        } catch (final InterruptedException e) {
          // INTERRUPTED
          Thread.currentThread().interrupt();
          break;
        }

        if (checkForExistentLeaderConnection(electionTurn))
          break;

        ++electionTurn;
      }
    } finally {
      synchronized (this) {
        electionThread = null;
      }
    }
  }

  public HACluster getCluster() {
    return cluster;
  }

  /**
   * Ensures that all databases are accessible by attempting to get each one.
   * This is called when a server becomes leader to prevent DatabaseIsClosedException
   * during the leader failover window. If a database is closed, getDatabase() will
   * reopen it.
   */
  private void ensureDatabasesAccessible() {
    final Set<String> databaseNames = server.getDatabaseNames();
    if (databaseNames.isEmpty()) {
      LogManager.instance().log(this, Level.FINE, "No databases to verify accessibility");
      return;
    }

    LogManager.instance().log(this, Level.INFO, "Verifying %d database(s) are accessible during role transition",
        databaseNames.size());

    for (final String dbName : databaseNames) {
      try {
        // Attempt to get the database - this will reopen it if closed
        server.getDatabase(dbName);
        LogManager.instance().log(this, Level.FINE, "Database '%s' is accessible", dbName);
      } catch (final Exception e) {
        // Log but don't fail - the database might not exist or have permissions issues
        // These will be handled when actual operations are attempted
        LogManager.instance().log(this, Level.WARNING,
            "Could not verify accessibility of database '%s' after becoming leader: %s",
            dbName, e.getMessage());
      }
    }

    LogManager.instance().log(this, Level.INFO, "Database accessibility verification complete");
  }

}
