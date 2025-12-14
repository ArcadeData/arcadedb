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
  private final       Map<ServerInfo, Leader2ReplicaNetworkExecutor> replicaConnections                = new ConcurrentHashMap<>();
  private final       Map<ServerInfo, String>                        replicaHTTPAddresses              = new ConcurrentHashMap<>();
  private final       AtomicReference<Replica2LeaderNetworkExecutor> leaderConnection                  = new AtomicReference<>();
  private final       AtomicLong                                     lastDistributedOperationNumber    = new AtomicLong(-1);
  private final       AtomicLong                                     lastForwardOperationNumber        = new AtomicLong(0);
  private final       Map<Long, QuorumMessage>                       messagesWaitingForQuorum          = new ConcurrentHashMap<>(
      1024);
  private final       Map<Long, ForwardedMessage>                    forwardMessagesWaitingForResponse = new ConcurrentHashMap<>(
      1024);
  private final       Object                                         sendingLock                       = new Object();

  //  private final       Set<ServerInfo>                                serverAddressList                 = new HashSet<>();
  private         HACluster             cluster;
  private final   ServerRole            serverRole;
  protected final String                replicationPath;
  private         LeaderNetworkListener listener;
  private         long                  lastConfigurationOutputHash = 0;
  private         ServerInfo            serverAddress;
  //  private         String                replicasHTTPAddresses;
  private         boolean               started;
  private         Thread                electionThread;
  private         ReplicationLogFile    replicationLogFile;
  protected       Pair<Long, String>    lastElectionVote;

  public record ServerInfo(String host, int port, String alias) {

    public static ServerInfo fromString(String address) {
      final String[] parts = HostUtil.parseHostAddress(address, DEFAULT_PORT);
      return new ServerInfo(parts[0], Integer.parseInt(parts[1]), parts[2]);
    }

    @Override
    public String toString() {
      return "{%s}%s:%d".formatted(alias, host, port);
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
          LogManager.instance().log(this, Level.INFO, "find by alias %s - Found server %s", serverAlias, server);
          return Optional.of(server);
        }
      }

      LogManager.instance().log(this, Level.SEVERE, "NOT Found server %s on %s", serverAlias, servers);
      return Optional.empty();
    }

    /**
     * Finds a server by host and port combination.
     * This is useful when we need to match a server by its network address.
     *
     * @param host the hostname or IP address
     * @param port the port number
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

    serverAddress = new ServerInfo(server.getHostAddress(), listener.getPort(), server.getServerName());
    LogManager.instance().log(this, Level.INFO, "Starting HA service on %s", serverAddress);
    configureCluster();

    if (leaderConnection.get() == null && serverRole != ServerRole.REPLICA) {
      startElection(false);
    }
  }

  private void waitForHttpServerConnection() {
    while (!server.getHttpServer().isConnected()) {
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
      if (electionThread == null) {
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

    setElectionStatus(ElectionStatus.LEADER_WAITING_FOR_QUORUM);

    LogManager.instance()
        .log(this, Level.INFO, "Contacting all the servers for the new leadership (turn=%d)...", lastElectionVote.getFirst());

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

  public Leader2ReplicaNetworkExecutor getReplica(final String replicaName) {
    return replicaConnections.get(replicaName);
  }

  public void disconnectAllReplicas() {
    final List<Leader2ReplicaNetworkExecutor> replicas = new ArrayList<>(replicaConnections.values());
    replicaConnections.clear();

    for (Leader2ReplicaNetworkExecutor replica : replicas) {
      try {
        replica.close();
        setReplicaStatus(replica.getRemoteServerName(), false);
      } catch (Exception e) {
        // IGNORE IT
      }
    }
    configuredServers = 1;
  }

  public void setReplicaStatus(final ServerInfo remoteServerName, final boolean online) {
    final Leader2ReplicaNetworkExecutor c = replicaConnections.get(remoteServerName);
    if (c == null) {
      LogManager.instance().log(this, Level.SEVERE, "Replica '%s' was not registered", remoteServerName);
      return;
    }

    c.setStatus(online ? Leader2ReplicaNetworkExecutor.STATUS.ONLINE : Leader2ReplicaNetworkExecutor.STATUS.OFFLINE);

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

  public void registerIncomingConnection(final ServerInfo replicaServerName, final Leader2ReplicaNetworkExecutor connection) {
    final Leader2ReplicaNetworkExecutor previousConnection = replicaConnections.put(replicaServerName, connection);
    if (previousConnection != null && previousConnection != connection) {
      // MERGE CONNECTIONS
      connection.mergeFrom(previousConnection);
    }

    final int totReplicas = replicaConnections.size();
    if (1 + totReplicas > configuredServers)
      // UPDATE SERVER COUNT
      configuredServers = 1 + totReplicas;

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
   * Resolves a server alias to the actual server information.
   * This method is used to resolve alias placeholders (e.g., {arcade2}proxy:8667)
   * to actual server addresses in Docker/K8s environments.
   *
   * @param serverInfo The server info potentially containing an alias to resolve
   * @return The resolved ServerInfo with actual host/port, or the original if alias is empty or not found
   */
  public ServerInfo resolveAlias(final ServerInfo serverInfo) {
    if (serverInfo.alias().isEmpty()) {
      return serverInfo;
    }

    return cluster.findByAlias(serverInfo.alias())
        .orElse(serverInfo);
  }

  public void setServerAddresses(final HACluster receivedCluster) {

    LogManager.instance().log(this, Level.INFO, "Current cluster:: %s  - Received cluster  %s", cluster, receivedCluster);
//    if (serverAddress != null && !serverAddress.isEmpty()) {
////      serverAddressList.clear();
//
//      for (ServerInfo entry : serverAddress) {
//
//        for (ServerInfo server : cluster.servers) {
//          if (server.equals(entry)) {
//            // ALREADY IN THE LIST
//            continue;
//          } else if (server.host.equals(entry.host)) {
//            // ALREADY IN THE LIST
//            continue;
//          } else
//            serverAddressList.add(entry);
//        }
//      }
//
//      this.configuredServers = serverAddressList.size();
//    } else
//      this.configuredServers = 1;
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

        // WRITE THE MESSAGE INTO THE LOG FIRST
        replicationLogFile.appendMessage(new ReplicationMessage(opNumber, buffer));

        // OK
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
   * @param serverInfo the ServerInfo of the replica
   * @param httpAddress the HTTP address (host:port) of the replica
   */
  public void setReplicaHTTPAddress(final ServerInfo serverInfo, final String httpAddress) {
    replicaHTTPAddresses.put(serverInfo, httpAddress);
    LogManager.instance().log(this, Level.FINE, "Stored HTTP address for replica %s: %s", serverInfo.alias(), httpAddress);
  }

  /**
   * Returns a comma-separated list of HTTP addresses of all replica servers.
   * This is used by clients to discover available HTTP endpoints for load balancing and failover.
   *
   * @return comma-separated list of replica HTTP addresses, or empty string if no replicas
   */
  public String getReplicaServersHTTPAddressesList() {
    final StringBuilder list = new StringBuilder();
    for (final Map.Entry<ServerInfo, String> entry : replicaHTTPAddresses.entrySet()) {
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
  public void removeServer(final ServerInfo serverInfo) {
    final Leader2ReplicaNetworkExecutor c = replicaConnections.remove(serverInfo);
    if (c != null) {
      LogManager.instance()
          .log(this, Level.SEVERE, "Replica '%s' seems not active, removing it from the cluster", serverInfo);
      c.close();
    }

    // Also remove the HTTP address mapping
    replicaHTTPAddresses.remove(serverInfo);

    configuredServers = 1 + replicaConnections.size();
  }

  /**
   * Removes a server from the cluster by name (alias or host:port string).
   * This method provides backward compatibility for code that uses String identifiers.
   * It attempts to find the ServerInfo by:
   * 1. Looking up by alias in the cluster
   * 2. Parsing the string as host:port and matching against replicaConnections keys
   *
   * @param remoteServerName the server name (alias) or "host:port" string
   */
  public void removeServer(final String remoteServerName) {
    ServerInfo serverInfo = null;

    // First, try to find by alias in the cluster
    if (cluster != null) {
      serverInfo = cluster.findByAlias(remoteServerName).orElse(null);
    }

    // If not found by alias, try to parse as host:port and find in replicaConnections
    if (serverInfo == null) {
      // Try to match against existing ServerInfo keys in replicaConnections
      for (ServerInfo key : replicaConnections.keySet()) {
        if (key.alias().equals(remoteServerName) ||
            (key.host() + ":" + key.port()).equals(remoteServerName)) {
          serverInfo = key;
          break;
        }
      }
    }

    if (serverInfo != null) {
      removeServer(serverInfo);
    } else {
      LogManager.instance()
          .log(this, Level.WARNING, "Cannot remove server '%s' - not found in cluster", remoteServerName);
    }
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

    ResultInternal line = new ResultInternal();
    list.add(new RecordTableFormatter.TableRecordRow(line));

    Date date = new Date(startedOn);
    String dateFormatted = startedOn > 0 ?
        DateUtils.areSameDay(date, new Date()) ?
            DateUtils.format(date, "HH:mm:ss") :
            DateUtils.format(date, "yyyy-MM-dd HH:mm:ss") :
        "";

    line.setProperty("SERVER", getServerName());
    line.setProperty("HOST:PORT", getServerAddress());
    line.setProperty("ROLE", "Leader");
    line.setProperty("STATUS", "ONLINE");
    line.setProperty("JOINED ON", dateFormatted);
    line.setProperty("LEFT ON", "");
    line.setProperty("THROUGHPUT", "");
    line.setProperty("LATENCY", "");

    for (final Leader2ReplicaNetworkExecutor c : replicaConnections.values()) {
      line = new ResultInternal();
      list.add(new RecordTableFormatter.TableRecordRow(line));

      final Leader2ReplicaNetworkExecutor.STATUS status = c.getStatus();

      line.setProperty("SERVER", c.getRemoteServerName());
      line.setProperty("HOST:PORT", c.getRemoteServerAddress());
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

  public void resendMessagesToReplica(final long fromMessageNumber, final ServerInfo replicaName) {
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
  private void connectToLeader(ServerInfo server) {
    LogManager.instance().log(this, Level.INFO, "Connecting to leader server %s", server);
    final Replica2LeaderNetworkExecutor lc = leaderConnection.get();
    if (lc != null) {
      // CLOSE ANY LEADER CONNECTION STILL OPEN
      lc.kill();
      leaderConnection.set(null);
    }

    // KILL ANY ACTIVE REPLICA CONNECTION
    for (final Leader2ReplicaNetworkExecutor r : replicaConnections.values())
      r.close();
    replicaConnections.clear();

    leaderConnection.set(new Replica2LeaderNetworkExecutor(this, server));
    leaderConnection.get().startup();

    // START SEPARATE THREAD TO EXECUTE LEADER'S REQUESTS
    leaderConnection.get().start();
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
              ServerInfo serverInfo = new ServerInfo(entry.getKey(), 2424, "");
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

}
