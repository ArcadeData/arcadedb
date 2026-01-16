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
package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.HostUtil;
import com.arcadedb.network.binary.ChannelBinaryClient;
import com.arcadedb.network.binary.ConnectionException;
import com.arcadedb.network.binary.NetworkProtocolException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.server.ReplicationCallback;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ha.message.DatabaseStructureRequest;
import com.arcadedb.server.ha.message.DatabaseStructureResponse;
import com.arcadedb.server.ha.message.FileContentRequest;
import com.arcadedb.server.ha.message.FileContentResponse;
import com.arcadedb.server.ha.message.HACommand;
import com.arcadedb.server.ha.message.ReplicaConnectFullResyncResponse;
import com.arcadedb.server.ha.message.ReplicaConnectRequest;
import com.arcadedb.server.ha.message.ReplicaReadyRequest;
import com.arcadedb.server.ha.message.TxRequest;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;

import java.io.EOFException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public class Replica2LeaderNetworkExecutor extends Thread {
  private final    HAServer            server;
  private HAServer.ServerInfo leader;
  private          String              leaderServerHTTPAddress;
  private          ChannelBinaryClient channel;
  private volatile boolean             shutdown                     = false;
  private volatile boolean             connectInProgress            = false;
  private final    Object              channelOutputLock            = new Object();
  private final    Object              channelInputLock             = new Object();
  private          long                installDatabaseLastLogNumber = -1;

  public Replica2LeaderNetworkExecutor(final HAServer ha, HAServer.ServerInfo leader) {
    this.server = ha;
    this.leader = leader;
    connect();
  }

  @Override
  public void run() {
    LogManager.instance().setContext(server.getServer().getServerName());

    // REUSE THE SAME BUFFER TO AVOID MALLOC
    final Binary buffer = new Binary(8192);
    buffer.setAllocationChunkSize(1024);

    long lastReqId = -1;

    while (!shutdown) {
      long reqId = -1;
      try {
        final byte[] requestBytes = receiveResponse();

        if (shutdown)
          break;

        final Pair<ReplicationMessage, HACommand> request = server.getMessageFactory().deserializeCommand(buffer, requestBytes);

        if (request == null) {
          LogManager.instance().log(this, Level.SEVERE, "Error on receiving message NULL, reconnecting (threadId=%d)",
              Thread.currentThread().threadId());
          reconnect(null);
          continue;
        }

        final ReplicationMessage message = request.getFirst();

        reqId = message.messageNumber;

        lastReqId = reqId;

        if (reqId > -1)
          LogManager.instance()
              .log(this, Level.FINE, "Received request %d from the Leader (threadId=%d)", reqId, Thread.currentThread().threadId());
        else
          LogManager.instance()
              .log(this, Level.FINE, "Received response %d from the Leader (threadId=%d)", reqId,
                  Thread.currentThread().threadId());

        // NUMBERS <0 ARE FORWARD FROM REPLICA TO LEADER WITHOUT A VALID SEQUENCE
        if (reqId > -1) {
          final long lastMessage = server.getReplicationLogFile().getLastMessageNumber();

          if (reqId <= lastMessage) {
            //TODO: CHECK IF THE MESSAGE IS IDENTICAL?
            LogManager.instance()
                .log(this, Level.FINE, "Message %d already applied on local server (last=%d). Skip this", reqId, lastMessage);
            continue;
          }

          if (server.getReplicationLogFile().isWrongMessageOrder(message)) {
            // SKIP
            closeChannel();
            connect();
            startup();
            continue;
          }
        }

        if (installDatabaseLastLogNumber > -1 && request.getSecond() instanceof TxRequest)
          ((TxRequest) request.getSecond()).installDatabaseLastLogNumber = installDatabaseLastLogNumber;

        // WAL SEMANTICS: Log the message BEFORE executing to ensure durability.
        // If the replica crashes after logging but before executing, the message
        // can be replayed on restart. This matches the leader's behavior.
        if (reqId > -1) {
          if (!server.getReplicationLogFile().appendMessage(message)) {
            // ERROR IN THE SEQUENCE, FORCE A RECONNECTION
            closeChannel();
            connect();
            startup();
            continue;
          }
        }

        // Now execute the command after it's safely logged
        final HACommand response = request.getSecond().execute(server, leader, reqId);

        server.getServer().lifecycleEvent(ReplicationCallback.Type.REPLICA_MSG_RECEIVED, request);

        if (response != null)
          sendCommandToLeader(buffer, response, reqId);
        reqId = -1;

      } catch (final SocketTimeoutException e) {
        // IGNORE IT
      } catch (final Exception e) {
        LogManager.instance()
            .log(this, Level.INFO, "Exception during execution of request %d (shutdown=%s name=%s error=%s)", e, reqId, shutdown,
                getName(), e.toString());
        reconnect(e);
      } finally {
        //DatabaseContext.INSTANCE.clear();
      }
    }

    LogManager.instance()
        .log(this, Level.INFO, "Replica message thread closed (shutdown=%s name=%s threadId=%d lastReqId=%d)", shutdown, getName(),
            Thread.currentThread().getId(), lastReqId);
  }

  public String getRemoteServerName() {
    return leader.alias();
  }

  public String getRemoteAddress() {
    return leader.host() + ":" + leader.port();
  }

  private void reconnect(final Exception e) {
    if (Thread.currentThread().isInterrupted())
      shutdown();

    if (!shutdown) {
      closeChannel();

      if (server.getLeader() != this) {
        // LEADER ALREADY CONNECTED (RE-ELECTED?)
        LogManager.instance()
            .log(this, Level.SEVERE, "Removing connection to the previous Leader ('%s'). New Leader is: %s", getRemoteServerName(),
                server.getLeader().getRemoteServerName());
        close();
        return;
      }

      LogManager.instance()
          .log(this, Level.FINE, "Error on communication between current replica and the Leader ('%s'), reconnecting... (error=%s)",
              getRemoteServerName(), e);

      if (!shutdown) {
        try {
          connect();
          startup();
        } catch (final Exception e1) {
          LogManager.instance()
              .log(this, Level.SEVERE, "Error on re-connecting to the Leader ('%s') (error=%s)", getRemoteServerName(), e1);

//          HashSet<HAServer.ServerInfo> serverAddressListCopy = new HashSet<>(server.);

          // RECONNECT TO THE NEXT SERVER
          for (int retry = 0; retry < 3 && !shutdown && !server.getCluster().getServers().isEmpty(); ++retry) {
            for (final HAServer.ServerInfo serverAddress : server.getCluster().getServers()) {
              try {
                if (server.isCurrentServer(serverAddress))
                  // SKIP LOCAL SERVER
                  continue;

                connect();
                startup();
                return;
              } catch (final Exception e2) {
                LogManager.instance()
                    .log(this, Level.SEVERE, "Error on re-connecting to the server '%s' (error=%s)", getRemoteAddress(), e2);
              }
            }

            try {
              Thread.sleep(2000);
            } catch (final InterruptedException interruptedException) {
              Thread.currentThread().interrupt();
              shutdown = true;
              return;
            }

//            serverAddressListCopy = new HashSet<>(server.getServerAddressList());
          }

          server.startElection(true);
        }
      }
    }
  }

  public void sendCommandToLeader(final Binary buffer, final HACommand response, final long messageNumber) throws IOException {
    if (messageNumber > -1)
      LogManager.instance()
          .log(this, Level.FINE, "Sending message (response to %d) to the Leader '%s'...", messageNumber, response);
    else
      LogManager.instance().log(this, Level.FINE, "Sending message (request %d) to the Leader '%s'...", messageNumber, response);

    server.getMessageFactory().serializeCommand(response, buffer, messageNumber);

    synchronized (channelOutputLock) {
      final ChannelBinaryClient c = channel;
      if (c == null)
        throw new ReplicationException(
            "Error on sending command back to the leader server '" + leader + "' (cause=socket closed)");

      c.writeVarLengthBytes(buffer.getContent(), buffer.size());
      c.flush();
    }
  }

  public void close() {
    shutdown();
    closeChannel();
  }

  public void kill() {
    shutdown();
    interrupt();
    close();

    // Wait for any in-progress connection attempt to finish
    // This prevents race conditions in 2-server clusters where old and new executors
    // might try to connect simultaneously, causing "Connection reset" errors
    final long maxWaitMs = 10000; // 10 seconds max wait
    final long startWait = System.currentTimeMillis();
    while (connectInProgress && (System.currentTimeMillis() - startWait) < maxWaitMs) {
      try {
        Thread.sleep(50);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    if (connectInProgress) {
      LogManager.instance().log(this, Level.WARNING,
          "Connection attempt still in progress after %dms wait during kill()",
          System.currentTimeMillis() - startWait);
    }

    // WAIT THE THREAD IS DEAD
    try {
      join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public boolean isConnectInProgress() {
    return connectInProgress;
  }

  public HAServer.ServerInfo getLeader() {
    return leader;
  }

  /**
   * Test purpose only.
   */
  public void closeChannel() {
    final ChannelBinaryClient c = channel;
    if (c != null) {
      c.close();
      channel = null;
    }
  }

  public String getRemoteHTTPAddress() {
    return leaderServerHTTPAddress;
  }

  @Override
  public String toString() {
    return leader.toString();
  }

  private byte[] receiveResponse() throws IOException {
    synchronized (channelInputLock) {
      return channel.readBytes();
    }
  }

  public void connect() {
    connectInProgress = true;
    try {
      final int maxAttempts = server.getServer().getConfiguration().getValueAsInteger(GlobalConfiguration.HA_REPLICA_CONNECT_RETRY_MAX_ATTEMPTS);
      final long baseDelayMs = server.getServer().getConfiguration().getValueAsLong(GlobalConfiguration.HA_REPLICA_CONNECT_RETRY_BASE_DELAY_MS);
      final long maxDelayMs = server.getServer().getConfiguration().getValueAsLong(GlobalConfiguration.HA_REPLICA_CONNECT_RETRY_MAX_DELAY_MS);

      ConnectionException lastException = null;
      final long startTime = System.currentTimeMillis();

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      // Check if shutdown was requested before each attempt
      // This is critical for 2-server clusters where connection replacement can happen mid-retry
      if (shutdown) {
        LogManager.instance().log(this, Level.INFO, "Connection retry aborted: shutdown requested before attempt %d/%d", attempt, maxAttempts);
        throw new ConnectionException(leader.toString(), "Connection aborted: executor shutdown");
      }

      try {
        if (attempt > 1) {
          LogManager.instance().log(this, Level.INFO, "Connection attempt %d/%d to leader %s", attempt, maxAttempts, leader);
        }

        attemptConnect();

        // Success - log total retry time if there were retries
        if (attempt > 1) {
          final long totalTime = System.currentTimeMillis() - startTime;
          LogManager.instance().log(this, Level.INFO,
              "Successfully connected to leader %s after %d attempts in %dms", leader, attempt, totalTime);
        }
        return;

      } catch (final ServerIsNotTheLeaderException e) {
        // Server we tried is not the leader - extract actual leader and retry
        final String leaderAddress = e.getLeaderAddress();
        LogManager.instance().log(this, Level.INFO,
            "Server %s is not the leader, redirecting to %s (attempt %d/%d)",
            leader, leaderAddress, attempt, maxAttempts);

        // Parse the actual leader address and update our target
        final String[] leaderParts = HostUtil.parseHostAddress(leaderAddress, HAServer.DEFAULT_PORT);
        this.leader = new HAServer.ServerInfo(leaderParts[0], Integer.parseInt(leaderParts[1]), leaderParts[2]);

        // Continue retry loop with new leader target (no delay needed for redirect)
        continue;

      } catch (final ReplicationException e) {
        // Replication exceptions should not trigger retry - they are logical errors
        throw e;

      } catch (final ConnectionException e) {
        lastException = e;

        if (attempt == maxAttempts) {
          LogManager.instance().log(this, Level.SEVERE,
              "Failed to connect to leader %s after %d attempts in %dms",
              leader, maxAttempts, System.currentTimeMillis() - startTime);
          break;
        }

        // Check if shutdown was requested
        if (shutdown) {
          LogManager.instance().log(this, Level.INFO, "Connection retry aborted due to shutdown request");
          throw e;
        }

        // Calculate delay with exponential backoff and jitter
        final long exponentialDelay = baseDelayMs * (1L << (attempt - 1));
        final long cappedDelay = Math.min(exponentialDelay, maxDelayMs);
        final long jitter = (long) (cappedDelay * 0.1 * Math.random()); // 0-10% jitter
        final long delayMs = cappedDelay + jitter;

        LogManager.instance().log(this, Level.FINE,
            "Connection attempt %d/%d failed: %s. Retrying in %dms...",
            attempt, maxAttempts, e.getMessage(), delayMs);

        try {
          Thread.sleep(delayMs);
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          LogManager.instance().log(this, Level.INFO, "Connection retry interrupted");
          throw e;
        }
        }
      }

      // All attempts failed
      throw lastException;
    } finally {
      connectInProgress = false;
    }
  }

  private void attemptConnect() {
    LogManager.instance().log(this, Level.INFO, "Connecting to leader %s", leader);

    try {
      channel = server.createNetworkConnection(leader, ReplicationProtocol.COMMAND_CONNECT);
      channel.flush();

      // READ RESPONSE
      synchronized (channelInputLock) {
        final boolean connectionAccepted = channel.readBoolean();
        if (!connectionAccepted) {
          final byte reasonCode = channel.readByte();

          final String reason = channel.readString();

          switch (reasonCode) {
          case ReplicationProtocol.ERROR_CONNECT_NOLEADER:
            final String leaderServerName = channel.readString();
            final String leaderAddress = channel.readString();
            LogManager.instance().log(this, Level.INFO,
                "Cannot accept incoming connections: remote server is not a Leader, connecting to the current Leader '%s' (%s)",
                leaderServerName, leaderAddress);
            closeChannel();
            throw new ServerIsNotTheLeaderException(
                "Remote server is not a Leader, connecting to the current Leader '" + leaderServerName + "' (" + leaderAddress
                    + ")", leaderAddress);

          case ReplicationProtocol.ERROR_CONNECT_ELECTION_PENDING:
            LogManager.instance()
                .log(this, Level.INFO, "Cannot accept incoming connections: an election for the Leader server is in progress");
            closeChannel();
            throw new ReplicationException("An election for the Leader server is pending");

          case ReplicationProtocol.ERROR_CONNECT_UNSUPPORTEDPROTOCOL:
            LogManager.instance()
                .log(this, Level.INFO, "Cannot accept incoming connections: remote server does not support protocol %d",
                    ReplicationProtocol.PROTOCOL_VERSION);
            break;

          case ReplicationProtocol.ERROR_CONNECT_WRONGCLUSTERNAME:
            LogManager.instance()
                .log(this, Level.INFO, "Cannot accept incoming connections: remote server joined a different cluster than '%s'",
                    server.getClusterName());
            break;

          case ReplicationProtocol.ERROR_CONNECT_SAME_SERVERNAME:
            LogManager.instance().log(this, Level.INFO,
                "Cannot accept incoming connections: remote server has the same name as the local server '%s'",
                server.getServerName());
            break;

          default:
            LogManager.instance().log(this, Level.INFO, "Cannot accept incoming connections: unknown reason code '%s'", reasonCode);
          }

          closeChannel();
          throw new ConnectionException(leader.toString(), reason);
        }

        String leaderServerName = channel.readString();
        final long leaderElectedAtTurn = channel.readLong();
        leaderServerHTTPAddress = channel.readString();
        final String memberList = channel.readString();

        server.lastElectionVote = new Pair<>(leaderElectedAtTurn, leaderServerName);

//        server.setServerAddresses(server.parseServerList(memberList));
      }

    } catch (final EOFException e) {
      // Connection closed during handshake - this is a transient error that should trigger retry
      LogManager.instance().log(this, Level.FINE,
          "Connection closed during handshake with leader %s (will retry)", leader);
      closeChannel();
      throw new ConnectionException(leader.toString(), "Handshake interrupted: connection closed by remote server");

    } catch (final ServerIsNotTheLeaderException | ReplicationException e) {
      // These are logical errors, not connection issues - propagate without wrapping
      // This allows connect() to catch ServerIsNotTheLeaderException and handle redirects
      // within the bounded retry loop, or let ReplicationException propagate to caller
      throw e;

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Error on connecting to the server %s (cause=%s)", leader, e.toString());

      //shutdown();
      throw new ConnectionException(leader.toString(), e);
    }
  }

  public void startup() {
    LogManager.instance().log(this, Level.INFO, "Server connected to the Leader server %s, members=[%s]", leader,
        server.getCluster().getServers());

    setName(server.getServerName() + " replica2leader<-" + getRemoteServerName());

    LogManager.instance()
        .log(this, Level.INFO, "Server started as Replica in HA mode (cluster=%s leader=%s)", server.getClusterName(), leader);

    installDatabases();
  }

  private void installDatabases() {
    final Binary buffer = new Binary(8192);
    buffer.setAllocationChunkSize(1024);

    final long lastLogNumber = server.getReplicationLogFile().getLastMessageNumber();

    LogManager.instance().log(this, Level.INFO, "Requesting install of databases up to log %d...", lastLogNumber);

    try {
      sendCommandToLeader(buffer, new ReplicaConnectRequest(lastLogNumber), -1);
      final HACommand response = receiveCommandFromLeaderDuringJoin(buffer);

      if (response instanceof ReplicaConnectFullResyncResponse fullSync) {
        LogManager.instance().log(this, Level.INFO, "Asking for a full resync...");

        server.getServer().lifecycleEvent(ReplicationCallback.Type.REPLICA_FULL_RESYNC, null);

        final Set<String> databases = fullSync.getDatabases();

        for (final String db : databases)
          requestInstallDatabase(buffer, db);

      } else {
        LogManager.instance().log(this, Level.INFO, "Receiving hot resync (from=%d)...", lastLogNumber);
        server.getServer().lifecycleEvent(ReplicationCallback.Type.REPLICA_HOT_RESYNC, null);
      }

      LogManager.instance().log(this, Level.INFO,
          "Resync complete, sending ReplicaReadyRequest to leader '%s'",
          getRemoteServerName());
      sendCommandToLeader(buffer, new ReplicaReadyRequest(), -1);

    } catch (final Exception e) {
      shutdown();
      LogManager.instance().log(this, Level.SEVERE, "Error starting HA service (error=%s)", e, e.getMessage());
      throw new ServerException("Cannot start HA service", e);
    }
  }

  public void requestInstallDatabase(final Binary buffer, final String db) throws IOException {
    sendCommandToLeader(buffer, new DatabaseStructureRequest(db), -1);
    final DatabaseStructureResponse dbStructure = (DatabaseStructureResponse) receiveCommandFromLeaderDuringJoin(buffer);

    // REQUEST A DELTA BACKUP FROM THE LAST LOG NUMBER
    server.getReplicationLogFile().setLastMessageNumber(dbStructure.getCurrentLogNumber());

    final DatabaseInternal database = server.getServer().getOrCreateDatabase(db);

    // WRITE THE SCHEMA
    try (final FileWriter schemaFile = new FileWriter(database.getDatabasePath() + File.separator + LocalSchema.SCHEMA_FILE_NAME,
        DatabaseFactory.getDefaultCharset())) {
      schemaFile.write(dbStructure.getSchemaJson());
    }

    long databaseSize = 0L;
    // WRITE ALL THE FILES
    final List<Map.Entry<Integer, String>> list = new ArrayList<>(dbStructure.getFileNames().entrySet());
    for (int i = 0; i < list.size(); i++) {
      final Map.Entry<Integer, String> f = list.get(i);
      try {
        databaseSize += installFile(buffer, db, f.getKey(), f.getValue(), 0, -1);
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on installing file '%s' (%s %d/%d files)", e, f.getKey(),
            FileUtils.getSizeAsString(databaseSize), i, list.size());
        database.getEmbedded().drop();
        throw new ReplicationException("Error on installing database '" + db + "'", e);
      }
    }

    // GET THE LATEST LOG NUMBER
    sendCommandToLeader(buffer, new DatabaseStructureRequest(db), -1);
    final DatabaseStructureResponse lastStructure = (DatabaseStructureResponse) receiveCommandFromLeaderDuringJoin(buffer);
    this.installDatabaseLastLogNumber = lastStructure.getCurrentLogNumber();

    // RELOAD THE SCHEMA
    database.getSchema().getEmbedded().close();
    DatabaseContext.INSTANCE.init(database);
    database.getSchema().getEmbedded().load(ComponentFile.MODE.READ_WRITE, true);

    LogManager.instance()
        .log(this, Level.INFO, "Database '%s' installed from the cluster (%s - %d files lastLogNumber=%d)", null, db,
            FileUtils.getSizeAsString(databaseSize), list.size(), installDatabaseLastLogNumber);
  }

  private long installFile(final Binary buffer, final String db, final int fileId, final String fileName,
      final int pageFromInclusive, final int pageToInclusive) throws IOException {

    int from = pageFromInclusive;

    LogManager.instance().log(this, Level.FINE, "Installing file '%s'...", fileName);

    int pagesWritten = 0;
    long fileSize = 0;
    while (true) {
      sendCommandToLeader(buffer, new FileContentRequest(db, fileId, from, pageToInclusive), -1);
      final FileContentResponse fileChunk = (FileContentResponse) receiveCommandFromLeaderDuringJoin(buffer);

      fileSize += fileChunk.getPagesContent().size();

      fileChunk.execute(server, null, -1);

      if (fileChunk.getPages() == 0)
        break;

      pagesWritten += fileChunk.getPages();

      if (fileChunk.isLast())
        break;

      from += fileChunk.getPages();
    }

    LogManager.instance().log(this, Level.FINE, "File '%s' installed (pagesWritten=%d size=%s)", fileName, pagesWritten,
        FileUtils.getSizeAsString(fileSize));

    return fileSize;
  }

  private HACommand receiveCommandFromLeaderDuringJoin(final Binary buffer) throws IOException {
    try {
      final byte[] response = receiveResponse();

      final Pair<ReplicationMessage, HACommand> command = server.getMessageFactory().deserializeCommand(buffer, response);
      if (command == null)
        throw new NetworkProtocolException("Error on reading response, message " + response[0] + " not valid");

      return command.getSecond();

    } catch (final EOFException e) {
      // Connection closed during database installation - log and re-throw as IOException
      LogManager.instance().log(this, Level.WARNING,
          "Connection closed during database installation from leader %s", leader);
      throw new IOException("Database installation interrupted: connection closed by leader", e);
    }
  }

  private void shutdown() {
    LogManager.instance().log(this, Level.WARNING, "Shutting down thread %s (id=%d)...", getName(), getId());
    shutdown = true;
  }
}
