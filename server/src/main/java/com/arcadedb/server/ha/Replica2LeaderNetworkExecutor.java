/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.server.ha;

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ChannelBinaryClient;
import com.arcadedb.network.binary.ConnectionException;
import com.arcadedb.network.binary.NetworkProtocolException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.TestCallback;
import com.arcadedb.server.ha.message.*;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;

import java.io.FileWriter;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public class Replica2LeaderNetworkExecutor extends Thread {
  private final    HAServer            server;
  private          String              host;
  private          int                 port;
  private          String              leaderServerName  = "?";
  private          String              leaderServerHTTPAddress;
  private final    boolean             testOn;
  private          ChannelBinaryClient channel;
  private volatile boolean             shutdown          = false;
  private          Object              channelOutputLock = new Object();
  private          Object              channelInputLock  = new Object();

  public Replica2LeaderNetworkExecutor(final HAServer ha, final String host, final int port) {
    this.server = ha;
    this.testOn = ha.getServer().getConfiguration().getValueAsBoolean(GlobalConfiguration.TEST);

    this.host = host;
    this.port = port;

    connect();
  }

  @Override
  public void run() {
    LogManager.instance().setContext(server.getServer().getServerName());

    // REUSE THE SAME BUFFER TO AVOID MALLOC
    final Binary buffer = new Binary(8192);
    buffer.setAllocationChunkSize(1024);

    while (!shutdown) {
      long reqId = -1;
      try {
        final byte[] requestBytes = receiveResponse();

        if (shutdown)
          break;

        final Pair<ReplicationMessage, HACommand> request = server.getMessageFactory().deserializeCommand(buffer, requestBytes);

        if (request == null) {
          server.getServer().log(this, Level.SEVERE, "Error on receiving message NULL, reconnecting (threadId=%d)", Thread.currentThread().getId());
          reconnect(null);
          continue;
        }

        final ReplicationMessage message = request.getFirst();

        reqId = message.messageNumber;

        if (reqId > -1)
          server.getServer().log(this, Level.FINE, "Received request %d from the Leader (threadId=%d)", reqId, Thread.currentThread().getId());
        else
          server.getServer().log(this, Level.FINE, "Received response %d from the Leader (threadId=%d)", reqId, Thread.currentThread().getId());

        // NUMBERS <0 ARE FORWARD FROM REPLICA TO LEADER WITHOUT A VALID SEQUENCE
        if (reqId > -1) {
          final long lastMessage = server.getReplicationLogFile().getLastMessageNumber();

          if (reqId <= lastMessage) {
            //TODO: CHECK IF THE MESSAGE IS IDENTICAL?
            server.getServer().log(this, Level.FINE, "Message %d already applied on local server (last=%d). Skip this", reqId, lastMessage);
            continue;
          }

          if (!server.getReplicationLogFile().checkMessageOrder(message)) {
            // SKIP
            channel.close();
            connect();
            continue;
          }
        }

        // TODO: LOG THE TX BEFORE EXECUTING TO RECOVER THE DB IN CASE OF CRASH

        final HACommand response = request.getSecond().execute(server, leaderServerName, reqId);

        if (reqId > -1) {
          if (!server.getReplicationLogFile().appendMessage(message)) {
            // ERROR IN THE SEQUENCE, FORCE A RECONNECTION
            channel.close();
            connect();
            continue;
          }
        }

        if (testOn)
          server.getServer().lifecycleEvent(TestCallback.TYPE.REPLICA_MSG_RECEIVED, request);

        if (response != null)
          sendCommandToLeader(buffer, response, reqId);
        reqId = -1;

      } catch (SocketTimeoutException e) {
        // IGNORE IT
      } catch (Exception e) {
        server.getServer()
            .log(this, Level.INFO, "Exception during execution of request %d (shutdown=%s name=%s error=%s)", reqId, shutdown, getName(), e.toString());
        reconnect(e);
      }
    }

    server.getServer()
        .log(this, Level.INFO, "Replica message thread closed (shutdown=%s name=%s threadId=%d)", shutdown, getName(), Thread.currentThread().getId());
  }

  public String getRemoteServerName() {
    return leaderServerName;
  }

  public String getRemoteAddress() {
    return host + ":" + port;
  }

  private void reconnect(final Exception e) {
    if (Thread.currentThread().isInterrupted())
      shutdown();

    if (!shutdown) {
      closeChannel();

      if (server.getLeader() != this) {
        // LEADER ALREADY CONNECTED (RE-ELECTED?)
        server.getServer().log(this, Level.SEVERE, "Removing connection to the previous Leader ('%s'). New Leader is: %s", getRemoteServerName(),
            server.getLeader().getRemoteServerName());
        close();
        return;
      }

      server.getServer()
          .log(this, Level.SEVERE, "Error on communication between current replica and the Leader ('%s'), reconnecting... (error=%s)", getRemoteServerName(),
              e);

      if (!shutdown) {
        try {
          connect();
        } catch (Exception e1) {
          server.getServer().log(this, Level.SEVERE, "Error on re-connecting to the Leader ('%s') (error=%s)", getRemoteServerName(), e1);

          HashSet<String> serverAddressListCopy = new HashSet<>(Arrays.asList(server.getServerAddressList().split(",")));

          for (int retry = 0; retry < 3 && !shutdown && !serverAddressListCopy.isEmpty(); ++retry) {
            for (String serverAddress : serverAddressListCopy) {
              try {
                if (server.isCurrentServer(serverAddress))
                  // SKIP LOCAL SERVER
                  continue;

                final String[] parts = serverAddress.split(":");

                host = parts[0];
                port = Integer.parseInt(parts[1]);

                connect();
                return;
              } catch (Exception e2) {
                server.getServer().log(this, Level.SEVERE, "Error on re-connecting to the server '%s' (error=%s)", getRemoteAddress(), e2);
              }
            }

            try {
              Thread.sleep(2000);
            } catch (InterruptedException interruptedException) {
              Thread.currentThread().interrupt();
              shutdown = true;
              return;
            }

            serverAddressListCopy = new HashSet<>(Arrays.asList(server.getServerAddressList().split(",")));
          }

          server.startElection();
        }
      }
    }
  }

  public void sendCommandToLeader(final Binary buffer, final HACommand response, final long messageNumber) throws IOException {
    if (messageNumber > -1)
      server.getServer().log(this, Level.FINE, "Sending message (response to %d) to the Leader '%s'...", messageNumber, response);
    else
      server.getServer().log(this, Level.FINE, "Sending message (request %d) to the Leader '%s'...", messageNumber, response);

    server.getMessageFactory().serializeCommand(response, buffer, messageNumber);

    synchronized (channelOutputLock) {
      final ChannelBinaryClient c = channel;
      if (c == null)
        throw new ReplicationException("Error on sending command back to the leader server '" + leaderServerName + "' (cause=socket closed)");

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

    // WAIT THE THREAD IS DEAD
    try {
      join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
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
    return leaderServerName;
  }

  private byte[] receiveResponse() throws IOException {
    synchronized (channelInputLock) {
      return channel.readBytes();
    }
  }

  private void connect() {
    server.getServer().log(this, Level.FINE, "Connecting to server %s:%d...", host, port);

    try {
      channel = server.createNetworkConnection(host, port, ReplicationProtocol.COMMAND_CONNECT);
      channel.flush();

      // READ RESPONSE
      synchronized (channelInputLock) {
        final boolean connectionAccepted = channel.readBoolean();
        if (!connectionAccepted) {
          byte reasonCode = channel.readByte();

          final String reason = channel.readString();

          switch (reasonCode) {
          case ReplicationProtocol.ERROR_CONNECT_NOLEADER:
            final String leaderServerName = channel.readString();
            final String leaderAddress = channel.readString();
            server.getServer()
                .log(this, Level.INFO, "Cannot accept incoming connections: remote server is not a Leader, connecting to the current Leader '%s' (%s)",
                    leaderServerName, leaderAddress);
            channel.close();
            throw new ServerIsNotTheLeaderException(
                "Remote server is not a Leader, connecting to the current Leader '" + leaderServerName + "' (" + leaderAddress + ")", leaderAddress);

          case ReplicationProtocol.ERROR_CONNECT_ELECTION_PENDING:
            server.getServer().log(this, Level.INFO, "Cannot accept incoming connections: an election for the Leader server is in progress");
            channel.close();
            throw new ReplicationException("An election for the Leader server is pending");

          case ReplicationProtocol.ERROR_CONNECT_UNSUPPORTEDPROTOCOL:
            server.getServer()
                .log(this, Level.INFO, "Cannot accept incoming connections: remote server does not support protocol %d", ReplicationProtocol.PROTOCOL_VERSION);
            break;

          case ReplicationProtocol.ERROR_CONNECT_WRONGCLUSTERNAME:
            server.getServer()
                .log(this, Level.INFO, "Cannot accept incoming connections: remote server joined a different cluster than '%s'", server.getClusterName());
            break;

          case ReplicationProtocol.ERROR_CONNECT_SAME_SERVERNAME:
            server.getServer()
                .log(this, Level.INFO, "Cannot accept incoming connections: remote server has the same name as the local server '%s'", server.getServerName());
            break;
          }

          channel.close();
          throw new ConnectionException(host + ":" + port, reason);
        }

        leaderServerName = channel.readString();
        final long leaderElectedAtTurn = channel.readLong();
        leaderServerHTTPAddress = channel.readString();
        final String memberList = channel.readString();

        server.lastElectionVote = new Pair<>(leaderElectedAtTurn, leaderServerName);

        server.setServerAddresses(memberList);
      }

      server.getServer().log(this, Level.INFO, "Server connected to the Leader server %s:%d, members=[%s]", host, port, server.getServerAddressList());

      setName(Constants.PRODUCT + "-ha-replica2leader/" + server.getServerName() + "/" + getRemoteServerName());

      server.getServer().log(this, Level.INFO, "Server started as Replica in HA mode (cluster=%s leader=%s:%d)", server.getClusterName(), host, port);

      installDatabases();

    } catch (Exception e) {
      server.getServer().log(this, Level.FINE, "Error on connecting to the server %s:%d (cause=%s)", host, port, e.toString());

      //shutdown();
      throw new ConnectionException(host + ":" + port, e);
    }
  }

  private void installDatabases() {
    final Binary buffer = new Binary(8192);
    buffer.setAllocationChunkSize(1024);

    final ReplicationMessage lastMessage = server.getReplicationLogFile().getLastMessage();
    long lastLogNumber = lastMessage != null ? lastMessage.messageNumber - 1 : -1;

    server.getServer().log(this, Level.INFO, "Requesting install of databases...");

    try {
      sendCommandToLeader(buffer, new ReplicaConnectRequest(lastLogNumber), -1);
      final HACommand response = receiveCommandFromLeaderDuringJoining(buffer);

      if (response instanceof ReplicaConnectFullResyncResponse) {
        server.getServer().log(this, Level.INFO, "Asking for a full resync...");

        if (testOn)
          server.getServer().lifecycleEvent(TestCallback.TYPE.REPLICA_FULL_RESYNC, null);

        final ReplicaConnectFullResyncResponse fullSync = (ReplicaConnectFullResyncResponse) response;

        final Set<String> databases = fullSync.getDatabases();

        for (String db : databases) {
          sendCommandToLeader(buffer, new DatabaseStructureRequest(db), -1);
          final DatabaseStructureResponse dbStructure = (DatabaseStructureResponse) receiveCommandFromLeaderDuringJoining(buffer);

          final DatabaseInternal database = (DatabaseInternal) server.getServer().getOrCreateDatabase(db);

          installDatabase(buffer, db, dbStructure, database);
        }

        sendCommandToLeader(buffer, new ReplicaReadyRequest(), -1);

      } else {
        server.getServer().log(this, Level.INFO, "Receiving hot resync (from=%d)...", lastLogNumber);

        if (testOn)
          server.getServer().lifecycleEvent(TestCallback.TYPE.REPLICA_HOT_RESYNC, null);
      }

      sendCommandToLeader(buffer, new ReplicaReadyRequest(), -1);

    } catch (Exception e) {
      shutdown();
      server.getServer().log(this, Level.SEVERE, "Error starting HA service (error=%s)", e);
      throw new ServerException("Cannot start HA service", e);
    }
  }

  private void installDatabase(final Binary buffer, final String db, final DatabaseStructureResponse dbStructure, final DatabaseInternal database)
      throws IOException {

    // WRITE THE SCHEMA
    final FileWriter schemaFile = new FileWriter(database.getDatabasePath() + "/" + SchemaImpl.SCHEMA_FILE_NAME);
    try {
      schemaFile.write(dbStructure.getSchemaJson());
    } finally {
      schemaFile.close();
    }

    // WRITE ALL THE FILES
    for (Map.Entry<Integer, String> f : dbStructure.getFileNames().entrySet()) {
      installFile(buffer, db, database, f.getKey(), f.getValue());
    }

    // RELOAD THE SCHEMA
    ((SchemaImpl) database.getSchema()).close();
    DatabaseContext.INSTANCE.init(database);
    ((SchemaImpl) database.getSchema()).load(PaginatedFile.MODE.READ_ONLY);
  }

  private void installFile(final Binary buffer, final String db, final DatabaseInternal database, final int fileId, final String fileName) throws IOException {
    final PageManager pageManager = database.getPageManager();

    final PaginatedFile file = database.getFileManager().getOrCreateFile(fileId, database.getDatabasePath() + "/" + fileName);

    final int pageSize = file.getPageSize();

    int from = 0;

    server.getServer().log(this, Level.FINE, "Installing file '%s'...", fileName);

    int pages = 0;
    long fileSize = 0;

    while (true) {
      sendCommandToLeader(buffer, new FileContentRequest(db, fileId, from), -1);
      final FileContentResponse fileChunk = (FileContentResponse) receiveCommandFromLeaderDuringJoining(buffer);

      if (fileChunk.getPages() == 0)
        break;

      if (fileChunk.getPagesContent().size() != fileChunk.getPages() * pageSize) {
        server.getServer().log(this, Level.SEVERE, "Error on received chunk for file '%s': size=%s, expected=%s (pages=%d)", fileName,
            FileUtils.getSizeAsString(fileChunk.getPagesContent().size()), FileUtils.getSizeAsString(fileChunk.getPages() * pageSize), pages);
        throw new ReplicationException("Invalid file chunk");
      }

      for (int i = 0; i < fileChunk.getPages(); ++i) {
        final MutablePage page = new MutablePage(pageManager, new PageId(file.getFileId(), from + i), pageSize);
        System.arraycopy(fileChunk.getPagesContent().getContent(), i * pageSize, page.getTrackable().getContent(), 0, pageSize);
        page.loadMetadata();
        pageManager.overridePage(page);

        ++pages;
        fileSize += pageSize;
      }

      if (fileChunk.isLast())
        break;

      from += fileChunk.getPages();
    }

    server.getServer().log(this, Level.FINE, "File '%s' installed (pages=%d size=%s)", fileName, pages, FileUtils.getSizeAsString(fileSize));
  }

  private HACommand receiveCommandFromLeaderDuringJoining(final Binary buffer) throws IOException {
    final byte[] response = receiveResponse();

    final Pair<ReplicationMessage, HACommand> command = server.getMessageFactory().deserializeCommand(buffer, response);
    if (command == null)
      throw new NetworkProtocolException("Error on reading response, message " + response[0] + " not valid");

    return command.getSecond();
  }

  private void shutdown() {
    server.getServer().log(this, Level.FINE, "Shutting down thread %s (id=%d)...", getName(), getId());
    shutdown = true;
  }
}
