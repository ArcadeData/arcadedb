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

import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ChannelBinaryServer;
import com.arcadedb.network.binary.ConnectionException;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ha.network.ServerSocketFactory;
import com.arcadedb.utility.Pair;

import java.io.IOException;
import java.net.*;
import java.util.logging.Level;

public class LeaderNetworkListener extends Thread {

  public interface ClientConnected {
    void connected();
  }

  private final    HAServer            ha;
  private          ServerSocketFactory socketFactory;
  private          ServerSocket        serverSocket;
  private          InetSocketAddress   inboundAddr;
  private volatile boolean             active           = true;
  private          int                 socketBufferSize = 0;
  private          int                 protocolVersion  = -1;
  private final    String              hostName;
  private          int                 port;
  private          ClientConnected     callback;

  public LeaderNetworkListener(final HAServer ha, final ServerSocketFactory iSocketFactory, final String iHostName, final String iHostPortRange) {
    super(ha.getServerName() + " replication listen at " + iHostName + ":" + iHostPortRange);

    this.ha = ha;
    this.hostName = iHostName;
    this.socketFactory = iSocketFactory == null ? ServerSocketFactory.getDefault() : iSocketFactory;

    listen(iHostName, iHostPortRange);

    start();
  }

  @Override
  public void run() {
    LogManager.instance().setContext(ha.getServerName());

    try {
      while (active) {
        try {
          // listen for and accept a client connection to serverSocket
          final Socket socket = serverSocket.accept();

          socket.setPerformancePreferences(0, 2, 1);
          if (socketBufferSize > 0) {
            socket.setSendBufferSize(socketBufferSize);
            socket.setReceiveBufferSize(socketBufferSize);
          }

          handleConnection(socket);

        } catch (Exception e) {
          if (active)
            ha.getServer().log(this, Level.WARNING, "Error on connection from another server (error=%s)", e);
        }
      }
    } finally {
      try {
        if (serverSocket != null && !serverSocket.isClosed())
          serverSocket.close();
      } catch (IOException ioe) {
      }
    }
  }

  public String getHost() {
    return hostName;
  }

  public int getPort() {
    return port;
  }

  public void close() {
    this.active = false;

    if (serverSocket != null)
      try {
        serverSocket.close();
      } catch (IOException e) {
        // IGNORE IT
      }
  }

  public void setCallback(final ClientConnected callback) {
    this.callback = callback;
  }

  @Override
  public String toString() {
    return serverSocket.getLocalSocketAddress().toString();
  }

  /**
   * Initialize a server socket for communicating with the client.
   *
   * @param hostPortRange
   * @param hostName
   */
  private void listen(final String hostName, final String hostPortRange) {

    for (int tryPort : getPorts(hostPortRange)) {
      inboundAddr = new InetSocketAddress(hostName, tryPort);
      try {
        serverSocket = socketFactory.createServerSocket(tryPort, 0, InetAddress.getByName(hostName));

        if (serverSocket.isBound()) {
          ha.getServer().log(this, Level.INFO,
              "Listening for replication connections on $ANSI{green " + inboundAddr.getAddress().getHostAddress() + ":" + inboundAddr.getPort() + "} " + (
                  ha.getServerAddress() != null ? ("current host $ANSI{green " + ha.getServerAddress() + "} ") : "") + "(protocol v." + protocolVersion + ")");

          port = tryPort;
          return;
        }
      } catch (BindException be) {
        ha.getServer().log(this, Level.WARNING, "Port %s:%d busy, trying the next available...", hostName, tryPort);
      } catch (SocketException se) {
        ha.getServer().log(this, Level.SEVERE, "Unable to create socket", se);
        throw new RuntimeException(se);
      } catch (IOException ioe) {
        ha.getServer().log(this, Level.SEVERE, "Unable to read data from an open socket", ioe);
        throw new RuntimeException(ioe);
      }
    }

    ha.getServer().log(this, Level.SEVERE, "Unable to listen for connections using the configured ports '%s' on host '%s'", null, hostPortRange, hostName);

    throw new ServerException("Unable to listen for connections using the configured ports '" + hostPortRange + "' on host '" + hostName + "'");
  }

  private void handleConnection(final Socket socket) throws IOException {
    final ChannelBinaryServer channel = new ChannelBinaryServer(socket, ha.getServer().getConfiguration());

    final long mn = channel.readLong();
    if (mn != ReplicationProtocol.MAGIC_NUMBER) {
      // INVALID PROTOCOL, WAIT (TO AVOID SPOOFING) AND CLOSE THE SOCKET
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // IGNORE IT
      }
      socket.close();
      throw new ConnectionException(socket.getInetAddress().toString(), "Bad protocol");
    }

    final short remoteProtocolVersion = channel.readShort();
    if (remoteProtocolVersion != ReplicationProtocol.PROTOCOL_VERSION) {
      channel.writeBoolean(false);
      channel.writeByte(ReplicationProtocol.ERROR_CONNECT_UNSUPPORTEDPROTOCOL);
      channel.writeString("Network protocol version " + remoteProtocolVersion + " is different than local server " + ReplicationProtocol.PROTOCOL_VERSION);
      channel.flush();
      throw new ConnectionException(socket.getInetAddress().toString(),
          "Network protocol version " + remoteProtocolVersion + " is different than local server " + ReplicationProtocol.PROTOCOL_VERSION);
    }

    final String remoteClusterName = channel.readString();
    if (!remoteClusterName.equals(ha.getClusterName())) {
      channel.writeBoolean(false);
      channel.writeByte(ReplicationProtocol.ERROR_CONNECT_WRONGCLUSTERNAME);
      channel.writeString("Cluster name '" + remoteClusterName + "' does not match");
      channel.flush();
      throw new ConnectionException(socket.getInetAddress().toString(), "Cluster name '" + remoteClusterName + "' does not match");
    }

    final String remoteServerName = channel.readString();
    final String remoteServerAddress = channel.readString();
    final String remoteServerHTTPAddress = channel.readString();

    final short command = channel.readShort();

    switch (command) {
    case ReplicationProtocol.COMMAND_CONNECT: {

      if (remoteServerName.equals(ha.getServerName())) {
        channel.writeBoolean(false);
        channel.writeByte(ReplicationProtocol.ERROR_CONNECT_SAME_SERVERNAME);
        channel.writeString("Remote server is attempting to connect with the same server name '" + ha.getServerName() + "'");
        throw new ConnectionException(channel.socket.getInetAddress().toString(),
            "Remote server is attempting to connect with the same server name '" + ha.getServerName() + "'");
      }

      // CREATE A NEW PROTOCOL INSTANCE
      final Leader2ReplicaNetworkExecutor connection = new Leader2ReplicaNetworkExecutor(ha, channel, remoteServerName, remoteServerAddress,
          remoteServerHTTPAddress);

      ha.registerIncomingConnection(connection.getRemoteServerName(), connection);

      connection.start();

      if (callback != null)
        callback.connected();
      break;
    }

    case ReplicationProtocol.COMMAND_VOTE_FOR_ME: {
      final long voteTurn = channel.readLong();
      final long lastReplicationMessage = channel.readLong();

      final long localServerLastMessageNumber = ha.getReplicationLogFile().getLastMessageNumber();

      if (localServerLastMessageNumber > lastReplicationMessage) {
        // LOCAL SERVER HAS A HIGHER LSN, START ELECTION PROCESS IF NOT THE LEADER
        ha.getServer().log(this, Level.INFO,
            "Server '%s' asked for election (lastReplicationMessage=%d my=%d) on turn %d, but cannot give my vote because my LSN is higher", remoteServerName,
            lastReplicationMessage, localServerLastMessageNumber, voteTurn);
        channel.writeByte((byte) 2);
        ha.lastElectionVote = new Pair<>(voteTurn, "-");
        final Replica2LeaderNetworkExecutor leader = ha.getLeader();
        channel.writeString(leader != null ? leader.getRemoteAddress() : ha.getServerAddress());

        if (leader == null)
          ha.startElection();

      } else if (lastReplicationMessage >= localServerLastMessageNumber && (ha.lastElectionVote == null || ha.lastElectionVote.getFirst() < voteTurn)) {
        ha.getServer().log(this, Level.INFO, "Server '%s' asked for election (lastReplicationMessage=%d my=%d) on turn %d, giving my vote", remoteServerName,
            lastReplicationMessage, localServerLastMessageNumber, voteTurn);
        channel.writeByte((byte) 0);
        ha.lastElectionVote = new Pair<>(voteTurn, remoteServerName);
        ha.setElectionStatus(HAServer.ELECTION_STATUS.VOTING_FOR_OTHERS);
      } else {
        ha.getServer().log(this, Level.INFO,
            "Server '%s' asked for election (lastReplicationMessage=%d my=%d) on turn %d, but cannot give my vote (votedFor='%s' on turn %d)", remoteServerName,
            lastReplicationMessage, localServerLastMessageNumber, voteTurn, ha.lastElectionVote != null ? ha.lastElectionVote.getSecond() : "-",
            ha.lastElectionVote.getFirst());
        channel.writeByte((byte) 1);
        final Replica2LeaderNetworkExecutor leader = ha.getLeader();
        channel.writeString(leader != null ? leader.getRemoteAddress() : ha.getServerAddress());
      }
      channel.flush();
      break;
    }

    case ReplicationProtocol.COMMAND_ELECTION_COMPLETED: {
      final long voteTurn = channel.readLong();

      ha.lastElectionVote = new Pair<>(voteTurn, remoteServerName);
      channel.close();

      ha.getServer().log(this, Level.INFO, "Received new leadership from server '%s' (turn=%d)", remoteServerName, voteTurn);

      if (ha.connectToLeader(remoteServerAddress))
        // ELECTION FINISHED, THE SERVER IS A REPLICA
        ha.setElectionStatus(HAServer.ELECTION_STATUS.DONE);
      else
        // CANNOT CONTACT THE ELECTED LEADER, START ELECTION AGAIN
        ha.startElection();
      break;
    }
    }
  }

  private static int[] getPorts(final String iHostPortRange) {
    int[] ports;

    if (iHostPortRange.contains(",")) {
      // MULTIPLE ENUMERATED PORTS
      String[] portValues = iHostPortRange.split(",");
      ports = new int[portValues.length];
      for (int i = 0; i < portValues.length; ++i)
        ports[i] = Integer.parseInt(portValues[i]);

    } else if (iHostPortRange.contains("-")) {
      // MULTIPLE RANGE PORTS
      String[] limits = iHostPortRange.split("-");
      int lowerLimit = Integer.parseInt(limits[0]);
      int upperLimit = Integer.parseInt(limits[1]);
      ports = new int[upperLimit - lowerLimit + 1];
      for (int i = 0; i < upperLimit - lowerLimit + 1; ++i)
        ports[i] = lowerLimit + i;

    } else
      // SINGLE PORT SPECIFIED
      ports = new int[] { Integer.parseInt(iHostPortRange) };

    return ports;
  }
}
