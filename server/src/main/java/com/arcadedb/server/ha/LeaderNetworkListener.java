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

import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ChannelBinaryServer;
import com.arcadedb.network.binary.ConnectionException;
import com.arcadedb.server.ReplicationCallback;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ha.network.ServerSocketFactory;
import com.arcadedb.utility.Pair;

import java.io.EOFException;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Optional;
import java.util.logging.Level;

public class LeaderNetworkListener extends Thread {
  private final        HAServer            ha;
  private final        ServerSocketFactory socketFactory;
  private              ServerSocket        serverSocket;
  private volatile     boolean             active          = true;
  private final static int                 protocolVersion = -1;
  private final        String              hostName;
  private              int                 port;

  public LeaderNetworkListener(final HAServer ha, final ServerSocketFactory serverSocketFactory, final String hostName,
      final String hostPortRange) {
    super(ha.getServerName() + " replication listen at " + hostName + ":" + hostPortRange);

    this.ha = ha;
    this.hostName = hostName;
    this.socketFactory = serverSocketFactory;

    listen(hostName, hostPortRange);

    start();
  }

  @Override
  public void run() {
    LogManager.instance().setContext(ha.getServerName());

    try {
      while (active) {
        try {
          handleIncomingConnection();
        } catch (final Exception e) {
          handleConnectionException(e);
        }
      }
    } finally {
      closeServerSocket();
    }
  }

  private void handleIncomingConnection() throws IOException {
    final Socket socket = serverSocket.accept();
    socket.setPerformancePreferences(0, 2, 1);
    handleConnection(socket);
  }

  private void handleConnectionException(Exception e) {
    if (active) {
      final String message = e.getMessage() != null ? e.getMessage() : e.toString();
      LogManager.instance().log(this, Level.FINE, "Error on connection from another server (error=%s)", message);
    }
  }

  private void closeServerSocket() {
    try {
      if (serverSocket != null && !serverSocket.isClosed()) {
        serverSocket.close();
      }
    } catch (final IOException ioe) {
      // IGNORE EXCEPTION FROM CLOSE
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
      } catch (final IOException e) {
        // IGNORE IT
      }
  }

  @Override
  public String toString() {
    return serverSocket.getLocalSocketAddress().toString();
  }

  /**
   * Initialize a server socket for communicating with the client.
   */
  private void listen(final String hostName, final String hostPortRange) {

    for (final int tryPort : getPorts(hostPortRange)) {
      final InetSocketAddress inboundAddr = new InetSocketAddress(hostName, tryPort);
      try {
        serverSocket = socketFactory.createServerSocket(tryPort, 0, InetAddress.getByName(hostName));

        if (serverSocket.isBound()) {
          LogManager.instance().log(this, Level.INFO,
              "Listening for replication connections on $ANSI{green " + inboundAddr.getAddress().getHostAddress() + ":"
                  + inboundAddr.getPort() + "} " + (ha.getServerAddress() != null ?
                  ("current host $ANSI{green " + ha.getServerAddress() + "} ") :
                  "") + "(protocol v." + protocolVersion + ")");

          port = tryPort;

          // UPDATE THE NAME WITH THE ACTUAL PORT BOUND
          setName(ha.getServerName() + " replication listen at " + hostName + ":" + port);

          return;
        }
      } catch (final BindException be) {
        LogManager.instance().log(this, Level.WARNING, "Port %s:%d busy, trying the next available...", hostName, tryPort);
      } catch (final SocketException se) {
        LogManager.instance().log(this, Level.SEVERE, "Unable to create socket", se);
        throw new ArcadeDBException(se);
      } catch (final IOException ioe) {
        LogManager.instance().log(this, Level.SEVERE, "Unable to read data from an open socket", ioe);
        throw new ArcadeDBException(ioe);
      }
    }

    LogManager.instance()
        .log(this, Level.SEVERE, "Unable to listen for connections using the configured ports '%s' on host '%s'", null,
            hostPortRange, hostName);

    throw new ServerException(
        "Unable to listen for connections using the configured ports '" + hostPortRange + "' on host '" + hostName + "'");
  }

  private void handleConnection(final Socket socket) throws IOException {
    final ChannelBinaryServer channel = new ChannelBinaryServer(socket, ha.getServer().getConfiguration());

    long mn = 0;
    try {
      mn = channel.readLong();
    } catch (EOFException e) {
      // IGNORE IT, TREAT IT AS BAD PROTOCOL
    }

    if (mn != ReplicationProtocol.MAGIC_NUMBER) {
      // INVALID PROTOCOL, WAIT (TO AVOID SPOOFING) AND CLOSE THE SOCKET
      try {
        Thread.sleep(500);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        // IGNORE IT
      }
      socket.close();
      throw new ConnectionException(socket.getInetAddress().toString(),
          "Bad replication protocol. The connected server is not an ArcadeDB Server");
    }

    readProtocolVersion(socket, channel);
    readClusterName(socket, channel);

    final String remoteServerName = channel.readString();
    final String remoteServerAddress = channel.readString();
    final String remoteHTTPAddress = channel.readString();
    LogManager.instance().log(this, Level.INFO,
        "Connection from serverName '%s'  - serverAddress '%s' - httpAddress '%s'",
        remoteServerName, remoteServerAddress, remoteHTTPAddress);
    final short command = channel.readShort();

    HAServer.HACluster cluster = ha.getCluster();

    Optional<HAServer.ServerInfo> serverInfo = cluster.findByAlias(remoteServerName);
    serverInfo.ifPresent(server -> {

          switch (command) {
          case ReplicationProtocol.COMMAND_CONNECT:
            try {
              connect(channel, server, remoteHTTPAddress);
            } catch (IOException e) {
              handleConnectionException(e);
            }
            break;

          case ReplicationProtocol.COMMAND_VOTE_FOR_ME:
            try {
              voteForMe(channel, remoteServerName);
            } catch (IOException e) {
              handleConnectionException(e);
            }
            break;

          case ReplicationProtocol.COMMAND_ELECTION_COMPLETED:
            try {
              electionComplete(channel, server);
            } catch (IOException e) {
              handleConnectionException(e);
            }
            break;

          default:
            throw new ConnectionException(channel.socket.getInetAddress().toString(),
                "Replication command '" + command + "' not supported");
          }
        }

    );

  }

  private void electionComplete(final ChannelBinaryServer channel, final HAServer.ServerInfo serverInfo)
      throws IOException {
    final long voteTurn = channel.readLong();

    ha.lastElectionVote = new Pair<>(voteTurn, serverInfo.alias());
    channel.close();

    LogManager.instance().log(this, Level.INFO, "Received new leadership from server '%s' (turn=%d)", serverInfo.alias(), voteTurn);

    if (ha.connectToLeader(serverInfo, null)) {
      // ELECTION FINISHED, THE SERVER IS A REPLICA
      ha.setElectionStatus(HAServer.ElectionStatus.DONE);
      try {
        ha.getServer().lifecycleEvent(ReplicationCallback.Type.LEADER_ELECTED, serverInfo.alias());
      } catch (final Exception e) {
        throw new ArcadeDBException("Error on propagating election status", e);
      }
    } else
      // CANNOT CONTACT THE ELECTED LEADER, START ELECTION AGAIN
      ha.startElection(false);
  }

  private void voteForMe(final ChannelBinaryServer channel, final String remoteServerName) throws IOException {
    final long voteTurn = channel.readLong();
    final long lastReplicationMessage = channel.readLong();

    final long localServerLastMessageNumber = ha.getReplicationLogFile().getLastMessageNumber();

    if (localServerLastMessageNumber > lastReplicationMessage) {
      // LOCAL SERVER HAS A HIGHER LSN, START ELECTION PROCESS IF NOT THE LEADER
      LogManager.instance().log(this, Level.INFO,
          "Server '%s' asked for election (lastReplicationMessage=%d my=%d) on turn %d, but cannot give my vote because my LSN is higher",
          remoteServerName, lastReplicationMessage, localServerLastMessageNumber, voteTurn);
      channel.writeByte((byte) 2);
      ha.lastElectionVote = new Pair<>(voteTurn, "-");
      final Replica2LeaderNetworkExecutor leader = ha.getLeader();
      channel.writeString(leader != null ? leader.getRemoteAddress() : ha.getServerAddress().toString());

      if (leader == null || remoteServerName.equals(leader.getRemoteServerName()))
        // NO LEADER OR THE SERVER ASKING FOR ELECTION IS THE CURRENT LEADER
        ha.startElection(false);

    } else if (ha.lastElectionVote == null || ha.lastElectionVote.getFirst() < voteTurn) {
      LogManager.instance()
          .log(this, Level.INFO, "Server '%s' asked for election (lastReplicationMessage=%d my=%d) on turn %d, giving my vote",
              remoteServerName, lastReplicationMessage, localServerLastMessageNumber, voteTurn);
      channel.writeByte((byte) 0);
      ha.lastElectionVote = new Pair<>(voteTurn, remoteServerName);
      ha.setElectionStatus(HAServer.ElectionStatus.VOTING_FOR_OTHERS);
    } else {
      LogManager.instance().log(this, Level.INFO,
          "Server '%s' asked for election (lastReplicationMessage=%d my=%d) on turn %d, but cannot give my vote (votedFor='%s' on turn %d)",
          remoteServerName, lastReplicationMessage, localServerLastMessageNumber, voteTurn, ha.lastElectionVote.getSecond(),
          ha.lastElectionVote.getFirst());
      channel.writeByte((byte) 1);
      final Replica2LeaderNetworkExecutor leader = ha.getLeader();
      channel.writeString(leader != null ? leader.getRemoteAddress() : ha.getServerAddress().toString());
    }
    channel.flush();
  }

  private void connect(final ChannelBinaryServer channel, HAServer.ServerInfo remoteServer, final String remoteHTTPAddress) throws IOException {
    if (remoteServer.alias().equals(ha.getServerName())) {
      channel.writeBoolean(false);
      channel.writeByte(ReplicationProtocol.ERROR_CONNECT_SAME_SERVERNAME);
      channel.writeString("Remote server is attempting to connect with the same server name '" + ha.getServerName() + "'");
      throw new ConnectionException(channel.socket.getInetAddress().toString(),
          "Remote server is attempting to connect with the same server name '" + ha.getServerName() + "'");
    }

    // CREATE A NEW PROTOCOL INSTANCE
    final Leader2ReplicaNetworkExecutor connection = new Leader2ReplicaNetworkExecutor(ha, channel, remoteServer);

    // Store the replica's HTTP address for client redirects
    ha.setReplicaHTTPAddress(remoteServer, remoteHTTPAddress);

    ha.registerIncomingConnection(connection.getRemoteServerName(), connection);

    connection.start();
  }

  private void readClusterName(final Socket socket, final ChannelBinaryServer channel) throws IOException {
    final String remoteClusterName = channel.readString();
    if (!remoteClusterName.equals(ha.getClusterName())) {
      channel.writeBoolean(false);
      channel.writeByte(ReplicationProtocol.ERROR_CONNECT_WRONGCLUSTERNAME);
      channel.writeString("Cluster name '" + remoteClusterName + "' does not match");
      channel.flush();
      throw new ConnectionException(socket.getInetAddress().toString(), "Cluster name '" + remoteClusterName + "' does not match");
    }
  }

  private void readProtocolVersion(final Socket socket, final ChannelBinaryServer channel) throws IOException {
    final short remoteProtocolVersion = channel.readShort();
    if (remoteProtocolVersion != ReplicationProtocol.PROTOCOL_VERSION) {
      channel.writeBoolean(false);
      channel.writeByte(ReplicationProtocol.ERROR_CONNECT_UNSUPPORTEDPROTOCOL);
      channel.writeString("Network protocol version " + remoteProtocolVersion + " is different than local server "
          + ReplicationProtocol.PROTOCOL_VERSION);
      channel.flush();
      throw new ConnectionException(socket.getInetAddress().toString(),
          "Network protocol version " + remoteProtocolVersion + " is different than local server "
              + ReplicationProtocol.PROTOCOL_VERSION);
    }
  }

  private static int[] getPorts(final String iHostPortRange) {
    final int[] ports;

    if (iHostPortRange.contains(",")) {
      // MULTIPLE ENUMERATED PORTS
      final String[] portValues = iHostPortRange.split(",");
      ports = new int[portValues.length];
      for (int i = 0; i < portValues.length; ++i)
        ports[i] = Integer.parseInt(portValues[i]);

    } else if (iHostPortRange.contains("-")) {
      // MULTIPLE RANGE PORTS
      final String[] limits = iHostPortRange.split("-");
      final int lowerLimit = Integer.parseInt(limits[0]);
      final int upperLimit = Integer.parseInt(limits[1]);
      ports = new int[upperLimit - lowerLimit + 1];
      for (int i = 0; i < upperLimit - lowerLimit + 1; ++i)
        ports[i] = lowerLimit + i;

    } else
      // SINGLE PORT SPECIFIED
      ports = new int[] { Integer.parseInt(iHostPortRange) };

    return ports;
  }
}
