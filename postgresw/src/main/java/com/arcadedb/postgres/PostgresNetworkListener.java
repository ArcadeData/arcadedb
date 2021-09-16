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
package com.arcadedb.postgres;

import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ha.network.ServerSocketFactory;

import java.io.IOException;
import java.net.*;
import java.util.logging.Level;

public class PostgresNetworkListener extends Thread {

  public interface ClientConnected {
    void connected();
  }

  private final ArcadeDBServer      server;
  private final ServerSocketFactory socketFactory;
  private       ServerSocket        serverSocket;
  private          InetSocketAddress   inboundAddr;
  private volatile boolean active           = true;
  private final    int     socketBufferSize = 0;
  private final    int     protocolVersion  = -1;
  private final String hostName;
  private          int                 port;
  private          ClientConnected     callback;

  public PostgresNetworkListener(final ArcadeDBServer server, final ServerSocketFactory iSocketFactory, final String iHostName, final String iHostPortRange) {
    super(server.getServerName() + " PostgresW listening at " + iHostName + ":" + iHostPortRange);

    this.server = server;
    this.hostName = iHostName;
    this.socketFactory = iSocketFactory == null ? ServerSocketFactory.getDefault() : iSocketFactory;

    listen(iHostName, iHostPortRange);

    start();
  }

  @Override
  public void run() {
    try {
      while (active) {
        try {
          final Socket socket = serverSocket.accept();

          socket.setPerformancePreferences(0, 2, 1);
          if (socketBufferSize > 0) {
            socket.setSendBufferSize(socketBufferSize);
            socket.setReceiveBufferSize(socketBufferSize);
          }
          // CREATE A NEW PROTOCOL INSTANCE
          // TODO: OPEN A DATABASE
          final PostgresNetworkExecutor connection = new PostgresNetworkExecutor(server, socket, null);
          connection.start();

          if (callback != null)
            callback.connected();

        } catch (Exception e) {
          if (active)
            server.log(this, Level.WARNING, "Error on client connection", e);
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
          server.log(this, Level.INFO,
              "Listening for replication connections on $ANSI{green " + inboundAddr.getAddress().getHostAddress() + ":" + inboundAddr.getPort()
                  + "} (protocol v." + protocolVersion + ")");

          port = tryPort;
          return;
        }
      } catch (BindException be) {
        server.log(this, Level.WARNING, "Port %s:%d busy, trying the next available...", hostName, tryPort);
      } catch (SocketException se) {
        server.log(this, Level.SEVERE, "Unable to create socket", se);
        throw new RuntimeException(se);
      } catch (IOException ioe) {
        server.log(this, Level.SEVERE, "Unable to read data from an open socket", ioe);
        throw new RuntimeException(ioe);
      }
    }

    server.log(this, Level.SEVERE, "Unable to listen for connections using the configured ports '%s' on host '%s'", null, hostPortRange, hostName);

    throw new ServerException("Unable to listen for connections using the configured ports '" + hostPortRange + "' on host '" + hostName + "'");
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
