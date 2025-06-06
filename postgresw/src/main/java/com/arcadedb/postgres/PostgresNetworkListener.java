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
package com.arcadedb.postgres;

import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ha.network.ServerSocketFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Level;

public class PostgresNetworkListener extends Thread {
  private final    ArcadeDBServer      server;
  private final    ServerSocketFactory socketFactory;
  private          ServerSocket        serverSocket;
  private volatile boolean             active          = true;
  private final    int                 protocolVersion = -1;

  public interface ClientConnected {
    void connected();
  }

  public PostgresNetworkListener(final ArcadeDBServer server,
      final ServerSocketFactory iSocketFactory,
      final String hostName,
      final String hostPortRange) {
    super(server.getServerName() + " PostgresW listening at " + hostName + ":" + hostPortRange);

    this.server = server;
    this.socketFactory = iSocketFactory;

    listen(hostName, hostPortRange);
    start();
  }

  @Override
  public void run() {
    try {
      while (active) {
        try {
          final Socket socket = serverSocket.accept();

          socket.setPerformancePreferences(0, 2, 1);

          // CREATE A NEW PROTOCOL INSTANCE
          // TODO: OPEN A DATABASE
          final PostgresNetworkExecutor connection = new PostgresNetworkExecutor(server, socket, null);
          connection.start();

        } catch (final Exception e) {
          if (active)
            LogManager.instance().log(this, Level.WARNING, "Error on client connection", e);
        }
      }
    } finally {
      try {
        if (serverSocket != null && !serverSocket.isClosed())
          serverSocket.close();
      } catch (final IOException ioe) {
      }
    }
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
   *
   * @param hostPortRange
   * @param hostName
   */
  private void listen(final String hostName, final String hostPortRange) {

    for (final int tryPort : getPorts(hostPortRange)) {
      final InetSocketAddress inboundAddr = new InetSocketAddress(hostName, tryPort);
      try {
        serverSocket = socketFactory.createServerSocket(tryPort, 0, InetAddress.getByName(hostName));

        if (serverSocket.isBound()) {
          LogManager.instance().log(this, Level.INFO,
              "Listening for incoming connections on $ANSI{green " + inboundAddr.getAddress().getHostAddress() + ":"
                  + inboundAddr.getPort() + "} (protocol v."
                  + protocolVersion + ")");

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
        .log(this, Level.SEVERE, "Unable to listen for connections using the configured ports '%s' on host '%s'", hostPortRange,
            hostName);

    throw new ServerException(
        "Unable to listen for connections using the configured ports '" + hostPortRange + "' on host '" + hostName + "'");
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
