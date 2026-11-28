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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.network.AcceptFailureHandler;
import com.arcadedb.server.network.MultiAddressServerSocket;
import com.arcadedb.server.network.PreAuthConnectionGate;
import com.arcadedb.server.network.ServerSocketFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * TCP listener for Neo4j BOLT protocol connections.
 * Accepts incoming connections and spawns BoltNetworkExecutor threads to handle them.
 */
public class BoltNetworkListener extends Thread {
  private static final int BOLT_PROTOCOL_VERSION = 4; // BOLT v4.4

  private final    ArcadeDBServer                      server;
  private final    ServerSocketFactory                 socketFactory;
  private final    BoltSslHelper                       sslHelper;
  private volatile MultiAddressServerSocket            serverSocket;
  private volatile boolean                             active = true;
  private final    Set<BoltNetworkExecutor>            activeConnections = ConcurrentHashMap.newKeySet();
  private final    int                                 maxConnections;
  /** Bounds how many accepted connections can sit un-authenticated at once (issue #6412). */
  private final    PreAuthConnectionGate               preAuthGate;
  private final    AcceptFailureHandler                failureHandler = new AcceptFailureHandler("BOLT");

  public BoltNetworkListener(final ArcadeDBServer server,
      final ServerSocketFactory socketFactory,
      final BoltSslHelper sslHelper,
      final String hostName,
      final String hostPortRange) {
    super(server.getServerName() + " BOLT listening at " + hostName + ":" + hostPortRange);

    this.server = server;
    this.socketFactory = socketFactory;
    this.sslHelper = sslHelper;
    // Both read from THIS server's configuration: the settings are SCOPE.SERVER, and the GlobalConfiguration
    // enum only ever carries a -D or an environment variable, so a cap declared in the server configuration
    // file or through SET SERVER SETTING was silently ignored (issue #7233).
    this.maxConnections = server.getConfiguration().getValueAsInteger(GlobalConfiguration.BOLT_MAX_CONNECTIONS);
    this.preAuthGate = new PreAuthConnectionGate("BOLT", server.getConfiguration());

    listen(hostName, hostPortRange);
    start();
  }

  @Override
  public void run() {
    try {
      while (active) {
        try {
          final Socket socket;
          try {
            socket = serverSocket.accept();
          } catch (final Throwable e) {
            // The socket itself failed, which it keeps doing at once on every call for as long as the cause lasts: retried at
            // a bounded rate rather than in a hot loop
            if (active)
              failureHandler.handleAcceptFailure(this, "Error accepting BOLT connections", e);
            continue;
          }

          // Check connection limit
          if (maxConnections > 0 && activeConnections.size() >= maxConnections) {
            LogManager.instance().log(this, Level.WARNING,
                "BOLT connection limit reached (%d connections), rejecting new connection from %s",
                maxConnections, socket.getRemoteSocketAddress());
            try {
              socket.close();
            } catch (final IOException e) {
              // Ignore
            }
            continue;
          }

          // One thread and one file descriptor are committed here, before the client has proved anything: past
          // the cap the socket is closed straight away rather than being given either (issue #6412). This is
          // narrower than the BOLT_MAX_CONNECTIONS cap above, which counts every connection including the
          // authenticated ones a healthy client pool holds open.
          final PreAuthConnectionGate.Ticket ticket = preAuthGate.accept(socket);
          if (ticket == null) {
            preAuthGate.logRefusal(this, socket.getRemoteSocketAddress());
            try {
              socket.close();
            } catch (final IOException e) {
              // Ignore
            }
            continue;
          }

          // Hand the raw socket to a dedicated per-connection thread. Transport (TLS) detection and the
          // blocking TLS handshake are performed on that thread, never here on the accept path, so a slow,
          // aborted or untrusted handshake can only affect its own connection and never wedges the shared
          // listener for other clients (issue #5106).
          BoltNetworkExecutor connection = null;
          try {
            socket.setPerformancePreferences(0, 2, 1);
            socket.setTcpNoDelay(true);

            connection = new BoltNetworkExecutor(server, socket, this, sslHelper, ticket);
            activeConnections.add(connection);
            startConnection(connection);
          } catch (final Throwable e) {
            // The executor never started, so nothing will hand the permit back for it, remove it from the open
            // connections or close its socket.
            if (connection != null)
              activeConnections.remove(connection);
            ticket.abandon();
            throw e;
          }

        } catch (final Throwable e) {
          // An error is no more a reason to stop accepting than an exception is: nothing starts this thread again
          if (active)
            failureHandler.handle(this, "Error accepting BOLT connection", e);
        }
      }
    } finally {
      if (serverSocket != null)
        serverSocket.close();
    }
  }

  /**
   * Starts the thread that serves an accepted connection.
   */
  void startConnection(final BoltNetworkExecutor connection) {
    connection.start();
  }

  /**
   * Called by BoltNetworkExecutor when a connection is closed.
   */
  void removeConnection(final BoltNetworkExecutor connection) {
    activeConnections.remove(connection);
  }

  /** Whether this listener is meant to accept connections: started and not closed (issue #12552). */
  boolean isListening() {
    return active;
  }

  public void close() {
    this.active = false;

    if (serverSocket != null)
      serverSocket.close();
  }

  /**
   * The local port the server socket is bound to, or -1 when it is not bound (never bound, or closed). Not
   * necessarily the configured one: {@code 0} asks the operating system for a free port, which is how a test server
   * avoids colliding with anything already listening (issue #8209).
   */
  public int getPort() {
    final MultiAddressServerSocket socket = serverSocket;
    return socket != null ? socket.getLocalPort() : -1;
  }

  @Override
  public String toString() {
    return serverSocket != null ? serverSocket.toString() : "BOLT (not bound)";
  }

  /**
   * Initialize a server socket for listening to BOLT connections.
   */
  private void listen(final String hostName, final String hostPortRange) {
    for (final int tryPort : getPorts(hostPortRange)) {
      try {
        serverSocket = MultiAddressServerSocket.bind(socketFactory, hostName, tryPort);

        if (serverSocket.getLocalPort() > 0) {
          LogManager.instance().log(this, Level.INFO,
              "Listening for incoming BOLT connections on $ANSI{green " + serverSocket + "} (protocol v." + BOLT_PROTOCOL_VERSION + ", TLS: " + sslHelper.getTlsMode() + ")");

          return;
        }
      } catch (final BindException be) {
        LogManager.instance().log(this, Level.WARNING, "Port %s:%d busy, trying the next available...", hostName, tryPort);
      } catch (final SocketException se) {
        LogManager.instance().log(this, Level.SEVERE, "Unable to create BOLT socket", se);
        throw new ArcadeDBException(se);
      } catch (final IOException ioe) {
        LogManager.instance().log(this, Level.SEVERE, "Unable to read data from an open socket", ioe);
        throw new ArcadeDBException(ioe);
      }
    }

    LogManager.instance()
        .log(this, Level.SEVERE, "Unable to listen for BOLT connections using the configured ports '%s' on host '%s'",
            hostPortRange, hostName);

    throw new ServerException(
        "Unable to listen for BOLT connections using the configured ports '" + hostPortRange + "' on host '" + hostName + "'");
  }

  private static int[] getPorts(final String hostPortRange) {
    final int[] ports;

    if (hostPortRange.contains(",")) {
      // Multiple enumerated ports
      final String[] portValues = hostPortRange.split(",");
      ports = new int[portValues.length];
      for (int i = 0; i < portValues.length; ++i)
        ports[i] = Integer.parseInt(portValues[i].trim());

    } else if (hostPortRange.contains("-")) {
      // Multiple range ports
      final String[] limits = hostPortRange.split("-");
      final int lowerLimit = Integer.parseInt(limits[0].trim());
      final int upperLimit = Integer.parseInt(limits[1].trim());
      ports = new int[upperLimit - lowerLimit + 1];
      for (int i = 0; i < ports.length; ++i)
        ports[i] = lowerLimit + i;

    } else {
      // Single port specified
      ports = new int[] { Integer.parseInt(hostPortRange.trim()) };
    }

    return ports;
  }
}
