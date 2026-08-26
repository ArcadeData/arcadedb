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
package com.arcadedb.network.binary;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;

import java.io.*;
import java.net.*;

public class ChannelBinaryClient extends ChannelBinary {
  protected final int    socketTimeout;
  protected       String url;

  public ChannelBinaryClient(final String remoteHost, final int remotePort, final ContextConfiguration config) throws IOException {
    super(SocketFactory.instance(config).createSocket(), config.getValueAsInteger(GlobalConfiguration.HA_REPLICATION_CHUNK_MAXSIZE));
    try {

      url = remoteHost + ":" + remotePort;
      socketTimeout = config.getValueAsInteger(GlobalConfiguration.NETWORK_SOCKET_TIMEOUT);

      try {
        if (remoteHost.contains(":")) {
          // IPV6
          final InetAddress[] addresses = Inet6Address.getAllByName(remoteHost);
          socket.connect(new InetSocketAddress(addresses[0], remotePort), socketTimeout);

        } else {
          // IPV4
          socket.connect(new InetSocketAddress(remoteHost, remotePort), socketTimeout);
        }
        setReadResponseTimeout();

      } catch (final SocketTimeoutException e) {
        throw new IOException("Cannot connect to host " + remoteHost + ":" + remotePort + " (timeout=" + socketTimeout + ")", e);
      }
      try {
        inStream = new BufferedInputStream(socket.getInputStream());
        outStream = new BufferedOutputStream(socket.getOutputStream());

        in = new DataInputStream(inStream);
        out = new DataOutputStream(outStream);

      } catch (final IOException e) {
        throw new NetworkProtocolException("Error on reading data from remote server " + socket.getRemoteSocketAddress() + ": ", e);
      }

    } catch (final Throwable e) {
      // Every failure path above throws a CHECKED exception (IOException / SocketException / the wrapping
      // NetworkProtocolException), so a catch limited to RuntimeException never ran at all, and the
      // isConnected() guard inside it was wrong anyway: a connect that failed leaves the socket unconnected.
      // On JDK 21 the descriptor is not actually leaked - NioSocketImpl releases it itself when connect()
      // fails - so this closes a dead code path rather than a live leak, and keeps the cleanup honest for any
      // socket implementation (SSL, a future JDK) that does hold on to it (issue #6761).
      try {
        socket.close();
      } catch (final IOException ignore) {
        // closing a socket that never connected is best-effort; the original failure is what the caller needs
      }
      throw e;
    }
  }

  /**
   * Tells if the channel is connected.
   *
   * @return true if it's connected, otherwise false.
   */
  public boolean isConnected() {
    final Socket s = socket;
    return s != null && !s.isClosed() && s.isConnected() && !s.isInputShutdown() && !s.isOutputShutdown();
  }

  protected void setReadResponseTimeout() throws SocketException {
    final Socket s = socket;
    if (s != null && s.isConnected() && !s.isClosed())
      s.setSoTimeout(socketTimeout);
  }

  public String getURL() {
    return url;
  }
}
