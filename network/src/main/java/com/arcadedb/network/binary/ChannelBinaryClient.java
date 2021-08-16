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

      } catch (SocketTimeoutException e) {
        throw new IOException("Cannot connect to host " + remoteHost + ":" + remotePort + " (timeout=" + socketTimeout + ")", e);
      }
      try {
        if (socketBufferSize > 0) {
          inStream = new BufferedInputStream(socket.getInputStream(), socketBufferSize);
          outStream = new BufferedOutputStream(socket.getOutputStream(), socketBufferSize);
        } else {
          inStream = new BufferedInputStream(socket.getInputStream());
          outStream = new BufferedOutputStream(socket.getOutputStream());
        }

        in = new DataInputStream(inStream);
        out = new DataOutputStream(outStream);

      } catch (IOException e) {
        throw new NetworkProtocolException("Error on reading data from remote server " + socket.getRemoteSocketAddress() + ": ", e);
      }

    } catch (RuntimeException e) {
      if (socket.isConnected())
        socket.close();
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
