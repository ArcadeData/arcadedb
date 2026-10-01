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
package com.arcadedb.redis;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.http.ssl.SslUtils;
import com.arcadedb.server.network.ServerSocketFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

/**
 * {@link ServerSocketFactory} that produces full-TLS server sockets for the Redis wire protocol, so the
 * cleartext {@code AUTH} credentials are encrypted in transit. Unlike BOLT's opportunistic STARTTLS, the
 * Redis listener is a dedicated TLS port (matching {@code redis-server --tls}): every accepted connection
 * negotiates TLS from the first byte. The key/trust stores are the same {@code arcadedb.ssl.*} settings
 * shared with the HTTP server.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RedisSslServerSocketFactory extends ServerSocketFactory {
  private final SSLContext sslContext;

  public RedisSslServerSocketFactory(final ContextConfiguration configuration) {
    this.sslContext = SslUtils.createServerSslContext(configuration, "Redis");
  }

  @Override
  public ServerSocket createServerSocket(final int port, final int backlog, final InetAddress ifAddress) throws IOException {
    final SSLServerSocket serverSocket = (SSLServerSocket) sslContext.getServerSocketFactory()
        .createServerSocket(port, backlog, ifAddress);
    serverSocket.setUseClientMode(false);
    return serverSocket;
  }
}
