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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.net.Socket;

/**
 * TLS configuration of the Redis wire protocol. With {@code arcadedb.redis.tls} on, the Redis listener is a dedicated TLS port
 * (matching {@code redis-server --tls}): every accepted connection negotiates TLS from the first byte, and there is no
 * plaintext mode to fall back to on that port. The other wire protocols decide per connection instead, each under its own
 * setting: BOLT ({@code arcadedb.bolt.ssl}) has no in-band upgrade, it reads the first bytes and starts a direct TLS handshake
 * when they are a TLS record header; Postgres ({@code arcadedb.postgres.ssl}) negotiates through the protocol's own
 * {@code SSLRequest}. The key/trust stores are the same {@code arcadedb.ssl.*} settings shared with the HTTP server.
 * <p>
 * The listener binds a plain server socket and layers TLS over each accepted socket, as the Postgres and BOLT listeners do:
 * the pre-authentication gate is handed the plain socket, and the TLS socket goes to the connection's own thread.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class RedisSslHelper {
  private final SSLContext sslContext;

  public RedisSslHelper(final ContextConfiguration configuration) {
    this.sslContext = SslUtils.createServerSslContext(configuration, "Redis");
  }

  /**
   * Layers TLS over an accepted plain socket. Nothing is read or written here: the handshake runs on the first read of the
   * connection, on the thread that serves it, bounded by the read timeout of the underlying socket. Closing the returned socket
   * closes {@code socket}.
   */
  public SSLSocket wrapWithTls(final Socket socket) throws IOException {
    final SSLSocket sslSocket = (SSLSocket) sslContext.getSocketFactory().createSocket(socket, null, socket.getPort(), true);
    sslSocket.setUseClientMode(false);
    return sslSocket;
  }
}
