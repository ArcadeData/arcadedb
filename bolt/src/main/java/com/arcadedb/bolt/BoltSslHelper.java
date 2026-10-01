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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.server.http.ssl.SslUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Locale;

/**
 * Handles TLS configuration and socket wrapping for BOLT protocol connections.
 * Reuses the global SSL keystore/truststore settings shared with the HTTP server.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BoltSslHelper {

  public enum TlsMode {
    DISABLED, OPTIONAL, REQUIRED
  }

  private final TlsMode    tlsMode;
  private final SSLContext  sslContext;

  public BoltSslHelper(final ContextConfiguration configuration) {
    final String modeString = configuration.getValueAsString(GlobalConfiguration.BOLT_SSL);
    try {
      this.tlsMode = TlsMode.valueOf(modeString.toUpperCase(Locale.ROOT));
    } catch (final IllegalArgumentException e) {
      throw new ConfigurationException(
          "Invalid value '" + modeString + "' for " + GlobalConfiguration.BOLT_SSL.getKey()
              + ". Valid values: DISABLED, OPTIONAL, REQUIRED");
    }

    if (tlsMode == TlsMode.DISABLED) {
      this.sslContext = null;
      return;
    }

    this.sslContext = SslUtils.createServerSslContext(configuration, "BOLT");
  }

  public TlsMode getTlsMode() {
    return tlsMode;
  }

  /**
   * Wraps a plain socket with TLS, replaying any bytes already consumed from the socket.
   * Uses {@code SSLSocketFactory.createSocket(Socket, InputStream, boolean)} (Java 9+)
   * to feed the consumed bytes back into the SSL engine.
   *
   * @param socket        the raw TCP socket
   * @param consumedBytes bytes already read from the socket (e.g., TLS ClientHello header)
   * @return an SSLSocket in server mode with the TLS handshake completed
   */
  public SSLSocket wrapWithTls(final Socket socket, final byte[] consumedBytes) throws IOException {
    final SSLSocketFactory factory = sslContext.getSocketFactory();
    final SSLSocket sslSocket = (SSLSocket) factory.createSocket(
        socket,
        new ByteArrayInputStream(consumedBytes),
        true);
    sslSocket.setUseClientMode(false);
    sslSocket.startHandshake();
    return sslSocket;
  }
}
