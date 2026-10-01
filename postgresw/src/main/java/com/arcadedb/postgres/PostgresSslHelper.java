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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.server.http.ssl.SslUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.net.Socket;
import java.util.Locale;

/**
 * TLS configuration of the Postgres wire protocol. TLS is negotiated in-band: the client sends an SSLRequest, the
 * server answers {@code S} and both sides then run the TLS handshake on the same connection. The key/trust stores
 * are the shared {@code arcadedb.ssl.*} settings, the ones the HTTP server reads.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostgresSslHelper {

  public enum TlsMode {
    DISABLED, OPTIONAL, REQUIRED
  }

  private final TlsMode    tlsMode;
  private final SSLContext sslContext;

  private PostgresSslHelper() {
    this.tlsMode = TlsMode.DISABLED;
    this.sslContext = null;
  }

  public PostgresSslHelper(final ContextConfiguration configuration) {
    final String modeString = configuration.getValueAsString(GlobalConfiguration.POSTGRES_SSL);
    try {
      this.tlsMode = TlsMode.valueOf(modeString.trim().toUpperCase(Locale.ROOT));
    } catch (final IllegalArgumentException e) {
      throw new ConfigurationException("Invalid value '" + modeString + "' for " + GlobalConfiguration.POSTGRES_SSL.getKey()
          + ". Valid values: DISABLED, OPTIONAL, REQUIRED");
    }

    this.sslContext = tlsMode == TlsMode.DISABLED ? null : SslUtils.createServerSslContext(configuration, "Postgres");
  }

  /**
   * A helper that never negotiates TLS, for a caller that has no TLS configuration (unit tests).
   */
  public static PostgresSslHelper disabled() {
    return new PostgresSslHelper();
  }

  public TlsMode getTlsMode() {
    return tlsMode;
  }

  /**
   * Layers TLS over a plain socket on which the SSLRequest was consumed and {@code S} was sent, and completes the
   * handshake. The client sends nothing before it has read {@code S}, so no byte has to be replayed. The read timeout
   * of the underlying socket bounds the handshake, so a peer that stalls in it cannot hold the thread forever.
   */
  public SSLSocket wrapWithTls(final Socket socket) throws IOException {
    final SSLSocket sslSocket = (SSLSocket) sslContext.getSocketFactory().createSocket(socket, null, socket.getPort(), true);
    sslSocket.setUseClientMode(false);
    sslSocket.startHandshake();
    return sslSocket;
  }
}
