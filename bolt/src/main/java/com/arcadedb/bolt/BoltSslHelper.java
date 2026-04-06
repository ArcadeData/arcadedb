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
import com.arcadedb.network.binary.SocketFactory;
import com.arcadedb.server.http.ssl.KeystoreType;
import com.arcadedb.server.http.ssl.SslUtils;
import com.arcadedb.server.http.ssl.TlsProtocol;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.KeyStore;
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

    try {
      final String keystorePath = getRequiredProperty(configuration, GlobalConfiguration.NETWORK_SSL_KEYSTORE,
          "BOLT TLS is enabled but SSL key store path is not configured (" + GlobalConfiguration.NETWORK_SSL_KEYSTORE.getKey() + ")");
      final String keystorePassword = getRequiredProperty(configuration, GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD,
          "BOLT TLS is enabled but SSL key store password is not configured (" + GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD.getKey() + ")");
      final String truststorePath = getRequiredProperty(configuration, GlobalConfiguration.NETWORK_SSL_TRUSTSTORE,
          "BOLT TLS is enabled but SSL trust store path is not configured (" + GlobalConfiguration.NETWORK_SSL_TRUSTSTORE.getKey() + ")");
      final String truststorePassword = getRequiredProperty(configuration, GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD,
          "BOLT TLS is enabled but SSL trust store password is not configured (" + GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD.getKey() + ")");

      final KeyStore keyStore = SslUtils.loadKeystoreFromStream(
          SocketFactory.getAsStream(keystorePath), keystorePassword,
          SslUtils.getDefaultKeystoreTypeForKeystore(() -> KeystoreType.PKCS12));

      final KeyStore trustStore = SslUtils.loadKeystoreFromStream(
          SocketFactory.getAsStream(truststorePath), truststorePassword,
          SslUtils.getDefaultKeystoreTypeForTruststore(() -> KeystoreType.JKS));

      final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(keyStore, keystorePassword.toCharArray());
      final KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();

      final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(trustStore);
      final TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();

      this.sslContext = SSLContext.getInstance(TlsProtocol.getLatestTlsVersion().getTlsVersion());
      this.sslContext.init(keyManagers, trustManagers, null);

    } catch (final ConfigurationException e) {
      throw e;
    } catch (final Exception e) {
      throw new ConfigurationException("Failed to initialize SSL context for BOLT TLS", e);
    }
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

  private static String getRequiredProperty(final ContextConfiguration configuration,
      final GlobalConfiguration key, final String errorMessage) {
    final String value = configuration.getValueAsString(key);
    if (value == null || value.isEmpty())
      throw new ConfigurationException(errorMessage);
    return value;
  }
}
