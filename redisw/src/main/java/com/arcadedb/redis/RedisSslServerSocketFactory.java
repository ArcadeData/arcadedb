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
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.network.binary.SocketFactory;
import com.arcadedb.server.http.ssl.KeystoreType;
import com.arcadedb.server.http.ssl.SslUtils;
import com.arcadedb.server.http.ssl.TlsProtocol;
import com.arcadedb.server.network.ServerSocketFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.security.KeyStore;

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
    try {
      final String keystorePath = getRequiredProperty(configuration, GlobalConfiguration.NETWORK_SSL_KEYSTORE,
          "Redis TLS is enabled but SSL key store path is not configured (" + GlobalConfiguration.NETWORK_SSL_KEYSTORE.getKey() + ")");
      final String keystorePassword = getRequiredProperty(configuration, GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD,
          "Redis TLS is enabled but SSL key store password is not configured (" + GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD.getKey()
              + ")");
      final String truststorePath = getRequiredProperty(configuration, GlobalConfiguration.NETWORK_SSL_TRUSTSTORE,
          "Redis TLS is enabled but SSL trust store path is not configured (" + GlobalConfiguration.NETWORK_SSL_TRUSTSTORE.getKey() + ")");
      final String truststorePassword = getRequiredProperty(configuration, GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD,
          "Redis TLS is enabled but SSL trust store password is not configured (" + GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD.getKey()
              + ")");

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
      throw new ConfigurationException("Failed to initialize SSL context for Redis TLS", e);
    }
  }

  @Override
  public ServerSocket createServerSocket(final int port, final int backlog, final InetAddress ifAddress) throws IOException {
    final SSLServerSocket serverSocket = (SSLServerSocket) sslContext.getServerSocketFactory()
        .createServerSocket(port, backlog, ifAddress);
    serverSocket.setUseClientMode(false);
    return serverSocket;
  }

  private static String getRequiredProperty(final ContextConfiguration configuration, final GlobalConfiguration key,
      final String errorMessage) {
    final String value = configuration.getValueAsString(key);
    if (value == null || value.isEmpty())
      throw new ConfigurationException(errorMessage);
    return value;
  }
}
