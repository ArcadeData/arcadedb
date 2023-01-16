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
import com.arcadedb.exception.ConfigurationException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.*;
import java.net.*;
import java.security.*;

public class SocketFactory {

  private javax.net.SocketFactory socketFactory;
  private boolean                 useSSL  = false;
  private SSLContext              context = null;

  private final String keyStorePath;
  private final String keyStorePassword;
  private final String keyStoreType   = KeyStore.getDefaultType();
  private final String trustStorePath;
  private final String trustStorePassword;
  private final String trustStoreType = KeyStore.getDefaultType();

  private SocketFactory(final ContextConfiguration iConfig) {
    useSSL = iConfig.getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    keyStorePath = iConfig.getValueAsString(GlobalConfiguration.NETWORK_SSL_KEYSTORE);
    keyStorePassword = iConfig.getValueAsString(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD);
    trustStorePath = iConfig.getValueAsString(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE);
    trustStorePassword = iConfig.getValueAsString(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD);
  }

  public static SocketFactory instance(final ContextConfiguration iConfig) {
    return new SocketFactory(iConfig);
  }

  private javax.net.SocketFactory getBackingFactory() {
    if (socketFactory == null) {
      if (useSSL) {
        socketFactory = getSSLContext().getSocketFactory();
      } else {
        socketFactory = javax.net.SocketFactory.getDefault();
      }
    }
    return socketFactory;
  }

  protected SSLContext getSSLContext() {
    if (context == null) {
      context = createSSLContext();
    }
    return context;
  }

  protected SSLContext createSSLContext() {
    try {
      if (keyStorePath != null && trustStorePath != null) {
        if (keyStorePassword == null || keyStorePassword.equals("")) {
          throw new ConfigurationException("Please provide a keystore password");
        }
        if (trustStorePassword == null || trustStorePassword.equals("")) {
          throw new ConfigurationException("Please provide a truststore password");
        }

        final SSLContext context = SSLContext.getInstance("TLS");

        final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

        final KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        final char[] keyStorePass = keyStorePassword.toCharArray();
        keyStore.load(getAsStream(keyStorePath), keyStorePass);

        kmf.init(keyStore, keyStorePass);

        final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        final KeyStore trustStore = KeyStore.getInstance(trustStoreType);
        final char[] trustStorePass = trustStorePassword.toCharArray();
        trustStore.load(getAsStream(trustStorePath), trustStorePass);
        tmf.init(trustStore);

        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

        return context;
      } else {
        return SSLContext.getDefault();
      }
    } catch (final Exception e) {
      throw new ConfigurationException("Failed to create ssl context", e);
    }

  }

  protected InputStream getAsStream(String path) throws IOException {

    InputStream input;

    try {
      final URL url = new URL(path);
      input = url.openStream();
    } catch (final MalformedURLException ignore) {
      input = null;
    }

    if (input == null)
      input = getClass().getResourceAsStream(path);

    if (input == null)
      input = getClass().getClassLoader().getResourceAsStream(path);

    if (input == null) {
      try {
        // This resolves an issue on Windows with relative paths not working correctly.
        path = new File(path).getAbsolutePath();
        input = new FileInputStream(path);
      } catch (final FileNotFoundException ignore) {
        input = null;
      }
    }

    if (input == null)
      throw new IOException("Could not load resource from path: " + path);

    return input;
  }

  private Socket configureSocket(final Socket socket) {
    // Add possible timeouts?
    return socket;
  }

  public Socket createSocket() throws IOException {
    return configureSocket(getBackingFactory().createSocket());
  }

  public Socket createSocket(final String host, final int port) throws IOException {
    return configureSocket(getBackingFactory().createSocket(host, port));
  }

  public Socket createSocket(final InetAddress host, final int port) throws IOException {
    return configureSocket(getBackingFactory().createSocket(host, port));
  }

  public Socket createSocket(final String host, final int port, final InetAddress localHost, final int localPort) throws IOException {
    return configureSocket(getBackingFactory().createSocket(host, port, localHost, localPort));
  }

  public Socket createSocket(final InetAddress address, final int port, final InetAddress localAddress, final int localPort) throws IOException {
    return configureSocket(getBackingFactory().createSocket(address, port, localAddress, localPort));
  }

}
