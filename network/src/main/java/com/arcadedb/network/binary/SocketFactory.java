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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.security.KeyStore;

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
    keyStorePath = (String) iConfig.getValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE);
    keyStorePassword = (String) iConfig.getValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD);
    trustStorePath = (String) iConfig.getValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE);
    trustStorePassword = (String) iConfig.getValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD);
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
    } catch (Exception e) {
      throw new ConfigurationException("Failed to create ssl context", e);
    }

  }

  protected InputStream getAsStream(String path) throws IOException {

    InputStream input;

    try {
      URL url = new URL(path);
      input = url.openStream();
    } catch (MalformedURLException ignore) {
      input = null;
    }

    if (input == null)
      input = getClass().getResourceAsStream(path);

    if (input == null)
      input = getClass().getClassLoader().getResourceAsStream(path);

    if (input == null) {
      try {
        // This resolves an issue on Windows with relative paths not working correctly.
        path = new java.io.File(path).getAbsolutePath();
        input = new FileInputStream(path);
      } catch (FileNotFoundException ignore) {
        input = null;
      }
    }

    if (input == null)
      throw new IOException("Could not load resource from path: " + path);

    return input;
  }

  private Socket configureSocket(Socket socket) {
    // Add possible timeouts?
    return socket;
  }

  public Socket createSocket() throws IOException {
    return configureSocket(getBackingFactory().createSocket());
  }

  public Socket createSocket(String host, int port) throws IOException {
    return configureSocket(getBackingFactory().createSocket(host, port));
  }

  public Socket createSocket(InetAddress host, int port) throws IOException {
    return configureSocket(getBackingFactory().createSocket(host, port));
  }

  public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
    return configureSocket(getBackingFactory().createSocket(host, port, localHost, localPort));
  }

  public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
    return configureSocket(getBackingFactory().createSocket(address, port, localAddress, localPort));
  }

}
