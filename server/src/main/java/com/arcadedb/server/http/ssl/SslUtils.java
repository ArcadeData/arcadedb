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
package com.arcadedb.server.http.ssl;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.network.binary.SocketFactory;
import com.arcadedb.server.security.ServerSecurityException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.function.Supplier;

public class SslUtils {

  private static final String JAVAX_NET_SSL_KEYSTORE_TYPE = "javax.net.ssl.keyStoreType";
  private static final String JAVAX_NET_SSL_TRUSTSTORE_TYPE = "javax.net.ssl.trustStoreType";

  private SslUtils() {
  }

  public static KeyStore loadKeystoreFromStream(InputStream keystoreInputStream,
                                                String keystorePassword,
                                                String keystoreType)
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {

    try (InputStream validatedInputStream = doValidateKeystoreStream(keystoreInputStream)) {
      KeyStore keystore = KeyStore.getInstance(keystoreType);
      keystore.load(validatedInputStream,
          keystorePassword.toCharArray());
      return keystore;
    }

  }

  /**
   * Builds the server-side {@link SSLContext} of a wire protocol from the shared {@code arcadedb.ssl.*} key store and
   * trust store settings, the same ones the HTTP server reads.
   *
   * @param protocolName the protocol the context is for, used in the error messages (e.g. "Postgres")
   *
   * @throws ConfigurationException when a store path or password is not configured, or a store cannot be loaded
   */
  public static SSLContext createServerSslContext(final ContextConfiguration configuration, final String protocolName) {
    try {
      final String keystorePath = getRequiredSetting(configuration, GlobalConfiguration.NETWORK_SSL_KEYSTORE, protocolName,
          "key store path");
      final String keystorePassword = getRequiredSetting(configuration, GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD,
          protocolName, "key store password");
      final String truststorePath = getRequiredSetting(configuration, GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, protocolName,
          "trust store path");
      final String truststorePassword = getRequiredSetting(configuration, GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD,
          protocolName, "trust store password");

      final KeyStore keyStore = loadKeystoreFromStream(SocketFactory.getAsStream(keystorePath), keystorePassword,
          getDefaultKeystoreTypeForKeystore(() -> KeystoreType.PKCS12));
      final KeyStore trustStore = loadKeystoreFromStream(SocketFactory.getAsStream(truststorePath), truststorePassword,
          getDefaultKeystoreTypeForTruststore(() -> KeystoreType.JKS));

      final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(keyStore, keystorePassword.toCharArray());

      final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(trustStore);

      final SSLContext sslContext = SSLContext.getInstance(TlsProtocol.getLatestTlsVersion().getTlsVersion());
      sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
      return sslContext;

    } catch (final ConfigurationException e) {
      throw e;
    } catch (final Exception e) {
      throw new ConfigurationException("Failed to initialize SSL context for " + protocolName + " TLS", e);
    }
  }

  private static String getRequiredSetting(final ContextConfiguration configuration, final GlobalConfiguration setting,
      final String protocolName, final String what) {
    final String value = configuration.getValueAsString(setting);
    if (value == null || value.isEmpty())
      throw new ConfigurationException(
          protocolName + " TLS is enabled but the SSL " + what + " is not configured (" + setting.getKey() + ")");
    return value;
  }

  public static String getDefaultKeystoreTypeForKeystore(Supplier<KeystoreType> defaultSupplier) {
    return doGetDefaultKeystoreType(JAVAX_NET_SSL_KEYSTORE_TYPE,
        defaultSupplier);
  }

  public static String getDefaultKeystoreTypeForTruststore(Supplier<KeystoreType> defaultSupplier) {
    return doGetDefaultKeystoreType(JAVAX_NET_SSL_TRUSTSTORE_TYPE,
        defaultSupplier);
  }

  private static String doGetDefaultKeystoreType(String propertyName,
                                                 Supplier<KeystoreType> defaultSupplier) {

    String keystoreTypeValue = System.getProperty(propertyName);
    // Keystore type is not defined take value from 'defaultSupplier'
    if ((keystoreTypeValue == null) || keystoreTypeValue.isEmpty()) {
      return doValidateDefaultKeystoreSupplier(defaultSupplier)
          .get()
          .getKeystoreType();
    }
    // Try to match with supported keystore types and return the first that matches
    // If no match return the value from 'defaultSupplier'
    return KeystoreType.getFromStringWithDefault(keystoreTypeValue,
            doValidateDefaultKeystoreSupplier(defaultSupplier).get())
        .getKeystoreType();

  }

  private static Supplier<KeystoreType> doValidateDefaultKeystoreSupplier(Supplier<KeystoreType> defaultSupplier) {
    if ((defaultSupplier == null) || (defaultSupplier.get() == null)) {
      throw new ServerSecurityException("Default key store supplier is not configured correctly");
    }
    return defaultSupplier;
  }

  private static InputStream doValidateKeystoreStream(InputStream keystoreInputStream) {
    if (keystoreInputStream == null) {
      throw new ServerSecurityException("Key store stream cannot be null");
    }
    return keystoreInputStream;
  }

}
