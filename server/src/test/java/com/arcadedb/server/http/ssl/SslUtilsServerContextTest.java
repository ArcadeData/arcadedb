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
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Error reporting of {@link SslUtils#createServerSslContext}, shared by the Postgres, Bolt and Redis listeners (issue
 * #8840): a protocol is never started with a half-configured TLS setup, and the message names the protocol and the
 * setting to fix.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SslUtilsServerContextTest {

  @Test
  void validStoresBuildAContextThatServes() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, "src/test/resources/keystore.pkcs12");
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "sos0nmzWniR0");
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, "src/test/resources/truststore.jks");
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, "nphgDK7ugjGR");

    final SSLContext context = SslUtils.createServerSslContext(configuration, "Postgres");

    assertThat(context.getProtocol()).startsWith("TLS");
    assertThat(context.getServerSocketFactory()).isNotNull();
  }

  @Test
  void missingKeyStorePathNamesTheProtocolAndTheSetting() {
    assertThatThrownBy(() -> SslUtils.createServerSslContext(new ContextConfiguration(), "Postgres"))
        .isInstanceOf(ConfigurationException.class).hasMessageContaining("Postgres TLS")
        .hasMessageContaining("key store path").hasMessageContaining(GlobalConfiguration.NETWORK_SSL_KEYSTORE.getKey());
  }

  @Test
  void missingTrustStorePathIsReportedAfterTheKeyStore() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, "keystore.pkcs12");
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "secret");

    assertThatThrownBy(() -> SslUtils.createServerSslContext(configuration, "Redis")).isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Redis TLS").hasMessageContaining("trust store path");
  }

  @Test
  void unreadableKeyStoreIsWrappedInAConfigurationException() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, "/nonexistent/keystore.pkcs12");
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "secret");
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, "/nonexistent/truststore.jks");
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, "secret");

    assertThatThrownBy(() -> SslUtils.createServerSslContext(configuration, "BOLT")).isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("BOLT");
  }
}
