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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for BoltSslHelper.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BoltSslHelperTest {

  @Test
  void disabledModeByDefault() {
    final ContextConfiguration config = new ContextConfiguration();
    final BoltSslHelper helper = new BoltSslHelper(config);
    assertThat(helper.getTlsMode()).isEqualTo(BoltSslHelper.TlsMode.DISABLED);
  }

  @Test
  void parsesOptionalMode() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.BOLT_SSL, "OPTIONAL");
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, "../server/src/test/resources/keystore.pkcs12");
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "sos0nmzWniR0");
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, "../server/src/test/resources/truststore.jks");
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, "nphgDK7ugjGR");
    final BoltSslHelper helper = new BoltSslHelper(config);
    assertThat(helper.getTlsMode()).isEqualTo(BoltSslHelper.TlsMode.OPTIONAL);
  }

  @Test
  void parsesRequiredMode() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.BOLT_SSL, "REQUIRED");
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, "../server/src/test/resources/keystore.pkcs12");
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "sos0nmzWniR0");
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, "../server/src/test/resources/truststore.jks");
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, "nphgDK7ugjGR");
    final BoltSslHelper helper = new BoltSslHelper(config);
    assertThat(helper.getTlsMode()).isEqualTo(BoltSslHelper.TlsMode.REQUIRED);
  }

  @Test
  void caseInsensitiveModeParsing() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.BOLT_SSL, "optional");
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, "../server/src/test/resources/keystore.pkcs12");
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "sos0nmzWniR0");
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, "../server/src/test/resources/truststore.jks");
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, "nphgDK7ugjGR");
    final BoltSslHelper helper = new BoltSslHelper(config);
    assertThat(helper.getTlsMode()).isEqualTo(BoltSslHelper.TlsMode.OPTIONAL);
  }

  @Test
  void invalidModeThrowsException() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.BOLT_SSL, "INVALID");
    assertThatThrownBy(() -> new BoltSslHelper(config))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Invalid value 'INVALID'");
  }

  @Test
  void missingKeystoreThrowsException() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.BOLT_SSL, "REQUIRED");
    // No keystore configured
    assertThatThrownBy(() -> new BoltSslHelper(config))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("key store path");
  }

  @Test
  void missingTruststoreThrowsException() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.BOLT_SSL, "REQUIRED");
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, "../server/src/test/resources/keystore.pkcs12");
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "sos0nmzWniR0");
    // No truststore configured
    assertThatThrownBy(() -> new BoltSslHelper(config))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("trust store path");
  }
}
