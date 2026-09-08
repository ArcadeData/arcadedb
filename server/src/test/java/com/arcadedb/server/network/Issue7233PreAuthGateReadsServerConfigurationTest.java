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
package com.arcadedb.server.network;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7233: {@code arcadedb.network.maxPreAuthConnections} is SCOPE.SERVER, so a value written into the server's
 * {@link ContextConfiguration} - by the server configuration file, {@code SET SERVER SETTING} or the MCP tool - has
 * to reach the gate. It used to be read off the {@link GlobalConfiguration} enum, which only a system property or
 * an environment variable ever writes, so every listener ran on the compiled-in default instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7233PreAuthGateReadsServerConfigurationTest {

  @Test
  void theCapComesFromTheServerConfiguration() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.NETWORK_MAX_PREAUTH_CONNECTIONS, 7);

    assertThat(new PreAuthConnectionGate("TEST", configuration).getMaxConnections()).isEqualTo(7);
  }

  @Test
  void aServerConfigurationFileValueReachesTheCapToo() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.fromJSON("{\"configuration\":{\"network.maxPreAuthConnections\":11}}");

    assertThat(new PreAuthConnectionGate("TEST", configuration).getMaxConnections()).isEqualTo(11);
  }

  @Test
  void anEmptyConfigurationStillReadsTheDefault() {
    assertThat(new PreAuthConnectionGate("TEST", new ContextConfiguration()).getMaxConnections()).isEqualTo(
        GlobalConfiguration.NETWORK_MAX_PREAUTH_CONNECTIONS.getValueAsInteger());
  }
}
