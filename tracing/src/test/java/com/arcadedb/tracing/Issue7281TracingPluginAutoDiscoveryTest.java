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
package com.arcadedb.tracing;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7281.
 * <p>
 * {@code arcadedb.serverMetrics.tracing.enabled=true} read back correctly from {@code /api/v1/server} and yet no
 * span was ever exported: the plugin did not override
 * {@link com.arcadedb.server.ServerPlugin#isAutoDiscovered}, whose default is {@code false}, so
 * {@code PluginManager} installed it only for a deployment that ALSO named it in {@code arcadedb.server.plugins}.
 * The enable flag governed what {@code configure()} did, and nothing governed whether {@code configure()} ran.
 */
class Issue7281TracingPluginAutoDiscoveryTest {

  @AfterEach
  void restoreTheSettings() {
    GlobalConfiguration.SERVER_METRICS_TRACING_ENABLED.reset();
  }

  @Test
  void tracingStaysOffOnAServerThatEnablesNothing() {
    assertThat(new TracingPlugin().isAutoDiscovered(new ContextConfiguration())).isFalse();
  }

  /**
   * The {@code -D}/environment-variable channel, which is the one the Helm chart renders.
   */
  @Test
  void theEnableFlagSetAsASystemPropertyIsEnoughToActivateThePlugin() {
    GlobalConfiguration.SERVER_METRICS_TRACING_ENABLED.setValue(true);

    assertThat(new TracingPlugin().isAutoDiscovered(new ContextConfiguration())).isTrue();
  }

  /**
   * The server-configuration-file channel: {@code fromJSON} writes into the {@link ContextConfiguration} overlay
   * that {@code PluginManager} reads.
   */
  @Test
  void theEnableFlagSetInTheServerConfigurationIsEnoughToActivateThePlugin() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_METRICS_TRACING_ENABLED.getKey(), true);

    assertThat(new TracingPlugin().isAutoDiscovered(cfg)).isTrue();
  }
}
