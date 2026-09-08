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
package com.arcadedb.metrics.otlp;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerPlugin;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end half of issue #7281: a real server boot, driven only by the enable flag the Helm chart renders, with
 * nothing named in {@code arcadedb.server.plugins}. This is the shape of the reporter's deployment, and before the
 * fix {@code PluginManager} skipped the {@code ServiceLoader}-discovered plugin here even though the jar was on the
 * classpath and the flag read back correctly from {@code /api/v1/server}.
 */
class Issue7281OtlpPluginDiscoveryIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_METRICS_OTLP_ENABLED.getKey(), true);
    // This test never speaks HTTP - it asks the running server which plugins it installed. Give it a port range of
    // its own so it neither contends with, nor is answered by, anything already listening on the default 2480-2489.
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, "2580-2589");
  }

  @Test
  void theOtlpPluginIsInstalledWithoutAServerPluginsEntry() {
    // The test is only meaningful while nothing names the plugin: otherwise the old code path would install it too.
    assertThat(GlobalConfiguration.SERVER_PLUGINS.getValueAsString()).doesNotContain("Otlp");

    assertThat(getServer(0).getPlugins())
        .filteredOn(plugin -> plugin instanceof OtlpMetricsPlugin)
        .hasSize(1);
  }

  @Test
  void everyOtherPluginIsStillOptIn() {
    // Auto-discovery is per-plugin and gated on that plugin's own flag: turning OTLP on must not drag in the
    // Prometheus scrape endpoint, whose own gate (arcadedb.serverMetrics) defaults to true.
    assertThat(getServer(0).getPlugins())
        .extracting(ServerPlugin::getName)
        .doesNotContain("PrometheusMetricsPlugin");
  }
}
