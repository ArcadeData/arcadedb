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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7281.
 * <p>
 * Setting {@code arcadedb.serverMetrics.otlp.enabled=true} was not enough to get the plugin running: the plugin did
 * not override {@link com.arcadedb.server.ServerPlugin#isAutoDiscovered}, whose default is {@code false}, so
 * {@code PluginManager} skipped the {@code ServiceLoader}-discovered instance unless the deployment ALSO named it in
 * {@code arcadedb.server.plugins}. {@code configure()} was never called, which is why the reporter saw no OTLP log
 * line at all while the setting read back correctly from {@code /api/v1/server}.
 * <p>
 * The two settings also gain a {@link GlobalConfiguration} entry here: they were read through the raw-string
 * {@code getValue(String, default)} overload, so they worked as a {@code -D} but appeared in neither the startup
 * dump nor {@code /api/v1/server} - the reporter's "There are no otlp settings".
 */
class Issue7281OtlpPluginAutoDiscoveryTest {

  @AfterEach
  void restoreTheSettings() {
    GlobalConfiguration.SERVER_METRICS.reset();
    GlobalConfiguration.SERVER_METRICS_OTLP_ENABLED.reset();
    GlobalConfiguration.SERVER_METRICS_OTLP_ENDPOINT.reset();
  }

  @Test
  void aServerThatNamesNoPluginDoesNotGetAnOtlpExporter() {
    assertThat(new OtlpMetricsPlugin().isAutoDiscovered(new ContextConfiguration())).isFalse();
  }

  /**
   * The {@code -D}/environment-variable channel: {@code readConfiguration()} stores the value on the enum, and an
   * empty {@link ContextConfiguration} overlay reads straight through to it. This is the channel the Helm chart
   * uses.
   */
  @Test
  void theEnableFlagSetAsASystemPropertyIsEnoughToActivateThePlugin() {
    GlobalConfiguration.SERVER_METRICS_OTLP_ENABLED.setValue(true);

    assertThat(new OtlpMetricsPlugin().isAutoDiscovered(new ContextConfiguration())).isTrue();
  }

  /**
   * The server-configuration-file channel: {@code fromJSON} writes into the {@link ContextConfiguration} overlay,
   * which {@code PluginManager} is handed and which shadows the enum value.
   */
  @Test
  void theEnableFlagSetInTheServerConfigurationIsEnoughToActivateThePlugin() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_METRICS_OTLP_ENABLED.getKey(), true);

    assertThat(new OtlpMetricsPlugin().isAutoDiscovered(cfg)).isTrue();
  }

  /**
   * {@code arcadedb.serverMetrics=false} turns metrics off wholesale, and {@code configure()} already honours it.
   * Auto-discovery has to agree with that gate, otherwise the plugin is installed only to do nothing.
   */
  @Test
  void theGlobalMetricsKillSwitchAlsoKeepsThePluginOut() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_METRICS.getKey(), false);
    cfg.setValue(GlobalConfiguration.SERVER_METRICS_OTLP_ENABLED.getKey(), true);

    assertThat(new OtlpMetricsPlugin().isAutoDiscovered(cfg)).isFalse();
  }

  /**
   * Both OTLP settings are declared, so the startup dump and {@code /api/v1/server} - which both enumerate
   * {@link GlobalConfiguration#values()} - list them like every other setting.
   */
  @Test
  void bothOtlpSettingsAreDeclaredAndVisibleToTheSettingsApi() {
    assertThat(GlobalConfiguration.findByKey("arcadedb.serverMetrics.otlp.enabled"))
        .isSameAs(GlobalConfiguration.SERVER_METRICS_OTLP_ENABLED);
    assertThat(GlobalConfiguration.findByKey("arcadedb.serverMetrics.otlp.endpoint"))
        .isSameAs(GlobalConfiguration.SERVER_METRICS_OTLP_ENDPOINT);

    assertThat(GlobalConfiguration.SERVER_METRICS_OTLP_ENABLED.getDefValue()).isEqualTo(Boolean.FALSE);
    assertThat(GlobalConfiguration.SERVER_METRICS_OTLP_ENDPOINT.getDefValue()).isEqualTo("http://localhost:4317");
    assertThat(GlobalConfiguration.SERVER_METRICS_OTLP_ENABLED.getScope()).isEqualTo(GlobalConfiguration.SCOPE.SERVER);
    assertThat(GlobalConfiguration.SERVER_METRICS_OTLP_ENDPOINT.getScope()).isEqualTo(GlobalConfiguration.SCOPE.SERVER);
  }

  /**
   * The endpoint keeps being read from the same key it was read from before it was declared, so an existing
   * {@code -D arcadedb.serverMetrics.otlp.endpoint=...} deployment keeps working.
   */
  @Test
  void theEndpointIsStillReadFromItsOriginalKey() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue("arcadedb.serverMetrics.otlp.endpoint", "http://otel-agent.sf-infra:4317");

    assertThat(cfg.getValueAsString(GlobalConfiguration.SERVER_METRICS_OTLP_ENDPOINT))
        .isEqualTo("http://otel-agent.sf-infra:4317");
  }
}
