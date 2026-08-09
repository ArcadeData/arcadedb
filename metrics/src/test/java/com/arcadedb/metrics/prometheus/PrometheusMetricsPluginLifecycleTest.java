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
package com.arcadedb.metrics.prometheus;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link PrometheusMetricsPlugin} is re-instantiated by {@code PluginManager} via {@code ServiceLoader}
 * on every server start/stop cycle (same lifecycle as {@link com.arcadedb.metrics.otlp.OtlpMetricsPlugin}).
 * Without a {@code stopService()} that removes its registry, every restart added another
 * {@link PrometheusMeterRegistry} to the JVM-global {@link Metrics#globalRegistry} composite and never
 * removed the previous one.
 */
class PrometheusMetricsPluginLifecycleTest {

  @AfterEach
  void cleanup() {
    // Remove any registry this test added so it does not leak into other tests sharing the JVM-global registry.
    final List<MeterRegistry> registries = new ArrayList<>(Metrics.globalRegistry.getRegistries());
    registries.forEach(Metrics::removeRegistry);
  }

  @Test
  void disabledByDefaultRegistersNothing() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_METRICS, false);
    final int before = Metrics.globalRegistry.getRegistries().size();

    new PrometheusMetricsPlugin().configure(null, cfg);

    assertThat(Metrics.globalRegistry.getRegistries().size()).isEqualTo(before);
  }

  @Test
  void stopServiceRemovesTheRegistryItAdded() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_METRICS, true);
    final int before = Metrics.globalRegistry.getRegistries().size();

    final PrometheusMetricsPlugin plugin = new PrometheusMetricsPlugin();
    plugin.configure(null, cfg);
    assertThat(Metrics.globalRegistry.getRegistries().size()).isEqualTo(before + 1);

    plugin.stopService();
    assertThat(Metrics.globalRegistry.getRegistries().size()).isEqualTo(before);
  }

  @Test
  void stopServiceIsNullSafeWhenNeverConfigured() {
    // configure() was never called (enabled stays false), so registry is null: stopService must not NPE.
    new PrometheusMetricsPlugin().stopService();
  }

  @Test
  void repeatedStartStopCyclesDoNotAccumulateRegistries() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_METRICS, true);
    final int before = Metrics.globalRegistry.getRegistries().size();

    for (int i = 0; i < 5; i++) {
      final PrometheusMetricsPlugin plugin = new PrometheusMetricsPlugin();
      plugin.configure(null, cfg);
      plugin.startService();
      plugin.stopService();
    }

    assertThat(Metrics.globalRegistry.getRegistries().size()).isEqualTo(before);
  }
}
