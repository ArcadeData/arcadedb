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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the opt-in {@link OtlpMetricsPlugin}: it registers an OTLP registry only when both
 * server metrics and the OTLP flag are enabled, and is a no-op otherwise (default-off).
 */
class OtlpMetricsPluginTest {

  @AfterEach
  void cleanup() {
    // Remove any registry this test added so it does not leak into other tests sharing the JVM-global registry.
    final List<MeterRegistry> registries = new ArrayList<>(Metrics.globalRegistry.getRegistries());
    registries.forEach(Metrics::removeRegistry);
  }

  @Test
  void disabledByDefaultRegistersNothing() {
    final int before = Metrics.globalRegistry.getRegistries().size();
    final OtlpMetricsPlugin plugin = new OtlpMetricsPlugin();
    plugin.configure(null, new ContextConfiguration());
    assertThat(Metrics.globalRegistry.getRegistries().size()).isEqualTo(before);
  }

  @Test
  void enabledRequiresBothFlags() {
    final ContextConfiguration cfg = new ContextConfiguration();
    // OTLP flag on but server metrics explicitly off (SERVER_METRICS defaults to true): still a no-op.
    cfg.setValue("arcadedb.serverMetrics", false);
    cfg.setValue("arcadedb.serverMetrics.otlp.enabled", true);
    final int before = Metrics.globalRegistry.getRegistries().size();
    new OtlpMetricsPlugin().configure(null, cfg);
    assertThat(Metrics.globalRegistry.getRegistries().size()).isEqualTo(before);
  }

  @Test
  void enabledRegistersOtlpRegistry() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue("arcadedb.serverMetrics", true);
    cfg.setValue("arcadedb.serverMetrics.otlp.enabled", true);
    final int before = Metrics.globalRegistry.getRegistries().size();

    final OtlpMetricsPlugin plugin = new OtlpMetricsPlugin();
    plugin.configure(null, cfg);
    assertThat(Metrics.globalRegistry.getRegistries().size()).isEqualTo(before + 1);

    // stopService must unregister and close cleanly.
    plugin.stopService();
    assertThat(Metrics.globalRegistry.getRegistries().size()).isEqualTo(before);
  }
}
