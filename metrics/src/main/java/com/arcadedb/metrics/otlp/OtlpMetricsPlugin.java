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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;

import java.util.logging.Level;

/**
 * Optional {@link ServerPlugin} that pushes Micrometer metrics to an OTLP endpoint, alongside (never
 * replacing) the Prometheus scrape endpoint. Disabled unless both {@code arcadedb.serverMetrics} and
 * {@code arcadedb.serverMetrics.otlp.enabled} are true, so the default behavior is byte-for-byte
 * unchanged. The Prometheus scrape path is untouched whether or not OTLP is enabled.
 */
public class OtlpMetricsPlugin implements ServerPlugin {
  private OtlpMeterRegistry registry;
  private boolean           enabled;

  @Override
  public void configure(final ArcadeDBServer server, final ContextConfiguration configuration) {
    final boolean metricsOn = configuration.getValueAsBoolean(GlobalConfiguration.SERVER_METRICS);
    final boolean otlpOn = asBoolean(configuration.getValue("arcadedb.serverMetrics.otlp.enabled", (Object) Boolean.FALSE));
    enabled = metricsOn && otlpOn;
    if (!enabled)
      return;

    final String endpoint = asString(configuration.getValue("arcadedb.serverMetrics.otlp.endpoint", (Object) "http://localhost:4317"));
    final OtlpConfig otlpConfig = key -> "otlp.url".equals(key) ? endpoint : null;
    registry = new OtlpMeterRegistry(otlpConfig, Clock.SYSTEM);
    Metrics.addRegistry(registry);
  }

  @Override
  public void startService() {
    if (enabled)
      LogManager.instance().log(this, Level.INFO, "OTLP metrics export enabled");
  }

  @Override
  public void stopService() {
    if (registry != null) {
      Metrics.removeRegistry(registry);
      registry.close();
      registry = null;
    }
  }

  /**
   * Coerces a config value to a boolean. The value may be a {@link Boolean} (set programmatically /
   * in tests) or a {@link String} (read from a config file or system property).
   */
  private static boolean asBoolean(final Object value) {
    if (value instanceof Boolean b)
      return b;
    return value != null && Boolean.parseBoolean(value.toString());
  }

  private static String asString(final Object value) {
    return value != null ? value.toString() : null;
  }
}
