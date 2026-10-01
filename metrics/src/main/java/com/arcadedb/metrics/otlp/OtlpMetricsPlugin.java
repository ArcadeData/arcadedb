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
import com.arcadedb.server.monitor.OtelResourceAttributes;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
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

  /**
   * The enable flag is the whole opt-in: a deployment that sets it does not also have to name this plugin in
   * {@code arcadedb.server.plugins} (issue #7281). Before this, the flag governed what {@code configure()} did and
   * nothing governed whether {@code configure()} ran, so setting it alone produced no exporter and no log line.
   */
  @Override
  public boolean isAutoDiscovered(final ContextConfiguration configuration) {
    return isEnabledBy(configuration);
  }

  @Override
  public void configure(final ArcadeDBServer server, final ContextConfiguration configuration) {
    enabled = isEnabledBy(configuration);
    if (!enabled)
      return;

    registry = new OtlpMeterRegistry(otlpConfig(configuration, System.getenv()), Clock.SYSTEM);
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
   * The OTLP registry's configuration: the endpoint from the ArcadeDB setting, and the resource attributes resolved by
   * {@link OtelResourceAttributes}, the same resolution the tracing plugin uses, so metrics and spans report the same
   * {@code service.name} (issue #7295). Micrometer's own default read the OpenTelemetry variables too, but let a
   * {@code service.name} in {@code OTEL_RESOURCE_ATTRIBUTES} win over {@code OTEL_SERVICE_NAME} and otherwise reported
   * {@code unknown_service}.
   */
  static OtlpConfig otlpConfig(final ContextConfiguration configuration, final Map<String, String> environment) {
    final String configured = configuration.getValueAsString(GlobalConfiguration.SERVER_METRICS_OTLP_ENDPOINT);
    if (looksLikeGrpcEndpoint(configured))
      LogManager.instance().log(OtlpMetricsPlugin.class, Level.WARNING,
          "OTLP metrics endpoint '%s' looks like the OTLP/gRPC port (4317), but metrics are exported over OTLP/HTTP: use the collector's HTTP receiver, e.g. http://host:4318/v1/metrics",
          configured);
    final String endpoint = normalizeEndpoint(configured);
    final Map<String, String> resourceAttributes = OtelResourceAttributes.resolve(configuration, environment);
    return new OtlpConfig() {
      @Override
      public String get(final String key) {
        return "otlp.url".equals(key) ? endpoint : null;
      }

      @Override
      public Map<String, String> resourceAttributes() {
        return resourceAttributes;
      }
    };
  }

  /**
   * Micrometer POSTs to the configured URL as is, so a base URL without a path reaches the collector root and is
   * answered with 404. The standard OTLP/HTTP metrics path is appended in that case; an explicit path is kept (issue #7294).
   */
  static String normalizeEndpoint(final String endpoint) {
    if (endpoint == null)
      return null;
    try {
      final URI uri = new URI(endpoint.trim());
      final String path = uri.getRawPath();
      if (uri.getHost() != null && (path == null || path.isEmpty() || "/".equals(path))) {
        final String base = endpoint.trim();
        int cut = base.length();
        for (final char delimiter : new char[] { '?', '#' }) {
          final int pos = base.indexOf(delimiter);
          if (pos >= 0 && pos < cut)
            cut = pos;
        }
        final String head = base.substring(0, cut);
        return (head.endsWith("/") ? head.substring(0, head.length() - 1) : head) + "/v1/metrics" + base.substring(cut);
      }
    } catch (final URISyntaxException e) {
      // leave it untouched, the exporter reports the bad URL
    }
    return endpoint;
  }

  /**
   * True for an endpoint on the OTLP/gRPC port 4317, which the HTTP-based metrics registry cannot talk to.
   */
  static boolean looksLikeGrpcEndpoint(final String endpoint) {
    if (endpoint == null)
      return false;
    try {
      return new URI(endpoint.trim()).getPort() == 4317;
    } catch (final URISyntaxException e) {
      return false;
    }
  }

  /**
   * The one reading of "OTLP is on", shared by the activation gate and by {@code configure()} so the two cannot
   * disagree: the global metrics kill switch wins over the OTLP flag, exactly as it did before.
   */
  private static boolean isEnabledBy(final ContextConfiguration configuration) {
    return configuration.getValueAsBoolean(GlobalConfiguration.SERVER_METRICS)
        && configuration.getValueAsBoolean(GlobalConfiguration.SERVER_METRICS_OTLP_ENABLED);
  }
}
