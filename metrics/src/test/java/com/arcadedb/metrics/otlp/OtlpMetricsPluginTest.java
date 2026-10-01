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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.registry.otlp.OtlpConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

  /**
   * Issue #7295: the OTLP registry reports the same service.name the tracing plugin does - "arcadedb" by default rather
   * than Micrometer's "unknown_service" - and keeps the endpoint from the ArcadeDB setting.
   */
  @Test
  void otlpConfigReportsArcadedbServiceNameByDefault() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_METRICS_OTLP_ENDPOINT, "http://collector:4318/v1/metrics");

    final OtlpConfig config = OtlpMetricsPlugin.otlpConfig(cfg, Map.of());

    assertThat(config.resourceAttributes().get("service.name")).isEqualTo("arcadedb");
    assertThat(config.url()).isEqualTo("http://collector:4318/v1/metrics");
  }

  /**
   * Issue #7294: Micrometer exports OTLP over HTTP/protobuf, so the default must be the collector's HTTP receiver
   * (port 4318, path /v1/metrics), not the gRPC port 4317 that the metrics registry cannot speak.
   */
  @Test
  void defaultEndpointIsTheOtlpHttpMetricsReceiver() {
    assertThat(GlobalConfiguration.SERVER_METRICS_OTLP_ENDPOINT.getDefValue()).isEqualTo("http://localhost:4318/v1/metrics");
    assertThat(OtlpMetricsPlugin.otlpConfig(new ContextConfiguration(), Map.of()).url()).isEqualTo("http://localhost:4318/v1/metrics");
  }

  /**
   * Issue #7294: a base URL with no path (what the gRPC-style documentation example looked like) would be POSTed to the
   * collector root and answered with 404, so the standard OTLP/HTTP metrics path is appended. An explicit path is kept.
   */
  @Test
  void endpointWithoutPathGetsTheStandardMetricsPath() {
    assertThat(OtlpMetricsPlugin.normalizeEndpoint("http://otel-collector:4318")).isEqualTo("http://otel-collector:4318/v1/metrics");
    assertThat(OtlpMetricsPlugin.normalizeEndpoint("http://otel-collector:4318/")).isEqualTo("http://otel-collector:4318/v1/metrics");
    assertThat(OtlpMetricsPlugin.normalizeEndpoint("http://otel-collector:4318/v1/metrics")).isEqualTo("http://otel-collector:4318/v1/metrics");
    assertThat(OtlpMetricsPlugin.normalizeEndpoint("https://ingest.example.com/otlp/v1/metrics?x=1"))
        .isEqualTo("https://ingest.example.com/otlp/v1/metrics?x=1");
    assertThat(OtlpMetricsPlugin.normalizeEndpoint("https://ingest.example.com/custom")).isEqualTo("https://ingest.example.com/custom");
  }

  /**
   * Issue #7294: the gRPC port 4317 is the classic misconfiguration; it is recognised so the plugin can warn about it.
   */
  @Test
  void grpcPortIsRecognised() {
    assertThat(OtlpMetricsPlugin.looksLikeGrpcEndpoint("http://otel-collector:4317")).isTrue();
    assertThat(OtlpMetricsPlugin.looksLikeGrpcEndpoint("http://otel-collector:4317/")).isTrue();
    assertThat(OtlpMetricsPlugin.looksLikeGrpcEndpoint("http://otel-collector:4318/v1/metrics")).isFalse();
    assertThat(OtlpMetricsPlugin.looksLikeGrpcEndpoint("not a url")).isFalse();
  }

  /**
   * Issue #7295: OTEL_SERVICE_NAME wins over a service.name in OTEL_RESOURCE_ATTRIBUTES, as the OpenTelemetry
   * specification requires. Micrometer's default resolution did the opposite.
   */
  @Test
  void otlpConfigLetsOtelServiceNameWinOverResourceAttributes() {
    final OtlpConfig config = OtlpMetricsPlugin.otlpConfig(new ContextConfiguration(),
        Map.of("OTEL_SERVICE_NAME", "graph-prod", "OTEL_RESOURCE_ATTRIBUTES", "service.name=other,deployment.environment=prod"));

    assertThat(config.resourceAttributes().get("service.name")).isEqualTo("graph-prod");
    assertThat(config.resourceAttributes().get("deployment.environment")).isEqualTo("prod");
  }
}
