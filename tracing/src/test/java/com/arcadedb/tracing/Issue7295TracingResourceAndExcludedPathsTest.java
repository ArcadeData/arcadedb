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
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7295: spans carry a real {@code service.name} (not {@code unknown_service:java}), resolved from the
 * OpenTelemetry environment variables and ArcadeDB's setting, and the readiness/health probes produce no span.
 */
class Issue7295TracingResourceAndExcludedPathsTest {
  private static final AttributeKey<String> SERVICE_NAME = AttributeKey.stringKey("service.name");

  @Test
  void spansReportArcadedbAsServiceNameByDefault() {
    final SpanData span = exportOneSpan(new ContextConfiguration(), Map.of());

    assertThat(span.getResource().getAttribute(SERVICE_NAME)).isEqualTo("arcadedb");
    // The SDK's own resource attributes survive the merge.
    assertThat(span.getResource().getAttribute(AttributeKey.stringKey("telemetry.sdk.language"))).isEqualTo("java");
  }

  @Test
  void spansReportTheConfiguredServiceName() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_METRICS_SERVICE_NAME, "orders-graph");

    assertThat(exportOneSpan(configuration, Map.of()).getResource().getAttribute(SERVICE_NAME)).isEqualTo("orders-graph");
  }

  @Test
  void spansReportOtelServiceNameAndResourceAttributes() {
    final SpanData span = exportOneSpan(new ContextConfiguration(),
        Map.of("OTEL_SERVICE_NAME", "graph-prod", "OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=prod"));

    assertThat(span.getResource().getAttribute(SERVICE_NAME)).isEqualTo("graph-prod");
    assertThat(span.getResource().getAttribute(AttributeKey.stringKey("deployment.environment"))).isEqualTo("prod");
  }

  @Test
  void spansReportAServiceNameSuppliedOnlyByResourceAttributes() {
    final SpanData span = exportOneSpan(new ContextConfiguration(),
        Map.of("OTEL_RESOURCE_ATTRIBUTES", "service.name=graph-attrs,service.namespace=data"));

    assertThat(span.getResource().getAttribute(SERVICE_NAME)).isEqualTo("graph-attrs");
    assertThat(span.getResource().getAttribute(AttributeKey.stringKey("service.namespace"))).isEqualTo("data");
  }

  @Test
  void anExcludedProbeStaysVisibleToOtherObservationHandlers() {
    final ObservationRegistry registry = ObservationRegistry.create();
    final AtomicInteger otherHandlerStarts = new AtomicInteger();
    registry.observationConfig().observationHandler(new ObservationHandler<>() {
      @Override
      public boolean supportsContext(final Observation.Context context) {
        return true;
      }

      @Override
      public void onStart(final Observation.Context context) {
        otherHandlerStarts.incrementAndGet();
      }
    });
    final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    final TracingPlugin plugin = new TracingPlugin();
    plugin.attachForTest(registry, exporter, new ContextConfiguration(), Map.of());
    try {
      httpObservation(registry, "/api/v1/ready").observe(() -> {
      });

      // Only the tracing handler declines the probe; the Observation itself is not vetoed.
      assertThat(exporter.getFinishedSpanItems()).isEmpty();
      assertThat(otherHandlerStarts.get()).isEqualTo(1);
    } finally {
      plugin.stopService();
    }
  }

  @Test
  void healthProbesAreNotTracedByDefault() {
    final ObservationRegistry registry = ObservationRegistry.create();
    final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    final TracingPlugin plugin = new TracingPlugin();
    plugin.attachForTest(registry, exporter, new ContextConfiguration(), Map.of());
    try {
      httpObservation(registry, "/api/v1/ready").observe(() -> {
      });
      httpObservation(registry, "/api/v1/health").observe(() -> {
      });
      // A trailing slash does not smuggle a probe past the exclusion.
      httpObservation(registry, "/api/v1/ready/").observe(() -> {
      });
      assertThat(exporter.getFinishedSpanItems()).as("probe requests must not produce spans").isEmpty();

      httpObservation(registry, "/api/v1/query/graph").observe(() -> {
      });
      assertThat(exporter.getFinishedSpanItems()).hasSize(1);
    } finally {
      plugin.stopService();
    }
  }

  @Test
  void excludedPathsAreConfigurableAndAnEmptySettingTracesEverything() {
    final ContextConfiguration custom = new ContextConfiguration();
    custom.setValue(GlobalConfiguration.SERVER_METRICS_TRACING_EXCLUDED_PATHS, " /prometheus/ , ,/api/v1/ready, /api/v1/ready/");
    assertThat(TracingPlugin.excludedPaths(custom)).containsExactly("/prometheus", "/api/v1/ready");

    final ContextConfiguration none = new ContextConfiguration();
    none.setValue(GlobalConfiguration.SERVER_METRICS_TRACING_EXCLUDED_PATHS, "");
    assertThat(TracingPlugin.excludedPaths(none)).isEmpty();

    final ObservationRegistry registry = ObservationRegistry.create();
    final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    final TracingPlugin plugin = new TracingPlugin();
    plugin.attachForTest(registry, exporter, none, Map.of());
    try {
      httpObservation(registry, "/api/v1/ready").observe(() -> {
      });
      assertThat(exporter.getFinishedSpanItems()).hasSize(1);
    } finally {
      plugin.stopService();
    }
  }

  @Test
  void theExclusionIsInertOnceThePluginStops() {
    final ObservationRegistry registry = ObservationRegistry.create();
    final InMemorySpanExporter stoppedExporter = InMemorySpanExporter.create();
    final TracingPlugin stopped = new TracingPlugin();
    stopped.attachForTest(registry, stoppedExporter, new ContextConfiguration(), Map.of());
    stopped.stopService();

    // The registry offers no API to remove a handler, so a stopped plugin's handler (and its exclusion list) stays
    // registered. It must not keep declining observations for a plugin attached afterwards with another configuration.
    final ContextConfiguration none = new ContextConfiguration();
    none.setValue(GlobalConfiguration.SERVER_METRICS_TRACING_EXCLUDED_PATHS, "");
    final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    final TracingPlugin plugin = new TracingPlugin();
    plugin.attachForTest(registry, exporter, none, Map.of());
    try {
      httpObservation(registry, "/api/v1/ready").observe(() -> {
      });
      assertThat(exporter.getFinishedSpanItems()).hasSize(1);
    } finally {
      plugin.stopService();
    }
  }

  private static SpanData exportOneSpan(final ContextConfiguration configuration, final Map<String, String> environment) {
    final ObservationRegistry registry = ObservationRegistry.create();
    final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    final TracingPlugin plugin = new TracingPlugin();
    plugin.attachForTest(registry, exporter, configuration, environment);
    try {
      Observation.createNotStarted("test.op", registry).observe(() -> {
      });
      final List<SpanData> spans = exporter.getFinishedSpanItems();
      assertThat(spans).hasSize(1);
      return spans.get(0);
    } finally {
      plugin.stopService();
    }
  }

  /**
   * An Observation shaped like the one the HTTP handler opens: its context carries the raw request path under the key
   * the server publishes.
   */
  private static Observation httpObservation(final ObservationRegistry registry, final String requestPath) {
    return Observation.createNotStarted("arcadedb.http.server.requests", () -> {
      final Observation.Context context = new Observation.Context();
      context.put(AbstractServerHttpHandler.OBSERVATION_REQUEST_PATH, requestPath);
      return context;
    }, registry);
  }
}
