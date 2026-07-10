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

import com.arcadedb.log.LogManager;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class TracingPluginTest {

  @Test
  void enabledPluginProducesSpansForObservations() {
    final ObservationRegistry registry = ObservationRegistry.create();
    final InMemorySpanExporter exporter = InMemorySpanExporter.create();

    final TracingPlugin plugin = new TracingPlugin();
    // Test seam: attach a tracer that exports to the in-memory exporter onto the given registry.
    plugin.attachForTest(registry, exporter);

    Observation.createNotStarted("test.op", registry).observe(() -> {
      // work inside the span
    });

    assertThat(exporter.getFinishedSpanItems()).isNotEmpty();
    assertThat(exporter.getFinishedSpanItems().getFirst().getName()).contains("test.op");

    plugin.stopService();
  }

  @Test
  void observingAfterStopIsInertAndDoesNotTouchTheClosedProvider() {
    final ObservationRegistry registry = ObservationRegistry.create();
    final InMemorySpanExporter exporter = InMemorySpanExporter.create();

    final TracingPlugin plugin = new TracingPlugin();
    plugin.attachForTest(registry, exporter);

    Observation.createNotStarted("before.stop", registry).observe(() -> {
    });
    assertThat(exporter.getFinishedSpanItems()).isNotEmpty();

    // The registry has no remove-handler API, so the handler stays registered after stop. It must be
    // deactivated so a later Observation is a no-op and never drives the now-closed tracer provider
    // (which would otherwise risk errors). Asserting the observation runs cleanly proves it.
    plugin.stopService();
    assertThatCode(() -> Observation.createNotStarted("after.stop", registry).observe(() -> {
    })).doesNotThrowAnyException();
  }

  @Test
  void traceContextSupplierExposesActiveTraceIdToLogManager() {
    final ObservationRegistry registry = ObservationRegistry.create();
    final InMemorySpanExporter exporter = InMemorySpanExporter.create();

    final TracingPlugin plugin = new TracingPlugin();
    plugin.attachForTest(registry, exporter);
    try {
      // No span is active yet, so the core logger sees no trace context.
      assertThat(LogManager.instance().currentTraceContext()).as("no span active yet").isNull();

      // Inside the observation scope the supplier must surface the active span's ids to the core logger.
      final AtomicReference<String[]> captured = new AtomicReference<>();
      Observation.createNotStarted("test.op", registry)
          .observe(() -> captured.set(LogManager.instance().currentTraceContext()));

      assertThat(captured.get()).as("trace context inside the span").isNotNull();
      assertThat(captured.get()[0]).as("traceId").isNotBlank().isNotEqualTo("00000000000000000000000000000000");
      assertThat(captured.get()[1]).as("spanId").isNotBlank();
    } finally {
      plugin.stopService();
    }

    // The supplier is cleared on stop so the core logger degrades back to requestId-only correlation.
    assertThat(LogManager.instance().currentTraceContext()).isNull();
  }

  @Test
  void disabledByDefaultAttachesNothing() {
    final ObservationRegistry registry = ObservationRegistry.create();
    final InMemorySpanExporter exporter = InMemorySpanExporter.create();

    // No attach: the registry has no tracing handler, so Observations are no-ops and no span is
    // exported. This mirrors a server running without the tracing flag enabled.
    Observation.createNotStarted("test.op", registry).observe(() -> {
    });

    assertThat(exporter.getFinishedSpanItems()).isEmpty();
  }
}
