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

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.transport.RequestReplyReceiverContext;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TraceparentPropagationTest {

  @Test
  void continuesUpstreamTraceFromTraceparentHeader() {
    final ObservationRegistry registry = ObservationRegistry.create();
    final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    final TracingPlugin plugin = new TracingPlugin();
    plugin.attachForTest(registry, exporter);

    final String parentTraceId = "4bf92f3577b34da6a3ce929d0e0e4736";
    final Map<String, String> headers = Map.of("traceparent", "00-" + parentTraceId + "-00f067aa0ba902b7-01");

    final RequestReplyReceiverContext<Map<String, String>, Object> ctx =
        new RequestReplyReceiverContext<>((carrier, key) -> carrier.get(key));
    ctx.setCarrier(headers);

    Observation.createNotStarted("http.in", () -> ctx, registry).observe(() -> {
      // handle the request inside the continued trace
    });

    assertThat(exporter.getFinishedSpanItems()).isNotEmpty();
    assertThat(exporter.getFinishedSpanItems().getFirst().getTraceId()).isEqualTo(parentTraceId);

    plugin.stopService();
  }
}
