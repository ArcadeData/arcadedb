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

import com.arcadedb.server.BaseGraphServerTest;

import io.micrometer.observation.ObservationRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end verification that, with the tracing plugin attached to a real running server, an HTTP
 * request produces an exported span, and an inbound W3C {@code traceparent} header is continued by
 * the production handler wiring. Uses an in-memory exporter rather than the OTLP exporter.
 */
@Tag("slow")
class ServerTracingIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Test
  void httpRequestProducesSpanAndContinuesInboundTrace() throws Exception {
    final ObservationRegistry registry = getServer(0).getObservationRegistry();
    final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    final TracingPlugin plugin = new TracingPlugin();
    // Attach a tracer that exports to the in-memory exporter onto the live server registry.
    plugin.attachForTest(registry, exporter);

    try {
      final String parentTraceId = "4bf92f3577b34da6a3ce929d0e0e4736";
      final HttpURLConnection c = (HttpURLConnection) URI.create("http://localhost:2480/api/v1/ready").toURL().openConnection();
      c.setRequestMethod("GET");
      c.setRequestProperty("traceparent", "00-" + parentTraceId + "-00f067aa0ba902b7-01");
      c.connect();
      assertThat(c.getResponseCode()).isEqualTo(204);
      c.disconnect();

      // The span is exported in the handler's finally block, just after the client gets the
      // response; poll for the HTTP span that continues the inbound trace (other unrelated spans
      // may also be exported, so match on the upstream trace id rather than taking the first one).
      SpanData span = null;
      for (int attempt = 0; attempt < 100 && span == null; attempt++) {
        final List<SpanData> spans = exporter.getFinishedSpanItems();
        span = spans.stream()
            .filter(s -> "arcadedb.http.server.requests".equals(s.getName()))
            .filter(s -> parentTraceId.equals(s.getTraceId()))
            .findFirst()
            .orElse(null);
        if (span == null)
          Thread.sleep(20);
      }

      assertThat(span).as("an HTTP span continuing the inbound traceparent must be exported").isNotNull();
      // It must be a child of the inbound span carried by the traceparent header.
      assertThat(span.getTraceId()).isEqualTo(parentTraceId);
      assertThat(span.getParentSpanId()).isEqualTo("00f067aa0ba902b7");
    } finally {
      plugin.stopService();
    }
  }

  @Test
  void httpCommandProducesQuerySpanNestedUnderTheHttpSpan() throws Exception {
    final ObservationRegistry registry = getServer(0).getObservationRegistry();
    final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    final TracingPlugin plugin = new TracingPlugin();
    plugin.attachForTest(registry, exporter);

    try {
      final HttpURLConnection c = (HttpURLConnection) URI.create("http://localhost:2480/api/v1/command/graph").toURL().openConnection();
      c.setRequestMethod("POST");
      c.setDoOutput(true);
      c.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      c.setRequestProperty("Content-Type", "application/json");
      c.getOutputStream().write("{\"language\":\"sql\",\"command\":\"SELECT 1 AS one\"}".getBytes(StandardCharsets.UTF_8));
      c.connect();
      assertThat(c.getResponseCode()).isEqualTo(200);
      c.disconnect();

      // The engine-boundary QueryTracer must produce an arcadedb.query span tagged with the
      // originating protocol (http here), nested under the HTTP request span.
      SpanData querySpan = null;
      SpanData httpSpan = null;
      for (int attempt = 0; attempt < 100 && querySpan == null; attempt++) {
        final List<SpanData> spans = exporter.getFinishedSpanItems();
        querySpan = spans.stream().filter(s -> "arcadedb.query".equals(s.getName())).findFirst().orElse(null);
        httpSpan = spans.stream().filter(s -> "arcadedb.http.server.requests".equals(s.getName())).findFirst().orElse(null);
        if (querySpan == null)
          Thread.sleep(20);
      }

      assertThat(querySpan).as("an arcadedb.query span must be produced for the command").isNotNull();
      // The span must be tagged with the originating wire protocol.
      assertThat(querySpan.getAttributes().get(AttributeKey.stringKey("protocol"))).isEqualTo("http");
      assertThat(httpSpan).as("the enclosing HTTP span must be present").isNotNull();
      // In-process nesting: the query span is a child of the HTTP request span (same trace).
      assertThat(querySpan.getTraceId()).isEqualTo(httpSpan.getTraceId());
      assertThat(querySpan.getParentSpanId()).isEqualTo(httpSpan.getSpanId());
    } finally {
      plugin.stopService();
    }
  }
}
