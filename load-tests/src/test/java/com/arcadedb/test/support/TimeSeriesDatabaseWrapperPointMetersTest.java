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
package com.arcadedb.test.support;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11654: the time series ingestion meters count points on every protocol. A line protocol call carries up to 500 points, so
 * a meter moved once per call reads a lost batch of 500 points as 1.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesDatabaseWrapperPointMetersTest {

  private static final int POINTS = 1_200; // two full batches of 500 and one of 200

  private static final int DROPPED_PER_CALL = 10;

  private final AtomicInteger       writeStatus   = new AtomicInteger(204);
  private final AtomicInteger       writeRequests = new AtomicInteger();
  private final AtomicInteger       linesReceived = new AtomicInteger();
  private       HttpServer          stub;
  private       SimpleMeterRegistry registry;

  @BeforeEach
  void startStub() throws IOException {
    registry = new SimpleMeterRegistry();
    Metrics.addRegistry(registry);

    // The OS picks the port and the wrapper is given the one bound, so no other build on the machine can answer instead
    stub = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    // A standalone server: the client then talks to it directly instead of looking for a cluster
    stub.createContext("/api/v1/server", exchange -> answer(exchange, 200, "{}"));
    stub.createContext("/api/v1/ts/", exchange -> {
      final String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
      final int lines = (int) body.lines().count();
      writeRequests.incrementAndGet();
      linesReceived.addAndGet(lines);
      final int status = writeStatus.get();
      if (status == 204)
        answer(exchange, 204, null);
      else if (status == 400) {
        // The server's partial-write answer: the samples it wrote are committed, the others are named as dropped
        final int dropped = Math.min(DROPPED_PER_CALL, lines);
        answer(exchange, 400,
            "{\"error\":\"partial write: unable to parse " + dropped + " line(s)\",\"written\":" + (lines - dropped) + ",\"dropped\":"
                + dropped + "}");
      } else
        answer(exchange, status, "{\"error\":\"refused by the stub\"}");
    });
    stub.start();
  }

  @AfterEach
  void stopStub() {
    if (stub != null)
      stub.stop(0);
    Metrics.removeRegistry(registry);
    registry.close();
  }

  @Test
  void aRefusedLineProtocolBatchCountsEveryPointItLost() {
    // Only 503 is retried, so each of the 3 calls fails at once and loses all of its points
    writeStatus.set(500);

    try (final TimeSeriesDatabaseWrapper wrapper = wrapper()) {
      wrapper.ingestSeries("sensor-0", "region-0", 1_000L, POINTS);
    }

    assertThat(writeRequests.get()).as("line protocol calls the stub answered").isEqualTo(3);
    assertThat(linesReceived.get()).isEqualTo(POINTS);
    assertThat(counter("arcadedb.test.ts.inserted.points.error")).isEqualTo(POINTS);
    assertThat(counter("arcadedb.test.ts.inserted.points")).isZero();
    assertThat(requestTimer().count()).isEqualTo(3);
  }

  @Test
  void anAcceptedLineProtocolBatchCountsEveryPointItInserted() {
    try (final TimeSeriesDatabaseWrapper wrapper = wrapper()) {
      wrapper.ingestSeries("sensor-0", "region-0", 1_000L, POINTS);
      wrapper.ingestVerificationSeries();
    }

    // 3 batches for the series, then the 4 verification points one call each
    assertThat(writeRequests.get()).isEqualTo(3 + 4);
    assertThat(counter("arcadedb.test.ts.inserted.points")).isEqualTo(POINTS + 4);
    assertThat(counter("arcadedb.test.ts.inserted.points.error")).isZero();
    assertThat(requestTimer().count()).isEqualTo(3 + 4);
  }

  @Test
  void aPartialWriteCountsWhatTheServerWroteAsInsertedAndOnlyTheRestAsLost() {
    writeStatus.set(400);

    try (final TimeSeriesDatabaseWrapper wrapper = wrapper()) {
      wrapper.ingestSeries("sensor-0", "region-0", 1_000L, POINTS);
    }

    assertThat(writeRequests.get()).as("a partial write is final, not retried").isEqualTo(3);
    assertThat(counter("arcadedb.test.ts.inserted.points.error")).isEqualTo(3 * DROPPED_PER_CALL);
    assertThat(counter("arcadedb.test.ts.inserted.points")).isEqualTo(POINTS - 3 * DROPPED_PER_CALL);
  }

  @Test
  void theWrittenCountOfAPartialWriteIsReadFromTheAnswerAndKeptWithinTheBatch() {
    assertThat(TimeSeriesDatabaseWrapper.writtenInPartialWrite("{\"written\":490,\"dropped\":10}", 500)).isEqualTo(490);
    assertThat(TimeSeriesDatabaseWrapper.writtenInPartialWrite("{\"written\":900}", 500)).isEqualTo(500);
    assertThat(TimeSeriesDatabaseWrapper.writtenInPartialWrite("{\"written\":-1}", 500)).isZero();
    // A 400 that refused the request before writing anything carries no report
    assertThat(TimeSeriesDatabaseWrapper.writtenInPartialWrite("{ \"error\" : \"Request body is empty\"}", 500)).isZero();
    assertThat(TimeSeriesDatabaseWrapper.writtenInPartialWrite("", 500)).isZero();
    assertThat(TimeSeriesDatabaseWrapper.writtenInPartialWrite(null, 500)).isZero();
    assertThat(TimeSeriesDatabaseWrapper.writtenInPartialWrite("<html>Bad Request</html>", 500)).isZero();
  }

  private TimeSeriesDatabaseWrapper wrapper() {
    return new TimeSeriesDatabaseWrapper(new ServerWrapper("127.0.0.1", stub.getAddress().getPort(), -1),
        TimeSeriesDatabaseWrapper.Protocol.LINE_PROTOCOL);
  }

  private double counter(final String name) {
    return registry.find(name).counters().stream().mapToDouble(Counter::count).sum();
  }

  private Timer requestTimer() {
    final Timer timer = registry.find("arcadedb.test.ts.ingest.requests").timer();
    assertThat(timer).as("the ingest request timer is registered").isNotNull();
    return timer;
  }

  private static void answer(final HttpExchange exchange, final int status, final String body) throws IOException {
    try (exchange) {
      if (body == null) {
        exchange.sendResponseHeaders(status, -1);
        return;
      }
      final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(status, bytes.length);
      try (final OutputStream os = exchange.getResponseBody()) {
        os.write(bytes);
      }
    }
  }
}
