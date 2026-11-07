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

package com.arcadedb.containers.ha.chaos;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class WorkloadTest {
  private final AtomicInteger status   = new AtomicInteger(200);
  private final AtomicInteger delayMs  = new AtomicInteger();
  private final AtomicInteger requests = new AtomicInteger();
  private final List<String>  bodies   = new CopyOnWriteArrayList<>();
  private       HttpServer    server;

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.setExecutor(Executors.newCachedThreadPool());
    server.createContext("/", exchange -> {
      bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
      requests.incrementAndGet();
      try {
        Thread.sleep(delayMs.get());
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      exchange.sendResponseHeaders(status.get(), -1);
      exchange.close();
    });
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  private static Endpoints single(final int port) {
    final Endpoint endpoint = new Endpoint("127.0.0.1", port);
    return new Endpoints() {
      @Override
      public int size() {
        return 1;
      }

      @Override
      public Endpoint endpoint(final int node) {
        return endpoint;
      }
    };
  }

  private Workload workload(final Ledger ledger, final Endpoints endpoints, final int readTimeoutMs) {
    final ChaosConfig config = ChaosConfig.fromProperties(ChaosConfigTest.props("chaos.seed", "1", "chaos.writers", "2"));
    return new Workload(config, ledger, endpoints, ChaosSchema.DATABASE, 1_000, readTimeoutMs, 1);
  }

  @Test
  void successfulWritesAreAcked() {
    final Ledger ledger = new Ledger(2);
    try (final Workload workload = workload(ledger, single(server.getAddress().getPort()), 2_000)) {
      workload.start();
      await().atMost(Duration.ofSeconds(10)).until(() -> ledger.count(Ledger.ACKED) >= 50);
    }
    assertThat(ledger.count(Ledger.IN_FLIGHT)).isZero();
    assertThat(ledger.count(Ledger.UNKNOWN) + ledger.count(Ledger.FAILED)).isZero();
  }

  @Test
  void serverErrorsAreUnknown() {
    status.set(503);
    final Ledger ledger = new Ledger(2);
    try (final Workload workload = workload(ledger, single(server.getAddress().getPort()), 2_000)) {
      workload.start();
      await().atMost(Duration.ofSeconds(10)).until(() -> ledger.count(Ledger.UNKNOWN) >= 20);
    }
    assertThat(ledger.count(Ledger.ACKED)).isZero();
  }

  @Test
  void authenticationRejectionIsFailed() {
    status.set(401);
    final Ledger ledger = new Ledger(2);
    try (final Workload workload = workload(ledger, single(server.getAddress().getPort()), 2_000)) {
      workload.start();
      await().atMost(Duration.ofSeconds(10)).until(() -> ledger.count(Ledger.FAILED) >= 20);
    }
  }

  @Test
  void refusedConnectionIsFailed() throws IOException {
    final int closedPort;
    try (final ServerSocket socket = new ServerSocket(0)) {
      closedPort = socket.getLocalPort();
    }
    final Ledger ledger = new Ledger(2);
    try (final Workload workload = workload(ledger, single(closedPort), 2_000)) {
      workload.start();
      await().atMost(Duration.ofSeconds(10)).until(() -> ledger.count(Ledger.FAILED) >= 20);
    }
    assertThat(ledger.count(Ledger.UNKNOWN)).isZero();
  }

  @Test
  void readTimeoutIsUnknown() {
    delayMs.set(1_000);
    final Ledger ledger = new Ledger(2);
    try (final Workload workload = workload(ledger, single(server.getAddress().getPort()), 200)) {
      workload.start();
      await().atMost(Duration.ofSeconds(10)).until(() -> ledger.count(Ledger.UNKNOWN) >= 3);
    }
    assertThat(ledger.count(Ledger.ACKED)).isZero();
  }

  @Test
  void quiesceStopsTrafficUntilResume() throws InterruptedException {
    final Ledger ledger = new Ledger(2);
    try (final Workload workload = workload(ledger, single(server.getAddress().getPort()), 2_000)) {
      workload.start();
      await().atMost(Duration.ofSeconds(10)).until(() -> requests.get() >= 10);
      workload.quiesce();
      final int frozen = requests.get();
      assertThat(ledger.count(Ledger.IN_FLIGHT)).isZero();
      Thread.sleep(500);
      assertThat(requests.get()).isEqualTo(frozen);
      workload.resume();
      await().atMost(Duration.ofSeconds(10)).until(() -> requests.get() > frozen);
    }
  }

  @Test
  void payloadsCarryTheSchemaStatementsAndParameters() {
    final Ledger ledger = new Ledger(2);
    try (final Workload workload = workload(ledger, single(server.getAddress().getPort()), 2_000)) {
      workload.start();
      await().atMost(Duration.ofSeconds(20)).until(() -> ledger.count(Ledger.ACKED) >= 200);
    }
    assertThat(bodies).anySatisfy(body -> assertThat(body).contains("\"language\":\"sql\"").contains("INSERT INTO ChaosOp"));
    assertThat(bodies).anySatisfy(body -> assertThat(body).contains("\"language\":\"sqlscript\"").contains("\"target\""));
  }

  /** Node 0 is the live server, node 1 a second server that rejects every write (FAILED), held bound so no port is ever guessed. */
  private Endpoints liveAndRejecting(final HttpServer rejecting) {
    final Endpoint live = new Endpoint("127.0.0.1", server.getAddress().getPort());
    final Endpoint other = new Endpoint("127.0.0.1", rejecting.getAddress().getPort());
    return new Endpoints() {
      @Override
      public int size() {
        return 2;
      }

      @Override
      public Endpoint endpoint(final int node) {
        return node == 0 ? live : other;
      }
    };
  }

  private static HttpServer startRejectingServer() throws IOException {
    final HttpServer rejecting = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    rejecting.createContext("/", exchange -> {
      exchange.getRequestBody().readAllBytes();
      exchange.sendResponseHeaders(401, -1);
      exchange.close();
    });
    rejecting.start();
    return rejecting;
  }

  private static long countOutcome(final Ledger ledger, final int writer, final byte outcome) {
    long count = 0;
    for (int seq = 0; seq < ledger.size(writer); seq++)
      if (ledger.outcome(Ledger.key(writer, seq)) == outcome)
        ++count;
    return count;
  }

  @Test
  void routedAroundNodeIsNotWrittenToExceptByTheBlindWriter() throws IOException {
    final Ledger ledger = new Ledger(2);
    final HttpServer rejecting = startRejectingServer();
    try (final Workload workload = workload(ledger, liveAndRejecting(rejecting), 2_000)) {
      workload.routeAround(1L << 1);
      workload.start();
      await().atMost(Duration.ofSeconds(20)).until(() -> countOutcome(ledger, 1, Ledger.FAILED) >= 5);
    } finally {
      rejecting.stop(0);
    }
    assertThat(countOutcome(ledger, 0, Ledger.FAILED)).as("writer 0 must avoid the routed-around node").isZero();
    assertThat(countOutcome(ledger, 0, Ledger.ACKED)).isPositive();
    assertThat(countOutcome(ledger, 1, Ledger.FAILED)).as("the last writer stays blind to keep the frozen-node coverage").isPositive();
  }

  @Test
  void routingAroundEveryNodeFallsBackToAnyNode() throws IOException {
    final Ledger ledger = new Ledger(2);
    final HttpServer rejecting = startRejectingServer();
    try (final Workload workload = workload(ledger, liveAndRejecting(rejecting), 2_000)) {
      workload.routeAround(0b11);
      workload.start();
      await().atMost(Duration.ofSeconds(20)).until(() -> ledger.count(Ledger.ACKED) >= 5);
    } finally {
      rejecting.stop(0);
    }
  }

  @Test
  void clearingTheRouteLetsEveryWriterReachTheNodeAgain() throws IOException {
    final Ledger ledger = new Ledger(2);
    final HttpServer rejecting = startRejectingServer();
    try (final Workload workload = workload(ledger, liveAndRejecting(rejecting), 2_000)) {
      workload.routeAround(1L << 1);
      workload.start();
      await().atMost(Duration.ofSeconds(20)).until(() -> ledger.count(Ledger.ACKED) >= 20);
      workload.routeAround(0);
      await().atMost(Duration.ofSeconds(20)).until(() -> countOutcome(ledger, 0, Ledger.FAILED) >= 1);
    } finally {
      rejecting.stop(0);
    }
  }

  @Test
  void outcomeClassification() {
    assertThat(OpOutcome.fromStatus(200)).isEqualTo(Ledger.ACKED);
    assertThat(OpOutcome.fromStatus(204)).isEqualTo(Ledger.ACKED);
    assertThat(OpOutcome.fromStatus(401)).isEqualTo(Ledger.FAILED);
    assertThat(OpOutcome.fromStatus(403)).isEqualTo(Ledger.FAILED);
    for (final int code : new int[] { 400, 409, 500, 503 })
      assertThat(OpOutcome.fromStatus(code)).isEqualTo(Ledger.UNKNOWN);
    assertThat(OpOutcome.fromException(new ChaosHttp.NotSentException(new ConnectException("refused")))).isEqualTo(Ledger.FAILED);
    assertThat(OpOutcome.fromException(new SocketTimeoutException("read"))).isEqualTo(Ledger.UNKNOWN);
  }
}
