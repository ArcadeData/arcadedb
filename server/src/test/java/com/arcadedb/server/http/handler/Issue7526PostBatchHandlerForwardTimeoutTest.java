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
package com.arcadedb.server.http.handler;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.StallAwareStopwatch;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issues #7526/#7542: {@link PostBatchHandler}'s {@code forwardBatchToLeader} dialled the
 * leader with a bare {@code HttpClient.newHttpClient()} and built the request with no {@code .timeout(...)},
 * so a leader that accepted the connection and never answered parked the Undertow worker thread serving
 * {@code POST /api/v1/batch} until the OS tore the socket down.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7526PostBatchHandlerForwardTimeoutTest {

  /** The tripwire between "the deadline fired" and "the call is unbounded" (minutes, without the fix). */
  private static final long GAVE_UP_BOUND_MS = 30_000L;

  private static PostBatchHandler handlerWith(final ContextConfiguration cfg) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(cfg);
    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);
    return new PostBatchHandler(httpServer);
  }

  private static HAServerPlugin haPointingAt(final String leaderAddress) {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getLeaderAddress()).thenReturn(leaderAddress);
    when(ha.getClusterToken()).thenReturn("test-token");
    return ha;
  }

  private static ServerSecurityUser rootUser() {
    final ServerSecurityUser user = mock(ServerSecurityUser.class);
    when(user.getName()).thenReturn("root");
    return user;
  }

  private static PostBatchHandler.CountingInputStream emptyBody() {
    return new PostBatchHandler.CountingInputStream(new HttpServerExchange(null),
        new ByteArrayInputStream(new byte[0]));
  }

  @Test
  void aLeaderThatAcceptsAndNeverAnswersIsGivenUp504() throws Exception {
    try (final StalledLeader leader = new StalledLeader()) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, 1_000L);

      final PostBatchHandler handler = handlerWith(cfg);
      final HttpServerExchange exchange = new HttpServerExchange(null);

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final ExecutionResponse response = handler.forwardBatchToLeader(exchange, haPointingAt(leader.address()),
          "mydb", rootUser(), "application/x-ndjson", emptyBody(), false);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS,
          "a 1s forward deadline from the unbounded wait a leader that never answers used to produce");

      assertThat(response.getCode()).isEqualTo(504);
      assertThat(new JSONObject(response.getResponse()).getString("error")).contains(leader.address());
      assertThat(leader.acceptedConnections()).isGreaterThanOrEqualTo(1);
    }
  }

  @Test
  void aLeaderThatCannotBeConnectedToIsGivenUp504() throws Exception {
    final String unreachable;
    try (final ServerSocket probe = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      unreachable = probe.getInetAddress().getHostAddress() + ":" + probe.getLocalPort();
    }

    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 2_000L);
    cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, 60_000L);

    final PostBatchHandler handler = handlerWith(cfg);
    final HttpServerExchange exchange = new HttpServerExchange(null);

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final ExecutionResponse response = handler.forwardBatchToLeader(exchange, haPointingAt(unreachable),
        "mydb", rootUser(), "application/x-ndjson", emptyBody(), false);
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "the configured connect timeout, not an OS-level connect refusal delay");

    assertThat(response.getCode()).isEqualTo(504);
    assertThat(new JSONObject(response.getResponse()).getString("error")).contains(unreachable);
  }

  @Test
  void theClientCarriesTheConfiguredConnectTimeout() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 2_500L);

    final PostBatchHandler handler = handlerWith(cfg);

    final Field field = PostBatchHandler.class.getDeclaredField("httpClient");
    field.setAccessible(true);
    final HttpClient client = (HttpClient) field.get(handler);

    assertThat(client.connectTimeout()).contains(Duration.ofMillis(2_500L));
  }

  /**
   * {@link PostBatchHandler#buildForwardRequest} carries the deadline all the way to the built request when
   * one is given, and none when it is not - the shape the production forward and the pre-existing request-shape
   * tests respectively rely on.
   */
  @Test
  void buildForwardRequestCarriesTheGivenTimeout() {
    final HttpRequest withTimeout = PostBatchHandler.buildForwardRequest(
        "http://leader:2480/api/v1/batch/db", "application/x-ndjson", "token", "root", -1,
        new ByteArrayInputStream("x".getBytes(StandardCharsets.UTF_8)), null, Duration.ofMillis(1234L));
    assertThat(withTimeout.timeout()).contains(Duration.ofMillis(1234L));

    final HttpRequest withoutTimeout = PostBatchHandler.buildForwardRequest(
        "http://leader:2480/api/v1/batch/db", "application/x-ndjson", "token", "root", -1,
        new ByteArrayInputStream("x".getBytes(StandardCharsets.UTF_8)));
    assertThat(withoutTimeout.timeout()).isEmpty();
  }

  /** A server socket that accepts connections and answers nothing at all. */
  private static final class StalledLeader implements AutoCloseable {
    private final ServerSocket   serverSocket;
    private final Thread         acceptor;
    private final CountDownLatch started = new CountDownLatch(1);
    private volatile int         accepted = 0;

    StalledLeader() throws IOException, InterruptedException {
      serverSocket = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
      acceptor = new Thread(() -> {
        started.countDown();
        while (!serverSocket.isClosed()) {
          try {
            final Socket socket = serverSocket.accept();
            accepted++;
            // never answers: the socket stays open until close() tears it down
          } catch (final IOException e) {
            return;
          }
        }
      }, "issue7526-stalled-leader");
      acceptor.setDaemon(true);
      acceptor.start();
      started.await(10, TimeUnit.SECONDS);
    }

    String address() {
      return serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort();
    }

    int acceptedConnections() {
      return accepted;
    }

    @Override
    public void close() {
      try {
        serverSocket.close();
      } catch (final IOException ignored) {
        // best effort
      }
    }
  }
}
