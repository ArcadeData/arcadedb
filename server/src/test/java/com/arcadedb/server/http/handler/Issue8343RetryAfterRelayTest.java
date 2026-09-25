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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.PostBatchHandler.CountingInputStream;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8343: a follower that forwarded a request to the leader relayed only the leader's status and body, so the
 * {@code Retry-After} of the leader's {@code 409} (a retry whose {@code X-Request-Id} twin is still executing, issue
 * #8324) and of its {@code 503} (a snapshot install) never reached a client that talked to a follower.
 * <p>
 * The leader is a real loopback HTTP listener, so the headers asserted are the ones that crossed a socket.
 */
class Issue8343RetryAfterRelayTest {

  private StubLeader leader;

  @BeforeEach
  void startLeader() throws IOException {
    leader = new StubLeader();
  }

  @AfterEach
  void stopLeader() {
    leader.close();
    LeaderForwardContext.clear();
  }

  /** The reported path: the leader's in-flight 409 relayed by the buffered forward of an admin command. */
  @Test
  void theLeadersInFlightConflictReachesTheClientWithItsRetryAfter() throws Exception {
    leader.answer(409, "{\"error\":\"A request with the same X-Request-Id is still executing\"}",
        Map.of("Retry-After", "5"));
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config()));

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
        "/api/v1/server", "{\"command\":\"restore database x\"}", true);

    assertThat(response.getCode()).isEqualTo(409);
    assertThat(response.getHeader("Retry-After"))
        .as("the machine-readable back-off a client talking to the leader gets")
        .isEqualTo("5");
  }

  /** The leader's snapshot-install 503, the other answer that carries a Retry-After. */
  @Test
  void theLeadersServiceUnavailableReachesTheClientWithItsRetryAfter() throws Exception {
    leader.answer(503, "{\"error\":\"Server is installing a snapshot, please retry\"}", Map.of("Retry-After", "5"));
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config()));

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server/users"), user("root"),
        "/api/v1/server/users", "{\"name\":\"bob\"}");

    assertThat(response.getCode()).isEqualTo(503);
    assertThat(response.getHeader("Retry-After")).isEqualTo("5");
  }

  /**
   * A client that asked for a progress stream and got a refusal instead: the non-stream branch of the streaming relay
   * reads the answer whole, and must relay its Retry-After exactly as the buffered forward does.
   */
  @Test
  void aRefusalAnsweredToAStreamRequestAlsoKeepsItsRetryAfter() throws Exception {
    leader.answer(409, "{\"error\":\"A request with the same X-Request-Id is still executing\"}",
        Map.of("Retry-After", "7"));
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config()));
    forwarder.streamTargetFactory = exchange -> contentType -> {
      throw new AssertionError("no stream may be started for an answer that is not one");
    };
    final HttpServerExchange exchange = exchange("/api/v1/server");
    exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server",
        "{\"command\":\"restore database x\"}", true, true);

    assertThat(response.getCode()).isEqualTo(409);
    assertThat(response.getHeader("Retry-After")).isEqualTo("7");
  }

  /**
   * An allow-list, not a copy: the leader's other response headers describe the leader's own exchange and must not be
   * passed off as this node's.
   */
  @Test
  void onlyTheAllowListedHeadersCrossTheHop() throws Exception {
    final Map<String, String> headers = new LinkedHashMap<>();
    headers.put("Set-Cookie", "leader-session=abc");
    headers.put("X-Request-Id", "the-leaders-own-correlation-id");
    headers.put("X-Custom", "leader-only");
    leader.answer(200, "{\"result\":\"ok\"}", headers);
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config()));

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
        "/api/v1/server", "{}");

    assertThat(response.getCode()).isEqualTo(200);
    assertThat(response.getHeader("Retry-After")).isNull();
    assertThat(response.getHeader("Set-Cookie")).isNull();
    assertThat(response.getHeader("X-Request-Id")).isNull();
    assertThat(response.getHeader("X-Custom")).isNull();
  }

  /** The batch relay, the other follower-to-leader forward in this module, shares the same allow-list. */
  @Test
  void theBatchRelayKeepsTheLeadersRetryAfterToo() throws Exception {
    leader.answer(503, "{\"error\":\"Server is installing a snapshot, please retry\"}", Map.of("Retry-After", "5"));
    final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha(), config()));

    final ExecutionResponse response = handler.forwardBatchToLeader(exchange("/api/v1/batch/graph"), ha(), "graph",
        user("root"), "application/json", body("{\"@type\":\"vertex\"}\n"), false);

    assertThat(response.getCode()).isEqualTo(503);
    assertThat(response.getHeader("Retry-After")).isEqualTo("5");
  }

  /** What makes the relayed header reach the client: {@code send} writes it onto the exchange's response headers. */
  @Test
  void theHeadersOfAResponseAreWrittenOntoTheExchange() {
    final ExecutionResponse response = new ExecutionResponse(409, "{}").setHeader("Retry-After", "5");
    final HeaderMap target = new HeaderMap();

    response.applyHeaders(target);

    assertThat(target.getFirst("Retry-After")).isEqualTo("5");
  }

  @Test
  void settingAHeaderTwiceKeepsTheLastValueAndAResponseWithoutHeadersWritesNone() {
    final ExecutionResponse response = new ExecutionResponse(409, "{}").setHeader("Retry-After", "5")
        .setHeader("retry-after", "9");
    assertThat(response.getHeader("RETRY-AFTER")).isEqualTo("9");

    final HeaderMap target = new HeaderMap();
    new ExecutionResponse(200, "{}").applyHeaders(target);
    assertThat(target.size()).isZero();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------------------------------------------

  private HAServerPlugin ha() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.isLeader()).thenReturn(false);
    when(ha.getLeaderAddress()).thenReturn(leader.address());
    when(ha.getLeaderName()).thenReturn("leader");
    when(ha.getLeaderPeerId()).thenReturn("peer-leader");
    when(ha.getLocalPeerId()).thenReturn("peer-follower");
    when(ha.getClusterToken()).thenReturn("issue8343-cluster-token");
    return ha;
  }

  private static ContextConfiguration config() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_READ_TIMEOUT, 30_000L);
    cfg.setValue(GlobalConfiguration.HA_PROXY_LONG_COMMAND_TIMEOUT, 30_000L);
    cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
    cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, 30_000L);
    return cfg;
  }

  private static HttpServer httpServerWith(final HAServerPlugin ha, final ContextConfiguration configuration) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getHA()).thenReturn(ha);
    when(server.getConfiguration()).thenReturn(configuration);
    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);
    return httpServer;
  }

  private static HttpServerExchange exchange(final String path) {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    exchange.setRequestPath(path);
    exchange.setRequestURI(path);
    exchange.setRequestMethod(Methods.POST);
    exchange.getRequestHeaders().put(new HttpString("Authorization"), "Basic cm9vdDpwbGF5d2l0aGRhdGE=");
    return exchange;
  }

  private static CountingInputStream body(final String payload) {
    return new CountingInputStream(new HttpServerExchange(null),
        new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8)));
  }

  private static ServerSecurityUser user(final String name) {
    final ServerSecurityUser user = mock(ServerSecurityUser.class);
    when(user.getName()).thenReturn(name);
    return user;
  }

  /** The leader: a loopback HTTP listener that answers every request with the status, body and headers set last. */
  private static final class StubLeader implements AutoCloseable {
    private final    com.sun.net.httpserver.HttpServer server;
    private final    ExecutorService                   executor = Executors.newCachedThreadPool();
    private volatile int                               status   = 200;
    private volatile String                            body     = "{}";
    private volatile Map<String, String>               headers  = Map.of();

    StubLeader() throws IOException {
      server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 16);
      server.setExecutor(executor);
      server.createContext("/", exchange -> {
        try {
          exchange.getRequestBody().readAllBytes();
          final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          headers.forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
          exchange.sendResponseHeaders(status, bytes.length);
          exchange.getResponseBody().write(bytes);
        } finally {
          exchange.close();
        }
      });
      server.start();
    }

    void answer(final int status, final String body, final Map<String, String> headers) {
      this.status = status;
      this.body = body;
      this.headers = headers;
    }

    String address() {
      return "127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
      server.stop(0);
      executor.shutdownNow();
    }
  }
}
