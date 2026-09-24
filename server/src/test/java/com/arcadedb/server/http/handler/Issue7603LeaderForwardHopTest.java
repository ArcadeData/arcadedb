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
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.IdempotencyCache;
import com.arcadedb.server.http.handler.PostBatchHandler.CountingInputStream;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.StallAwareStopwatch;
import com.sun.net.httpserver.HttpExchange;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7603: three gaps in {@link LeaderCommandForwarder}, all on what a forward carries across the hop and what
 * it tells the caller when the hop does not work out.
 * <ol>
 *   <li>{@code X-Request-Id} was not relayed, so the leader ran a forwarded command outside its idempotency cache
 *   while the follower promised the client it was inside one;</li>
 *   <li>{@code Accept} was not relayed and the answer was read with {@code BodyHandlers.ofString()}, so a restore's
 *   progress stream reached a client that asked a follower for it as one buffered object at the end;</li>
 *   <li>the one-hop refusal answered a routine leadership change with the non-retryable 400 meant for a
 *   misconfigured {@code arcadedb.ha.serverList}, and used up the warning latch that misconfiguration gets.</li>
 * </ol>
 * The leader is a real HTTP listener on the loopback interface, so the headers asserted are the ones that reached a
 * socket, and the stream is observed arriving event by event.
 */
class Issue7603LeaderForwardHopTest {

  private static final String LOCAL_PEER  = "peer-follower";
  private static final String LEADER_PEER = "peer-leader";

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

  // ---------------------------------------------------------------------------------------------------------------
  // 1. X-Request-Id
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void theRequestIdTravelsToTheLeaderSoItsIdempotencyCacheSeesTheCommand() throws Exception {
    leader.answerJson(200, "{\"result\":\"ok\"}");
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
    final HttpServerExchange exchange = exchange("/api/v1/server");
    exchange.getRequestHeaders().put(new HttpString(IdempotencyCache.HEADER_REQUEST_ID), "issue7603-request-abc");

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server",
        "{\"command\":\"create database Foo\"}", false);

    assertThat(response.getCode()).isEqualTo(200);
    assertThat(leader.header(IdempotencyCache.HEADER_REQUEST_ID))
        .as("the leader's idempotency gate keys on this header: without it the command runs there uncached")
        .isEqualTo("issue7603-request-abc");
  }

  @Test
  void aForwardWithNoRequestIdSendsNone() throws Exception {
    leader.answerJson(200, "{\"result\":\"ok\"}");
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));

    forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"), "/api/v1/server", "{}", false);

    assertThat(leader.header(IdempotencyCache.HEADER_REQUEST_ID)).isNull();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // 2. Accept, and the stream relayed as it arrives
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void theAcceptHeaderTravelsToTheLeader() throws Exception {
    leader.answerJson(200, "{\"result\":\"ok\"}");
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
    final HttpServerExchange exchange = exchange("/api/v1/server");
    exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

    forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server", "{}", true, false);

    assertThat(leader.header("Accept")).isEqualTo("text/event-stream");
  }

  /**
   * The reported defect, observed: the leader writes one progress event and then waits. With the old
   * {@code BodyHandlers.ofString()} relay the client saw nothing until the leader finished, so the first event
   * could never reach it while the leader was still holding the second one back.
   */
  @Test
  void aProgressStreamReachesTheClientEventByEventWhileTheLeaderIsStillRunning() throws Exception {
    final CountDownLatch releaseSecondEvent = new CountDownLatch(1);
    leader.answerStream(releaseSecondEvent, true);
    final RecordingTarget target = new RecordingTarget();
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
    forwarder.streamTargetFactory = exchange -> target;
    final HttpServerExchange exchange = exchange("/api/v1/server");
    exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

    final ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      final Future<ExecutionResponse> forward = caller.submit(() ->
          forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server", "{\"command\":\"restore database x\"}",
              true, true));

      await().atMost(20, TimeUnit.SECONDS).until(() -> target.flushed().contains("\"first\""));
      assertThat(target.flushed())
          .as("the second event is held back by the leader, so only a relay that streams can have shown the first")
          .doesNotContain("\"completed\"");
      assertThat(target.contentType).isEqualTo("text/event-stream");

      releaseSecondEvent.countDown();
      assertThat(forward.get(20, TimeUnit.SECONDS))
          .as("the stream is already on the wire, so the caller must answer nothing more")
          .isSameAs(LeaderCommandForwarder.STREAMED);
      assertThat(target.flushed()).contains("\"first\"").contains("\"completed\"");
      assertThat(target.closed).isTrue();
    } finally {
      releaseSecondEvent.countDown();
      caller.shutdownNow();
    }
  }

  /**
   * The bound issue #7507 put on every forward survives the move to a streaming relay: a leader that starts the
   * stream and then stops writing is given up on at the deadline, and - the status line being long gone - the client
   * is told so in an SSE error frame naming the setting.
   */
  @Test
  void aStreamThatStallsMidwayIsGivenUpOnAtTheDeadlineAndEndsWithAnErrorFrame() throws Exception {
    final CountDownLatch neverReleased = new CountDownLatch(1);
    leader.answerStream(neverReleased, true);
    final RecordingTarget target = new RecordingTarget();
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(1_000L)));
    forwarder.streamTargetFactory = exchange -> target;
    final HttpServerExchange exchange = exchange("/api/v1/server");
    exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

    try {
      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final ExecutionResponse response = forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server",
          "{\"command\":\"restore database x\"}", true, true);
      watch.assertGaveUpWithin(30_000L, "a 1s forward deadline from a relay parked on a stream the leader stopped writing");

      assertThat(response).isSameAs(LeaderCommandForwarder.STREAMED);
      assertThat(target.flushed()).contains("\"first\"");
      assertThat(target.flushed()).contains("\"status\":\"error\"")
          .contains(GlobalConfiguration.HA_PROXY_LONG_COMMAND_TIMEOUT.getKey());
      assertThat(target.closed).isTrue();
    } finally {
      neverReleased.countDown();
    }
  }

  /** A refusal issued before the operation started is an ordinary status-carrying answer, and stays one. */
  @Test
  void aLeaderThatAnswersAStreamRequestWithoutAStreamIsRelayedAsItsBufferedAnswer() throws Exception {
    leader.answerJson(400, "{\"error\":\"Database 'x' already exists\"}");
    final RecordingTarget target = new RecordingTarget();
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
    forwarder.streamTargetFactory = exchange -> target;
    final HttpServerExchange exchange = exchange("/api/v1/server");
    exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server",
        "{\"command\":\"restore database x\"}", true, true);

    assertThat(response.getCode()).isEqualTo(400);
    assertThat(response.getResponse()).contains("already exists");
    assertThat(target.contentType).as("no stream may be started for an answer that is not one").isNull();
  }

  /**
   * A leader whose buffered answer to a stream request breaks part-way - it declares a length and closes the
   * connection short of it - fails the forward as an I/O error, exactly as the {@code ofString()} relay did, rather
   * than relaying a truncated body as if it were whole.
   */
  @Test
  void aBufferedAnswerThatBreaksPartWayFailsTheForwardInsteadOfRelayingATruncatedBody() {
    leader.answerTruncatedJson();
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
    forwarder.streamTargetFactory = exchange -> contentType -> {
      throw new AssertionError("a JSON answer must not start a stream");
    };
    final HttpServerExchange exchange = exchange("/api/v1/server");
    exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

    assertThatThrownBy(() -> forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server",
        "{\"command\":\"restore database x\"}", true, true))
        .isInstanceOf(IOException.class);
  }

  /**
   * A route that cannot stream - the {@code /server/users} ones - never gets {@link LeaderCommandForwarder#STREAMED}
   * back, whatever its client's Accept header says: its handler would have nothing to return it as.
   */
  @Test
  void aRouteThatDidNotOptInIsNeverHandedTheStreamedSentinel() throws Exception {
    leader.answerStream(new CountDownLatch(0), true);
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
    forwarder.streamTargetFactory = exchange -> {
      throw new AssertionError("a route that did not opt in must not start a stream");
    };
    final HttpServerExchange exchange = exchange("/api/v1/server/users");
    exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server/users",
        "{}");

    assertThat(response).isNotSameAs(LeaderCommandForwarder.STREAMED);
    assertThat(response.getCode()).isEqualTo(200);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // 3. The one-hop refusal: leadership moved (503) vs an address that names the wrong node (400)
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void theForwardNamesTheLeaderItMeansToReach() throws Exception {
    leader.answerJson(200, "{}");
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));

    forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"), "/api/v1/server", "{}", false);

    assertThat(leader.header(LeaderForwardContext.FORWARDED_LEADER_ID_HEADER)).isEqualTo(LEADER_PEER);
    assertThat(leader.header(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER)).isEqualTo("true");
  }

  @Test
  void aForwardThatSawLeadershipChangeWhileResolvingNamesNoLeader() throws Exception {
    leader.answerJson(200, "{}");
    final HAServerPlugin ha = ha();
    // Read once before the address is resolved and once after: a change in between means the id may not be the
    // node the address names, so the only honest thing to send is nothing.
    when(ha.getLeaderPeerId()).thenReturn(LEADER_PEER, "peer-new-leader");
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha, config(30_000L)));

    forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"), "/api/v1/server", "{}", false);

    assertThat(leader.header(LeaderForwardContext.FORWARDED_LEADER_ID_HEADER)).isNull();
  }

  /**
   * The routine case the issue is about: the peer dialled this very node as the leader, and it stopped being the
   * leader while the request travelled. Retryable - the refusal names no leader, which the HTTP layer answers 503 -
   * and it must not use up the one warning the genuine misconfiguration gets.
   */
  @Test
  void aRefusalCausedByLeadershipMovingIsRetryableAndLeavesTheWarningForTheMisconfiguration() {
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
    LeaderForwardContext.markAlreadyForwarded(LOCAL_PEER);

    final ServerIsNotTheLeaderException refusal = catchThrowableOfType(ServerIsNotTheLeaderException.class,
        () -> forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"), "/api/v1/server", "{}", false));

    assertThat(refusal.getLeaderAddress())
        .as("a named leader is what AbstractServerHttpHandler answers 400; an unnamed one is the retryable 503")
        .isNull();
    assertThat(refusal.getMessage()).contains("leadership moved").contains("retry");
    assertThat(forwarder.misconfigurationWarned()).isFalse();
    assertThat(leader.requests()).as("a refused hop is never relayed on").isZero();
  }

  @Test
  void aRefusalCausedByAnAddressThatNamesTheWrongNodeIsTheConfigurationError() {
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
    LeaderForwardContext.markAlreadyForwarded(LEADER_PEER);

    final ServerIsNotTheLeaderException refusal = catchThrowableOfType(ServerIsNotTheLeaderException.class,
        () -> forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"), "/api/v1/server", "{}", false));

    assertThat(refusal.getLeaderAddress()).isEqualTo("leader");
    assertThat(refusal.getMessage()).contains("already forwarded").contains("does not identify")
        .contains(GlobalConfiguration.HA_SERVER_LIST.getKey()).doesNotContain("retry");
    assertThat(forwarder.misconfigurationWarned()).isTrue();
  }

  /** A peer that says nothing - an older node mid rolling upgrade - is answered exactly as before. */
  @Test
  void aRefusalWithNoIntendedLeaderIsAnsweredAsBefore() {
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
    LeaderForwardContext.markAlreadyForwarded();

    assertThatThrownBy(() -> forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"), "/api/v1/server",
        "{}", false))
        .isInstanceOf(ServerIsNotTheLeaderException.class)
        .hasMessageContaining("Either leadership moved")
        .satisfies(e -> assertThat(((ServerIsNotTheLeaderException) e).getLeaderAddress()).isEqualTo("leader"));
    assertThat(forwarder.misconfigurationWarned()).isTrue();
  }

  /** The same split on the batch relay, the other follower-to-leader forward in this module. */
  @Test
  void theBatchRelayAnswersALeadershipChangeWith503AndAWrongAddressWith400() throws Exception {
    final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha(), config(30_000L)));

    LeaderForwardContext.markAlreadyForwarded(LOCAL_PEER);
    final ExecutionResponse moved = handler.forwardBatchToLeader(exchange("/api/v1/batch/graph"), ha(), "graph",
        user("root"), "application/json", body("{\"@type\":\"vertex\"}\n"), false);
    assertThat(moved.getCode()).isEqualTo(503);
    assertThat(moved.getResponse()).contains("leadership moved").contains("retry");

    LeaderForwardContext.markAlreadyForwarded(LEADER_PEER);
    final ExecutionResponse misidentified = handler.forwardBatchToLeader(exchange("/api/v1/batch/graph"), ha(), "graph",
        user("root"), "application/json", body("{\"@type\":\"vertex\"}\n"), false);
    assertThat(misidentified.getCode()).isEqualTo(400);
    assertThat(misidentified.getResponse()).contains("does not identify");
    assertThat(leader.requests()).isZero();
  }

  @Test
  void theBatchRelayNamesTheLeaderItMeansToReach() {
    final HttpRequest request = PostBatchHandler.buildForwardRequest("http://leader:2480/api/v1/batch/graph",
        "application/json", "token", "root", 3, new ByteArrayInputStream(new byte[3]), null, Duration.ofSeconds(5),
        LEADER_PEER);

    assertThat(request.headers().firstValue(LeaderForwardContext.FORWARDED_LEADER_ID_HEADER)).contains(LEADER_PEER);
    assertThat(PostBatchHandler.buildForwardRequest("http://leader:2480/api/v1/batch/graph", "application/json",
        "token", "root", 3, new ByteArrayInputStream(new byte[3]), null, Duration.ofSeconds(5)).headers()
        .firstValue(LeaderForwardContext.FORWARDED_LEADER_ID_HEADER)).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------------------------------------------

  private HAServerPlugin ha() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.isLeader()).thenReturn(false);
    when(ha.getLeaderAddress()).thenReturn(leader.address());
    when(ha.getLeaderName()).thenReturn("leader");
    when(ha.getLeaderPeerId()).thenReturn(LEADER_PEER);
    when(ha.getLocalPeerId()).thenReturn(LOCAL_PEER);
    when(ha.getClusterToken()).thenReturn("issue7603-cluster-token");
    return ha;
  }

  private static ContextConfiguration config(final long longCommandTimeoutMs) {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_READ_TIMEOUT, 30_000L);
    cfg.setValue(GlobalConfiguration.HA_PROXY_LONG_COMMAND_TIMEOUT, longCommandTimeoutMs);
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

  /** Records what the relay wrote and whether it flushed it, the way a client socket would see it. */
  private static final class RecordingTarget implements LeaderCommandForwarder.StreamTarget {
    private final    ByteArrayOutputStream written = new ByteArrayOutputStream();
    private volatile String                flushed = "";
    private volatile String                contentType;
    private volatile boolean               closed;

    @Override
    public OutputStream open(final String contentType) {
      this.contentType = contentType;
      return new OutputStream() {
        @Override
        public void write(final int b) {
          synchronized (written) {
            written.write(b);
          }
        }

        @Override
        public void write(final byte[] b, final int off, final int len) {
          synchronized (written) {
            written.write(b, off, len);
          }
        }

        @Override
        public void flush() {
          synchronized (written) {
            flushed = written.toString(StandardCharsets.UTF_8);
          }
        }

        @Override
        public void close() {
          flush();
          closed = true;
        }
      };
    }

    String flushed() {
      return flushed;
    }
  }

  /** The leader: a loopback HTTP listener that records the request headers and answers as a test tells it to. */
  private static final class StubLeader implements AutoCloseable {
    private final com.sun.net.httpserver.HttpServer server;
    private final Map<String, String>              headers  = new ConcurrentHashMap<>();
    private final ExecutorService                  executor = Executors.newCachedThreadPool();
    private volatile int                           requests;
    private volatile Answer                        answer;

    private interface Answer {
      void write(HttpExchange exchange) throws Exception;
    }

    StubLeader() throws IOException {
      server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 16);
      server.setExecutor(executor);
      server.createContext("/", exchange -> {
        requests++;
        exchange.getRequestHeaders().forEach((name, values) -> headers.put(name.toLowerCase(), values.getFirst()));
        exchange.getRequestBody().readAllBytes();
        try {
          answer.write(exchange);
        } catch (final Exception e) {
          // the forwarder gave up and closed the connection: nothing left to answer
        } finally {
          exchange.close();
        }
      });
      server.start();
    }

    void answerJson(final int status, final String json) {
      answer = exchange -> {
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
      };
    }

    /** Declares a hundred bytes of JSON, sends five, and closes the connection. */
    void answerTruncatedJson() {
      answer = exchange -> {
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, 100);
        exchange.getResponseBody().write("{\"res".getBytes(StandardCharsets.UTF_8));
        exchange.getResponseBody().flush();
        exchange.getHttpContext().getServer().stop(0);
      };
    }

    /** One progress event, then - once {@code release} opens - a completion event. */
    void answerStream(final CountDownLatch release, final boolean eventStream) {
      answer = exchange -> {
        exchange.getResponseHeaders().add("Content-Type", eventStream ? "text/event-stream" : "application/json");
        exchange.sendResponseHeaders(200, 0);
        final OutputStream out = exchange.getResponseBody();
        out.write(("data: " + new JSONObject().put("status", "progress").put("message", "first") + "\n\n")
            .getBytes(StandardCharsets.UTF_8));
        out.flush();
        release.await(60, TimeUnit.SECONDS);
        out.write(("data: " + new JSONObject().put("status", "completed").put("message", "done") + "\n\n")
            .getBytes(StandardCharsets.UTF_8));
        out.flush();
      };
    }

    String address() {
      return "127.0.0.1:" + server.getAddress().getPort();
    }

    String header(final String name) {
      return headers.get(name.toLowerCase());
    }

    int requests() {
      return requests;
    }

    @Override
    public void close() {
      server.stop(0);
      executor.shutdownNow();
    }
  }
}
