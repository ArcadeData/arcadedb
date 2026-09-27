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
import com.arcadedb.server.http.handler.PostBatchHandler.CountingInputStream;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.BlockingHandler;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8486: the two follower-to-leader forwarders of the {@code server} module - {@link LeaderCommandForwarder} for
 * server commands and the security REST routes, and {@link PostBatchHandler} for bulk loads - relayed the answer of a
 * leader that had just stepped down, "not the leader, and I cannot name one" (503), to the client at once. This node
 * still named that ex-leader, so a client retrying without back-off dialled it again through the same stale view, over
 * and over for a whole election timeout. {@code RaftReplicatedDatabase}'s forward of a SQL write holds the same refusal
 * back until its view moves (issue #8480); these two forwarders now do the same.
 * <p>
 * The leader is a real loopback HTTP listener. The follower's leader view is a counter-driven probe: it keeps naming
 * the ex-leader for a fixed number of reads after the refusal was answered, then names another node. Whether the
 * forwarder waited is read off that counter, never off the clock.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class Issue8486ForwardersHoldUnnamedLeaderRefusalTest {

  private static final String EX_LEADER  = "peer-leader";
  private static final String NEW_LEADER = "peer-new-leader";

  /** What the ex-leader's error pipeline answers for a {@code ServerIsNotTheLeaderException} with no leader address. */
  private static final String UNNAMED_REFUSAL = new JSONObject()
      .put("error", "Cannot execute command")
      .put("exception", ServerIsNotTheLeaderException.class.getName())
      .toString();

  private StubLeader leader;
  private LeaderView view;

  @BeforeEach
  void setUp() throws IOException {
    leader = new StubLeader();
    view = new LeaderView(leader);
  }

  @AfterEach
  void tearDown() {
    leader.close();
    LeaderForwardContext.clear();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // LeaderCommandForwarder
  // ---------------------------------------------------------------------------------------------------------------

  /** The reported path: a server command forwarded to an ex-leader that could name no successor. */
  @Test
  void aForwardedServerCommandHoldsTheExLeadersUnnamedRefusalUntilTheViewMoves() throws Exception {
    leader.answer(503, UNNAMED_REFUSAL);
    view.moveAfterReads(3);
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(20_000L)));

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
        "/api/v1/server", "{\"command\":\"create database x\"}");

    assertThat(response.getCode()).as("the refusal is still relayed - the client's retry is what gets past it")
        .isEqualTo(503);
    assertThat(view.moved()).as("the refusal was held until this node stopped naming the ex-leader").isTrue();
    assertThat(view.readsAfterRefusal()).isGreaterThanOrEqualTo(4);
  }

  /** The security REST routes share the forwarder, so a user creation forwarded to an ex-leader waits the same way. */
  @Test
  void aForwardedSecurityRouteHoldsTheRefusalToo() throws Exception {
    leader.answer(503, UNNAMED_REFUSAL);
    view.moveAfterReads(2);
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(20_000L)));

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server/users"), user("root"),
        "/api/v1/server/users", "{\"name\":\"bob\"}");

    assertThat(response.getCode()).isEqualTo(503);
    assertThat(view.moved()).isTrue();
  }

  /**
   * A client that asked for a progress stream and got the refusal instead: the non-stream branch of the streaming
   * relay reads the answer whole, and holds it exactly as the buffered forward does.
   */
  @Test
  void theRefusalAnsweredToAStreamRequestIsHeldToo() throws Exception {
    leader.answer(503, UNNAMED_REFUSAL);
    view.moveAfterReads(2);
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(20_000L)));
    forwarder.streamTargetFactory = exchange -> contentType -> {
      throw new AssertionError("no stream may be started for an answer that is not one");
    };
    final HttpServerExchange exchange = exchange("/api/v1/server");
    exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server",
        "{\"command\":\"restore database x\"}", true, true);

    assertThat(response.getCode()).isEqualTo(503);
    assertThat(view.moved()).isTrue();
  }

  /** Bounded: a view that never moves releases the refusal after {@code arcadedb.ha.forwardLeaderWaitTimeoutMs}. */
  @Test
  void aViewThatNeverMovesReleasesTheRefusalAfterTheConfiguredWait() throws Exception {
    leader.answer(503, UNNAMED_REFUSAL);
    view.neverMove();
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(300L)));

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
        "/api/v1/server", "{}");

    assertThat(response.getCode()).isEqualTo(503);
    assertThat(view.moved()).isFalse();
    assertThat(view.readsAfterRefusal()).as("it did poll the view before giving up").isGreaterThanOrEqualTo(2);
  }

  /** The setting at 0 restores the fail-fast relay, as it does for the leaderless wait it bounds. */
  @Test
  void aZeroWaitRelaysTheRefusalAtOnce() throws Exception {
    leader.answer(503, UNNAMED_REFUSAL);
    view.neverMove();
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(0L)));

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
        "/api/v1/server", "{}");

    assertThat(response.getCode()).isEqualTo(503);
    assertThat(view.readsAfterRefusal()).isZero();
  }

  /** A refusal that names the leader is 400 and tells the client where to go: nothing to wait for. */
  @Test
  void aRefusalThatNamesTheLeaderIsNotHeld() throws Exception {
    leader.answer(400, new JSONObject().put("error", "Cannot execute command")
        .put("exception", ServerIsNotTheLeaderException.class.getName()).put("exceptionArgs", "10.0.0.3:2480").toString());
    view.neverMove();
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(20_000L)));

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
        "/api/v1/server", "{}");

    assertThat(response.getCode()).isEqualTo(400);
    assertThat(view.readsAfterRefusal()).isZero();
  }

  /** Every other 503 - here the snapshot-install refusal - says nothing about who leads, and is relayed at once. */
  @Test
  void anotherServiceUnavailableIsNotHeld() throws Exception {
    leader.answer(503, "{\"error\":\"Server is installing a snapshot, please retry\"}");
    view.neverMove();
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(20_000L)));

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
        "/api/v1/server", "{}");

    assertThat(response.getCode()).isEqualTo(503);
    assertThat(view.readsAfterRefusal()).isZero();
  }

  /** Without a stable id for the node dialled, there is no "view still names it" to wait out. */
  @Test
  void aForwardWithoutAStableIntendedLeaderIsNotHeld() throws Exception {
    leader.answer(503, UNNAMED_REFUSAL);
    view.noLeaderId();
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(20_000L)));

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
        "/api/v1/server", "{}");

    assertThat(response.getCode()).isEqualTo(503);
    assertThat(view.readsAfterRefusal()).isZero();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // PostBatchHandler
  // ---------------------------------------------------------------------------------------------------------------

  /** The other reported path: a buffered batch forwarded to an ex-leader. */
  @Test
  void aForwardedBatchHoldsTheExLeadersUnnamedRefusalUntilTheViewMoves() throws Exception {
    leader.answer(503, UNNAMED_REFUSAL);
    view.moveAfterReads(3);
    final HAServerPlugin ha = ha();
    final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha, config(20_000L)));

    final ExecutionResponse response = handler.forwardBatchToLeader(exchange("/api/v1/batch/graph"), ha, "graph",
        user("root"), "application/json", body("{\"@type\":\"vertex\"}\n"), false);

    assertThat(response.getCode()).isEqualTo(503);
    assertThat(view.moved()).isTrue();
    assertThat(view.readsAfterRefusal()).isGreaterThanOrEqualTo(4);
  }

  /**
   * A client that negotiated the NDJSON stream and got the refusal: the stream relay's buffered fallback. That relay
   * arms its read deadline on the exchange's IO thread, so the follower is a real Undertow listener here.
   */
  @Test
  void aStreamingBatchHoldsTheRefusalToo() throws Exception {
    leader.answer(503, UNNAMED_REFUSAL);
    view.moveAfterReads(2);
    final HAServerPlugin ha = ha();
    final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha, config(20_000L)));
    final ServerSecurityUser user = user("root");
    final AtomicInteger relayedStatus = new AtomicInteger();

    final Undertow follower = Undertow.builder()
        .addHttpListener(0, "127.0.0.1")
        .setHandler(new BlockingHandler(exchange -> {
          final ExecutionResponse response = handler.forwardBatchToLeader(exchange, ha, "graph", user,
              "application/json", new CountingInputStream(exchange, exchange.getInputStream()), true);
          relayedStatus.set(response != null ? response.getCode() : 200);
          if (response != null) {
            exchange.setStatusCode(response.getCode());
            exchange.getOutputStream().write(response.getResponse().getBytes(StandardCharsets.UTF_8));
          }
        }))
        .build();
    follower.start();
    try {
      final int port = ((InetSocketAddress) follower.getListenerInfo().get(0).getAddress()).getPort();
      final HttpResponse<String> answer = HttpClient.newHttpClient().send(HttpRequest.newBuilder()
              .uri(URI.create("http://127.0.0.1:" + port + "/api/v1/batch/graph"))
              .header("Accept", "application/x-ndjson")
              .POST(HttpRequest.BodyPublishers.ofString("{\"@type\":\"vertex\"}\n")).build(),
          HttpResponse.BodyHandlers.ofString());

      assertThat(answer.statusCode()).isEqualTo(503);
      assertThat(relayedStatus.get()).isEqualTo(503);
      assertThat(view.moved()).isTrue();
    } finally {
      follower.stop();
    }
  }

  @Test
  void aBatchRefusalThatNamesTheLeaderIsNotHeld() throws Exception {
    leader.answer(400, new JSONObject().put("error", "Cannot execute command")
        .put("exception", ServerIsNotTheLeaderException.class.getName()).put("exceptionArgs", "10.0.0.3:2480").toString());
    view.neverMove();
    final HAServerPlugin ha = ha();
    final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha, config(20_000L)));

    final ExecutionResponse response = handler.forwardBatchToLeader(exchange("/api/v1/batch/graph"), ha, "graph",
        user("root"), "application/json", body("{\"@type\":\"vertex\"}\n"), false);

    assertThat(response.getCode()).isEqualTo(400);
    assertThat(view.readsAfterRefusal()).isZero();
  }

  /**
   * What an ex-leader answers a batch a peer forwarded to it while leadership moved (issue #7603's LEADERSHIP_MOVED)
   * used to be a bare 503 with no exception class, which no forwarder could tell from any other 503. It now carries
   * the same {@code ServerIsNotTheLeaderException}, with no leader address, that the server-command route and the SQL
   * forward answer, so the follower that relayed it recognizes the refusal and holds it.
   */
  @Test
  void theExLeadersOwnRefusalOfAForwardedBatchIsTheRecognizableUnnamedRefusal() throws Exception {
    final HAServerPlugin exLeader = mock(HAServerPlugin.class);
    when(exLeader.isLeader()).thenReturn(false);
    when(exLeader.getLocalPeerId()).thenReturn(EX_LEADER);
    when(exLeader.getLeaderName()).thenReturn(null);
    final PostBatchHandler handler = new PostBatchHandler(httpServerWith(exLeader, config(20_000L)));
    LeaderForwardContext.markAlreadyForwarded(EX_LEADER);

    final ExecutionResponse refusal = handler.forwardBatchToLeader(exchange("/api/v1/batch/graph"), exLeader, "graph",
        user("root"), "application/json", body("{\"@type\":\"vertex\"}\n"), false);

    assertThat(refusal.getCode()).isEqualTo(503);
    final JSONObject json = new JSONObject(refusal.getResponse());
    assertThat(json.getString("exception", null)).isEqualTo(ServerIsNotTheLeaderException.class.getName());
    assertThat(json.has("exceptionArgs")).isFalse();
    assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(refusal)).isTrue();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // The recognizer and the wait on their own
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void onlyAnUnnamedNotTheLeaderServiceUnavailableIsRecognized() {
    assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(new ExecutionResponse(503, UNNAMED_REFUSAL))).isTrue();
    assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(new ExecutionResponse(503, new JSONObject()
        .put("exception", ServerIsNotTheLeaderException.class.getName()).put("exceptionArgs", "").toString()))).isTrue();

    assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(new ExecutionResponse(503, new JSONObject()
        .put("exception", ServerIsNotTheLeaderException.class.getName()).put("exceptionArgs", "h:2480").toString())))
        .as("a named refusal").isFalse();
    assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(new ExecutionResponse(400, UNNAMED_REFUSAL)))
        .as("not a 503").isFalse();
    assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(new ExecutionResponse(503, new JSONObject()
        .put("exception", "com.arcadedb.exception.NeedRetryException").toString()))).as("another exception").isFalse();
    assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(new ExecutionResponse(503, "not json "
        + ServerIsNotTheLeaderException.class.getName()))).as("an unparsable body").isFalse();
    assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(new ExecutionResponse(503, (String) null))).isFalse();
    assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(null)).isFalse();
  }

  @Test
  void theWaitReturnsAsSoonAsTheViewNamesAnotherNodeOrNone() {
    final AtomicInteger reads = new AtomicInteger();
    assertThat(LeaderForwardContext.awaitLeaderViewMovedFrom(() -> reads.incrementAndGet() < 3 ? EX_LEADER : null,
        EX_LEADER, 20_000L, 1L)).as("an election in progress is a moved view").isTrue();
    assertThat(reads.get()).isEqualTo(3);

    reads.set(0);
    assertThat(LeaderForwardContext.awaitLeaderViewMovedFrom(() -> reads.incrementAndGet() < 3 ? EX_LEADER : NEW_LEADER,
        EX_LEADER, 20_000L, 1L)).isTrue();

    assertThat(LeaderForwardContext.awaitLeaderViewMovedFrom(() -> EX_LEADER, EX_LEADER, 50L, 1L)).isFalse();
    assertThat(LeaderForwardContext.awaitLeaderViewMovedFrom(() -> EX_LEADER, EX_LEADER, 0L, 1L)).isFalse();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------------------------------------------

  private HAServerPlugin ha() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.isLeader()).thenReturn(false);
    when(ha.getLeaderAddress()).thenReturn(leader.address());
    when(ha.getLeaderName()).thenReturn("leader");
    when(ha.getLeaderPeerId()).thenAnswer(invocation -> view.read());
    when(ha.getLocalPeerId()).thenReturn("peer-follower");
    when(ha.getClusterToken()).thenReturn("issue8486-cluster-token");
    return ha;
  }

  private static ContextConfiguration config(final long forwardLeaderWaitMs) {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_READ_TIMEOUT, 30_000L);
    cfg.setValue(GlobalConfiguration.HA_PROXY_LONG_COMMAND_TIMEOUT, 30_000L);
    cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
    cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, 30_000L);
    cfg.setValue(GlobalConfiguration.HA_FORWARD_LEADER_WAIT_TIMEOUT_MS, forwardLeaderWaitMs);
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

  /**
   * The follower's leader view. It names the ex-leader until the stub leader has answered, then keeps naming it for a
   * fixed number of further reads before naming another node - so "did the forwarder wait for the view to move" is
   * a count of reads, not a measurement of time.
   */
  private static final class LeaderView {
    private final    StubLeader    leader;
    private final    AtomicInteger readsAfterRefusal = new AtomicInteger();
    private volatile int           movesAfterReads   = Integer.MAX_VALUE;
    private volatile boolean       noLeaderId;
    private volatile boolean       moved;

    LeaderView(final StubLeader leader) {
      this.leader = leader;
    }

    void moveAfterReads(final int reads) {
      movesAfterReads = reads;
    }

    void neverMove() {
      movesAfterReads = Integer.MAX_VALUE;
    }

    void noLeaderId() {
      noLeaderId = true;
    }

    String read() {
      if (noLeaderId)
        return null;
      if (leader.answered() == 0)
        return EX_LEADER;
      if (readsAfterRefusal.incrementAndGet() > movesAfterReads) {
        moved = true;
        return NEW_LEADER;
      }
      return EX_LEADER;
    }

    boolean moved() {
      return moved;
    }

    int readsAfterRefusal() {
      return readsAfterRefusal.get();
    }
  }

  /** The ex-leader: a loopback HTTP listener that answers every request with the status and body set last. */
  private static final class StubLeader implements AutoCloseable {
    private final    com.sun.net.httpserver.HttpServer server;
    private final    ExecutorService                   executor = Executors.newCachedThreadPool();
    private final    AtomicInteger                     answered = new AtomicInteger();
    private volatile int                               status   = 200;
    private volatile String                            body     = "{}";

    StubLeader() throws IOException {
      server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 16);
      server.setExecutor(executor);
      server.createContext("/", exchange -> {
        try {
          exchange.getRequestBody().readAllBytes();
          final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          // Counted before the answer leaves, so every read of the view that follows it sees the refusal as sent.
          answered.incrementAndGet();
          exchange.sendResponseHeaders(status, bytes.length);
          exchange.getResponseBody().write(bytes);
        } finally {
          exchange.close();
        }
      });
      server.start();
    }

    void answer(final int status, final String body) {
      this.status = status;
      this.body = body;
    }

    int answered() {
      return answered.get();
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
