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
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.http.FakeLeader;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.StallAwareStopwatch;
import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.BlockingHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8325: the follower-to-leader forwards bounded the leader with the JDK request timeout
 * ({@code HttpRequest.Builder.timeout}) alone, and what that timeout covers depends on the JDK:
 * <ul>
 * <li>on JDK 21-25 it stops at the response HEADERS, so a leader that answers {@code 200} with a
 * {@code Content-Length} and then stalls inside its body parks a forward that reads it with
 * {@code BodyHandlers.ofString()} with no bound at all;</li>
 * <li>on JDK 26+ it covers the whole body, so on the streamed {@code /api/v1/batch} relay it turned
 * {@code arcadedb.ha.proxyBatchReadTimeout} - meant as a bound on the leader's SILENCE (#7738) - into a cap on the
 * total length of a load that is working.</li>
 * </ul>
 * Both halves are driven here on whichever JDK runs the suite: the first fails on JDK 21-25 without the fix, the
 * second on JDK 26+.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8325LeaderForwardBodyDeadlineTest {
  /**
   * Real HTTP servers the helpers build: each owns cleanup threads that only stopService() ends. Static because the
   * helpers are, and safe because this class's tests run one at a time.
   */
  private static final List<HttpServer> HTTP_SERVERS = new ArrayList<>();

  private static final long   BUDGET_MS        = 1_000L;
  /** The tripwire between "the deadline fired" and "the forward is unbounded" (forever, without the fix). */
  private static final long   GAVE_UP_BOUND_MS = 15_000L;
  /** A hang detector, not a latency bound: how long the test waits before calling the forward unbounded. */
  private static final long   HANG_DETECT_MS   = 60_000L;
  private static final int    CLIENT_READ_MS   = 30_000;
  /** Headers promising 100 bytes, then five of them, then silence. */
  private static final String STALLED_BODY     =
      "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"res";

  private Undertow follower;

  @AfterEach
  void stopFollower() {
    HTTP_SERVERS.forEach(HttpServer::stopService);
    HTTP_SERVERS.clear();
    if (follower != null)
      follower.stop();
  }

  // ------------------------------------------------------------------------------------------------------------
  // The shared bound
  // ------------------------------------------------------------------------------------------------------------

  /** The defect at its root: a buffered read of a body the leader stops writing half-way. */
  @Test
  void sendBoundedGivesUpOnALeaderThatStallsInsideItsBody() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> write(out, STALLED_BODY));
        final HttpClient client = HttpClient.newHttpClient()) {
      // No request timeout at all: the bound under test is sendBounded's own, on every JDK.
      final HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + leader.address() + "/")).GET().build();

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> callWithin(
          () -> LeaderDial.sendBounded(client, request, HttpResponse.BodyHandlers.ofString(), BUDGET_MS), leader))
          .isInstanceOf(HttpTimeoutException.class)
          .hasMessageContaining(leader.address());
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS,
          "a 1s deadline over the whole exchange from the unbounded body read the JDK 21-25 request timeout leaves");

      assertThat(leader.awaitConnectionClosedByClient(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS))
          .as("the exchange is cancelled, not only abandoned by the calling thread").isTrue();
    }
  }

  @Test
  void sendBoundedReturnsAWholeAnswer() throws Exception {
    final String body = "{\"result\":[1,2,3]}";
    try (final FakeLeader leader = FakeLeader.scripted(out -> write(out,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + body.length() + "\r\n\r\n" + body));
        final HttpClient client = HttpClient.newHttpClient()) {
      final HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + leader.address() + "/")).GET().build();

      final HttpResponse<String> response = LeaderDial.sendBounded(client, request, HttpResponse.BodyHandlers.ofString(),
          BUDGET_MS * 10);

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(response.body()).isEqualTo(body);
    }
  }

  /** A connect failure keeps its own type, so the callers' "cannot connect" arms still see it. */
  @Test
  void sendBoundedRethrowsAConnectFailureAsItself() throws Exception {
    final String unreachable;
    try (final ServerSocket probe = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      unreachable = probe.getInetAddress().getHostAddress() + ":" + probe.getLocalPort();
    }
    try (final HttpClient client = HttpClient.newHttpClient()) {
      final HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + unreachable + "/")).GET().build();

      assertThatThrownBy(() -> LeaderDial.sendBounded(client, request, HttpResponse.BodyHandlers.ofString(), BUDGET_MS * 10))
          .isInstanceOf(ConnectException.class);
    }
  }

  /** An interrupt while waiting reaches the caller as itself, and the exchange it abandons is cancelled. */
  @Test
  void sendBoundedInterruptedWhileWaitingThrowsTheInterruptAndCancelsTheExchange() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> write(out, STALLED_BODY));
        final HttpClient client = HttpClient.newHttpClient()) {
      final HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + leader.address() + "/")).GET().build();
      final AtomicReference<Throwable> thrown = new AtomicReference<>();
      final CountDownLatch done = new CountDownLatch(1);
      final Thread caller = new Thread(() -> {
        try {
          LeaderDial.sendBounded(client, request, HttpResponse.BodyHandlers.ofString(), HANG_DETECT_MS);
        } catch (final Throwable t) {
          thrown.set(t);
        } finally {
          done.countDown();
        }
      }, "issue8325-interrupted-forward");
      caller.setDaemon(true);
      caller.start();

      assertThat(leader.awaitRequestReceived(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
      caller.interrupt();

      assertThat(done.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).as("the interrupt releases the caller").isTrue();
      assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
      assertThat(leader.awaitConnectionClosedByClient(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
    }
  }

  /**
   * The interrupt path's clean-up, handed the one future a live race produces too rarely to test: one that already
   * failed. {@code getNow} would rethrow that failure in place of the interrupt the caller is about to rethrow.
   */
  @Test
  void closingTheBodyOfAFailedExchangeThrowsNothing() {
    LeaderDial.closeBodyOf(CompletableFuture.failedFuture(new IOException("connection reset")));
    LeaderDial.closeBodyOf(new CompletableFuture<>());
  }

  // ------------------------------------------------------------------------------------------------------------
  // PostBatchHandler.forwardBatchToLeader
  // ------------------------------------------------------------------------------------------------------------

  /** Part (1) of the issue on the /batch forward: the non-streaming branch reads the leader's answer buffered. */
  @Test
  void aBufferedBatchForwardWhoseLeaderStallsInsideItsBodyIsAnswered504() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> write(out, STALLED_BODY))) {
      final PostBatchHandler handler = handlerWith(config());

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final ExecutionResponse response = callWithin(() -> handler.forwardBatchToLeader(new HttpServerExchange(null),
          haPointingAt(leader.address()), "mydb", rootUser(), "application/x-ndjson", emptyBody(), false), leader);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s deadline from the unbounded ofString() body read");

      assertThat(response.getCode()).isEqualTo(504);
      assertThat(new JSONObject(response.getResponse()).getString("error")).contains(leader.address());
      assertThat(leader.awaitConnectionClosedByClient(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
    }
  }

  /**
   * With the request timeout taken off the streamed forward, the wait for the leader's first line must still be
   * bounded: this is the #7526 guarantee, on the encoding it had not been driven through before.
   */
  @Test
  void aStreamingBatchForwardWhoseLeaderNeverAnswersIsStillAnswered504() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> {
      // headers never come
    })) {
      final PostBatchHandler handler = handlerWith(config());

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final ExecutionResponse response = callWithin(() -> handler.forwardBatchToLeader(new HttpServerExchange(null),
          haPointingAt(leader.address()), "mydb", rootUser(), "application/x-ndjson", emptyBody(), true), leader);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s bound on the wait for the leader's response headers");

      assertThat(response.getCode()).isEqualTo(504);
      assertThat(new JSONObject(response.getResponse()).getString("error")).contains(leader.address());
    }
  }

  /**
   * Part (2) of the issue: a streamed load that takes several budgets in total but is never silent for one is a load
   * that is working, and is relayed whole. On JDK 26+ the request timeout covered the whole streamed body and cut
   * this off after one budget.
   */
  @Test
  void aStreamedAnswerLongerThanTheBudgetButNeverSilentIsRelayedWhole() throws Exception {
    final List<String> sent = new ArrayList<>();
    for (int i = 0; i < 12; i++)
      sent.add("{\"type\":\"progress\",\"verticesCreated\":" + i + "}");
    sent.add("{\"type\":\"summary\"}");

    try (final FakeLeader leader = FakeLeader.scripted(out -> {
      write(out, "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nTransfer-Encoding: chunked\r\n\r\n");
      for (final String line : sent) {
        writeChunk(out, line + "\n");
        sleep(BUDGET_MS / 4);
      }
      write(out, "0\r\n\r\n");
    })) {
      startFollowerRelayingTo(leader.address());

      assertThat(readFollowerStream()).as("about 3 budgets in total, never one of silence").isEqualTo(sent);
    }
  }

  // ------------------------------------------------------------------------------------------------------------

  private static ContextConfiguration config() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
    cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, BUDGET_MS);
    return cfg;
  }

  private static PostBatchHandler handlerWith(final ContextConfiguration cfg) {
    final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
    final HttpServer httpServer = new HttpServer(server);
    HTTP_SERVERS.add(httpServer);
    return new PostBatchHandler(httpServer);
  }

  private static HAServerPlugin haPointingAt(final String leaderAddress) {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getLeaderAddress()).thenReturn(leaderAddress);
    when(ha.getClusterToken()).thenReturn("test-token");
    return ha;
  }

  private static ServerSecurityUser rootUser() {
    final ServerSecurityUser user = TestServerHelper.securityUser("root");
    return user;
  }

  private static PostBatchHandler.CountingInputStream emptyBody() {
    return new PostBatchHandler.CountingInputStream(new HttpServerExchange(null), new ByteArrayInputStream(new byte[0]));
  }

  /**
   * Runs {@code call} on its own thread and fails - rather than hanging the suite - when it has not returned within
   * the hang detector. Closing the leader is what releases a forward that never gave up.
   */
  private static <T> T callWithin(final Callable<T> call, final FakeLeader leader) throws Exception {
    final FutureTask<T> task = new FutureTask<>(call);
    final Thread thread = new Thread(task, "issue8325-forward");
    thread.setDaemon(true);
    thread.start();
    try {
      return task.get(HANG_DETECT_MS, TimeUnit.MILLISECONDS);
    } catch (final TimeoutException e) {
      leader.close();
      throw new AssertionError("The forward was still waiting on the stalled leader after " + HANG_DETECT_MS
          + " ms: nothing bounds the read of the leader's body", e);
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof Exception cause)
        throw cause;
      throw e;
    }
  }

  private void startFollowerRelayingTo(final String leaderAddress) {
    final PostBatchHandler handler = handlerWith(config());
    final HAServerPlugin ha = haPointingAt(leaderAddress);
    final ServerSecurityUser user = rootUser();

    follower = Undertow.builder()
        .addHttpListener(0, "127.0.0.1")
        .setHandler(new BlockingHandler(exchange -> {
          final ExecutionResponse response = handler.forwardBatchToLeader(exchange, ha, "mydb", user,
              "application/x-ndjson", new PostBatchHandler.CountingInputStream(exchange, exchange.getInputStream()), true);
          if (response != null) {
            exchange.setStatusCode(response.getCode());
            exchange.getOutputStream().write(response.getResponse().getBytes(StandardCharsets.UTF_8));
          }
        }))
        .build();
    follower.start();
  }

  private List<String> readFollowerStream() throws IOException {
    final InetSocketAddress address = (InetSocketAddress) follower.getListenerInfo().get(0).getAddress();
    final HttpURLConnection conn = (HttpURLConnection) URI.create(
        "http://127.0.0.1:" + address.getPort() + "/api/v1/batch/mydb").toURL().openConnection();
    conn.setRequestProperty("Accept", "application/x-ndjson");
    conn.setReadTimeout(CLIENT_READ_MS);
    // An upload with a declared length, as a real load has: the forward then relays it with that length too
    // (issue #5618), which is the request shape the JDK 26 response timer cuts the streamed answer off on.
    final byte[] upload = "{\"@type\":\"vertex\",\"type\":\"V\"}\n".getBytes(StandardCharsets.UTF_8);
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setFixedLengthStreamingMode(upload.length);
    try (final OutputStream out = conn.getOutputStream()) {
      out.write(upload);
    }
    assertThat(conn.getResponseCode()).isEqualTo(200);

    final List<String> lines = new ArrayList<>();
    try (final BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
      for (String line = in.readLine(); line != null; line = in.readLine())
        lines.add(line);
    } catch (final SocketTimeoutException e) {
      throw new AssertionError("The follower relayed " + lines + " and then held the stream open", e);
    } catch (final IOException e) {
      // A stream cut short is an ending too; the assertion on what arrived says whether it was cut.
    }
    return lines;
  }

  private static void write(final OutputStream out, final String data) throws IOException {
    out.write(data.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

  private static void writeChunk(final OutputStream out, final String data) throws IOException {
    final byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
    out.write((Integer.toHexString(bytes.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
    out.write(bytes);
    out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
    out.flush();
  }

  private static void sleep(final long ms) {
    try {
      Thread.sleep(ms);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
