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
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.StallAwareStopwatch;
import io.undertow.Undertow;
import io.undertow.server.handlers.BlockingHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #7738: #7526/#7542 bounded the follower-to-leader {@code /api/v1/batch} relay with
 * {@code arcadedb.ha.proxyBatchReadTimeout} as the JDK request timeout, which expires only until the leader's
 * response HEADERS arrive. On the streaming encoding ({@code Accept: application/x-ndjson}, #7311) the headers
 * arrive with the leader's first progress line, and the relay then read the rest with no deadline at all, so a
 * leader that answered 200 and stalled parked the follower's worker thread indefinitely.
 * <p>
 * Driven through a real Undertow exchange, because the bound is scheduled on the exchange's XNIO thread and
 * {@code relayNdJsonFromLeader} writes through the exchange's own output stream; the leader is a raw socket so
 * the test controls exactly when it goes silent.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7738StreamingBatchRelayReadDeadlineTest {

  private static final long   BUDGET_MS        = 1_000L;
  /** The tripwire between "the bound fired" and "the relay is unbounded" (forever, without the fix). */
  private static final long   GAVE_UP_BOUND_MS = 15_000L;
  /** How long the client waits on the follower before calling the relay unbounded. */
  private static final int    CLIENT_READ_MS   = 30_000;
  private static final String FIRST_LINE       = "{\"type\":\"progress\",\"verticesCreated\":1}";

  private Undertow follower;

  @AfterEach
  void stopFollower() {
    if (follower != null)
      follower.stop();
  }

  /** The defect as filed: headers plus the first progress line, then silence. */
  @Test
  void aLeaderThatStallsMidStreamIsGivenUpOnAndItsConnectionClosed() throws Exception {
    try (final ScriptedLeader leader = new ScriptedLeader(out -> {
      writeStreamingHeaders(out);
      writeChunk(out, FIRST_LINE + "\n");
      // ... and nothing else, ever.
    })) {
      final RelayResult relay = startFollowerRelayingTo(leader.address());

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final List<String> lines = readFollowerStream();
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS,
          "a 1s silence bound on the relayed stream from the unbounded readLine() the follower used to park in");

      assertThat(lines).as("what the leader sent before stalling still reaches the client").containsExactly(FIRST_LINE);
      assertThat(relay.returned.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS))
          .as("the follower's worker thread is given back").isTrue();
      assertThat(leader.connectionClosedByFollower.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS))
          .as("the connection to the stalled leader is closed, not left open").isTrue();
    }
  }

  /**
   * The bound is on silence, not on length: a leader whose answer takes several budgets in total but never
   * pauses longer than one is relayed whole.
   */
  @Test
  void aLeaderThatKeepsSendingIsNeverCutOffHoweverLongTheAnswerTakes() throws Exception {
    final List<String> sent = new ArrayList<>();
    for (int i = 0; i < 8; i++)
      sent.add("{\"type\":\"progress\",\"verticesCreated\":" + i + "}");
    sent.add("{\"type\":\"summary\"}");

    try (final ScriptedLeader leader = new ScriptedLeader(out -> {
      writeStreamingHeaders(out);
      for (final String line : sent) {
        writeChunk(out, line + "\n");
        sleep(BUDGET_MS / 3);
      }
      out.write("0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
      out.flush();
    })) {
      startFollowerRelayingTo(leader.address());

      assertThat(readFollowerStream()).as("roughly 3 budgets in total, never one of silence").isEqualTo(sent);
    }
  }

  /**
   * The non-streaming branch of the same relay: the leader refused with a status-carrying body, sent part of it
   * and stalled. No status has reached the client yet, so it is answered 504 like a leader that never answered.
   */
  @Test
  void aLeaderThatStallsInsideABufferedRefusalIsAnswered504() throws Exception {
    try (final ScriptedLeader leader = new ScriptedLeader(out -> {
      out.write(("HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n"
          + "{\"error\":").getBytes(StandardCharsets.US_ASCII));
      out.flush();
    })) {
      final RelayResult relay = startFollowerRelayingTo(leader.address());

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final HttpURLConnection conn = openFollower();
      final int status = conn.getResponseCode();
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s silence bound from the unbounded readAllBytes()");

      assertThat(status).isEqualTo(504);
      assertThat(relay.response.get()).isNotNull();
      assertThat(relay.response.get().getCode()).isEqualTo(504);
    }
  }

  // ---------------------------------------------------------------------------------------------------------

  private static final class RelayResult {
    final CountDownLatch                     returned = new CountDownLatch(1);
    final AtomicReference<ExecutionResponse> response = new AtomicReference<>();
  }

  /** An Undertow listener whose only handler is the follower's relay, exactly as PostBatchHandler calls it. */
  private RelayResult startFollowerRelayingTo(final String leaderAddress) {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
    cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, BUDGET_MS);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(cfg);
    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);
    final PostBatchHandler handler = new PostBatchHandler(httpServer);

    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getLeaderAddress()).thenReturn(leaderAddress);
    when(ha.getClusterToken()).thenReturn("test-token");
    final ServerSecurityUser user = mock(ServerSecurityUser.class);
    when(user.getName()).thenReturn("root");

    final RelayResult result = new RelayResult();
    follower = Undertow.builder()
        .addHttpListener(0, "127.0.0.1")
        .setHandler(new BlockingHandler(exchange -> {
          try {
            final ExecutionResponse response = handler.forwardBatchToLeader(exchange, ha, "mydb", user,
                "application/x-ndjson",
                new PostBatchHandler.CountingInputStream(exchange, new ByteArrayInputStream(new byte[0])), true);
            result.response.set(response);
            if (response != null) {
              exchange.setStatusCode(response.getCode());
              exchange.getOutputStream().write(response.getResponse().getBytes(StandardCharsets.UTF_8));
            }
          } finally {
            result.returned.countDown();
          }
        }))
        .build();
    follower.start();
    return result;
  }

  private HttpURLConnection openFollower() throws IOException {
    final InetSocketAddress address = (InetSocketAddress) follower.getListenerInfo().get(0).getAddress();
    final HttpURLConnection conn = (HttpURLConnection) URI.create(
        "http://127.0.0.1:" + address.getPort() + "/api/v1/batch/mydb").toURL().openConnection();
    conn.setRequestProperty("Accept", "application/x-ndjson");
    conn.setReadTimeout(CLIENT_READ_MS);
    return conn;
  }

  /** Everything the follower relays, until its stream ends. */
  private List<String> readFollowerStream() throws IOException {
    final HttpURLConnection conn = openFollower();
    assertThat(conn.getResponseCode()).isEqualTo(200);
    final List<String> lines = new ArrayList<>();
    try (final BufferedReader in = new BufferedReader(
        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
      for (String line = in.readLine(); line != null; line = in.readLine())
        lines.add(line);
    } catch (final SocketTimeoutException e) {
      throw new AssertionError("The follower relayed " + lines + " and then held the stream open for "
          + CLIENT_READ_MS + "ms: the relay of a stalled leader is unbounded", e);
    } catch (final IOException e) {
      // A stream that ends abruptly is an ending too; what matters is that it ended.
    }
    return lines;
  }

  private static void writeStreamingHeaders(final OutputStream out) throws IOException {
    out.write(("HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nTransfer-Encoding: chunked\r\n\r\n")
        .getBytes(StandardCharsets.US_ASCII));
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

  @FunctionalInterface
  private interface LeaderScript {
    void answer(OutputStream out) throws IOException;
  }

  /**
   * A leader that reads one request's headers, answers with a script, and then keeps the connection open until
   * the follower closes it - which it records. Closing the leader tears the connection down from its side too,
   * which is what releases a follower that never gave up: without it an unbounded relay would hold the Undertow
   * worker and hang {@code Undertow.stop()} rather than fail the test.
   */
  private static final class ScriptedLeader implements AutoCloseable {
    private final ServerSocket            serverSocket;
    private final AtomicReference<Socket> accepted                   = new AtomicReference<>();
    final CountDownLatch                  connectionClosedByFollower = new CountDownLatch(1);

    ScriptedLeader(final LeaderScript script) throws IOException {
      serverSocket = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
      final Thread acceptor = new Thread(() -> {
        try (final Socket socket = serverSocket.accept()) {
          accepted.set(socket);
          final InputStream in = socket.getInputStream();
          skipRequestHeaders(in);
          script.answer(socket.getOutputStream());
          // Drain whatever the follower still sends (the empty chunked upload) until it closes the connection.
          final byte[] buffer = new byte[1024];
          while (in.read(buffer) >= 0) {
            // discard
          }
          connectionClosedByFollower.countDown();
        } catch (final IOException e) {
          // A reset is the follower closing the connection too.
          connectionClosedByFollower.countDown();
        }
      }, "issue7738-scripted-leader");
      acceptor.setDaemon(true);
      acceptor.start();
    }

    String address() {
      return serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort();
    }

    private static void skipRequestHeaders(final InputStream in) throws IOException {
      int matched = 0;
      final byte[] terminator = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
      while (matched < terminator.length) {
        final int b = in.read();
        if (b < 0)
          throw new IOException("the follower closed before sending its request");
        matched = b == terminator[matched] ? matched + 1 : (b == terminator[0] ? 1 : 0);
      }
    }

    @Override
    public void close() throws IOException {
      serverSocket.close();
      final Socket socket = accepted.get();
      if (socket != null)
        socket.close();
    }
  }
}
