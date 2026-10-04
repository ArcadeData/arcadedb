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
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.StallAwareStopwatch;
import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.BlockingHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8719: a streamed {@code /api/v1/batch} load sent to a FOLLOWER waited for the leader's
 * response with {@code arcadedb.ha.proxyBatchReadTimeout} counted from the start of the send. On HTTP/1.1 the JDK
 * client hands the response back only once the relayed upload has been published whole (pinned by
 * {@code Issue8674StreamedRelayBodyCapTerminalLineTest.theJdkClientHandsBackTheResponseOnlyOnceTheUploadIsPublished}),
 * so that deadline capped the length of the whole upload: a load whose upload outlasted it was answered 504 although
 * the leader was working and answering. The deadline now counts from the last byte of the upload relayed, so an upload
 * that keeps moving is never cut off, and one that stops moving with no answer is still given up on. The bound itself is
 * tested in {@code network}, by {@code Issue8719SendWhileProgressingTest}.
 */
class Issue8719StreamedForwardUploadDeadlineTest {

  private static final long   BUDGET_MS        = 1_000L;
  /** The tripwire between "the deadline fired" and "the forward is unbounded". */
  private static final long   GAVE_UP_BOUND_MS = 15_000L;
  /** A hang detector, not a latency bound: how long the test waits before calling the forward unbounded. */
  private static final long   HANG_DETECT_MS   = 60_000L;
  private static final int    CLIENT_READ_MS   = 30_000;
  /** How many records the trickling upload sends, one every quarter budget: about three budgets in total. */
  private static final int    UPLOAD_RECORDS   = 12;
  private static final String RECORD           = "{\"@type\":\"vertex\",\"type\":\"V\"}\n";
  private static final String PROGRESS_LINE    = "{\"progress\":{\"phase\":\"vertices\",\"verticesCreated\":0}}";
  private static final String SUMMARY_LINE     = "{\"summary\":{\"verticesCreated\":" + UPLOAD_RECORDS + "}}";

  private Undertow follower;

  @AfterEach
  void stopFollower() {
    if (follower != null)
      follower.stop();
  }

  // ------------------------------------------------------------------------------------------------------------
  // PostBatchHandler.forwardBatchToLeader
  // ------------------------------------------------------------------------------------------------------------

  /**
   * The defect as filed: an upload that takes about three budgets, never one of stillness, to a leader that answers at
   * once and finishes when the upload ends. It used to be answered 504 after one budget.
   */
  @Test
  void aStreamedUploadLongerThanTheBudgetButNeverStillIsRelayedWhole() throws Exception {
    try (final UploadDrainingLeader leader = new UploadDrainingLeader(true)) {
      startFollowerRelayingTo(leader.address(), new TricklingUpload(UPLOAD_RECORDS, BUDGET_MS / 4, null));

      final List<String> lines = readFollowerStream();

      assertThat(lines).as("the leader's answer, relayed whole after an upload of about three budgets")
          .containsExactly(PROGRESS_LINE, SUMMARY_LINE);
      assertThat(leader.uploadEnded.getCount()).as("the leader received the whole upload").isZero();
    }
  }

  /**
   * The bound the fix keeps: an upload that stops moving, to a leader that never answers, is still given up on and
   * answered 504 - it must not become a forward that waits forever.
   */
  @Test
  void aStreamedUploadThatStopsMovingToASilentLeaderIsAnswered504() throws Exception {
    final CountDownLatch release = new CountDownLatch(1);
    try (final UploadDrainingLeader leader = new UploadDrainingLeader(false)) {
      final PostBatchHandler handler = handlerWith(config());
      // Two records, then the upload hangs until the test lets it go.
      final PostBatchHandler.CountingInputStream body = new PostBatchHandler.CountingInputStream(
          new HttpServerExchange(null), new TricklingUpload(2, 10, release));

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final ExecutionResponse response = callWithin(() -> handler.forwardBatchToLeader(new HttpServerExchange(null),
          haPointingAt(leader.address()), "mydb", rootUser(), "application/x-ndjson", body, true), leader);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s bound on an upload that stopped moving from an unbounded wait");

      assertThat(response.getCode()).isEqualTo(504);
      assertThat(new JSONObject(response.getResponse()).getString("error")).contains(leader.address())
          .contains("last byte of the upload relayed");
      assertThat(body.getBytesRead()).as("the stall came after the upload had started moving")
          .isEqualTo(2L * RECORD.length());
      assertThat(leader.connectionClosed.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS))
          .as("the forward is cancelled, its connection to the leader closed").isTrue();
    } finally {
      release.countDown();
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

  /** Runs {@code call} on its own thread and fails, rather than hanging the suite, past the hang detector. */
  private static <T> T callWithin(final Callable<T> call, final UploadDrainingLeader leader) throws Exception {
    final FutureTask<T> task = new FutureTask<>(call);
    final Thread thread = new Thread(task, "issue8719-forward");
    thread.setDaemon(true);
    thread.start();
    try {
      return task.get(HANG_DETECT_MS, TimeUnit.MILLISECONDS);
    } catch (final TimeoutException e) {
      leader.close();
      throw new AssertionError("The forward was still waiting after " + HANG_DETECT_MS + " ms: nothing bounds it", e);
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof Exception cause)
        throw cause;
      throw e;
    }
  }

  /** A follower whose forward relays {@code upload} as the client's body, on the streaming encoding. */
  private void startFollowerRelayingTo(final String leaderAddress, final InputStream upload) {
    final PostBatchHandler handler = handlerWith(config());
    final HAServerPlugin ha = haPointingAt(leaderAddress);
    final ServerSecurityUser user = rootUser();

    follower = Undertow.builder()
        .addHttpListener(0, "127.0.0.1")
        .setHandler(new BlockingHandler(exchange -> {
          final ExecutionResponse response = handler.forwardBatchToLeader(exchange, ha, "mydb", user,
              "application/x-ndjson", new PostBatchHandler.CountingInputStream(exchange, upload), true);
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
    final int status = conn.getResponseCode();
    if (status != 200) {
      final InputStream error = conn.getErrorStream();
      throw new AssertionError("The follower answered " + status + ": "
          + (error != null ? new String(error.readAllBytes(), StandardCharsets.UTF_8) : ""));
    }

    final List<String> lines = new ArrayList<>();
    try (final BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
      for (String line = in.readLine(); line != null; line = in.readLine())
        lines.add(line);
    } catch (final SocketTimeoutException e) {
      throw new AssertionError("The follower relayed " + lines + " and then held the stream open", e);
    }
    return lines;
  }

  private static void sleep(final long ms) {
    try {
      Thread.sleep(ms);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * An upload of {@code records} records, one every {@code pauseMs}; then, with a {@code hold}, a read that blocks until
   * it is released - an upload that stopped moving - and the end of the body otherwise.
   */
  private static final class TricklingUpload extends InputStream {
    private final byte[]         record = RECORD.getBytes(StandardCharsets.UTF_8);
    private final long           pauseMs;
    private final CountDownLatch hold;
    private       int            left;

    TricklingUpload(final int records, final long pauseMs, final CountDownLatch hold) {
      this.left = records;
      this.pauseMs = pauseMs;
      this.hold = hold;
    }

    @Override
    public int read() throws IOException {
      final byte[] one = new byte[1];
      return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      if (left == 0) {
        if (hold != null)
          try {
            hold.await(HANG_DETECT_MS, TimeUnit.MILLISECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
          }
        return -1;
      }
      sleep(pauseMs);
      final int n = Math.min(len, record.length);
      System.arraycopy(record, 0, b, off, n);
      --left;
      return n;
    }
  }

  /**
   * A leader that reads one request's headers and, when {@code answers}, writes 200 and a progress line at once -
   * before the upload is over, as a leader streaming a large load does. It then drains the chunked upload and, when
   * {@code answers}, ends its answer with a summary once the upload has ended.
   */
  private static final class UploadDrainingLeader implements AutoCloseable {
    private static final byte[] LAST_CHUNK = "0\r\n\r\n".getBytes(StandardCharsets.US_ASCII);

    private final ServerSocket            serverSocket;
    private final AtomicReference<Socket> accepted         = new AtomicReference<>();
    final         CountDownLatch          uploadEnded      = new CountDownLatch(1);
    final         CountDownLatch          connectionClosed = new CountDownLatch(1);

    UploadDrainingLeader(final boolean answers) throws IOException {
      // The literal IPv4 loopback: under java.net.preferIPv6Addresses=true getLoopbackAddress() is ::1, and an
      // unbracketed IPv6 literal followed by ":port" is not a URI the forwarder can build. The port comes from the
      // shared allocator rather than the ephemeral range.
      serverSocket = new ServerSocket(StaticBaseServerTest.allocateFreePorts(1)[0], 16, InetAddress.getByName("127.0.0.1"));
      final Thread acceptor = new Thread(() -> {
        try (final Socket socket = serverSocket.accept()) {
          accepted.set(socket);
          final InputStream in = socket.getInputStream();
          final OutputStream out = socket.getOutputStream();
          skipUntil(in, "\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
          if (answers) {
            out.write("HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nTransfer-Encoding: chunked\r\n\r\n"
                .getBytes(StandardCharsets.US_ASCII));
            writeChunk(out, PROGRESS_LINE + "\n");
          }
          if (skipUntil(in, LAST_CHUNK)) {
            uploadEnded.countDown();
            if (answers) {
              writeChunk(out, SUMMARY_LINE + "\n");
              out.write(LAST_CHUNK);
              out.flush();
            }
          }
          final byte[] buffer = new byte[1024];
          while (in.read(buffer) >= 0) {
            // discard, until the follower closes the connection
          }
        } catch (final IOException ignored) {
          // the follower closed the connection
        } finally {
          connectionClosed.countDown();
        }
      }, "issue8719-scripted-leader");
      acceptor.setDaemon(true);
      acceptor.start();
    }

    String address() {
      return serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort();
    }

    /** Reads until {@code marker} has gone by; false when the stream ended first. */
    private static boolean skipUntil(final InputStream in, final byte[] marker) throws IOException {
      int matched = 0;
      while (matched < marker.length) {
        final int b = in.read();
        if (b < 0)
          return false;
        matched = b == marker[matched] ? matched + 1 : (b == marker[0] ? 1 : 0);
      }
      return true;
    }

    private static void writeChunk(final OutputStream out, final String data) throws IOException {
      final byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
      out.write((Integer.toHexString(bytes.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
      out.write(bytes);
      out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
      out.flush();
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
