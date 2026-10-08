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
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RequestTooBigException;
import io.undertow.server.handlers.BlockingHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #9216: a streamed {@code /api/v1/batch} load sent to a FOLLOWER saw none of the leader's
 * progress lines until its whole upload had been relayed, because the JDK client the forward used hands back an
 * HTTP/1.1 response only once the request body has been published whole. The forward is now sent full duplex, so the
 * leader's acknowledgements cross the hop while the upload is still going, and #8674's in-band 413 for a cap trip after
 * the leader started answering is reached on the live forward rather than only by a scripted one.
 */
class Issue9216StreamedForwardIncrementalAcksTest {
  private static final long   BUDGET_MS      = 10_000L;
  private static final int    CLIENT_READ_MS = 30_000;
  /** How long the upload waits for the client to see the leader's first line before it gives up waiting and ends. */
  private static final long   HOLD_MS        = 10_000L;
  private static final long   CAP_BYTES      = 1_024L;
  private static final String RECORD         = "{\"@type\":\"vertex\",\"type\":\"V\"}\n";
  private static final String PROGRESS_LINE  = "{\"progress\":{\"phase\":\"vertices\",\"verticesCreated\":1,\"edgesCreated\":0}}";
  private static final String SUMMARY_LINE   = "{\"summary\":{\"verticesCreated\":2}}";

  @RegisterExtension
  static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

  private Undertow follower;

  @AfterEach
  void stopFollower() {
    if (follower != null)
      follower.stop();
  }

  /** The defect as filed: the leader's progress line reaches the client while the client's upload is still pending. */
  @Test
  @Timeout(value = 90, unit = TimeUnit.SECONDS)
  void theLeadersProgressLineReachesTheClientWhileTheUploadIsStillPending() throws Exception {
    final CountDownLatch clientSawFirstLine = new CountDownLatch(1);
    final AtomicBoolean releasedByItsOwnTimeout = new AtomicBoolean(false);

    try (final AcknowledgingLeader leader = new AcknowledgingLeader()) {
      startFollower(leader.address(), exchange -> new PostBatchHandler.CountingInputStream(exchange,
          new HeldUpload(RECORD, clientSawFirstLine, releasedByItsOwnTimeout, RECORD)), haPointingAt(leader.address()));

      final HttpURLConnection conn = openFollower();
      assertThat(conn.getResponseCode()).isEqualTo(200);
      final List<String> lines = new ArrayList<>();
      try (final BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
        lines.add(in.readLine());
        clientSawFirstLine.countDown();
        for (String line = in.readLine(); line != null; line = in.readLine())
          lines.add(line);
      }

      assertThat(releasedByItsOwnTimeout.get())
          .as("the client saw the leader's first line while its upload was still pending").isFalse();
      assertThat(lines).containsExactly(PROGRESS_LINE, SUMMARY_LINE);
      assertThat(leader.upload.toString(StandardCharsets.UTF_8)).as("the leader received the whole upload")
          .isEqualTo(RECORD + RECORD);
    }
  }

  /**
   * #8674's in-band 413, on the live forward: this node's cap trips after the leader has answered 200 and a progress
   * line. The status is on the wire, so the refusal travels as the stream's terminal line.
   */
  @Test
  @Timeout(value = 90, unit = TimeUnit.SECONDS)
  void aCapTripAfterTheLeaderStartedAnsweringEndsTheRelayWithAnIn413Line() throws Exception {
    final CountDownLatch clientSawFirstLine = new CountDownLatch(1);
    final AtomicBoolean releasedByItsOwnTimeout = new AtomicBoolean(false);
    final AtomicReference<PostBatchHandler.CountingInputStream> body = new AtomicReference<>();

    try (final AcknowledgingLeader leader = new AcknowledgingLeader()) {
      startFollower(leader.address(), exchange -> {
        body.set(new PostBatchHandler.CountingInputStream(exchange,
            new HeldUpload(RECORD, clientSawFirstLine, releasedByItsOwnTimeout, "x".repeat((int) CAP_BYTES * 4)),
            CAP_BYTES));
        return body.get();
      }, haPointingAt(leader.address()));

      final HttpURLConnection conn = openFollower();
      assertThat(conn.getResponseCode()).as("the leader's 200 crossed the hop before the cap tripped").isEqualTo(200);
      final List<String> lines = new ArrayList<>();
      try (final BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
        lines.add(in.readLine());
        clientSawFirstLine.countDown();
        for (String line = in.readLine(); line != null; line = in.readLine())
          lines.add(line);
      }

      assertThat(releasedByItsOwnTimeout.get()).isFalse();
      assertThat(body.get().refusedOverCap()).isNotNull();
      assertThat(lines).as("the leader's progress line, then the in-band refusal").hasSize(2);
      assertThat(lines.get(0)).isEqualTo(PROGRESS_LINE);
      final JSONObject error = new JSONObject(lines.get(1)).getJSONObject("error");
      assertThat(error.getInt("status", 0)).isEqualTo(413);
      assertThat(error.getString("exception", "")).isEqualTo(RequestTooBigException.class.getName());
      assertThat(error.getLong("verticesCreated", -1)).isEqualTo(1L);
      assertThat(error.getBoolean("partialCommit", false)).isTrue();
      assertThat(leader.uploadEnded.getCount()).as("the leader saw the upload cut, never its end").isEqualTo(1L);
    }
  }

  /**
   * A cap trip BEFORE the leader answered still fails the send itself, with the 413 #8161 answers: the forward
   * recognises the refusal although the connection was aborted underneath the wait for the answer.
   */
  @Test
  @Timeout(value = 90, unit = TimeUnit.SECONDS)
  void aCapTripBeforeTheLeaderAnswersIsStillRefused413() throws Exception {
    try (final SilentLeader leader = new SilentLeader()) {
      final PostBatchHandler.CountingInputStream body = new PostBatchHandler.CountingInputStream(
          new HttpServerExchange(null), new ByteArrayInputStream(new byte[(int) CAP_BYTES * 4]), CAP_BYTES);

      final Throwable thrown = catchThrowable(() -> handlerWith(config())
          .forwardBatchToLeader(new HttpServerExchange(null), haPointingAt(leader.address()), "mydb", rootUser(),
              "application/x-ndjson", body, true));

      assertThat(thrown).isInstanceOf(RequestTooBigException.class);
    }
  }

  /**
   * A leader that refuses at once, before reading the upload, and closes: its answer is relayed as the buffered answer
   * it is, not lost to the upload that can no longer be sent.
   */
  @Test
  @Timeout(value = 90, unit = TimeUnit.SECONDS)
  void anEarlyRefusalFromTheLeaderIsRelayed() throws Exception {
    final String refusal = "{\"error\":\"bad refMode\"}";
    try (final RefusingLeader leader = new RefusingLeader(refusal)) {
      final byte[] upload = RECORD.repeat(10_000).getBytes(StandardCharsets.UTF_8);
      startFollower(leader.address(),
          exchange -> new PostBatchHandler.CountingInputStream(exchange, new ByteArrayInputStream(upload)),
          haPointingAt(leader.address()));

      final HttpURLConnection conn = openFollower();
      assertThat(conn.getResponseCode()).isEqualTo(400);
      assertThat(new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8)).isEqualTo(refusal);
    }
  }

  /** The request head the leader receives: the forward's own headers, framed by the client's declared length. */
  @Test
  @Timeout(value = 90, unit = TimeUnit.SECONDS)
  void aDeclaredLengthIsRelayedAsContentLengthWithTheForwardsHeaders() throws Exception {
    try (final SilentLeader leader = new SilentLeader()) {
      final byte[] payload = RECORD.getBytes(StandardCharsets.UTF_8);
      final HttpRequest request = PostBatchHandler.buildForwardRequest(
          "http://" + leader.address() + "/api/v1/batch/mydb?commitEvery=10", "application/x-ndjson", "test-token", "root",
          payload.length, new ByteArrayInputStream(payload), NdJsonResultStream.CONTENT_TYPE, null, "peer-1");

      final Thread sender = new Thread(() -> {
        try (final DuplexHttpExchange ignored = DuplexHttpExchange.send(HttpClient.newHttpClient(), request,
            new ByteArrayInputStream(payload), 2_000L, () -> 0L)) {
          // the silent leader never answers
        } catch (final Exception expected) {
          // the deadline
        }
      }, "issue9216-sender");
      sender.setDaemon(true);
      sender.start();

      final String received = leader.awaitBytes(payload.length, CLIENT_READ_MS);
      final String head = received.substring(0, received.indexOf("\r\n\r\n"));
      assertThat(head).startsWith("POST /api/v1/batch/mydb?commitEvery=10 HTTP/1.1\r\n");
      assertThat(head).contains("\r\nHost: " + leader.address())
          .contains("\r\nContent-Length: " + payload.length)
          .contains("\r\nX-ArcadeDB-Cluster-Token: test-token")
          .contains("\r\nX-ArcadeDB-Forwarded-User: root")
          .contains("\r\n" + LeaderForwardContext.FORWARDED_LEADER_ID_HEADER + ": peer-1")
          .contains("\r\nAccept: " + NdJsonResultStream.CONTENT_TYPE)
          .doesNotContainIgnoringCase("Transfer-Encoding");
      assertThat(received.substring(received.indexOf("\r\n\r\n") + 4)).isEqualTo(RECORD);
      sender.join(CLIENT_READ_MS);
    }
  }

  /** An HTTPS dial goes out as TLS, never as the cleartext request the cluster asked not to send (issue #7508). */
  @Test
  @Timeout(value = 90, unit = TimeUnit.SECONDS)
  void anHttpsDialSpeaksTls() throws Exception {
    try (final SilentLeader leader = new SilentLeader()) {
      final HAServerPlugin ha = haPointingAt("127.0.0.1:1");
      when(ha.getLeaderHttpsAddress()).thenReturn(leader.address());
      when(ha.getPeerHttpsClient()).thenReturn(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build());
      final ContextConfiguration cfg = config();
      cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, 1_000L);

      final ExecutionResponse response = handlerWith(cfg).forwardBatchToLeader(new HttpServerExchange(null), ha, "mydb",
          rootUser(), "application/x-ndjson", new PostBatchHandler.CountingInputStream(new HttpServerExchange(null),
              new ByteArrayInputStream(RECORD.getBytes(StandardCharsets.UTF_8))), true);

      assertThat(response.getCode()).as("the silent leader never completes the handshake").isEqualTo(504);
      final byte[] first = leader.awaitFirstBytes(3, CLIENT_READ_MS);
      assertThat(first[0]).as("a TLS handshake record, not '" + new String(first, StandardCharsets.ISO_8859_1) + "'")
          .isEqualTo((byte) 0x16);
      assertThat(first[1]).as("TLS major version").isEqualTo((byte) 0x03);
    }
  }

  // ------------------------------------------------------------------------------------------------------------

  private static ContextConfiguration config() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
    cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, BUDGET_MS);
    cfg.setValue(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE, CAP_BYTES);
    return cfg;
  }

  private static PostBatchHandler handlerWith(final ContextConfiguration cfg) {
    final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
    final HttpServer httpServer = HTTP_SERVERS.of(server);
    return new PostBatchHandler(httpServer);
  }

  private static HAServerPlugin haPointingAt(final String leaderAddress) {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getLeaderAddress()).thenReturn(leaderAddress);
    when(ha.getClusterToken()).thenReturn("test-token");
    return ha;
  }

  private static ServerSecurityUser rootUser() {
    return TestServerHelper.securityUser("root");
  }

  /** A follower whose forward relays the body {@code bodyFor} builds, on the streaming encoding. */
  private void startFollower(final String leaderAddress,
      final Function<HttpServerExchange, PostBatchHandler.CountingInputStream> bodyFor, final HAServerPlugin ha) {
    final PostBatchHandler handler = handlerWith(config());
    final ServerSecurityUser user = rootUser();

    follower = Undertow.builder()
        .addHttpListener(0, "127.0.0.1")
        .setHandler(new BlockingHandler(exchange -> {
          final ExecutionResponse response = handler.forwardBatchToLeader(exchange, ha, "mydb", user,
              "application/x-ndjson", bodyFor.apply(exchange), true);
          if (response != null) {
            exchange.setStatusCode(response.getCode());
            exchange.getOutputStream().write(response.getResponse().getBytes(StandardCharsets.UTF_8));
          }
        }))
        .build();
    follower.start();
  }

  private HttpURLConnection openFollower() throws IOException {
    final InetSocketAddress address = (InetSocketAddress) follower.getListenerInfo().get(0).getAddress();
    final HttpURLConnection conn = (HttpURLConnection) URI.create(
        "http://127.0.0.1:" + address.getPort() + "/api/v1/batch/mydb").toURL().openConnection();
    conn.setRequestProperty("Accept", "application/x-ndjson");
    conn.setReadTimeout(CLIENT_READ_MS);
    return conn;
  }

  /**
   * The client's upload, as the follower reads it: {@code first}, then a read that blocks until the client has seen the
   * leader's first line - or {@link #HOLD_MS} has passed, which the flag records - then {@code rest}, then the end.
   */
  private static final class HeldUpload extends InputStream {
    private final byte[]         first;
    private final byte[]         rest;
    private final CountDownLatch release;
    private final AtomicBoolean  releasedByItsOwnTimeout;
    private       int            pos;

    HeldUpload(final String first, final CountDownLatch release, final AtomicBoolean releasedByItsOwnTimeout,
        final String rest) {
      this.first = first.getBytes(StandardCharsets.UTF_8);
      this.rest = rest.getBytes(StandardCharsets.UTF_8);
      this.release = release;
      this.releasedByItsOwnTimeout = releasedByItsOwnTimeout;
    }

    @Override
    public int read() throws IOException {
      final byte[] one = new byte[1];
      return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      if (pos < first.length) {
        final int n = Math.min(len, first.length - pos);
        System.arraycopy(first, pos, b, off, n);
        pos += n;
        return n;
      }
      if (pos == first.length)
        try {
          if (!release.await(HOLD_MS, TimeUnit.MILLISECONDS))
            releasedByItsOwnTimeout.set(true);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("interrupted", e);
        }
      final int restPos = pos - first.length;
      if (restPos >= rest.length)
        return -1;
      final int n = Math.min(len, rest.length - restPos);
      System.arraycopy(rest, restPos, b, off, n);
      pos += n;
      return n;
    }
  }

  /** A base for the single-connection leaders below: one accepted connection, served on its own thread. */
  private abstract static class OneConnectionLeader implements AutoCloseable {
    private final   ServerSocket            serverSocket;
    protected final AtomicReference<Socket> accepted = new AtomicReference<>();

    OneConnectionLeader() throws IOException {
      // The literal IPv4 loopback and a port from the shared allocator, as the other forward tests do.
      serverSocket = new ServerSocket(StaticBaseServerTest.allocateFreePorts(1)[0], 16, InetAddress.getByName("127.0.0.1"));
      final Thread acceptor = new Thread(() -> {
        try (final Socket socket = serverSocket.accept()) {
          accepted.set(socket);
          serve(socket.getInputStream(), socket.getOutputStream());
        } catch (final IOException ignored) {
          // the follower closed or aborted the connection
        }
      }, "issue9216-leader");
      acceptor.setDaemon(true);
      acceptor.start();
    }

    abstract void serve(InputStream in, OutputStream out) throws IOException;

    String address() {
      return serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort();
    }

    @Override
    public void close() throws IOException {
      serverSocket.close();
      final Socket socket = accepted.get();
      if (socket != null)
        socket.close();
    }

    /** Reads until {@code marker} has gone by, keeping what was read before it; false when the stream ended first. */
    static boolean readUntil(final InputStream in, final byte[] marker, final ByteArrayOutputStream kept)
        throws IOException {
      int matched = 0;
      final ByteArrayOutputStream window = new ByteArrayOutputStream();
      while (matched < marker.length) {
        final int b = in.read();
        if (b < 0)
          return false;
        window.write(b);
        matched = b == marker[matched] ? matched + 1 : (b == marker[0] ? 1 : 0);
      }
      if (kept != null) {
        final byte[] all = window.toByteArray();
        kept.write(all, 0, all.length - marker.length);
      }
      return true;
    }

    static void writeChunk(final OutputStream out, final String data) throws IOException {
      final byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
      out.write((Integer.toHexString(bytes.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
      out.write(bytes);
      out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
      out.flush();
    }
  }

  /**
   * Answers 200 and a progress line as soon as the request head is in, decodes the chunked upload, and ends its answer
   * with a summary once the upload has ended - a leader streaming a load.
   */
  private static final class AcknowledgingLeader extends OneConnectionLeader {
    final ByteArrayOutputStream upload      = new ByteArrayOutputStream();
    final CountDownLatch        uploadEnded = new CountDownLatch(1);

    AcknowledgingLeader() throws IOException {
    }

    @Override
    void serve(final InputStream in, final OutputStream out) throws IOException {
      readUntil(in, "\r\n\r\n".getBytes(StandardCharsets.US_ASCII), null);
      out.write("HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nTransfer-Encoding: chunked\r\n\r\n"
          .getBytes(StandardCharsets.US_ASCII));
      writeChunk(out, PROGRESS_LINE + "\n");
      final byte[] crlf = "\r\n".getBytes(StandardCharsets.US_ASCII);
      while (true) {
        final ByteArrayOutputStream sizeLine = new ByteArrayOutputStream();
        if (!readUntil(in, crlf, sizeLine))
          return;
        final int size = Integer.parseInt(sizeLine.toString(StandardCharsets.US_ASCII).trim(), 16);
        if (size == 0)
          break;
        upload.write(in.readNBytes(size));
        if (!readUntil(in, crlf, null))
          return;
      }
      readUntil(in, crlf, null);
      uploadEnded.countDown();
      writeChunk(out, SUMMARY_LINE + "\n");
      out.write("0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
      out.flush();
    }
  }

  /** Answers a buffered 400 before reading anything of the upload, and closes. */
  private static final class RefusingLeader extends OneConnectionLeader {
    private final String refusal;

    RefusingLeader(final String refusal) throws IOException {
      this.refusal = refusal;
    }

    @Override
    void serve(final InputStream in, final OutputStream out) throws IOException {
      readUntil(in, "\r\n\r\n".getBytes(StandardCharsets.US_ASCII), null);
      final byte[] payload = refusal.getBytes(StandardCharsets.UTF_8);
      out.write(("HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: " + payload.length
          + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
      out.write(payload);
      out.flush();
    }
  }

  /** Records what it receives and never answers. */
  private static final class SilentLeader extends OneConnectionLeader {
    private final ByteArrayOutputStream received = new ByteArrayOutputStream();

    SilentLeader() throws IOException {
    }

    @Override
    void serve(final InputStream in, final OutputStream out) throws IOException {
      final byte[] buffer = new byte[1024];
      for (int n = in.read(buffer); n >= 0; n = in.read(buffer))
        synchronized (received) {
          received.write(buffer, 0, n);
          received.notifyAll();
        }
    }

    byte[] awaitFirstBytes(final int count, final long timeoutMs) throws InterruptedException {
      final long end = System.currentTimeMillis() + timeoutMs;
      synchronized (received) {
        while (received.size() < count && System.currentTimeMillis() < end)
          received.wait(100);
        return received.toByteArray();
      }
    }

    /** The request head and {@code bodyBytes} of body, once they have arrived. */
    String awaitBytes(final int bodyBytes, final long timeoutMs) throws InterruptedException, SocketTimeoutException {
      final long end = System.currentTimeMillis() + timeoutMs;
      synchronized (received) {
        while (System.currentTimeMillis() < end) {
          final String text = received.toString(StandardCharsets.ISO_8859_1);
          final int headEnd = text.indexOf("\r\n\r\n");
          if (headEnd >= 0 && text.length() - headEnd - 4 >= bodyBytes)
            return text;
          received.wait(100);
        }
      }
      throw new SocketTimeoutException("the leader received only " + received.size() + " bytes");
    }
  }
}
