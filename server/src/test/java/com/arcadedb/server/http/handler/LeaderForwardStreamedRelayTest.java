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
import com.arcadedb.exception.TransactionOutcomeUnknownException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.http.FakeLeader;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.StallAwareStopwatch;
import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RequestTooBigException;
import io.undertow.server.handlers.BlockingHandler;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.net.ssl.SSLSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The streamed {@code /api/v1/batch} relay from a follower to the leader: read deadlines, body caps, upload deadlines and incremental acknowledgements.
 * <p>
 * One {@code @Nested} class per regression, each keeping the fixture and the Javadoc of the issue it pins.
 */
class LeaderForwardStreamedRelayTest {

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
  @Nested
  class StreamingRelayReadDeadline {
    private static final long   BUDGET_MS        = 1_000L;
    /** The tripwire between "the bound fired" and "the relay is unbounded" (forever, without the fix). */
    private static final long   GAVE_UP_BOUND_MS = 15_000L;
    /** How long the client waits on the follower before calling the relay unbounded. */
    private static final int    CLIENT_READ_MS   = 30_000;
    private static final String FIRST_LINE       = "{\"type\":\"progress\",\"verticesCreated\":1}";

    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

    private Undertow follower;

    @AfterEach
    void stopFollower() {
      if (follower != null)
        follower.stop();
    }

    /** The defect as filed: headers plus the first progress line, then silence. */
    @Test
    void aLeaderThatStallsMidStreamIsGivenUpOnAndItsConnectionClosed() throws Exception {
      try (final FakeLeader leader = FakeLeader.scripted(out -> {
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
        assertThat(leader.awaitConnectionClosedByClient(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS))
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

      try (final FakeLeader leader = FakeLeader.scripted(out -> {
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
      try (final FakeLeader leader = FakeLeader.scripted(out -> {
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

      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
      final HttpServer httpServer = HTTP_SERVERS.of(server);
      final PostBatchHandler handler = new PostBatchHandler(httpServer);

      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getLeaderAddress()).thenReturn(leaderAddress);
      when(ha.getClusterToken()).thenReturn("test-token");
      final ServerSecurityUser user = TestServerHelper.securityUser("root");

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
  }

  /**
   * Regression test for issue #8161: a chunked batch upload over {@code arcadedb.server.httpBodyContentMaxSize} sent
   * to a FOLLOWER tripped the capped {@link PostBatchHandler.CountingInputStream} while the JDK client was relaying
   * it to the leader. The refusal surfaced out of {@code HttpClient.send} wrapped in a plain {@link IOException} and
   * landed in {@code forwardBatchToLeader}'s generic arm, which RETURNED a hand-built 503 "Error forwarding batch to
   * leader" - a retryable status, blaming the leader, for a request that can only ever be refused. The leader answers
   * the same request with the documented JSON 413, built by {@code sendMappedErrorResponse}; the follower now
   * rethrows the {@link RequestTooBigException} so the same arm builds the same answer.
   */
  @Nested
  class FollowerForwardBodyCap {
    private static final long CAP_BYTES  = 1_024L;
    private static final int  BODY_BYTES = 256 * 1_024;

    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

    private static PostBatchHandler handler() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, 60_000L);
      cfg.setValue(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE, CAP_BYTES);
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

    private static byte[] ndjson(final int size) {
      final StringBuilder sb = new StringBuilder(size + 64);
      int i = 0;
      while (sb.length() < size)
        sb.append("{\"@type\":\"vertex\",\"@id\":\"v").append(i++).append("\",\"@class\":\"V\"}\n");
      return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * The follower relays a body with no declared length (a bare exchange answers -1, as a chunked upload does) that
     * is far over this node's cap. Both encodings take the same path: the cap trips while the leader is still
     * reading, before it has sent any response header, so nothing is on the wire yet and the status is still ours.
     */
    @ParameterizedTest(name = "streaming={0}")
    @ValueSource(booleans = { false, true })
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void anOverCapChunkedBodyIsRefusedAs413NotAnsweredAs503(final boolean streaming) throws Exception {
      try (final FakeLeader leader = FakeLeader.draining()) {
        final HttpServerExchange exchange = new HttpServerExchange(null);
        assertThat(exchange.getRequestContentLength()).isEqualTo(-1L);
        final PostBatchHandler.CountingInputStream body = new PostBatchHandler.CountingInputStream(exchange,
            new ByteArrayInputStream(ndjson(BODY_BYTES)), CAP_BYTES);

        final PostBatchHandler handler = handler();
        final Throwable thrown = catchThrowable(() -> handler.forwardBatchToLeader(exchange,
            haPointingAt(leader.address()), "mydb", rootUser(), "application/x-ndjson", body, streaming));
        assertThat(thrown).isInstanceOf(RequestTooBigException.class)
            .hasMessageContaining(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE.getKey());

        // The user-visible contract, not just the intermediate step: the rethrown refusal goes through the same
        // classification sendMappedErrorResponse sends from, and comes out as the leader's 413 naming the setting.
        final AbstractServerHttpHandler.ErrorClassification classification = handler.classifyError(thrown);
        assertThat(classification.status()).isEqualTo(413);
        assertThat(classification.message()).contains(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE.getKey());
        assertThat(classification.exceptionArgs()).isEqualTo(String.valueOf(CAP_BYTES));

        assertThat(body.hasBodyFailed()).isTrue();
        // The kernel backlog can take the relayed bytes before accept() returns on the fixture thread: wait for it.
        assertThat(leader.awaitFirstConnection(30, TimeUnit.SECONDS)).isTrue();
      }
    }

    /**
     * The discrimination has to be on WHY the forward failed, not on the fact that it did: a leader that drops the
     * connection while a body UNDER the cap is being relayed is still a leader-side failure, not the 413 of the cap. Since
     * issue #11899 that failure is answered 504 outcome unknown rather than 503: from this node a connection the leader
     * closed cannot be told apart from one it closed after taking the load.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aLeaderThatDropsTheConnectionIsALeaderSideFailureNotTheCap() throws Exception {
      try (final FakeLeader leader = FakeLeader.dropping()) {
        final HttpServerExchange exchange = new HttpServerExchange(null);
        final PostBatchHandler.CountingInputStream body = new PostBatchHandler.CountingInputStream(exchange,
            new ByteArrayInputStream(ndjson(512)), CAP_BYTES);

        final ExecutionResponse response = handler().forwardBatchToLeader(exchange, haPointingAt(leader.address()),
            "mydb", rootUser(), "application/x-ndjson", body, false);

        assertThat(response.getCode()).isEqualTo(504);
        assertThat(new JSONObject(response.getResponse()).getString("exception", ""))
            .isEqualTo(TransactionOutcomeUnknownException.class.getName());
        assertThat(body.hasBodyFailed()).isFalse();
      }
    }
  }

  /**
   * Regression test for issue #8674, the streaming sub-path #8161 left open: a chunked {@code /api/v1/batch} upload sent
   * to a FOLLOWER with {@code Accept: application/x-ndjson} crossing {@code arcadedb.server.httpBodyContentMaxSize} AFTER
   * the leader had already answered 200 and a first progress line. No status is left to change there, and the relay
   * ended the client's stream without a terminal line, logging the failure as the leader's. The leader, fed the same
   * body directly, writes an in-band {@code error} line carrying {@code "status": 413}; the relay now writes that line.
   * <p>
   * The relay is driven here with a scripted leader answer, which reaches every shape of the leader's ending. The live
   * forward reaches the same path since issue #9216 sent it full duplex ({@link DuplexHttpExchange}), and
   * {@code LeaderForwardStreamedRelayTest.StreamedForwardIncrementalAcks} drives it there. Before that the forward used {@link HttpClient},
   * which hands back an HTTP/1.1 response only after the request body has been published whole, so a cap trip always
   * failed the send itself; {@link #theJdkClientHandsBackTheResponseOnlyOnceTheUploadIsPublished} still pins that JDK
   * behaviour, as the reason the streaming forward does not go back to it.
   * <p>
   * Driven through a real Undertow exchange, like {@link LeaderForwardStreamedRelayTest.StreamingRelayReadDeadline}, because the relay
   * writes through the exchange's own output stream.
   */
  @Nested
  class StreamedRelayBodyCapTerminalLine {
    private static final long   CAP_BYTES      = 1_024L;
    private static final long   BUDGET_MS      = 10_000L;
    private static final int    CLIENT_READ_MS = 30_000;
    /** Written by the leader's own writer, so a change to its format breaks this test rather than the relay's reading. */
    private static final String PROGRESS_LINE  = leaderLine("progress",
        new JSONObject().put("phase", "vertices").put("verticesCreated", 7L).put("edgesCreated", 0L));

    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

    private Undertow follower;

    @AfterEach
    void stopFollower() {
      if (follower != null)
        follower.stop();
    }

    /** The defect as filed: the leader streamed a progress line, then this node's cap cut the relayed upload. */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aCapTripAfterTheLeaderStartedStreamingEndsWithTheLeadersIn413ErrorLine() throws Exception {
      final List<String> lines = relay(overCapBody(), PROGRESS_LINE + "\n", true);

      assertThat(lines).as("the leader's progress line, then exactly one terminal line").hasSize(2);
      assertThat(lines.get(0)).isEqualTo(PROGRESS_LINE);
      assertIn413ErrorLine(lines.get(1), 7L);
    }

    /**
     * The cut does not always surface as a failed read: a leader whose answer simply ends after the upload was cut leaves
     * the relay at end of stream, with the same missing terminal line. The stream is asked, not the failure.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aCapTripFollowedByTheLeaderEndingItsAnswerStillGetsTheIn413ErrorLine() throws Exception {
      final List<String> lines = relay(overCapBody(), PROGRESS_LINE + "\n", false);

      assertThat(lines).hasSize(2);
      assertThat(lines.get(0)).isEqualTo(PROGRESS_LINE);
      assertIn413ErrorLine(lines.get(1), 7L);
    }

    /**
     * A leader cut mid-line leaves a fragment as the last line relayed. The counters still come from the last progress
     * line: a fragment carries none, and reading them off it would claim nothing was loaded - {@code partialCommit:
     * false} - to a client that would then re-send the whole load on top of what the leader already committed.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aFragmentAfterTheLastProgressLineDoesNotResetTheCounters() throws Exception {
      final String fragment = "{\"progress\":{\"phase\":\"verti";
      final List<String> lines = relay(overCapBody(), PROGRESS_LINE + "\n" + fragment, false);

      assertThat(lines).hasSize(3);
      assertThat(lines.get(0)).isEqualTo(PROGRESS_LINE);
      assertThat(lines.get(1)).isEqualTo(fragment);
      assertIn413ErrorLine(lines.get(2), 7L);
    }

    /** No acknowledgement relayed yet: the line still goes out, with counters that claim nothing was loaded. */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aCapTripBeforeAnyProgressLineReportsNoPartialCommit() throws Exception {
      final List<String> lines = relay(overCapBody(), "", true);

      assertThat(lines).hasSize(1);
      assertIn413ErrorLine(lines.get(0), 0L);
    }

    /**
     * The leader may already have written its own terminal line before the connection died. The stream then has its
     * ending, and a second terminal line would make it contradict itself.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aTerminalLineTheLeaderAlreadyWroteIsNotFollowedByASecondOne() throws Exception {
      final String leaderError = "{\"error\":{\"error\":\"truncated\",\"status\":400}}";
      final List<String> lines = relay(overCapBody(), PROGRESS_LINE + "\n" + leaderError + "\n", true);

      assertThat(lines).containsExactly(PROGRESS_LINE, leaderError);
    }

    /** A terminal line stays the ending whatever the leader wrote after it: nothing is appended. */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aLineAfterTheLeadersTerminalLineDoesNotReopenTheStream() throws Exception {
      final String leaderError = "{\"error\":{\"error\":\"truncated\",\"status\":400}}";
      final List<String> lines = relay(overCapBody(), PROGRESS_LINE + "\n" + leaderError + "\n" + PROGRESS_LINE + "\n", true);

      assertThat(lines).containsExactly(PROGRESS_LINE, leaderError, PROGRESS_LINE);
    }

    /** Same with a {@code summary}, the other terminal event, followed by a stray blank line that ends nothing. */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aSummaryTheLeaderAlreadyWroteIsNotFollowedByASecondTerminalLineEvenAfterABlankLine() throws Exception {
      final String summary = leaderLine("summary", new JSONObject().put("verticesCreated", 7L).put("edgesCreated", 0L));
      final List<String> lines = relay(overCapBody(), PROGRESS_LINE + "\n" + summary + "\n\n", false);

      assertThat(lines).containsExactly(PROGRESS_LINE, summary, "");
    }

    /**
     * The discrimination is on WHY the relay ended: a leader-side failure on a body this node did not refuse keeps the
     * contract the encoding is built on - no terminal line, so the client knows the answer did not arrive whole.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aLeaderFailureOnABodyUnderTheCapStillEndsWithoutATerminalLine() throws Exception {
      final PostBatchHandler.CountingInputStream underCap = new PostBatchHandler.CountingInputStream(null,
          new ByteArrayInputStream(new byte[16]), CAP_BYTES);
      underCap.readAllBytes();
      assertThat(underCap.refusedOverCap()).isNull();

      assertThat(relay(underCap, PROGRESS_LINE + "\n", true)).containsExactly(PROGRESS_LINE);
    }

    /**
     * Why the streaming forward does not use the JDK client (issue #9216): it holds the response back until the upload is
     * published whole, so a client loading through a follower would see none of the leader's acknowledgements until its
     * upload ended. If this starts failing, a JDK now delivers the response mid-upload and {@link DuplexHttpExchange}
     * could be retired in favour of it.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void theJdkClientHandsBackTheResponseOnlyOnceTheUploadIsPublished() throws Exception {
      try (final FakeLeader leader = FakeLeader.scripted(StreamedRelayBodyCapTerminalLine::answerWithOneProgressLine)) {
        final CountDownLatch release = new CountDownLatch(1);
        // Trickles a byte at a time until the leader has answered - so the request, headers and chunks, keeps reaching
        // it whatever thread the JDK happens to pull the body on - and only then holds the rest of the upload back.
        final InputStream pendingUpload = new InputStream() {
          @Override
          public int read() throws IOException {
            final byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
          }

          @Override
          public int read(final byte[] b, final int off, final int len) throws IOException {
            try {
              if (!leader.awaitAnswered(50, TimeUnit.MILLISECONDS)) {
                b[off] = '\n';
                return 1;
              }
              release.await(CLIENT_READ_MS, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return -1;
          }
        };

        final HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        try {
          final CompletableFuture<HttpResponse<InputStream>> sent = client.sendAsync(
              PostBatchHandler.buildForwardRequest("http://" + leader.address() + "/api/v1/batch/mydb",
                  "application/x-ndjson", "test-token", "root", -1, pendingUpload, NdJsonResultStream.CONTENT_TYPE),
              HttpResponse.BodyHandlers.ofInputStream());

          assertThat(leader.awaitAnswered(CLIENT_READ_MS, TimeUnit.MILLISECONDS))
              .as("the leader answered 200 and a progress line while the upload is still pending").isTrue();
          assertThat(catchThrowable(() -> sent.get(2, TimeUnit.SECONDS)))
              .as("the response is withheld while the upload is pending - if this fails, a JDK now delivers it mid-upload: "
                  + "not an ArcadeDB regression, see #8719").isInstanceOf(TimeoutException.class);

          release.countDown();
          assertThat(sent.get(CLIENT_READ_MS, TimeUnit.MILLISECONDS).statusCode())
              .as("and handed back once it has been published").isEqualTo(200);
        } finally {
          release.countDown();
          client.shutdownNow();
        }
      }
    }

    // ---------------------------------------------------------------------------------------------------------

    private static String leaderLine(final String kind, final JSONObject body) {
      final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try (final NdJsonResultStream stream = new NdJsonResultStream(buffer)) {
        stream.writeEvent(kind, body, true);
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
      return buffer.toString(StandardCharsets.UTF_8).stripTrailing();
    }

    private static void assertIn413ErrorLine(final String line, final long verticesCreated) {
      final JSONObject terminal = new JSONObject(line);
      assertThat(terminal.has("error")).as("the terminal line is an in-band error: " + line).isTrue();
      final JSONObject error = terminal.getJSONObject("error");
      assertThat(error.getInt("status", 0)).isEqualTo(413);
      assertThat(error.getString("error", "")).contains(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE.getKey());
      assertThat(error.getString("exception", "")).isEqualTo(RequestTooBigException.class.getName());
      assertThat(error.getString("exceptionArgs", "")).isEqualTo(String.valueOf(CAP_BYTES));
      // The counters of the last acknowledgement relayed, as the leader's own error line carries them.
      assertThat(error.getLong("verticesCreated", -1)).isEqualTo(verticesCreated);
      assertThat(error.getLong("edgesCreated", -1)).isEqualTo(0L);
      assertThat(error.getBoolean("partialCommit", true)).isEqualTo(verticesCreated > 0);
    }

    /** A body this node's cap has refused, as the JDK publisher thread leaves it after the cut. */
    private static PostBatchHandler.CountingInputStream overCapBody() {
      final PostBatchHandler.CountingInputStream body = new PostBatchHandler.CountingInputStream(null,
          new ByteArrayInputStream(new byte[(int) CAP_BYTES * 4]), CAP_BYTES);
      assertThat(catchThrowable(body::readAllBytes)).isInstanceOf(RequestTooBigException.class);
      assertThat(body.refusedOverCap()).isNotNull();
      return body;
    }

    /**
     * Runs the relay behind an Undertow listener on a scripted leader answer - {@code leaderLines}, then a failed read
     * when {@code failAtEnd} (the aborted connection) or a clean end of stream otherwise - and returns what the client
     * received.
     */
    private List<String> relay(final PostBatchHandler.CountingInputStream body, final String leaderLines,
        final boolean failAtEnd) throws IOException {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, BUDGET_MS);
      cfg.setValue(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE, CAP_BYTES);
      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
      final HttpServer httpServer = HTTP_SERVERS.of(server);
      final PostBatchHandler handler = new PostBatchHandler(httpServer);

      final byte[] scripted = leaderLines.getBytes(StandardCharsets.UTF_8);
      final InputStream leaderBody = new InputStream() {
        private int pos;

        @Override
        public int read() throws IOException {
          if (pos < scripted.length)
            return scripted[pos++] & 0xFF;
          if (failAtEnd)
            throw new IOException("the relayed request was aborted");
          return -1;
        }
      };

      follower = Undertow.builder()
          .addHttpListener(0, "127.0.0.1")
          .setHandler(new BlockingHandler(exchange -> handler.relayNdJsonFromLeader(exchange, "mydb",
              "http://leader/api/v1/batch/mydb", new ScriptedResponse(leaderBody), BUDGET_MS, body)))
          .build();
      follower.start();

      final InetSocketAddress address = (InetSocketAddress) follower.getListenerInfo().get(0).getAddress();
      final HttpURLConnection conn = (HttpURLConnection) URI.create(
          "http://127.0.0.1:" + address.getPort() + "/api/v1/batch/mydb").toURL().openConnection();
      conn.setRequestProperty("Accept", "application/x-ndjson");
      conn.setReadTimeout(CLIENT_READ_MS);
      assertThat(conn.getResponseCode()).isEqualTo(200);

      final List<String> lines = new ArrayList<>();
      try (final BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
        for (String line = in.readLine(); line != null; line = in.readLine())
          lines.add(line);
      }
      return lines;
    }

    /** The leader's 200 streaming answer, as {@code HttpClient} hands it to the relay. */
    private static final class ScriptedResponse implements HttpResponse<InputStream> {
      private final InputStream body;

      ScriptedResponse(final InputStream body) {
        this.body = body;
      }

      @Override
      public int statusCode() {
        return 200;
      }

      @Override
      public HttpHeaders headers() {
        return HttpHeaders.of(Map.of("Content-Type", List.of(NdJsonResultStream.CONTENT_TYPE)), (a, b) -> true);
      }

      @Override
      public InputStream body() {
        return body;
      }

      @Override
      public HttpRequest request() {
        return null;
      }

      @Override
      public Optional<HttpResponse<InputStream>> previousResponse() {
        return Optional.empty();
      }

      @Override
      public Optional<SSLSession> sslSession() {
        return Optional.empty();
      }

      @Override
      public URI uri() {
        return URI.create("http://leader/api/v1/batch/mydb");
      }

      @Override
      public HttpClient.Version version() {
        return HttpClient.Version.HTTP_1_1;
      }
    }

    /**
     * Answers 200 and a progress line at once - before the upload is over, as a leader streaming a large load does -
     * after which the leader drains the upload until the client closes the connection.
     */
    private static void answerWithOneProgressLine(final OutputStream out) throws IOException {
      final byte[] line = (PROGRESS_LINE + "\n").getBytes(StandardCharsets.UTF_8);
      out.write(("HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nTransfer-Encoding: chunked\r\n\r\n"
          + Integer.toHexString(line.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
      out.write(line);
      out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
    }
  }

  /**
   * Regression test for issue #8719: a streamed {@code /api/v1/batch} load sent to a FOLLOWER waited for the leader's
   * response with {@code arcadedb.ha.proxyBatchReadTimeout} counted from the start of the send. On HTTP/1.1 the JDK
   * client hands the response back only once the relayed upload has been published whole (pinned by
   * {@code LeaderForwardStreamedRelayTest.StreamedRelayBodyCapTerminalLine.theJdkClientHandsBackTheResponseOnlyOnceTheUploadIsPublished}),
   * so that deadline capped the length of the whole upload: a load whose upload outlasted it was answered 504 although
   * the leader was working and answering. The deadline now counts from the last byte of the upload relayed, so an upload
   * that keeps moving is never cut off, and one that stops moving with no answer is still given up on. The bound itself is
   * tested in {@code network}, by {@code Issue8719SendWhileProgressingTest}.
   */
  @Nested
  class StreamedForwardUploadDeadline {
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

    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

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
    @Tag("slow")
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

  /**
   * Regression test for issue #9216: a streamed {@code /api/v1/batch} load sent to a FOLLOWER saw none of the leader's
   * progress lines until its whole upload had been relayed, because the JDK client the forward used hands back an
   * HTTP/1.1 response only once the request body has been published whole. The forward is now sent full duplex, so the
   * leader's acknowledgements cross the hop while the upload is still going, and #8674's in-band 413 for a cap trip after
   * the leader started answering is reached on the live forward rather than only by a scripted one.
   */
  @Nested
  class StreamedForwardIncrementalAcks {
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
}
