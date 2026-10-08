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
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.http.FakeLeader;
import com.arcadedb.server.http.HttpServer;
import io.undertow.Undertow;
import io.undertow.server.RequestTooBigException;
import io.undertow.server.handlers.BlockingHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.net.ssl.SSLSession;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for issue #8674, the streaming sub-path #8161 left open: a chunked {@code /api/v1/batch} upload sent
 * to a FOLLOWER with {@code Accept: application/x-ndjson} crossing {@code arcadedb.server.httpBodyContentMaxSize} AFTER
 * the leader had already answered 200 and a first progress line. No status is left to change there, and the relay
 * ended the client's stream without a terminal line, logging the failure as the leader's. The leader, fed the same
 * body directly, writes an in-band {@code error} line carrying {@code "status": 413}; the relay now writes that line.
 * <p>
 * The relay is driven here with a scripted leader answer, which reaches every shape of the leader's ending. The live
 * forward reaches the same path since issue #9216 sent it full duplex ({@link DuplexHttpExchange}), and
 * {@code Issue9216StreamedForwardIncrementalAcksTest} drives it there. Before that the forward used {@link HttpClient},
 * which hands back an HTTP/1.1 response only after the request body has been published whole, so a cap trip always
 * failed the send itself; {@link #theJdkClientHandsBackTheResponseOnlyOnceTheUploadIsPublished} still pins that JDK
 * behaviour, as the reason the streaming forward does not go back to it.
 * <p>
 * Driven through a real Undertow exchange, like {@link Issue7738StreamingBatchRelayReadDeadlineTest}, because the relay
 * writes through the exchange's own output stream.
 */
class Issue8674StreamedRelayBodyCapTerminalLineTest {
  /**
   * Real HTTP servers the helpers build: each owns cleanup threads that only stopService() ends. Static because the
   * helpers are, and safe because this class's tests run one at a time.
   */
  private static final List<HttpServer> HTTP_SERVERS = new ArrayList<>();

  private static final long   CAP_BYTES      = 1_024L;
  private static final long   BUDGET_MS      = 10_000L;
  private static final int    CLIENT_READ_MS = 30_000;
  /** Written by the leader's own writer, so a change to its format breaks this test rather than the relay's reading. */
  private static final String PROGRESS_LINE  = leaderLine("progress",
      new JSONObject().put("phase", "vertices").put("verticesCreated", 7L).put("edgesCreated", 0L));

  private Undertow follower;

  @AfterEach
  void stopFollower() {
    HTTP_SERVERS.forEach(HttpServer::stopService);
    HTTP_SERVERS.clear();
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
    try (final FakeLeader leader = FakeLeader.scripted(Issue8674StreamedRelayBodyCapTerminalLineTest::answerWithOneProgressLine)) {
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
    final HttpServer httpServer = new HttpServer(server);
    HTTP_SERVERS.add(httpServer);
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
