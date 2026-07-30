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
package com.arcadedb.server;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpTimeoutException;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A batch load that fails on the CONTENT of the payload must say so to the client, at once.
 * <p>
 * Reported on issue #5470 with a 25M-line upload: an edge referenced a vertex that the file never created, the server
 * diagnosed it exactly - {@code Unknown temporary ID '...' at line 25124852} - and the client was told nothing for
 * fifteen minutes, after which the status could no longer be set at all
 * ({@code UT000002: The response has already been started}). From the outside that is a load that hangs and then dies
 * without a reason, which is what the issue was originally opened about.
 * <p>
 * Three defects sat behind it, and this class pins all of them:
 * <ul>
 *   <li>the answer waited for the REST of the upload. The failing record has already been read, so nothing about the
 *   remaining bytes can change the verdict - but closing Undertow's request stream reads the body to the end first,
 *   and the try-with-resources did exactly that before the error was ever surfaced;</li>
 *   <li>by then the response had been started, so the 400 never reached the client;</li>
 *   <li>and the truncation probe was consulted for a failure truncation cannot cause, reporting a valid line of the
 *   user's file as "not part of the payload".</li>
 * </ul>
 * Most cases here announce a large body and then never send the rest of it, so a server that insists on having it
 * cannot answer at all and the test times out - which is precisely the user's experience.
 * {@link #aStreamingHttpClientReceivesTheErrorMidUpload} is the realistic shape: a real client streaming a large
 * payload that keeps flowing while the server refuses it. The last cases guard what must NOT change - a genuinely cut
 * upload is still reported as truncated rather than blamed on the client, and a load that succeeds still leaves its
 * connection reusable.
 * <p>
 * Note on the two cases that write in a tight loop without reading: refusing to read a body means closing a
 * connection the peer is still writing to, so those may observe a reset instead of the answer. They assert on what
 * arrives, and on arriving PROMPTLY, never on the reset - see {@link #readResponseOrReset}. Delivery to a client that
 * behaves normally is pinned by the {@code HttpClient} case instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue5470BatchErrorDeliveryIT extends BaseGraphServerTest {

  /** Generous next to a verdict the server can reach immediately, unforgiving next to waiting for the upload. */
  private static final int  RESPONSE_TIMEOUT_MS   = 20_000;
  /** A verdict the server can reach immediately must land inside this, however the connection ends. */
  private static final long PROMPT_ANSWER_MS     = 10_000;
  /** Announced but never sent, so the server can only answer by deciding it does not need it. */
  private static final long ANNOUNCED_EXTRA_BYTES = 64L * 1024 * 1024;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    // Long enough that a hang is a hang and not this timeout firing and rescuing the test.
    GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.setValue(120_000);
    GlobalConfiguration.SERVER_HTTP_STREAMING_READ_TIMEOUT.setValue(120_000);
  }

  @Test
  void anEdgeReferencingAnUnknownVertexIsReportedWithoutWaitingForTheRestOfTheUpload() throws Exception {
    final StringBuilder payload = new StringBuilder();
    // A few good vertices, so the failure is unambiguously about the edge and not an empty batch.
    appendVertices(payload, 900_000, 50);
    // ... then an edge pointing at a vertex this payload never creates.
    payload.append("{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":\"v0\",\"@to\":\"nonexistent-vertex\"}\n");

    final Response response = postAndStopSending(payload.toString());

    assertThat(response.status()).as("the server must answer at all").isNotNull();
    assertThat(response.status()).as("an edge referencing an unknown vertex is a client error, not a 500 and not a 200")
        .contains("400");
    assertThat(response.body()).as("the answer must name what was wrong, so the file can be fixed")
        .contains("nonexistent-vertex");
    assertThat(response.body()).as("... and where").contains("line");
    assertThat(response.body()).as("the verdict is final: a record that was read in full is not a truncated upload")
        .doesNotContain("truncated");
  }

  /**
   * The same for a vertex that arrives after the first edge: also a well-formed record the server will not accept,
   * also a verdict no further byte can change.
   */
  @Test
  void aVertexAfterTheFirstEdgeIsReportedWithoutWaitingForTheRestOfTheUpload() throws Exception {
    final StringBuilder payload = new StringBuilder();
    appendVertices(payload, 910_000, 10);
    payload.append("{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":\"v0\",\"@to\":\"v1\"}\n");
    payload.append("{\"@type\":\"vertex\",\"@class\":\"V1\",\"@id\":\"late\",\"id\":919999}\n");

    final Response response = postAndStopSending(payload.toString());

    assertThat(response.status()).as("the server must answer at all").isNotNull();
    assertThat(response.status()).contains("400");
    assertThat(response.body()).as("the answer must explain the ordering rule that was broken")
        .contains("All vertices must appear before edges");
  }

  /**
   * The sharp edge of the classification, and the case the original bug reported. The client sends a bad edge and
   * then closes: the body HAS provably ended before the announced length, so the truncation check would fire - and
   * would answer 408 "the last record read is not part of the payload" about a record that was read in full and is
   * unquestionably in the user's file. The verdict on a well-formed record does not depend on the upload's fate.
   */
  @Test
  void anUnknownVertexIsBlamedOnThePayloadEvenWhenTheUploadAlsoEndedEarly() throws Exception {
    final StringBuilder payload = new StringBuilder();
    appendVertices(payload, 960_000, 50);
    payload.append("{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":\"v0\",\"@to\":\"nonexistent-vertex\"}\n");

    final byte[] sent = payload.toString().getBytes(StandardCharsets.UTF_8);

    try (final Socket socket = openSocket()) {
      final OutputStream out = socket.getOutputStream();
      out.write(requestHeaders(sent.length + ANNOUNCED_EXTRA_BYTES).getBytes(StandardCharsets.US_ASCII));
      out.write(sent);
      out.flush();
      // Less than announced, and provably finished: both a truncated upload AND an invalid record.
      socket.shutdownOutput();

      final Response response = readResponse(socket);

      assertThat(response.status()).as("the server must answer at all").isNotNull();
      assertThat(response.status()).as("the record is invalid on its own terms: that is a 400, not a 408 about bytes")
          .contains("400");
      assertThat(response.body()).as("and it must still name what was wrong").contains("nonexistent-vertex");
    }
  }

  /**
   * The reported shape: the client is streaming a huge file and keeps streaming while the server decides it cannot
   * use it. The answer must not wait for the upload to finish - and it must survive the server hanging up on a peer
   * that is still writing, which is what makes this different from a client that politely stops.
   */
  @Test
  void theErrorReachesAClientThatIsStillUploading() throws Exception {
    final StringBuilder head = new StringBuilder();
    appendVertices(head, 920_000, 50);
    head.append("{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":\"v0\",\"@to\":\"nonexistent-vertex\"}\n");
    final byte[] sent = head.toString().getBytes(StandardCharsets.UTF_8);

    final AtomicBoolean stopWriting = new AtomicBoolean();

    try (final Socket socket = openSocket()) {
      final OutputStream out = socket.getOutputStream();
      out.write(requestHeaders(sent.length + ANNOUNCED_EXTRA_BYTES).getBytes(StandardCharsets.US_ASCII));
      out.write(sent);
      out.flush();

      // Keep the upload going, as a real bulk load would. These bytes are valid records that will never be read;
      // the writer dies as soon as the server hangs up, which is expected and must not fail the test.
      final Thread writer = new Thread(() -> {
        final byte[] more = "{\"@type\":\"vertex\",\"@class\":\"V1\",\"id\":999999}\n".getBytes(StandardCharsets.UTF_8);
        try {
          while (!stopWriting.get()) {
            out.write(more);
            out.flush();
          }
        } catch (final IOException expected) {
          // The server closed the read side of a connection it is no longer using: exactly the intent.
        }
      });
      writer.setDaemon(true);
      writer.start();

      try {
        final Response response = readResponseOrReset(socket);
        if (response != null) {
          assertThat(response.status()).contains("400");
          assertThat(response.body()).contains("nonexistent-vertex");
        }
      } finally {
        stopWriting.set(true);
        writer.join(RESPONSE_TIMEOUT_MS);
      }
    }
  }

  /**
   * The guard on the classification. A body that stops mid-record IS a truncated upload, and the malformed last line
   * it produces must not be blamed on the client: that line is not in their file. This is the case the truncation
   * probe exists for, and it has to keep working now that a well-formed-but-invalid record bypasses it.
   */
  @Test
  void aRecordCutInHalfIsStillReportedAsATruncatedUpload() throws Exception {
    final StringBuilder payload = new StringBuilder();
    appendVertices(payload, 930_000, 50);
    // A record the client never finished sending: no closing brace, no newline.
    payload.append("{\"@type\":\"vertex\",\"@class\":\"V1\",\"id\":93");

    final byte[] sent = payload.toString().getBytes(StandardCharsets.UTF_8);

    try (final Socket socket = openSocket()) {
      final OutputStream out = socket.getOutputStream();
      // vertexBatchSize=1 commits every record, so the counts the 408 reports are exactly what reached the database.
      out.write(requestHeaders(sent.length + ANNOUNCED_EXTRA_BYTES, "?vertexBatchSize=1")
          .getBytes(StandardCharsets.US_ASCII));
      out.write(sent);
      out.flush();
      // A clean half-close: the announced bytes provably stop here.
      socket.shutdownOutput();

      final Response response = readResponse(socket);

      assertThat(response.status()).as("a cut upload is a 408 to resume, not a 400 blaming the payload")
          .contains("408");
      assertThat(response.body()).as("and it must carry the counts needed to resume").isNotNull();
      final JSONObject error = new JSONObject(response.body());
      assertThat(error.getLong("verticesCreated")).isEqualTo(50);
      assertThat(error.getBoolean("partialCommit")).isTrue();
    }
  }

  /**
   * The other side of the classification: a record that really is malformed, from a client that really is still
   * sending. Truncation cannot be established here - the body has not ended - so the load must be reported as the
   * bad payload it is, promptly. This is the only test that exercises the truncation probe's read loop, since a cut
   * upload short-circuits on the end of the body; the probe must look at the bytes it already has and give up
   * rather than spend the response waiting for more.
   */
  @Test
  void aMalformedRecordFromALiveClientIsReportedAsABadPayload() throws Exception {
    final StringBuilder head = new StringBuilder();
    appendVertices(head, 950_000, 20);
    // A line that is not JSON at all, and a client that keeps sending after it.
    head.append("this is not json\n");
    final byte[] sent = head.toString().getBytes(StandardCharsets.UTF_8);

    final AtomicBoolean stopWriting = new AtomicBoolean();

    try (final Socket socket = openSocket()) {
      final OutputStream out = socket.getOutputStream();
      out.write(requestHeaders(sent.length + ANNOUNCED_EXTRA_BYTES).getBytes(StandardCharsets.US_ASCII));
      out.write(sent);
      out.flush();

      final Thread writer = new Thread(() -> {
        final byte[] more = "{\"@type\":\"vertex\",\"@class\":\"V1\",\"id\":999998}\n".getBytes(StandardCharsets.UTF_8);
        try {
          while (!stopWriting.get()) {
            out.write(more);
            out.flush();
          }
        } catch (final IOException expected) {
          // The server hung up on a connection it is no longer reading: expected.
        }
      });
      writer.setDaemon(true);
      writer.start();

      try {
        final Response response = readResponseOrReset(socket);
        if (response != null) {
          assertThat(response.status()).as("the body did not end, so this is a 400 about the payload, not a 408")
              .contains("400");
          assertThat(response.body()).as("and it must name the line to fix").contains("line 21");
        }
      } finally {
        stopWriting.set(true);
        writer.join(RESPONSE_TIMEOUT_MS);
      }
    }
  }

  /**
   * A malformed record from a client that has gone quiet without closing: the one case where the truncation check
   * cannot reach a verdict either way. It must not wait to find out. Blocking for the announced bytes here means
   * blocking for bytes that may never come - the load stalls on a client that is merely slow - so the check gives up
   * and the malformed record is reported as itself.
   */
  @Test
  void aMalformedRecordFromASilentClientIsAnsweredWithoutWaitingForMoreBytes() throws Exception {
    final StringBuilder payload = new StringBuilder();
    appendVertices(payload, 970_000, 20);
    payload.append("this is not json\n");

    // Announces more, sends none of it, and keeps the socket open: nothing can prove whether the rest is coming.
    final Response response = postAndStopSending(payload.toString());

    assertThat(response.status()).as("the server must not wait for bytes it cannot know are coming").isNotNull();
    assertThat(response.status()).contains("400");
    assertThat(response.body()).as("and must name the line to fix").contains("line 21");
  }

  /**
   * The reported shape with a REAL client: a large payload streamed by {@link HttpClient}, failing part-way.
   * <p>
   * An ordinary HTTP client reads the response while it uploads, so it usually IS handed the 400 mid-upload - but not
   * always, and the honest guarantee is narrower than that. Declining to read the rest of a body means closing a
   * connection the peer is still writing to, and the reset that follows can discard what the client had already
   * received; measured here, that costs the response body roughly one time in five. So delivery is asserted when it
   * happens, and the invariant this pins is the one that always holds: the client is never left WAITING. A hang -
   * which is what draining the remainder would cause, and what the issue was about - fails the test, both as a
   * request timeout and on elapsed time.
   * <p>
   * Deterministic delivery is pinned elsewhere, on the shapes where TCP cannot interfere:
   * {@link #aRejectedButCompletePayloadKeepsItsConnection} (body fully arrived) and the cases where the client has
   * stopped sending.
   */
  @Test
  void aStreamingHttpClientReceivesTheErrorMidUpload() throws Exception {
    // A body that fails at record 51 and then keeps going for far longer than the answer takes.
    final InputStream body = new SequenceInputStream(new Enumeration<>() {
      private int chunk;

      @Override
      public boolean hasMoreElements() {
        return chunk < 20_000;
      }

      @Override
      public InputStream nextElement() {
        final StringBuilder text = new StringBuilder();
        if (chunk++ == 0) {
          appendVertices(text, 990_000, 50);
          text.append("{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":\"v0\",\"@to\":\"nonexistent-vertex\"}\n");
        } else
          for (int i = 0; i < 100; i++)
            text.append("{\"@type\":\"vertex\",\"@class\":\"V1\",\"id\":999997}\n");
        return new ByteArrayInputStream(text.toString().getBytes(StandardCharsets.UTF_8));
      }
    });

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:2480/api/v1/batch/" + getDatabaseName()))
        .header("Authorization", "Basic " + basicAuth())
        .header("Content-Type", "application/x-ndjson")
        .timeout(Duration.ofMillis(RESPONSE_TIMEOUT_MS))
        .POST(HttpRequest.BodyPublishers.ofInputStream(() -> body))
        .build();

    final long start = System.currentTimeMillis();
    try (final HttpClient client = HttpClient.newHttpClient()) {
      final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      assertThat(response.statusCode()).as("a streaming client that is answered must be answered correctly")
          .isEqualTo(400);
      assertThat(response.body()).as("and told which record to fix").contains("nonexistent-vertex");
    } catch (final HttpTimeoutException hang) {
      throw new AssertionError("the server must not make a streaming client wait for its own upload", hang);
    } catch (final IOException reset) {
      // The server stopped reading a body it will not use, and closed. A client in the middle of writing can lose
      // the response to that; it cannot lose the fact that the load failed, and the reason is in the server log.
    }

    assertThat(System.currentTimeMillis() - start)
        .as("answered or reset, it must resolve at once rather than after the upload")
        .isLessThan(PROMPT_ANSWER_MS);
  }

  /**
   * The production shape, and the only test that can see {@code exchange.setPersistent(false)} do its job.
   * <p>
   * Every other failing case here sends {@code Connection: close}, which makes the exchange non-persistent before
   * the handler ever runs - so Undertow would not drain the remainder at {@code endExchange} whether or not the
   * handler retired the connection itself. A real bulk loader uses a pooled client and sends no such header, and
   * then the abandoned remainder is only skipped because the handler asked for it. Without that, Undertow keeps the
   * connection alive, which means first reading the megabytes that are never coming.
   */
  @Test
  void aFailedBatchOnAKeepAliveConnectionIsAnsweredAndTheConnectionRetired() throws Exception {
    final StringBuilder payload = new StringBuilder();
    appendVertices(payload, 980_000, 50);
    payload.append("{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":\"v0\",\"@to\":\"nonexistent-vertex\"}\n");
    final byte[] sent = payload.toString().getBytes(StandardCharsets.UTF_8);

    try (final Socket socket = openSocket()) {
      final OutputStream out = socket.getOutputStream();
      // Announces far more than it sends, and asks for keep-alive.
      out.write(keepAliveRequestHeaders(sent.length + ANNOUNCED_EXTRA_BYTES).getBytes(StandardCharsets.US_ASCII));
      out.write(sent);
      out.flush();

      final BufferedReader in = new BufferedReader(
          new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
      final Response response = readOneResponse(in);

      assertThat(response.status()).as("a keep-alive client must be answered without the rest of its upload")
          .isNotNull();
      assertThat(response.status()).contains("400");
      assertThat(response.body()).contains("nonexistent-vertex");

      // A connection whose request body was abandoned cannot carry another request - the unread remainder would be
      // read as the next request line - so the server has to retire it instead of keeping it alive.
      assertThat(in.read()).as("the connection must be closed, not returned to the pool with an unread body")
          .isEqualTo(-1);
    }
  }

  /**
   * A payload that arrived IN FULL and was rejected on a record in the middle must not cost the connection. Its
   * remainder is already buffered, so skipping it saves nothing and dropping a pooled connection on every rejected
   * small batch would be a needless regression - the point of not draining is the upload that has not arrived, not
   * the one that has. The second request over the same socket is the assertion.
   */
  @Test
  void aRejectedButCompletePayloadKeepsItsConnection() throws Exception {
    final StringBuilder payload = new StringBuilder();
    appendVertices(payload, 995_000, 50);
    // Fails here ...
    payload.append("{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":\"v0\",\"@to\":\"nonexistent-vertex\"}\n");
    // ... with this much of the payload already sent and unread.
    for (int i = 0; i < 10; i++)
      payload.append("{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":\"v0\",\"@to\":\"v1\"}\n");

    final byte[] sent = payload.toString().getBytes(StandardCharsets.UTF_8);

    try (final Socket socket = openSocket()) {
      final OutputStream out = socket.getOutputStream();
      final BufferedReader in = new BufferedReader(
          new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

      // An exact Content-Length: the whole body is sent, and the load simply stops early inside it.
      out.write(keepAliveRequestHeaders(sent.length).getBytes(StandardCharsets.US_ASCII));
      out.write(sent);
      out.flush();

      final Response rejected = readOneResponse(in);
      assertThat(rejected.status()).as("the rejection must arrive").isNotNull();
      assertThat(rejected.status()).contains("400");
      assertThat(rejected.body()).contains("nonexistent-vertex");

      // Same socket, second request: only possible if the server consumed the remainder and kept the connection.
      final StringBuilder good = new StringBuilder();
      appendVertices(good, 996_000, 5);
      final byte[] goodBytes = good.toString().getBytes(StandardCharsets.UTF_8);
      out.write(keepAliveRequestHeaders(goodBytes.length).getBytes(StandardCharsets.US_ASCII));
      out.write(goodBytes);
      out.flush();

      final Response reused = readOneResponse(in);
      assertThat(reused.status()).as("a connection whose body was fully received must still be usable").isNotNull();
      assertThat(reused.status()).contains("200");
      assertThat(new JSONObject(reused.body()).getLong("verticesCreated")).isEqualTo(5);
    }
  }

  /**
   * The same guarantee for a remainder too big to sit in one buffer. {@code available()} reports what is buffered
   * right now, so a tail spanning several of Undertow's buffers is the case where the drain could stop early - see
   * it report 0 at a boundary, conclude the client still owes bytes, and retire a connection whose body had in fact
   * arrived in full. Sized under {@link PostBatchHandler} MAX_ABANDONED_BODY_DRAIN so the cap is not what decides it.
   */
  @Test
  void aRejectedButCompletePayloadKeepsItsConnectionAcrossSeveralBuffers() throws Exception {
    final StringBuilder payload = new StringBuilder();
    appendVertices(payload, 985_000, 50);
    payload.append("{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":\"v0\",\"@to\":\"nonexistent-vertex\"}\n");
    // ~56KB of already-sent remainder: many Undertow buffer refills, and close enough to the drain budget
    // (MAX_ABANDONED_BODY_DRAIN, 64KB) that the whole range over which the connection is meant to survive is
    // exercised, not just the first buffer.
    while (payload.length() < 56_000)
      payload.append("{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":\"v0\",\"@to\":\"v1\"}\n");

    final byte[] sent = payload.toString().getBytes(StandardCharsets.UTF_8);

    try (final Socket socket = openSocket()) {
      final OutputStream out = socket.getOutputStream();
      final BufferedReader in = new BufferedReader(
          new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

      out.write(keepAliveRequestHeaders(sent.length).getBytes(StandardCharsets.US_ASCII));
      out.write(sent);
      out.flush();

      final Response rejected = readOneResponse(in);
      assertThat(rejected.status()).as("the rejection must arrive").isNotNull();
      assertThat(rejected.status()).contains("400");

      final StringBuilder good = new StringBuilder();
      appendVertices(good, 986_000, 5);
      final byte[] goodBytes = good.toString().getBytes(StandardCharsets.UTF_8);
      out.write(keepAliveRequestHeaders(goodBytes.length).getBytes(StandardCharsets.US_ASCII));
      out.write(goodBytes);
      out.flush();

      final Response reused = readOneResponse(in);
      assertThat(reused.status()).as("a multi-buffer remainder that had arrived must still keep the connection")
          .isNotNull();
      assertThat(reused.status()).contains("200");
    }
  }

  /**
   * What must not regress: the request stream is no longer closed by the handler, so a load that reads its body to
   * the end has to leave the connection exactly as usable as before. Two batches over one socket prove it.
   */
  @Test
  void aSuccessfulLoadLeavesTheConnectionReusable() throws Exception {
    try (final Socket socket = openSocket()) {
      final OutputStream out = socket.getOutputStream();
      final BufferedReader in = new BufferedReader(
          new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

      for (int request = 0; request < 2; request++) {
        final StringBuilder payload = new StringBuilder();
        appendVertices(payload, 940_000 + request * 100, 10);
        final byte[] sent = payload.toString().getBytes(StandardCharsets.UTF_8);

        out.write(keepAliveRequestHeaders(sent.length).getBytes(StandardCharsets.US_ASCII));
        out.write(sent);
        out.flush();

        final Response response = readOneResponse(in);
        assertThat(response.status()).as("request %s on a reused connection must be answered", request).isNotNull();
        assertThat(response.status()).as("request %s must succeed", request).contains("200");
        assertThat(new JSONObject(response.body()).getLong("verticesCreated")).isEqualTo(10);
      }
    }
  }

  private void appendVertices(final StringBuilder payload, final int firstId, final int count) {
    for (int i = 0; i < count; i++)
      payload.append("{\"@type\":\"vertex\",\"@class\":\"V1\",\"@id\":\"v").append(i).append("\",\"id\":")
          .append(firstId + i).append("}\n");
  }

  /**
   * Announces far more than it sends and then stops, so an answer can only come from a server that has decided it
   * does not need the rest. The socket stays open: this is a live client that simply has not sent the remainder.
   */
  private Response postAndStopSending(final String payload) throws Exception {
    final byte[] sent = payload.getBytes(StandardCharsets.UTF_8);

    try (final Socket socket = openSocket()) {
      final OutputStream out = socket.getOutputStream();
      out.write(requestHeaders(sent.length + ANNOUNCED_EXTRA_BYTES).getBytes(StandardCharsets.US_ASCII));
      out.write(sent);
      out.flush();

      return readResponse(socket);
    }
  }

  private Socket openSocket() throws IOException {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("127.0.0.1", 2480), 10_000);
    socket.setSoTimeout(RESPONSE_TIMEOUT_MS);
    return socket;
  }

  private String requestHeaders(final long contentLength) {
    return requestHeaders(contentLength, "");
  }

  private String requestHeaders(final long contentLength, final String queryString) {
    return "POST /api/v1/batch/" + getDatabaseName() + queryString + " HTTP/1.1\r\n"
        + "Host: 127.0.0.1:2480\r\n"
        + "Authorization: Basic " + basicAuth() + "\r\n"
        + "Content-Type: application/x-ndjson\r\n"
        + "Content-Length: " + contentLength + "\r\n"
        + "Connection: close\r\n"
        + "\r\n";
  }

  private String keepAliveRequestHeaders(final long contentLength) {
    return "POST /api/v1/batch/" + getDatabaseName() + " HTTP/1.1\r\n"
        + "Host: 127.0.0.1:2480\r\n"
        + "Authorization: Basic " + basicAuth() + "\r\n"
        + "Content-Type: application/x-ndjson\r\n"
        + "Content-Length: " + contentLength + "\r\n"
        + "\r\n";
  }

  private String basicAuth() {
    return Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Reads the answer to a request from a client that is writing in a tight loop and not reading until it is done -
   * the worst case there is. Refusing to read a body means closing a connection the peer is still writing to, and
   * the reset that follows discards whatever the kernel had already received, the answer included. So {@code null}
   * (reset) is an accepted outcome and the caller asserts only on what it did receive.
   * <p>
   * What is NOT optional is that this returns promptly: a read timeout propagates and fails the test, which is the
   * property these cases exist to pin. A client that reads while it uploads always gets the answer itself - see
   * {@link #aStreamingHttpClientReceivesTheErrorMidUpload}, which is the realistic shape.
   */
  private Response readResponseOrReset(final Socket socket) throws IOException {
    try {
      final Response response = readResponse(socket);
      return response.status() != null ? response : null;
    } catch (final SocketException reset) {
      // Reset by a server that has stopped reading: fast, and the reason is in the server log.
      return null;
    }
  }

  /** Reads a response whose end is the end of the stream ({@code Connection: close}). */
  private Response readResponse(final Socket socket) throws IOException {
    final BufferedReader in = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

    final String status = in.readLine();

    String line;
    while ((line = in.readLine()) != null && !line.isEmpty())
      ;

    final StringBuilder body = new StringBuilder();
    while ((line = in.readLine()) != null)
      body.append(line);

    return new Response(status, body.toString());
  }

  /**
   * Reads exactly one response off a keep-alive connection, so the next one can be read after it. The body is
   * delimited by {@code Content-Length} rather than by the end of the stream.
   * <p>
   * {@code Content-Length} counts bytes and this reads characters, which holds only because every body asserted on
   * here is ASCII JSON. A response echoing multibyte content would leave this waiting for characters that do not
   * exist - so if one is ever added, read the exact byte count off the raw stream instead.
   */
  private Response readOneResponse(final BufferedReader in) throws IOException {
    final String status = in.readLine();

    int contentLength = -1;
    String line;
    while ((line = in.readLine()) != null && !line.isEmpty()) {
      final int colon = line.indexOf(':');
      if (colon > 0 && "content-length".equalsIgnoreCase(line.substring(0, colon).trim()))
        contentLength = Integer.parseInt(line.substring(colon + 1).trim());
    }

    if (contentLength < 0)
      return new Response(status, null);

    final char[] body = new char[contentLength];
    int read = 0;
    while (read < contentLength) {
      final int n = in.read(body, read, contentLength - read);
      if (n < 0)
        break;
      read += n;
    }
    return new Response(status, new String(body, 0, read));
  }

  private record Response(String status, String body) {
  }
}
