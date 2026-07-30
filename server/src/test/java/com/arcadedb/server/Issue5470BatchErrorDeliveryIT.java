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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
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
 * The first two tests therefore announce a large body and then never send the rest of it, so a server that insists on
 * having it cannot answer at all. The third keeps the upload flowing, which is the reported case. The last two guard
 * what must NOT change: a genuinely cut upload is still reported as truncated rather than blamed on the client, and a
 * load that succeeds still leaves its connection reusable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue5470BatchErrorDeliveryIT extends BaseGraphServerTest {

  /** Generous next to a verdict the server can reach immediately, unforgiving next to waiting for the upload. */
  private static final int  RESPONSE_TIMEOUT_MS   = 20_000;
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
        final Response response = readResponse(socket);

        assertThat(response.status()).as("a client that is still uploading must still be told why the load failed")
            .isNotNull();
        assertThat(response.status()).contains("400");
        assertThat(response.body()).contains("nonexistent-vertex");
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
        final Response response = readResponse(socket);

        assertThat(response.status()).as("a live client sending a bad record must be answered").isNotNull();
        assertThat(response.status()).as("the body did not end, so this is a 400 about the payload, not a 408")
            .contains("400");
        assertThat(response.body()).as("and it must name the line to fix").contains("line 21");
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
