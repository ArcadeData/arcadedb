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

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7311: {@code POST /api/v1/batch/{database}} answers a client that sends
 * {@code Accept: application/x-ndjson} with per-chunk acknowledgements written while its upload is still being
 * read - the acknowledgement half of the gRPC {@code InsertBidirectional} shape, which HTTP had no counterpart
 * for. A client that does not negotiate the encoding gets exactly what it got before.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class PostBatchStreamingIT extends BaseGraphServerTest {

  private static final String NDJSON = "application/x-ndjson";

  /**
   * The point of the whole issue: a progress line has to reach the client BEFORE the request body has been fully
   * sent, or the encoding is only a differently-shaped version of the answer that already existed.
   * <p>
   * The proof is not the ordering of statements in this method - that would only show that the client read
   * before it finished writing - but the {@code bytesRead} the server puts on the line it sent: strictly fewer
   * than the {@code Content-Length} it was promised. A server that answered only at the end of the body could
   * not produce that number.
   */
  @Test
  @Timeout(60)
  void progressReachesTheClientBeforeTheBodyHasBeenFullySent() throws Exception {
    final byte[] head = ndjsonVertices(100_000, 4);
    final byte[] tail = ndjsonVertices(100_010, 4);
    final int announced = head.length + tail.length;

    try (final Socket socket = new Socket("127.0.0.1", httpPort())) {
      socket.setSoTimeout(30_000);
      final OutputStream out = socket.getOutputStream();
      out.write(requestHead("?vertexBatchSize=2", NDJSON, announced));
      // Only the first half. The rest is deliberately withheld until a line has come back.
      out.write(head);
      out.flush();

      final BufferedReader raw = new BufferedReader(
          new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
      assertThat(readStatusLine(raw)).as("the streaming encoding always answers 200").contains("200");
      final java.util.Map<String, String> headers = readHeaders(raw);
      assertThat(headers).containsEntry("content-type", NDJSON);
      final NdJsonBodyReader in = new NdJsonBodyReader(raw, headers);

      final JSONObject first = new JSONObject(in.nextLine());
      assertThat(first.has("progress"))
          .as("the first line of a load still uploading must be a chunk acknowledgement, not the summary")
          .isTrue();

      final JSONObject progress = first.getJSONObject("progress");
      assertThat(progress.getString("phase")).isEqualTo("vertices");
      assertThat(progress.getLong("verticesCreated")).isEqualTo(2);
      assertThat(progress.getLong("bytesRead"))
          .as("the acknowledgement was written having read less than the announced body, which is what makes it "
              + "an acknowledgement and not a summary")
          .isLessThan(announced);

      // Only now does the rest of the payload go out.
      out.write(tail);
      out.flush();

      final List<JSONObject> rest = readEvents(in);
      final JSONObject summary = terminal(rest, "summary");
      assertThat(summary.getLong("verticesCreated")).isEqualTo(8);
      assertThat(summary.getLong("bytesRead")).isEqualTo(announced);
      assertThat(countEvents(rest, "progress")).as("the remaining chunks are acknowledged too").isGreaterThan(0);
    }

    assertThat(countVertices(100_000, 100_020)).isEqualTo(8);
  }

  /**
   * The terminal line is not a second, parallel shape of the answer: it is the very object the buffered encoding
   * would have sent, produced by the same code. A client can therefore apply one piece of logic to both.
   */
  @Test
  @Timeout(60)
  void theSummaryLineCarriesTheSameObjectTheUnaryEncodingWouldHaveSent() throws Exception {
    final JSONObject buffered = postBuffered(ndjsonVertices(200_000, 6), "?vertexBatchSize=2", NDJSON, 200);
    final JSONObject streamed = terminal(postStreamed(ndjsonVertices(200_010, 6), "?vertexBatchSize=2", NDJSON),
        "summary");

    assertThat(streamed.keySet())
        .as("the summary must carry the same fields as the buffered body, so a client parses one shape")
        .isEqualTo(buffered.keySet());
    assertThat(streamed.getLong("verticesCreated")).isEqualTo(buffered.getLong("verticesCreated"));
    assertThat(streamed.getLong("linesRead")).isEqualTo(buffered.getLong("linesRead"));
    assertThat(streamed.getLong("linesSkipped")).isEqualTo(buffered.getLong("linesSkipped"));
    assertThat(streamed.getLong("bytesRead")).isEqualTo(buffered.getLong("bytesRead"));
    assertThat(streamed.getJSONObject("idMapping").length())
        .isEqualTo(buffered.getJSONObject("idMapping").length());
  }

  /**
   * The additive half of the contract: a caller that does not ask for the stream must not be able to tell that
   * it exists. No {@code Accept}, another type, and the RFC 9110 spelling of "anything but that one" all keep
   * the single buffered object under {@code application/json}.
   */
  @Test
  @Timeout(60)
  void theUnaryEncodingIsUnchangedWhenTheStreamIsNotNegotiated() throws Exception {
    final String[] accepts = { null, "application/json", "application/json, application/x-ndjson;q=0" };
    for (int i = 0; i < accepts.length; i++) {
      final String accept = accepts[i];
      final HttpURLConnection conn = open("?vertexBatchSize=2", NDJSON, accept);
      writeBody(conn, ndjsonVertices(300_000 + i * 10, 4));

      try {
        assertThat(conn.getResponseCode()).as("accept=" + accept).isEqualTo(200);
        assertThat(conn.getContentType()).as("accept=" + accept).contains("application/json");
        final JSONObject result = new JSONObject(readAll(conn.getInputStream()));
        assertThat(result.getLong("verticesCreated")).isEqualTo(4);
        assertThat(result.has("progress")).as("a buffered answer carries no stream envelope").isFalse();
      } finally {
        conn.disconnect();
      }
    }
  }

  /**
   * A failure that the load itself reaches - a malformed reference, a vertex after the first edge - happens with
   * a 200 already on the wire. The status it would have carried travels in band instead, alongside the
   * partial-commit counters, because those are what the client reconciles with: the load is not atomic.
   */
  @Test
  @Timeout(60)
  void aClientInputFailureAfterTheStreamStartedIsReportedInBand() throws Exception {
    final String body = """
        {"@type":"vertex","@class":"V1","@id":"s1","id":400000}
        {"@type":"vertex","@class":"V1","@id":"s2","id":400001}
        {"@type":"edge","@class":"E1","@from":"s1","@to":"nobody-declared-this"}
        """;

    final List<JSONObject> events = postStreamed(body.getBytes(StandardCharsets.UTF_8), "?vertexBatchSize=1", NDJSON);
    assertThat(countEvents(events, "progress")).isGreaterThan(0);

    final JSONObject error = terminal(events, "error");
    assertThat(error.getInt("status"))
        .as("the status the buffered encoding would have sent, in band because 200 is already on the wire")
        .isEqualTo(400);
    assertThat(error.getString("error")).contains("nobody-declared-this");
    assertThat(error.getBoolean("partialCommit")).isTrue();
    assertThat(error.getLong("verticesCreated")).isEqualTo(2);
  }

  /**
   * A body that stops before its announced length is the failure issue #5470 exists for, and it must stay just
   * as loud on this encoding: never a summary line with a partial count, always the truncation with the counts
   * needed to resume.
   */
  @Test
  @Timeout(60)
  void aTruncatedUploadAfterTheStreamStartedIsReportedInBand() throws Exception {
    final byte[] sent = ndjsonVertices(500_000, 4);

    try (final Socket socket = new Socket("127.0.0.1", httpPort())) {
      socket.setSoTimeout(30_000);
      final OutputStream out = socket.getOutputStream();
      out.write(requestHead("?vertexBatchSize=1", NDJSON, 1_000_000));
      out.write(sent);
      out.flush();
      socket.shutdownOutput();

      final BufferedReader raw = new BufferedReader(
          new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
      assertThat(readStatusLine(raw)).contains("200");
      final NdJsonBodyReader in = new NdJsonBodyReader(raw, readHeaders(raw));

      final JSONObject error = terminal(readEvents(in), "error");
      assertThat(error.getInt("status"))
          .as("a truncated upload keeps the 408 it always had, in band")
          .isEqualTo(408);
      assertThat(error.getBoolean("partialCommit")).isTrue();
      assertThat(error.getLong("verticesCreated")).isEqualTo(4);
      assertThat(error.getLong("bytesRead")).isEqualTo(sent.length);
    }
  }

  /**
   * The other side of the in-band rule, and the one that is easy to get wrong: a failure raised BEFORE anything
   * has been written can still be answered with a real status code, so it must be. Downgrading a
   * {@code DuplicatedKeyException} to a 200 whose body has to be parsed would tell a client's retry policy - which
   * keys on the status - the opposite of what 409 means.
   */
  @Test
  @Timeout(60)
  void aFailureBeforeTheStreamStartedKeepsItsRealStatusCode() throws Exception {
    // V1.id carries a unique index, so loading the same id twice is refused by the engine on the first commit -
    // before a single progress line exists.
    postStreamed(ndjsonVertices(600_000, 1), "?vertexBatchSize=1", NDJSON);

    final HttpURLConnection conn = open("?vertexBatchSize=1", NDJSON, NDJSON);
    writeBody(conn, ndjsonVertices(600_000, 1));
    try {
      assertThat(conn.getResponseCode())
          .as("a duplicate key is answered 409 on both encodings: nothing had been written yet")
          .isEqualTo(409);
    } finally {
      conn.disconnect();
    }

    // Same rule for a request the handler refuses on its parameters, which it parses before writing anything.
    final HttpURLConnection bad = open("?refMode=whatever", NDJSON, NDJSON);
    writeBody(bad, ndjsonVertices(600_100, 1));
    try {
      assertThat(bad.getResponseCode()).isEqualTo(400);
    } finally {
      bad.disconnect();
    }
  }

  /** The edge phase is acknowledged too, on the {@code commitEvery} boundary GraphBatch writes edges at. */
  @Test
  @Timeout(60)
  void theEdgePhaseIsAcknowledgedOnItsOwnBoundary() throws Exception {
    final StringBuilder body = new StringBuilder();
    for (int i = 0; i < 6; i++)
      body.append("{\"@type\":\"vertex\",\"@class\":\"V1\",\"@id\":\"e").append(i).append("\",\"id\":")
          .append(700_000 + i).append("}\n");
    for (int i = 0; i < 5; i++)
      body.append("{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":\"e").append(i).append("\",\"@to\":\"e")
          .append(i + 1).append("\"}\n");

    final List<JSONObject> events = postStreamed(body.toString().getBytes(StandardCharsets.UTF_8),
        "?vertexBatchSize=2&commitEvery=2", NDJSON);

    assertThat(events.stream().filter(e -> e.has("progress"))
        .map(e -> e.getJSONObject("progress").getString("phase")).toList())
        .as("both phases report, so a client is not blind for the whole of the edge half")
        .contains("vertices", "edges");
    assertThat(terminal(events, "summary").getLong("edgesCreated")).isEqualTo(5);
  }

  /** The encoding is a property of the response, so it is available whatever format the request body is in. */
  @Test
  @Timeout(60)
  void aCsvPayloadStreamsProgressToo() throws Exception {
    final StringBuilder body = new StringBuilder("@type,@class,@id,id\n");
    for (int i = 0; i < 4; i++)
      body.append("vertex,V1,c").append(i).append(',').append(800_000 + i).append('\n');

    final List<JSONObject> events = postStreamed(body.toString().getBytes(StandardCharsets.UTF_8),
        "?vertexBatchSize=2", "text/csv");
    assertThat(countEvents(events, "progress")).isGreaterThan(0);
    assertThat(terminal(events, "summary").getLong("verticesCreated")).isEqualTo(4);
  }

  /**
   * The idempotency cache is keyed on method, path, database and body - never on the negotiated encoding - so a
   * hit recorded by a buffered request would be replayed to a streaming caller as one {@code application/json}
   * object its reader cannot parse. A streaming request stays out of that cache entirely.
   */
  @Test
  @Timeout(60)
  void anIdempotentRetryNeverReplaysABufferedBodyToAStreamingCaller() throws Exception {
    final String requestId = "batch-streaming-7311";

    final HttpURLConnection first = open("?vertexBatchSize=2", NDJSON, null);
    first.setRequestProperty("X-Request-Id", requestId);
    writeBody(first, ndjsonVertices(900_000, 2));
    try {
      assertThat(first.getResponseCode()).isEqualTo(200);
      assertThat(first.getContentType()).contains("application/json");
    } finally {
      first.disconnect();
    }

    final HttpURLConnection second = open("?vertexBatchSize=2", NDJSON, NDJSON);
    second.setRequestProperty("X-Request-Id", requestId);
    writeBody(second, ndjsonVertices(900_010, 2));
    try {
      assertThat(second.getResponseCode()).isEqualTo(200);
      assertThat(second.getContentType())
          .as("the streaming caller must not be served the buffered body cached by the earlier retry")
          .contains(NDJSON);
      assertThat(terminal(parseEvents(readAll(second.getInputStream())), "summary").getLong("verticesCreated"))
          .isEqualTo(2);
    } finally {
      second.disconnect();
    }
  }

  /** A standalone database has no Raft index to bookmark, so the terminal line must not invent one. */
  @Test
  @Timeout(60)
  void theCommitIndexIsAbsentOnAStandaloneDatabase() throws Exception {
    final JSONObject summary = terminal(postStreamed(ndjsonVertices(950_000, 2), "?vertexBatchSize=2", NDJSON),
        "summary");
    assertThat(summary.has("commitIndex")).isFalse();
  }

  // ---------------------------------------------------------------------------------------------------------

  /**
   * The port the server under test actually bound, never the 2480 it was asked for. Anything already listening
   * there - an IDE-left server, another agent's run - would otherwise take these requests and answer them as a
   * different build, which surfaces as an authentication failure or as a content type this branch does not
   * produce rather than as a port conflict.
   */
  private int httpPort() {
    return getServer(0).getHttpServer().getPort();
  }

  private byte[] ndjsonVertices(final int firstId, final int count) {
    final StringBuilder body = new StringBuilder();
    for (int i = 0; i < count; i++)
      body.append("{\"@type\":\"vertex\",\"@class\":\"V1\",\"@id\":\"t").append(firstId + i).append("\",\"id\":")
          .append(firstId + i).append("}\n");
    return body.toString().getBytes(StandardCharsets.UTF_8);
  }

  private byte[] requestHead(final String queryString, final String contentType, final int contentLength) {
    final String auth = Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
    return ("POST /api/v1/batch/" + getDatabaseName() + queryString + " HTTP/1.1\r\n"
        + "Host: 127.0.0.1:" + httpPort() + "\r\n"
        + "Authorization: Basic " + auth + "\r\n"
        + "Content-Type: " + contentType + "\r\n"
        + "Accept: " + NDJSON + "\r\n"
        + "Content-Length: " + contentLength + "\r\n"
        + "\r\n").getBytes(StandardCharsets.UTF_8);
  }

  private HttpURLConnection open(final String queryString, final String contentType, final String accept)
      throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URL(
        "http://127.0.0.1:" + httpPort() + "/api/v1/batch/" + getDatabaseName() + queryString).openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    conn.setRequestProperty("Content-Type", contentType);
    if (accept != null)
      conn.setRequestProperty("Accept", accept);
    conn.setDoOutput(true);
    return conn;
  }

  private void writeBody(final HttpURLConnection conn, final byte[] body) throws Exception {
    conn.setFixedLengthStreamingMode(body.length);
    try (final DataOutputStream wr = new DataOutputStream(conn.getOutputStream())) {
      wr.write(body);
    }
  }

  private JSONObject postBuffered(final byte[] body, final String queryString, final String contentType,
      final int expectedStatus) throws Exception {
    final HttpURLConnection conn = open(queryString, contentType, null);
    writeBody(conn, body);
    try {
      assertThat(conn.getResponseCode()).isEqualTo(expectedStatus);
      return new JSONObject(readAll(conn.getInputStream()));
    } finally {
      conn.disconnect();
    }
  }

  private List<JSONObject> postStreamed(final byte[] body, final String queryString, final String contentType)
      throws Exception {
    final HttpURLConnection conn = open(queryString, contentType, NDJSON);
    writeBody(conn, body);
    try {
      assertThat(conn.getResponseCode()).isEqualTo(200);
      assertThat(conn.getContentType()).contains(NDJSON);
      return parseEvents(readAll(conn.getInputStream()));
    } finally {
      conn.disconnect();
    }
  }

  private static String readAll(final java.io.InputStream in) throws Exception {
    try (in) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static List<JSONObject> parseEvents(final String body) {
    final List<JSONObject> events = new ArrayList<>();
    for (final String line : body.split("\n"))
      if (!line.isBlank())
        events.add(new JSONObject(line));
    return events;
  }

  private static List<JSONObject> readEvents(final NdJsonBodyReader in) throws Exception {
    final List<JSONObject> events = new ArrayList<>();
    for (String line = in.nextLine(); line != null; line = in.nextLine()) {
      if (line.isBlank())
        continue;
      final JSONObject event = new JSONObject(line);
      events.add(event);
      // The terminal line ends the stream; the connection may stay open past it.
      if (event.has("summary") || event.has("error"))
        break;
    }
    return events;
  }

  private static JSONObject terminal(final List<JSONObject> events, final String kind) {
    assertThat(events).as("the stream must end with a terminal line").isNotEmpty();
    final JSONObject last = events.get(events.size() - 1);
    assertThat(last.has(kind)).as("expected a '" + kind + "' terminal line, got " + last).isTrue();
    return last.getJSONObject(kind);
  }

  private static long countEvents(final List<JSONObject> events, final String kind) {
    return events.stream().filter(e -> e.has(kind)).count();
  }

  private static String readStatusLine(final BufferedReader in) throws Exception {
    return in.readLine();
  }

  private static java.util.Map<String, String> readHeaders(final BufferedReader in) throws Exception {
    final java.util.Map<String, String> headers = new java.util.HashMap<>();
    for (String line = in.readLine(); line != null && !line.isEmpty(); line = in.readLine()) {
      final int colon = line.indexOf(':');
      if (colon > 0)
        headers.put(line.substring(0, colon).trim().toLowerCase(), line.substring(colon + 1).trim());
    }
    return headers;
  }

  /**
   * Reads the NDJSON lines of a response taken off a raw socket, whichever transfer encoding Undertow chose for
   * it. The distinction is not cosmetic: a streamed answer whose connection stays alive is chunked, so a reader
   * that treated the body as plain text would parse the hexadecimal chunk sizes as JSON, while the truncated
   * load that retires its connection sends the same lines with no framing at all.
   * <p>
   * Chunk sizes count bytes and this reads characters, which agrees here because every line is ASCII JSON.
   */
  private static final class NdJsonBodyReader {
    private final BufferedReader        in;
    private final boolean               chunked;
    private final java.util.ArrayDeque<String> pending = new java.util.ArrayDeque<>();

    private NdJsonBodyReader(final BufferedReader in, final java.util.Map<String, String> headers) {
      this.in = in;
      this.chunked = "chunked".equalsIgnoreCase(headers.getOrDefault("transfer-encoding", ""));
    }

    private String nextLine() throws Exception {
      if (!chunked)
        return in.readLine();

      while (pending.isEmpty()) {
        final String sizeLine = in.readLine();
        if (sizeLine == null)
          return null;
        if (sizeLine.isBlank())
          continue;
        final int size = Integer.parseInt(sizeLine.trim().split(";")[0], 16);
        if (size == 0)
          return null;

        final char[] buffer = new char[size];
        int read = 0;
        while (read < size) {
          final int n = in.read(buffer, read, size - read);
          if (n < 0)
            break;
          read += n;
        }
        in.readLine(); // the CRLF that closes the chunk
        for (final String line : new String(buffer, 0, read).split("\n"))
          if (!line.isBlank())
            pending.add(line);
      }
      return pending.poll();
    }
  }

  private long countVertices(final int fromId, final int toId) throws Exception {
    final JSONObject count = executeCommand(0, "sql",
        "SELECT count(*) as total FROM V1 WHERE id >= " + fromId + " AND id < " + toId);
    return count.getJSONObject("result").getJSONArray("records").getJSONObject(0).getLong("total");
  }
}
