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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7381: what {@code POST /api/v1/batch/{database}} guarantees once its body and its response are streamed
 * rather than buffered. Two defects, one contract.
 * <ol>
 * <li><b>The idempotency key ignored the payload.</b> The key binds a cached response to the request that
 * produced it, and the request body is the field doing the binding. This endpoint never buffers its body, so
 * nothing handed it to the key and the key degenerated to {@code (X-Request-Id, POST, path, database)}: a client
 * reusing one correlation id across two bulk loads of the same database - common proxy and tracing practice -
 * was answered with the FIRST load's summary and the second load was never executed. Its records were silently
 * absent and it was told they had been created. Issue #7311 had closed this for the streaming encoding only.</li>
 * <li><b>The streamed response was unbounded.</b> It is written while the upload is still being read and grows
 * with the size of the load, so a client that uploads everything before reading anything can fill the socket
 * buffers between the two: the server blocks inside a response write, and a server blocked there is not reading
 * the request either. Nothing completes and the worker thread is held for as long as the client keeps the
 * connection open. The read side has been watched since issue #5470; the write side now is too.</li>
 * </ol>
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7381BatchContractIT extends BaseGraphServerTest {

  private static final String NDJSON = "application/x-ndjson";

  /**
   * The write-side budget under test. Short enough that the test does not spend minutes proving a bound, far
   * longer than any write this machine could legitimately still be making progress on.
   */
  private static final int WRITE_BUDGET_MS = 10_000;
  /**
   * Separates "the server gave up on a response nobody is reading" from "it is still holding the worker thread".
   * An order of magnitude above the budget: a wider bound here cannot turn a passing run red, while the
   * behaviour it rules out is unbounded.
   */
  private static final long SEPARATION_MS  = 120_000;
  /**
   * Payload of the stall scenario. Each vertex carries a temporary id long enough that the acknowledgement it
   * produces is a few kilobytes rather than a few hundred bytes, so the response outgrows the socket buffers
   * after tens of records instead of after millions. The total is far larger than anything that can sit in
   * flight, so the client's own upload blocks too - which is the deadlock, observed from the client side.
   */
  private static final int STALL_VERTICES  = 4_000;
  private static final int STALL_ID_LENGTH = 4_000;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_HTTP_STREAMING_WRITE_TIMEOUT.setValue(WRITE_BUDGET_MS);
  }

  /**
   * Part 1, buffered encoding - the path issue #7311 left exposed. Two loads of DIFFERENT payloads under one
   * {@code X-Request-Id}: the second must be executed rather than answered with the first one's summary.
   */
  @Test
  @Timeout(120)
  void aReusedRequestIdDoesNotReplayADifferentBufferedLoad() throws Exception {
    final String requestId = "batch-idempotency-7381-buffered";

    final JSONObject first = postBuffered(vertices(600_000, 2), requestId, null);
    assertThat(first.getLong("verticesCreated")).isEqualTo(2);

    final JSONObject second = postBuffered(vertices(600_100, 3), requestId, null);
    assertThat(second.getLong("verticesCreated"))
        .as("the second load carries three vertices: answering it with the first load's summary would report two")
        .isEqualTo(3);

    assertThat(countVertices(600_100, 600_110))
        .as("and the records of the second load must be in the database, which is what a replayed answer hid")
        .isEqualTo(3);
  }

  /**
   * Part 1, streaming encoding. This half already held before the fix, through the {@code Accept}-header gate
   * issue #7311 added for a different reason - a buffered hit replayed to an NDJSON caller is a body its reader
   * cannot parse - so this is a guard, not a reproduction: it is here so the route-level exclusion cannot later
   * be dropped in favour of that gate, or of the gate in favour of it, without one of the two halves reopening
   * in silence.
   */
  @Test
  @Timeout(120)
  void aReusedRequestIdDoesNotReplayADifferentStreamedLoad() throws Exception {
    final String requestId = "batch-idempotency-7381-streamed";

    assertThat(terminal(postStreamed(vertices(610_000, 2), requestId), "summary").getLong("verticesCreated"))
        .isEqualTo(2);

    final JSONObject second = terminal(postStreamed(vertices(610_100, 3), requestId), "summary");
    assertThat(second.getLong("verticesCreated")).isEqualTo(3);
    assertThat(countVertices(610_100, 610_110)).isEqualTo(3);
  }

  /**
   * Part 2. A client that writes its whole body before reading anything - which is exactly what a plain
   * {@code HttpURLConnection} that calls {@code getResponseCode()} after writing does - stops draining the
   * response. Once the buffers fill, the server blocks inside a response write and stops reading the upload,
   * so the client's own write blocks against a peer that will never read again.
   * <p>
   * What is asserted is the CLIENT's write failing: the server closing the connection is the only thing that can
   * release it, and before this fix nothing ever did. The test deliberately never reads a byte of the response -
   * reading is what makes the scenario impossible.
   */
  @Test
  @Tag("slow")
  @Timeout(300)
  void aClientThatNeverReadsTheStreamIsGivenUpOnRatherThanHeldForever() throws Exception {
    final byte[] body = inflatedVertices(620_000, STALL_VERTICES, STALL_ID_LENGTH);

    try (final Socket socket = new Socket()) {
      // Set before connect, which is what pins the advertised receive window: a small window makes the server's
      // send side fill after tens of acknowledgements instead of after a load this test cannot afford to run.
      socket.setReceiveBufferSize(4096);
      socket.connect(new InetSocketAddress("127.0.0.1", httpPort()), 30_000);

      final OutputStream out = socket.getOutputStream();
      out.write(requestHead("?vertexBatchSize=1&idMapping=true", NDJSON, body.length));
      out.flush();

      final AtomicReference<Exception> uploadFailure = new AtomicReference<>();
      final Thread uploader = new Thread(() -> {
        try {
          // In pieces, so the thread is inside a blocking write when the server stops reading rather than
          // having handed the whole payload to the kernel.
          for (int offset = 0; offset < body.length; offset += 32 * 1024) {
            out.write(body, offset, Math.min(32 * 1024, body.length - offset));
            out.flush();
          }
        } catch (final Exception e) {
          uploadFailure.set(e);
        }
      }, "issue7381-uploader");
      uploader.setDaemon(true);

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      uploader.start();
      uploader.join(SEPARATION_MS);

      assertThat(uploadFailure.get())
          .as("the server must give up on a streamed answer nobody is reading and close the connection, which is "
              + "the only thing that can release this upload; a null here means it is still blocked")
          .isNotNull();
      watch.assertGaveUpWithin(SEPARATION_MS,
          "a streamed answer whose writes are bounded from one that holds a worker thread until the client goes away");
    }

    // The worker thread that served the abandoned load is back: an ordinary request is answered as usual.
    assertThat(terminal(postStreamed(vertices(630_000, 2), null), "summary").getLong("verticesCreated"))
        .isEqualTo(2);
  }

  // ---------------------------------------------------------------------------------------------------------

  /**
   * The port the server under test actually bound, never the 2480 it was asked for: anything already listening
   * there would answer these requests as a different build.
   */
  private int httpPort() {
    return getServer(0).getHttpServer().getPort();
  }

  private byte[] vertices(final int firstId, final int count) {
    final StringBuilder body = new StringBuilder();
    for (int i = 0; i < count; i++)
      body.append("{\"@type\":\"vertex\",\"@class\":\"V1\",\"@id\":\"i").append(firstId + i).append("\",\"id\":")
          .append(firstId + i).append("}\n");
    return body.toString().getBytes(StandardCharsets.UTF_8);
  }

  /** The same, with a temporary id padded to {@code idLength} characters so each acknowledgement is large. */
  private byte[] inflatedVertices(final int firstId, final int count, final int idLength) {
    final String padding = "x".repeat(idLength);
    final StringBuilder body = new StringBuilder(count * (idLength + 64));
    for (int i = 0; i < count; i++)
      body.append("{\"@type\":\"vertex\",\"@class\":\"V1\",\"@id\":\"").append(padding).append(firstId + i)
          .append("\",\"id\":").append(firstId + i).append("}\n");
    return body.toString().getBytes(StandardCharsets.UTF_8);
  }

  private byte[] requestHead(final String queryString, final String accept, final int contentLength) {
    final String auth = Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
    return ("POST /api/v1/batch/" + getDatabaseName() + queryString + " HTTP/1.1\r\n"
        + "Host: 127.0.0.1:" + httpPort() + "\r\n"
        + "Authorization: Basic " + auth + "\r\n"
        + "Content-Type: " + NDJSON + "\r\n"
        + "Accept: " + accept + "\r\n"
        + "Content-Length: " + contentLength + "\r\n"
        + "\r\n").getBytes(StandardCharsets.UTF_8);
  }

  private HttpURLConnection open(final String queryString, final String requestId, final String accept)
      throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URL(
        "http://127.0.0.1:" + httpPort() + "/api/v1/batch/" + getDatabaseName() + queryString).openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    conn.setRequestProperty("Content-Type", NDJSON);
    if (accept != null)
      conn.setRequestProperty("Accept", accept);
    if (requestId != null)
      conn.setRequestProperty("X-Request-Id", requestId);
    conn.setDoOutput(true);
    return conn;
  }

  private JSONObject postBuffered(final byte[] body, final String requestId, final String accept) throws Exception {
    final HttpURLConnection conn = open("?vertexBatchSize=2", requestId, accept);
    conn.setFixedLengthStreamingMode(body.length);
    try (final DataOutputStream wr = new DataOutputStream(conn.getOutputStream())) {
      wr.write(body);
    }
    try {
      assertThat(conn.getResponseCode()).isEqualTo(200);
      return new JSONObject(readAll(conn.getInputStream()));
    } finally {
      conn.disconnect();
    }
  }

  private List<JSONObject> postStreamed(final byte[] body, final String requestId) throws Exception {
    final HttpURLConnection conn = open("?vertexBatchSize=2", requestId, NDJSON);
    conn.setFixedLengthStreamingMode(body.length);
    try (final DataOutputStream wr = new DataOutputStream(conn.getOutputStream())) {
      wr.write(body);
    }
    try {
      assertThat(conn.getResponseCode()).isEqualTo(200);
      assertThat(conn.getContentType()).contains(NDJSON);
      final List<JSONObject> events = new ArrayList<>();
      for (final String line : readAll(conn.getInputStream()).split("\n"))
        if (!line.isBlank())
          events.add(new JSONObject(line));
      return events;
    } finally {
      conn.disconnect();
    }
  }

  private static JSONObject terminal(final List<JSONObject> events, final String kind) {
    assertThat(events).as("a stream that ends with no terminal line did not arrive whole").isNotEmpty();
    final JSONObject last = events.getLast();
    assertThat(last.has(kind)).as("the last line must be the '%s' terminator, was %s", kind, last).isTrue();
    return last.getJSONObject(kind);
  }

  private static String readAll(final InputStream in) throws IOException {
    try (in) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  /**
   * Counted on the server's own database rather than over {@code executeCommand}, whose URL is hardcoded to port
   * 2480: this class already talks to {@link #httpPort()}, the port the server under test actually bound, and a
   * count that went to a different one would answer for a different build.
   */
  private long countVertices(final int fromId, final int toId) {
    try (final ResultSet rs = getServer(0).getDatabase(getDatabaseName())
        .query("sql", "SELECT count(*) as total FROM V1 WHERE id >= ? AND id < ?", fromId, toId)) {
      return ((Number) rs.next().getProperty("total")).longValue();
    }
  }
}
