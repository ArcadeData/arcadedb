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
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #5470: a streaming batch load must not be silently truncated when the server
 * pauses longer than the socket read timeout between two reads of the request body.
 * <p>
 * In the field the pause came from a full index compaction (195 s) followed by the replication of a
 * 45 MB schema entry: the worker thread was blocked inside a commit, nobody called {@code read()} on
 * the request channel, Undertow's read-timeout task killed the connection and the batch ended after
 * 630,000 of 16,000,000 vertices without any error.
 * <p>
 * The test compresses the same scenario: the socket read timeout is lowered to 2 s and a
 * fault-injection hook makes the very first vertex commit take longer than that while the client has
 * already pushed the whole body into the socket.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue5470BatchStreamStallIT extends BaseGraphServerTest {

  private static final int READ_TIMEOUT_MS  = 2_000;
  private static final int STALL_MS         = 5_000;
  /** More than one server-side vertex batch (PostBatchHandler flushes every 10,000 vertices). */
  private static final int TOTAL_VERTICES   = 25_000;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.setValue(READ_TIMEOUT_MS);
    // Keep the streaming budget well above the stall injected below: setting it to 0 (the pre-fix behaviour)
    // makes serverSideStallDoesNotTruncateTheBatch fail with a connection reset.
    GlobalConfiguration.SERVER_HTTP_STREAMING_READ_TIMEOUT.setValue(60_000);
  }

  @AfterEach
  void clearHook() {
    GraphBatch.TEST_BEFORE_VERTEX_COMMIT_HOOK = null;
  }

  @Test
  void serverSideStallDoesNotTruncateTheBatch() throws Exception {
    final boolean[] stalled = { false };
    GraphBatch.TEST_BEFORE_VERTEX_COMMIT_HOOK = attempt -> {
      if (stalled[0])
        return;
      stalled[0] = true;
      try {
        Thread.sleep(STALL_MS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final StringBuilder body = new StringBuilder(TOTAL_VERTICES * 64);
    for (int i = 0; i < TOTAL_VERTICES; i++)
      body.append("{\"@type\":\"vertex\",\"@class\":\"V1\",\"id\":").append(1_000_000 + i)
          .append(",\"name\":\"padding-to-make-the-body-large-enough-").append(i).append("\"}\n");

    final byte[] data = body.toString().getBytes(StandardCharsets.UTF_8);

    final HttpURLConnection conn = (HttpURLConnection) new URL(
        "http://127.0.0.1:2480/api/v1/batch/" + getDatabaseName()).openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    conn.setRequestProperty("Content-Type", "application/x-ndjson");
    conn.setDoOutput(true);
    conn.setFixedLengthStreamingMode(data.length);

    int responseCode;
    String response;
    try {
      try (final OutputStream out = conn.getOutputStream()) {
        out.write(data);
      }
      responseCode = conn.getResponseCode();
      response = responseCode < 400 ? readResponse(conn) : readError(conn);
    } finally {
      conn.disconnect();
    }

    // The load must either complete in full or fail loudly - never report success on a truncated stream.
    assertThat(responseCode).as("response body: %s", response).isEqualTo(200);

    final JSONObject result = new JSONObject(response);
    assertThat(result.getLong("verticesCreated")).isEqualTo(TOTAL_VERTICES);

    final JSONObject count = executeCommand(0, "sql", "SELECT count(*) as total FROM V1 WHERE id >= 1000000");
    assertThat(count.getJSONObject("result").getJSONArray("records").getJSONObject(0).getLong("total"))
        .isEqualTo(TOTAL_VERTICES);
  }

  /**
   * The relaxed watchdog must not turn into a free pass for a client that stops sending: every blocking read is
   * still bounded by {@code arcadedb.network.socketTimeout}, so a truncated upload is closed down quickly and
   * answered with a 408 carrying the partial-commit counters instead of pinning a worker thread.
   */
  @Test
  void stalledClientIsCutOffAndAnswered() throws Exception {
    final String line = "{\"@type\":\"vertex\",\"@class\":\"V1\",\"id\":2000000}\n";
    final String auth = Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());

    try (final Socket socket = new Socket("127.0.0.1", 2480)) {
      socket.setSoTimeout(60_000);

      final OutputStream out = socket.getOutputStream();
      // Announce far more bytes than are ever sent, then go silent.
      out.write(("POST /api/v1/batch/" + getDatabaseName() + " HTTP/1.1\r\n"
          + "Host: 127.0.0.1:2480\r\n"
          + "Authorization: Basic " + auth + "\r\n"
          + "Content-Type: application/x-ndjson\r\n"
          + "Content-Length: 1000000\r\n"
          + "\r\n").getBytes(StandardCharsets.UTF_8));
      out.write(line.getBytes(StandardCharsets.UTF_8));
      out.flush();

      final long startMs = System.currentTimeMillis();
      final BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
      final String statusLine = in.readLine();
      final long elapsedMs = System.currentTimeMillis() - startMs;

      // Either the server answers (preferred: 408 with the counters) or it closes the connection, but it must
      // never wait for the relaxed streaming budget before reacting.
      assertThat(elapsedMs).isLessThan(60_000L);
      if (statusLine != null)
        assertThat(statusLine).contains("408");
    }
  }
}
