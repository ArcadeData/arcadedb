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
import com.arcadedb.query.RunningQuery;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpSessionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9953: the client-disconnect watch of #9689 resumes reads on the connection for the whole statement, and the
 * connection carries {@code arcadedb.network.socketTimeout} as its read timeout. Once that much time passed with no byte
 * from the client - which is exactly what a client waiting for its answer sends - Undertow's read-timeout timer fired: the
 * watch took the timeout for a disconnect and terminated the statement, and the timer closed the connection, so any
 * statement longer than the socket timeout (30 s by default) was answered with nothing at all. The read timeout is lifted
 * while the watch is armed and put back when it is disarmed, so it keeps guarding an idle keep-alive connection.
 * <p>
 * The socket timeout is lowered to one second here so a three-second statement crosses it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
// A hang detector, not a latency bound
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class Issue9953DisconnectWatchReadTimeoutTest extends BaseGraphServerTest {
  private static final int    SOCKET_TIMEOUT_MS = 1_000;
  /** Three times the socket timeout, and a statement the disconnect watch is armed for. */
  private static final String LONG_STATEMENT    = "SLEEP 3000";
  /** About 16 s on one core when left alone: a read that runs until it is terminated. */
  private static final String LONG_CYPHER       =
      "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j RETURN sum(sin(toFloat(i * j))) AS total";

  private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.NETWORK_SOCKET_TIMEOUT, SOCKET_TIMEOUT_MS);
  }

  @Test
  void aStatementLongerThanTheSocketTimeoutIsAnswered() throws Exception {
    try (final Socket socket = connect()) {
      send(socket, sqlCommand(LONG_STATEMENT, null));
      assertThat(readResponse(socket.getInputStream())).as("the client waited and is answered").startsWith("HTTP/1.1 200");
    }
  }

  /**
   * A body larger than one read buffer is read from the socket while the request is parsed, which leaves the read-timeout
   * timer of the connection running into the statement: the watch must not inherit it.
   */
  @Test
  void aStatementWithALargeBodyLongerThanTheSocketTimeoutIsAnswered() throws Exception {
    try (final Socket socket = connect()) {
      send(socket, sqlCommand(LONG_STATEMENT, "x".repeat(512 * 1024)));
      assertThat(readResponse(socket.getInputStream())).as("the client waited and is answered").startsWith("HTTP/1.1 200");
    }
  }

  /** The read timeout is put back when the watch stops: an idle keep-alive connection is still closed by the server. */
  @Test
  void theReadTimeoutGuardsTheIdleConnectionAgainAfterTheStatement() throws Exception {
    try (final Socket socket = connect()) {
      send(socket, sqlCommand(LONG_STATEMENT, null));
      final InputStream in = socket.getInputStream();
      assertThat(readResponse(in)).startsWith("HTTP/1.1 200");

      // The connection serves its next request at once, then sits idle
      send(socket, sqlCommand("SELECT 1 AS answer", null));
      assertThat(readResponse(in)).startsWith("HTTP/1.1 200").contains("\"answer\":1");

      socket.setSoTimeout(30_000);
      try {
        assertThat(in.read()).as("the server closes the idle connection after its read timeout").isEqualTo(-1);
      } catch (final SocketTimeoutException e) {
        fail("the idle connection was never closed: the read timeout was not restored after the statement");
      }
    }
  }

  /**
   * A request pipelined behind a running one ends the watch from its own read, before the statement does: the read
   * timeout is put back on that path too, so the connection is still closed once it goes idle.
   */
  @Test
  void theReadTimeoutIsRestoredAfterAPipelinedRequest() throws Exception {
    try (final Socket socket = connect()) {
      send(socket, sqlCommand(LONG_STATEMENT, null));
      // Sent while the statement runs, so the watch reads it, rather than together with the first request, which would
      // leave it in Undertow's buffer and the watch unarmed
      Thread.sleep(500);
      send(socket, sqlCommand("SELECT 2 AS answer", null));
      final InputStream in = socket.getInputStream();
      assertThat(readResponse(in)).startsWith("HTTP/1.1 200");
      assertThat(readResponse(in)).startsWith("HTTP/1.1 200").contains("\"answer\":2");

      socket.setSoTimeout(30_000);
      try {
        assertThat(in.read()).as("the server closes the idle connection after its read timeout").isEqualTo(-1);
      } catch (final SocketTimeoutException e) {
        fail("the idle connection was never closed: the read timeout was not restored after the pipelined request");
      }
    }
  }

  /** The watch still does its job with the read timeout lifted: a client that leaves terminates its statement. */
  @Test
  void aClientThatLeavesIsStillSeen() throws Exception {
    final RunningQuery entry;
    try (final Socket socket = connect()) {
      send(socket, cypherCommand(LONG_CYPHER));
      entry = awaitRunning();
    }
    assertThat(entry.awaitEnd(30_000)).as("the statement of a client that went away stops").isTrue();
    assertThat(entry.getTerminatedBy()).isEqualTo("client disconnected");
  }

  /**
   * The related point of #9953: a read terminated inside a {@code /begin} session leaves the session open (#9798), and the
   * {@code terminate query} answer used to give no hint of it, so a coordinator waiting for the session to go waited for
   * its expiry. The answer now names the session and says whether it is still open.
   */
  @Test
  void terminateQueryTellsWhetherTheSessionIsStillOpen() throws Exception {
    final String readSession = begin();
    final Socket readSocket = connect();
    try {
      send(readSocket, sessionRequest("query", readSession));
      final JSONObject answer = terminate(awaitRunning().getId());
      assertThat(answer.getString("status")).isEqualTo("terminated");
      assertThat(answer.getString("sessionId")).isEqualTo(readSession);
      assertThat(answer.getBoolean("sessionOpen")).as("a terminated read leaves its session open").isTrue();
      assertThat(readResponse(readSocket.getInputStream())).startsWith("HTTP/1.1 409");
    } finally {
      readSocket.close();
      post("/api/v1/rollback/" + getDatabaseName(), readSession, null);
    }

    final String commandSession = begin();
    try (final Socket commandSocket = connect()) {
      send(commandSocket, sessionRequest("command", commandSession));
      final JSONObject answer = terminate(awaitRunning().getId());
      assertThat(answer.getString("status")).isEqualTo("terminated");
      assertThat(answer.getString("sessionId")).isEqualTo(commandSession);
      assertThat(answer.getBoolean("sessionOpen")).as("a terminated command ends its session").isFalse();
      assertThat(readResponse(commandSocket.getInputStream())).startsWith("HTTP/1.1 409");
    }
  }

  private JSONObject terminate(final String queryId) throws Exception {
    final HttpResponse<String> response = post("/api/v1/server", null,
        new JSONObject().put("command", "terminate query " + queryId).put("wait", 10_000).toString());
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    return new JSONObject(response.body()).getJSONObject("result");
  }

  private String begin() throws Exception {
    final HttpResponse<String> response = post("/api/v1/begin/" + getDatabaseName(), null, null);
    assertThat(response.statusCode()).isEqualTo(204);
    return response.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElseThrow();
  }

  private HttpResponse<String> post(final String path, final String sessionId, final String body) throws Exception {
    final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, path)))
        .timeout(Duration.ofSeconds(60))
        .header("Authorization", basicAuth())
        .header("Content-Type", "application/json")
        .POST(body != null ? HttpRequest.BodyPublishers.ofString(body) : HttpRequest.BodyPublishers.noBody());
    if (sessionId != null)
      builder.header(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
    return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }

  private RunningQuery awaitRunning() {
    final RunningQuery[] found = new RunningQuery[1];
    await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).until(() -> {
      for (final RunningQuery q : getServer(0).getRunningQueries().getRunning())
        if (LONG_CYPHER.equals(q.getText())) {
          found[0] = q;
          return true;
        }
      return false;
    });
    return found[0];
  }

  private Socket connect() throws IOException {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerHttpPort(0)), 5_000);
    socket.setSoTimeout(60_000);
    return socket;
  }

  private String sqlCommand(final String sql, final String padding) {
    final JSONObject payload = new JSONObject().put("language", "sql").put("command", sql);
    if (padding != null)
      payload.put("params", new JSONObject().put("padding", padding));
    return request("command", null, payload);
  }

  private String cypherCommand(final String cypher) {
    return request("command", null, new JSONObject().put("language", "opencypher").put("command", cypher));
  }

  private String sessionRequest(final String route, final String sessionId) {
    return request(route, sessionId, new JSONObject().put("language", "opencypher").put("command", LONG_CYPHER));
  }

  private String request(final String route, final String sessionId, final JSONObject payload) {
    final byte[] body = payload.toString().getBytes(StandardCharsets.UTF_8);
    return "POST /api/v1/" + route + "/" + getDatabaseName() + " HTTP/1.1\r\n" + "Host: localhost\r\n" + "Authorization: "
        + basicAuth() + "\r\n" + (sessionId != null ? HttpSessionManager.ARCADEDB_SESSION_ID + ": " + sessionId + "\r\n" : "")
        + "Content-Type: application/json\r\n" + "Content-Length: " + body.length + "\r\n\r\n" + new String(body,
        StandardCharsets.UTF_8);
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private static void send(final Socket socket, final String request) throws IOException {
    final OutputStream out = socket.getOutputStream();
    out.write(request.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

  /** Reads one response with a Content-Length body: the status line, the headers and the body. */
  private static String readResponse(final InputStream in) throws IOException {
    final ByteArrayOutputStream head = new ByteArrayOutputStream();
    int matched = 0;
    while (matched < 4) {
      final int b = in.read();
      if (b < 0)
        throw new IOException("Connection closed while reading the response headers: " + head);
      head.write(b);
      matched = (b == '\r' && (matched == 0 || matched == 2)) || (b == '\n' && (matched == 1 || matched == 3)) ? matched + 1 : 0;
    }
    final String headers = head.toString(StandardCharsets.UTF_8);
    int length = 0;
    for (final String line : headers.split("\r\n"))
      if (line.toLowerCase().startsWith("content-length:"))
        length = Integer.parseInt(line.substring("content-length:".length()).trim());
    final byte[] body = in.readNBytes(length);
    return headers + new String(body, StandardCharsets.UTF_8);
  }
}
