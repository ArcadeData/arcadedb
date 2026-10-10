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

import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9689: an HTTP client that goes away while its statement runs - its own deadline, a killed process,
 * {@code curl --max-time} - terminates the statement, instead of leaving the server busy on an answer nobody will read
 * until the statement completes or reaches {@code arcadedb.command.timeout}. A client that is still there is never
 * mistaken for one that left: a keep-alive connection serves its next request, and a request a client pipelines behind
 * a running one is answered once the first is. And a Java client is told its statement was terminated with the type that
 * says so, rather than a generic remote failure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9689HttpRunningQueryTest extends BaseGraphServerTest {
  /** About 16 s on one core when left alone, all of it inside one aggregation. */
  private static final String LONG_CYPHER =
      "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j RETURN sum(sin(toFloat(i * j))) AS total";

  @Test
  void aClientThatClosesTheConnectionTerminatesItsStatement() throws Exception {
    final RunningQuery entry;
    try (final Socket socket = connect()) {
      send(socket, command(LONG_CYPHER));
      entry = awaitRunning();
      assertThat(entry.isTerminated()).isFalse();
    }
    // The socket is closed: the client is gone
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThat(entry.awaitEnd(30_000)).as("the statement of a client that went away must stop").isTrue();
    watch.assertGaveUpWithin(10_000, "a statement stopped when its client leaves, against one that runs for 16 s");
    assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
    assertThat(entry.getTerminatedBy()).isEqualTo("client disconnected");
  }

  @Test
  void aKeepAliveConnectionServesItsNextRequests() throws Exception {
    try (final Socket socket = connect()) {
      for (int i = 0; i < 3; i++) {
        send(socket, command("RETURN " + i + " AS answer"));
        final String response = readResponse(socket.getInputStream());
        assertThat(response).startsWith("HTTP/1.1 200").contains("\"answer\":" + i);
      }
    }
  }

  @Test
  void aRequestPipelinedBehindARunningOneIsNotMistakenForADisconnect() throws Exception {
    try (final Socket socket = connect()) {
      send(socket, command(LONG_CYPHER));
      final RunningQuery entry = awaitRunning();

      // The client sends its next request while the first still runs: it is alive, and the bytes are the next request's
      send(socket, command("RETURN 7 AS answer"));
      Thread.sleep(500);
      assertThat(entry.isTerminated()).as("a client that sends is still there").isFalse();

      // Stopped here instead of waiting 16 s for it; both requests are then answered, in order, on the same connection
      entry.terminate("root");
      final InputStream in = socket.getInputStream();
      assertThat(readResponse(in)).startsWith("HTTP/1.1 409");
      assertThat(readResponse(in)).startsWith("HTTP/1.1 200").contains("\"answer\":7");
    }
  }

  @Test
  void aRemoteClientIsToldItsStatementWasTerminated() throws Exception {
    try (final RemoteDatabase remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(0), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {
      final CompletableFuture<Void> running = CompletableFuture.runAsync(() -> {
        try (final ResultSet rs = remote.command("opencypher", LONG_CYPHER)) {
          while (rs.hasNext())
            rs.next();
        }
      });
      awaitRunning().terminate("root");
      assertThatThrownBy(() -> running.get(30, TimeUnit.SECONDS)).hasCauseInstanceOf(QueryTerminatedException.class);
    }
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

  private String command(final String cypher) {
    final byte[] body = new JSONObject().put("language", "opencypher").put("command", cypher).toString()
        .getBytes(StandardCharsets.UTF_8);
    final String auth = Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    return "POST /api/v1/command/" + getDatabaseName() + " HTTP/1.1\r\n" + "Host: localhost\r\n" + "Authorization: Basic " + auth + "\r\n"
        + "Content-Type: application/json\r\n" + "Content-Length: " + body.length + "\r\n\r\n" + new String(body,
        StandardCharsets.UTF_8);
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
