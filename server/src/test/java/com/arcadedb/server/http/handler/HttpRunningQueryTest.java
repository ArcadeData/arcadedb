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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.utility.StallAwareStopwatch;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Statements running on behalf of an HTTP client: the client-disconnect watch, the query tag a client labels its
 * statement with, and listing and terminating running statements.
 */
class HttpRunningQueryTest extends BaseGraphServerTest {
  /*
   * Issue #9689: an HTTP client that goes away while its statement runs - its own deadline, a killed process,
   * {@code curl --max-time} - terminates the statement, instead of leaving the server busy on an answer nobody will read
   * until the statement completes or reaches {@code arcadedb.command.timeout}. A client that is still there is never
   * mistaken for one that left: a keep-alive connection serves its next request, and a request a client pipelines behind
   * a running one is answered once the first is. And a Java client is told its statement was terminated with the type that
   * says so, rather than a generic remote failure.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // A hang detector, not a latency bound: a terminate that regressed would otherwise leave each test running its 16 s statement
  /** About 16 s on one core when left alone, all of it inside one aggregation. */
  private static final String LONG_CYPHER =
      "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j RETURN sum(sin(toFloat(i * j))) AS total";

  // Regression: #9689 a client that goes away terminates its statement, one that stays is never mistaken for it
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
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

  // Regression: #9689 a client that goes away terminates its statement, one that stays is never mistaken for it
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @Test
  void theWatchCanBeTurnedOffForClientsThatHalfClose() throws Exception {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_TERMINATE_ON_CLIENT_DISCONNECT, false);
    try {
      final RunningQuery entry;
      try (final Socket socket = connect()) {
        send(socket, command(LONG_CYPHER));
        entry = awaitRunning();
        // A client that half-closes after its request, still reading: with the watch off the statement goes on
        socket.shutdownOutput();
        Thread.sleep(1_000);
        assertThat(entry.isTerminated()).as("the watch is off: nothing terminates the statement").isFalse();
        entry.terminate("root");
        assertThat(readResponse(socket.getInputStream())).startsWith("HTTP/1.1 409");
      }
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_TERMINATE_ON_CLIENT_DISCONNECT, true);
    }
  }

  // Regression: #9689 a client that goes away terminates its statement, one that stays is never mistaken for it
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
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

  // Regression: #9689 a client that goes away terminates its statement, one that stays is never mistaken for it
  /**
   * A streamed answer completes the exchange inside the handler, before the statement's entry is closed: Undertow has
   * already resumed reads for the next request by then, and the watch must leave them alone. It used to suspend them
   * again, so the next request on the connection was never read (found by the TypeScript client's end-to-end suite).
   */
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @Test
  void aStreamedAnswerLeavesTheConnectionReadyForTheNextRequest() throws Exception {
    try (final Socket socket = connect()) {
      for (int i = 0; i < 3; i++) {
        send(socket, streamingQuery("UNWIND range(1, 50) AS x RETURN x"));
        final String streamed = readChunkedResponse(socket.getInputStream());
        assertThat(streamed).startsWith("HTTP/1.1 200").contains("\"stats\"");

        send(socket, command("RETURN " + i + " AS answer"));
        assertThat(readResponse(socket.getInputStream())).as("the request after a streamed answer, round %s", i)
            .startsWith("HTTP/1.1 200").contains("\"answer\":" + i);
      }
    }
  }

  // Regression: #9689 a client that goes away terminates its statement, one that stays is never mistaken for it
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
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

  // Regression: #9689 a client that goes away terminates its statement, one that stays is never mistaken for it
  /**
   * The listener swap under load: clients that keep their connection, pipeline, and walk away in the middle of a long
   * statement, all at once. Every request a client stayed for is answered, in order, with its own answer - no byte lost
   * or misplaced by a watch - and every statement whose client left is stopped.
   */
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @Test
  void manyClientsPipeliningAndLeavingAreEachServedOrStopped() throws Exception {
    final int clients = 8;
    final int rounds = 12;
    final ExecutorService pool = Executors.newFixedThreadPool(clients);
    try {
      final List<Future<Integer>> results = new ArrayList<>();
      for (int c = 0; c < clients; c++) {
        final int client = c;
        results.add(pool.submit(() -> {
          final Random random = new Random(9689L + client);
          int answered = 0;
          Socket socket = connect();
          try {
            for (int round = 0; round < rounds; round++) {
              final int choice = random.nextInt(3);
              if (choice == 0) {
                // One request at a time on a kept-alive connection
                final int n = client * 1000 + round;
                send(socket, command("RETURN " + n + " AS answer"));
                assertThat(readResponse(socket.getInputStream())).startsWith("HTTP/1.1 200").contains("\"answer\":" + n);
                ++answered;
              } else if (choice == 1) {
                // Two requests in one write: both answered, in order
                final int a = client * 1000 + round;
                final int b = a + 500;
                send(socket, command("RETURN " + a + " AS answer") + command("RETURN " + b + " AS answer"));
                assertThat(readResponse(socket.getInputStream())).startsWith("HTTP/1.1 200").contains("\"answer\":" + a);
                assertThat(readResponse(socket.getInputStream())).startsWith("HTTP/1.1 200").contains("\"answer\":" + b);
                answered += 2;
              } else {
                // A long statement whose client leaves: a fresh connection afterwards
                send(socket, command(LONG_CYPHER));
                Thread.sleep(random.nextInt(200));
                socket.close();
                socket = connect();
              }
            }
          } finally {
            socket.close();
          }
          return answered;
        }));
      }
      int answered = 0;
      for (final Future<Integer> result : results)
        answered += result.get(120, TimeUnit.SECONDS);
      assertThat(answered).isPositive();
    } finally {
      pool.shutdownNow();
    }

    // Every long statement was left by its client, and every one of them stops
    await().atMost(Duration.ofSeconds(60)).until(() -> getServer(0).getRunningQueries().getRunning().isEmpty());
  }

  // Regression: #9689 a client that goes away terminates its statement, one that stays is never mistaken for it
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
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

  private String streamingQuery(final String cypher) {
    final byte[] body = new JSONObject().put("language", "opencypher").put("command", cypher).toString()
        .getBytes(StandardCharsets.UTF_8);
    final String auth = Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    return "POST /api/v1/query/" + getDatabaseName() + " HTTP/1.1\r\n" + "Host: localhost\r\n" + "Authorization: Basic " + auth
        + "\r\n" + "Accept: application/x-ndjson\r\n" + "Content-Type: application/json\r\n" + "Content-Length: " + body.length
        + "\r\n\r\n" + new String(body, StandardCharsets.UTF_8);
  }

  /** Reads one response sent with {@code Transfer-Encoding: chunked}, the encoding a streamed answer uses. */
  private static String readChunkedResponse(final InputStream in) throws IOException {
    final String headers = readHead(in);
    if (!headers.toLowerCase().contains("transfer-encoding: chunked"))
      throw new IOException("Expected a chunked response, got: " + headers);
    final StringBuilder body = new StringBuilder(headers);
    while (true) {
      final int size = Integer.parseInt(readLine(in).trim(), 16);
      if (size == 0) {
        readLine(in);
        return body.toString();
      }
      body.append(new String(in.readNBytes(size), StandardCharsets.UTF_8));
      readLine(in);
    }
  }

  private static String readLine(final InputStream in) throws IOException {
    final ByteArrayOutputStream line = new ByteArrayOutputStream();
    int b;
    while ((b = in.read()) >= 0 && b != '\n')
      if (b != '\r')
        line.write(b);
    if (b < 0)
      throw new IOException("Connection closed while reading a chunk");
    return line.toString(StandardCharsets.UTF_8);
  }

  /** The status line and headers of a response, up to and including the blank line. */
  private static String readHead(final InputStream in) throws IOException {
    final ByteArrayOutputStream head = new ByteArrayOutputStream();
    int matched = 0;
    while (matched < 4) {
      final int b = in.read();
      if (b < 0)
        throw new IOException("Connection closed while reading the response headers: " + head);
      head.write(b);
      matched = (b == '\r' && (matched == 0 || matched == 2)) || (b == '\n' && (matched == 1 || matched == 3)) ? matched + 1 : 0;
    }
    return head.toString(StandardCharsets.UTF_8);
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

  /*
   * Issue #9798: the client-disconnect watch of #9689 terminates the statement of a client whose connection closes, and a
   * terminated statement that ran in a {@code /begin} session used to end the session and roll its transaction back
   * whatever the route. A client read timeout on a long READ then destroyed the writes the client had made in its
   * transaction, which it reaches from any connection and still believed it owned. Whether a terminate may end the
   * caller's transaction is now the question the route already answers for a failure:
   * {@code rollsBackSessionTransactionOnFailure()}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // A hang detector, not a latency bound: a terminate that regressed would leave each test running its 16 s statement
  private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  // Regression: #9798 a terminate on disconnect ends the session transaction only on the routes that roll it back on failure
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @Test
  void aReadWhoseClientLeavesDoesNotRollBackTheSessionTransaction() throws Exception {
    final String sessionId = begin();
    command(sessionId, "CREATE VERTEX V1 SET id = 9798, name = 'kept'");

    // A long read in the session, over a connection the client then closes: its own read timeout
    final RunningQuery entry = runAndLeave("query", sessionId);
    assertThat(entry.awaitEnd(30_000)).as("the read of a client that went away stops").isTrue();
    assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
    assertThat(entry.getTerminatedBy()).isEqualTo("client disconnected");
    assertThat(isSessionRegistered(sessionId)).as("the read left the session alone").isTrue();

    // The client retries on a fresh connection and commits what it wrote before the read
    assertThat(post("/api/v1/commit/" + getDatabaseName(), sessionId, null).statusCode()).isEqualTo(204);
    assertThat(countKept()).as("the write made before the read is committed").isEqualTo(1L);
  }

  // Regression: #9798 a terminate on disconnect ends the session transaction only on the routes that roll it back on failure
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @Test
  void anAdministratorTerminatingAReadInASessionLeavesTheTransactionToTheClient() throws Exception {
    final String sessionId = begin();
    command(sessionId, "CREATE VERTEX V1 SET id = 9799, name = 'kept'");

    try (final Socket socket = connect()) {
      send(socket, request("query", sessionId));
      final RunningQuery entry = awaitRunning();
      entry.terminate("root");
      assertThat(entry.awaitEnd(30_000)).isTrue();
    }

    assertThat(post("/api/v1/commit/" + getDatabaseName(), sessionId, null).statusCode()).isEqualTo(204);
    assertThat(countKept()).isEqualTo(1L);
  }

  // Regression: #9798 a terminate on disconnect ends the session transaction only on the routes that roll it back on failure
  /** The counter-case that keeps the two above meaningful: a terminated COMMAND still ends its session, as its failure would. */
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @Test
  void aCommandWhoseClientLeavesStillEndsTheSession() throws Exception {
    final String sessionId = begin();
    command(sessionId, "CREATE VERTEX V1 SET id = 9800, name = 'kept'");

    final RunningQuery entry = runAndLeave("command", sessionId);
    assertThat(entry.awaitEnd(30_000)).isTrue();
    assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);

    assertThat(isSessionRegistered(sessionId)).as("a terminated command ends its session").isFalse();
    assertThat(countKept()).as("the session of a terminated command was rolled back").isZero();
  }

  // Regression: #9798 a terminate on disconnect ends the session transaction only on the routes that roll it back on failure
  /** The routes that stand outside the caller's transaction, or cannot write in it, are the ones a terminate leaves alone. */
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @Test
  void theRoutesThatLeaveTheTransactionAloneAreTheOnesThatDoNotRollItBackOnFailure() {
    assertThat(new PostQueryHandler(null).rollsBackSessionTransactionOnFailure()).isFalse();
    assertThat(new GetQueryHandler(null).rollsBackSessionTransactionOnFailure()).isFalse();
    assertThat(new PostTimeSeriesQueryHandler(null).rollsBackSessionTransactionOnFailure()).isFalse();
    assertThat(new PostTimeSeriesWriteHandler(null).rollsBackSessionTransactionOnFailure()).isFalse();
    assertThat(new GetTimeSeriesLatestHandler(null).rollsBackSessionTransactionOnFailure()).isFalse();
    assertThat(new PostPrometheusReadHandler(null).rollsBackSessionTransactionOnFailure()).isFalse();
    assertThat(new PostPrometheusWriteHandler(null).rollsBackSessionTransactionOnFailure()).isFalse();
    assertThat(new PostVectorSearchHandler(null).rollsBackSessionTransactionOnFailure()).isFalse();
    assertThat(new GetPromQLQueryRangeHandler(null).rollsBackSessionTransactionOnFailure()).isFalse();
    assertThat(new PostGrafanaQueryHandler(null).rollsBackSessionTransactionOnFailure()).isFalse();

    assertThat(new PostCommandHandler(null).rollsBackSessionTransactionOnFailure()).isTrue();
  }

  private RunningQuery runAndLeave(final String route, final String sessionId) throws IOException {
    final RunningQuery entry;
    try (final Socket socket = connect()) {
      send(socket, request(route, sessionId));
      entry = awaitRunning();
      assertThat(entry.getSessionId()).isEqualTo(sessionId);
    }
    return entry;
  }

  private boolean isSessionRegistered(final String sessionId) {
    return getServer(0).getHttpServer().getSessionManager().isSessionRegistered(sessionId);
  }

  private long countKept() throws Exception {
    final HttpResponse<String> response = post("/api/v1/query/" + getDatabaseName(), null,
        new JSONObject().put("language", "sql").put("command", "SELECT count(*) AS c FROM V1 WHERE name = 'kept'").toString());
    assertThat(response.statusCode()).isEqualTo(200);
    return new JSONObject(response.body()).getJSONArray("result").getJSONObject(0).getLong("c");
  }

  private String begin() throws Exception {
    final HttpResponse<String> response = post("/api/v1/begin/" + getDatabaseName(), null, null);
    assertThat(response.statusCode()).isEqualTo(204);
    return response.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElseThrow();
  }

  private void command(final String sessionId, final String sql) throws Exception {
    final HttpResponse<String> response = post("/api/v1/command/" + getDatabaseName(), sessionId,
        new JSONObject().put("language", "sql").put("command", sql).toString());
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
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

  private String request(final String route, final String sessionId) {
    final byte[] body = new JSONObject().put("language", "opencypher").put("command", LONG_CYPHER).toString()
        .getBytes(StandardCharsets.UTF_8);
    return "POST /api/v1/" + route + "/" + getDatabaseName() + " HTTP/1.1\r\n" + "Host: localhost\r\n" + "Authorization: "
        + basicAuth() + "\r\n" + HttpSessionManager.ARCADEDB_SESSION_ID + ": " + sessionId + "\r\n"
        + "Content-Type: application/json\r\n" + "Content-Length: " + body.length + "\r\n\r\n" + new String(body,
        StandardCharsets.UTF_8);
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  /*
   * Issue #9775: {@code /begin} read the {@code X-ArcadeDB-Query-Tag} header only after it had created and published the
   * transaction session, so a header refused there left a session in the manager, with a transaction attached, whose id the
   * client was never sent. The header is now checked with the rest of the request envelope, before any route runs, so a
   * refused one allocates nothing on any route.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String TAG_HEADER = AbstractServerHttpHandler.QUERY_TAG_HEADER;

  // Regression: #9775 an unusable query tag is refused before any route allocates a session
  @Test
  void aBeginWithATooLongTagIsRefusedWithoutLeavingASession() throws Exception {
    final HttpSessionManager sessions = getServer(0).getHttpServer().getSessionManager();
    final int before = sessions.getActiveSessions();

    final Response response = call("POST", "/api/v1/begin/graph", new JSONObject(),
        Map.of(TAG_HEADER, "x".repeat(AbstractServerHttpHandler.MAX_QUERY_TAG_LENGTH + 1)));

    assertThat(response.status).isEqualTo(400);
    assertThat(response.sessionId).as("no session id is handed out").isNull();
    assertThat(new JSONObject(response.body).getString("error"))
        .as("the client is told which header was refused, outside the detail production mode conceals")
        .contains(TAG_HEADER);
    assertThat(sessions.getActiveSessions())
        .as("the refused begin published no transaction session nobody could ever end")
        .isEqualTo(before);
  }

  // Regression: #9775 an unusable query tag is refused before any route allocates a session
  @Test
  void aBeginWithAControlCharacterInTheTagIsRefusedWithoutLeavingASession() throws Exception {
    final HttpSessionManager sessions = getServer(0).getHttpServer().getSessionManager();
    final int before = sessions.getActiveSessions();

    final Response response = call("POST", "/api/v1/begin/graph", new JSONObject(), Map.of(TAG_HEADER, "bad\u0007tag"));

    assertThat(response.status).isEqualTo(400);
    assertThat(response.sessionId).isNull();
    assertThat(sessions.getActiveSessions()).isEqualTo(before);
  }

  // Regression: #9775 an unusable query tag is refused before any route allocates a session
  @Test
  void aBeginWithAValidTagKeepsItOnTheSession() throws Exception {
    final Response begin = call("POST", "/api/v1/begin/graph", new JSONObject(), Map.of(TAG_HEADER, "  batch-9775  "));
    assertThat(begin.status).isEqualTo(204);
    assertThat(begin.sessionId).isNotNull();

    try {
      final HttpSession session = getServer(0).getHttpServer().getSessionManager().getSessions().stream()
          .filter(s -> s.id.equals(begin.sessionId)).findFirst().orElseThrow();
      assertThat(session.getTag()).isEqualTo("batch-9775");
    } finally {
      assertThat(call("POST", "/api/v1/rollback/graph", new JSONObject(), Map.of(HttpSessionManager.ARCADEDB_SESSION_ID, begin.sessionId)).status)
          .isEqualTo(204);
    }
  }

  // Regression: #9775 an unusable query tag is refused before any route allocates a session
  /**
   * The check belongs to the envelope, not to the routes that list a running statement: a route outside the admission gate
   * refuses the header too, rather than each handler having to remember to ask.
   */
  @Test
  void anUnusableTagIsRefusedOnARouteOutsideTheAdmissionGate() throws Exception {
    final Response response = call("POST", "/api/v1/server", new JSONObject().put("command", "list databases"),
        Map.of(TAG_HEADER, "x".repeat(AbstractServerHttpHandler.MAX_QUERY_TAG_LENGTH + 1)));
    assertThat(response.status).isEqualTo(400);

    assertThat(call("POST", "/api/v1/server", new JSONObject().put("command", "list databases"), Map.of(TAG_HEADER, "ok")).status)
        .isEqualTo(200);
  }

  // Regression: #9775 an unusable query tag is refused before any route allocates a session
  /**
   * The check runs before authentication: a request with no credentials and an unusable label is answered 400, and the answer
   * never echoes the label back.
   */
  @Test
  void anUnusableTagWithoutCredentialsIsRefusedWithoutEchoingIt() throws Exception {
    final String tag = "echo9775-" + "y".repeat(AbstractServerHttpHandler.MAX_QUERY_TAG_LENGTH);
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(0, "/api/v1/begin/graph")).toURL().openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty(TAG_HEADER, tag);
      final int status = connection.getResponseCode();
      assertThat(status).isEqualTo(400);
      assertThat(body(connection, status)).doesNotContain("echo9775");
    } finally {
      connection.disconnect();
    }
  }

  private record Response(int status, String body, String sessionId) {
  }

  private Response call(final String method, final String path, final JSONObject payload, final Map<String, String> headers)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(0, path)).toURL().openConnection();
    try {
      connection.setRequestMethod(method);
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      for (final Map.Entry<String, String> header : headers.entrySet())
        connection.setRequestProperty(header.getKey(), header.getValue());
      if (payload != null)
        formatPayload(connection, payload);
      final int status = connection.getResponseCode();
      return new Response(status, body(connection, status), connection.getHeaderField(HttpSessionManager.ARCADEDB_SESSION_ID));
    } finally {
      connection.disconnect();
    }
  }

  private static String body(final HttpURLConnection connection, final int status) throws IOException {
    try (final InputStream in = status < 400 ? connection.getInputStream() : connection.getErrorStream()) {
      return in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  // Regression: #9680 list the running statements, find one by its tag and terminate it
  /**
   * Issue #9680: over HTTP a client can list the statements a server is running, find its own by the label it gave it in
   * {@code X-ArcadeDB-Query-Tag}, terminate it, be told the work stopped, and see it gone from the list - with or without
   * a transaction session, as root or as the user who runs it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ListAndTerminate {
    /** About 16 s on one core when left alone, all of it inside one aggregation (the statement of the issue). */
    private static final String LONG_CYPHER =
        "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j RETURN sum(sin(toFloat(i * j))) AS total";

    private static final String TAG_HEADER   = "X-ArcadeDB-Query-Tag";
    private static final String OWNER        = "owner9680";
    private static final String OTHER        = "other9680";
    private static final String PASSWORD     = "pwd9680-secret";
    private static final String ROOT         = "root";

    @AfterEach
    void cleanUp() {
      GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
      GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
      final ServerSecurity security = getServer(0).getSecurity();
      for (final String user : new String[] { OWNER, OTHER })
        if (security.getUser(user) != null)
          security.dropUser(user);
    }

    @Test
    void aTaggedStatementIsListedTerminatedAndThenGone() throws Exception {
      final CompletableFuture<Response> running = runAsync(ROOT, "/api/v1/command/graph", cypher(LONG_CYPHER), Map.of(TAG_HEADER, "bench-1"));

      final JSONObject entry = awaitSingleEntry(ROOT, "bench-1");
      assertThat(entry.getString("language")).isEqualTo("opencypher");
      assertThat(entry.getString("text")).isEqualTo(LONG_CYPHER);
      assertThat(entry.getString("user")).isEqualTo(ROOT);
      assertThat(entry.getString("database")).isEqualTo("graph");
      assertThat(entry.getString("protocol")).isEqualTo("http");
      assertThat(entry.has("sessionId")).isFalse();

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final JSONObject terminated = serverCommand(ROOT, "terminate query " + entry.getString("id")).getJSONObject("result");
      assertThat(terminated.getString("id")).isEqualTo(entry.getString("id"));
      assertThat(terminated.getString("status")).isEqualTo("terminated");

      final Response response = running.get(30, TimeUnit.SECONDS);
      watch.assertGaveUpWithin(10_000, "a terminated statement stopping at its next check, against one that runs for 16 s");
      assertThat(response.status).isEqualTo(409);
      assertThat(response.body).contains("Query terminated").contains("QueryTerminatedException");

      assertThat(listQueries(ROOT, "bench-1")).isEmpty();
      assertThat(serverCommand(ROOT, "terminate query " + entry.getString("id")).getJSONObject("result").getString("status"))
          .isEqualTo("not found");

      final Response fresh = call(ROOT, "POST", "/api/v1/command/graph", cypher("RETURN 42 AS answer"), Map.of());
      assertThat(fresh.status).isEqualTo(200);
      assertThat(fresh.body).contains("42");
    }

    @Test
    void aLongSqlStatementIsTerminatedByItsTag() throws Exception {
      final Database database = getServerDatabase(0, "graph");
      if (!database.getSchema().existsType("Node9680")) {
        database.getSchema().createVertexType("Node9680");
        database.transaction(() -> {
          for (int i = 0; i < 5_000; i++)
            database.newVertex("Node9680").set("v", i).save();
        });
      }
      final String sql = "MATCH {type: Node9680, as: a}, {type: Node9680, as: b, where: (v + $matched.a.v = -1)} RETURN a.v";
      final CompletableFuture<Response> running = runAsync(ROOT, "/api/v1/query/graph",
          new JSONObject().put("language", "sql").put("command", sql), Map.of(TAG_HEADER, "bench-sql"));
      awaitSingleEntry(ROOT, "bench-sql");

      final JSONArray results = serverCommand(ROOT, "terminate queries tag bench-sql").getJSONArray("result");
      assertThat(results.length()).isEqualTo(1);
      assertThat(results.getJSONObject(0).getString("status")).isEqualTo("terminated");
      assertThat(running.get(30, TimeUnit.SECONDS).status).isEqualTo(409);
      assertThat(listQueries(ROOT, "bench-sql")).isEmpty();
    }

    @Test
    void terminatingAStatementInASessionRollsTheSessionBackAndEndsIt() throws Exception {
      final Database database = getServerDatabase(0, "graph");
      if (!database.getSchema().existsType("Tx9680"))
        database.getSchema().createDocumentType("Tx9680");

      // The tag is given to the session, and the statements run in it inherit it
      final Response begin = call(ROOT, "POST", "/api/v1/begin/graph", new JSONObject(), Map.of(TAG_HEADER, "bench-tx"));
      assertThat(begin.status).isEqualTo(204);
      final String session = begin.sessionId;
      assertThat(session).isNotNull();

      final Response write = call(ROOT, "POST", "/api/v1/command/graph",
          new JSONObject().put("language", "sql").put("command", "INSERT INTO Tx9680 SET x = 1"), Map.of("arcadedb-session-id", session));
      assertThat(write.status).isEqualTo(200);

      final JSONArray transactions = serverCommand(ROOT, "list transactions").getJSONArray("result");
      assertThat(findById(transactions, session)).isNotNull();
      assertThat(findById(transactions, session).getString("tag")).isEqualTo("bench-tx");

      final CompletableFuture<Response> running = runAsync(ROOT, "/api/v1/command/graph", cypher(LONG_CYPHER),
          Map.of("arcadedb-session-id", session));
      final JSONObject entry = awaitSingleEntry(ROOT, "bench-tx");
      assertThat(entry.getString("sessionId")).isEqualTo(session);

      assertThat(serverCommand(ROOT, "terminate query " + entry.getString("id")).getJSONObject("result").getString("status"))
          .isEqualTo("terminated");
      final Response response = running.get(30, TimeUnit.SECONDS);
      assertThat(response.status).isEqualTo(409);

      // The session is gone and what it wrote with it. A commit naming it publishes nothing and is answered as for any
      // session that no longer resolves (issue #7714): the idempotent 204, with the header saying the session is gone
      assertThat(findById(serverCommand(ROOT, "list transactions").getJSONArray("result"), session)).isNull();
      assertCommitFindsNoSession(session);
      assertThat(database.countType("Tx9680", false)).isZero();
    }

    @Test
    void anIdleSessionIsTerminatedAndRolledBack() throws Exception {
      final Database database = getServerDatabase(0, "graph");
      if (!database.getSchema().existsType("Idle9680"))
        database.getSchema().createDocumentType("Idle9680");

      final Response begin = call(ROOT, "POST", "/api/v1/begin/graph", new JSONObject(), Map.of());
      final String session = begin.sessionId;
      assertThat(call(ROOT, "POST", "/api/v1/command/graph",
          new JSONObject().put("language", "sql").put("command", "INSERT INTO Idle9680 SET x = 1"),
          Map.of("arcadedb-session-id", session)).status).isEqualTo(200);

      assertThat(serverCommand(ROOT, "terminate transaction " + session).getJSONObject("result").getString("status"))
          .isEqualTo("terminated");
      assertThat(serverCommand(ROOT, "terminate transaction " + session).getJSONObject("result").getString("status"))
          .isEqualTo("not found");
      assertCommitFindsNoSession(session);
      assertThat(database.countType("Idle9680", false)).isZero();
    }

    @Test
    void terminatingABusySessionStopsItsStatementAndEndsIt() throws Exception {
      final Database database = getServerDatabase(0, "graph");
      if (!database.getSchema().existsType("Busy9680"))
        database.getSchema().createDocumentType("Busy9680");

      final String session = call(ROOT, "POST", "/api/v1/begin/graph", new JSONObject(), Map.of(TAG_HEADER, "bench-busy")).sessionId;
      assertThat(call(ROOT, "POST", "/api/v1/command/graph",
          new JSONObject().put("language", "sql").put("command", "INSERT INTO Busy9680 SET x = 1"),
          Map.of("arcadedb-session-id", session)).status).isEqualTo(200);

      final CompletableFuture<Response> running = runAsync(ROOT, "/api/v1/command/graph", cypher(LONG_CYPHER),
          Map.of("arcadedb-session-id", session));
      final JSONObject entry = awaitSingleEntry(ROOT, "bench-busy");
      final JSONObject listed = findById(serverCommand(ROOT, "list transactions").getJSONArray("result"), session);
      assertThat(listed.getBoolean("busy")).isTrue();
      assertThat(listed.getJSONArray("runningQueries").getString(0)).isEqualTo(entry.getString("id"));

      assertThat(serverCommand(ROOT, "terminate transaction " + session).getJSONObject("result").getString("status"))
          .isEqualTo("terminated");
      assertThat(running.get(30, TimeUnit.SECONDS).status).isEqualTo(409);
      assertThat(listQueries(ROOT, "bench-busy")).isEmpty();
      assertThat(findById(serverCommand(ROOT, "list transactions").getJSONArray("result"), session)).isNull();
      assertCommitFindsNoSession(session);
      assertThat(database.countType("Busy9680", false)).isZero();
    }

    @Test
    void aUserSeesAndStopsOnlyTheirOwnStatements() throws Exception {
      createUser(OWNER);
      createUser(OTHER);

      final CompletableFuture<Response> running = runAsync(OWNER, "/api/v1/command/graph", cypher(LONG_CYPHER),
          Map.of(TAG_HEADER, "bench-owner"));
      final JSONObject entry = awaitSingleEntry(ROOT, "bench-owner");
      assertThat(entry.getString("user")).isEqualTo(OWNER);

      // Another user neither sees it nor can stop it, and cannot tell it exists
      assertThat(listQueries(OTHER, "bench-owner")).isEmpty();
      assertThat(serverCommand(OTHER, "terminate query " + entry.getString("id")).getJSONObject("result").getString("status"))
          .isEqualTo("not found");
      assertThat(serverCommand(OTHER, "terminate queries tag bench-owner").getJSONArray("result")).isEmpty();
      assertThat(listQueries(ROOT, "bench-owner")).hasSize(1);

      // Its owner sees it and stops it
      assertThat(listQueries(OWNER, "bench-owner")).hasSize(1);
      assertThat(serverCommand(OWNER, "terminate query " + entry.getString("id")).getJSONObject("result").getString("status"))
          .isEqualTo("terminated");
      assertThat(running.get(30, TimeUnit.SECONDS).status).isEqualTo(409);

      // The other commands stay root-only
      assertThat(call(OWNER, "POST", "/api/v1/server", new JSONObject().put("command", "list backups graph"), Map.of()).status)
          .isEqualTo(403);
    }

    @Test
    void aSaturatedAdmissionGateDoesNotHoldBackTheTermination() throws Exception {
      GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
      GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(60_000L);

      final CompletableFuture<Response> running = runAsync(ROOT, "/api/v1/command/graph", cypher(LONG_CYPHER),
          Map.of(TAG_HEADER, "bench-gate"));
      final JSONObject entry = awaitSingleEntry(ROOT, "bench-gate");
      // The only slot is taken: a second statement waits for it, and is not listed while it waits
      final CompletableFuture<Response> queued = runAsync(ROOT, "/api/v1/command/graph", cypher("RETURN 1 AS one"),
          Map.of(TAG_HEADER, "bench-queued"));
      Thread.sleep(200);
      assertThat(listQueries(ROOT, "bench-queued")).isEmpty();

      assertThat(serverCommand(ROOT, "terminate query " + entry.getString("id")).getJSONObject("result").getString("status"))
          .isEqualTo("terminated");
      assertThat(running.get(30, TimeUnit.SECONDS).status).isEqualTo(409);
      assertThat(queued.get(30, TimeUnit.SECONDS).status).isEqualTo(200);
    }

    @Test
    void anUnusableTagIsRefused() throws Exception {
      final Response response = call(ROOT, "POST", "/api/v1/command/graph", cypher("RETURN 1 AS one"),
          Map.of(TAG_HEADER, "x".repeat(AbstractServerHttpHandler.MAX_QUERY_TAG_LENGTH + 1)));
      assertThat(response.status).isEqualTo(400);

      assertThat(AbstractServerHttpHandler.validateQueryTag("  bench  ")).isEqualTo("bench");
      assertThat(AbstractServerHttpHandler.validateQueryTag("   ")).isNull();
      assertThatThrownBy(() -> AbstractServerHttpHandler.validateQueryTag("a\nb")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void malformedCommandsAreRefused() throws Exception {
      for (final String command : new String[] { "terminate query", "terminate queries q1", "list queries foo", "list transactions x",
          "terminate transaction" })
        assertThat(call(ROOT, "POST", "/api/v1/server", new JSONObject().put("command", command), Map.of()).status)
            .as(command).isEqualTo(400);
    }

    // ---------------------------------------------------------------------------------------------

    private record Response(int status, String body, String sessionId, String sessionExpired) {
    }

    private void assertCommitFindsNoSession(final String session) throws Exception {
      final Response commit = call(ROOT, "POST", "/api/v1/commit/graph", new JSONObject(), Map.of("arcadedb-session-id", session));
      // A commit of a terminated session commits nothing, and must not answer the 204 of one that did (issue #10164)
      assertThat(commit.status).isEqualTo(404);
      assertThat(commit.sessionExpired).isEqualTo(session);
    }

    private static JSONObject cypher(final String text) {
      return new JSONObject().put("language", "opencypher").put("command", text);
    }

    private void createUser(final String name) {
      final ServerSecurity security = getServer(0).getSecurity();
      if (security.getUser(name) == null)
        security.createUser(new JSONObject().put("name", name).put("password", security.encodePassword(PASSWORD))
            .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { "admin" }))));
    }

    private CompletableFuture<Response> runAsync(final String user, final String path, final JSONObject payload,
        final Map<String, String> headers) {
      return CompletableFuture.supplyAsync(() -> {
        try {
          return call(user, "POST", path, payload, headers);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    /** Waits for the statement with {@code tag} to be listed, and returns its entry: there must be exactly one. */
    private JSONObject awaitSingleEntry(final String user, final String tag) throws Exception {
      final long deadline = System.currentTimeMillis() + 15_000;
      while (System.currentTimeMillis() < deadline) {
        final JSONArray entries = listQueries(user, tag);
        // Listed once the statement is known too: the entry is registered when the request is admitted and its text recorded
        // once the handler has parsed it, so a listing in between shows an entry whose language is absent or null yet
        if (!entries.isEmpty() && entries.getJSONObject(0).getString("language", null) != null) {
          assertThat(entries.length()).isEqualTo(1);
          assertThat(entries.getJSONObject(0).getString("tag")).isEqualTo(tag);
          return entries.getJSONObject(0);
        }
        Thread.sleep(20);
      }
      throw new AssertionError("The statement tagged '" + tag + "' was never listed");
    }

    private JSONArray listQueries(final String user, final String tag) throws Exception {
      return serverCommand(user, "list queries tag " + tag).getJSONArray("result");
    }

    private JSONObject serverCommand(final String user, final String command) throws Exception {
      final Response response = call(user, "POST", "/api/v1/server", new JSONObject().put("command", command), Map.of());
      assertThat(response.status).as("%s: %s", command, response.body).isEqualTo(200);
      return new JSONObject(response.body);
    }

    private static JSONObject findById(final JSONArray array, final String id) {
      for (int i = 0; i < array.length(); i++)
        if (id.equals(array.getJSONObject(i).getString("id")))
          return array.getJSONObject(i);
      return null;
    }

    private Response call(final String user, final String method, final String path, final JSONObject payload,
        final Map<String, String> headers) throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(0, path)).toURL().openConnection();
      try {
        connection.setRequestMethod(method);
        final String password = ROOT.equals(user) ? DEFAULT_PASSWORD_FOR_TESTS : PASSWORD;
        connection.setRequestProperty("Authorization",
            "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8)));
        connection.setReadTimeout(60_000);
        for (final Map.Entry<String, String> header : headers.entrySet())
          connection.setRequestProperty(header.getKey(), header.getValue());
        if (payload != null)
          formatPayload(connection, payload);
        final int status = connection.getResponseCode();
        return new Response(status, body(connection, status), connection.getHeaderField("arcadedb-session-id"),
            connection.getHeaderField(DatabaseAbstractHandler.SESSION_EXPIRED));
      } finally {
        connection.disconnect();
      }
    }

    private static String body(final HttpURLConnection connection, final int status) throws IOException {
      try (final InputStream in = status < 400 ? connection.getInputStream() : connection.getErrorStream()) {
        return in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      }
    }
  }
}
