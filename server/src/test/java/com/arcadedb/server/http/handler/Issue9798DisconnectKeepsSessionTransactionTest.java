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

import com.arcadedb.query.RunningQuery;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpSessionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
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
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class Issue9798DisconnectKeepsSessionTransactionTest extends BaseGraphServerTest {
  /** About 16 s on one core when left alone, all of it inside one aggregation. */
  private static final String LONG_CYPHER =
      "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j RETURN sum(sin(toFloat(i * j))) AS total";

  private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

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

  /** The counter-case that keeps the two above meaningful: a terminated COMMAND still ends its session, as its failure would. */
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

  /** The routes that stand outside the caller's transaction, or cannot write in it, are the ones a terminate leaves alone. */
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

  private static void send(final Socket socket, final String request) throws IOException {
    final OutputStream out = socket.getOutputStream();
    out.write(request.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }
}
