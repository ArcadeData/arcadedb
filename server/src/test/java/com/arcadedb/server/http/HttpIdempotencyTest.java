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
package com.arcadedb.server.http;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end {@code X-Request-Id} idempotency: which retries are replayed, refused while in flight, or executed.
 */
class HttpIdempotencyTest {
  // Regression: #5023 a different write reusing an X-Request-Id was replayed
  /**
   * End-to-end regression for issue #5023. The idempotency cache used to key solely on the client
   * {@code X-Request-Id}, so a second, DIFFERENT write that happened to reuse the same correlation id had
   * the first request's response replayed and its write silently skipped. It also must still de-duplicate a
   * genuine retry of the SAME request so a double-commit does not occur.
   * <p>
   * Assertions compare the response bodies (which carry the inserted record @rid) rather than reading back
   * through the embedded database, so they are not subject to HTTP-commit / embedded-read visibility timing.
   * A distinct INSERT returns a distinct @rid, so a replayed response is byte-identical to the original.
   */
  @Nested
  class KeyReplay extends BaseGraphServerTest {

    private static final String DATABASE_NAME = "graph";

    @Test
    void sameRequestIdDifferentCommandStillExecutes() throws Exception {
      // Uses the pre-existing "Person" type (created in BaseGraphServerTest.populateDatabase).
      testEachServer(serverIndex -> {
        // First write with X-Request-Id: abc
        final String body1 = postCommand(serverIndex, "abc", "INSERT INTO Person SET marker = 'idem5023', n = 1");

        // Second write reuses the SAME X-Request-Id but is a DIFFERENT command. Before the fix this replayed
        // the first response (body-identical) and never executed the second write.
        final String body2 = postCommand(serverIndex, "abc", "INSERT INTO Person SET marker = 'idem5023', n = 2");

        // Both executed independently -> distinct inserted records -> distinct response bodies.
        assertThat(body2).isNotEqualTo(body1);
        assertThat(body1).contains("\"n\":1");
        assertThat(body2).contains("\"n\":2");
      });
    }

    @Test
    void sameRequestIdSameCommandIsDeduplicated() throws Exception {
      testEachServer(serverIndex -> {
        // Two identical retries of the same logical request (same id, same body): the second call must replay
        // the cached response instead of executing a second insert, so the bodies (including the @rid) match.
        final String body1 = postCommand(serverIndex, "retry-1", "INSERT INTO Person SET marker = 'idem5023dup'");
        final String body2 = postCommand(serverIndex, "retry-1", "INSERT INTO Person SET marker = 'idem5023dup'");

        assertThat(body2).isEqualTo(body1);
      });
    }

    private String postCommand(final int serverIndex, final String requestId, final String command) throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) new URL(getServerHttpUrl(serverIndex, "/api/v1/command/" + DATABASE_NAME)).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.setRequestProperty(IdempotencyCache.HEADER_REQUEST_ID, requestId);

      final JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", command);
      payload.put("autoCommit", true);
      formatPayload(connection, payload);
      connection.connect();
      try {
        final String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        return response;
      } finally {
        connection.disconnect();
      }
    }
  }

  // Regression: #8324 a retry of an in-flight request ran it a second time
  /**
   * End-to-end regression for issue #8324. A retry whose {@code X-Request-Id} (and method, path, database and body)
   * matches a request that is STILL executing used to wait 5 s for it and then "fall through and execute uncached":
   * the same write ran a second time, side by side with the first. Behind a follower's 504 for a long
   * {@code restore database} that is the retry most likely to arrive, and it ran the restore twice.
   * <p>
   * The first request here is a script that sleeps well past that 5 s wait and then inserts one marker record; the
   * retry arrives while it sleeps. The retry must be refused with {@code 409} + {@code Retry-After}, the marker must be
   * inserted exactly once, and a retry after the first execution settled must replay its answer.
   */
  @Nested
  @Tag("slow")
  class InFlightRetryIsNotExecutedTwice extends BaseGraphServerTest {

    private static final String DATABASE_NAME = "graph";
    private static final String REQUEST_ID    = "restore-8324";
    private static final String SCRIPT        = "SLEEP 9000; INSERT INTO Person SET marker = 'idem8324';";

    @Test
    void aRetryOfARequestStillExecutingIsRefusedAndNotExecutedASecondTime() throws Exception {
      final IdempotencyCache cache = getServer(0).getHttpServer().getIdempotencyCache();
      final int entriesBefore = cache.size();

      final CompletableFuture<Answer> first = CompletableFuture.supplyAsync(this::post);

      // The first request has reserved the key once its pending marker is in the cache.
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (cache.size() == entriesBefore && System.nanoTime() < deadline)
        Thread.sleep(20);
      assertThat(cache.size()).as("the first request never reserved its X-Request-Id").isGreaterThan(entriesBefore);

      final Answer retry = post();
      assertThat(first.isDone())
          .as("the retry must have been answered while the first execution was still running")
          .isFalse();
      assertThat(retry.status).as("body=%s", retry.body).isEqualTo(409);
      assertThat(retry.retryAfter).isEqualTo("5");
      assertThat(new JSONObject(retry.body).getString("error")).contains("still executing");
      // Issue #8343: the body names the refusal and its back-off, so a follower that forwarded this request to the
      // leader can rebuild it and answer its own client 409 + Retry-After instead of a generic 500.
      assertThat(new JSONObject(retry.body).getString("exception")).isEqualTo(RequestStillInFlightException.class.getName());
      assertThat(new JSONObject(retry.body).getString("exceptionArgs")).isEqualTo("5");

      final Answer original = first.get(60, TimeUnit.SECONDS);
      assertThat(original.status).as("body=%s", original.body).isEqualTo(200);

      // The same retry once the first execution settled replays its answer instead of executing.
      final Answer afterwards = post();
      assertThat(afterwards.status).isEqualTo(200);
      assertThat(afterwards.body).isEqualTo(original.body);

      try (final ResultSet rs = getServerDatabase(0, DATABASE_NAME).query("sql",
          "SELECT count(*) AS n FROM Person WHERE marker = 'idem8324'")) {
        assertThat(rs.next().<Long>getProperty("n"))
            .as("the script ran more than once: the refused retry (or the replayed one) executed it again")
            .isEqualTo(1L);
      }
    }

    private record Answer(int status, String body, String retryAfter) {
    }

    private Answer post() {
      try {
        final HttpURLConnection connection = (HttpURLConnection) new URL(
            getServerHttpUrl(0, "/api/v1/command/" + DATABASE_NAME)).openConnection();
        connection.setRequestMethod("POST");
        connection.setReadTimeout(60_000);
        connection.setRequestProperty("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
        connection.setRequestProperty(IdempotencyCache.HEADER_REQUEST_ID, REQUEST_ID);

        final JSONObject payload = new JSONObject();
        payload.put("language", "sqlscript");
        payload.put("command", SCRIPT);
        formatPayload(connection, payload);
        connection.connect();
        try {
          final int status = connection.getResponseCode();
          final String body = status < 400 ? readResponse(connection) : readError(connection);
          return new Answer(status, body, connection.getHeaderField("Retry-After"));
        } finally {
          connection.disconnect();
        }
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  // Regression: #8331 a streamed restore was not cached, so a retry restored again
  /**
   * Issue #8331: a {@code restore database} (or {@code restore backup}, {@code import database}) asked for as an SSE
   * progress stream wrote the stream itself and returned no response, so its {@code X-Request-Id} reservation was
   * aborted instead of completed and a retry with the same id ran the restore again. It is now answered to the cache in
   * both encodings: a retry is replayed as a one-frame stream of the {@code completed} event when it asks for a stream,
   * and as the JSON body otherwise.
   * <p>
   * The restored database is dropped between the request and its retry: a retry that executed again would restore it
   * once more, so its absence is what proves the retry was a replay.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class StreamedRestore extends BaseGraphServerTest {
    private final HttpClient client = HttpClient.newHttpClient();

    @Override
    protected void onServerConfiguration(final ContextConfiguration config) {
      super.onServerConfiguration(config);
      // Restoring from a local file is an operator action that has to be opted into (issue #5027)
      config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
    }

    @Test
    void aStreamedRestoreIsReplayedNotExecutedAgain() throws Exception {
      final String command = "restore database restored8331 file://" + backup().getAbsolutePath();
      try {
        final HttpResponse<String> first = serverCommand(command, "req-8331-stream", true);
        assertThat(first.statusCode()).as(first.body()).isEqualTo(200);
        assertThat(first.headers().firstValue("Content-Type")).hasValueSatisfying(v -> assertThat(v).contains("text/event-stream"));
        assertThat(first.body()).contains("\"status\":\"completed\"");
        assertThat(getServer(0).existsDatabase("restored8331")).isTrue();
        drop("restored8331");

        // The same request again, still asking for a stream: the terminal event, once, and no second restore
        final HttpResponse<String> streamedRetry = serverCommand(command, "req-8331-stream", true);
        assertThat(streamedRetry.statusCode()).as(streamedRetry.body()).isEqualTo(200);
        assertThat(streamedRetry.headers().firstValue("Content-Type")).hasValueSatisfying(v -> assertThat(v).contains("text/event-stream"));
        assertThat(streamedRetry.body()).startsWith("data: ").contains("\"status\":\"completed\"")
            .contains("restored8331 restored successfully").doesNotContain("\"status\":\"progress\"");
        assertThat(streamedRetry.body().split("\n\n")).hasSize(1);

        // ...and as the JSON a buffered request is answered with
        final HttpResponse<String> bufferedRetry = serverCommand(command, "req-8331-stream", false);
        assertThat(bufferedRetry.statusCode()).as(bufferedRetry.body()).isEqualTo(200);
        assertThat(new JSONObject(bufferedRetry.body()).getString("result")).isEqualTo("ok");

        assertThat(getServer(0).existsDatabase("restored8331")).as("a retry restored the database a second time").isFalse();
      } finally {
        drop("restored8331");
      }
    }

    @Test
    void aBufferedRestoreRetriedAsAStreamGetsTheTerminalEvent() throws Exception {
      final String command = "restore database restored8331b file://" + backup().getAbsolutePath();
      try {
        final HttpResponse<String> first = serverCommand(command, "req-8331-buffered", false);
        assertThat(first.statusCode()).as(first.body()).isEqualTo(200);
        assertThat(getServer(0).existsDatabase("restored8331b")).isTrue();
        drop("restored8331b");

        final HttpResponse<String> streamedRetry = serverCommand(command, "req-8331-buffered", true);
        assertThat(streamedRetry.statusCode()).as(streamedRetry.body()).isEqualTo(200);
        assertThat(streamedRetry.body()).startsWith("data: ").contains("\"status\":\"completed\"");
        assertThat(getServer(0).existsDatabase("restored8331b")).isFalse();
      } finally {
        drop("restored8331b");
      }
    }

    @Test
    void aStreamedRestoreThatFailsIsNotReplayed() throws Exception {
      // Failures are not cached, exactly as for a buffered request: the retry executes again (and fails again)
      final String command = "restore database restored8331c file:///nonexistent/archive-8331.zip";
      // The restore had started streaming when it failed, so the failure is an error frame of a 200 stream
      final HttpResponse<String> first = serverCommand(command, "req-8331-failed", true);
      assertThat(first.body()).contains("\"status\":\"error\"");
      final HttpResponse<String> retry = serverCommand(command, "req-8331-failed", true);
      assertThat(retry.body()).contains("\"status\":\"error\"").doesNotContain("\"status\":\"completed\"");
      assertThat(getServer(0).existsDatabase("restored8331c")).isFalse();
    }

    private File backup() throws Exception {
      final HttpRequest request = HttpRequest.newBuilder()
          .uri(new URI(getServerHttpUrl("/api/v1/command/graph")))
          .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", "sql").put("command", "backup database").toString()))
          .setHeader("Authorization", authorization())
          .build();
      final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      final String fileName = new JSONObject(response.body()).getJSONArray("result").getJSONObject(0).getString("backupFile");
      final File file = new File("./target/backups/graph", fileName);
      assertThat(file).exists();
      return file;
    }

    private HttpResponse<String> serverCommand(final String command, final String requestId, final boolean stream) throws Exception {
      final HttpRequest.Builder builder = HttpRequest.newBuilder()
          .uri(new URI(getServerHttpUrl("/api/v1/server")))
          .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
          .setHeader("Authorization", authorization())
          .setHeader("Content-Type", "application/json")
          .setHeader(IdempotencyCache.HEADER_REQUEST_ID, requestId);
      if (stream)
        builder.setHeader("Accept", "text/event-stream");
      return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private void drop(final String database) throws Exception {
      if (!getServer(0).existsDatabase(database))
        return;
      final HttpRequest request = HttpRequest.newBuilder()
          .uri(new URI(getServerHttpUrl("/api/v1/server")))
          .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", "drop database " + database).toString()))
          .setHeader("Authorization", authorization())
          .build();
      final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    }

    private static String authorization() {
      return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
    }
  }
}
