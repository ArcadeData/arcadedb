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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.function.java.JavaClassFunctionLibraryDefinition;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Issue #7306, part 1: the HTTP query endpoints buffered the whole result server-side before the first byte
 * reached the client, so a large result was fully resident in the server heap whether or not the client meant to
 * read it all.
 * <p>
 * The tests that matter here are the two that a buffered implementation cannot pass:
 * {@link #postQueryDeliversTheFirstRowBeforeTheLastIsProduced()} and its GET twin, which block the engine inside
 * the query and then require the first row to have already arrived. Every other test in this class would pass
 * against a "stream" that simply serialized everything and sent it in one write, which is precisely the
 * implementation the issue is about.
 * <p>
 * The rest pins what must NOT change: the {@code application/json} body of the same request, byte for byte.
 */
public class Issue7306HttpStreamingQueryIT extends BaseGraphServerTest {
  private static final String TYPE_NAME    = "Stream7306";
  private static final int    ROW_COUNT    = 12;
  private static final String NDJSON       = "application/x-ndjson";
  private static final String GATE_LIBRARY = "gate7306";

  /**
   * How long the client waits for the response to start. Sized to fail a buffered implementation quickly and
   * deterministically - the server would otherwise close the idle connection first and the failure would read as
   * a socket EOF - while being far longer than a working stream ever needs, which starts on the first row.
   */
  private static final Duration STREAM_OPEN_TIMEOUT = Duration.ofSeconds(20);

  /**
   * A SQL function that lets the test stop the engine in the middle of a result set. The first call returns
   * immediately - that is the row the client must receive - and every later call blocks until the test releases
   * it. Both latches are replaced per test method, since a {@link CountDownLatch} cannot be reset.
   */
  public static class StreamGate {
    static volatile CountDownLatch release      = new CountDownLatch(1);
    static volatile CountDownLatch firstRowDone = new CountDownLatch(1);
    static final    AtomicInteger  calls        = new AtomicInteger();

    static void reset() {
      release = new CountDownLatch(1);
      firstRowDone = new CountDownLatch(1);
      calls.set(0);
    }

    public static int hold() {
      final int call = calls.incrementAndGet();
      if (call == 1)
        return call;
      firstRowDone.countDown();
      try {
        // A generous bound: it exists so a broken run fails instead of hanging the suite, and it is never
        // reached on a working one, where the test releases the latch as soon as it has read the first row.
        if (!release.await(60, TimeUnit.SECONDS))
          throw new IllegalStateException("the streaming test never released the gate");
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted while gating the stream", e);
      }
      return call;
    }
  }

  @Override
  protected void populateDatabase() {
    super.populateDatabase();

    final Database db = getDatabase(0);
    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME + " BUCKETS 1");
      db.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".idx INTEGER");
      db.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".name STRING");
      for (int i = 0; i < ROW_COUNT; i++)
        db.newDocument(TYPE_NAME).set("idx", i).set("name", "row-" + i).save();
    });
  }

  // ───────────────────────────── the tests a buffered implementation fails ─────────────────────────────

  /**
   * The whole point of the feature. The query is gated so that the engine cannot produce row 2 until the test
   * says so; the test then requires row 1 to have already been delivered. A handler that serializes the result
   * before writing anything deadlocks here and the read times out, which is what makes this test able to fail.
   */
  @Test
  void postQueryDeliversTheFirstRowBeforeTheLastIsProduced() throws Exception {
    assertFirstRowArrivesWhileTheEngineIsBlocked(this::openGatedPostStream);
  }

  /**
   * The same property on the GET endpoint, which additionally has to have moved itself off the Undertow IO
   * thread to be able to write blocking output at all.
   */
  @Test
  void getQueryDeliversTheFirstRowBeforeTheLastIsProduced() throws Exception {
    assertFirstRowArrivesWhileTheEngineIsBlocked(this::openGatedGetStream);
  }

  private interface StreamOpener {
    HttpResponse<InputStream> open() throws Exception;
  }

  private void assertFirstRowArrivesWhileTheEngineIsBlocked(final StreamOpener opener) throws Exception {
    StreamGate.reset();
    registerGate();

    final ExecutorService reader = Executors.newSingleThreadExecutor();
    try {
      // The server does not send the response headers until the first flush, so a buffered implementation makes
      // this call itself block until the whole result is ready - which the gate prevents from ever happening.
      // Catch that here rather than letting it surface as an opaque socket EOF: the message is what tells the
      // next reader that the encoding regressed, not the connection.
      final HttpResponse<InputStream> response;
      try {
        response = opener.open();
      } catch (final IOException e) {
        fail("nothing reached the client while the engine was still producing the result: the response is "
            + "buffered, not streamed (" + e + ")");
        return;
      }
      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(response.headers().firstValue("Content-Type").orElse("")).contains(NDJSON);

      final BufferedReader lines = new BufferedReader(
          new InputStreamReader(response.body(), StandardCharsets.UTF_8));

      // Wait until the engine is definitely parked inside row 2, so "the first row arrived" cannot be read as
      // "the query had already finished".
      assertThat(StreamGate.firstRowDone.await(60, TimeUnit.SECONDS))
          .as("the engine never reached the gated row")
          .isTrue();

      final Future<String> firstLine = reader.submit(lines::readLine);
      final String first;
      try {
        first = firstLine.get(30, TimeUnit.SECONDS);
      } catch (final TimeoutException e) {
        firstLine.cancel(true);
        StreamGate.release.countDown();
        fail("no row reached the client while the engine was still producing the result: the response is "
            + "buffered, not streamed");
        return;
      }

      assertThat(new JSONObject(first).getJSONObject("record").getInt("idx")).isEqualTo(0);

      // Let the rest of the result through and check the stream completes normally.
      StreamGate.release.countDown();
      final List<JSONObject> rest = readAll(lines);
      assertThat(rest).hasSize(ROW_COUNT); // ROW_COUNT-1 remaining records plus the trailer
      assertThat(rest.getLast().has("stats")).isTrue();
      assertThat(rest.getLast().getJSONObject("stats").getInt("returned")).isEqualTo(ROW_COUNT);

    } finally {
      StreamGate.release.countDown();
      reader.shutdownNow();
      unregisterGate();
    }
  }

  // ───────────────────────────── format, equivalence and back-compatibility ─────────────────────────────

  @Test
  void postQueryStreamsEveryRowFollowedByATrailer() throws Exception {
    final List<JSONObject> events = readAllEvents(postStream(
        "SELECT idx, name FROM " + TYPE_NAME + " ORDER BY idx", "query"));

    assertThat(events).hasSize(ROW_COUNT + 1);
    for (int i = 0; i < ROW_COUNT; i++)
      assertThat(events.get(i).getJSONObject("record").getInt("idx")).isEqualTo(i);

    final JSONObject stats = events.getLast().getJSONObject("stats");
    assertThat(stats.getInt("returned")).isEqualTo(ROW_COUNT);
    assertThat(stats.getBoolean("truncated")).isFalse();
  }

  @Test
  void postCommandStreamsEveryRowFollowedByATrailer() throws Exception {
    final List<JSONObject> events = readAllEvents(postStream(
        "SELECT idx, name FROM " + TYPE_NAME + " ORDER BY idx", "command"));

    assertThat(events).hasSize(ROW_COUNT + 1);
    assertThat(events.getLast().getJSONObject("stats").getInt("returned")).isEqualTo(ROW_COUNT);
  }

  @Test
  void getQueryStreamsEveryRowFollowedByATrailer() throws Exception {
    final List<JSONObject> events = readAllEvents(getStream(
        "SELECT idx, name FROM " + TYPE_NAME + " ORDER BY idx", null));

    assertThat(events).hasSize(ROW_COUNT + 1);
    assertThat(events.getLast().getJSONObject("stats").getInt("returned")).isEqualTo(ROW_COUNT);
  }

  /**
   * The streamed rows must be the buffered rows: a caller that switches encoding to bound its memory must not
   * have to change how it reads a row.
   */
  @Test
  void aStreamedRowIsTheSameObjectTheBufferedResponseWouldHaveCarried() throws Exception {
    final String command = "SELECT idx, name FROM " + TYPE_NAME + " ORDER BY idx";

    final JSONArray buffered = postBuffered(command).getJSONArray("result");
    final List<JSONObject> streamed = readAllEvents(postStream(command, "query"));

    assertThat(streamed).hasSize(buffered.length() + 1);
    for (int i = 0; i < buffered.length(); i++)
      assertThat(streamed.get(i).getJSONObject("record").toString())
          .isEqualTo(buffered.getJSONObject(i).toString());
  }

  /**
   * The regression the issue explicitly asks for: a request that does not negotiate the stream must receive the
   * body it always received. Pinned as the exact top-level shape, so a stray field added to the buffered
   * envelope fails here rather than in somebody's client.
   */
  @Test
  void theBufferedResponseShapeIsUnchanged() throws Exception {
    final JSONObject buffered = postBuffered("SELECT idx, name FROM " + TYPE_NAME + " ORDER BY idx");

    assertThat(buffered.keySet())
        .containsExactlyInAnyOrder("user", "result", "limit", "returned", "truncated");
    assertThat(buffered.getJSONArray("result").length()).isEqualTo(ROW_COUNT);
    assertThat(buffered.getInt("returned")).isEqualTo(ROW_COUNT);
    assertThat(buffered.getBoolean("truncated")).isFalse();

    final JSONObject firstRow = buffered.getJSONArray("result").getJSONObject(0);
    assertThat(firstRow.getInt("idx")).isEqualTo(0);
    assertThat(firstRow.getString("name")).isEqualTo("row-0");
  }

  /**
   * An {@code Accept} naming anything else - including a type the server does know, and including no header at
   * all - keeps the buffered encoding. Negotiation that leaked would break every existing client.
   */
  @Test
  void anUnrelatedAcceptHeaderStillGetsTheBufferedBody() throws Exception {
    final HttpResponse<String> response = send(postRequest(
        "SELECT idx FROM " + TYPE_NAME, "query", "text/plain, application/json"));

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Content-Type").orElse("")).contains("application/json");
    assertThat(new JSONObject(response.body()).getJSONArray("result").length()).isEqualTo(ROW_COUNT);
  }

  @Test
  void truncationIsReportedInTheTrailer() throws Exception {
    final List<JSONObject> events = readAllEvents(postStream(
        "SELECT idx FROM " + TYPE_NAME + " ORDER BY idx", "query", 3, null));

    assertThat(events).hasSize(4);
    final JSONObject stats = events.getLast().getJSONObject("stats");
    assertThat(stats.getInt("limit")).isEqualTo(3);
    assertThat(stats.getInt("returned")).isEqualTo(3);
    assertThat(stats.getBoolean("truncated")).isTrue();
  }

  /**
   * The two aggregating serializers have no row stream to give: they accumulate the whole result into one
   * document. Refusing with a 400 that names the alternative is the honest answer - emitting the aggregate as a
   * single line would stream nothing while claiming to.
   */
  @Test
  void theGraphAndStudioSerializersAreRefusedForStreaming() throws Exception {
    for (final String serializer : List.of("graph", "studio")) {
      final HttpResponse<String> response = send(postRequest(
          "SELECT FROM " + TYPE_NAME, "query", NDJSON, null, serializer));

      assertThat(response.statusCode()).as("serializer %s", serializer).isEqualTo(400);
      assertThat(response.body()).contains("has no row stream");
    }
  }

  /**
   * The envelope properties the stream has no room for are refused rather than silently dropped. A caller that
   * asked for the plan and got a plain row stream would have to notice the absence to find out.
   */
  @Test
  void aRequestForTheEnvelopePropertiesIsRefusedRatherThanSilentlyStripped() throws Exception {
    final HttpRequest profiled = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl() + "/command/" + getDatabaseName()))
        .timeout(STREAM_OPEN_TIMEOUT)
        .header("Authorization", authorization())
        .header("Content-Type", "application/json")
        .header("Accept", NDJSON)
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
            .put("language", "sql")
            .put("command", "SELECT idx FROM " + TYPE_NAME)
            .put("profileExecution", "detailed").toString()))
        .build();

    final HttpResponse<String> response = send(profiled);
    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("'profileExecution'");

    final HttpResponse<String> explained = send(postRequest("EXPLAIN SELECT idx FROM " + TYPE_NAME, "command", NDJSON));
    assertThat(explained.statusCode()).isEqualTo(400);
    assertThat(explained.body()).contains("EXPLAIN produces a plan");
  }

  /**
   * The hard ceiling has to be decided before the first byte, because a 413 cannot be sent once a 200 is on the
   * wire. A caller stating a cap above the ceiling therefore gets the same refusal the buffered encoding gives
   * it, and gets it with an intact JSON error body rather than half a stream.
   */
  @Test
  void theResultCeilingIsRefusedBeforeAnyByteIsStreamed() throws Exception {
    final int ceiling = 5;
    getServer(0).getConfiguration()
        .setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, ceiling);
    try {
      final HttpResponse<String> response = send(postRequest(
          "SELECT idx FROM " + TYPE_NAME, "query", NDJSON, ceiling + 100, null));

      assertThat(response.statusCode()).isEqualTo(413);
      assertThat(response.headers().firstValue("Content-Type").orElse("")).contains("application/json");
      final JSONObject error = new JSONObject(response.body());
      assertThat(error.getString("error")).contains("Result set too large");
      assertThat(error.getString("detail")).contains("maximum of " + ceiling + " rows");
    } finally {
      getServer(0).getConfiguration()
          .setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS,
              GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getDefValue());
    }
  }

  // ───────────────────────────── plumbing ─────────────────────────────

  private void registerGate() throws Exception {
    getServerDatabase(0, getDatabaseName()).getSchema()
        .registerFunctionLibrary(new JavaClassFunctionLibraryDefinition(GATE_LIBRARY, StreamGate.class));
  }

  private void unregisterGate() {
    try {
      getServerDatabase(0, getDatabaseName()).getSchema().unregisterFunctionLibrary(GATE_LIBRARY);
    } catch (final Exception ignored) {
      // The library may already be gone if the test failed before registering it; nothing to undo.
    }
  }

  /**
   * The gated query carries no ORDER BY on purpose. A sort has to drain the whole result set before it can emit
   * anything, so the engine would park inside the gate before the first row could leave and the test would be
   * measuring the sort rather than the encoding. The type is created with a single bucket, so the rows come back
   * in insertion order without one.
   */
  private String gatedQuery() {
    return "SELECT idx, `" + GATE_LIBRARY + ".hold`() AS gate FROM " + TYPE_NAME;
  }

  private HttpResponse<InputStream> openGatedPostStream() throws Exception {
    return postStream(gatedQuery(), "query");
  }

  private HttpResponse<InputStream> openGatedGetStream() throws Exception {
    return getStream(gatedQuery(), null);
  }

  private String baseUrl() {
    return "http://localhost:" + getServer(0).getHttpServer().getPort() + "/api/v1";
  }

  private static String authorization() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private HttpRequest postRequest(final String command, final String operation, final String accept) {
    return postRequest(command, operation, accept, null, null);
  }

  private HttpRequest postRequest(final String command, final String operation, final String accept,
      final Integer limit, final String serializer) {
    final JSONObject payload = new JSONObject().put("language", "sql").put("command", command);
    if (limit != null)
      payload.put("limit", limit);
    if (serializer != null)
      payload.put("serializer", serializer);

    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl() + "/" + operation + "/" + getDatabaseName()))
        .timeout(STREAM_OPEN_TIMEOUT)
        .header("Authorization", authorization())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()));
    if (accept != null)
      builder.header("Accept", accept);
    return builder.build();
  }

  private HttpResponse<InputStream> postStream(final String command, final String operation) throws Exception {
    return postStream(command, operation, null, null);
  }

  private HttpResponse<InputStream> postStream(final String command, final String operation, final Integer limit,
      final String serializer) throws Exception {
    return newClient().send(postRequest(command, operation, NDJSON, limit, serializer),
        HttpResponse.BodyHandlers.ofInputStream());
  }

  private HttpResponse<InputStream> getStream(final String command, final Integer limit) throws Exception {
    // URLEncoder targets application/x-www-form-urlencoded, where a space is '+'. This is a path segment,
    // where '+' is a literal plus and the space has to be %20 - otherwise the server parses a different query
    // and answers 400.
    String url = baseUrl() + "/query/" + getDatabaseName() + "/sql/"
        + URLEncoder.encode(command, StandardCharsets.UTF_8).replace("+", "%20");
    if (limit != null)
      url += "?limit=" + limit;
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(STREAM_OPEN_TIMEOUT)
        .header("Authorization", authorization())
        .header("Accept", NDJSON)
        .GET()
        .build();
    return newClient().send(request, HttpResponse.BodyHandlers.ofInputStream());
  }

  private JSONObject postBuffered(final String command) throws Exception {
    final HttpResponse<String> response = send(postRequest(command, "query", null));
    assertThat(response.statusCode()).isEqualTo(200);
    return new JSONObject(response.body());
  }

  private HttpResponse<String> send(final HttpRequest request) throws Exception {
    return newClient().send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static HttpClient newClient() {
    return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
  }

  private List<JSONObject> readAllEvents(final HttpResponse<InputStream> response) throws Exception {
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Content-Type").orElse("")).contains(NDJSON);
    try (final BufferedReader reader = new BufferedReader(
        new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
      return readAll(reader);
    }
  }

  private static List<JSONObject> readAll(final BufferedReader reader) throws Exception {
    final List<JSONObject> events = new ArrayList<>();
    String line;
    while ((line = reader.readLine()) != null)
      if (!line.isBlank())
        events.add(new JSONObject(line));
    return events;
  }
}
