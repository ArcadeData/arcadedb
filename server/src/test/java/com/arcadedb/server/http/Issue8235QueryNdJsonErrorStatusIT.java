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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8235: a query streamed as NDJSON that fails after its 200 went out reported only
 * {@code {"error":{"message":...}}} - no status, no exception class, no {@code exceptionArgs} - so a client could not
 * tell a retryable conflict (503 on the buffered encoding) from a security refusal (403) or a server fault (500)
 * without parsing the message text.
 * <p>
 * Every test fails the query on its SECOND row, so the first row has already been written and flushed - the 200 is
 * on the wire and the failure can only be reported in band. The in-band line must carry the status the buffered
 * encoding answers the same failure with, decided by the same classifier.
 */
public class Issue8235QueryNdJsonErrorStatusIT extends BaseGraphServerTest {
  private static final String TYPE_NAME = "Stream8235";
  private static final String NDJSON    = "application/x-ndjson";
  private static final String FUNCTION  = "fail8235";

  // Shared by every test of the class and reset in registerFunction(): sound only because the methods of one class run
  // sequentially against one server. Enabling parallel execution for this class would need per-test state.
  private static volatile Supplier<RuntimeException> failure;
  private static final    AtomicInteger              calls = new AtomicInteger();

  /**
   * Lets the first row through, then throws whatever the test installed for every later one. A SQL function rather
   * than a Java function library on purpose: a library call wraps whatever its method throws in a
   * {@code FunctionExecutionException}, which the buffered encoding answers 500 for exactly as this one does, so the
   * fine-grained statuses would be unreachable. A SQL function's exception reaches the result set unwrapped, which is
   * how an engine failure raised while iterating does.
   */
  private static final class FailOnSecondRow extends SQLFunctionAbstract {
    FailOnSecondRow() {
      super(FUNCTION);
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
        final Object[] params, final CommandContext context) {
      final int call = calls.incrementAndGet();
      if (call > 1)
        throw failure.get();
      return call;
    }

    @Override
    public String getSyntax() {
      return FUNCTION + "()";
    }
  }

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database db = getDatabase(0);
    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME + " BUCKETS 1");
      for (int i = 0; i < 5; i++)
        db.newDocument(TYPE_NAME).set("idx", i).save();
    });
  }

  @BeforeEach
  void registerFunction() {
    calls.set(0);
    sqlEngine().getFunctionFactory().register(new FailOnSecondRow());
  }

  @AfterEach
  void unregisterFunction() {
    sqlEngine().getFunctionFactory().unregister(FUNCTION);
  }

  private SQLQueryEngine sqlEngine() {
    return (SQLQueryEngine) getServerDatabase(0, getDatabaseName()).getQueryEngine("sql");
  }

  @Test
  void aRetryableConflictIsReportedAs503OnPostQuery() throws Exception {
    failure = () -> new ConcurrentModificationException("conflict while streaming");
    final JSONObject error = streamedError(postStream("query"));
    assertThat(error.getInt("status")).as("error line: %s", error).isEqualTo(503);
    assertThat(error.getString("exception")).isEqualTo(ConcurrentModificationException.class.getName());
    assertThat(error.has("exceptionArgs")).isFalse();
    // The message every existing consumer reads is unchanged
    assertThat(error.getString("message")).contains("conflict while streaming");
  }

  @Test
  void aSecurityRefusalIsReportedAs403OnGetQuery() throws Exception {
    failure = () -> new SecurityException("not allowed to read this row");
    final JSONObject error = streamedError(getStream());
    assertThat(error.getInt("status")).as("error line: %s", error).isEqualTo(403);
    assertThat(error.getString("exception")).isEqualTo(SecurityException.class.getName());
  }

  @Test
  void aDuplicatedKeyCarriesItsExceptionArgsOnPostCommand() throws Exception {
    failure = () -> new DuplicatedKeyException("Stream8235[idx]", "[1]", new RID(3, 0));
    final JSONObject error = streamedError(postStream("command"));
    assertThat(error.getInt("status")).as("error line: %s", error).isEqualTo(409);
    assertThat(error.getString("exception")).isEqualTo(DuplicatedKeyException.class.getName());
    assertThat(error.getString("exceptionArgs")).startsWith("Stream8235[idx]|");
    assertThat(error.getString("exceptionArgs")).endsWith("|#3:0");
  }

  @Test
  void anUnexpectedFailureIsReportedAs500() throws Exception {
    failure = () -> new IllegalStateException("engine fault");
    final JSONObject error = streamedError(postStream("query"));
    assertThat(error.getInt("status")).as("error line: %s", error).isEqualTo(500);
    assertThat(error.has("exception")).isTrue();
  }

  /**
   * The ceiling refusal is the other in-band error of this encoding: what the buffered path answers 413 for. It goes
   * through the same classifier, so it says 413 too.
   */
  @Test
  void aCeilingThatTruncatesIsReportedAs413() throws Exception {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, 2);
    try {
      final List<JSONObject> events = readAllEvents(send(postRequest("SELECT idx FROM " + TYPE_NAME, "query", 100)));
      assertThat(events.stream().anyMatch(e -> e.has("stats"))).isFalse();
      final JSONObject error = events.getLast().getJSONObject("error");
      assertThat(error.getInt("status")).as("error line: %s", error).isEqualTo(413);
      assertThat(error.getString("exception")).isEqualTo(ResultSetTooLargeException.class.getName());
      assertThat(error.getString("message")).contains("maximum of 2 rows");
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS,
          GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getDefValue());
    }
  }

  /** Requires the 200, at least one row before the failure, and the error as the terminal line with no trailer. */
  private JSONObject streamedError(final HttpResponse<InputStream> response) throws Exception {
    final List<JSONObject> events = readAllEvents(response);
    assertThat(events.getFirst().has("record")).as("the failure must come after a row was already sent").isTrue();
    assertThat(events.stream().anyMatch(e -> e.has("stats"))).isFalse();
    assertThat(events.getLast().has("error")).as("terminal line: %s", events.getLast()).isTrue();
    return events.getLast().getJSONObject("error");
  }

  private String failingQuery() {
    return "SELECT idx, " + FUNCTION + "() AS f FROM " + TYPE_NAME;
  }

  private String baseUrl() {
    return "http://localhost:" + getServer(0).getHttpServer().getPort() + "/api/v1";
  }

  private static String authorization() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private HttpRequest postRequest(final String command, final String operation, final Integer limit) {
    final JSONObject payload = new JSONObject().put("language", "sql").put("command", command);
    if (limit != null)
      payload.put("limit", limit);
    return HttpRequest.newBuilder()
        .uri(URI.create(baseUrl() + "/" + operation + "/" + getDatabaseName()))
        .timeout(Duration.ofSeconds(60))
        .header("Authorization", authorization())
        .header("Content-Type", "application/json")
        .header("Accept", NDJSON)
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
  }

  private HttpResponse<InputStream> postStream(final String operation) throws Exception {
    return send(postRequest(failingQuery(), operation, null));
  }

  private HttpResponse<InputStream> getStream() throws Exception {
    final String url = baseUrl() + "/query/" + getDatabaseName() + "/sql/"
        + URLEncoder.encode(failingQuery(), StandardCharsets.UTF_8).replace("+", "%20");
    return send(HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(60))
        .header("Authorization", authorization())
        .header("Accept", NDJSON)
        .GET()
        .build());
  }

  private static HttpResponse<InputStream> send(final HttpRequest request) throws Exception {
    return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build()
        .send(request, HttpResponse.BodyHandlers.ofInputStream());
  }

  private static List<JSONObject> readAllEvents(final HttpResponse<InputStream> response) throws Exception {
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Content-Type").orElse("")).contains(NDJSON);
    final List<JSONObject> events = new ArrayList<>();
    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null)
        if (!line.isBlank())
          events.add(new JSONObject(line));
    }
    return events;
  }
}
