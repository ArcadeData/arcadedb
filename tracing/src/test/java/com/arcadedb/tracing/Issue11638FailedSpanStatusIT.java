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
package com.arcadedb.tracing;

import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.server.BaseGraphServerTest;
import io.micrometer.observation.ObservationRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #11638: a failed HTTP request and a failed query were exported as spans with no error status and no exception event,
 * indistinguishable from a success by every tracing backend. A query span that failed now carries the ERROR status and the
 * exception; the HTTP span does for a 5xx answer, and not for a 4xx, the client's mistake (OpenTelemetry HTTP server spans).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11638FailedSpanStatusIT extends BaseGraphServerTest {
  private static final String FAILING_FUNCTION = "issue11638Fails";
  private static final String HTTP_SPAN        = "arcadedb.http.server.requests";
  private static final String QUERY_SPAN       = "arcadedb.query";

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Test
  void aSuccessfulCommandFailsNeitherSpan() throws Exception {
    // the control: without it the assertions below could pass on spans that are always ERROR
    final SpanData[] spans = runTraced("SELECT 1 AS one", 200);
    assertThat(spans[0].getStatus().getStatusCode()).isNotEqualTo(StatusCode.ERROR);
    assertThat(spans[1].getStatus().getStatusCode()).isNotEqualTo(StatusCode.ERROR);
    assertThat(exceptionEvents(spans[0])).isEmpty();
    assertThat(exceptionEvents(spans[1])).isEmpty();
  }

  @Test
  void aServerErrorFailsTheQuerySpanAndTheHttpSpan() throws Exception {
    // an INSERT runs before the command returns: a failure raised while a caller iterates a lazy result set is past the span
    getServerDatabase(0, getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE Issue11638");
    final SQLQueryEngine sql = (SQLQueryEngine) getServer(0).getDatabase(getDatabaseName()).getQueryEngine("sql");
    sql.getFunctionFactory().register(new SQLFunctionAbstract(FAILING_FUNCTION) {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        throw new IllegalStateException("issue 11638 internal failure");
      }

      @Override
      public String getSyntax() {
        return FAILING_FUNCTION + "()";
      }
    });
    try {
      final SpanData[] spans = runTraced("INSERT INTO Issue11638 SET v = " + FAILING_FUNCTION + "()", 500);

      assertThat(spans[0].getStatus().getStatusCode()).as("the query span of a failed query").isEqualTo(StatusCode.ERROR);
      assertThat(exceptionEvents(spans[0])).as("the exception of the failed query").isNotEmpty();
      assertThat(spans[1].getStatus().getStatusCode()).as("the HTTP span of a 500 answer").isEqualTo(StatusCode.ERROR);
      assertThat(exceptionEvents(spans[1])).as("the exception that reached the request boundary").isNotEmpty();
    } finally {
      sql.getFunctionFactory().unregister(FAILING_FUNCTION);
    }
  }

  @Test
  void aClientErrorFailsTheQuerySpanButNotTheHttpSpan() throws Exception {
    final SpanData[] spans = runTraced("SELEC 1", 400);

    assertThat(spans[0].getStatus().getStatusCode()).as("the query span of a query that could not be parsed")
        .isEqualTo(StatusCode.ERROR);
    final List<EventData> events = exceptionEvents(spans[0]);
    assertThat(events).isNotEmpty();
    assertThat(events.getFirst().getAttributes().get(AttributeKey.stringKey("exception.type"))).contains("Parsing");
    assertThat(spans[1].getStatus().getStatusCode()).as("the HTTP span of a 4xx answer, the client's mistake")
        .isNotEqualTo(StatusCode.ERROR);
    assertThat(exceptionEvents(spans[1])).isEmpty();
  }

  /** Runs the SQL command over HTTP with a tracer attached, and returns its query span and its HTTP span. */
  private SpanData[] runTraced(final String command, final int expectedStatus) throws Exception {
    final ObservationRegistry registry = getServer(0).getObservationRegistry();
    final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    final TracingPlugin plugin = new TracingPlugin();
    plugin.attachForTest(registry, exporter);
    try {
      final HttpURLConnection c = (HttpURLConnection) new URL(getServerHttpUrl("/api/v1/command/" + getDatabaseName())).openConnection();
      c.setRequestMethod("POST");
      c.setDoOutput(true);
      c.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      c.setRequestProperty("Content-Type", "application/json");
      c.getOutputStream().write(("{\"language\":\"sql\",\"command\":\"" + command + "\"}").getBytes(StandardCharsets.UTF_8));
      assertThat(c.getResponseCode()).isEqualTo(expectedStatus);
      c.disconnect();

      final SpanData[] found = new SpanData[2];
      // the HTTP span is exported in the handler's finally block, after the client has its answer
      await().atMost(30, TimeUnit.SECONDS).pollInterval(20, TimeUnit.MILLISECONDS).untilAsserted(() -> {
        final List<SpanData> spans = exporter.getFinishedSpanItems();
        final SpanData query = spans.stream()
            // the HTTP command handler appends the result limit to the statement it runs
            .filter(s -> QUERY_SPAN.equals(s.getName()) && String.valueOf(s.getAttributes().get(AttributeKey.stringKey("db.statement")))
                .startsWith(command))
            .findFirst().orElse(null);
        assertThat(query).as("the query span of " + command + " among " + spans.stream()
            .map(s -> s.getName() + "=" + s.getAttributes().get(AttributeKey.stringKey("db.statement"))).toList()).isNotNull();
        final SpanData http = spans.stream().filter(s -> HTTP_SPAN.equals(s.getName()) && s.getTraceId().equals(query.getTraceId()))
            .findFirst().orElse(null);
        assertThat(http).as("the HTTP span enclosing the query span").isNotNull();
        found[0] = query;
        found[1] = http;
      });
      return found;
    } finally {
      plugin.stopService();
    }
  }

  private static List<EventData> exceptionEvents(final SpanData span) {
    return span.getEvents().stream().filter(e -> "exception".equals(e.getName())).toList();
  }
}
