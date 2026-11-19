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

import com.arcadedb.engine.timeseries.TimeSeriesWalkCoarsenedException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.QueryAdmissionException;
import com.arcadedb.exception.QueryHeapBudgetExceededException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.RetryLaterException;
import com.arcadedb.server.http.handler.openapi.CoreApiSpec;
import com.arcadedb.server.security.ServerSecurityUser;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static com.arcadedb.server.http.HttpSessionManager.ARCADEDB_SESSION_ID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #12440 and #12441: a commit that loses a race with a concurrent commit is answered 503 with the retryable
 * {@link ConcurrentModificationException}, and the answer carried no {@code Retry-After}, unlike the other retryable refusals that
 * say how long to wait. The status stays 503, the one every client reads as "retry the request as it is" (the Java client never
 * retries a 409), and every retryable 503 now carries a {@code Retry-After}: 0 for a refusal that clears by itself, a conflict
 * whose competing commit has already landed, and 1 for a server too busy to start the request.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12440CommitConflictRetryAfterTest extends BaseGraphServerTest {
  private final HttpClient client = HttpClient.newHttpClient();

  /** The reproduction of the reports: the transaction that read the record first commits second, at REPEATABLE_READ. */
  @Test
  void aCommitThatLosesARaceIs503WithRetryAfter() throws Exception {
    command(null, "CREATE DOCUMENT TYPE T");
    command(null, "INSERT INTO T SET id = 1, v = 0");

    final String first = begin("{\"isolationLevel\":\"REPEATABLE_READ\"}");
    final String second = begin(null);

    assertThat(command(first, "SELECT v FROM T WHERE id = 1").statusCode()).isEqualTo(200);
    assertThat(command(second, "UPDATE T SET v = 42 WHERE id = 1").statusCode()).isEqualTo(200);
    assertThat(commit(second).statusCode()).isEqualTo(204);
    assertThat(command(first, "UPDATE T SET v = 5 WHERE id = 1").statusCode()).isEqualTo(200);

    final HttpResponse<String> lost = commit(first);
    assertThat(lost.statusCode()).isEqualTo(503);
    assertThat(lost.headers().firstValue("Retry-After")).as("the back-off a client reads from the status line's headers").hasValue("0");
    final JSONObject body = new JSONObject(lost.body());
    assertThat(body.getString("exception")).isEqualTo(ConcurrentModificationException.class.getName());

    // the conflict cleared by itself: the losing write, re-issued at once in a new transaction, commits
    final String retry = begin(null);
    assertThat(command(retry, "UPDATE T SET v = 5 WHERE id = 1").statusCode()).isEqualTo(200);
    assertThat(commit(retry).statusCode()).isEqualTo(204);
    final HttpResponse<String> read = command(null, "SELECT v FROM T WHERE id = 1");
    assertThat(new JSONObject(read.body()).getJSONArray("result").getJSONObject(0).getInt("v")).isEqualTo(5);
  }

  /** Every retryable 503 says how long to wait, and a RetryLaterException keeps the wait it names. */
  @Test
  void everyRetryable503CarriesRetryAfter() {
    final ClassifyingHandler handler = new ClassifyingHandler(getServer(0).getHttpServer());

    assertRetryable(handler.classifyError(new ConcurrentModificationException("conflict")), "0");
    // the same conflict as the engine raises it at commit time, under the transaction wrapper
    assertRetryable(handler.classifyError(new TransactionException("Error on commit", new ConcurrentModificationException("x"))), "0");
    assertRetryable(handler.classifyError(new LockTimeoutException("lock")), "0");
    assertRetryable(handler.classifyError(new TimeSeriesWalkCoarsenedException("coarsened")), "0");
    // a server too busy to start the request: back off
    assertRetryable(handler.classifyError(new QueryHeapBudgetExceededException("heap")), "1");
    assertRetryable(handler.classifyError(new QueryAdmissionException("queue")), "1");
    assertRetryable(handler.classifyError(new RetryLaterException("snapshot", 7)), "7");
  }

  /** The published contract of the commit route lists the answer (issue #12440), with the header it carries. */
  @Test
  void theCommitRouteDocumentsTheConflict() {
    final OpenAPI openAPI = new OpenAPI();
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new CoreApiSpec().contribute(openAPI);

    final ApiResponse conflict = openAPI.getPaths().get("/api/v1/commit/{database}").getPost().getResponses().get("503");
    assertThat(conflict).isNotNull();
    assertThat(conflict.getDescription()).contains("ConcurrentModificationException");
    assertThat(conflict.getHeaders()).containsKey("Retry-After");
  }

  private static void assertRetryable(final AbstractServerHttpHandler.ErrorClassification classification, final String retryAfter) {
    assertThat(classification.status()).as(classification.reported().getClass().getSimpleName()).isEqualTo(503);
    assertThat(classification.retryAfter()).as(classification.reported().getClass().getSimpleName()).isEqualTo(retryAfter);
  }

  private String begin(final String body) throws Exception {
    final HttpResponse<String> response = send("/api/v1/begin/" + getDatabaseName(), null, body);
    assertThat(response.statusCode()).isEqualTo(204);
    return response.headers().firstValue(ARCADEDB_SESSION_ID).orElseThrow().trim();
  }

  private HttpResponse<String> commit(final String session) throws Exception {
    return send("/api/v1/commit/" + getDatabaseName(), session, null);
  }

  private HttpResponse<String> command(final String session, final String sql) throws Exception {
    return send("/api/v1/command/" + getDatabaseName(), session,
        new JSONObject().put("language", "sql").put("command", sql).toString());
  }

  private HttpResponse<String> send(final String path, final String session, final String body) throws Exception {
    final HttpRequest.Builder request = HttpRequest.newBuilder(new URI(getServerHttpUrl(path)))
        .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(
            StandardCharsets.UTF_8)))
        .POST(body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
    if (body != null)
      request.header("Content-Type", "application/json");
    if (session != null)
      request.header(ARCADEDB_SESSION_ID, session);
    return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  private static final class ClassifyingHandler extends AbstractServerHttpHandler {
    private ClassifyingHandler(final HttpServer httpServer) {
      super(httpServer);
    }

    @Override
    protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
      throw new UnsupportedOperationException();
    }
  }
}
