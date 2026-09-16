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

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7575: {@code GET /query} had no {@code ExplainResultSet} branch in either arm, so it answered an
 * {@code EXPLAIN} in two ways neither POST operation ever would.
 * <ul>
 * <li>Streamed, it wrote the single plan row as an {@code NdJsonQueryEvent} {@code record} - a line no consumer
 * of that schema knows how to read - where {@code POST /command} refuses the request with a 400 before any byte
 * leaves.</li>
 * <li>Buffered, it serialized the plan row as if it were a record, so the {@code explain} and
 * {@code explainPlan} envelope properties the POST operations produce were unreachable from this operation
 * entirely.</li>
 * </ul>
 * The three operations now share {@code AbstractQueryHandler.requireStreamableResultSet},
 * {@code drainExplainResultSet} and {@code reportExplainPlan}, so every assertion below is written as a
 * comparison between GET and its POST twin rather than as a bare check: "all three answer EXPLAIN the same way"
 * is the property, and a test that only asserted GET's new shape would still pass if POST later diverged.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7575">issue #7575</a>
 */
public class Issue7575ExplainContractIsTheSameOnEveryQueryOperationIT extends BaseGraphServerTest {
  private static final String   TYPE_NAME = "Explain7575";
  private static final String   NDJSON    = "application/x-ndjson";
  private static final String   JSON      = "application/json";
  private static final Duration TIMEOUT   = Duration.ofSeconds(30);

  @Override
  protected void populateDatabase() {
    super.populateDatabase();

    final Database db = getDatabase(0);
    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME + " BUCKETS 1");
      db.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".idx INTEGER");
      for (int i = 0; i < 3; i++)
        db.newDocument(TYPE_NAME).set("idx", i).save();
    });
  }

  private String explain() {
    return "EXPLAIN SELECT idx FROM " + TYPE_NAME;
  }

  /**
   * The divergence from the issue. GET used to answer 200 with one ndjson line carrying the plan; it now gives
   * the refusal POST has given since issue #7306, word for word - the two read it from one constant.
   */
  @Test
  void aStreamedExplainIsRefusedByEveryOperationWithTheSameMessage() throws Exception {
    final HttpResponse<String> viaGet = send(getRequest(explain(), NDJSON));
    final HttpResponse<String> viaPostQuery = send(postRequest(explain(), "query", NDJSON));
    final HttpResponse<String> viaPostCommand = send(postRequest(explain(), "command", NDJSON));

    for (final HttpResponse<String> response : List.of(viaGet, viaPostQuery, viaPostCommand)) {
      assertThat(response.statusCode())
          .as("a plan is not a row stream, answered %s", response.body())
          .isEqualTo(400);
      assertThat(response.body())
          .contains("EXPLAIN produces a plan, not a row stream")
          .as("the refusal has to name the encoding that does work, or it is a dead end")
          .contains("application/json");
    }

    assertThat(errorOf(viaGet))
        .as("the three read the message from one constant, so it cannot diverge between them")
        .isEqualTo(errorOf(viaPostQuery))
        .isEqualTo(errorOf(viaPostCommand));
  }

  /**
   * The refusal happens before a byte of the response body is written, which is the whole reason it exists: once
   * a 200 and the first row are on the wire the status cannot be taken back. A body that parsed as ndjson would
   * mean the check ran too late.
   */
  @Test
  void theStreamedRefusalIsAStatusCodeRatherThanAnInBandError() throws Exception {
    final HttpResponse<String> response = send(getRequest(explain(), NDJSON));

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.headers().firstValue("Content-Type").orElse(""))
        .as("the answer is an error body, not a started stream")
        .doesNotContain(NDJSON);
    assertThat(response.body())
        .as("the plan row must not have reached the client at all")
        .doesNotContain("executionPlanAsString");
  }

  /**
   * The buffered half, which the issue asked to settle in the same pass. GET used to serialize the plan row into
   * {@code result} and produce no envelope at all; it now produces exactly what the POST operations produce.
   */
  @Test
  void aBufferedExplainCarriesThePlanInTheEnvelopeOnEveryOperation() throws Exception {
    final JSONObject viaGet = new JSONObject(send(getRequest(explain(), JSON)).body());
    final JSONObject viaPostQuery = new JSONObject(send(postRequest(explain(), "query", JSON)).body());
    final JSONObject viaPostCommand = new JSONObject(send(postRequest(explain(), "command", JSON)).body());

    for (final JSONObject response : List.of(viaGet, viaPostQuery, viaPostCommand)) {
      assertThat(response.has("explain"))
          .as("the plan as text belongs in the envelope, response was %s", response)
          .isTrue();
      assertThat(response.getString("explain"))
          .as("and it has to be the plan, not an empty string")
          .isNotBlank();
      assertThat(response.has("explainPlan"))
          .as("and the structured form beside it, for a caller that reads the steps")
          .isTrue();
      assertThat(response.getJSONArray("result"))
          .as("the single plan row is drained: the plan travels in the envelope, not also as a row")
          .isEmpty();
    }

    assertThat(viaGet.getString("explain"))
        .as("the three now run the same three methods on AbstractQueryHandler, so the plan text must match")
        .isEqualTo(viaPostQuery.getString("explain"))
        .isEqualTo(viaPostCommand.getString("explain"));
  }

  /**
   * The row-count trailer is reported on the EXPLAIN branch too, so a client reading {@code returned} does not
   * have to special-case the one statement whose answer is not rows. This is the member the GET branch would
   * most easily have left out, since its buffered arm reports limits outside the branch.
   */
  @Test
  void aBufferedExplainStillReportsTheRowAccounting() throws Exception {
    final JSONObject viaGet = new JSONObject(send(getRequest(explain(), JSON)).body());

    assertThat(viaGet.has("returned")).as("response was %s", viaGet).isTrue();
    assertThat(viaGet.getInt("returned")).as("the plan row is drained, so no row was serialized").isZero();
    assertThat(viaGet.getBoolean("truncated")).isFalse();
  }

  /**
   * The branch is chosen by the RESULT SET, not by the command text, so a nested spelling is caught too. Pinned
   * because matching the text is the cheap alternative a reviewer would reach for, and it would pass a bare
   * "EXPLAIN SELECT" test while letting this one through.
   */
  @Test
  void theBranchIsChosenByTheResultSetSoACasedOrPaddedSpellingIsCaughtToo() throws Exception {
    for (final String spelling : List.of("explain SELECT idx FROM " + TYPE_NAME,
        "  ExPlAiN   SELECT idx FROM " + TYPE_NAME)) {
      final HttpResponse<String> streamed = send(getRequest(spelling, NDJSON));
      assertThat(streamed.statusCode()).as("spelling '%s' answered %s", spelling, streamed.body()).isEqualTo(400);
      assertThat(streamed.body()).contains("EXPLAIN produces a plan");

      final JSONObject buffered = new JSONObject(send(getRequest(spelling, JSON)).body());
      assertThat(buffered.has("explain")).as("spelling '%s' buffered: %s", spelling, buffered).isTrue();
    }
  }

  /** Nothing changes for a statement that is not an EXPLAIN: both encodings still answer rows. */
  @Test
  void anOrdinaryQueryIsUntouchedInBothEncodings() throws Exception {
    final String select = "SELECT idx FROM " + TYPE_NAME;

    final HttpResponse<String> streamed = send(getRequest(select, NDJSON));
    assertThat(streamed.statusCode()).as("answered %s", streamed.body()).isEqualTo(200);
    assertThat(streamed.headers().firstValue("Content-Type").orElse("")).contains(NDJSON);

    final JSONObject buffered = new JSONObject(send(getRequest(select, JSON)).body());
    assertThat(buffered.getJSONArray("result")).hasSize(3);
    assertThat(buffered.has("explain"))
        .as("the envelope properties belong to EXPLAIN and to a profiled run, not to every query")
        .isFalse();
  }

  private static String errorOf(final HttpResponse<String> response) {
    return new JSONObject(response.body()).getString("error");
  }

  private String baseUrl() {
    return "http://localhost:" + getServer(0).getHttpServer().getPort() + "/api/v1";
  }

  private static String authorization() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private HttpRequest getRequest(final String command, final String accept) {
    // URLEncoder targets application/x-www-form-urlencoded, where a space is '+'. This is a path segment, where
    // '+' is a literal plus and the space has to be %20 - otherwise the server parses a different statement.
    final String url = baseUrl() + "/query/" + getDatabaseName() + "/sql/"
        + URLEncoder.encode(command, StandardCharsets.UTF_8).replace("+", "%20");
    return HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(TIMEOUT)
        .header("Authorization", authorization())
        .header("Accept", accept)
        .GET()
        .build();
  }

  private HttpRequest postRequest(final String command, final String operation, final String accept) {
    return HttpRequest.newBuilder()
        .uri(URI.create(baseUrl() + "/" + operation + "/" + getDatabaseName()))
        .timeout(TIMEOUT)
        .header("Authorization", authorization())
        .header("Content-Type", "application/json")
        .header("Accept", accept)
        .POST(HttpRequest.BodyPublishers.ofString(
            new JSONObject().put("language", "sql").put("command", command).toString()))
        .build();
  }

  private HttpResponse<String> send(final HttpRequest request) throws Exception {
    return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build()
        .send(request, HttpResponse.BodyHandlers.ofString());
  }
}
