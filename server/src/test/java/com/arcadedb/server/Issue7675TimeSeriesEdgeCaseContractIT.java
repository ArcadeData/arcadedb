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
package com.arcadedb.server;

import com.arcadedb.database.Database;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7675: the two HTTP time-series endpoints used to answer three edge-case request members in ways gRPC
 * never did, and in one case in ways they did not share with each other.
 * <p>
 * The damaging one is {@code POST /ts/{db}/query} with {@code bucketInterval: 0}. The engine reads a
 * non-positive interval as "one bucket over the whole range", so the caller got a {@code 200} carrying a single
 * aggregate - which looks exactly like a legitimate answer to a legitimate question, and is never what the
 * uninitialised variable or the unfilled template that sent the {@code 0} meant. The Grafana endpoint answered
 * the same input by substituting a derived interval, or the literal {@code 60000}, which draws a panel at a
 * resolution nobody asked for. gRPC refused it from the start, and so has the Java client
 * ({@code TimeSeriesQuery.aggregate}).
 * <p>
 * The projection is the wrong-DATA case rather than the wrong-STATUS one: an unresolvable {@code fields} name
 * was dropped, so a typo NARROWED the projection silently, and a projection where nothing resolved collapsed to
 * the empty array the engine reads as "every column" - a typo that WIDENED it.
 */
class Issue7675TimeSeriesEdgeCaseContractIT extends BaseGraphServerTest {

  private static final String TYPE = "contractmetric";

  // ---------------------------------------------------------------------------------------------------------
  // bucketInterval
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void aNonPositiveBucketIntervalIsRefusedOnBothHttpEndpoints() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithSamples(serverIndex);

      for (final long stated : new long[] { 0L, -1L, -60_000L }) {
        final JSONObject refusal = post(serverIndex, "/api/v1/ts/graph/query", aggregationPayload(stated), 400);
        assertThat(refusal.getString("error"))
            .as("/ts/query must name the member rather than answering one bucket over the whole range")
            .contains("aggregation.bucketInterval")
            .contains("positive");

        final JSONObject frame = grafanaTargetResult(serverIndex, grafanaPayload(stated, null));
        assertThat(frame.getString("error"))
            .as("the Grafana endpoint must refuse a STATED non-positive interval, not substitute 60000")
            .contains("aggregation.bucketInterval")
            .contains("positive");
        assertThat(frame.getJSONArray("frames")).as("a refused target carries no frames").isEmpty();
      }
    });
  }

  /**
   * A fractional {@code bucketInterval} reaches the same place: {@code JSONObject.getLong} narrows with
   * {@code Number.longValue()}, so {@code 0.5} truncates to {@code 0} and used to land in the whole-range branch
   * without a word to the caller. The positivity test catches it on the way.
   */
  @Test
  void aFractionalBucketIntervalBelowOneIsRefusedRatherThanTruncatedIntoTheWholeRangeBranch() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithSamples(serverIndex);

      final JSONObject aggregation = new JSONObject();
      aggregation.put("bucketInterval", 0.5d);
      aggregation.put("requests", oneSumRequest());

      final JSONObject payload = new JSONObject();
      payload.put("type", TYPE);
      payload.put("aggregation", aggregation);

      assertThat(post(serverIndex, "/api/v1/ts/graph/query", payload, 400).getString("error"))
          .contains("aggregation.bucketInterval");
    });
  }

  /**
   * The Grafana endpoint's derivation is NOT what changed: an ABSENT {@code bucketInterval} is genuinely
   * optional there and still means "derive one from maxDataPoints, else 60000". Only a value the caller stated
   * is held to the rule.
   */
  @Test
  void anAbsentGrafanaBucketIntervalIsStillDerivedRatherThanRefused() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithSamples(serverIndex);

      final JSONObject noInterval = grafanaPayload(null, 10);
      final JSONObject result = grafanaTargetResult(serverIndex, noInterval);
      assertThat(result.has("error")).as("an omitted bucketInterval is derived, not refused").isFalse();
      assertThat(result.getJSONArray("frames")).isNotEmpty();
    });
  }

  @Test
  void aPositiveBucketIntervalStillAnswersOnBothEndpoints() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithSamples(serverIndex);

      final JSONObject answered = post(serverIndex, "/api/v1/ts/graph/query", aggregationPayload(1_000_000L), 200);
      assertThat(answered.getJSONArray("buckets")).isNotEmpty();

      final JSONObject frame = grafanaTargetResult(serverIndex, grafanaPayload(1_000_000L, null));
      assertThat(frame.has("error")).isFalse();
      assertThat(frame.getJSONArray("frames")).isNotEmpty();
    });
  }

  // ---------------------------------------------------------------------------------------------------------
  // aggregation.requests
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void anEmptyAggregationRequestsArrayIsRefusedOnBothHttpEndpoints() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithSamples(serverIndex);

      final JSONObject aggregation = new JSONObject();
      aggregation.put("bucketInterval", 1_000L);
      aggregation.put("requests", new JSONArray());

      final JSONObject payload = new JSONObject();
      payload.put("type", TYPE);
      payload.put("aggregation", aggregation);

      assertThat(post(serverIndex, "/api/v1/ts/graph/query", payload, 400).getString("error"))
          .as("an empty requests array answered buckets whose 'values' were empty, a shape no client uses")
          .contains("aggregation.requests")
          .contains("at least one");

      final JSONObject target = new JSONObject();
      target.put("refId", "A");
      target.put("type", TYPE);
      target.put("aggregation", aggregation);
      final JSONArray targets = new JSONArray();
      targets.put(target);
      final JSONObject grafana = new JSONObject();
      grafana.put("targets", targets);

      final JSONObject frame = grafanaTargetResult(serverIndex, grafana);
      assertThat(frame.getString("error")).contains("aggregation.requests").contains("at least one");
    });
  }

  // ---------------------------------------------------------------------------------------------------------
  // fields projection
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void aFieldsNameThatMatchesNoColumnIsRefusedOnBothHttpEndpoints() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithSamples(serverIndex);

      final JSONObject payload = new JSONObject();
      payload.put("type", TYPE);
      payload.put("fields", new JSONArray().put("valeu"));

      assertThat(post(serverIndex, "/api/v1/ts/graph/query", payload, 400).getString("error"))
          .as("a typo used to answer 200 with a timestamp-only row")
          .contains("valeu")
          .contains("declared columns");

      final JSONObject target = new JSONObject();
      target.put("refId", "A");
      target.put("type", TYPE);
      target.put("fields", new JSONArray().put("valeu"));
      final JSONArray targets = new JSONArray();
      targets.put(target);
      final JSONObject grafana = new JSONObject();
      grafana.put("targets", targets);

      assertThat(grafanaTargetResult(serverIndex, grafana).getString("error")).contains("valeu");
    });
  }

  /**
   * The partial case: one name resolves and one does not. The projection used to come back NARROWED to the one
   * that did, indistinguishable by the caller from a projection the server answered correctly.
   */
  @Test
  void aPartiallyResolvableFieldsProjectionIsRefusedRatherThanNarrowed() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithSamples(serverIndex);

      final JSONObject payload = new JSONObject();
      payload.put("type", TYPE);
      payload.put("fields", new JSONArray().put("valeu").put("value"));

      assertThat(post(serverIndex, "/api/v1/ts/graph/query", payload, 400).getString("error")).contains("valeu");
    });
  }

  @Test
  void aResolvableFieldsProjectionStillAnswersTheRequestedColumn() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithSamples(serverIndex);

      final JSONObject payload = new JSONObject();
      payload.put("type", TYPE);
      payload.put("fields", new JSONArray().put("value"));

      final JSONObject answered = post(serverIndex, "/api/v1/ts/graph/query", payload, 200);
      assertThat(answered.getJSONArray("columns").toList()).containsExactly("ts", "value");
      assertThat(answered.getInt("count")).isEqualTo(3);
    });
  }

  /**
   * And naming the TIMESTAMP column alongside a field is still a legitimate spelling of the projection - the
   * timestamp is always emitted and always first, so it selects nothing further. Refusing it would have broken
   * every caller that writes the projection out in full.
   */
  @Test
  void namingTheTimestampColumnInTheProjectionIsStillAccepted() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithSamples(serverIndex);

      final JSONObject payload = new JSONObject();
      payload.put("type", TYPE);
      payload.put("fields", new JSONArray().put("ts").put("value"));

      assertThat(post(serverIndex, "/api/v1/ts/graph/query", payload, 200).getJSONArray("columns").toList())
          .containsExactly("ts", "value");
    });
  }

  // ---------------------------------------------------------------------------------------------------------
  // fixtures
  // ---------------------------------------------------------------------------------------------------------

  private static JSONArray oneSumRequest() {
    final JSONObject sum = new JSONObject();
    sum.put("field", "value");
    sum.put("type", "SUM");
    sum.put("alias", "s");
    return new JSONArray().put(sum);
  }

  /** A {@code /ts/query} aggregation payload stating {@code bucketInterval}. */
  private static JSONObject aggregationPayload(final long bucketInterval) {
    final JSONObject aggregation = new JSONObject();
    aggregation.put("bucketInterval", bucketInterval);
    aggregation.put("requests", oneSumRequest());

    final JSONObject payload = new JSONObject();
    payload.put("type", TYPE);
    payload.put("aggregation", aggregation);
    return payload;
  }

  /**
   * A Grafana payload with one target.
   *
   * @param bucketInterval stated when non-null; OMITTED when null, which is the case that still derives
   * @param maxDataPoints  stated when non-null, so the derivation has something to derive from
   */
  private static JSONObject grafanaPayload(final Long bucketInterval, final Integer maxDataPoints) {
    final JSONObject aggregation = new JSONObject();
    if (bucketInterval != null)
      aggregation.put("bucketInterval", bucketInterval.longValue());
    aggregation.put("requests", oneSumRequest());

    final JSONObject target = new JSONObject();
    target.put("refId", "A");
    target.put("type", TYPE);
    target.put("aggregation", aggregation);

    final JSONObject grafana = new JSONObject();
    grafana.put("targets", new JSONArray().put(target));
    grafana.put("from", 0L);
    grafana.put("to", 10_000L);
    if (maxDataPoints != null)
      grafana.put("maxDataPoints", maxDataPoints.intValue());
    return grafana;
  }

  /**
   * The result object filed under {@code refId} "A". The Grafana endpoint answers 200 for the REQUEST and
   * reports a bad TARGET as that target's error frame, so one mistyped panel cannot blank the panels that are
   * fine - which is why the status here is 200 even for the refusals.
   */
  private JSONObject grafanaTargetResult(final int serverIndex, final JSONObject payload) throws Exception {
    return post(serverIndex, "/api/v1/ts/graph/grafana/query", payload, 200)
        .getJSONObject("results").getJSONObject("A");
  }

  private void createTypeWithSamples(final int serverIndex) throws Exception {
    final Database database = getServerDatabase(serverIndex, getDatabaseName());
    if (database.getSchema().existsType(TYPE))
      database.getSchema().dropType(TYPE);

    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(TYPE);
    database.begin();
    tsType.getEngine().appendSamples(new long[] { 1_000L, 2_000L, 3_000L }, new Object[] { 4.0, 1.0, 5.0 });
    database.commit();
  }

  private JSONObject post(final int serverIndex, final String path, final JSONObject body,
      final int expectedStatus) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + path).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body.toString().getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    assertThat(connection.getResponseCode()).isEqualTo(expectedStatus);
    try (final InputStream is = expectedStatus < 400 ? connection.getInputStream() : connection.getErrorStream()) {
      return new JSONObject(new String(is.readAllBytes(), StandardCharsets.UTF_8));
    }
  }
}
