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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The contract of the time-series HTTP surface (time-series query, Grafana, and a non-transactional DELETE or
 * TRUNCATE in an HTTP session) at its edges: field projection, empty and degenerate requests, the aggregation row
 * ceiling and its metrics, and a database that does not exist.
 */
class TimeSeriesHttpContractIT {

  /**
   * Issue #10342: {@code DELETE FROM <timeseries type>} and {@code TRUNCATE TYPE} remove samples outside the transaction they run in,
   * as an {@code INSERT} adds them outside it (issue #7410). Over HTTP every command is wrapped in a transaction and a client can hold a
   * session open, so this is the path on which a caller meets the rule: the answer says {@code transactional: false}, and a
   * {@code ROLLBACK} of the session brings back nothing.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DeleteOverHttp extends BaseGraphServerTest {
    private static final String TYPE = "Ts10342";

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    @Test
    void aDeleteOrATruncateInAnHttpSessionIsNotUndoneByTheRollback() throws Exception {
      sql(null, "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE)");
      for (int i = 0; i < 10; i++)
        sql(null, "INSERT INTO " + TYPE + " SET ts = " + (1_000L + i * 1_000L) + ", host = 'h" + (i % 2) + "', v = " + i);
      assertThat(count()).isEqualTo(10L);

      // a DELETE in a session: the answer says it is not transactional, the samples are gone, and the rollback restores none of them
      final String session = begin();
      final JSONObject deleted = sql(session, "DELETE FROM " + TYPE + " WHERE host = 'h0' AND ts < 6000").getJSONArray("result")
          .getJSONObject(0);
      assertThat(deleted.getLong("count")).isEqualTo(3L);
      assertThat(deleted.getBoolean("transactional")).isFalse();
      rollback(session);
      assertThat(count()).as("the rollback of the session did not bring the samples back").isEqualTo(7L);

      // a TRUNCATE in a session: the same
      final String second = begin();
      final JSONObject truncated = sql(second, "TRUNCATE TYPE " + TYPE).getJSONArray("result").getJSONObject(0);
      assertThat(truncated.getLong("count")).isEqualTo(7L);
      assertThat(truncated.getBoolean("transactional")).isFalse();
      rollback(second);
      assertThat(count()).as("the rollback of the session did not bring the samples back").isZero();

      // the type takes samples again
      sql(null, "INSERT INTO " + TYPE + " SET ts = 50000, host = 'h1', v = 1");
      assertThat(count()).isEqualTo(1L);
    }

    private long count() throws Exception {
      return sql(null, "SELECT count(*) AS c FROM " + TYPE).getJSONArray("result").getJSONObject(0).getLong("c");
    }

    private String begin() throws Exception {
      final HttpResponse<String> begun = send("/begin/" + getDatabaseName(), null, "");
      assertThat(begun.statusCode()).isEqualTo(204);
      final String session = begun.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElse(null);
      assertThat(session).as("POST /begin must hand back a session id").isNotBlank();
      return session;
    }

    private void rollback(final String session) throws Exception {
      assertThat(send("/rollback/" + getDatabaseName(), session, "").statusCode()).isEqualTo(204);
    }

    private JSONObject sql(final String session, final String command) throws Exception {
      final HttpResponse<String> response = send("/command/" + getDatabaseName(), session,
          new JSONObject().put("language", "sql").put("command", command).toString());
      assertThat(response.statusCode()).as("command failed: %s", response.body()).isEqualTo(200);
      return new JSONObject(response.body());
    }

    private HttpResponse<String> send(final String path, final String session, final String body) throws Exception {
      final HttpRequest.Builder request = HttpRequest.newBuilder(
              URI.create("http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1" + path))
          .timeout(Duration.ofSeconds(30))
          .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
          .header("Content-Type", "application/json");
      if (session != null)
        request.header(HttpSessionManager.ARCADEDB_SESSION_ID, session);
      return http.send(request.POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
  }

  /**
   * A Grafana raw query that projects a subset of fields must return those fields' values, under those fields'
   * names.
   * <p>
   * Found while building the gRPC time-series API (issue #7305). The projection's indices count NON-timestamp
   * columns - {@code TimeSeriesBucket.readRow} always prepends the timestamp and then tests each non-timestamp
   * column's own ordinal against the array - but the resolver produced full-schema indices, and
   * {@code PostGrafanaQueryHandler} turned them back into columns with {@code columns.get(idx)}. Every field was
   * therefore shifted by one: the schema named {@code temperature} while the values were {@code location}'s, and
   * the last column came back all null. Both halves are now resolved by {@code TimeSeriesGateway}, which the
   * {@code /ts/query} and gRPC paths share.
   * <p>
   * {@code GrafanaTimeSeriesHandlerIT} did not catch it because it never projects: its assertions run against the
   * full column set.
   */
  @Nested
  class GrafanaFieldProjection extends BaseGraphServerTest {

    private static final String TYPE = "weather";

    @Test
    void aProjectedGrafanaQueryReturnsTheNamedFieldsValues() throws Exception {
      // The port the test server actually bound: the configured range starts at 2480, and a server already
      // listening there pushes this one up.
      final int port = getServer(0).getHttpServer().getPort();

      try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", port, getDatabaseName(), "root",
          DEFAULT_PASSWORD_FOR_TESTS)) {
        database.command("sql", "CREATE TIMESERIES TYPE " + TYPE
            + " TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE, humidity DOUBLE)");
        database.command("sql", "INSERT INTO " + TYPE
            + " SET ts = 1000, location = 'us-east', temperature = 22.5, humidity = 41.0");
        database.command("sql", "INSERT INTO " + TYPE
            + " SET ts = 2000, location = 'us-east', temperature = 23.5, humidity = 42.0");
      }

      final JSONObject target = new JSONObject()
          .put("refId", "A")
          .put("type", TYPE)
          .put("fields", new JSONArray().put("humidity"));
      final JSONObject request = new JSONObject().put("targets", new JSONArray().put(target));

      final JSONObject response = postGrafanaQuery(port, request);
      final JSONObject frame = response.getJSONObject("results").getJSONObject("A")
          .getJSONArray("frames").getJSONObject(0);

      final JSONArray fields = frame.getJSONObject("schema").getJSONArray("fields");
      assertThat(fields.length()).as("timestamp plus the one projected field").isEqualTo(2);
      assertThat(fields.getJSONObject(0).getString("name")).isEqualTo("ts");
      assertThat(fields.getJSONObject(1).getString("name")).isEqualTo("humidity");

      // The values, not just the names. Before the fix this column carried 'location' - the string 'us-east'
      // under the name 'humidity' - so an assertion on the schema alone passed against the defect.
      final JSONArray values = frame.getJSONObject("data").getJSONArray("values");
      assertThat(values.length()).isEqualTo(2);
      assertThat(values.getJSONArray(0).getLong(0)).isEqualTo(1_000L);
      assertThat(values.getJSONArray(1).getDouble(0)).isEqualTo(41.0);
      assertThat(values.getJSONArray(1).getDouble(1)).isEqualTo(42.0);
    }

    private JSONObject postGrafanaQuery(final int port, final JSONObject request) throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) URI.create(
          "http://127.0.0.1:" + port + "/api/v1/ts/" + getDatabaseName() + "/grafana/query").toURL().openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);

      try (final OutputStream os = connection.getOutputStream()) {
        os.write(request.toString().getBytes(StandardCharsets.UTF_8));
        os.flush();
      }

      assertThat(connection.getResponseCode()).isEqualTo(200);
      try (final InputStream is = connection.getInputStream()) {
        return new JSONObject(new String(is.readAllBytes(), StandardCharsets.UTF_8));
      }
    }
  }

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
  @Nested
  class EdgeCaseContract extends BaseGraphServerTest {

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

  /**
   * The HTTP half of issues #7724 and #7717, on the two aggregation surfaces that answer over HTTP.
   * <p>
   * #7724: the ceiling on the number of buckets a response may carry was enforced on the RESULT, so a request with
   * a tiny bucket interval over a wide range paid the whole scan and the whole bucket allocation and was refused
   * afterwards. The bound is now carried into the scan. What must not change is the REFUSAL - the same status and
   * the same wording, naming the setting that decided - because that is the contract a caller reads; what changes
   * is only its price, which the engine test {@code TimeSeriesAggregationCeilingTest} pins on block counts.
   * <p>
   * #7717: every {@code /ts/**} handler passed {@code null} where the engine takes its read counters, so nothing
   * an operator can see recorded whether the sealed push-downs were working. This asserts they now reach the
   * sink for a real request over a real server, which is the verification the issue asks for.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AggregationCeilingAndMetrics extends BaseGraphServerTest {

    private static final String TYPE          = "ceilingmetric";
    private static final int    MAX_ROWS      = 20;
    private static final int    SAMPLES       = 200;
    private static final long   BUCKET        = 1_000L;

    @Override
    protected void onServerConfiguration(final ContextConfiguration config) {
      // A ceiling low enough that a one-bucket-per-sample request is refused, and the metrics subsystem on so the
      // read counters have somewhere to go.
      config.setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, MAX_ROWS);
      config.setValue(GlobalConfiguration.SERVER_METRICS, true);
    }

    @Override
    protected int getServerCount() {
      return 1;
    }

    /**
     * One bucket per sample over 200 samples, against a ceiling of 20: refused, with the wording unchanged. The
     * request is the cheap abusive shape - a small bucket interval over the whole range - and it is exactly the
     * one the bound now stops early.
     */
    @Test
    void anOversizedBucketCountIsStillRefusedInTheSameWords() throws Exception {
      createSealedType(0);

      final Response refused = post(0, "/api/v1/ts/graph/query", aggregationPayload(BUCKET));

      assertThat(refused.status).as("the ceiling answers 413, as it does on every other read surface").isEqualTo(413);
      assertThat(refused.body)
          .as("the refusal has to name the setting that decided, or an operator cannot act on it")
          .contains(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getKey());

      // And the Grafana aggregation branch, which had no ceiling of its own at all, now refuses the same request.
      final Response grafana = post(0, "/api/v1/ts/graph/grafana/query", grafanaPayload(BUCKET));
      assertThat(grafana.status).isEqualTo(413);
      assertThat(grafana.body).contains(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getKey());
    }

    /**
     * The counter-case that keeps the refusal above meaningful: the same data with an interval wide enough to
     * bring the bucket count under the ceiling is answered in full. A bound that refused both would be a bug, not
     * a ceiling.
     */
    @Test
    void aRequestUnderTheCeilingIsAnsweredInFull() throws Exception {
      createSealedType(0);

      // Ten buckets over the whole range, comfortably under MAX_ROWS.
      final Response answered = post(0, "/api/v1/ts/graph/query", aggregationPayload(SAMPLES * BUCKET / 10));

      assertThat(answered.status).isEqualTo(200);
      final JSONObject body = new JSONObject(answered.body);
      assertThat(body.getJSONArray("buckets").length()).isPositive().isLessThanOrEqualTo(MAX_ROWS);
      assertThat(body.getInt("count")).isEqualTo(body.getJSONArray("buckets").length());
    }

    /**
     * Issue #7717: the read counters reach the sink. The type's samples are sealed, so the read has blocks to
     * decide about, and the meter is tagged with the database, the type and the endpoint - which is what makes the
     * ratio readable per tenant rather than as one server-wide number.
     */
    @Test
    void theReadCountersReachTheMetricsSink() throws Exception {
      createSealedType(0);

      final Response answered = post(0, "/api/v1/ts/graph/query", aggregationPayload(SAMPLES * BUCKET / 10));
      assertThat(answered.status).isEqualTo(200);

      assertThat(blocksCounted("ts-query"))
          .as("a read over a type with sealed blocks must report the blocks it decided about")
          .isPositive();

      // The raw branch counts too, and lands on the same tagged series.
      final JSONObject raw = new JSONObject();
      raw.put("type", TYPE);
      raw.put("from", 0L);
      raw.put("to", SAMPLES * BUCKET);
      raw.put("limit", 5);
      assertThat(post(0, "/api/v1/ts/graph/query", raw).status).isEqualTo(200);

      assertThat(Search.in(Metrics.globalRegistry).name("arcadedb.timeseries.read.rows")
          .tag("db", getDatabaseName()).tag("type", TYPE).tag("surface", "ts-query").counter())
          .as("the raw branch reports the rows it materialised").isNotNull();
    }

    private double blocksCounted(final String surface) {
      double total = 0;
      for (final Counter counter : Search.in(Metrics.globalRegistry)
          .name("arcadedb.timeseries.read.blocks").tag("db", getDatabaseName()).tag("type", TYPE)
          .tag("surface", surface).counters())
        total += counter.count();
      return total;
    }

    // ---- fixtures ----

    /** One sample per bucket, all of them compacted into sealed blocks before anything is read. */
    private void createSealedType(final int serverIndex) throws Exception {
      final Database database = getServerDatabase(serverIndex, getDatabaseName());
      if (database.getSchema().existsType(TYPE))
        database.getSchema().dropType(TYPE);

      database.command("sql", "CREATE TIMESERIES TYPE " + TYPE
          + " TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1 COMPACTION_INTERVAL 1 SECONDS");

      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(TYPE);
      final long[] timestamps = new long[SAMPLES];
      final Object[] values = new Object[SAMPLES];
      for (int i = 0; i < SAMPLES; i++) {
        timestamps[i] = i * BUCKET;
        values[i] = (double) i;
      }

      database.begin();
      tsType.getEngine().appendSamples(timestamps, values);
      database.commit();

      tsType.getEngine().compactAll();
    }

    private JSONObject aggregationPayload(final long bucketInterval) {
      final JSONObject sum = new JSONObject();
      sum.put("field", "value");
      sum.put("type", "SUM");
      sum.put("alias", "s");

      final JSONObject aggregation = new JSONObject();
      aggregation.put("bucketInterval", bucketInterval);
      aggregation.put("requests", new JSONArray().put(sum));

      final JSONObject payload = new JSONObject();
      payload.put("type", TYPE);
      payload.put("from", 0L);
      payload.put("to", SAMPLES * BUCKET);
      payload.put("aggregation", aggregation);
      return payload;
    }

    private JSONObject grafanaPayload(final long bucketInterval) {
      final JSONObject target = new JSONObject();
      target.put("refId", "A");
      target.put("type", TYPE);
      target.put("aggregation", aggregationPayload(bucketInterval).getJSONObject("aggregation"));

      final JSONObject grafana = new JSONObject();
      grafana.put("from", 0L);
      grafana.put("to", SAMPLES * BUCKET);
      grafana.put("targets", new JSONArray().put(target));
      return grafana;
    }

    /** The status and body of one request, because a refusal is asserted on both. */
    private record Response(int status, String body) {
    }

    private Response post(final int serverIndex, final String path, final JSONObject body) throws Exception {
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

      final int status = connection.getResponseCode();
      final InputStream stream = status < 400 ? connection.getInputStream() : connection.getErrorStream();
      try (stream) {
        return new Response(status, stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8));
      }
    }
  }

  /**
   * Issue #7831: {@code POST /api/v1/ts/{database}/prom/write} against a database that does not exist answers
   * {@code 404}, and that is the contract, not a regression.
   * <p>
   * Since issue #7681 reparented {@code AbstractBinaryHttpHandler} onto {@code DatabaseAbstractHandler}, the
   * database is resolved (with {@code allowLoad=false}) before {@code PostPrometheusWriteHandler.execute} runs, so
   * the handler's own empty-body {@code 400} is never reached for a database that is not there. The resolution
   * raises {@code DatabaseNotAvailableException}, which {@code AbstractServerHttpHandler} answers {@code 404 Database
   * not found} since issue #6778. That is the same answer every other database-scoped route gives, and it is the
   * accurate one: the body is irrelevant when the target does not exist.
   * <p>
   * What the HA-side regression test ({@code RaftTimeSeriesWriteReadYourWritesIT}) was written to prevent is a
   * {@code 500}; it pinned {@code 400} only because that was the status before #7681. This class pins the actual
   * contract on every {@code /api/v1/ts/{database}/...} route, on a single server, so a later reordering that lets
   * any of them reach an engine call against a missing database and surface a {@code 500} fails here and not only
   * in the HA lane.
   */
  @Nested
  class NonexistentDatabaseRoutes extends BaseGraphServerTest {

    // Per-request hang detector, not a latency bound.
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final String BOGUS_DB = "issue7831_database_does_not_exist";

    private final HttpClient client = HttpClient.newHttpClient();

    @Test
    void prometheusWriteWithEmptyBodyOnANonexistentDatabaseAnswers404() throws Exception {
      final HttpResponse<String> response = post("/prom/write", new byte[0], "application/x-protobuf");
      assertThat(response.statusCode()).as("empty body, nonexistent database: " + response.body()).isEqualTo(404);
      assertThat(response.body()).contains("Database not found");
    }

    @Test
    void prometheusWriteWithZeroSeriesOnANonexistentDatabaseAnswers404() throws Exception {
      final byte[] body = Snappy.compress(new WriteRequest(List.of()).encode());
      final HttpResponse<String> response = post("/prom/write", body, "application/x-protobuf");
      assertThat(response.statusCode()).as("zero-series body, nonexistent database: " + response.body()).isEqualTo(404);
    }

    @Test
    void prometheusWriteWithSamplesOnANonexistentDatabaseAnswers404() throws Exception {
      final byte[] body = Snappy.compress(new WriteRequest(List.of(new TimeSeries(
          List.of(new Label("__name__", "issue7831_metric"), new Label("host", "h1")),
          List.of(new Sample(1.0, 1000))))).encode());
      final HttpResponse<String> response = post("/prom/write", body, "application/x-protobuf");
      assertThat(response.statusCode()).as("real samples, nonexistent database: " + response.body()).isEqualTo(404);
    }

    @Test
    void prometheusWriteWithEmptyBodyOnAnExistingDatabaseStillAnswers400() throws Exception {
      // The handler's own empty-body check is still the answer when the database IS there: the 404 above comes
      // from the missing database, not from the handler losing its body validation.
      final HttpResponse<String> response = client.send(HttpRequest.newBuilder(
              URI.create(getServerHttpUrl("/api/v1/ts/" + getDatabaseName() + "/prom/write")))
          .timeout(REQUEST_TIMEOUT)
          .header("Authorization", basicAuth())
          .header("Content-Type", "application/x-protobuf")
          .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[0]))
          .build(), HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
      assertThat(response.body()).contains("Request body is empty");
    }

    @Test
    void everyTimeSeriesRouteAnswers404ForANonexistentDatabase() throws Exception {
      final byte[] promRead = Snappy.compress(new byte[0]);
      final byte[] lineProtocol = "cpu,host=h1 value=1.0 1000".getBytes(StandardCharsets.UTF_8);
      final byte[] json = "{\"type\":\"cpu\"}".getBytes(StandardCharsets.UTF_8);

      // The bodies only matter if a route ever validates them before resolving the database. Should that happen
      // it answers 400 here and this fails: that is the regression, do not "fix" it by loosening to 4xx.
      // Soft: one regressed route must not hide the state of the others.
      final SoftAssertions soft = new SoftAssertions();
      assertNotFound(soft, post("/prom/read", promRead, "application/x-protobuf"), "POST prom/read");
      assertNotFound(soft, post("/write", lineProtocol, "text/plain"), "POST write");
      assertNotFound(soft, post("/query", json, "application/json"), "POST query");
      assertNotFound(soft, post("/grafana/query", json, "application/json"), "POST grafana/query");
      assertNotFound(soft, get("/latest?type=cpu"), "GET latest");
      assertNotFound(soft, get("/grafana/health"), "GET grafana/health");
      assertNotFound(soft, get("/grafana/metadata"), "GET grafana/metadata");
      assertNotFound(soft, get("/prom/api/v1/query?query=cpu"), "GET prom query");
      assertNotFound(soft, get("/prom/api/v1/query_range?query=cpu&start=0&end=10&step=1"), "GET prom query_range");
      assertNotFound(soft, get("/prom/api/v1/labels"), "GET prom labels");
      assertNotFound(soft, get("/prom/api/v1/label/host/values"), "GET prom label values");
      assertNotFound(soft, get("/prom/api/v1/series?match%5B%5D=cpu"), "GET prom series");

      soft.assertAll();
    }

    private static void assertNotFound(final SoftAssertions soft, final HttpResponse<String> response, final String route) {
      soft.assertThat(response.statusCode()).as(route + " on a nonexistent database: " + response.body()).isEqualTo(404);
    }

    private HttpResponse<String> post(final String suffix, final byte[] body, final String contentType) throws Exception {
      return client.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl("/api/v1/ts/" + BOGUS_DB + suffix)))
          .timeout(REQUEST_TIMEOUT)
          .header("Authorization", basicAuth())
          .header("Content-Type", contentType)
          .POST(HttpRequest.BodyPublishers.ofByteArray(body))
          .build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(final String suffix) throws Exception {
      return client.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl("/api/v1/ts/" + BOGUS_DB + suffix)))
          .timeout(REQUEST_TIMEOUT)
          .header("Authorization", basicAuth())
          .GET()
          .build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String basicAuth() {
      return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    }
  }
}
