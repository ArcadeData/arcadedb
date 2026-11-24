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

import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.LabelMatcher;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.MatchType;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Query;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadRequest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadResponse;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.TimeZone;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The HTTP read surfaces of a TIMESERIES type ({@code /ts/{db}/query}, {@code /latest}, the Grafana query and the
 * Prometheus remote read) against types whose layout or tag values are easy to misread.
 */
class TimeSeriesHttpReadSurfacesTest extends BaseGraphServerTest {
  // Regression: #7899 a TIMESTAMP column not declared first: names and values line up on every read surface
  /**
   * Issue #7899: every HTTP read surface for a TIMESERIES type publishes its values in the layout the engine
   * produces - {@code [timestamp, non-TIMESTAMP columns in schema order...]} - and names them from the type's
   * schema. When the TIMESTAMP column is not declared first the two stopped lining up, and since issue #7702 the
   * grammar can spell such a declaration.
   * <p>
   * The type below puts the TIMESTAMP column LAST, so the three positions it can be confused with are all
   * occupied: {@code value} sits where the timestamp is returned and {@code host} where {@code value} is.
   * <ul>
   *   <li>{@code POST /ts/{db}/query} and {@code GET /ts/{db}/latest} take their {@code columns} array from
   *       {@code TimeSeriesGateway.columnNames(columns, null)}, whose {@code null} branch answered the raw
   *       schema instead of the row order its own javadoc promises;</li>
   *   <li>the Grafana frame names its fields from {@code TimeSeriesGateway.selectedColumns} and then reads
   *       {@code row[c]} against them, so a mis-ordered name list also mistypes the field;</li>
   *   <li>the Prometheus remote read indexed the row by SCHEMA position for both its labels and its sample
   *       value, so it reported a label carrying another column's content and a sample carrying the timestamp.</li>
   * </ul>
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7899">issue #7899</a>
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class NonFirstTimestampReads {
    private static final String TYPE_NAME = "cpu_usage";
    private static final long   TS        = 1_700_000_000_000L;

    private final HttpClient client = HttpClient.newHttpClient();

    @BeforeEach
    void createSeries() throws Exception {
      // TIMESTAMP declared LAST: schema order is [value, host, ts], engine row order is [ts, value, host].
      assertThat(command("CREATE TIMESERIES TYPE " + TYPE_NAME
          + " FIELDS (value DOUBLE) TAGS (host STRING) TIMESTAMP ts")).isEqualTo(200);
      // Through SQL, which resolves every column by name, so the stored sample is right whatever the reads do.
      assertThat(command("INSERT INTO " + TYPE_NAME + " SET ts = " + TS + ", host = 'srv-1', value = 42.5"))
          .isEqualTo(200);
    }

    @Test
    void theQueryEndpointNamesTheColumnsInTheOrderItReturnsTheirValues() throws Exception {
      final JSONObject body = queryOk(new JSONObject().put("type", TYPE_NAME));

      assertThat(body.getJSONArray("columns").toList())
          .as("the names must describe the rows beside them").containsExactly("ts", "value", "host");

      final JSONArray row = body.getJSONArray("rows").getJSONArray(0);
      assertThat(row.getLong(0)).isEqualTo(TS);
      assertThat(row.getDouble(1)).isEqualTo(42.5);
      assertThat(row.getString(2)).isEqualTo("srv-1");
    }

    @Test
    void theLatestEndpointNamesTheColumnsInTheOrderItReturnsTheirValues() throws Exception {
      final HttpResponse<String> response = client.send(HttpRequest.newBuilder()
          .uri(new URI(baseUrl() + "/api/v1/ts/" + getDatabaseName() + "/latest?type=" + TYPE_NAME))
          .GET().setHeader("Authorization", basicAuth()).build(), BodyHandlers.ofString());
      assertThat(response.statusCode()).isEqualTo(200);

      final JSONObject body = new JSONObject(response.body());
      assertThat(body.getJSONArray("columns").toList()).containsExactly("ts", "value", "host");

      final JSONArray latest = body.getJSONArray("latest");
      assertThat(latest.getLong(0)).isEqualTo(TS);
      assertThat(latest.getDouble(1)).isEqualTo(42.5);
      assertThat(latest.getString(2)).isEqualTo("srv-1");
    }

    @Test
    void theGrafanaFrameNamesEachFieldAfterTheValueItCarries() throws Exception {
      final HttpResponse<String> response = client.send(HttpRequest.newBuilder()
          .uri(new URI(baseUrl() + "/api/v1/ts/" + getDatabaseName() + "/grafana/query"))
          .POST(HttpRequest.BodyPublishers.ofString(new JSONObject()
              .put("targets", new JSONArray().put(new JSONObject().put("refId", "A").put("type", TYPE_NAME)))
              .toString()))
          .setHeader("Content-Type", "application/json")
          .setHeader("Authorization", basicAuth())
          .build(), BodyHandlers.ofString());
      assertThat(response.statusCode()).isEqualTo(200);

      final JSONObject frame = new JSONObject(response.body()).getJSONObject("results").getJSONObject("A")
          .getJSONArray("frames").getJSONObject(0);

      final JSONArray fields = frame.getJSONObject("schema").getJSONArray("fields");
      assertThat(fields.getJSONObject(0).getString("name")).isEqualTo("ts");
      assertThat(fields.getJSONObject(0).getString("type")).as("the time field must be the timestamp column")
          .isEqualTo("time");
      assertThat(fields.getJSONObject(1).getString("name")).isEqualTo("value");
      assertThat(fields.getJSONObject(2).getString("name")).isEqualTo("host");

      final JSONArray values = frame.getJSONObject("data").getJSONArray("values");
      assertThat(values.getJSONArray(0).getLong(0)).isEqualTo(TS);
      assertThat(values.getJSONArray(1).getDouble(0)).isEqualTo(42.5);
      assertThat(values.getJSONArray(2).getString(0)).isEqualTo("srv-1");
    }

    @Test
    void thePrometheusRemoteReadLabelsAndSamplesComeFromTheirOwnColumns() throws Exception {
      final ReadResponse response = promReadOk(new ReadRequest(List.of(
          new Query(0, TS + 1, List.of(new LabelMatcher(MatchType.EQ, "__name__", TYPE_NAME))))));

      assertThat(response.getResults()).hasSize(1);
      assertThat(response.getResults().getFirst().getTimeSeries()).hasSize(1);
      final TimeSeries series = response.getResults().getFirst().getTimeSeries().getFirst();

      assertThat(series.getLabels().stream().filter(l -> "host".equals(l.name())).findFirst())
          .as("the 'host' label must carry the TAG column's value, not the neighbouring column's")
          .hasValueSatisfying(label -> assertThat(label.value()).isEqualTo("srv-1"));

      assertThat(series.getSamples()).hasSize(1);
      assertThat(series.getSamples().getFirst().timestampMs()).isEqualTo(TS);
      assertThat(series.getSamples().getFirst().value())
          .as("the sample must be the 'value' FIELD, not the timestamp read off its schema position")
          .isEqualTo(42.5);
    }

    // ---------------------------------------------------------------------------------------------------
    // Plumbing
    // ---------------------------------------------------------------------------------------------------

    private JSONObject queryOk(final JSONObject request) throws Exception {
      final HttpResponse<String> response = client.send(HttpRequest.newBuilder()
          .uri(new URI(baseUrl() + "/api/v1/ts/" + getDatabaseName() + "/query"))
          .POST(HttpRequest.BodyPublishers.ofString(request.toString()))
          .setHeader("Content-Type", "application/json")
          .setHeader("Authorization", basicAuth())
          .build(), BodyHandlers.ofString());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      return new JSONObject(response.body());
    }

    private ReadResponse promReadOk(final ReadRequest request) throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) new URI(
          baseUrl() + "/api/v1/ts/" + getDatabaseName() + "/prom/read").toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", basicAuth());
      connection.setRequestProperty("Content-Type", "application/x-protobuf");
      connection.setRequestProperty("Content-Encoding", "snappy");
      connection.setDoOutput(true);
      try (final OutputStream os = connection.getOutputStream()) {
        os.write(Snappy.compress(request.encode()));
        os.flush();
      }
      assertThat(connection.getResponseCode()).isEqualTo(200);
      try (final InputStream is = connection.getInputStream()) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        is.transferTo(baos);
        return ReadResponse.decode(Snappy.uncompress(baos.toByteArray()));
      }
    }

    private int command(final String sql) throws Exception {
      final HttpResponse<String> response = client.send(HttpRequest.newBuilder()
          .uri(new URI(baseUrl() + "/api/v1/command/" + getDatabaseName()))
          .POST(HttpRequest.BodyPublishers.ofString(
              new JSONObject().put("language", "sql").put("command", sql).toString()))
          .setHeader("Content-Type", "application/json")
          .setHeader("Authorization", basicAuth())
          .build(), BodyHandlers.ofString());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      return response.statusCode();
    }

    private static String basicAuth() {
      return "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    }

    /** The server's real port, never a hardcoded one: when 2480 is taken the server binds the next free port. */
    private String baseUrl() {
      return "http://127.0.0.1:" + getServerHttpPort();
    }
  }

  // Regression: #8140 a TIMESTAMP column not declared first: the aggregation surfaces aggregate the named field
  /**
   * Issue #8140 on the two HTTP aggregation surfaces: {@code POST /api/v1/ts/{db}/query} with an
   * {@code aggregation} member, and the Grafana query route's aggregation branch.
   * <p>
   * Both resolve the caller's field name to a SCHEMA index and used to hand that number to the engine as a
   * {@code MultiColumnAggregationRequest.columnIndex()}, which is a position in the ENGINE ROW. The two coincide
   * only while the TIMESTAMP column is declared FIRST, so the type below declares it LAST: {@code value} has
   * schema index 0 and row index 1, and the schema reading points the engine's mutable half at the timestamp.
   * <p>
   * Every assertion is made twice, once against the samples in the mutable bucket and once after
   * {@code compactAll()}, because the mutable half and the sealed half disagreed and either one alone passes
   * against a fix that corrects only the other.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8140">issue #8140</a>
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class NonFirstTimestampAggregation {
    private static final String TYPE_NAME = "cpu_agg_8140";
    private static final long   TS        = 1_700_000_000_000L;
    private static final long   HOUR      = 3_600_000L;
    private static final int    SAMPLES   = 5;

    private final HttpClient client = HttpClient.newHttpClient();

    @BeforeEach
    void createSeries() throws Exception {
      // TIMESTAMP declared LAST: schema order is [value, host, ts], engine row order is [ts, value, host].
      assertThat(command("CREATE TIMESERIES TYPE " + TYPE_NAME
          + " FIELDS (value DOUBLE) TAGS (host STRING) TIMESTAMP ts")).isEqualTo(200);
      for (int i = 0; i < SAMPLES; i++)
        assertThat(command("INSERT INTO " + TYPE_NAME + " SET ts = " + (TS + i * 1000L)
            + ", host = 'srv-1', value = " + (i + 1) + ".0")).isEqualTo(200);
    }

    /** 1+2+3+4+5, and the largest of them - not the timestamps, which sum to about 8.5e12. */
    @Test
    void theQueryEndpointAggregatesTheNamedFieldBeforeAndAfterCompaction() throws Exception {
      assertQueryAggregation("mutable");
      compact();
      assertQueryAggregation("sealed");
    }

    @Test
    void theGrafanaAggregationBranchAggregatesTheNamedFieldBeforeAndAfterCompaction() throws Exception {
      assertGrafanaAggregation("mutable");
      compact();
      assertGrafanaAggregation("sealed");
    }

    /**
     * COUNT reads no column on either half, so it must stay right while the column-carrying requests beside it
     * move onto the row convention - the request it builds names no column at all.
     */
    @Test
    void countIsUnaffectedByTheColumnConvention() throws Exception {
      assertThat(aggregateValue("COUNT")).isEqualTo((double) SAMPLES);
      compact();
      assertThat(aggregateValue("COUNT")).isEqualTo((double) SAMPLES);
    }

    /**
     * The guard that keeps row position 0 - the timestamp - out of an aggregation request, and therefore keeps
     * the sealed half's new refusal unreachable from this endpoint. A TIMESTAMP column is DELTA_OF_DELTA encoded,
     * so {@code requireAggregatableColumn} refuses it as a 400 naming the column, exactly as before (issue
     * #7725); the fix must not have turned that into an engine-level failure.
     */
    @Test
    void aggregatingTheTimestampColumnIsStillA400NamingIt() throws Exception {
      final HttpResponse<String> response = post("/query", new JSONObject()
          .put("type", TYPE_NAME)
          .put("aggregation", new JSONObject()
              .put("bucketInterval", HOUR)
              .put("requests", new JSONArray()
                  .put(new JSONObject().put("field", "ts").put("type", "SUM").put("alias", "a")))));

      assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
      assertThat(response.body()).contains("ts").contains("is not stored as a number");
    }

    // ---------------------------------------------------------------------------------------------------
    // Assertions
    // ---------------------------------------------------------------------------------------------------

    private void assertQueryAggregation(final String half) throws Exception {
      final JSONObject body = queryOk(new JSONObject()
          .put("type", TYPE_NAME)
          .put("aggregation", new JSONObject()
              .put("bucketInterval", HOUR)
              .put("requests", new JSONArray()
                  .put(new JSONObject().put("field", "value").put("type", "SUM").put("alias", "s"))
                  .put(new JSONObject().put("field", "value").put("type", "MAX").put("alias", "m")))));

      assertThat(body.getJSONArray("aggregations").toList()).containsExactly("s", "m");
      assertThat(body.getInt("count")).as("one bucket holds every sample").isEqualTo(1);

      final JSONArray values = body.getJSONArray("buckets").getJSONObject(0).getJSONArray("values");
      assertThat(values.getDouble(0)).as("%s: SUM(value) must sum the FIELD, not the timestamps", half).isEqualTo(15.0);
      assertThat(values.getDouble(1)).as("%s: MAX(value) must read the FIELD", half).isEqualTo(5.0);
    }

    private void assertGrafanaAggregation(final String half) throws Exception {
      final HttpResponse<String> response = post("/grafana/query", new JSONObject()
          .put("targets", new JSONArray().put(new JSONObject()
              .put("refId", "A")
              .put("type", TYPE_NAME)
              .put("aggregation", new JSONObject()
                  .put("bucketInterval", HOUR)
                  .put("requests", new JSONArray()
                      .put(new JSONObject().put("field", "value").put("type", "SUM").put("alias", "s"))
                      .put(new JSONObject().put("field", "value").put("type", "MAX").put("alias", "m")))))));
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);

      final JSONObject frame = new JSONObject(response.body()).getJSONObject("results").getJSONObject("A")
          .getJSONArray("frames").getJSONObject(0);
      final JSONArray fields = frame.getJSONObject("schema").getJSONArray("fields");
      assertThat(fields.getJSONObject(1).getString("name")).isEqualTo("s");
      assertThat(fields.getJSONObject(2).getString("name")).isEqualTo("m");

      final JSONArray values = frame.getJSONObject("data").getJSONArray("values");
      assertThat(values.getJSONArray(1).getDouble(0))
          .as("%s: the SUM field must carry the FIELD's sum, not the timestamps'", half).isEqualTo(15.0);
      assertThat(values.getJSONArray(2).getDouble(0)).as("%s: MAX", half).isEqualTo(5.0);
    }

    private double aggregateValue(final String type) throws Exception {
      final JSONObject body = queryOk(new JSONObject()
          .put("type", TYPE_NAME)
          .put("aggregation", new JSONObject()
              .put("bucketInterval", HOUR)
              .put("requests", new JSONArray()
                  .put(new JSONObject().put("field", "value").put("type", type).put("alias", "a")))));
      return body.getJSONArray("buckets").getJSONObject(0).getJSONArray("values").getDouble(0);
    }

    // ---------------------------------------------------------------------------------------------------
    // Plumbing
    // ---------------------------------------------------------------------------------------------------

    /** Seals the samples through the server's own database: no wire protocol asks for compaction. */
    private void compact() throws IOException {
      ((LocalTimeSeriesType) getServerDatabase(0, getDatabaseName()).getSchema().getType(TYPE_NAME))
          .getEngine().compactAll();
    }

    private JSONObject queryOk(final JSONObject request) throws Exception {
      final HttpResponse<String> response = post("/query", request);
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      return new JSONObject(response.body());
    }

    private HttpResponse<String> post(final String path, final JSONObject request) throws Exception {
      return client.send(HttpRequest.newBuilder()
          .uri(new URI(baseUrl() + "/api/v1/ts/" + getDatabaseName() + path))
          .POST(HttpRequest.BodyPublishers.ofString(request.toString()))
          .setHeader("Content-Type", "application/json")
          .setHeader("Authorization", basicAuth())
          .build(), BodyHandlers.ofString());
    }

    private int command(final String sql) throws Exception {
      final HttpResponse<String> response = client.send(HttpRequest.newBuilder()
          .uri(new URI(baseUrl() + "/api/v1/command/" + getDatabaseName()))
          .POST(HttpRequest.BodyPublishers.ofString(
              new JSONObject().put("language", "sql").put("command", sql).toString()))
          .setHeader("Content-Type", "application/json")
          .setHeader("Authorization", basicAuth())
          .build(), BodyHandlers.ofString());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      return response.statusCode();
    }

    private static String basicAuth() {
      return "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    }

    /** The server's real port, never a hardcoded one: when 2480 is taken the server binds the next free port. */
    private String baseUrl() {
      return "http://127.0.0.1:" + getServerHttpPort();
    }
  }

  // Regression: #9879 a DATETIME tag filter is read in the database time zone, not the JVM one
  /**
   * Issue #9879: the HTTP time-series endpoints read a DATETIME tag value given as text in the JVM's default time zone, while the
   * {@code INSERT} that wrote the tag read it in the database's. On a JVM whose zone is not the database's, the {@code tags} object
   * of {@code POST /ts/{database}/query} and of the Grafana query, and the {@code tag=name:value} parameter of
   * {@code GET /ts/{database}/latest}, named another epoch and selected nothing.
   * <p>
   * The database captures its zone when the server opens it; the JVM default is moved afterwards, to a zone with an offset on the
   * stored date, so the two differ wherever the test runs.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class TagFilterDatabaseTimeZone {
    private static final String STORED = "2024-01-01 00:00:00";

    private final HttpClient client = HttpClient.newHttpClient();
    private       TimeZone   jvmZone;

    @BeforeEach
    void createTypeAndMoveTheJvmZone() throws Exception {
      jvmZone = TimeZone.getDefault();
      final TimeZone databaseZone = getServerDatabase(0, getDatabaseName()).getSchema().getTimeZone();
      TimeZone.setDefault(TimeZone.getTimeZone(databaseZone.getRawOffset() == 9 * 3_600_000 ? "America/New_York" : "Asia/Tokyo"));

      command("CREATE TIMESERIES TYPE ev9879 TIMESTAMP ts TAGS (code DATETIME) FIELDS (value DOUBLE)");
      command("INSERT INTO ev9879 SET ts = 1000, code = '" + STORED + "', value = 4.0");
    }

    @AfterEach
    void restoreTheJvmZone() {
      TimeZone.setDefault(jvmZone);
    }

    @Test
    void queryTagsObjectSelectsTheRowTheInsertWrote() throws Exception {
      final JSONObject request = new JSONObject().put("type", "ev9879").put("tags", new JSONObject().put("code", STORED));
      final JSONObject result = new JSONObject(send("/api/v1/ts/" + getDatabaseName() + "/query", request));
      assertThat(result.getInt("count")).isEqualTo(1);

      request.put("tags", new JSONObject().put("code", "2025-06-01 00:00:00"));
      assertThat(new JSONObject(send("/api/v1/ts/" + getDatabaseName() + "/query", request)).getInt("count")).isZero();
    }

    @Test
    void latestTagParameterSelectsTheRowTheInsertWrote() throws Exception {
      final JSONObject result = new JSONObject(send("/api/v1/ts/" + getDatabaseName() + "/latest?type=ev9879&tag="
          + URLEncoder.encode("code:" + STORED, StandardCharsets.UTF_8), null));
      assertThat(result.isNull("latest")).isFalse();
      assertThat(result.getJSONArray("latest").getDouble(2)).isEqualTo(4.0);
    }

    @Test
    void grafanaTargetTagsSelectTheRowTheInsertWrote() throws Exception {
      final JSONObject target = new JSONObject().put("refId", "A").put("type", "ev9879")
          .put("tags", new JSONObject().put("code", STORED));
      final JSONObject request = new JSONObject().put("from", 0L).put("to", 10_000L)
          .put("targets", new JSONArray().put(target));
      final JSONObject result = new JSONObject(send("/api/v1/ts/" + getDatabaseName() + "/grafana/query", request));

      final JSONObject frame = result.getJSONObject("results").getJSONObject("A").getJSONArray("frames").getJSONObject(0);
      assertThat(frame.getJSONObject("data").getJSONArray("values").getJSONArray(0).length()).isEqualTo(1);
    }

    /** POSTs {@code body} to {@code path}, or GETs it when {@code body} is null, and answers the 200 body. */
    private String send(final String path, final JSONObject body) throws Exception {
      final HttpRequest.Builder builder = HttpRequest.newBuilder().uri(new URI(getServerHttpUrl(0, path)))
          .setHeader("Authorization", basicAuth());
      if (body != null)
        builder.POST(HttpRequest.BodyPublishers.ofString(body.toString())).setHeader("Content-Type", "application/json");
      else
        builder.GET();
      final HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      return response.body();
    }

    private void command(final String sql) throws Exception {
      send("/api/v1/command/" + getDatabaseName(), new JSONObject().put("language", "sql").put("command", sql));
    }

    private static String basicAuth() {
      return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    }
  }
}
