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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.LabelMatcher;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.MatchType;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Query;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadRequest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadResponse;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue7899NonFirstTimestampReadSurfacesTest extends BaseGraphServerTest {

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
