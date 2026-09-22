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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue8140NonFirstTimestampAggregationTest extends BaseGraphServerTest {

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
