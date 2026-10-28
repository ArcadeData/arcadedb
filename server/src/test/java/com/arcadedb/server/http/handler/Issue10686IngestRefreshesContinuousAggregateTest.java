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

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.ContinuousAggregate;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10686: the HTTP ingest endpoints of a TIMESERIES type - the line protocol and the Prometheus remote write - stored the
 * samples and refreshed none of the continuous aggregates over the type, so an aggregate fed only through them kept the buckets of
 * its creation for good. Each endpoint is driven end to end and asserted on its own.
 */
class Issue10686IngestRefreshesContinuousAggregateTest extends BaseGraphServerTest {
  private static final String TYPE = "cpu10686";
  private static final long   HOUR = 3_600_000L;
  private static final long   H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

  private final HttpClient client = HttpClient.newHttpClient();

  private record Bucket(long start, String host, double total) {
  }

  @BeforeEach
  void createSourceAndAggregate() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
    database.command("sql", "CREATE CONTINUOUS AGGREGATE " + TYPE + "_hourly AS SELECT ts.timeBucket('1h', ts) AS hr, host, "
        + "sum(value) AS total FROM " + TYPE + " GROUP BY hr, host");
  }

  @Test
  void aLineProtocolWriteRefreshesTheAggregate() throws Exception {
    final long before = aggregate().getRefreshCount();

    final String body = TYPE + ",host=a value=1 " + (H0 + 1_000) + "\n" //
        + TYPE + ",host=a value=2 " + (H0 + 2_000) + "\n" //
        + TYPE + ",host=b value=4 " + (H0 + HOUR + 1_000);
    final HttpResponse<String> response = client.send(HttpRequest.newBuilder()
        .uri(new URI(baseUrl() + "/api/v1/ts/" + getDatabaseName() + "/write?precision=ms"))
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .setHeader("Content-Type", "text/plain")
        .setHeader("Authorization", basicAuth())
        .build(), BodyHandlers.ofString());
    assertThat(response.statusCode()).as(response.body()).isEqualTo(204);

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 3), new Bucket(H0 + HOUR, "b", 4));
  }

  @Test
  void aPrometheusRemoteWriteRefreshesTheAggregate() throws Exception {
    final long before = aggregate().getRefreshCount();

    final WriteRequest request = new WriteRequest(List.of(
        new TimeSeries(List.of(new Label("__name__", TYPE), new Label("host", "a")),
            List.of(new Sample(1, H0 + 1_000), new Sample(2, H0 + 2_000))),
        new TimeSeries(List.of(new Label("__name__", TYPE), new Label("host", "b")), List.of(new Sample(4, H0 + HOUR + 1_000)))));
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        baseUrl() + "/api/v1/ts/" + getDatabaseName() + "/prom/write").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", basicAuth());
    connection.setRequestProperty("Content-Type", "application/x-protobuf");
    connection.setRequestProperty("Content-Encoding", "snappy");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(Snappy.compress(request.encode()));
    }
    assertThat(connection.getResponseCode()).isEqualTo(204);

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 3), new Bucket(H0 + HOUR, "b", 4));
  }

  private ContinuousAggregate aggregate() {
    return getServerDatabase(0, getDatabaseName()).getSchema().getContinuousAggregate(TYPE + "_hourly");
  }

  private List<Bucket> buckets() {
    final List<Bucket> result = new ArrayList<>();
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql",
        "SELECT FROM " + TYPE + "_hourly ORDER BY hr, host")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        result.add(new Bucket(DateUtils.toEpochMillis(row.getProperty("hr")), row.getProperty("host"),
            ((Number) row.getProperty("total")).doubleValue()));
      }
    }
    return result;
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  /** The server's real port, never a hardcoded one. */
  private String baseUrl() {
    return "http://127.0.0.1:" + getServerHttpPort();
  }
}
