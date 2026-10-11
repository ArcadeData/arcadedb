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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.LabelMatcher;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.MatchType;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Query;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadRequest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadResponse;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9623: {@code POST /api/v1/ts/{db}/prom/read} grouped a query's rows into series on the TAG values
 * joined with {@code '|'}, unescaped and without the label name, so two distinct label sets could spell one key
 * and come back as ONE {@code TimeSeries} carrying both series' samples under the first one's labels. Two ways
 * to collide: a value containing {@code '|'}, and a leading empty tag, which suppressed its own separator.
 * <p>
 * The key now length-prefixes each label's name and value, like the PromQL paths do, and leaves an empty value
 * out of both the key and the emitted labels, because an empty Prometheus label is an absent one (#7712).
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/9623">issue #9623</a>
 */
class Issue9623PrometheusReadSeriesKeyTest extends BaseGraphServerTest {

  private static final String TYPE_NAME = "req_total";
  private static final long   TS        = 1_700_000_000_000L;

  private final HttpClient client = HttpClient.newHttpClient();

  @BeforeEach
  void createType() throws Exception {
    command("CREATE TIMESERIES TYPE " + TYPE_NAME + " TIMESTAMP ts TAGS (host STRING, region STRING) FIELDS (value DOUBLE)");
  }

  @Test
  void aPipeInsideATagValueDoesNotMergeTwoSeries() throws Exception {
    insert(TS, "'a|b'", "'c'", 1.0);
    insert(TS, "'a'", "'b|c'", 2.0);

    final Map<Map<String, String>, List<Sample>> series = readAll();

    assertThat(series).as("two distinct label sets are two series").hasSize(2);
    assertThat(series.get(labels("host", "a|b", "region", "c"))).containsExactly(new Sample(1.0, TS));
    assertThat(series.get(labels("host", "a", "region", "b|c"))).containsExactly(new Sample(2.0, TS));
  }

  @Test
  void aLeadingEmptyTagDoesNotSwallowItsSeparator() throws Exception {
    // host="" contributed nothing AND suppressed the separator, so both rows spelled "x|y"
    insert(TS, "''", "'x|y'", 3.0);
    insert(TS, "'x'", "'y'", 4.0);

    final Map<Map<String, String>, List<Sample>> series = readAll();

    assertThat(series).hasSize(2);
    assertThat(series.get(labels("region", "x|y"))).as("an empty host is an absent label")
        .containsExactly(new Sample(3.0, TS));
    assertThat(series.get(labels("host", "x", "region", "y"))).containsExactly(new Sample(4.0, TS));
  }

  @Test
  void anEmptyValueInDifferentColumnsDoesNotMergeTwoSeries() throws Exception {
    // dropping the empty tag from the key is safe only because the key carries the label NAME too
    insert(TS, "''", "'x'", 5.0);
    insert(TS, "'x'", "''", 6.0);

    final Map<Map<String, String>, List<Sample>> series = readAll();

    assertThat(series).hasSize(2);
    assertThat(series.get(labels("region", "x"))).containsExactly(new Sample(5.0, TS));
    assertThat(series.get(labels("host", "x"))).containsExactly(new Sample(6.0, TS));
  }

  @Test
  void aNullAndAnEmptyTagAreTheSameAbsentLabel() throws Exception {
    // Prometheus identifies a series by its non-empty labels, so these two rows are one series, reported
    // WITHOUT a host label - the key and the emitted labels have to agree on that
    insert(TS, "null", "'q'", 7.0);
    insert(TS + 1, "''", "'q'", 8.0);

    final Map<Map<String, String>, List<Sample>> series = readAll();

    assertThat(series).hasSize(1);
    assertThat(series.get(labels("region", "q"))).containsExactly(new Sample(7.0, TS), new Sample(8.0, TS + 1));
  }

  // ---------------------------------------------------------------------------------------------------
  // Plumbing
  // ---------------------------------------------------------------------------------------------------

  private void insert(final long ts, final String host, final String region, final double value) throws Exception {
    command("INSERT INTO " + TYPE_NAME + " SET ts = " + ts + ", host = " + host + ", region = " + region + ", value = "
        + value);
  }

  /** Every series the remote read answers, by its label set (metric name left out), in the order received. */
  private Map<Map<String, String>, List<Sample>> readAll() throws Exception {
    final ReadResponse response = promReadOk(new ReadRequest(List.of(
        new Query(0, TS + 10, List.of(new LabelMatcher(MatchType.EQ, "__name__", TYPE_NAME))))));
    assertThat(response.getResults()).hasSize(1);

    final Map<Map<String, String>, List<Sample>> result = new LinkedHashMap<>();
    for (final TimeSeries ts : response.getResults().getFirst().getTimeSeries()) {
      final Map<String, String> labels = new LinkedHashMap<>();
      for (final Label label : ts.getLabels())
        if (!"__name__".equals(label.name()))
          labels.put(label.name(), label.value());
      assertThat(result.put(labels, ts.getSamples())).as("label set %s reported twice", labels).isNull();
    }
    return result;
  }

  private static Map<String, String> labels(final String... nameValues) {
    final Map<String, String> labels = new LinkedHashMap<>();
    for (int i = 0; i < nameValues.length; i += 2)
      labels.put(nameValues[i], nameValues[i + 1]);
    return labels;
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

  private void command(final String sql) throws Exception {
    final HttpResponse<String> response = client.send(HttpRequest.newBuilder()
        .uri(new URI(baseUrl() + "/api/v1/command/" + getDatabaseName()))
        .POST(HttpRequest.BodyPublishers.ofString(
            new JSONObject().put("language", "sql").put("command", sql).toString()))
        .setHeader("Content-Type", "application/json")
        .setHeader("Authorization", basicAuth())
        .build(), BodyHandlers.ofString());
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private String baseUrl() {
    return "http://127.0.0.1:" + getServerHttpPort();
  }
}
