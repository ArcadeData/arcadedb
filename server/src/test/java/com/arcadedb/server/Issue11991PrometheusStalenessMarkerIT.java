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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11991 over the wire: the staleness marker a Prometheus remote write carries (the NaN with bits
 * {@code 0x7ff0000000000002}) has to reach the PromQL evaluator with those very bits, through the protobuf decoder, the
 * auto-created DOUBLE column and the store. A marker that turned into an ordinary NaN on the way would be answered as a value
 * again, and the engine-level test, which appends the marker straight to the engine, could not tell.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue11991PrometheusStalenessMarkerIT extends BaseGraphServerTest {

  /** Prometheus' value.StaleNaN, spelled out so the test pins the wire value. */
  private static final double STALE = Double.longBitsToDouble(0x7ff0000000000002L);

  @Test
  void aSeriesEndedOverRemoteWriteLeavesTheInstantVector() throws Exception {
    testEachServer(serverIndex -> {
      postPromWrite(serverIndex, new WriteRequest(List.of(
          new TimeSeries(
              List.of(new Label("__name__", "stale_up"), new Label("job", "a")),
              List.of(new Sample(1.0, 1_000), new Sample(1.0, 16_000), new Sample(STALE, 31_000))),
          new TimeSeries(
              List.of(new Label("__name__", "stale_up"), new Label("job", "b")),
              List.of(new Sample(1.0, 1_000), new Sample(1.0, 16_000), new Sample(1.0, 31_000))),
          // an ORDINARY NaN is a value: Prometheus keeps the series and answers NaN
          new TimeSeries(
              List.of(new Label("__name__", "stale_up"), new Label("job", "c")),
              List.of(new Sample(Double.NaN, 31_000))))));

      // before the marker the ended series is still there
      assertThat(jobs(serverIndex, "stale_up", 20)).containsExactlyInAnyOrder("a", "b");
      // from the marker on it is gone, the ordinary NaN stays
      assertThat(jobs(serverIndex, "stale_up", 32)).containsExactlyInAnyOrder("b", "c");

      // the marker is no sample of a range: job a has the two samples at 1s and 16s in (-28s, 32s]
      final JSONArray counts = query(serverIndex, "count_over_time(stale_up{job=\"a\"}[1m])", 32);
      assertThat(counts.length()).isEqualTo(1);
      assertThat(counts.getJSONObject(0).getJSONArray("value").getString(1)).isEqualTo("2");
    });
  }

  private Set<String> jobs(final int serverIndex, final String promQL, final long atSeconds) throws Exception {
    final JSONArray result = query(serverIndex, promQL, atSeconds);
    final Set<String> jobs = new HashSet<>();
    for (int i = 0; i < result.length(); i++)
      jobs.add(result.getJSONObject(i).getJSONObject("metric").getString("job"));
    return jobs;
  }

  private JSONArray query(final int serverIndex, final String promQL, final long atSeconds) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(serverIndex,
        "/api/v1/ts/graph/prom/api/v1/query?query=" + URLEncoder.encode(promQL, StandardCharsets.UTF_8) + "&time=" + atSeconds))
        .toURL().openConnection();
    try {
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", basicAuth());
      assertThat(connection.getResponseCode()).isEqualTo(200);
      try (final InputStream is = connection.getInputStream()) {
        final JSONObject response = new JSONObject(new String(is.readAllBytes(), StandardCharsets.UTF_8));
        assertThat(response.getString("status")).isEqualTo("success");
        return response.getJSONObject("data").getJSONArray("result");
      }
    } finally {
      connection.disconnect();
    }
  }

  private void postPromWrite(final int serverIndex, final WriteRequest writeRequest) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(serverIndex, "/api/v1/ts/graph/prom/write"))
        .toURL().openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", basicAuth());
      connection.setRequestProperty("Content-Type", "application/x-protobuf");
      connection.setRequestProperty("Content-Encoding", "snappy");
      connection.setDoOutput(true);
      try (final OutputStream os = connection.getOutputStream()) {
        os.write(Snappy.compress(writeRequest.encode()));
      }
      assertThat(connection.getResponseCode()).isEqualTo(204);
    } finally {
      connection.disconnect();
    }
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
