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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9833: the label matchers other than {@code =} compared a typed tag's {@code toString()} with the matcher text, so on a
 * LONG tag holding 7 {@code {code="007"}} and {@code {code!="007"}} both selected the series and {@code {code=~"007"}} none. The
 * remote-read endpoint shares the evaluator's matcher logic, and the series endpoint decided every operator, {@code =}
 * included, on the text: both now read the matcher value in the tag's declared type, as {@code /query} does.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue9833PrometheusTypedTagMatchersTest extends BaseGraphServerTest {
  private static final long TS = 1_700_000_000_000L;

  private final HttpClient client = HttpClient.newHttpClient();

  @BeforeEach
  void createTypes() throws Exception {
    command("CREATE TIMESERIES TYPE disk TIMESTAMP ts TAGS (code LONG) FIELDS (value DOUBLE)");
    command("INSERT INTO disk SET ts = " + TS + ", code = 7, value = 4.0");
    command("INSERT INTO disk SET ts = " + TS + ", code = 8, value = 5.0");
    command("CREATE TIMESERIES TYPE ratio TIMESTAMP ts TAGS (code DOUBLE) FIELDS (value DOUBLE)");
    command("INSERT INTO ratio SET ts = " + TS + ", code = 5, value = 1.0");
  }

  @Test
  void remoteReadReadsEveryMatcherInTheTagsDeclaredType() throws Exception {
    assertThat(readCodes("disk", MatchType.EQ, "007")).containsExactly("7");
    assertThat(readCodes("disk", MatchType.NEQ, "007")).containsExactly("8");
    assertThat(readCodes("disk", MatchType.RE, "007")).containsExactly("7");
    assertThat(readCodes("disk", MatchType.NRE, "007")).containsExactly("8");

    assertThat(readCodes("ratio", MatchType.EQ, "5")).containsExactly("5.0");
    assertThat(readCodes("ratio", MatchType.NEQ, "5")).isEmpty();
    assertThat(readCodes("ratio", MatchType.RE, "5")).containsExactly("5.0");
    assertThat(readCodes("ratio", MatchType.NRE, "5")).isEmpty();

    // a real pattern is still matched against the label text the series is reported under
    assertThat(readCodes("disk", MatchType.RE, "7|9")).containsExactly("7");
  }

  @Test
  void seriesEndpointReadsEveryMatcherInTheTagsDeclaredType() throws Exception {
    assertThat(seriesCodes("disk{code=\"007\"}")).containsExactly("7");
    assertThat(seriesCodes("disk{code!=\"007\"}")).containsExactly("8");
    assertThat(seriesCodes("disk{code=~\"007\"}")).containsExactly("7");
    assertThat(seriesCodes("disk{code!~\"007\"}")).containsExactly("8");

    assertThat(seriesCodes("ratio{code=\"5\"}")).containsExactly("5.0");
    assertThat(seriesCodes("ratio{code!=\"5\"}")).isEmpty();

    assertThat(seriesCodes("disk{code=~\"7|9\"}")).containsExactly("7");
    assertThat(seriesCodes("disk{code=\"abc\"}")).as("text that is no number is no value of a LONG tag").isEmpty();
  }

  // ---------------------------------------------------------------------------------------------------
  // Plumbing
  // ---------------------------------------------------------------------------------------------------

  /** The {@code code} label of every series remote-read answers for {@code metric{code <op> value}}. */
  private List<String> readCodes(final String metric, final MatchType op, final String value) throws Exception {
    final ReadRequest request = new ReadRequest(List.of(new Query(0, TS + 100,
        List.of(new LabelMatcher(MatchType.EQ, "__name__", metric), new LabelMatcher(op, "code", value)))));
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        getServerHttpUrl(0, "/api/v1/ts/" + getDatabaseName() + "/prom/read")).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", basicAuth());
    connection.setRequestProperty("Content-Type", "application/x-protobuf");
    connection.setRequestProperty("Content-Encoding", "snappy");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(Snappy.compress(request.encode()));
    }
    assertThat(connection.getResponseCode()).isEqualTo(200);
    final ReadResponse response;
    try (final InputStream is = connection.getInputStream()) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      is.transferTo(baos);
      response = ReadResponse.decode(Snappy.uncompress(baos.toByteArray()));
    }
    return response.getResults().getFirst().getTimeSeries().stream()
        .flatMap(ts -> ts.getLabels().stream())
        .filter(label -> "code".equals(label.name()))
        .map(label -> label.value())
        .sorted()
        .toList();
  }

  /** The {@code code} label of every series {@code /series} answers for {@code selector}. */
  private List<String> seriesCodes(final String selector) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(0,
        "/api/v1/ts/" + getDatabaseName() + "/prom/api/v1/series?match[]=" + URLEncoder.encode(selector,
            StandardCharsets.UTF_8))).toURL().openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization", basicAuth());
    assertThat(connection.getResponseCode()).isEqualTo(200);
    final JSONObject response;
    try (final InputStream is = connection.getInputStream()) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      is.transferTo(baos);
      response = new JSONObject(baos.toString(StandardCharsets.UTF_8));
    }
    final JSONArray data = response.getJSONArray("data");
    final String[] codes = new String[data.length()];
    for (int i = 0; i < codes.length; i++)
      codes[i] = data.getJSONObject(i).getString("code");
    return Arrays.stream(codes).sorted().toList();
  }

  private void command(final String sql) throws Exception {
    final HttpResponse<String> response = client.send(HttpRequest.newBuilder()
        .uri(new URI(getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", "sql").put("command", sql).toString()))
        .setHeader("Content-Type", "application/json")
        .setHeader("Authorization", basicAuth())
        .build(), HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
