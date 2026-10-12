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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9803: {@code POST /api/v1/ts/{db}/prom/read} built its tag filter from the {@code =} matchers only and dropped
 * every other one, so {@code {host!="bad"}} and {@code {host=~"go.*"}} answered the very series they exclude - which
 * Prometheus does not filter again - and a {@code __name__} matcher that was not an equality answered no series at all,
 * the answer for a metric that does not exist. The endpoint now applies the engine's own PromQL matcher rules, the ones
 * {@code /query} and {@code /series} apply.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9803PrometheusReadMatchersTest extends BaseGraphServerTest {
  private static final long TS = 1_700_000_000_000L;

  private final HttpClient client = HttpClient.newHttpClient();

  @BeforeEach
  void createTypes() throws Exception {
    command("CREATE TIMESERIES TYPE cpu TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    command("CREATE TIMESERIES TYPE mem TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    command("CREATE TIMESERIES TYPE disk TIMESTAMP ts TAGS (rack LONG) FIELDS (value DOUBLE)");
    command("INSERT INTO cpu SET ts = " + TS + ", host = 'good', value = 1.0");
    command("INSERT INTO cpu SET ts = " + TS + ", host = 'bad', value = 2.0");
    command("INSERT INTO mem SET ts = " + TS + ", host = 'good', value = 3.0");
    command("INSERT INTO disk SET ts = " + TS + ", rack = 7, value = 4.0");
    command("INSERT INTO disk SET ts = " + TS + ", rack = 8, value = 5.0");
  }

  @Test
  void aNotEqualMatcherExcludesTheSeriesItNames() throws Exception {
    assertThat(read(eq("__name__", "cpu"), new LabelMatcher(MatchType.NEQ, "host", "bad"))).containsOnlyKeys("cpu{host=good}");
  }

  @Test
  void aRegexMatcherKeepsOnlyTheSeriesItMatches() throws Exception {
    assertThat(read(eq("__name__", "cpu"), new LabelMatcher(MatchType.RE, "host", "go.*"))).containsOnlyKeys("cpu{host=good}");
    // anchored, as in Prometheus: "o" alone matches neither value
    assertThat(read(eq("__name__", "cpu"), new LabelMatcher(MatchType.RE, "host", "o"))).isEmpty();
  }

  @Test
  void aNegatedRegexMatcherDropsTheSeriesItMatches() throws Exception {
    assertThat(read(eq("__name__", "cpu"), new LabelMatcher(MatchType.NRE, "host", "go.*"))).containsOnlyKeys("cpu{host=bad}");
  }

  @Test
  void anEqualityMatcherStillNarrowsTheScan() throws Exception {
    final Map<String, List<Sample>> series = read(eq("__name__", "cpu"), eq("host", "good"));
    assertThat(series).containsOnlyKeys("cpu{host=good}");
    assertThat(series.get("cpu{host=good}")).containsExactly(new Sample(1.0, TS));
  }

  @Test
  void anEqualityOnATypedTagIsCoercedToTheColumnType() throws Exception {
    assertThat(read(eq("__name__", "disk"), eq("rack", "7"))).containsOnlyKeys("disk{rack=7}");
    assertThat(read(eq("__name__", "disk"), new LabelMatcher(MatchType.NEQ, "rack", "7"))).containsOnlyKeys("disk{rack=8}");
    assertThat(read(eq("__name__", "disk"), eq("rack", "abc"))).as("text that is no number is no value of a LONG tag").isEmpty();
  }

  @Test
  void anEqualityOnABooleanTagMatchesOnlyABooleanText() throws Exception {
    command("CREATE TIMESERIES TYPE probe TIMESTAMP ts TAGS (up BOOLEAN) FIELDS (value DOUBLE)");
    command("INSERT INTO probe SET ts = " + TS + ", up = true, value = 1.0");
    command("INSERT INTO probe SET ts = " + TS + ", up = false, value = 0.0");

    assertThat(read(eq("__name__", "probe"), eq("up", "true"))).containsOnlyKeys("probe{up=true}");
    assertThat(read(eq("__name__", "probe"), eq("up", "FALSE"))).containsOnlyKeys("probe{up=false}");
    // text that is not a boolean is no value of the tag: it used to be coerced to false and select those series
    assertThat(read(eq("__name__", "probe"), eq("up", "yes"))).isEmpty();
  }

  @Test
  void aRegexMetricNameSelectsEveryMetricItMatches() throws Exception {
    assertThat(read(new LabelMatcher(MatchType.RE, "__name__", "cp.*"))).containsOnlyKeys("cpu{host=good}", "cpu{host=bad}");
    assertThat(read(new LabelMatcher(MatchType.RE, "__name__", "cpu|mem"), eq("host", "good")))
        .containsOnlyKeys("cpu{host=good}", "mem{host=good}");
    assertThat(read(new LabelMatcher(MatchType.NEQ, "__name__", "cpu"), eq("host", "good"))).containsOnlyKeys("mem{host=good}");
  }

  @Test
  void aLabelTheTypeDoesNotDeclareFollowsTheAbsentLabelRules() throws Exception {
    assertThat(read(eq("__name__", "cpu"), eq("zone", "eu"))).as("an absent label equals no non-empty value").isEmpty();
    assertThat(read(eq("__name__", "cpu"), new LabelMatcher(MatchType.NEQ, "zone", "eu"))).as("and differs from every one")
        .containsOnlyKeys("cpu{host=good}", "cpu{host=bad}");
  }

  @Test
  void anUnknownMetricIsStillAnEmptyAnswer() throws Exception {
    assertThat(read(eq("__name__", "nope"))).isEmpty();
    assertThat(read(new LabelMatcher(MatchType.RE, "__name__", "nope.*"))).isEmpty();
  }

  @Test
  void aMalformedOrReDoSRegexIsRefusedRatherThanAnsweredEmpty() throws Exception {
    assertThat(status(eq("__name__", "cpu"), new LabelMatcher(MatchType.RE, "host", "(a+)+"))).isEqualTo(400);
    assertThat(status(eq("__name__", "cpu"), new LabelMatcher(MatchType.RE, "host", "[unclosed"))).isEqualTo(400);
    assertThat(status(new LabelMatcher(MatchType.RE, "__name__", "(a+)+"))).isEqualTo(400);
  }

  @Test
  void theSamplesOfASeriesAreInTimestampOrder() throws Exception {
    // spread over the shards by the round-robin routing, then collected shard by shard
    for (int i = 9; i >= 1; i--)
      command("INSERT INTO cpu SET ts = " + (TS + i) + ", host = 'good', value = " + i);
    final List<Sample> samples = read(eq("__name__", "cpu"), eq("host", "good")).get("cpu{host=good}");
    assertThat(samples).hasSize(10);
    for (int i = 1; i < samples.size(); i++)
      assertThat(samples.get(i).timestampMs()).isGreaterThan(samples.get(i - 1).timestampMs());
  }

  // ---------------------------------------------------------------------------------------------------
  // Plumbing
  // ---------------------------------------------------------------------------------------------------

  private static LabelMatcher eq(final String name, final String value) {
    return new LabelMatcher(MatchType.EQ, name, value);
  }

  /** The series answered, by {@code metric{label=value,...}}. */
  private Map<String, List<Sample>> read(final LabelMatcher... matchers) throws Exception {
    final HttpURLConnection connection = post(matchers);
    assertThat(connection.getResponseCode()).isEqualTo(200);
    final ReadResponse response;
    try (final InputStream is = connection.getInputStream()) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      is.transferTo(baos);
      response = ReadResponse.decode(Snappy.uncompress(baos.toByteArray()));
    }
    assertThat(response.getResults()).hasSize(1);

    final Map<String, List<Sample>> result = new LinkedHashMap<>();
    for (final TimeSeries ts : response.getResults().getFirst().getTimeSeries()) {
      String metric = null;
      final List<String> labels = new ArrayList<>();
      for (final Label label : ts.getLabels())
        if ("__name__".equals(label.name()))
          metric = label.value();
        else
          labels.add(label.name() + "=" + label.value());
      final String key = metric + "{" + String.join(",", labels) + "}";
      assertThat(result.put(key, ts.getSamples())).as("series %s reported twice", key).isNull();
    }
    return result;
  }

  private int status(final LabelMatcher... matchers) throws Exception {
    return post(matchers).getResponseCode();
  }

  private HttpURLConnection post(final LabelMatcher... matchers) throws Exception {
    final ReadRequest request = new ReadRequest(List.of(new Query(0, TS + 100, List.of(matchers))));
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        getServerHttpUrl(0, "/api/v1/ts/" + getDatabaseName() + "/prom/read")).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", basicAuth());
    connection.setRequestProperty("Content-Type", "application/x-protobuf");
    connection.setRequestProperty("Content-Encoding", "snappy");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(Snappy.compress(request.encode()));
      os.flush();
    }
    return connection;
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
