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
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.LabelMatcher;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.MatchType;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Query;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadRequest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadResponse;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import java.io.ByteArrayOutputStream;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code POST /api/v1/ts/{db}/prom/read}: how the Prometheus remote-read endpoint groups rows into series and applies
 * the label matchers.
 */
class PrometheusRemoteReadTest extends BaseGraphServerTest {
  // Regression: #9623 two distinct label sets must not spell one series key
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
  @Nested
  class SeriesKey {
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

  // Regression: #9803 every label matcher is applied, not only the equality ones
  /**
   * Issue #9803: {@code POST /api/v1/ts/{db}/prom/read} built its tag filter from the {@code =} matchers only and dropped
   * every other one, so {@code {host!="bad"}} and {@code {host=~"go.*"}} answered the very series they exclude - which
   * Prometheus does not filter again - and a {@code __name__} matcher that was not an equality answered no series at all,
   * the answer for a metric that does not exist. The endpoint now applies the engine's own PromQL matcher rules, the ones
   * {@code /query} and {@code /series} apply.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LabelMatchers {
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

  // Regression: #9833 a matcher value is read in the tag's declared type
  /**
   * Issue #9833: the label matchers other than {@code =} compared a typed tag's {@code toString()} with the matcher text, so on a
   * LONG tag holding 7 {@code {code="007"}} and {@code {code!="007"}} both selected the series and {@code {code=~"007"}} none. The
   * remote-read endpoint shares the evaluator's matcher logic, and the series endpoint decided every operator, {@code =}
   * included, on the text: both now read the matcher value in the tag's declared type, as {@code /query} does.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class TypedTagMatchers {
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
}
