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
package com.arcadedb.server.http;

import com.arcadedb.database.Database;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * FACETS over HTTP: the buffered envelope carries the facets as its {@code facets} member, the ndjson stream carries them in its
 * {@code stats} trailer, each value written as a row's value of the same property, and neither the HTTP auto-limit nor the request
 * {@code limit} shrinks the facets, which count every match of the WHERE. A query without FACETS has no {@code facets} member and no
 * extra line.
 */
public class FacetsHttpIT extends BaseGraphServerTest {
  private static final String   TYPE    = ColumnarTestData.TYPE;
  private static final int      RECORDS = 2_000;
  private static final String   NDJSON  = "application/x-ndjson";
  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final String   QUERY   = "SELECT FROM " + TYPE + " WHERE fileSize > 0 FACETS brand TERMS TOP 5 LIMIT 3";

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database db = getDatabase(0);
    final ColumnarTestData data = new ColumnarTestData(7);
    data.createSchema(db);
    data.insertAssets(db, RECORDS);
    FacetOracle.awaitCaughtUp(db);
  }

  /** The server reopened the database: its projections are awaited again before a test compares facets. */
  @BeforeEach
  void awaitProjections() {
    FacetOracle.awaitCaughtUp(getServerDatabase(0, getDatabaseName()));
  }

  @Test
  void bufferedResponseCarriesFacets() throws Exception {
    final JSONObject response = post(QUERY, null, null, null);
    assertThat(response.getJSONArray("result").length()).isEqualTo(3);
    final JSONObject facets = response.getJSONObject("facets");
    assertThat(facets.getJSONObject("meta").getBoolean("exact")).isTrue();
    assertThat(facets.getJSONObject("brand").toString()).isEqualTo(embeddedFacets(QUERY).getJSONObject("brand").toString());
    assertThat(matches(facets.getJSONObject("brand"))).isEqualTo(embeddedCount("fileSize > 0"));
  }

  /** No LIMIT in the text: the handler appends one above its cap, yet the facets count all 2,000 records and the limits describe rows. */
  @Test
  void autoLimitDoesNotShrinkFacets() throws Exception {
    final String sql = "SELECT FROM " + TYPE + " FACETS brand TERMS TOP 5";
    final JSONObject response = post(sql, 10, null, null);
    assertThat(response.getJSONArray("result").length()).isEqualTo(10);
    assertThat(response.getInt("returned")).isEqualTo(10);
    assertThat(response.getBoolean("truncated")).isTrue();
    assertThat(matches(response.getJSONObject("facets").getJSONObject("brand"))).isEqualTo(RECORDS);
  }

  @Test
  void requestLimitAndPlanLimit() throws Exception {
    final String sql = "SELECT FROM " + TYPE + " WHERE fileSize > 0 FACETS brand TERMS TOP 5 LIMIT 5";
    final JSONObject response = post(sql, 2, null, null);
    assertThat(response.getJSONArray("result").length()).isEqualTo(2);
    assertThat(response.getInt("returned")).isEqualTo(2);
    final JSONObject brand = response.getJSONObject("facets").getJSONObject("brand");
    assertThat(matches(brand)).isEqualTo(embeddedCount("fileSize > 0"));
    assertThat(brand.toString()).isEqualTo(embeddedFacets(sql).getJSONObject("brand").toString());
  }

  /** Final review B-M2: the facets ride in the stats trailer, so a client that predates them meets no event it does not know. */
  @Test
  void ndjsonFacetsRideInTheStatsTrailer() throws Exception {
    final List<JSONObject> lines = postStream(QUERY);
    assertThat(lines).hasSize(4);
    for (int i = 0; i < 3; i++)
      assertThat(lines.get(i).keySet()).containsExactly("record");
    assertThat(lines.get(3).keySet()).containsExactly("stats");
    final JSONObject stats = lines.get(3).getJSONObject("stats");
    assertThat(stats.getInt("returned")).isEqualTo(3);
    assertThat(stats.getJSONObject("facets").getJSONObject("brand").toString())
        .isEqualTo(embeddedFacets(QUERY).getJSONObject("brand").toString());
  }

  /** The stream cut at its cap still counts every match, in the trailer that reports the cut. */
  @Test
  void ndjsonAutoLimitKeepsFacetsComplete() throws Exception {
    final List<JSONObject> lines = postStream("SELECT FROM " + TYPE + " FACETS brand TERMS TOP 5", 4);
    assertThat(lines).hasSize(5);
    final JSONObject stats = lines.get(4).getJSONObject("stats");
    assertThat(matches(stats.getJSONObject("facets").getJSONObject("brand"))).isEqualTo(RECORDS);
    assertThat(stats.getBoolean("truncated")).isTrue();
  }

  /**
   * Final review C-I8: a DATE, a DATETIME, a DECIMAL and a NaN facet value are written as a row's value of the same property, on the
   * buffered and the streamed encoding: a client reading the JSON finds the same text or number for the same value.
   */
  @Test
  void facetValuesAreWrittenAsRowValues() throws Exception {
    createTypedValues(getServerDatabase(0, getDatabaseName()));
    final String facets = "SELECT FROM Typed FACETS d: day TERMS, a: at TERMS, p: price TERMS, s: score TERMS";
    final JSONObject rows = post("SELECT day AS d, at AS a, price AS p, score AS s FROM Typed", null, null, null);
    final JSONObject buffered = post(facets, null, null, null).getJSONObject("facets");
    final List<JSONObject> lines = postStream(facets);
    final JSONObject streamed = lines.getLast().getJSONObject("stats").getJSONObject("facets");
    for (final String alias : List.of("d", "a", "p", "s")) {
      final Set<Object> rowValues = new HashSet<>();
      final JSONArray result = rows.getJSONArray("result");
      for (int i = 0; i < result.length(); i++)
        rowValues.add(result.getJSONObject(i).opt(alias));
      assertThat(termValues(buffered.getJSONObject(alias))).as(alias).isEqualTo(rowValues);
      assertThat(termValues(streamed.getJSONObject(alias))).as(alias).isEqualTo(rowValues);
    }
    // what a row of a date property is: the schema's date format, not an epoch number
    assertThat(termValues(buffered.getJSONObject("d"))).containsExactly("2024-02-29");
  }

  /** {@code day DATE}, {@code at DATETIME} and {@code score DOUBLE} with COLUMNAR indexes, {@code price DECIMAL} without (scanned). */
  public static void createTypedValues(final Database db) {
    db.command("sql", "CREATE DOCUMENT TYPE Typed BUCKETS 1");
    db.command("sql", "CREATE PROPERTY Typed.day DATE");
    db.command("sql", "CREATE PROPERTY Typed.at DATETIME");
    db.command("sql", "CREATE PROPERTY Typed.price DECIMAL");
    db.command("sql", "CREATE PROPERTY Typed.score DOUBLE");
    for (final String property : List.of("day", "at", "score"))
      db.command("sql", "CREATE INDEX ON Typed (" + property + ") COLUMNAR");
    db.transaction(() -> {
      for (int i = 0; i < 3; i++)
        db.newDocument("Typed").set("day", LocalDate.of(2024, 2, 29)).set("at", LocalDateTime.of(2024, 2, 29, 10, 30, 15))
            .set("price", new BigDecimal("12.5")).set("score", i == 0 ? Double.NaN : 1.25).save();
    });
    FacetOracle.awaitCaughtUp(db);
  }

  private static Set<Object> termValues(final JSONObject facet) {
    final Set<Object> values = new HashSet<>();
    final JSONArray buckets = facet.getJSONArray("buckets");
    for (int i = 0; i < buckets.length(); i++)
      if (!buckets.getJSONObject(i).getBoolean("missing", false))
        values.add(buckets.getJSONObject(i).opt("value"));
    return values;
  }

  @Test
  void detailedProfileKeepsFacets() throws Exception {
    final JSONObject response = post(QUERY, null, null, "detailed");
    assertThat(response.getJSONArray("result").length()).isEqualTo(3);
    assertThat(response.has("profile")).isTrue();
    assertThat(response.getJSONObject("facets").getJSONObject("brand").toString())
        .isEqualTo(embeddedFacets(QUERY).getJSONObject("brand").toString());

    // the profiler materializes only the rows the cap allows: the facets still count every record
    final JSONObject capped = post("SELECT FROM " + TYPE + " FACETS brand TERMS TOP 5", 5, null, "detailed");
    assertThat(capped.getJSONArray("result").length()).isEqualTo(5);
    assertThat(capped.getBoolean("truncated")).isTrue();
    assertThat(matches(capped.getJSONObject("facets").getJSONObject("brand"))).isEqualTo(RECORDS);
  }

  @Test
  void getQueryEndpointCarriesFacets() throws Exception {
    final HttpResponse<String> response = send(getRequest(QUERY, null));
    assertThat(response.statusCode()).isEqualTo(200);
    final JSONObject body = new JSONObject(response.body());
    assertThat(body.getJSONArray("result").length()).isEqualTo(3);
    assertThat(body.getJSONObject("facets").getJSONObject("brand").toString())
        .isEqualTo(embeddedFacets(QUERY).getJSONObject("brand").toString());

    final List<JSONObject> lines = readEvents(newClient().send(getRequest(QUERY, NDJSON), HttpResponse.BodyHandlers.ofInputStream()));
    assertThat(lines).hasSize(4);
    assertThat(lines.get(3).keySet()).containsExactly("stats");
    assertThat(lines.get(3).getJSONObject("stats").has("facets")).isTrue();
  }

  /** Without FACETS every transport answers exactly what it answered before: no facets member, no facets line. */
  @Test
  void noFacetsMemberWithoutClause() throws Exception {
    final String sql = "SELECT FROM " + TYPE + " WHERE fileSize > 0 LIMIT 3";
    final JSONObject buffered = post(sql, null, null, null);
    assertThat(buffered.getJSONArray("result").length()).isEqualTo(3);
    assertThat(buffered.has("facets")).isFalse();
    assertThat(buffered.keySet()).containsExactlyInAnyOrder("user", "result", "limit", "returned", "truncated");

    final List<JSONObject> lines = postStream(sql);
    assertThat(lines).hasSize(4);
    for (final JSONObject line : lines)
      assertThat(line.keySet()).doesNotContain("facets");
    assertThat(lines.getLast().getJSONObject("stats").keySet()).containsExactly("limit", "returned", "truncated");

    final JSONObject viaGet = new JSONObject(send(getRequest(sql, null)).body());
    assertThat(viaGet.has("facets")).isFalse();
  }

  /**
   * Byte level: the body of a query without FACETS is the body of the same query with FACETS minus its facets member, so the
   * member is the only thing this feature adds, and only to a FACETS query.
   */
  @Test
  void bodyWithoutFacetsIsTheFacetsBodyMinusTheMember() throws Exception {
    final HttpResponse<String> plain = send(postRequest("SELECT FROM " + TYPE + " WHERE fileSize > 0 LIMIT 3", null, null, null, null));
    final HttpResponse<String> faceted = send(postRequest(QUERY, null, null, null, null));
    assertThat(plain.statusCode()).isEqualTo(200);
    assertThat(faceted.statusCode()).isEqualTo(200);
    assertThat(plain.body()).doesNotContain("\"facets\"");
    assertThat(plain.body()).isEqualTo(withoutMember(faceted.body(), "facets"));
  }

  @Test
  void parseErrorsAre400() throws Exception {
    final HttpResponse<String> response = send(postRequest("SELECT FROM " + TYPE + " LIMIT 3 FACETS brand TERMS", null, null, null, null));
    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("FACETS must come before SKIP and LIMIT");
  }

  @Test
  void graphSerializerKeepsFacets() throws Exception {
    final JSONObject response = post(QUERY, null, "graph", null);
    assertThat(response.getJSONObject("facets").getJSONObject("brand").toString())
        .isEqualTo(embeddedFacets(QUERY).getJSONObject("brand").toString());
  }

  // ------------------------------------------------------------------------------------------------------------------------------

  private JSONObject embeddedFacets(final String sql) {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", sql)) {
      return rs.getFacets().toJSON();
    }
  }

  private long embeddedCount(final String where) {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "SELECT count(*) AS c FROM " + TYPE + " WHERE " + where)) {
      final long count = rs.next().<Number>getProperty("c").longValue();
      assertThat(count).isGreaterThan(0L);
      return count;
    }
  }

  /** The raw text with the top-level member {@code ,"name":{...}} cut out, matching braces outside string literals. */
  private static String withoutMember(final String body, final String name) {
    final int start = body.indexOf(",\"" + name + "\":{");
    assertThat(start).as(body).isGreaterThan(0);
    int depth = 0;
    boolean inString = false;
    for (int i = body.indexOf('{', start); i < body.length(); i++) {
      final char c = body.charAt(i);
      if (inString) {
        if (c == '\\')
          i++;
        else if (c == '"')
          inString = false;
      } else if (c == '"')
        inString = true;
      else if (c == '{')
        depth++;
      else if (c == '}' && --depth == 0)
        return body.substring(0, start) + body.substring(i + 1);
    }
    throw new AssertionError("unbalanced member '" + name + "' in " + body);
  }

  /** Every record the facet saw: the returned buckets, the values not returned and the records without a value. */
  private static long matches(final JSONObject facet) {
    long sum = facet.getLong("otherCount", 0L) + facet.getLong("missing", 0L);
    final JSONArray buckets = facet.getJSONArray("buckets");
    for (int i = 0; i < buckets.length(); i++)
      if (!buckets.getJSONObject(i).getBoolean("missing", false))
        sum += buckets.getJSONObject(i).getLong("count");
    return sum;
  }

  private JSONObject post(final String sql, final Integer limit, final String serializer, final String profile) throws Exception {
    final HttpResponse<String> response = send(postRequest(sql, null, limit, serializer, profile));
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    return new JSONObject(response.body());
  }

  private List<JSONObject> postStream(final String sql) throws Exception {
    return postStream(sql, null);
  }

  private List<JSONObject> postStream(final String sql, final Integer limit) throws Exception {
    return readEvents(newClient().send(postRequest(sql, NDJSON, limit, null, null), HttpResponse.BodyHandlers.ofInputStream()));
  }

  private HttpRequest postRequest(final String sql, final String accept, final Integer limit, final String serializer,
      final String profile) {
    final JSONObject payload = new JSONObject().put("language", "sql").put("command", sql);
    if (limit != null)
      payload.put("limit", limit);
    if (serializer != null)
      payload.put("serializer", serializer);
    if (profile != null)
      payload.put("profileExecution", profile);
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())))
        .timeout(TIMEOUT)
        .header("Authorization", authorization())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()));
    if (accept != null)
      builder.header("Accept", accept);
    return builder.build();
  }

  private HttpRequest getRequest(final String sql, final String accept) {
    // a path segment: a space is %20, not the '+' URLEncoder produces for a form
    final String url = getServerHttpUrl(0, "/api/v1/query/" + getDatabaseName() + "/sql/"
        + URLEncoder.encode(sql, StandardCharsets.UTF_8).replace("+", "%20"));
    final HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(url)).timeout(TIMEOUT)
        .header("Authorization", authorization()).GET();
    if (accept != null)
      builder.header("Accept", accept);
    return builder.build();
  }

  private static List<JSONObject> readEvents(final HttpResponse<InputStream> response) throws Exception {
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Content-Type").orElse("")).contains(NDJSON);
    final List<JSONObject> events = new ArrayList<>();
    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null)
        if (!line.isBlank())
          events.add(new JSONObject(line));
    }
    return events;
  }

  private static HttpResponse<String> send(final HttpRequest request) throws Exception {
    return newClient().send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static HttpClient newClient() {
    return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
  }

  private static String authorization() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
