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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.index.columnar.ColumnarStats;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.facet.AdvancedFacetsFixture;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The plan 5 facet kinds over HTTP end to end (next to plan 4's {@link FacetsHttpIT}): {@code facet.searchValues} rows, AFTER paging
 * with the cursor the response returns, the JSON members of HIERARCHY, DISTINCT and STATS, GROUP BY acceleration answering what the
 * normal plan answers, and the date-time values of a STATS facet and of a cursor written in the database's date-time format, as a
 * row's.
 */
public class FacetAdvancedHttpIT extends BaseGraphServerTest {
  private static final String   TYPE    = AdvancedFacetsFixture.TYPE;
  private static final int      ASSETS  = 300;
  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    AdvancedFacetsFixture.load(getDatabase(0), 14L, ASSETS);
  }

  /** The server reopened the database: its projections are awaited, then a FACETS query over HTTP must report no lag. */
  @BeforeEach
  void awaitProjections() {
    FacetOracle.awaitCaughtUp(getServerDatabase(0, getDatabaseName()));
    // a hang detector, not a latency bound
    await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertThat(
        post("SELECT FROM " + TYPE + " FACETS b: brand TERMS TOP 1 LIMIT 1", null, false).getJSONObject("facets").getJSONObject("meta")
            .getLong("lagMs")).isZero());
  }

  @Test
  void searchValuesOverHttp() throws Exception {
    final JSONArray result = post("SELECT expand(facet.searchValues('Asset', 'tags', 't/1', 5))", null, false).getJSONArray("result");
    assertThat(result.length()).isBetween(1, 5);
    assertThat(result.getJSONObject(0).getString("value")).startsWith("t/1");
    assertThat(result.getJSONObject(0).getLong("count")).isGreaterThan(0);

    // the rows the embedded call returns, member for member: value, count, and no "exact" on an exact batch
    final List<String> embedded = new ArrayList<>();
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql",
        "SELECT expand(facet.searchValues('Asset', 'tags', 't/1', 5))")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        embedded.add(row.getProperty("value") + "=" + row.getProperty("count") + (row.hasProperty("exact") ? "/" + row.getProperty("exact") : ""));
      }
    }
    final List<String> remote = new ArrayList<>();
    for (int i = 0; i < result.length(); i++) {
      final JSONObject row = result.getJSONObject(i);
      assertThat(row.keySet()).isSubsetOf("value", "count", "exact");
      remote.add(row.getString("value") + "=" + row.getLong("count") + (row.has("exact") ? "/" + row.getBoolean("exact") : ""));
    }
    assertThat(remote).isEqualTo(embedded);
  }

  /** Every page AFTER the cursor the previous one returned: the union of the pages is the GROUP BY, the last cursor is JSON null. */
  @Test
  void afterCursorPagesOverHttp() throws Exception {
    final Map<String, Long> paged = new HashMap<>();
    Object cursor = null;
    JSONObject last = null;
    for (int page = 0; page < 1_000; page++) {
      final JSONObject facet;
      if (page == 0)
        facet = post("SELECT FROM " + TYPE + " FACETS b: brand TERMS TOP 3 ORDER BY VALUE LIMIT 1", null, false).getJSONObject("facets")
            .getJSONObject("b");
      else
        facet = post("SELECT FROM " + TYPE + " FACETS b: brand TERMS TOP 3 AFTER :c LIMIT 1", new JSONObject().put("c", cursor), false)
            .getJSONObject("facets").getJSONObject("b");
      assertThat(facet.has("after")).as("every VALUE-ordered page carries the member").isTrue();
      final JSONArray buckets = facet.getJSONArray("buckets");
      for (int i = 0; i < buckets.length(); i++) {
        final JSONObject bucket = buckets.getJSONObject(i);
        if (bucket.getBoolean("missing", false))
          continue;
        assertThat(paged.put(bucket.getString("value"), bucket.getLong("count"))).as("a value on two pages").isNull();
      }
      last = facet;
      if (facet.isNull("after"))
        break;
      cursor = facet.get("after");
      assertThat(cursor).isEqualTo(buckets.getJSONObject(buckets.length() - 1).get("value"));
    }
    assertThat(last).isNotNull();
    assertThat(last.isNull("after")).isTrue();

    final Map<String, Long> groupBy = new HashMap<>();
    final JSONArray rows = post("SELECT brand, count(*) AS c FROM " + TYPE + " GROUP BY brand", null, false).getJSONArray("result");
    for (int i = 0; i < rows.length(); i++)
      groupBy.put(rows.getJSONObject(i).getString("brand"), rows.getJSONObject(i).getLong("c"));
    assertThat(paged).isEqualTo(groupBy);
  }

  @Test
  void hierarchyDistinctStatsJsonShape() throws Exception {
    final JSONObject facets = post("SELECT FROM " + TYPE + AdvancedFacetsFixture.ASSET_FACETS + " LIMIT 1", null, false)
        .getJSONObject("facets");

    final JSONArray tree = facets.getJSONObject("t").getJSONArray("buckets");
    final Map<String, JSONObject> byValue = new HashMap<>();
    for (int i = 0; i < tree.length(); i++) {
      final JSONObject bucket = tree.getJSONObject(i);
      if (bucket.getBoolean("missing", false))
        continue;
      assertThat(bucket.keySet()).as(bucket.toString()).contains("value", "count", "parent", "depth");
      byValue.put(bucket.getString("value"), bucket);
    }
    assertThat(byValue.get("t/1").isNull("parent")).isTrue();
    assertThat(byValue.get("t/1").getInt("depth")).isZero();
    assertThat(byValue.get("t/1").has("orphan")).isFalse();
    assertThat(byValue.get("t/1/2").getString("parent")).isEqualTo("t/1");
    assertThat(byValue.get("t/1/2").getInt("depth")).isEqualTo(1);
    assertThat(byValue.get("t/1/2/3").getString("parent")).isEqualTo("t/1/2");
    assertThat(byValue.get("t/1/2/3").getInt("depth")).isEqualTo(2);
    for (final String orphan : AdvancedFacetsFixture.ORPHANS) {
      assertThat(byValue.get(orphan).getBoolean("orphan")).as(orphan).isTrue();
      assertThat(byValue.get(orphan).isNull("parent")).as(orphan).isTrue();
      assertThat(byValue.get(orphan).getInt("depth")).as(orphan).isZero();
    }

    final JSONObject distinct = facets.getJSONObject("d");
    assertThat(distinct.keySet()).containsExactlyInAnyOrder("type", "source", "value", "exact", "missing");
    assertThat(distinct.getBoolean("exact")).isTrue();
    assertThat(distinct.getLong("value")).isEqualTo(
        post("SELECT brand FROM " + TYPE + " GROUP BY brand", null, false).getJSONArray("result").length());

    final JSONObject stats = facets.getJSONObject("s");
    assertThat(stats.keySet()).containsExactlyInAnyOrder("type", "source", "count", "missing", "min", "max", "sum", "avg");
    final JSONObject row = post("SELECT count(fileSize) AS c, min(fileSize) AS mn, max(fileSize) AS mx, sum(fileSize) AS sm,"
        + " avg(fileSize) AS av FROM " + TYPE, null, false).getJSONArray("result").getJSONObject(0);
    assertThat(stats.getLong("count")).isEqualTo(row.getLong("c"));
    for (final String[] pair : new String[][] { { "min", "mn" }, { "max", "mx" }, { "sum", "sm" }, { "avg", "av" } })
      assertThat(stats.get(pair[0])).as(pair[0]).isEqualTo(row.get(pair[1]));
    // the whole member is what the embedded API writes, values encoded as the row encoder writes them
    for (final String alias : List.of("t", "d", "s"))
      assertThat(facets.getJSONObject(alias).toString()).as(alias)
          .isEqualTo(embeddedFacets(AdvancedFacetsFixture.ASSET_FACETS).getJSONObject(alias).toString());
  }

  @Test
  void groupByAccelerationOverHttp() throws Exception {
    final String sql = "SELECT brand, count(*) AS c FROM " + TYPE + " GROUP BY brand ORDER BY c DESC, brand";
    final ColumnarStats stats = ((LocalDatabase) ((DatabaseInternal) getServerDatabase(0, getDatabaseName())).getEmbedded())
        .getColumnarIndexManager().getStats();

    final long before = stats.groupByAcceleratedCount();
    final JSONArray accelerated = post(sql, null, false).getJSONArray("result");
    assertThat(stats.groupByAcceleratedCount()).as("the statement was answered from the projection").isGreaterThan(before);

    post("ALTER DATABASE `arcadedb.columnar.groupByAcceleration` false", null, false);
    try {
      final long off = stats.groupByAcceleratedCount();
      final JSONArray normal = post(sql, null, false).getJSONArray("result");
      assertThat(stats.groupByAcceleratedCount()).as("the switch is read at execution time").isEqualTo(off);
      assertThat(accelerated.length()).isGreaterThan(1);
      assertThat(accelerated.toString()).isEqualTo(normal.toString());
    } finally {
      post("ALTER DATABASE `arcadedb.columnar.groupByAcceleration` true", null, false);
    }
  }

  /**
   * Final review C-I8 for the plan 5 members: a STATS facet's min and max over a DATETIME property and the cursor of a DATETIME terms
   * facet are written in the database's date-time format, exactly as a row's value of the property, and that text, echoed as AFTER,
   * starts the next page after it.
   */
  @Test
  void statsDatesAndCursorUseTheRowEncoding() throws Exception {
    final JSONObject facets = post("SELECT FROM " + TYPE + " FACETS p: publishDate STATS, pd: publishDate TERMS TOP 3 ORDER BY VALUE LIMIT 1",
        null, true).getJSONObject("facets");
    // the distinct values in order, as rows of the property write them: the column's date-time format and precision (an aggregate or
    // GROUP BY row, which records no property type, writes the schema's format without the milliseconds)
    final List<Object> ordered = new ArrayList<>();
    final JSONArray values = post("SELECT publishDate AS v FROM " + TYPE + " ORDER BY v", null, false).getJSONArray("result");
    for (int i = 0; i < values.length(); i++)
      if (ordered.isEmpty() || !ordered.getLast().equals(values.getJSONObject(i).get("v")))
        ordered.add(values.getJSONObject(i).get("v"));
    assertThat(ordered.size()).isGreaterThan(6);

    final JSONObject stats = facets.getJSONObject("p");
    assertThat(stats.get("min")).isInstanceOf(String.class);
    assertThat(stats.get("min")).isEqualTo(ordered.getFirst());
    assertThat(stats.get("max")).isEqualTo(ordered.getLast());
    assertThat(stats.isNull("sum")).isTrue();
    assertThat(stats.isNull("avg")).isTrue();
    // with type hints the client is told the values are DATETIME, as a row's column is
    assertThat(stats.getString("@props")).isEqualTo("value:" + Type.DATETIME.getId());
    final JSONObject first = facets.getJSONObject("pd");
    assertThat(first.getJSONArray("buckets").length()).isEqualTo(3);
    assertThat(bucketValues(first)).containsExactlyElementsOf(ordered.subList(0, 3));
    assertThat(first.get("after")).isEqualTo(ordered.get(2));

    // the text as received is a cursor: the next page starts strictly after it
    final JSONObject second = post("SELECT FROM " + TYPE + " FACETS pd: publishDate TERMS TOP 3 AFTER :c LIMIT 1",
        new JSONObject().put("c", first.get("after")), false).getJSONObject("facets").getJSONObject("pd");
    assertThat(bucketValues(second)).containsExactlyElementsOf(ordered.subList(3, 6));
    assertThat(second.get("after")).isEqualTo(ordered.get(5));
  }

  private static List<Object> bucketValues(final JSONObject facet) {
    final List<Object> values = new ArrayList<>();
    final JSONArray buckets = facet.getJSONArray("buckets");
    for (int i = 0; i < buckets.length(); i++)
      if (!buckets.getJSONObject(i).getBoolean("missing", false))
        values.add(buckets.getJSONObject(i).get("value"));
    return values;
  }

  // ------------------------------------------------------------------------------------------------------------------------------

  /** The facets the embedded API returns for {@code SELECT FROM Asset<facets>}, written as the HTTP serializer writes them. */
  private JSONObject embeddedFacets(final String facetsClause) {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "SELECT FROM " + TYPE + facetsClause + " LIMIT 1")) {
      return new JSONObject(JsonSerializer.createJsonSerializer()
          .serializeFacets(getServerDatabase(0, getDatabaseName()), rs.getFacets()).toString());
    }
  }

  private JSONObject post(final String sql, final JSONObject params, final boolean typeHints) throws Exception {
    final JSONObject payload = new JSONObject().put("language", "sql").put("command", sql);
    if (params != null)
      payload.put("params", params);
    if (typeHints)
      payload.put("typeHints", true);
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())))
        .timeout(TIMEOUT)
        .header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    final HttpResponse<String> response = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build()
        .send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    return new JSONObject(response.body());
  }
}
