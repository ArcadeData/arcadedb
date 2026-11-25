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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.index.columnar.ColumnarStats;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12016. {@code POST /api/v1/query} and {@code /command} used to push the response cap down by appending
 * {@code limit <cap + 1>} to the text of every SELECT or MATCH without a LIMIT of its own, and the planner could not tell that LIMIT
 * from one the caller wrote: a {@code SELECT DISTINCT} stayed a single-threaded streaming DISTINCT instead of the parallel GROUP BY,
 * and a GROUP BY without ORDER BY was refused the columnar projection. The cap now reaches the engine as a bound on the rows the
 * caller reads, which stops the steps that hold rows and changes nothing about the plan.
 * <p>
 * The plans are read from the {@code explain} member of a response to a request with {@code profileExecution}.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class HttpResultRowCapIT extends BaseGraphServerTest {
  private static final int    DEFAULT_LIMIT = 50;
  private static final int    ROWS          = 12_000;
  private static final String TYPE_NAME     = "RowCapDoc";
  private static final String ASSET         = ColumnarTestData.TYPE;

  private final HttpClient client = HttpClient.newHttpClient();

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_HTTP_QUERY_DEFAULT_LIMIT, DEFAULT_LIMIT);
  }

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database database = getDatabase(0);
    // A TYPE OF 4 BUCKETS WITH ENOUGH RECORDS FOR THE PLANNER TO SCAN IT IN PARALLEL AND TO REWRITE A DISTINCT AS A GROUP BY
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.getSchema().createVertexType(TYPE_NAME, 4);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex(TYPE_NAME).set("id", i, "grp", i % 100, "x", (double) ((i * 7919L) % ROWS)).save();
    });

    final ColumnarTestData data = new ColumnarTestData(7);
    data.createSchema(database);
    data.insertAssets(database, 500);
    FacetOracle.awaitCaughtUp(database);
  }

  @Test
  void aDistinctIsTheParallelGroupByOverHttp() throws Exception {
    final JSONObject response = query("SELECT DISTINCT grp FROM " + TYPE_NAME, null);

    assertThat(response.getString("explain")).contains("CALCULATE AGGREGATE PROJECTIONS (parallel").doesNotContain("+ DISTINCT");
    // THE 100 DISTINCT VALUES ARE ABOVE THE CAP: THE GROUPS HELD ARE THE FIRST ONES MET, AND THE CAP IS REPORTED
    assertThat(response.getJSONArray("result").length()).isEqualTo(DEFAULT_LIMIT);
    assertThat(response.getBoolean("truncated")).isTrue();

    final JSONObject all = query("SELECT DISTINCT grp FROM " + TYPE_NAME, -1);
    assertThat(all.getString("explain")).contains("CALCULATE AGGREGATE PROJECTIONS (parallel");
    assertThat(all.getJSONArray("result").length()).isEqualTo(100);
    assertThat(all.getBoolean("truncated")).isFalse();
  }

  @Test
  void aColumnarGroupByIsTakenOverHttp() throws Exception {
    final ColumnarStats stats = ((LocalDatabase) ((DatabaseInternal) getServerDatabase(0, getDatabaseName())).getEmbedded())
        .getColumnarIndexManager().getStats();
    final long accelerated = stats.groupByAcceleratedCount();

    final JSONObject response = query("SELECT brand, count(*) AS c FROM " + ASSET + " GROUP BY brand", null);

    assertThat(response.getString("explain")).contains("AGGREGATE FROM COLUMNAR PROJECTION");
    assertThat(response.getJSONArray("result").length()).isGreaterThan(1);
    assertThat(stats.groupByAcceleratedCount()).isEqualTo(accelerated + 1);
  }

  @Test
  void aGroupByHoldsTheCapGroupsOnly() throws Exception {
    final JSONObject response = query("SELECT grp, count(*) AS c FROM " + TYPE_NAME + " GROUP BY grp", null);

    assertThat(response.getJSONArray("result").length()).isEqualTo(DEFAULT_LIMIT);
    assertThat(response.getInt("limit")).isEqualTo(DEFAULT_LIMIT);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void anOrderByKeepsOnlyTheRowsOfTheCapAndTheTruncationIsReported() throws Exception {
    final JSONObject response = query("SELECT FROM " + TYPE_NAME + " ORDER BY x, id", null);

    // ONE ROW ABOVE THE CAP: THE EXTRA ROW IS NEVER SERIALIZED, IT TELLS A TRUNCATED RESULT FROM ONE THAT ENDS AT THE CAP
    assertThat(response.getString("explain")).contains("buffer size: " + (DEFAULT_LIMIT + 1));
    final JSONArray rows = response.getJSONArray("result");
    assertThat(rows.length()).isEqualTo(DEFAULT_LIMIT);
    assertThat(response.getInt("returned")).isEqualTo(DEFAULT_LIMIT);
    assertThat(response.getBoolean("truncated")).isTrue();
    // THE FIRST ROWS OF THE ORDER: x = (id * 7919) % 12000 IS 0 FOR id = 0 AND FOR EVERY id THAT IS A MULTIPLE OF 12000
    assertThat(rows.getJSONObject(0).getInt("id")).isZero();

    final JSONObject expected = query("SELECT FROM " + TYPE_NAME + " ORDER BY x, id LIMIT " + DEFAULT_LIMIT, null);
    for (int i = 0; i < DEFAULT_LIMIT; i++)
      assertThat(rows.getJSONObject(i).getInt("id")).isEqualTo(expected.getJSONArray("result").getJSONObject(i).getInt("id"));
  }

  @Test
  void aSkipIsCountedBeforeTheCap() throws Exception {
    final JSONObject response = query("SELECT FROM " + TYPE_NAME + " ORDER BY x, id SKIP 5", null);

    assertThat(response.getString("explain")).contains("buffer size: " + (5 + DEFAULT_LIMIT + 1));
    assertThat(response.getJSONArray("result").length()).isEqualTo(DEFAULT_LIMIT);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void aResultThatEndsAtTheCapIsNotTruncated() throws Exception {
    final JSONObject response = query("SELECT FROM " + TYPE_NAME + " WHERE id < " + DEFAULT_LIMIT + " ORDER BY id", null);

    assertThat(response.getJSONArray("result").length()).isEqualTo(DEFAULT_LIMIT);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void theLimitOfTheRequestIsTheCap() throws Exception {
    final JSONObject response = query("SELECT FROM " + TYPE_NAME + " ORDER BY x, id", 7);

    assertThat(response.getString("explain")).contains("buffer size: 8");
    assertThat(response.getJSONArray("result").length()).isEqualTo(7);
    assertThat(response.getInt("limit")).isEqualTo(7);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void aStatementWithItsOwnLimitIsNotCapped() throws Exception {
    final JSONObject response = query("SELECT FROM " + TYPE_NAME + " ORDER BY x, id LIMIT 80", null);

    assertThat(response.getString("explain")).contains("buffer size: 80");
    // THE LIMIT OF THE STATEMENT IS WHAT THE CALLER ASKED TO READ: IT RAISES THE DEFAULT CAP
    assertThat(response.getJSONArray("result").length()).isEqualTo(80);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void aScriptCapsItsLastStatementOnly() throws Exception {
    final HttpResponse<String> response = send("command", new JSONObject().put("language", "sqlscript")
        .put("command", "LET a = SELECT FROM " + TYPE_NAME + " ORDER BY x; SELECT $a.size() AS n, id FROM " + TYPE_NAME + " ORDER BY x, id"));

    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    final JSONObject json = new JSONObject(response.body());
    assertThat(json.getJSONArray("result").length()).isEqualTo(DEFAULT_LIMIT);
    assertThat(json.getBoolean("truncated")).isTrue();
    // THE LET HELD EVERY ROW
    assertThat(json.getJSONArray("result").getJSONObject(0).getInt("n")).isEqualTo(ROWS);
  }

  @Test
  void theCapOfARequestDoesNotReachTheNextOne() throws Exception {
    query("SELECT FROM " + TYPE_NAME + " ORDER BY x, id", 7);
    // THE WORKER THREAD THAT SERVED A CAPPED REQUEST SERVES THE NEXT ONE: A COMMAND THAT IS NOT A QUERY LEAVES NOTHING BEHIND
    final HttpResponse<String> insert = send("command", new JSONObject().put("language", "sql")
        .put("command", "INSERT INTO " + TYPE_NAME + " SET id = -1, grp = 0, x = 0"));
    assertThat(insert.statusCode()).as(insert.body()).isEqualTo(200);

    final JSONObject unlimited = query("SELECT FROM " + TYPE_NAME + " ORDER BY x, id", -1);
    assertThat(unlimited.getJSONArray("result").length()).isGreaterThan(DEFAULT_LIMIT);
  }

  @Test
  void aQuotaBelowTheCapIsARealLimitOfTheStatement() throws Exception {
    // THE QUOTA CUTS THE ROWS AS A LIMIT OF THE STATEMENT: THE RESULT ENDS BELOW THE CAP, SO IT IS NOT A TRUNCATION BY THE CAP
    final JSONObject json = queryAsUserWithAQuotaOf(7);

    assertThat(json.getJSONArray("result").length()).isEqualTo(7);
    assertThat(json.getInt("limit")).isEqualTo(DEFAULT_LIMIT);
    assertThat(json.getBoolean("truncated")).isFalse();
  }

  @Test
  void aQuotaAboveTheCapDoesNotRaiseIt() throws Exception {
    final JSONObject json = queryAsUserWithAQuotaOf(200);

    assertThat(json.getJSONArray("result").length()).isEqualTo(DEFAULT_LIMIT);
    assertThat(json.getInt("limit")).isEqualTo(DEFAULT_LIMIT);
    assertThat(json.getBoolean("truncated")).isTrue();
  }

  /** The response to a query with no LIMIT, sent by a user whose group has the given {@code resultSetLimit}. */
  private JSONObject queryAsUserWithAQuotaOf(final int resultSetLimit) throws Exception {
    final String group = "issue12016capped" + resultSetLimit;
    final String user = "issue12016reader" + resultSetLimit;
    final String password = "issue12016pwd";
    final ServerSecurity security = getServer(0).getSecurity();
    security.saveGroup(getDatabaseName(), group, groupWithQuota(resultSetLimit));
    if (security.existsUser(user))
      security.dropUser(user);
    security.createUser(new JSONObject().put("name", user).put("password", security.encodePassword(password))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(group))));
    try {
      final HttpResponse<String> response = send("query", new JSONObject().put("language", "sql")
          .put("command", "SELECT FROM " + TYPE_NAME + " ORDER BY x, id"), user, password);
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      return new JSONObject(response.body());
    } finally {
      security.dropUser(user);
      security.deleteGroup(getDatabaseName(), group);
    }
  }

  private static JSONObject groupWithQuota(final int resultSetLimit) {
    return new JSONObject().put("resultSetLimit", resultSetLimit)
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))));
  }

  private JSONObject query(final String sql, final Integer limit) throws Exception {
    final JSONObject payload = new JSONObject().put("language", "sql").put("command", sql).put("profileExecution", "basic");
    if (limit != null)
      payload.put("limit", limit);
    final HttpResponse<String> response = send("query", payload);
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    return new JSONObject(response.body());
  }

  private HttpResponse<String> send(final String endpoint, final JSONObject payload) throws Exception {
    return send(endpoint, payload, "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private HttpResponse<String> send(final String endpoint, final JSONObject payload, final String user, final String password)
      throws Exception {
    final HttpRequest request = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/" + endpoint + "/" + getDatabaseName())))
        .header("Authorization", "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    return client.send(request, BodyHandlers.ofString());
  }
}
