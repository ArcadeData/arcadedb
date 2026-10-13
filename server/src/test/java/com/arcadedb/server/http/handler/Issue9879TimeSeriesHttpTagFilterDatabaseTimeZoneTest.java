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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9879: the HTTP time-series endpoints read a DATETIME tag value given as text in the JVM's default time zone, while the
 * {@code INSERT} that wrote the tag read it in the database's. On a JVM whose zone is not the database's, the {@code tags} object
 * of {@code POST /ts/{database}/query} and of the Grafana query, and the {@code tag=name:value} parameter of
 * {@code GET /ts/{database}/latest}, named another epoch and selected nothing.
 * <p>
 * The database captures its zone when the server opens it; the JVM default is moved afterwards, to a zone with an offset on the
 * stored date, so the two differ wherever the test runs.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue9879TimeSeriesHttpTagFilterDatabaseTimeZoneTest extends BaseGraphServerTest {
  private static final String STORED = "2024-01-01 00:00:00";

  private final HttpClient client = HttpClient.newHttpClient();
  private       TimeZone   jvmZone;

  @BeforeEach
  void createTypeAndMoveTheJvmZone() throws Exception {
    jvmZone = TimeZone.getDefault();
    final TimeZone databaseZone = getServerDatabase(0, getDatabaseName()).getSchema().getTimeZone();
    TimeZone.setDefault(TimeZone.getTimeZone(databaseZone.getRawOffset() == 9 * 3_600_000 ? "America/New_York" : "Asia/Tokyo"));

    command("CREATE TIMESERIES TYPE ev9879 TIMESTAMP ts TAGS (code DATETIME) FIELDS (value DOUBLE)");
    command("INSERT INTO ev9879 SET ts = 1000, code = '" + STORED + "', value = 4.0");
  }

  @AfterEach
  void restoreTheJvmZone() {
    TimeZone.setDefault(jvmZone);
  }

  @Test
  void queryTagsObjectSelectsTheRowTheInsertWrote() throws Exception {
    final JSONObject request = new JSONObject().put("type", "ev9879").put("tags", new JSONObject().put("code", STORED));
    final JSONObject result = new JSONObject(send("/api/v1/ts/" + getDatabaseName() + "/query", request));
    assertThat(result.getInt("count")).isEqualTo(1);

    request.put("tags", new JSONObject().put("code", "2025-06-01 00:00:00"));
    assertThat(new JSONObject(send("/api/v1/ts/" + getDatabaseName() + "/query", request)).getInt("count")).isZero();
  }

  @Test
  void latestTagParameterSelectsTheRowTheInsertWrote() throws Exception {
    final JSONObject result = new JSONObject(send("/api/v1/ts/" + getDatabaseName() + "/latest?type=ev9879&tag="
        + URLEncoder.encode("code:" + STORED, StandardCharsets.UTF_8), null));
    assertThat(result.isNull("latest")).isFalse();
    assertThat(result.getJSONArray("latest").getDouble(2)).isEqualTo(4.0);
  }

  @Test
  void grafanaTargetTagsSelectTheRowTheInsertWrote() throws Exception {
    final JSONObject target = new JSONObject().put("refId", "A").put("type", "ev9879")
        .put("tags", new JSONObject().put("code", STORED));
    final JSONObject request = new JSONObject().put("from", 0L).put("to", 10_000L)
        .put("targets", new JSONArray().put(target));
    final JSONObject result = new JSONObject(send("/api/v1/ts/" + getDatabaseName() + "/grafana/query", request));

    final JSONObject frame = result.getJSONObject("results").getJSONObject("A").getJSONArray("frames").getJSONObject(0);
    assertThat(frame.getJSONObject("data").getJSONArray("values").getJSONArray(0).length()).isEqualTo(1);
  }

  /** POSTs {@code body} to {@code path}, or GETs it when {@code body} is null, and answers the 200 body. */
  private String send(final String path, final JSONObject body) throws Exception {
    final HttpRequest.Builder builder = HttpRequest.newBuilder().uri(new URI(getServerHttpUrl(0, path)))
        .setHeader("Authorization", basicAuth());
    if (body != null)
      builder.POST(HttpRequest.BodyPublishers.ofString(body.toString())).setHeader("Content-Type", "application/json");
    else
      builder.GET();
    final HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    return response.body();
  }

  private void command(final String sql) throws Exception {
    send("/api/v1/command/" + getDatabaseName(), new JSONObject().put("language", "sql").put("command", sql));
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
