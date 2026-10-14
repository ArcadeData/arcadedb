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

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
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
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9972: Prometheus remote write hands a label over as text, so a TIMESERIES type declaring a tag as anything but STRING
 * used to fail the whole request with a server error. Every label is now read through {@code ColumnDefinition.storableValue},
 * as the line protocol reads it: a convertible label is stored in the tag's type, and an unconvertible one drops only its series,
 * answered 400 and named in {@code unreadableValues}. The sample value reaches a non-DOUBLE field the same way.
 */
class Issue9972PrometheusTypedTagIT extends BaseGraphServerTest {

  @Test
  void anIntegerTagIsStoredInItsType() throws Exception {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sql", "CREATE TIMESERIES TYPE p9972a TIMESTAMP ts TAGS (host INTEGER) FIELDS (v DOUBLE) SHARDS 1");

    final Response response = post(new WriteRequest(List.of(series("p9972a", 1.0, 1000, label("host", "42")))));
    assertThat(response.status).as(response.body).isEqualTo(204);

    try (final ResultSet rs = db.query("sql", "SELECT host, v FROM p9972a")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("host").intValue()).isEqualTo(42);
      assertThat(r.<Number>getProperty("v").doubleValue()).isEqualTo(1.0);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void booleanAndDateTagsAreStoredInTheirTypes() throws Exception {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sql", "CREATE TIMESERIES TYPE p9972b TIMESTAMP ts TAGS (ok BOOLEAN, day DATE) FIELDS (v DOUBLE) SHARDS 1");

    final Response response = post(new WriteRequest(List.of(
        series("p9972b", 1.0, 1000, label("ok", "true"), label("day", "2026-10-14")))));
    assertThat(response.status).as(response.body).isEqualTo(204);

    try (final ResultSet rs = db.query("sql", "SELECT ok, day FROM p9972b")) {
      final Result r = rs.next();
      assertThat(r.<Boolean>getProperty("ok")).as("a BOOLEAN tag sent as \"true\" used to be stored as false").isTrue();
      final Object day = r.getProperty("day");
      final LocalDate stored = day instanceof LocalDate localDate ? localDate :
          ((Date) day).toInstant().atZone(ZoneOffset.UTC).toLocalDate();
      assertThat(stored).isEqualTo(LocalDate.of(2026, 10, 14));
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void anUnconvertibleLabelDropsOnlyItsSeriesAndIsNamed() throws Exception {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sql", "CREATE TIMESERIES TYPE p9972c TIMESTAMP ts TAGS (host INTEGER) FIELDS (v DOUBLE) SHARDS 1");

    final Response response = post(new WriteRequest(List.of(
        series("p9972c", 1.0, 1000, label("host", "7")),
        series("p9972c", 2.0, 2000, label("host", "not-a-number")))));
    assertThat(response.status).as("refused as the client's mistake, not a server error: " + response.body).isEqualTo(400);

    final JSONObject body = new JSONObject(response.body);
    assertThat(body.getInt("dropped")).isEqualTo(1);
    assertThat(body.getInt("droppedSamples")).isEqualTo(1);
    final JSONArray unreadable = body.getJSONArray("unreadableValues");
    assertThat(unreadable.length()).isEqualTo(1);
    assertThat(unreadable.getString(0)).startsWith("p9972c.host (tag): ");
    assertThat(body.getString("error")).contains("p9972c.host");

    assertThat(count(db, "p9972c")).as("the valid series of the same request is stored").isEqualTo(1);
  }

  @Test
  void aSampleValueAFieldCannotHoldDropsOnlyThatSample() throws Exception {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sql", "CREATE TIMESERIES TYPE p9972d TIMESTAMP ts TAGS (host STRING) FIELDS (value INTEGER) SHARDS 1");

    final List<Label> labels = List.of(new Label("__name__", "p9972d"), label("host", "h1"));
    final Response response = post(new WriteRequest(List.of(new TimeSeries(labels, List.of(
        new Sample(5.0, 1000),
        new Sample(1e12, 2000), // out of the INTEGER range: the append would store a truncated number
        new Sample(6.0, 3000))))));
    assertThat(response.status).as(response.body).isEqualTo(400);

    final JSONObject body = new JSONObject(response.body);
    assertThat(body.getInt("dropped")).as("no whole series is dropped").isEqualTo(0);
    assertThat(body.getInt("droppedSamples")).isEqualTo(1);
    assertThat(body.getJSONArray("unreadableValues").getString(0)).startsWith("p9972d.value (field): ");

    try (final ResultSet rs = db.query("sql", "SELECT value FROM p9972d ORDER BY ts")) {
      assertThat(rs.next().<Number>getProperty("value").intValue()).isEqualTo(5);
      assertThat(rs.next().<Number>getProperty("value").intValue()).isEqualTo(6);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  private static Label label(final String name, final String value) {
    return new Label(name, value);
  }

  private static TimeSeries series(final String metric, final double value, final long timestamp, final Label... labels) {
    final List<Label> all = new ArrayList<>();
    all.add(new Label("__name__", metric));
    all.addAll(List.of(labels));
    return new TimeSeries(all, List.of(new Sample(value, timestamp)));
  }

  private static long count(final Database db, final String type) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS n FROM " + type)) {
      return rs.next().<Long>getProperty("n");
    }
  }

  private record Response(int status, String body) {
  }

  private Response post(final WriteRequest request) throws Exception {
    final byte[] compressed = Snappy.compress(request.encode());
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServerHttpPort(0) + "/api/v1/ts/" + getDatabaseName() + "/prom/write").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/x-protobuf");
    connection.setRequestProperty("Content-Encoding", "snappy");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(compressed);
      os.flush();
    }
    final int status = connection.getResponseCode();
    final InputStream in = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
    final String body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
    return new Response(status, body);
  }
}
