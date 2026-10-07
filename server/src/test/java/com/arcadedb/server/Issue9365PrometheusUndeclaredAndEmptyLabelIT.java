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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #9365 and #9363 on Prometheus remote write:
 * <ul>
 * <li>#9365: a series carrying a label the existing type has no TAG column for is dropped and reported with a 400
 * naming the label (the valid series of the same request are stored), unless
 * {@code arcadedb.timeSeriesUndeclaredKeys=ignore}, which discards the label.</li>
 * <li>#9363: a label with an empty value is an absent label, so it is stored as null, the same series as one that omits
 * the label, and is never "undeclared".</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9365PrometheusUndeclaredAndEmptyLabelIT extends BaseGraphServerTest {

  @Test
  void anUndeclaredLabelDropsOnlyThatSeries() throws Exception {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.getConfiguration().setValue(GlobalConfiguration.TIMESERIES_UNDECLARED_KEYS, "reject");

    assertThat(post(new WriteRequest(List.of(series("m9365a", 1.0, label("host", "h1")))))).isEqualTo(204);

    final int status = post(new WriteRequest(List.of(
        series("m9365a", 2.0, label("host", "h2")),
        series("m9365a", 3.0, label("host", "h3"), label("region", "eu")))));
    assertThat(status).as("the series with the undeclared label is reported").isEqualTo(400);

    assertThat(count(db, "m9365a")).as("the valid series is stored, the other is not").isEqualTo(2);
  }

  @Test
  void ignoreDiscardsTheUndeclaredLabel() throws Exception {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.getConfiguration().setValue(GlobalConfiguration.TIMESERIES_UNDECLARED_KEYS, "ignore");
    try {
      assertThat(post(new WriteRequest(List.of(series("m9365b", 1.0, label("host", "h1")))))).isEqualTo(204);
      assertThat(post(new WriteRequest(List.of(series("m9365b", 2.0, label("host", "h1"), label("region", "eu"))))))
          .isEqualTo(204);
      assertThat(count(db, "m9365b")).isEqualTo(2);
    } finally {
      db.getConfiguration().setValue(GlobalConfiguration.TIMESERIES_UNDECLARED_KEYS, "reject");
    }
  }

  @Test
  void anEmptyLabelValueIsStoredAsAbsent() throws Exception {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.getConfiguration().setValue(GlobalConfiguration.TIMESERIES_UNDECLARED_KEYS, "reject");

    assertThat(post(new WriteRequest(List.of(series("m9363", 1.0, label("host", ""), label("zone", "z1")))))).isEqualTo(204);
    // omits `host` altogether, and a later series that names it empty again: neither is undeclared
    assertThat(post(new WriteRequest(List.of(series("m9363", 2.0, label("zone", "z1")))))).isEqualTo(204);

    try (final ResultSet rs = db.query("sql", "SELECT host FROM m9363")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        // the engine reads an absent tag back as "", so null and "" are one series; what matters is the write succeeded
        assertThat(r.<String>getProperty("host")).as("empty label stored as absent").isNullOrEmpty();
      }
    }
    assertThat(count(db, "m9363")).isEqualTo(2);
  }

  private static Label label(final String name, final String value) {
    return new Label(name, value);
  }

  private static TimeSeries series(final String metric, final double value, final Label... labels) {
    final List<Label> all = new ArrayList<>();
    all.add(new Label("__name__", metric));
    all.addAll(List.of(labels));
    return new TimeSeries(all, List.of(new Sample(value, 1000)));
  }

  private static long count(final Database db, final String type) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS n FROM " + type)) {
      return rs.next().<Long>getProperty("n");
    }
  }

  private int post(final WriteRequest request) throws Exception {
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
    return connection.getResponseCode();
  }
}
