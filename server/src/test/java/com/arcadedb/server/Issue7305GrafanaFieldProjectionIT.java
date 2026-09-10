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

import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A Grafana raw query that projects a subset of fields must return those fields' values, under those fields'
 * names.
 * <p>
 * Found while building the gRPC time-series API (issue #7305). The projection's indices count NON-timestamp
 * columns - {@code TimeSeriesBucket.readRow} always prepends the timestamp and then tests each non-timestamp
 * column's own ordinal against the array - but the resolver produced full-schema indices, and
 * {@code PostGrafanaQueryHandler} turned them back into columns with {@code columns.get(idx)}. Every field was
 * therefore shifted by one: the schema named {@code temperature} while the values were {@code location}'s, and
 * the last column came back all null. Both halves are now resolved by {@code TimeSeriesGateway}, which the
 * {@code /ts/query} and gRPC paths share.
 * <p>
 * {@code GrafanaTimeSeriesHandlerIT} did not catch it because it never projects: its assertions run against the
 * full column set.
 */
class Issue7305GrafanaFieldProjectionIT extends BaseGraphServerTest {

  private static final String TYPE = "weather";

  @Test
  void aProjectedGrafanaQueryReturnsTheNamedFieldsValues() throws Exception {
    // The port the test server actually bound: the configured range starts at 2480, and a server already
    // listening there pushes this one up.
    final int port = getServer(0).getHttpServer().getPort();

    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", port, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {
      database.command("sql", "CREATE TIMESERIES TYPE " + TYPE
          + " TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE, humidity DOUBLE)");
      database.command("sql", "INSERT INTO " + TYPE
          + " SET ts = 1000, location = 'us-east', temperature = 22.5, humidity = 41.0");
      database.command("sql", "INSERT INTO " + TYPE
          + " SET ts = 2000, location = 'us-east', temperature = 23.5, humidity = 42.0");
    }

    final JSONObject target = new JSONObject()
        .put("refId", "A")
        .put("type", TYPE)
        .put("fields", new JSONArray().put("humidity"));
    final JSONObject request = new JSONObject().put("targets", new JSONArray().put(target));

    final JSONObject response = postGrafanaQuery(port, request);
    final JSONObject frame = response.getJSONObject("results").getJSONObject("A")
        .getJSONArray("frames").getJSONObject(0);

    final JSONArray fields = frame.getJSONObject("schema").getJSONArray("fields");
    assertThat(fields.length()).as("timestamp plus the one projected field").isEqualTo(2);
    assertThat(fields.getJSONObject(0).getString("name")).isEqualTo("ts");
    assertThat(fields.getJSONObject(1).getString("name")).isEqualTo("humidity");

    // The values, not just the names. Before the fix this column carried 'location' - the string 'us-east'
    // under the name 'humidity' - so an assertion on the schema alone passed against the defect.
    final JSONArray values = frame.getJSONObject("data").getJSONArray("values");
    assertThat(values.length()).isEqualTo(2);
    assertThat(values.getJSONArray(0).getLong(0)).isEqualTo(1_000L);
    assertThat(values.getJSONArray(1).getDouble(0)).isEqualTo(41.0);
    assertThat(values.getJSONArray(1).getDouble(1)).isEqualTo(42.0);
  }

  private JSONObject postGrafanaQuery(final int port, final JSONObject request) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) URI.create(
        "http://127.0.0.1:" + port + "/api/v1/ts/" + getDatabaseName() + "/grafana/query").toURL().openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(request.toString().getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    assertThat(connection.getResponseCode()).isEqualTo(200);
    try (final InputStream is = connection.getInputStream()) {
      return new JSONObject(new String(is.readAllBytes(), StandardCharsets.UTF_8));
    }
  }
}
