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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11983: the aggregation branches of {@code POST /api/v1/ts/{db}/query} and {@code POST /api/v1/ts/{db}/grafana/query}
 * answered MIN/MAX/SUM of a LONG column as doubles, so a value past 2^53 came back as a different number than SQL answers for
 * the same samples ({@code 9007199254740993} read back as {@code 9007199254740992.0}). The response text is asserted, not a
 * parsed number, because what a client reads is the JSON literal.
 */
class Issue11983ExactLongAggregatesOverHttpIT extends BaseGraphServerTest {

  private static final String TYPE = "exactlong11983";

  @Test
  void minMaxAndSumOfALongColumnAreTheExactIntegersOnBothEndpoints() throws Exception {
    testEachServer(serverIndex -> {
      final Database database = getServerDatabase(serverIndex, getDatabaseName());
      if (database.getSchema().existsType(TYPE))
        database.getSchema().dropType(TYPE);
      database.command("sql", "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts FIELDS (n LONG) SHARDS 1");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO " + TYPE + " SET ts = 1000, n = 9007199254740993");
        database.command("sql", "INSERT INTO " + TYPE + " SET ts = 2000, n = 1");
      });

      final String ts = post(serverIndex, "/api/v1/ts/graph/query", aggregationRequest());
      final JSONArray values = new JSONObject(ts).getJSONArray("buckets").getJSONObject(0).getJSONArray("values");
      assertThat(values.length()).isEqualTo(3);
      assertThat(ts).contains("[1,9007199254740993,9007199254740994]");

      final JSONArray targets = new JSONArray();
      final JSONObject target = new JSONObject();
      target.put("refId", "A");
      target.put("type", TYPE);
      target.put("aggregation", aggregationRequest().getJSONObject("aggregation"));
      targets.put(target);
      final JSONObject grafana = new JSONObject();
      grafana.put("from", 0L);
      grafana.put("to", 10_000L);
      grafana.put("targets", targets);

      final String frames = post(serverIndex, "/api/v1/ts/graph/grafana/query", grafana);
      assertThat(frames).contains("[1]").contains("[9007199254740993]").contains("[9007199254740994]");
      assertThat(frames).doesNotContain("9007199254740992");
    });
  }

  private JSONObject aggregationRequest() {
    final JSONArray requests = new JSONArray();
    for (final String type : new String[] { "MIN", "MAX", "SUM" }) {
      final JSONObject request = new JSONObject();
      request.put("field", "n");
      request.put("type", type);
      request.put("alias", type.toLowerCase(Locale.ROOT));
      requests.put(request);
    }
    final JSONObject aggregation = new JSONObject();
    aggregation.put("bucketInterval", 1_000_000L);
    aggregation.put("requests", requests);

    final JSONObject payload = new JSONObject();
    payload.put("type", TYPE);
    payload.put("from", 0L);
    payload.put("to", 10_000L);
    payload.put("aggregation", aggregation);
    return payload;
  }

  private String post(final int serverIndex, final String path, final JSONObject body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI("http://127.0.0.1:" + getServerHttpPort(serverIndex) + path)
        .toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body.toString().getBytes(StandardCharsets.UTF_8));
    }
    assertThat(connection.getResponseCode()).isEqualTo(200);
    try (final InputStream is = connection.getInputStream()) {
      return new String(is.readAllBytes(), StandardCharsets.UTF_8);
    } finally {
      connection.disconnect();
    }
  }
}
