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

import com.arcadedb.database.Database;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11871: the HTTP command endpoint appends a LIMIT to every SELECT that has none, and that LIMIT
 * turned off the index seek of a range {@code min()}/{@code max()} (#8812), so over HTTP the aggregate read the whole range.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RangeMinMaxOverHttpTest extends BaseGraphServerTest {
  private static final String TYPE = "Issue11871";

  @Test
  void aRangeMinMaxKeepsTheIndexSeekUnderTheAppendedLimit() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    final VertexType type = database.getSchema().createVertexType(TYPE);
    type.createProperty("k", Type.INTEGER);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newVertex(TYPE).set("k", i).save();
    });

    assertSeek("SELECT min(k) AS m FROM " + TYPE + " WHERE k > 500", 501);
    assertSeek("SELECT max(k) AS m FROM " + TYPE + " WHERE k < 500", 499);
    assertSeek("SELECT min(k) AS m FROM " + TYPE + " WHERE k BETWEEN 200 AND 300", 200);
  }

  private void assertSeek(final String sql, final int expected) throws Exception {
    final JSONObject response = postProfiled(sql);
    final String plan = response.getString("explain");
    assertThat(plan).as("plan of '%s' over HTTP", sql).contains("FIRST ROW VALUE").doesNotContain("AGGREGATE");

    final JSONArray records = response.getJSONObject("result").getJSONArray("records");
    assertThat(records.length()).isEqualTo(1);
    assertThat(records.getJSONObject(0).getInt("m")).isEqualTo(expected);
  }

  private JSONObject postProfiled(final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl("/api/v1/command/graph")).toURL()
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    final JSONObject payload = new JSONObject();
    payload.put("language", "sql");
    payload.put("command", command);
    payload.put("serializer", "studio");
    payload.put("profileExecution", "detailed");
    formatPayload(connection, payload);
    connection.connect();
    try {
      final String response = readResponse(connection);
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
