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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A read-only {@code POST /api/v1/query} used to run inside the auto-commit transaction every handler inherits, so its
 * full scans were always sequential: a scan inside a transaction is, to keep isolation, and the parallel one needs none
 * (#8775). A query cannot write, so it has nothing to wrap, exactly like {@code GET /query}.
 */
class Issue8775PostQueryOutsideTransactionTest extends BaseGraphServerTest {
  private static final String SCAN = "EXPLAIN SELECT count(*) AS n FROM Scan8775 WHERE grp = 5";

  @Test
  void postQueryPlansTheSameParallelScanAsGetQuery() throws Exception {
    testEachServer(serverIndex -> {
      final Database database = getServerDatabase(serverIndex, "graph");
      database.getSchema().createDocumentType("Scan8775", 8);
      database.transaction(() -> {
        for (int i = 0; i < 20_000; i++)
          database.newDocument("Scan8775").set("id", i, "grp", i % 100).save();
      });

      final String viaGet = explainOf(serverIndex, "GET");
      assertThat(viaGet).as("GET /query is the reference: it never had a transaction").contains("(parallel)");
      assertThat(explainOf(serverIndex, "POST")).contains("(parallel)");
    });
  }

  @Test
  void postQueryInsideASessionTransactionStaysSequential() throws Exception {
    testEachServer(serverIndex -> {
      final Database database = getServerDatabase(serverIndex, "graph");
      database.getSchema().createDocumentType("Scan8775S", 8);
      database.transaction(() -> {
        for (int i = 0; i < 20_000; i++)
          database.newDocument("Scan8775S").set("id", i, "grp", i % 100).save();
      });

      final HttpURLConnection begin = open(serverIndex, "POST", "/api/v1/begin/graph");
      begin.setDoOutput(true);
      begin.getOutputStream().close();
      assertThat(begin.getResponseCode()).isEqualTo(204);
      final String session = begin.getHeaderField("arcadedb-session-id");
      begin.disconnect();
      assertThat(session).isNotNull();

      final HttpURLConnection query = open(serverIndex, "POST", "/api/v1/query/graph");
      query.setRequestProperty("arcadedb-session-id", session);
      final JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", "EXPLAIN SELECT count(*) AS n FROM Scan8775S WHERE grp = 5");
      formatPayload(query, payload);
      assertThat(readResponse(query)).doesNotContain("(parallel)");
      query.disconnect();

      final HttpURLConnection rollback = open(serverIndex, "POST", "/api/v1/rollback/graph");
      rollback.setRequestProperty("arcadedb-session-id", session);
      rollback.setDoOutput(true);
      rollback.getOutputStream().close();
      rollback.getResponseCode();
      rollback.disconnect();
    });
  }

  @Test
  void postQueryWithAnUnknownSessionIdIsStillRefused() throws Exception {
    testEachServer(serverIndex -> {
      final HttpURLConnection query = open(serverIndex, "POST", "/api/v1/query/graph");
      query.setRequestProperty("arcadedb-session-id", "AS-does-not-exist");
      final JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", "SELECT 1 AS n");
      formatPayload(query, payload);
      try {
        // NOT silently run outside the transaction its caller believes it is in (#7402)
        assertThat(query.getResponseCode()).isNotEqualTo(200);
      } finally {
        query.disconnect();
      }
    });
  }

  private String explainOf(final int serverIndex, final String method) throws Exception {
    final HttpURLConnection connection;
    if (method.equals("GET")) {
      connection = open(serverIndex, "GET",
          "/api/v1/query/graph/sql/" + java.net.URLEncoder.encode(SCAN, java.nio.charset.StandardCharsets.UTF_8).replace("+", "%20"));
    } else {
      connection = open(serverIndex, "POST", "/api/v1/query/graph");
      final JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", SCAN);
      formatPayload(connection, payload);
    }
    try {
      return new JSONObject(readResponse(connection)).getString("explain");
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection open(final int serverIndex, final String method, final String path) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(getServerHttpUrl(serverIndex, path)).openConnection();
    connection.setRequestMethod(method);
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    return connection;
  }
}
