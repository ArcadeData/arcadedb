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
import com.arcadedb.exception.GraphBatchInProgressException;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11907: a {@code POST /api/v1/batch} load refused because another GraphBatch is open on the same database answered
 * 500 "Internal error" and was logged with a stack trace. Nothing of it runs and the same load succeeds once the other one is
 * closed, so it is a 409 conflict, as a backup or a restore refused by another one in progress is.
 */
class Issue11907BatchInProgressConflictIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Test
  void aLoadRefusedForAnotherBatchInProgressIsAConflictAndLoadsOnceItIsClosed() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateVertexType("Issue11907V");
    final String load = "{\"@type\":\"vertex\",\"@class\":\"Issue11907V\",\"k\":\"a\"}\n";

    try (final GraphBatch holder = database.batch().build()) {
      assertThat(holder).isNotNull();

      final JSONObject refusal = post(409, load);
      assertThat(refusal.getString("error")).isEqualTo("A GraphBatch load is already in progress on this database");
      assertThat(refusal.getString("exception")).isEqualTo(GraphBatchInProgressException.class.getName());
      assertThat(database.countType("Issue11907V", false)).isZero();
    }

    assertThat(post(200, load).getLong("verticesCreated")).isEqualTo(1);
    assertThat(database.countType("Issue11907V", false)).isEqualTo(1);
  }

  private JSONObject post(final int expectedStatus, final String body) throws Exception {
    final String url = "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/batch/" + getDatabaseName();

    final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    conn.setRequestProperty("Content-Type", "application/x-ndjson");
    conn.setDoOutput(true);

    final byte[] data = body.getBytes(StandardCharsets.UTF_8);
    conn.setRequestProperty("Content-Length", Integer.toString(data.length));
    try (final DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
      out.write(data);
    }
    conn.connect();

    try {
      final int status = conn.getResponseCode();
      final InputStream in = status < 400 ? conn.getInputStream() : conn.getErrorStream();
      final String response = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      assertThat(status).as("response: %s", response).isEqualTo(expectedStatus);
      return new JSONObject(response);
    } finally {
      conn.disconnect();
    }
  }
}
