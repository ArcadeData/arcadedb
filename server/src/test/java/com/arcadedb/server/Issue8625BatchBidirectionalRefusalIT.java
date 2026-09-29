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
 * Issue #8625: a batch loaded with {@code bidirectional=false} used to skip the incoming side of an edge type declared
 * bidirectional, leaving edges no query could reach from their target. The load is now refused with a client error,
 * and a unidirectional type still loads.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8625BatchBidirectionalRefusalIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Test
  void aUnidirectionalBatchOnABidirectionalTypeIsAClientError() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateVertexType("Issue8625V");
    database.getSchema().createEdgeType("Issue8625Both");

    final JSONObject error = post(400, body("Issue8625Both"));
    assertThat(error.getString("error")).contains("Edge type 'Issue8625Both' is bidirectional");
    assertThat(database.countType("Issue8625Both", false)).isZero();
  }

  @Test
  void aUnidirectionalBatchOnAUnidirectionalTypeLoads() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateVertexType("Issue8625V");
    database.getSchema().buildEdgeType().withName("Issue8625One").withBidirectional(false).create();

    final JSONObject result = post(200, body("Issue8625One"));
    assertThat(result.getLong("edgesCreated")).isEqualTo(1);
    assertThat(database.countType("Issue8625One", false)).isEqualTo(1);
  }

  private static String body(final String edgeType) {
    return "{\"@type\":\"vertex\",\"@class\":\"Issue8625V\",\"@id\":\"a\"}\n"
        + "{\"@type\":\"vertex\",\"@class\":\"Issue8625V\",\"@id\":\"b\"}\n"
        + "{\"@type\":\"edge\",\"@class\":\"" + edgeType + "\",\"@from\":\"a\",\"@to\":\"b\"}\n";
  }

  private JSONObject post(final int expectedStatus, final String body) throws Exception {
    final String url = "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/batch/" + getDatabaseName()
        + "?bidirectional=false";

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
