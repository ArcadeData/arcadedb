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

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11907: a {@code POST /api/v1/batch} load refused because another GraphBatch is open on the same database answered
 * 500 "Internal error" and was logged with a stack trace. Nothing of it runs and the same load succeeds once the other one is
 * closed, so it is a 409 conflict, as a backup or a restore refused by another one in progress is. Both encodings of the answer
 * are pinned: the buffered one, and the streamed one a caller negotiates with {@code Accept: application/x-ndjson}, whose
 * refusal comes before the first line is written and so keeps its real status.
 */
class Issue11907BatchInProgressConflictIT extends BaseGraphServerTest {
  private static final String REFUSAL_LABEL = "A GraphBatch load is already in progress on this database";
  private static final String LOAD          = "{\"@type\":\"vertex\",\"@class\":\"Issue11907V\",\"k\":\"a\"}\n";

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Test
  void aLoadRefusedForAnotherBatchInProgressIsAConflictAndLoadsOnceItIsClosed() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateVertexType("Issue11907V");

    try (final GraphBatch holder = database.batch().build()) {
      assertThat(holder).isNotNull();

      final JSONObject refusal = new JSONObject(post(409, null, LOAD));
      assertThat(refusal.getString("error")).isEqualTo(REFUSAL_LABEL);
      assertThat(refusal.getString("exception")).isEqualTo(GraphBatchInProgressException.class.getName());
      assertThat(database.countType("Issue11907V", false)).isZero();
    }

    assertThat(new JSONObject(post(200, null, LOAD)).getLong("verticesCreated")).isEqualTo(1);
    assertThat(database.countType("Issue11907V", false)).isEqualTo(1);
  }

  @Test
  void aStreamedLoadRefusedForAnotherBatchInProgressIsTheSameConflictAndLoadsOnceItIsClosed() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateVertexType("Issue11907V");

    try (final GraphBatch holder = database.batch().build()) {
      assertThat(holder).isNotNull();

      // Nothing of the answer is written when the batch is refused, so the status line is still the load's to choose
      final JSONObject refusal = new JSONObject(post(409, "application/x-ndjson", LOAD));
      assertThat(refusal.getString("error")).isEqualTo(REFUSAL_LABEL);
      assertThat(refusal.getString("exception")).isEqualTo(GraphBatchInProgressException.class.getName());
      assertThat(database.countType("Issue11907V", false)).isZero();
    }

    final String streamed = post(200, "application/x-ndjson", LOAD).strip();
    final String terminal = streamed.substring(streamed.lastIndexOf('\n') + 1);
    assertThat(new JSONObject(terminal).getJSONObject("summary").getLong("verticesCreated")).isEqualTo(1);
    assertThat(database.countType("Issue11907V", false)).isEqualTo(1);
  }

  private String post(final int expectedStatus, final String accept, final String body) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) URI.create(getServerHttpUrl("/api/v1/batch/" + getDatabaseName()))
        .toURL().openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      conn.setRequestProperty("Content-Type", "application/x-ndjson");
      if (accept != null)
        conn.setRequestProperty("Accept", accept);
      conn.setDoOutput(true);

      final byte[] data = body.getBytes(StandardCharsets.UTF_8);
      conn.setFixedLengthStreamingMode(data.length);
      try (final OutputStream out = conn.getOutputStream()) {
        out.write(data);
      }

      final int status = conn.getResponseCode();
      final InputStream in = status < 400 ? conn.getInputStream() : conn.getErrorStream();
      final String response = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      assertThat(status).as("response: %s", response).isEqualTo(expectedStatus);
      return response;
    } finally {
      conn.disconnect();
    }
  }
}
