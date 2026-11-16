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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11633: an edge naming a type the schema does not have is refused by {@code GraphBatch} with a {@code SchemaException}
 * rather than an {@code IllegalArgumentException}. A batch load is not atomic, so the refusal must still carry how much of the
 * payload was committed before it, or a client re-posting the whole payload duplicates the committed vertices.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11633BatchMissingEdgeTypeTest extends BaseGraphServerTest {

  @Test
  void aMissingEdgeTypeStillReportsWhatWasCommitted() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE Issue11633V");

    final String payload = """
        {"@type":"vertex","@class":"Issue11633V","@id":"a"}
        {"@type":"vertex","@class":"Issue11633V","@id":"b"}
        {"@type":"edge","@class":"Issue11633NoSuchEdge","@from":"a","@to":"b"}
        """;
    // a chunk of one vertex: both vertices are committed before the edge is read
    final HttpURLConnection connection = postBatch(payload, "vertexBatchSize=1");
    try {
      assertThat(connection.getResponseCode()).isEqualTo(400);
      final JSONObject error = new JSONObject(readBody(connection.getErrorStream()));
      assertThat(error.getString("error")).contains("Issue11633NoSuchEdge");
      assertThat(error.getString("exception")).isEqualTo(SchemaException.class.getName());
      assertThat(error.getLong("verticesCreated")).isEqualTo(2);
      assertThat(error.getLong("edgesCreated")).isZero();
      assertThat(error.getBoolean("partialCommit")).isTrue();
    } finally {
      connection.disconnect();
    }
    assertThat(database.countType("Issue11633V", false)).isEqualTo(2);
  }

  private HttpURLConnection postBatch(final String body, final String query) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        getServerHttpUrl(0, "/api/v1/batch/" + getDatabaseName() + "?" + query)).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/x-ndjson");
    connection.setDoOutput(true);
    final byte[] data = body.getBytes(StandardCharsets.UTF_8);
    connection.setRequestProperty("Content-Length", Integer.toString(data.length));
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(data);
    }
    connection.connect();
    return connection;
  }

  private static String readBody(final InputStream in) throws Exception {
    try (in) {
      return FileUtils.readStreamAsString(in, "UTF-8");
    }
  }
}
