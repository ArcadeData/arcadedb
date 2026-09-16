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
 * Issue #7574 over the wire. {@code Issue7574MisplacedBatchControlKeyTest} pins the two parsers; this proves the
 * refusal reaches the client as a 400 rather than as the 408 a truncated body gets, that it carries the same
 * partial-commit accounting every other {@code /batch} failure carries, and that the load left nothing behind.
 * <p>
 * The payload in the first test is the one from the issue: before the fix it answered 200 with
 * {@code edgesCreated: 1} and the {@code @id} nowhere - neither stored nor reported.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7574">issue #7574</a>
 */
class Issue7574BatchMisplacedControlKeyIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Test
  void anIdOnAnEdgeLineIsRefusedWithA400InsteadOfBeingDropped() throws Exception {
    createSchema("Misplaced7574A");

    final String body = """
        {"@type":"vertex","@class":"Misplaced7574A","@id":"p1","name":"Alice"}
        {"@type":"vertex","@class":"Misplaced7574A","@id":"p2","name":"Bob"}
        {"@type":"edge","@class":"Misplaced7574AEdge","@from":"p1","@to":"p2","@id":"e1"}
        """;

    final JSONObject error = post(400, body, "application/x-ndjson");

    assertThat(error.getString("error"))
        .contains("'@id'")
        .contains("line 3")
        .contains("vertex line");
    // The accounting a client needs to reconcile: the two vertices were attempted before the offending line.
    assertThat(error.has("verticesCreated")).as("a batch failure reports what it had attempted").isTrue();
    assertThat(error.has("partialCommit")).isTrue();
  }

  @Test
  void aFromOnAVertexLineIsRefusedAndStoresNothing() throws Exception {
    createSchema("Misplaced7574B");

    final JSONObject error = post(400,
        "{\"@type\":\"vertex\",\"@class\":\"Misplaced7574B\",\"@id\":\"p1\",\"@from\":\"somewhere\"}\n",
        "application/x-ndjson");

    assertThat(error.getString("error")).contains("'@from'").contains("edge line");

    final Database db = getServerDatabase(0, getDatabaseName());
    assertThat(db.countType("Misplaced7574B", false))
        .as("the misplaced key is refused before the record is created, not after")
        .isZero();
  }

  /**
   * The CSV rule refuses a value, not a column, so the single-header form a client may already be sending keeps
   * loading. This is the payload that would have broken had the refusal been keyed on the header.
   */
  @Test
  void oneCsvHeaderAcrossBothSectionsStillLoadsWhenTheUnusedColumnsAreEmpty() throws Exception {
    createSchema("Misplaced7574C");

    final String body = """
        @type,@class,@id,@from,@to,name
        vertex,Misplaced7574C,p1,,,Alice
        vertex,Misplaced7574C,p2,,,Bob
        edge,Misplaced7574CEdge,,p1,p2,
        """;

    final JSONObject result = post(200, body, "text/csv");

    assertThat(result.getLong("verticesCreated")).isEqualTo(2);
    assertThat(result.getLong("edgesCreated")).isEqualTo(1);
  }

  /** And the same header refuses a value the row's kind cannot use. */
  @Test
  void aCsvRowCarryingAControlValueItsKindCannotUseIsRefused() throws Exception {
    createSchema("Misplaced7574D");

    final String body = """
        @type,@class,@id,@from,@to,name
        vertex,Misplaced7574D,p1,nowhere,,Alice
        """;

    final JSONObject error = post(400, body, "text/csv");

    assertThat(error.getString("error")).contains("'@from'").contains("line 2");
  }

  /**
   * Every test gets its own types, because the server database is shared across the methods of this class and the
   * assertions are about what a refused load left behind.
   */
  private void createSchema(final String vertexType) {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (!db.getSchema().existsType(vertexType))
      db.getSchema().createVertexType(vertexType);
    if (!db.getSchema().existsType(vertexType + "Edge"))
      db.getSchema().createEdgeType(vertexType + "Edge");
  }

  private JSONObject post(final int expectedStatus, final String body, final String contentType) throws Exception {
    // Never a hardcoded 2480: the server binds the first free port of the configured range, and a port pinned to
    // 2480 is answered by whatever else already listens there - which surfaces as a 403 rather than as a conflict.
    final String url = "http://127.0.0.1:" + getServer(0).getHttpServer().getPort()
        + "/api/v1/batch/" + getDatabaseName();

    final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    conn.setRequestProperty("Content-Type", contentType);
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
