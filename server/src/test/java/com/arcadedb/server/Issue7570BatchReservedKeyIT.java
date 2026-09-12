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
import com.arcadedb.database.Record;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7570, the half that is not documentation: the reported payload
 * <pre>{"@type":"vertex","@class":"Person","properties":{"name":"Alice"}}</pre>
 * answered 200 with {@code verticesCreated: 1} and stored a property literally named {@code properties} holding the
 * map. The counters were right and the data was wrong, and nothing failed until someone queried for {@code name} and
 * found nothing.
 * <p>
 * The unit tests in {@code Issue7570ReservedBatchKeyTest} pin the parsers. This one proves the refusal survives the
 * whole HTTP path - that it is reported as a 400 and not as the 408 a truncated body gets, that it carries the
 * partial-commit accounting the other {@code /batch} failures carry, and above all that the database is left with
 * nothing rather than with the wrong thing.
 */
class Issue7570BatchReservedKeyIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  /** The payload copied from the issue. */
  @Test
  void nestingPropertiesUnderAPropertiesKeyIsRefusedInsteadOfStoredVerbatim() throws Exception {
    createSchema("NestedProps");

    final JSONObject error = postExpecting(400,
        "{\"@type\":\"vertex\",\"@class\":\"NestedProps\",\"properties\":{\"name\":\"Alice\"}}\n",
        "application/x-ndjson");

    assertThat(error.getString("error"))
        .contains("'properties'")
        .contains("line 1")
        .contains("flat");
    assertThat(error.getLong("verticesCreated")).isZero();

    final Database db = getServerDatabase(0, getDatabaseName());
    assertThat(db.countType("NestedProps", false))
        .as("the misread line must leave nothing behind, not a vertex carrying a 'properties' map")
        .isZero();
  }

  /** A scalar of the same name is data, not a mistake, and still loads. */
  @Test
  void aScalarPropertiesValueStillLoads() throws Exception {
    createSchema("ScalarProps");

    final JSONObject result = post(200,
        "{\"@type\":\"vertex\",\"@class\":\"ScalarProps\",\"@id\":\"s1\",\"properties\":\"public\"}\n",
        "application/x-ndjson");

    assertThat(result.getLong("verticesCreated")).isEqualTo(1);

    final Database db = getServerDatabase(0, getDatabaseName());
    final List<Object> stored = new ArrayList<>();
    final Iterator<Record> records = db.iterateType("ScalarProps", false);
    while (records.hasNext())
      stored.add(records.next().asVertex().get("properties"));

    assertThat(stored).as("a scalar under this name is data, and has to be stored under it")
        .containsExactly("public");
  }

  /** An unrecognised control key used to be stored as a property whose name begins with {@code @}. */
  @Test
  void anUnknownAtPrefixedKeyIsRefused() throws Exception {
    createSchema("UnknownAtKey");

    final JSONObject error = postExpecting(400,
        "{\"@type\":\"vertex\",\"@class\":\"UnknownAtKey\",\"@id\":\"u1\",\"@rid\":\"#1:0\"}\n",
        "application/x-ndjson");

    assertThat(error.getString("error")).contains("'@rid'").contains("line 1");
  }

  /**
   * The refusal has to land as a 400 on a body that arrived whole. {@code PostBatchHandler} answers 408 for a
   * {@code MalformedBatchRecordException} raised on a body that ended early, which is why the reserved-key throw is
   * a plain {@code IllegalArgumentException}: a client sent a complete, well-formed line that the server declines,
   * and telling it to go hunting for a truncated upload would send it to the wrong place.
   */
  @Test
  void theRefusalKeepsTheAccountingAndTheLinesTheLoadHadReached() throws Exception {
    createSchema("Accounting");

    final String body = """
        {"@type":"vertex","@class":"Accounting","@id":"a1","name":"Alice"}
        {"@type":"vertex","@class":"Accounting","@id":"a2","name":"Bob"}
        {"@type":"vertex","@class":"Accounting","@id":"a3","@cat":"v"}
        """;

    final JSONObject error = postExpecting(400, body, "application/x-ndjson");

    assertThat(error.getString("error")).contains("'@cat'").contains("line 3");
    assertThat(error.getLong("linesRead")).as("the failing line was read too").isEqualTo(3);
    assertThat(error.getLong("linesSkipped")).isZero();
    assertThat(error.has("partialCommit")).isTrue();
  }

  /**
   * The streaming encoding answers 200 for a load that failed <em>after</em> it acknowledged a chunk, because the
   * status line is already gone by then. A reserved key on line 1 is rejected before anything is acknowledged, so the
   * request must still get its real 400 and the buffered error body - the guarantee the operation documents for its
   * 400 and 408, which a refusal introduced on this path must not quietly break.
   */
  @Test
  void theRefusalIsStillA400UnderTheStreamingEncoding() throws Exception {
    createSchema("Streaming");

    final JSONObject error = post(400,
        "{\"@type\":\"vertex\",\"@class\":\"Streaming\",\"properties\":{\"name\":\"Alice\"}}\n",
        "application/x-ndjson", "application/x-ndjson");

    assertThat(error.getString("error")).contains("'properties'").contains("line 1");
  }

  /** Same defect, CSV spelling: the control keys are column names there, so the header is where it is caught. */
  @Test
  void anUnknownAtPrefixedCsvColumnIsRefused() throws Exception {
    createSchema("CsvUnknownAtCol");

    final String body = """
        @type,@class,@id,@rid,name
        vertex,CsvUnknownAtCol,c1,#1:0,Carol
        """;

    final JSONObject error = postExpecting(400, body, "text/csv");

    assertThat(error.getString("error")).contains("'@rid'").contains("line 1");

    final Database db = getServerDatabase(0, getDatabaseName());
    assertThat(db.query("sql", "SELECT FROM CsvUnknownAtCol WHERE name = 'Carol'").stream().count()).isZero();
  }

  /** A CSV column named {@code properties} carries a scalar and is ordinary data, so it must keep working. */
  @Test
  void aCsvPropertiesColumnStillLoads() throws Exception {
    createSchema("CsvScalarProps");

    final String body = """
        @type,@class,@id,properties
        vertex,CsvScalarProps,c2,public
        """;

    final JSONObject result = post(200, body, "text/csv");

    assertThat(result.getLong("verticesCreated")).isEqualTo(1);
  }

  /**
   * Every test gets its own vertex type. The server database is shared across the methods of this class and the
   * assertions here are about what a refused load left behind, so a type another method had already written to would
   * make them pass or fail on execution order rather than on the fix.
   */
  private void createSchema(final String vertexType) {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (!db.getSchema().existsType(vertexType))
      db.getSchema().createVertexType(vertexType);
  }

  private JSONObject postExpecting(final int status, final String body, final String contentType) throws Exception {
    return post(status, body, contentType);
  }

  private JSONObject post(final int expectedStatus, final String body, final String contentType) throws Exception {
    return post(expectedStatus, body, contentType, null);
  }

  private JSONObject post(final int expectedStatus, final String body, final String contentType, final String accept)
      throws Exception {
    // Never a hardcoded 2480: the server binds the first free port of the configured range, and a port pinned to
    // 2480 is answered by whatever else already listens there - which surfaces as a 403 rather than as a conflict.
    final String url = "http://127.0.0.1:" + getServer(0).getHttpServer().getPort()
        + "/api/v1/batch/" + getDatabaseName();

    final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    conn.setRequestProperty("Content-Type", contentType);
    if (accept != null)
      conn.setRequestProperty("Accept", accept);
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
