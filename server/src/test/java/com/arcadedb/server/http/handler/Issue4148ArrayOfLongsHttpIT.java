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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4148: HTTP {@code params} with an integer JSON array bound to an
 * {@code ARRAY_OF_LONGS} property must round-trip without losing int64 precision.
 * <p>
 * The bug was that {@code JSONObject.toMap(true)} - used by {@code PostCommandHandler} on the
 * incoming JSON request - narrowed every numeric array to {@code float[]}, which collapses
 * distinct int64 values onto identical float bits past 2^24. This test asserts the parametrized
 * INSERT path now matches the literal-SQL path bit-for-bit.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue4148ArrayOfLongsHttpIT extends BaseGraphServerTest {

  private static final String TYPE_NAME = "TArrLongs";
  private static final long[] EXPECTED  = { 1000000000000L, 1000000000001L, 9999999999999L };

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database db = getDatabase(0);
    db.transaction(() -> {
      final DocumentType docType = db.getSchema().createDocumentType(TYPE_NAME);
      docType.createProperty("arr", Type.ARRAY_OF_LONGS);
    });
  }

  @Test
  void parametrizedArrayOfLongsRoundTripsWithoutPrecisionLoss() throws Exception {
    // Control: literal SQL must round-trip - confirms storage/serialization isn't the issue.
    final long[] literal = insertAndFetch(
        "INSERT INTO " + TYPE_NAME + " SET arr = [" + EXPECTED[0] + ", " + EXPECTED[1] + ", " + EXPECTED[2] + "] RETURN arr",
        null);
    assertThat(literal).as("control path (literal SQL)").containsExactly(EXPECTED);

    // Parametrized SQL: this was the BUG path.
    final JSONArray arr = new JSONArray();
    for (final long v : EXPECTED)
      arr.put(v);
    final JSONObject params = new JSONObject().put("arr", arr);

    final long[] parametrized = insertAndFetch(
        "INSERT INTO " + TYPE_NAME + " SET arr = :arr RETURN arr",
        params);
    assertThat(parametrized).as("parametrized path (regression #4148)").containsExactly(EXPECTED);
  }

  /**
   * Posts the INSERT command and parses the {@code arr} field of the first returned record back
   * into a {@code long[]}. Returns the bytes the server actually stored, so callers can compare
   * them directly against the input.
   */
  private long[] insertAndFetch(final String command, final JSONObject params) throws Exception {
    final JSONObject payload = new JSONObject();
    payload.put("language", "sql");
    payload.put("command", command);
    if (params != null)
      payload.put("params", params);

    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:2480/api/v1/command/" + getDatabaseName()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    final byte[] data = payload.toString().getBytes(StandardCharsets.UTF_8);
    connection.setRequestProperty("Content-Length", Integer.toString(data.length));
    try (final DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
      wr.write(data);
    }
    connection.connect();

    final int status;
    final String body;
    try {
      status = connection.getResponseCode();
      body = status >= 400
          ? (connection.getErrorStream() != null
              ? new String(connection.getErrorStream().readAllBytes(), StandardCharsets.UTF_8)
              : "<no error stream>")
          : readResponse(connection);
    } finally {
      connection.disconnect();
    }
    if (status != 200)
      throw new AssertionError("HTTP " + status + " for command [" + command + "]: " + body);

    final JSONArray records = new JSONObject(body).getJSONArray("result");
    assertThat(records.length()).as("INSERT ... RETURN must yield exactly one record").isEqualTo(1);
    final JSONArray returned = records.getJSONObject(0).getJSONArray("arr");
    final long[] out = new long[returned.length()];
    for (int i = 0; i < returned.length(); i++)
      out[i] = returned.getLong(i);
    return out;
  }
}
