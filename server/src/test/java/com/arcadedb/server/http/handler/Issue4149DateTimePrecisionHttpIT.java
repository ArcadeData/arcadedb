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
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4149: datetime properties created with explicit precision
 * (DATETIME, DATETIME_MICROS, DATETIME_NANOS, DATETIME_SECOND) must round-trip via the HTTP
 * params binding without losing fractional-second precision in the response payload.
 * <p>
 * Storage already truncates each value to the column's declared precision, but the JSON
 * serializer was using the schema-wide {@code arcadedb.dateTimeFormat} format string for every
 * column regardless of type. Default schema format is second precision so all variants came
 * back collapsed to seconds even though the bytes on disk carried sub-second data.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4149DateTimePrecisionHttpIT extends BaseGraphServerTest {

  private static final String TYPE_NAME = "TDateTime4149";
  private static final String VALUE     = "2026-05-09T12:34:56.123456789";

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database db = getDatabase(0);
    db.transaction(() -> {
      final DocumentType docType = db.getSchema().createDocumentType(TYPE_NAME);
      docType.createProperty("ts1", Type.DATETIME);
      docType.createProperty("tsm", Type.DATETIME_MICROS);
      docType.createProperty("tsn", Type.DATETIME_NANOS);
      docType.createProperty("tss", Type.DATETIME_SECOND);
    });
  }

  @Test
  void selectProjectionPreservesDateTimePrecision() throws Exception {
    final JSONObject params = new JSONObject().put("v", VALUE);
    runCommand("INSERT INTO " + TYPE_NAME + " SET ts1 = :v, tsm = :v, tsn = :v, tss = :v", params);
    final JSONObject record = runQuery("SELECT ts1, tsm, tsn, tss FROM " + TYPE_NAME);

    assertColumnPrecisions(record);
  }

  @Test
  void selectStarPreservesDateTimePrecision() throws Exception {
    final JSONObject params = new JSONObject().put("v", VALUE);
    runCommand("INSERT INTO " + TYPE_NAME + " SET ts1 = :v, tsm = :v, tsn = :v, tss = :v", params);
    final JSONObject record = runQuery("SELECT FROM " + TYPE_NAME);

    assertColumnPrecisions(record);
  }

  private static void assertColumnPrecisions(final JSONObject record) {
    // DATETIME stores millisecond precision: .123 expected.
    assertThat(record.getString("ts1")).as("DATETIME column must keep millisecond precision")
        .startsWith("2026-05-09").contains("12:34:56.123");

    // DATETIME_MICROS keeps microsecond precision: .123456 expected.
    assertThat(record.getString("tsm")).as("DATETIME_MICROS column must keep microsecond precision")
        .startsWith("2026-05-09").contains("12:34:56.123456");

    // DATETIME_NANOS keeps full nanosecond precision: .123456789 expected.
    assertThat(record.getString("tsn")).as("DATETIME_NANOS column must keep nanosecond precision")
        .startsWith("2026-05-09").contains("12:34:56.123456789");

    // DATETIME_SECOND drops fractional digits: just :56 (no fractional).
    assertThat(record.getString("tss")).as("DATETIME_SECOND column must drop fractional seconds")
        .startsWith("2026-05-09").contains("12:34:56").doesNotContain(".");
  }

  private void runCommand(final String command, final JSONObject params) throws Exception {
    insertAndFetch(command, params);
  }

  private JSONObject runQuery(final String query) throws Exception {
    return insertAndFetch(query, null);
  }

  private JSONObject insertAndFetch(final String command, final JSONObject params) throws Exception {
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
    return records.getJSONObject(0);
  }
}
