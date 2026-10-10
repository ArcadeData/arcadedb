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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9628: POST /api/v1/query is the route that cannot write, because the engine refuses anything that is not
 * idempotent. A write nested in a LET, a projection or a FROM target was classified by the outer statement alone and
 * walked through it, durably - including the server's own rewrite of the text with the truncation probe limit.
 */
class Issue9628NestedWriteQueryEndpointTest extends BaseGraphServerTest {

  @Test
  void queryEndpointRefusesANestedWriteAndWritesNothing() throws Exception {
    executeCommand(0, "sql", "CREATE DOCUMENT TYPE Victim IF NOT EXISTS");

    for (final String command : new String[] { //
        "SELECT FROM Victim LET $y = (INSERT INTO Victim SET a = 2)", //
        "SELECT (INSERT INTO Victim SET a = 5) as p FROM Victim", //
        "SELECT FROM (INSERT INTO Victim SET a = 6)", //
        "LET $x = (INSERT INTO Victim SET a = 1)", //
        "LET $z = (CREATE DOCUMENT TYPE SneakDur)" }) {
      final HttpURLConnection connection = post("sql", command);
      try {
        assertThat(connection.getResponseCode()).as(command).isEqualTo(400);
        assertThat(readError(connection)).as(command).contains("idempotent");
      } finally {
        connection.disconnect();
      }
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType("Victim", false)).isZero();
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().existsType("SneakDur")).isFalse();
  }

  @Test
  void queryEndpointStillRunsANestedRead() throws Exception {
    executeCommand(0, "sql", "CREATE DOCUMENT TYPE Readable IF NOT EXISTS");
    executeCommand(0, "sql", "INSERT INTO Readable SET a = 1");

    final HttpURLConnection connection = post("sql", "SELECT $y FROM Readable LET $y = (SELECT count(*) FROM Readable)");
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      assertThat(new JSONObject(readResponse(connection)).getJSONArray("result").length()).isEqualTo(1);
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection post(final String language, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(0, "/api/v1/query/" + getDatabaseName())).toURL()
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    final JSONObject payload = new JSONObject();
    payload.put("language", language);
    payload.put("command", command);
    formatPayload(connection, payload);
    connection.connect();
    return connection;
  }
}
