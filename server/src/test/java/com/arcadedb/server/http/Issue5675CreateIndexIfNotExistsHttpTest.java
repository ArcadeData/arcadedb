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
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5675 over HTTP, which is how it was reported. A {@code CREATE INDEX IF NOT EXISTS ... UNIQUE} that a
 * pre-existing {@code NOTUNIQUE} index cannot satisfy answered 200 with {@code "created": false}; the caller had no
 * way to tell that from the constraint actually being there.
 * <p>
 * The status matters as much as the refusal: an unsatisfiable schema request is a client mistake, so it has to come
 * back as 400. It reaches the handler as an {@code IllegalArgumentException}, which is one of the few exception types
 * with a 400 arm of its own - reporting it as anything else would degrade the answer to a 500 and tell clients and
 * load balancers to retry a request that can only ever fail the same way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5675CreateIndexIfNotExistsHttpTest extends BaseGraphServerTest {

  private static final String DATABASE_NAME = "graph";

  @Test
  void aGuardedUniqueOverANotUniqueIndexIsAClientError() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "sql", "CREATE VERTEX TYPE TightenHttp");
      executeCommand(serverIndex, "sql", "CREATE PROPERTY TightenHttp.Scalar STRING");
      executeCommand(serverIndex, "sql", "CREATE INDEX ON TightenHttp (Scalar) NOTUNIQUE");

      final JSONObject error = commandExpecting(serverIndex, 400,
          "CREATE INDEX IF NOT EXISTS ON TightenHttp (Scalar) UNIQUE");

      // Asserting the status alone would not do: an unrelated 400 would pass for the refusal under test.
      assertThat(error.getString("detail"))
          .as("the reason travels to the client, not just the status")
          .contains("TightenHttp[Scalar]");

      // And the duplicate the caller was told was impossible must still be impossible to hide: the index is
      // unchanged, so the insert that used to be silently accepted still is - loudly this time, because the caller
      // now knows the constraint was never created.
      commandExpecting(serverIndex, 200, "INSERT INTO TightenHttp SET Scalar = 'x'");
      commandExpecting(serverIndex, 200, "INSERT INTO TightenHttp SET Scalar = 'x'");
    });
  }

  /**
   * The idempotent case the guard exists for must stay a 200 answering {@code created: false}, so the test above
   * cannot pass by refusing every guarded statement.
   */
  @Test
  void aGuardedRepeatOfTheSameDefinitionIsStillANoOp() throws Exception {
    testEachServer(serverIndex -> {
      executeCommand(serverIndex, "sql", "CREATE VERTEX TYPE RepeatHttp");
      executeCommand(serverIndex, "sql", "CREATE PROPERTY RepeatHttp.Scalar STRING");
      executeCommand(serverIndex, "sql", "CREATE INDEX ON RepeatHttp (Scalar) UNIQUE");

      final JSONObject response = commandExpecting(serverIndex, 200,
          "CREATE INDEX IF NOT EXISTS ON RepeatHttp (Scalar) UNIQUE");

      assertThat(response.getJSONArray("result").getJSONObject(0).getBoolean("created")).isFalse();
    });
  }

  /** Posts a SQL command, asserts the HTTP status, and returns the parsed body (the error body on a failure). */
  private JSONObject commandExpecting(final int serverIndex, final int expectedStatus, final String sql)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    final JSONObject payload = new JSONObject();
    payload.put("language", "sql");
    payload.put("command", sql);
    formatPayload(connection, payload);
    connection.connect();

    try {
      // The status is read BEFORE the body: which stream carries it depends on the status, and asking for the error
      // stream of a successful response hands back null.
      final int status = connection.getResponseCode();
      final String body = status < 400 ? readResponse(connection) : readError(connection);
      assertThat(status).as("HTTP status for `%s`; body: %s", sql, body).isEqualTo(expectedStatus);
      return new JSONObject(body);
    } finally {
      connection.disconnect();
    }
  }
}
