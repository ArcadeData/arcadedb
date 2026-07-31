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

import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5545. #5602 moved the Cypher arithmetic family to
 * {@link ArithmeticErrorException} so it answers HTTP 400, but the SQL {@code abs()} kept throwing a bare
 * {@code CommandExecutionException}, which the handler cannot tell apart from a genuine internal fault - so the
 * identical client mistake answered 400 in Cypher and 500 in SQL.
 * <p>
 * Asserted through HTTP because the classification only exists at that boundary, and over both the read and the
 * write path because the auto-commit wrapper re-wraps the failure as a {@code TransactionException}, the shape that
 * historically degraded a client error back to 500.
 * <p>
 * The {@code detail} assertions below depend on the server not running in production mode:
 * {@code AbstractServerHttpHandler.buildErrorBody} emits {@code detail} only when verbose, to avoid leaking the
 * cause chain to a client probing endpoints. {@code SERVER_MODE} defaults to development, so it is present here.
 * The status code and the {@code exception} field are asserted independently of that, and they are the actual
 * subject of this test - {@code detail} only confirms which arithmetic error was reported.
 */
class Issue5545SqlArithmeticErrorHttpStatusIT extends BaseGraphServerTest {

  /**
   * {@code Long.MIN_VALUE} cannot be written as a SQL literal - the parser rejects the unsigned digits
   * ("Invalid integer: 9223372036854775808") before {@code abs()} is ever reached - so the value is built by
   * subtracting one from {@code MIN_VALUE + 1}, which is exact and stays in range.
   */
  private void createLongMinValueRecord(final int serverIndex, final String typeName) throws Exception {
    executeSql(serverIndex, "CREATE DOCUMENT TYPE " + typeName, 200);
    executeSql(serverIndex, "CREATE PROPERTY " + typeName + ".v LONG", 200);
    executeSql(serverIndex, "INSERT INTO " + typeName + " SET v = -9223372036854775807", 200);
    executeSql(serverIndex, "UPDATE " + typeName + " SET v = v - 1", 200);
  }

  @Test
  void sqlAbsOverflowReturns400OnTheReadPath() throws Exception {
    testEachServer(serverIndex -> {
      createLongMinValueRecord(serverIndex, "Issue5545Read");

      final JSONObject json = executeSql(serverIndex, "SELECT abs(v) AS r FROM Issue5545Read", 400);
      assertThat(json.getString("exception")).isEqualTo(ArithmeticErrorException.class.getName());
      assertThat(json.getString("detail")).contains("long overflow");
    });
  }

  @Test
  void sqlAbsOverflowReturns400OnTheWritePath() throws Exception {
    testEachServer(serverIndex -> {
      createLongMinValueRecord(serverIndex, "Issue5545Write");

      // UPDATE puts the statement on the auto-commit write path, where the failure arrives inside a
      // TransactionException wrapper instead of directly.
      final JSONObject json = executeSql(serverIndex, "UPDATE Issue5545Write SET w = abs(v)", 400);
      assertThat(json.getString("exception")).isEqualTo(ArithmeticErrorException.class.getName());
      assertThat(json.getString("detail")).contains("long overflow");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  /**
   * The guard fires on exactly one value per type. A stored value one above {@code MIN_VALUE} must still answer 200,
   * so a future tightening cannot turn ordinary data into a client error.
   */
  @Test
  void sqlAbsWithoutOverflowStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      executeSql(serverIndex, "CREATE DOCUMENT TYPE Issue5545Ok", 200);
      executeSql(serverIndex, "CREATE PROPERTY Issue5545Ok.v LONG", 200);
      executeSql(serverIndex, "INSERT INTO Issue5545Ok SET v = -9223372036854775807", 200);

      assertThat(executeSql(serverIndex, "SELECT abs(v) AS r FROM Issue5545Ok", 200).getJSONArray("result")
          .getJSONObject(0).getLong("r")).isEqualTo(Long.MAX_VALUE);
    });
  }

  private JSONObject executeSql(final int serverIndex, final String command, final int expectedStatus) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try {
      final JSONObject payload = new JSONObject().put("language", "sql").put("command", command);
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }

      final int statusCode = connection.getResponseCode();
      final String response = expectedStatus == 200 ? readResponse(connection) : readError(connection);

      assertThat(statusCode)
          .as("'%s' must return %d, got %d (body=%s)", command, expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
