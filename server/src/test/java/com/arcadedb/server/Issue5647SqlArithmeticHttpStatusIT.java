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
 * Regression test for issue #5647, asserted over HTTP because the status code is the whole point: a raw
 * {@code java.lang.ArithmeticException} - which is what SQL {@code 1/0} used to raise - is not an
 * {@link ArithmeticErrorException}, so it missed the classification #5602 added and fell through to the generic
 * {@code catch (Throwable)} arm as HTTP 500 with a stack trace in the server log, for what is only a caller mistake.
 * <p>
 * #5631 converted SQL {@code abs()} alone (issue #5545), so until now the same client error answered 400 in Cypher
 * and 500 in SQL. The overflow half is covered too, since silently wrapping past {@code Long.MAX_VALUE} answered
 * 200 with a wrong number rather than failing at all.
 * <p>
 * The {@code detail} assertions depend on the server not running in production mode:
 * {@code AbstractServerHttpHandler.buildErrorBody} emits {@code detail} only when verbose, to avoid leaking the
 * cause chain to a client probing endpoints. {@code SERVER_MODE} defaults to development, so it is present here.
 * The status code and the {@code exception} field are asserted independently of that and are the actual subject of
 * this test - {@code detail} only confirms which arithmetic error was reported.
 */
class Issue5647SqlArithmeticHttpStatusIT extends BaseGraphServerTest {

  @Test
  void sqlDivisionByZeroReturns400() throws Exception {
    testEachServer(serverIndex -> {
      executeSql(serverIndex, "CREATE DOCUMENT TYPE Issue5647Div", 200);
      executeSql(serverIndex, "INSERT INTO Issue5647Div SET v = 1", 200);

      final JSONObject divided = executeSql(serverIndex, "SELECT 1/0 AS r FROM Issue5647Div", 400);
      assertThat(divided.getString("exception")).isEqualTo(ArithmeticErrorException.class.getName());
      assertThat(divided.getString("detail")).contains("/ by zero");

      final JSONObject remainder = executeSql(serverIndex, "SELECT 1%0 AS r FROM Issue5647Div", 400);
      assertThat(remainder.getString("exception")).isEqualTo(ArithmeticErrorException.class.getName());
      assertThat(remainder.getString("detail")).contains("% by zero");
    });
  }

  /**
   * The overflow arrives as a stored property because {@code Long.MAX_VALUE} arithmetic has to happen on the
   * {@code Long} overload - an integer literal would widen to {@code long} instead of overflowing.
   */
  @Test
  void sqlIntegerOverflowReturns400OnTheReadPath() throws Exception {
    testEachServer(serverIndex -> {
      createLongMaxValueRecord(serverIndex, "Issue5647Read");

      for (final String expression : new String[] { "v * 2", "v + 1" }) {
        final JSONObject json = executeSql(serverIndex, "SELECT " + expression + " AS r FROM Issue5647Read", 400);
        assertThat(json.getString("exception")).isEqualTo(ArithmeticErrorException.class.getName());
        assertThat(json.getString("detail")).contains("long overflow");
      }
    });
  }

  /**
   * The write path is what made the silent wrap a data-corruption bug: this used to answer 200 and persist
   * {@code -2}. It also exercises the auto-commit wrapper, which re-wraps the failure as a
   * {@code TransactionException} - the shape that historically degraded a client error back to 500.
   */
  @Test
  void sqlIntegerOverflowReturns400OnTheWritePath() throws Exception {
    testEachServer(serverIndex -> {
      createLongMaxValueRecord(serverIndex, "Issue5647Write");

      final JSONObject json = executeSql(serverIndex, "UPDATE Issue5647Write SET v = v * 2", 400);
      assertThat(json.getString("exception")).isEqualTo(ArithmeticErrorException.class.getName());
      assertThat(json.getString("detail")).contains("long overflow");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");

      // nothing was persisted: the wrapped -2 must not have reached storage
      assertThat(executeSql(serverIndex, "SELECT v FROM Issue5647Write", 200).getJSONArray("result")
          .getJSONObject(0).getLong("v")).isEqualTo(Long.MAX_VALUE);
    });
  }

  /**
   * Arithmetic that does have an answer must still answer 200, so the new guards cannot turn ordinary data into a
   * client error. Division by a non-zero divisor and a product that stays in range are the two boundaries.
   */
  @Test
  void sqlArithmeticWithoutOverflowStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      executeSql(serverIndex, "CREATE DOCUMENT TYPE Issue5647Ok", 200);
      executeSql(serverIndex, "CREATE PROPERTY Issue5647Ok.v LONG", 200);
      executeSql(serverIndex, "INSERT INTO Issue5647Ok SET v = 4611686018427387903", 200);

      assertThat(executeSql(serverIndex, "SELECT v * 2 AS r FROM Issue5647Ok", 200).getJSONArray("result")
          .getJSONObject(0).getLong("r")).isEqualTo(9223372036854775806L);

      assertThat(executeSql(serverIndex, "SELECT 10/2 AS r FROM Issue5647Ok", 200).getJSONArray("result")
          .getJSONObject(0).getLong("r")).isEqualTo(5L);
    });
  }

  private void createLongMaxValueRecord(final int serverIndex, final String typeName) throws Exception {
    executeSql(serverIndex, "CREATE DOCUMENT TYPE " + typeName, 200);
    executeSql(serverIndex, "CREATE PROPERTY " + typeName + ".v LONG", 200);
    executeSql(serverIndex, "INSERT INTO " + typeName + " SET v = 9223372036854775807", 200);
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
