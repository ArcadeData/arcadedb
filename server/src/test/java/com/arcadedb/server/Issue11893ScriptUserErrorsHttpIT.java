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

import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11893, over the wire: a SQL script that makes a mistake in its own values or order - a SLEEP on a non-number, an unknown
 * CONSOLE level, a LOCK after a modification or twice, a comparison between two rows, a COMMIT with no transaction open, a BEGIN
 * past the nesting limit - must answer 400 with the message, not 500 with a stack trace in the server log.
 * <p>
 * The engine half is {@code Issue11893ScriptUserErrorsTest}; this pins the HTTP half, which classifies the exception in a ladder of
 * its own in {@code AbstractServerHttpHandler}.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue11893ScriptUserErrorsHttpIT extends BaseGraphServerTest {

  @Test
  void aSleepOnANonNumberReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeScript(serverIndex, "SLEEP \"a\";", 400);
      assertThat(json.getString("exception")).isEqualTo(TypeMismatchException.class.getName());
      assertThat(json.getString("detail")).contains("Invalid value for SLEEP");
    });
  }

  @Test
  void anUnknownConsoleLevelReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeScript(serverIndex, "CONSOLE.nope \"x\";", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSQLParsingException.class.getName());
      assertThat(json.getString("detail")).contains("Unsupported CONSOLE level 'nope'");
    });
  }

  @Test
  void aLockAfterAModificationReturns400() throws Exception {
    testEachServer(serverIndex -> {
      createType(serverIndex);
      final JSONObject json = executeScript(serverIndex, "INSERT INTO S11893 SET k = 2; LOCK TYPE S11893;", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("Explicit lock must be acquired before any modification");
    });
  }

  @Test
  void aSecondLockReturns400() throws Exception {
    testEachServer(serverIndex -> {
      createType(serverIndex);
      final JSONObject json = executeScript(serverIndex, "LOCK TYPE S11893; LOCK TYPE S11893;", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("Explicit lock already acquired");
    });
  }

  @Test
  void aComparisonBetweenTwoRowsReturns400() throws Exception {
    testEachServer(serverIndex -> {
      createType(serverIndex);
      final JSONObject json = executeScript(serverIndex,
          "LET $x = SELECT FROM S11893 LIMIT 1; IF ($x[0] < $x[0]) { RETURN 1; }", 400);
      assertThat(json.getString("exception")).isEqualTo(TypeMismatchException.class.getName());
      assertThat(json.getString("detail")).contains("Cannot compare a record with a record using '<'");
    });
  }

  @Test
  void aCommitInALoopWithNoTransactionLeftReturns400() throws Exception {
    testEachServer(serverIndex -> {
      // the first COMMIT ends the request's own transaction, as it always did; the second has none left to end
      final JSONObject json = executeScript(serverIndex, "LET $i = 0; WHILE ($i < 2) { LET $i = $i + 1; COMMIT; }", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSQLParsingException.class.getName());
      assertThat(json.getString("detail")).contains("Found COMMIT statement without a BEGIN");
    });
  }

  @Test
  void aBeginPastTheNestingLimitReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeScript(serverIndex, "LET $i = 0; WHILE ($i < 5) { LET $i = $i + 1; BEGIN; }", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("nested transactions");
    });
  }

  /** The counter-case: a script that makes none of these mistakes still answers 200, so the 400s are the engine's verdict. */
  @Test
  void aWellFormedScriptStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      createType(serverIndex);
      executeScript(serverIndex, "LET $x = 1; SLEEP $x; CONSOLE.log \"x\"; LOCK TYPE S11893; INSERT INTO S11893 SET k = 3; RETURN 1;",
          200);
    });
  }

  private void createType(final int serverIndex) throws Exception {
    executeScript(serverIndex, "CREATE DOCUMENT TYPE S11893 IF NOT EXISTS; INSERT INTO S11893 SET k = 1;", 200);
  }

  private JSONObject executeScript(final int serverIndex, final String script, final int expectedStatus) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:" + getServerHttpPort(serverIndex) + "/api/v1/command/graph").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try {
      final JSONObject payload = new JSONObject().put("language", "sqlscript").put("command", script);
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }

      final int statusCode = connection.getResponseCode();
      final String response = statusCode < 400 ? readResponse(connection) : readError(connection);

      assertThat(statusCode).as("expected %d, got %d (body=%s)", expectedStatus, statusCode, response).isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
