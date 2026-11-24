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
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12809, over the wire: a property attribute, an index definition, a script statement or a procedure argument the statement
 * alone decides is wrong answers 400 with its message, and so does a constraint the stored records violate, never 500 with a
 * stack trace in the server log. The engine half is {@code Issue12809ClientMistakesTest}; this pins the status each type is
 * answered with by {@code AbstractServerHttpHandler}.
 */
class Issue12809ClientMistakesHttpIT extends BaseGraphServerTest {

  @Test
  void propertyAttributeMistakesReturn400() throws Exception {
    testEachServer(serverIndex -> {
      createType(serverIndex);
      assertAnswer(serverIndex, "sql", "ALTER PROPERTY Q12809.k MANDATORY 'x'", 400, InvalidArgumentException.class,
          "MANDATORY takes true or false, not `x`");
      assertAnswer(serverIndex, "sql", "ALTER PROPERTY Q12809.k MIN 1.5", 400, InvalidArgumentException.class,
          "The MIN '1.5' of property 'Q12809.k' cannot be read as INTEGER");
      assertAnswer(serverIndex, "sql", "ALTER PROPERTY Q12809.k REGEXP '[a-z]+'", 400, InvalidArgumentException.class,
          "matches no text a value of INTEGER can be written as");
      assertAnswer(serverIndex, "sql", "CREATE PROPERTY Q12809.p1 STRING (custom 'x')", 400, CommandSQLParsingException.class,
          "Invalid attribute definition: 'custom'");
      final JSONObject single = assertAnswer(serverIndex, "sql", "CREATE PROPERTY Q12809.p4 BYTE (mandatory -1)", 400,
          InvalidArgumentException.class, "MANDATORY takes true or false, not `-1`");
      assertThat(single.getString("detail")).isEqualTo("MANDATORY takes true or false, not `-1`");
    });
  }

  @Test
  void aValueThatNeedsARecordReturns400() throws Exception {
    testEachServer(serverIndex -> {
      createType(serverIndex);
      assertAnswer(serverIndex, "sql", "ALTER PROPERTY Q12809.k mandatory @rid", 400, CommandSemanticException.class,
          "the value must be a constant");
      assertAnswer(serverIndex, "sql", "ALTER TYPE Q12809 CUSTOM x = @rid", 400, CommandSemanticException.class,
          "the value must be a constant");
      assertAnswer(serverIndex, "sqlscript", "CONSOLE.log @rid; RETURN 1;", 400, CommandSemanticException.class,
          "the value must be a constant");
    });
  }

  @Test
  void indexDefinitionMistakesReturn400() throws Exception {
    testEachServer(serverIndex -> {
      createType(serverIndex);
      assertAnswer(serverIndex, "sql", "CREATE INDEX ON Q12809 (m BY KEY) COLUMNAR", 400, IllegalArgumentException.class,
          "A COLUMNAR index does not support BY KEY or BY VALUE");
      assertAnswer(serverIndex, "sql", "CREATE INDEX ON Q12809 (s, k) COLUMNAR", 400, IllegalArgumentException.class,
          "A COLUMNAR index is defined on exactly one property");
      assertAnswer(serverIndex, "sql", "CREATE INDEX ON Q12809 (k) FULL_TEXT", 400, IllegalArgumentException.class,
          "Full text index can only be defined on STRING properties, found: INTEGER");
      assertAnswer(serverIndex, "sql", "CREATE INDEX ON Q12809 (k) GEOSPATIAL", 400, IllegalArgumentException.class,
          "Geospatial index can only be defined on STRING properties, found: INTEGER");
    });
  }

  @Test
  void scriptAndSettingMistakesReturn400() throws Exception {
    testEachServer(serverIndex -> {
      createType(serverIndex);
      assertAnswer(serverIndex, "sqlscript", "BEGIN; INSERT INTO Q12809 SET k = 1; COMMIT RETRY 0;", 400,
          CommandSQLParsingException.class, "Invalid retry number 0");
      assertAnswer(serverIndex, "sql", "ALTER DATABASE `arcadedb.dateFormat` null", 400, IllegalArgumentException.class,
          "arcadedb.dateFormat");
    });
  }

  @Test
  void procedureArgumentMistakesReturn400() throws Exception {
    testEachServer(serverIndex -> {
      createType(serverIndex);
      assertAnswer(serverIndex, "opencypher", "CALL db.index.fulltext.queryNodes('Q12809[s]', 'abc')", 400,
          InvalidArgumentException.class, "Index 'Q12809[s]' is not a full-text index");
      assertAnswer(serverIndex, "opencypher", "CALL db.index.fulltext.queryNodes(null, 'x')", 400, InvalidArgumentException.class,
          "db.index.fulltext.queryNodes(): indexName cannot be null");
      assertAnswer(serverIndex, "opencypher", "CALL db.index.vector.queryNodes(null, 3, [1.0])", 400,
          InvalidArgumentException.class, "db.index.vector.queryNodes(): indexName cannot be null");
    });
  }

  @Test
  void aConstraintTheRecordsViolateReturns400WithItsKind() throws Exception {
    testEachServer(serverIndex -> {
      createType(serverIndex);
      // the SQL spellings answered so already (issue #12436); the openCypher type constraint answered 500
      assertThat(assertAnswer(serverIndex, "sql", "CREATE PROPERTY Q12809.p2 BOOLEAN (MANDATORY true)", 400, ValidationException.class,
          "Cannot create property 'Q12809.p2': record #").getString("error")).isEqualTo("Constraint violated (MANDATORY)");
      assertThat(assertAnswer(serverIndex, "sql", "ALTER PROPERTY Q12809.s notnull true", 400, ValidationException.class,
          "Cannot set NOTNULL: record #").getString("error")).isEqualTo("Constraint violated (NOT_NULL)");
      execute(serverIndex, "sql", "UPDATE Q12809 SET loose = 'x' WHERE k = 1", 200);
      assertThat(assertAnswer(serverIndex, "opencypher", "CREATE CONSTRAINT q12809_loose_int FOR (n:Q12809) REQUIRE n.loose IS :: INTEGER",
          400, ValidationException.class, "Cannot require Q12809.loose IS :: INTEGER: record #").getString("error")).isEqualTo(
          "Constraint violated (TYPE)");
    });
  }

  private void createType(final int serverIndex) throws Exception {
    for (final String statement : new String[] { "CREATE VERTEX TYPE Q12809 IF NOT EXISTS", "CREATE PROPERTY Q12809.k IF NOT EXISTS INTEGER",
        "CREATE PROPERTY Q12809.s IF NOT EXISTS STRING", "CREATE PROPERTY Q12809.m IF NOT EXISTS MAP",
        "CREATE INDEX IF NOT EXISTS ON Q12809 (s) NOTUNIQUE", "DELETE FROM Q12809", "INSERT INTO Q12809 SET k = 1, s = 'a'",
        "INSERT INTO Q12809 SET k = 2, s = null" })
      execute(serverIndex, "sql", statement, 200);
  }

  private JSONObject assertAnswer(final int serverIndex, final String language, final String command, final int expectedStatus,
      final Class<? extends Throwable> expectedException, final String expectedDetail) throws Exception {
    final JSONObject json = execute(serverIndex, language, command, expectedStatus);
    assertThat(json.getString("exception")).as(command).isEqualTo(expectedException.getName());
    assertThat(json.getString("detail")).as(command).contains(expectedDetail);
    return json;
  }

  private JSONObject execute(final int serverIndex, final String language, final String command, final int expectedStatus)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(getServerHttpUrl(serverIndex, "/api/v1/command/graph"))
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try {
      final JSONObject payload = new JSONObject().put("language", language).put("command", command);
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }

      final int statusCode = connection.getResponseCode();
      final String response = statusCode < 400 ? readResponse(connection) : readError(connection);

      assertThat(statusCode).as("%s: expected %d, got %d (body=%s)", command, expectedStatus, statusCode, response)
          .isEqualTo(expectedStatus);

      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
