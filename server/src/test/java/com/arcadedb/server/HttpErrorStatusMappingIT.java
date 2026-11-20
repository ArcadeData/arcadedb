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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.UnknownFunctionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.index.fulltext.FullTextQueryParseException;
import com.arcadedb.log.LogManager;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How {@code POST /api/v1/command} maps a failing SQL statement (or a malformed request) to an HTTP status: a client mistake
 * (a parsing error, an arithmetic error, a violated constraint, a wrong field type) answers 4xx with the real cause, never a
 * 500 "Error on transaction commit". The Cypher side of the same mapping is {@link HttpCypherErrorStatusMappingIT}.
 * <p>
 * Each group of tests below keeps the explanation of the issue it was written for.
 */
class HttpErrorStatusMappingIT extends BaseGraphServerTest {

  /*
   * Test for issue #3155: HTTP error responses should include full exception cause chain.
   */

  // Regression: #3155 an HTTP error body carries the cause chain in 'detail'
  @Test
  void nestedExceptionInHttpError() throws Exception {
    testEachServer(serverIndex -> {
      // Execute a command that will produce a nested exception (invalid SQL syntax with a cause chain)
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:" + getServerHttpPort(serverIndex) + "/api/v1/command/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      // Use a query referencing a non-existent function to trigger an error with nested exception
      formatPayload(connection, "sql",
          "SELECT nonExistentFunction() FROM V", null, new HashMap<>());
      connection.connect();

      try {
        final int responseCode = connection.getResponseCode();
        LogManager.instance().log(this, Level.INFO, "Response code: %d", null, responseCode);

        // Should get a non-2xx response
        assertThat(responseCode).isGreaterThanOrEqualTo(400);

        // Read error response
        final String errorResponse = readError(connection);
        LogManager.instance().log(this, Level.INFO, "Error Response: %s", null, errorResponse);

        // Parse the error response
        final JSONObject errorJson = new JSONObject(errorResponse);
        assertThat(errorJson.has("error")).isTrue();
        assertThat(errorJson.has("detail")).isTrue();

        final String detail = errorJson.getString("detail");
        LogManager.instance().log(this, Level.INFO, "Error detail: %s", null, detail);

        // The detail should contain meaningful error information
        assertThat(detail).as("Error detail should contain error information")
            .isNotEmpty();

      } finally {
        connection.disconnect();
      }
    });
  }

  /*
   * Regression test for issue #5202: SQL statements using clauses that ArcadeDB's SQL dialect does not
   * support ({@code HAVING}, {@code UNION}, {@code INTERSECT}) fail at parse time with a
   * {@link CommandSQLParsingException}. Over HTTP these must surface as 400 Bad Request with the
   * descriptive parsing message, never 500 "Error on transaction commit". The query text is invalid
   * (a client error), so no transaction commit is even involved - the failure happens during parsing,
   * before execution. This is the SQL sibling of issue #5191 (Cypher validation): the same auto-commit
   * wrapper in {@code DatabaseAbstractHandler} wraps the parsing exception in a {@code TransactionException},
   * and {@code AbstractServerHttpHandler} now unwraps {@code CommandParsingException} (superclass of
   * {@code CommandSQLParsingException}) to report 400 with the real cause.
   */

  // Regression: #5202 unsupported SQL clauses are a 400 parsing error, not a 500 commit failure
  @Test
  void havingReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeSql(serverIndex, "SELECT count(*) AS cnt FROM A HAVING count(*) > 0", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSQLParsingException.class.getName());
      // The real parsing message must reach the client, not "Error on transaction commit".
      assertThat(json.getString("detail")).contains("SQL syntax error");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5202 unsupported SQL clauses are a 400 parsing error, not a 500 commit failure
  @Test
  void unionReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeSql(serverIndex, "SELECT value FROM A UNION SELECT value FROM A", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSQLParsingException.class.getName());
      assertThat(json.getString("detail")).contains("SQL syntax error");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5202 unsupported SQL clauses are a 400 parsing error, not a 500 commit failure
  @Test
  void intersectReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeSql(serverIndex, "SELECT value FROM A INTERSECT SELECT value FROM A", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSQLParsingException.class.getName());
      assertThat(json.getString("detail")).contains("SQL syntax error");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  /*
   * Regression test for issue #5222: the HTTP command endpoint used an unchecked Java cast to {@link String}
   * on the {@code language} and {@code command} request fields. A number, array or object value produced a
   * raw {@link ClassCastException} reported as HTTP 500 and leaked internal Java class names to the client.
   * These are client input errors (wrong JSON field type) and must surface as HTTP 400 with a clean,
   * field-level validation message.
   */

  // Regression: #5222 a non-string language/command field is a 400, not a ClassCastException 500
  @Test
  void languageAsNumberReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = postPayload(serverIndex, new JSONObject().put("language", 123).put("command", "RETURN 1"), 400);
      assertThat(json.getString("detail")).contains("language").contains("string");
      assertThat(json.getString("error")).doesNotContain("ClassCastException");
    });
  }

  // Regression: #5222 a non-string language/command field is a 400, not a ClassCastException 500
  @Test
  void languageAsArrayReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = postPayload(serverIndex,
          new JSONObject().put("language", new JSONArray().put("cypher")).put("command", "RETURN 1"), 400);
      assertThat(json.getString("detail")).contains("language").contains("string");
    });
  }

  // Regression: #5222 a non-string language/command field is a 400, not a ClassCastException 500
  @Test
  void languageAsObjectReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = postPayload(serverIndex,
          new JSONObject().put("language", new JSONObject().put("key", "value")).put("command", "RETURN 1"), 400);
      assertThat(json.getString("detail")).contains("language").contains("string");
    });
  }

  // Regression: #5222 a non-string language/command field is a 400, not a ClassCastException 500
  @Test
  void commandAsNumberReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = postPayload(serverIndex, new JSONObject().put("language", "cypher").put("command", 123), 400);
      assertThat(json.getString("detail")).contains("command").contains("string");
      assertThat(json.getString("error")).doesNotContain("ClassCastException");
    });
  }

  // Regression: #5222 a non-string language/command field is a 400, not a ClassCastException 500
  @Test
  void commandAsArrayReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = postPayload(serverIndex,
          new JSONObject().put("language", "cypher").put("command", new JSONArray().put("RETURN 1")), 400);
      assertThat(json.getString("detail")).contains("command").contains("string");
    });
  }

  // Regression: #5222 a non-string language/command field is a 400, not a ClassCastException 500
  @Test
  void commandAsObjectReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = postPayload(serverIndex,
          new JSONObject().put("language", "cypher").put("command", new JSONObject().put("query", "RETURN 1")), 400);
      assertThat(json.getString("detail")).contains("command").contains("string");
    });
  }

  // Regression: #5222 a non-string language/command field is a 400, not a ClassCastException 500
  @Test
  void validStringFieldsStillWork() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = postPayload(serverIndex, new JSONObject().put("language", "cypher").put("command", "RETURN 1 as one"), 200);
      assertThat(json.has("result")).isTrue();
    });
  }

  /*
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

  // Regression: #5545 SQL abs() overflow is a 400 arithmetic error like in Cypher
  @Test
  void sqlAbsOverflowReturns400OnTheReadPath() throws Exception {
    testEachServer(serverIndex -> {
      createLongMinValueRecord(serverIndex, "Issue5545Read");

      final JSONObject json = executeSql(serverIndex, "SELECT abs(v) AS r FROM Issue5545Read", 400);
      assertThat(json.getString("exception")).isEqualTo(ArithmeticErrorException.class.getName());
      assertThat(json.getString("detail")).contains("long overflow");
    });
  }

  // Regression: #5545 SQL abs() overflow is a 400 arithmetic error like in Cypher
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

  // Regression: #5545 SQL abs() overflow is a 400 arithmetic error like in Cypher
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

  /*
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

  // Regression: #5647 SQL division by zero and long overflow are 400 arithmetic errors
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

  // Regression: #5647 SQL division by zero and long overflow are 400 arithmetic errors
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

  // Regression: #5647 SQL division by zero and long overflow are 400 arithmetic errors
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

  // Regression: #5647 SQL division by zero and long overflow are 400 arithmetic errors
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

  /*
   * Issue #7862, over the wire: a Lucene expression the parser refuses, reaching the server through
   * {@code SEARCH_INDEX()} or a {@code db.index.fulltext.query*} procedure, must answer 400 with the parse
   * message - not 500 with a stack trace in the server log, which is what #7393 established for the hybrid and
   * full-text search APIs and what these four call sites were still doing.
   * <p>
   * The engine half is {@code FullTextMalformedQueryTest.EveryCallSiteClientError}; this pins the HTTP half, which
   * is a ladder of its own in {@code AbstractServerHttpHandler} rather than {@code ErrorCategory}, and therefore
   * had to be given its own arm.
   */

  // Regression: #7862 a malformed full-text query is a 400 parse error, not a 500
  @Test
  void aMalformedSearchIndexQueryReturns400() throws Exception {
    testEachServer(serverIndex -> {
      createFullTextIndex(serverIndex);

      final JSONObject json = postCommand(serverIndex, "sql",
          "SELECT FROM Doc7862 WHERE SEARCH_INDEX('Doc7862[content]', 'foo AND') = true", 400);

      assertThat(json.getString("exception")).isEqualTo(FullTextQueryParseException.class.getName());
      assertThat(json.getString("detail")).contains("Invalid search query");
      assertThat(json.getString("error")).doesNotContain("Internal error");
    });
  }

  // Regression: #7862 a malformed full-text query is a 400 parse error, not a 500
  @Test
  void aMalformedCypherFullTextProcedureQueryReturns400() throws Exception {
    testEachServer(serverIndex -> {
      createFullTextIndex(serverIndex);

      final JSONObject json = postCommand(serverIndex, "cypher",
          "CALL db.index.fulltext.queryNodes('Doc7862[content]', 'foo AND')", 400);

      assertThat(json.getString("exception")).isEqualTo(FullTextQueryParseException.class.getName());
      assertThat(json.getString("detail")).contains("Invalid search query");
    });
  }

  // Regression: #7862 a malformed full-text query is a 400 parse error, not a 500
  /**
   * The counter-case: a well-formed query on the same route still answers 200, so the 400s above are the
   * parser's verdict and not the route being broken.
   */
  @Test
  void aWellFormedSearchIndexQueryStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      createFullTextIndex(serverIndex);

      postCommand(serverIndex, "sql",
          "SELECT FROM Doc7862 WHERE SEARCH_INDEX('Doc7862[content]', 'gearbox') = true", 200);
    });
  }

  private void createFullTextIndex(final int serverIndex) throws Exception {
    postCommand(serverIndex, "sqlscript",
        "CREATE VERTEX TYPE Doc7862 IF NOT EXISTS;"
            + "CREATE PROPERTY Doc7862.content IF NOT EXISTS STRING;"
            + "CREATE INDEX IF NOT EXISTS ON Doc7862 (content) FULL_TEXT;", 200);
  }

  /*
   * Issue #11314 over HTTP: a value a declared property constraint refuses is the caller's mistake, answered 400 with the
   * {@link ValidationException} and the constraint it violates in {@code exceptionArgs}, and the Java client rebuilds the exception
   * with its {@link ValidationException.Kind}, as it rebuilds a taken name with its kind (#10153). A call of an unknown function is
   * a 400 too, not the 500 of a server fault.
   */

  // Regression: #11314 a violated property constraint is a 400 carrying its kind
  @Test
  void aViolatedConstraintAnswers400WithItsKind() throws Exception {
    assertThat(httpCommand("sql", "CREATE DOCUMENT TYPE H11314").statusCode()).isEqualTo(200);
    assertThat(httpCommand("sql", "CREATE PROPERTY H11314.nn STRING (notnull true)").statusCode()).isEqualTo(200);
    assertThat(httpCommand("sql", "CREATE PROPERTY H11314.mx STRING (max 3)").statusCode()).isEqualTo(200);

    assertRefused("INSERT INTO H11314 SET nn = null", "NOT_NULL");
    assertRefused("INSERT INTO H11314 SET nn = 'a', mx = 'abcdef'", "MAX_LENGTH");
  }

  // Regression: #11314 a violated property constraint is a 400 carrying its kind
  @Test
  void anUnknownFunctionAnswers400() throws Exception {
    final HttpResponse<String> response = httpCommand("sql", "SELECT foo(1)");
    assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
    assertThat(new JSONObject(response.body()).getString("exception")).isEqualTo(UnknownFunctionException.class.getName());
  }

  // Regression: #11314 a violated property constraint is a 400 carrying its kind
  @Test
  void theJavaClientRebuildsTheViolationWithItsKind() {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(0), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {
      database.command("sql", "CREATE DOCUMENT TYPE R11314");
      database.command("sql", "CREATE PROPERTY R11314.i INTEGER (max 10)");

      assertThatThrownBy(() -> database.command("sql", "INSERT INTO R11314 SET i = 11"))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("greater than")
          .satisfies(e -> assertThat(((ValidationException) e).getKind()).isEqualTo(ValidationException.Kind.MAX));
    }
  }

  private void assertRefused(final String statement, final String kind) throws Exception {
    final HttpResponse<String> response = httpCommand("sql", statement);
    assertThat(response.statusCode()).as("%s: %s", statement, response.body()).isEqualTo(400);
    final JSONObject body = new JSONObject(response.body());
    assertThat(body.getString("exception")).isEqualTo(ValidationException.class.getName());
    assertThat(body.getString("exceptionArgs", null)).isEqualTo(kind);
  }

  private HttpResponse<String> httpCommand(final String language, final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", language).put("command", command).toString()))
        .build();
    try (final HttpClient client = HttpClient.newHttpClient()) {
      return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
  }

  /*
   * Issues #11850 and #11796 over {@code POST /api/v1/command}: statements with a user error, and statements the engine does not
   * implement, answered HTTP 500 (a NullPointerException, a StackOverflowError, a bare UnsupportedOperationException, refusals
   * raised as CommandExecutionException). Each now answers 200 with a sensible result or 400 naming the problem.
   */

  // Regression: #11850 statement errors answer 400 and self references 200, never 500
  @Test
  void statementErrorsAnswer400AndSelfReferencesAnswer200() throws Exception {
    testEachServer(serverIndex -> {
      for (final String ddl : new String[] { "CREATE VERTEX TYPE S11850V IF NOT EXISTS", "CREATE PROPERTY S11850V.i IF NOT EXISTS INTEGER",
          "CREATE PROPERTY S11850V.s IF NOT EXISTS STRING", "CREATE INDEX IF NOT EXISTS ON S11850V (i) NOTUNIQUE",
          "CREATE INDEX IF NOT EXISTS ON S11850V (s) FULL_TEXT", "CREATE DOCUMENT TYPE S11850D IF NOT EXISTS",
          "CREATE VERTEX TYPE S11850P IF NOT EXISTS", "CREATE EDGE TYPE S11850K IF NOT EXISTS",
          "INSERT INTO S11850V SET i = 1, s = 'one word'", "INSERT INTO S11850D SET x = 1" })
        postCommand(serverIndex, "sql", ddl, 200);
      postCommand(serverIndex, "opencypher", "CREATE (:S11850P {i: 20})-[:S11850K]->(:S11850P {i: 21})", 200);

      // 200: @rid of a record not saved yet is null, a saved record holding itself is a link and its JSON ends
      postCommand(serverIndex, "sql", "INSERT INTO S11850D SET m = @rid", 200);
      final JSONObject updated = postCommand(serverIndex, "sql", "UPDATE S11850D SET self = @this RETURN AFTER", 200);
      assertThat(updated.toString()).contains("\"self\"");
      for (final String cypher : new String[] { "MATCH (n:S11850P {i: 20}) SET n.z = n RETURN n.z",
          "MATCH (n:S11850P {i: 20}) SET n.z = [1, n] RETURN n.z", "MATCH (n:S11850P {i: 20}) SET n += {z: n} RETURN n.z",
          "MATCH p = (n:S11850P {i: 20})-[:S11850K]->() SET n.z = p RETURN n.z" })
        postCommand(serverIndex, "opencypher", cypher, 200);

      // 400: the statement's error
      for (final String sql : new String[] { "INSERT INTO S11850D SET s = @this", "SELECT percentile(i, null) FROM S11850V",
          "SELECT FROM index:`S11850V[i]` WHERE asSet()", "SELECT search_index('S11850V[s]', null) FROM S11850V",
          "SELECT expand(true, 1)", "SELECT concat(list(mode(i))) FROM S11850V",
          // issue #11796: never implemented, refused with a message naming the statement
          "ALTER BUCKET s11850d NAME renamed", "ALTER BUCKET s11850d CUSTOM k = 'v'", "TRUNCATE RECORD #1:0",
          "TRUNCATE RECORD [#1:0]", "ALIGN DATABASE" }) {
        final JSONObject error = postCommand(serverIndex, "sql", sql, 400);
        assertThat(error.getString("detail", "")).as(sql).isNotEmpty();
      }
      for (final String cypher : new String[] { "RETURN duration(1) AS d", "RETURN duration(true) AS d",
          "WITH null AS x MERGE (n:S11850P {i: x}) RETURN n" })
        postCommand(serverIndex, "opencypher", cypher, 400);
    });
  }

  private JSONObject executeSql(final int serverIndex, final String command, final int expectedStatus) throws Exception {
    return postCommand(serverIndex, "sql", command, expectedStatus);
  }

  /**
   * Posts {@code payload} to {@code /api/v1/command/graph} on {@code serverIndex}, asserts the HTTP status and returns the
   * parsed body: the response stream below 400, the error stream from 400 on.
   */
  private JSONObject postPayload(final int serverIndex, final JSONObject payload, final int expectedStatus) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(getServerHttpUrl(serverIndex, "/api/v1/command/graph"))
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try {
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }
      final int statusCode = connection.getResponseCode();
      final String response;
      try (final InputStream in = statusCode < 400 ? connection.getInputStream() : connection.getErrorStream()) {
        response = in != null ? new String(in.readAllBytes(), StandardCharsets.UTF_8) : "{}";
      }
      assertThat(statusCode).as("%s answered %d, expected %d (body=%s)", payload, statusCode, expectedStatus, response)
          .isEqualTo(expectedStatus);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }

  private JSONObject postCommand(final int serverIndex, final String language, final String command, final int expectedStatus)
      throws Exception {
    return postPayload(serverIndex, new JSONObject().put("language", language).put("command", command), expectedStatus);
  }
}
