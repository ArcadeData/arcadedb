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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How {@code POST /api/v1/command} maps a failing Cypher statement to an HTTP status: a query mistake (a validation error, a
 * function argument of the wrong type, an arithmetic error) answers 400 with the real cause in {@code detail}, never a 500
 * "Error on transaction commit", while the legal counter-cases (null propagation, IEEE 754 float arithmetic) keep answering
 * 200. The SQL side of the same mapping is {@link HttpErrorStatusMappingIT}.
 * <p>
 * Each group of tests below keeps the explanation of the issue it was written for.
 */
class HttpCypherErrorStatusMappingIT extends BaseGraphServerTest {

  /*
   * Regression test for issue #5191: a Cypher validation error (invalid query text) must surface over
   * HTTP as 400 Bad Request with the descriptive validation message, never 500 "Error on transaction
   * commit". These are client errors (the query is malformed), not internal server faults - Neo4j,
   * Memgraph and FalkorDB all return descriptive client errors for the same queries. ArcadeDB detected
   * the problem correctly ({@link CommandParsingException}) but mis-classified it as a transaction-commit
   * failure because the exception was wrapped in a {@code TransactionException} by the auto-commit wrapper.
   */

  // Regression: #5191 a Cypher validation error is a 400, not a 500 commit failure
  @Test
  void mergeRebindReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "MERGE (a:U {id: 1}) MERGE (a:U {id: 2}) RETURN a.id", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandParsingException.class.getName());
      // The real validation message must reach the client, not "Error on transaction commit".
      assertThat(json.getString("detail")).contains("VariableAlreadyBound").contains("already defined");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5191 a Cypher validation error is a 400, not a 500 commit failure
  @Test
  void unknownVariableReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN unknownVariable", 400);
      // CommandSemanticException extends CommandParsingException, so it is mapped by the same 400 branch.
      assertThat(json.getString("exception")).contains("CommandSemanticException");
      assertThat(json.getString("detail")).contains("UndefinedVariable");
    });
  }

  // Regression: #5191 a Cypher validation error is a 400, not a 500 commit failure
  @Test
  void syntaxErrorReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN 1 +", 400);
      // Whatever the specific parsing message, it must be a client error (400) exposing the real cause.
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  /*
   * Regression test for issues #5203 and #5204: a Cypher runtime type error - passing an unsupported
   * argument type to a scalar function such as {@code toString(list)} (#5203) or {@code type(node)}
   * (#5204) - must surface over HTTP as 400 Bad Request with the descriptive type message, never 500
   * "Error on transaction commit". These are client errors (the query passes the wrong value type to a
   * function), classified by the openCypher TCK as a runtime TypeError/InvalidArgumentValue. ArcadeDB
   * detected the problem correctly but mis-classified it as a transaction-commit failure because the
   * type-validation exception was wrapped in a {@code TransactionException} by the auto-commit wrapper.
   */

  // Regression: #5203 a Cypher function argument type error is a 400 (also #5204)
  @Test
  void toStringOnListReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN toString([1, 2, 3]) AS result", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      // The real type message must reach the client, not "Error on transaction commit".
      assertThat(json.getString("detail")).contains("toString").contains("List");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5203 a Cypher function argument type error is a 400 (also #5204)
  @Test
  void toStringOnEmptyListReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN toString([]) AS result", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5203 a Cypher function argument type error is a 400 (also #5204)
  @Test
  void typeOnNodeReturns400() throws Exception {
    testEachServer(serverIndex -> {
      executeCypher(serverIndex, "CREATE (:TypeErrNode {id: 1})", 200);
      final JSONObject json = executeCypher(serverIndex, "MATCH (a:TypeErrNode) RETURN type(a) AS result", 400);
      // A statically-inferable node argument is caught by the semantic validator as a CommandParsingException,
      // while a type that can only be resolved at runtime reaches TypeFunction as a CommandSemanticException;
      // both extend CommandParsingException and both must be client errors (400), never a 500 commit failure.
      assertThat(json.getString("exception")).contains("Command").contains("Exception");
      // The real type message must reach the client, not "Error on transaction commit".
      assertThat(json.getString("detail")).contains("type").contains("relationship");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  /*
   * Regression test for issue #5221: Cypher integer division or modulo by zero is a runtime arithmetic
   * error, not a transaction-commit failure. The engine detects the failure correctly (detail
   * {@code "/ by zero"} / {@code "% by zero"}) but the auto-commit transaction wrapper in
   * DatabaseAbstractHandler re-wrapped the {@link CommandExecutionException} in a
   * {@code TransactionException}, so before the #5219 fix the HTTP layer degraded the response to 500
   * "Error on transaction commit", hiding the real cause and wrongly implying a durability/consistency
   * problem. The honest label is "Cannot execute command" (a runtime execution error), matching Neo4j
   * and Memgraph which both report the equivalent integer operations as query-level arithmetic errors.
   * <p>
   * <b>The status became 400 in issue #5602.</b> This test used to pin it to 500, reasoning that the failure is
   * data-dependent in the general case ({@code n.a / n.b}) and therefore a runtime error rather than a static client
   * error. That argument does not survive contact with the rest of the engine: {@code abs(n.name)} is data-dependent
   * in exactly the same way and has answered 400 since #5484. What decides the status is whose mistake it is, and a
   * zero divisor is the caller's - Neo4j agrees, classifying the whole arithmetic category (division by zero and
   * 64-bit overflow alike) as {@code Neo.ClientError.Statement.ArithmeticError}. #5602 moved the category together
   * rather than leaving overflow and division by zero on opposite sides of the 4xx/5xx line.
   * <p>
   * What #5221 was actually about is unchanged and still asserted here: the label stays "Cannot execute command"
   * rather than the misleading "Error on transaction commit", and the detail still names the real cause. The
   * exception is now {@link ArithmeticErrorException}, which extends {@link CommandExecutionException}, so the #5219
   * classification an embedded caller sees is intact. Float division/modulo by zero keep IEEE 754 semantics
   * (Infinity/NaN) and are not errors.
   */

  // Regression: #5221 integer division by zero is an arithmetic error, not a commit failure
  @Test
  void integerDivisionByZeroReturnsArithmeticError() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN 5 / 0", 400);
      assertThat(json.getString("exception")).isEqualTo(ArithmeticErrorException.class.getName());
      assertThat(json.getString("detail")).contains("/ by zero");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5221 integer division by zero is an arithmetic error, not a commit failure
  @Test
  void integerModuloByZeroReturnsArithmeticError() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN 5 % 0", 400);
      assertThat(json.getString("exception")).isEqualTo(ArithmeticErrorException.class.getName());
      assertThat(json.getString("detail")).contains("% by zero");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5221 integer division by zero is an arithmetic error, not a commit failure
  @Test
  void computedZeroDivisorReturnsArithmeticError() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN 1 / (2 - 2)", 400);
      assertThat(json.getString("exception")).isEqualTo(ArithmeticErrorException.class.getName());
      assertThat(json.getString("detail")).contains("/ by zero");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5221 integer division by zero is an arithmetic error, not a commit failure
  @Test
  void divisionByZeroInsideCaseReturnsArithmeticError() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN CASE WHEN 1 / 0 > 5 THEN 1 ELSE 0 END", 400);
      assertThat(json.getString("exception")).isEqualTo(ArithmeticErrorException.class.getName());
      assertThat(json.getString("detail")).contains("/ by zero");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5221 integer division by zero is an arithmetic error, not a commit failure
  @Test
  void floatDivisionByZeroReturnsInfinity() throws Exception {
    testEachServer(serverIndex -> {
      // Float division by zero keeps IEEE 754 semantics (+Infinity), never an error. JSON has no native
      // infinity, so it is serialized as the token "PosInfinity" (matches Neo4j/Memgraph "Infinity").
      final JSONObject json = executeCypher(serverIndex, "RETURN 5.0 / 0.0 AS result", 200);
      final Object value = json.getJSONArray("result").getJSONObject(0).get("result");
      assertThat(asDouble(value)).isEqualTo(Double.POSITIVE_INFINITY);
    });
  }

  // Regression: #5221 integer division by zero is an arithmetic error, not a commit failure
  @Test
  void floatModuloByZeroReturnsNaN() throws Exception {
    testEachServer(serverIndex -> {
      // Float modulo by zero keeps IEEE 754 semantics (NaN), never an error.
      final JSONObject json = executeCypher(serverIndex, "RETURN 5.0 % 0.0 AS result", 200);
      final Object value = json.getJSONArray("result").getJSONObject(0).get("result");
      assertThat(Double.isNaN(asDouble(value))).isTrue();
    });
  }

  /** JSON has no native Infinity/NaN, so ArcadeDB serializes them as the tokens "PosInfinity"/"NaN". */
  private static double asDouble(final Object value) {
    if (value instanceof Number number)
      return number.doubleValue();
    return switch (value.toString()) {
      case "PosInfinity", "Infinity" -> Double.POSITIVE_INFINITY;
      case "NegInfinity", "-Infinity" -> Double.NEGATIVE_INFINITY;
      case "NaN" -> Double.NaN;
      default -> Double.parseDouble(value.toString());
    };
  }

  // Regression: #5221 integer division by zero is an arithmetic error, not a commit failure
  @Test
  void serverRemainsOperationalAfterArithmeticError() throws Exception {
    testEachServer(serverIndex -> {
      executeCypher(serverIndex, "RETURN 5 / 0", 400);
      // A subsequent valid query must still succeed: the arithmetic error is limited to the failing
      // statement and never damages transaction state or database consistency.
      final JSONObject json = executeCypher(serverIndex, "RETURN 1 AS result", 200);
      final Object value = json.getJSONArray("result").getJSONObject(0).get("result");
      assertThat(((Number) value).intValue()).isEqualTo(1);
    });
  }

  /*
   * Regression test for issue #5224: a Cypher UNION whose branches expose different implicit return-column
   * names must surface over HTTP as 400 Bad Request with the descriptive validation message, never a
   * 500 "Error on transaction commit". Neo4j rejects the same query with
   * "All sub queries in an UNION must have the same return column names".
   */

  // Regression: #5224 a UNION with mismatched column names is a 400
  @Test
  void mismatchedUnionColumnNamesReturns400() throws Exception {
    testEachServer(serverIndex -> {
      // Seed the data (both branches match at least one row).
      executeCypher(serverIndex, "CREATE (a:A {id: 1}), (b:B {id: 2})", 200);

      final JSONObject json = executeCypher(serverIndex,
          "MATCH (a:A) RETURN a.id UNION MATCH (b:B) RETURN b.id", 400);
      assertThat(json.getString("exception")).contains(CommandParsingException.class.getSimpleName());
      assertThat(json.getString("detail")).contains("same return column names");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5224 a UNION with mismatched column names is a 400
  @Test
  void matchingAliasesReturns200() throws Exception {
    testEachServer(serverIndex -> {
      executeCypher(serverIndex, "CREATE (a:A {id: 1}), (b:B {id: 2})", 200);
      executeCypher(serverIndex, "MATCH (a:A) RETURN a.id AS x UNION MATCH (b:B) RETURN b.id AS x", 200);
    });
  }

  /*
   * End-to-end HTTP regression test for issue #5225: a range Cypher predicate against a String bound on an
   * index whose key type was inferred as numeric (e.g. {@code WHERE n.val < "zzz"}) used to return HTTP 500
   * with an internal {@code ClassCastException} ("String cannot be cast to Integer") or a
   * {@code NumberFormatException}. Cross-type comparisons are undefined (null) in Cypher, so the query must
   * complete (HTTP 200) and return the same result as the un-indexed full-scan path. Equality against the text a value was
   * written with finds it, since the property is declared INTEGER and a declaration defines its text form (#9794, #9935).
   * The declaration is made explicitly: the Cypher index used to infer it from the data, which it no longer does (#11579).
   */

  private long countCypher(final String command) throws Exception {
    final JSONArray result = executeCypher(0, command, 200).getJSONArray("result");
    assertThat(result.length()).isEqualTo(1);
    return result.getJSONObject(0).getLong("c");
  }

  // Regression: #5225 a range predicate with a String bound on a numeric index is a 200, not a 500
  @Test
  void mixedTypeRangeDoesNotReturn500() throws Exception {
    executeCypher(0, "CREATE (:BugIdx {id: 1, val: 1})", 200);
    executeCypher(0, "CREATE (:BugIdx {id: 2, val: 2})", 200);
    executeCypher(0, "CREATE (:BugIdx {id: 3})", 200);
    getServerDatabase(0, getDatabaseName()).command("sql", "CREATE PROPERTY BugIdx.val INTEGER");
    executeCypher(0, "CREATE INDEX FOR (n:BugIdx) ON (n.val)", 200);
    executeCypher(0, "MATCH (n:BugIdx) WHERE n.id = 1 SET n.val = \"1\" RETURN n.val", 200);

    // val is declared INTEGER, so the SET stored the integer 1, and since #9794 the text it was written with reads back as
    // that integer (#9935)
    final JSONArray equal = executeCypher(0, "MATCH (n:BugIdx) WHERE n.val = \"1\" RETURN n.id AS id, n.val AS val", 200).getJSONArray("result");
    assertThat(equal.length()).isEqualTo(1);
    assertThat(equal.getJSONObject(0).getInt("id")).isEqualTo(1);
    assertThat(equal.getJSONObject(0).get("val")).isEqualTo(1);
    // A text with no integer form still matches nothing
    assertThat(executeCypher(0, "MATCH (n:BugIdx) WHERE n.val = \"one\" RETURN n.id", 200).getJSONArray("result").length()).isZero();

    // Range predicates against a String bound must complete (no 500), matching the un-indexed result of 0.
    assertThat(countCypher("MATCH (n:BugIdx) WHERE n.val < \"zzz\" RETURN count(n) AS c")).isZero();
    assertThat(countCypher("MATCH (n:BugIdx) WHERE n.val > \"a\" RETURN count(n) AS c")).isZero();
    assertThat(countCypher("MATCH (n:BugIdx) WHERE n.val >= \"a\" AND n.val <= \"zzz\" RETURN count(n) AS c")).isZero();

    // A correctly-typed numeric range still uses the index and returns the right rows.
    assertThat(countCypher("MATCH (n:BugIdx) WHERE n.val >= 1 AND n.val <= 5 RETURN count(n) AS c")).isEqualTo(2);
  }

  /*
   * Regression test for issue #5294: {@code RETURN toFloat(true)} (and the other conversion functions with an
   * unsupported argument type) must surface over HTTP as 400 Bad Request with the descriptive type message,
   * never 500 "Error on transaction commit". These are client errors (the query passes the wrong value type to
   * a scalar function), classified by the openCypher TCK as a runtime TypeError/InvalidArgumentValue. Same class
   * of fix as issue #5203 for {@code toString()}.
   *
   * <p>Behaviour matches the Neo4j reference implementation: {@code toFloat()} accepts only STRING/INTEGER/FLOAT
   * (a BOOLEAN is a type error), whereas {@code toInteger()} additionally converts BOOLEAN to 1/0.
   */

  // Regression: #5294 toFloat(boolean) and friends are a 400 type error
  @Test
  void toFloatOnBooleanTrueReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN toFloat(true) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("toFloat").contains("Boolean");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5294 toFloat(boolean) and friends are a 400 type error
  @Test
  void toFloatOnBooleanFalseReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN toFloat(false) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5294 toFloat(boolean) and friends are a 400 type error
  @Test
  void toBooleanOnFloatReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN toBoolean(1.5) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5294 toFloat(boolean) and friends are a 400 type error
  @Test
  void toFloatOnIntegerStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN toFloat(1) AS r", 200);
      assertThat(json.getJSONArray("result").getJSONObject(0).getDouble("r")).isEqualTo(1.0);
    });
  }

  // Regression: #5294 toFloat(boolean) and friends are a 400 type error
  @Test
  void toIntegerOnBooleanStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN toInteger(true) AS r", 200);
      assertThat(json.getJSONArray("result").getJSONObject(0).getLong("r")).isEqualTo(1L);
    });
  }

  /*
   * Regression test for issue #5296: calling the Cypher {@code left()} or {@code right()} function with a
   * negative length is an invalid argument value and must surface over HTTP as 400 Bad Request with a
   * descriptive message, never 500 "Cannot execute command". Neo4j and Memgraph both return a client error.
   * ArcadeDB already rejected the negative length with a descriptive message, but threw a
   * {@link com.arcadedb.exception.CommandExecutionException}, which the HTTP handler classifies as a
   * server-side runtime error (500). Throwing {@link CommandSemanticException} (which extends
   * CommandParsingException) instead makes the handler report it as a 400 client error.
   *
   * Non-negative controls (positive, zero, larger-than-string) must keep working (200).
   */

  // Regression: #5296 left()/right() with a negative length is a 400
  @Test
  void leftNegativeLengthReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN left('hello', -2) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("left").contains("negative");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5296 left()/right() with a negative length is a 400
  @Test
  void rightNegativeLengthReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN right('hello', -2) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("right").contains("negative");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5296 left()/right() with a negative length is a 400
  @Test
  void computedNegativeLengthReturns400() throws Exception {
    // The impact scenario from the issue: an arithmetic expression that evaluates to a negative length must
    // still be a 400 client error, not an HTTP 500.
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN left('hello', 1 - 3) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5296 left()/right() with a negative length is a 400
  @Test
  void nonNegativeControlsStillWork() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(firstResult(executeCypher(serverIndex, "RETURN left('hello', 2) AS r", 200))).isEqualTo("he");
      assertThat(firstResult(executeCypher(serverIndex, "RETURN right('hello', 2) AS r", 200))).isEqualTo("lo");
      assertThat(firstResult(executeCypher(serverIndex, "RETURN left('hello', 0) AS r", 200))).isEqualTo("");
      assertThat(firstResult(executeCypher(serverIndex, "RETURN right('hello', 0) AS r", 200))).isEqualTo("");
      assertThat(firstResult(executeCypher(serverIndex, "RETURN left('hello', 10) AS r", 200))).isEqualTo("hello");
      assertThat(firstResult(executeCypher(serverIndex, "RETURN right('hello', 10) AS r", 200))).isEqualTo("hello");
    });
  }

  private String firstResult(final JSONObject json) {
    return json.getJSONArray("result").getJSONObject(0).getString("r");
  }

  /*
   * Regression test for issue #5299: the graph introspection functions {@code labels()}, {@code keys()} and
   * {@code properties()} must report an unsupported argument type as a 400 client error, the same way
   * {@code type()} already does since #5204. Passing a scalar such as {@code labels(42)} is a query mistake,
   * not a server failure, and Neo4j rejects it as a type error.
   */

  // Regression: #5299 labels()/keys()/properties() on a scalar are a 400
  @Test
  void labelsOnIntegerReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN labels(42) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("labels").contains("node");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5299 labels()/keys()/properties() on a scalar are a 400
  @Test
  void keysOnIntegerReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN keys(42) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("keys").contains("node");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5299 labels()/keys()/properties() on a scalar are a 400
  @Test
  void propertiesOnIntegerReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN properties(42) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("properties").contains("node");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5299 labels()/keys()/properties() on a scalar are a 400
  @Test
  void nullArgumentStillReturnsNull() throws Exception {
    testEachServer(serverIndex -> {
      // Neo4j returns null for a null argument on all three functions: that behaviour must not regress.
      for (final String function : new String[] { "labels", "keys", "properties" }) {
        final JSONObject json = executeCypher(serverIndex, "RETURN " + function + "(null) AS r", 200);
        assertThat(json.getJSONArray("result").getJSONObject(0).isNull("r"))
            .as("%s(null) must stay null", function)
            .isTrue();
      }
    });
  }

  /*
   * Regression test for issue #5476: {@code RETURN head(42)} (and the same for last()/tail() with any non-list
   * argument) used to answer HTTP 200 with {@code {"r": null}}, so a query with a type error looked like a
   * successful query returning no value. The Cypher signature is {@code head(list :: LIST<ANY>) :: ANY}, so a
   * non-list argument is a client error and must surface as 400 Bad Request with a descriptive type message,
   * matching Neo4j and Memgraph. Same class of fix as issues #5203 and #5294.
   */

  // Regression: #5476 head()/last()/tail() on a non-list are a 400, not a 200 null
  @Test
  void headOnIntegerReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN head(42) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("head()").contains("LIST").contains("INTEGER");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5476 head()/last()/tail() on a non-list are a 400, not a 200 null
  @Test
  void headOnFloatReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN head(3.14) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("head()").contains("FLOAT");
    });
  }

  // Regression: #5476 head()/last()/tail() on a non-list are a 400, not a 200 null
  @Test
  void headOnBooleanReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN head(true) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("head()").contains("BOOLEAN");
    });
  }

  // Regression: #5476 head()/last()/tail() on a non-list are a 400, not a 200 null
  @Test
  void lastAndTailOnNonListReturn400() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(executeCypher(serverIndex, "RETURN last('abc') AS r", 400).getString("detail")).contains("last()")
          .contains("STRING");
      assertThat(executeCypher(serverIndex, "RETURN tail(42) AS r", 400).getString("detail")).contains("tail()");
    });
  }

  // Regression: #5476 head()/last()/tail() on a non-list are a 400, not a 200 null
  @Test
  void headOnListStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN head([42]) AS r", 200);
      assertThat(json.getJSONArray("result").getJSONObject(0).getLong("r")).isEqualTo(42L);
    });
  }

  // Regression: #5476 head()/last()/tail() on a non-list are a 400, not a 200 null
  @Test
  void headOnNullStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeCypher(serverIndex, "RETURN head(null) AS r", 200);
      assertThat(json.getJSONArray("result").getJSONObject(0).isNull("r")).isTrue();
    });
  }

  /*
   * Regression test for issue #5477, reported against the HTTP command API: {@code RETURN size(42)} answered 200 with
   * {@code {"r": null}}, which a client cannot tell apart from legal null propagation. It must be 400 Bad Request with a
   * descriptive type message, while {@code size(null)} keeps answering 200 with {@code null}. Same class of fix as issues
   * #5476, #5294 and #5203.
   */

  // Regression: #5477 size() on a value without a size is a 400, not a 200 null
  @Test
  void sizeOnIntegerReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeOpenCypher(serverIndex, "RETURN size(42) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("size()").contains("INTEGER");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5477 size() on a value without a size is a 400, not a 200 null
  @Test
  void sizeOnBooleanReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeOpenCypher(serverIndex, "RETURN size(true) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("size()").contains("BOOLEAN");
    });
  }

  // Regression: #5477 size() on a value without a size is a 400, not a 200 null
  @Test
  void isEmptyOnIntegerReturns400() throws Exception {
    // isEmpty() already rejected the argument but reported it as a 500 server failure.
    testEachServer(serverIndex -> {
      final JSONObject json = executeOpenCypher(serverIndex, "RETURN isEmpty(42) AS r", 400);
      assertThat(json.getString("detail")).contains("isEmpty()");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5477 size() on a value without a size is a 400, not a 200 null
  @Test
  void sizeOnNullStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeOpenCypher(serverIndex, "RETURN size(null) AS r", 200);
      assertThat(json.getJSONArray("result").getJSONObject(0).isNull("r")).isTrue();
    });
  }

  // Regression: #5477 size() on a value without a size is a 400, not a 200 null
  @Test
  void sizeOnStringListAndMapStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(executeOpenCypher(serverIndex, "RETURN size('abc') AS r", 200).getJSONArray("result").getJSONObject(0)
          .getLong("r")).isEqualTo(3L);
      assertThat(executeOpenCypher(serverIndex, "RETURN size([1,2,3]) AS r", 200).getJSONArray("result").getJSONObject(0)
          .getLong("r")).isEqualTo(3L);
      assertThat(executeOpenCypher(serverIndex, "RETURN size({a:1}) AS r", 200).getJSONArray("result").getJSONObject(0)
          .getLong("r")).isEqualTo(1L);
    });
  }

  /*
   * Regression test for issue #5484, reported against the HTTP command API: {@code RETURN abs('hello')} answered 500
   * "internal server error" with an otherwise correct type message. Handing a STRING to a function declared as
   * {@code abs(input :: INTEGER | FLOAT)} is the client's mistake - Neo4j answers a {@code Neo.ClientError.Statement.TypeError}
   * - so the status must be 400. Same class of fix as issues #5477, #5476, #5294 and #5203.
   */

  // Regression: #5484 abs() on a non-numeric argument is a 400, not a 500
  @Test
  void absOnStringReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeOpenCypher(serverIndex, "RETURN abs('hello') AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("abs()").contains("STRING");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5484 abs() on a non-numeric argument is a 400, not a 500
  @Test
  void absOnBooleanReturns400() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeOpenCypher(serverIndex, "RETURN abs(true) AS r", 400);
      assertThat(json.getString("exception")).isEqualTo(CommandSemanticException.class.getName());
      assertThat(json.getString("detail")).contains("abs()").contains("BOOLEAN");
    });
  }

  // Regression: #5484 abs() on a non-numeric argument is a 400, not a 500
  @Test
  void otherNumericFunctionsOnStringReturn400() throws Exception {
    testEachServer(serverIndex -> {
      for (final String function : new String[] { "sqrt", "ceil", "floor", "sign", "round", "isNaN", "log" }) {
        final JSONObject json = executeOpenCypher(serverIndex, "RETURN " + function + "('hello') AS r", 400);
        assertThat(json.getString("detail")).contains(function + "()").contains("STRING");
      }
    });
  }

  // Regression: #5484 abs() on a non-numeric argument is a 400, not a 500
  @Test
  void absOnANonNumericPropertyReturns400() throws Exception {
    // Type known only at runtime, so this goes through AbsFunction rather than the parse-time check.
    testEachServer(serverIndex -> {
      executeOpenCypher(serverIndex, "CREATE (:Issue5484 {name: 'hello'})", 200);
      final JSONObject json = executeOpenCypher(serverIndex, "MATCH (n:Issue5484) RETURN abs(n.name) AS r", 400);
      assertThat(json.getString("detail")).contains("abs()").contains("STRING");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5484 abs() on a non-numeric argument is a 400, not a 500
  @Test
  void anUnsupportedDistanceUnitAlsoReturns400() throws Exception {
    // distance() gained an optional-unit declaration here; its unit error travels as an IllegalArgumentException, which
    // the HTTP layer already maps to 400. Asserted end to end so it cannot regress into a 500 unnoticed.
    testEachServer(serverIndex -> {
      final JSONObject json = executeOpenCypher(serverIndex, "RETURN distance(point({latitude: 0, longitude: 0}),"
          + " point({latitude: 0, longitude: 1}), 'furlongs') AS r", 400);
      assertThat(json.getString("detail")).contains("furlongs");
    });
  }

  // Regression: #5484 abs() on a non-numeric argument is a 400, not a 500
  @Test
  void absOnNullStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject json = executeOpenCypher(serverIndex, "RETURN abs(null) AS r", 200);
      assertThat(json.getJSONArray("result").getJSONObject(0).isNull("r")).isTrue();
    });
  }

  // Regression: #5484 abs() on a non-numeric argument is a 400, not a 500
  @Test
  void absOnNumbersStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(executeOpenCypher(serverIndex, "RETURN abs(-1) AS r", 200).getJSONArray("result").getJSONObject(0)
          .getLong("r")).isEqualTo(1L);
      assertThat(executeOpenCypher(serverIndex, "RETURN abs(-1.5) AS r", 200).getJSONArray("result").getJSONObject(0)
          .getDouble("r")).isEqualTo(1.5d);
    });
  }

  /*
   * Regression test for item 5 of issue #5602, reported against the HTTP command API. An arithmetic error - a 64-bit
   * integer overflow or a division by zero - answered 500 "internal server error" although nothing was wrong with the
   * server: the pair of values the caller sent has no representable answer. Neo4j classifies the whole category as
   * {@code Neo.ClientError.Statement.ArithmeticError}, so the status must be 400.
   * <p>
   * Asserted through HTTP rather than in the engine because the classification only exists at this boundary, and because
   * it has to survive the auto-commit transaction wrapper that wraps the failure on the write path (the shape that made
   * #5191 and #5219 report the wrong status).
   */

  // Regression: #5602 integer overflow and division by zero are 400 arithmetic errors
  @Test
  void integerOverflowReturns400() throws Exception {
    testEachServer(serverIndex -> {
      for (final String command : new String[] { //
          "RETURN abs(-9223372036854775808) AS r", //
          "RETURN 9223372036854775807 + 1 AS r", //
          "RETURN -9223372036854775808 - 1 AS r", //
          "RETURN 9223372036854775807 * 2 AS r" }) {
        final JSONObject json = executeOpenCypher(serverIndex, command, 400);
        assertThat(json.getString("exception")).isEqualTo(ArithmeticErrorException.class.getName());
        assertThat(json.getString("detail")).contains("overflow");
        assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
      }
    });
  }

  // Regression: #5602 integer overflow and division by zero are 400 arithmetic errors
  @Test
  void divisionByZeroReturns400() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(executeOpenCypher(serverIndex, "RETURN 1 / 0 AS r", 400).getString("detail")).contains("/ by zero");
      assertThat(executeOpenCypher(serverIndex, "RETURN 1 % 0 AS r", 400).getString("detail")).contains("% by zero");
    });
  }

  // Regression: #5602 integer overflow and division by zero are 400 arithmetic errors
  @Test
  void anOverflowOnAWriteAlsoReturns400() throws Exception {
    // The write path wraps the failure in the auto-commit TransactionException, which is where a client error most
    // often degrades back to 500. Asserted separately so the wrapped arm cannot rot.
    testEachServer(serverIndex -> {
      executeOpenCypher(serverIndex, "CREATE (:Issue5602 {v: 9223372036854775807})", 200);
      final JSONObject json = executeOpenCypher(serverIndex, "MATCH (n:Issue5602) SET n.v = n.v + 1 RETURN n", 400);
      assertThat(json.getString("detail")).contains("overflow");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5602 integer overflow and division by zero are 400 arithmetic errors
  /**
   * A wrong argument count reaching the runtime guard through {@code CALL} must be the same 400 an expression gives.
   * <p>
   * It is the double-wrap that makes this worth its own test: {@code CallStep} used to flatten the client error into
   * a {@code CommandExecutionException}, and on the auto-commit path the chain became
   * {@code TransactionException -> CommandExecutionException -> CommandSemanticException} - one level deeper than the
   * handler unwraps, so the same mistake answered 500 here while answering 400 in a plain read. {@code CallStep} now
   * rethrows a client error untouched.
   */
  @Test
  void aWrongArgumentCountThroughCallReturns400OnTheWritePath() throws Exception {
    testEachServer(serverIndex -> {
      // text.hammingDistance declares 2 arguments; the CREATE puts the statement on the auto-commit write path.
      final JSONObject json = executeOpenCypher(serverIndex,
          "CREATE (n:Issue5602Call) WITH n CALL text.hammingDistance('a') YIELD * RETURN n", 400);
      assertThat(json.getString("detail")).contains("hammingDistance").contains("2 arguments");
      assertThat(json.getString("error")).doesNotContain("Error on transaction commit");
    });
  }

  // Regression: #5602 integer overflow and division by zero are 400 arithmetic errors
  @Test
  void arithmeticThatDoesNotOverflowStillReturns200() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(executeOpenCypher(serverIndex, "RETURN 9223372036854775806 + 1 AS r", 200).getJSONArray("result")
          .getJSONObject(0).getLong("r")).isEqualTo(Long.MAX_VALUE);
      // A float overflow is Infinity under IEEE 754, as in Neo4j, and must not have become an error. JSON has no
      // literal for it, so the serializer renders it as a name - the assertion is that the request succeeded at all.
      assertThat(executeOpenCypher(serverIndex, "RETURN 1.0 / 0.0 AS r", 200).getJSONArray("result").getJSONObject(0)
          .get("r").toString()).contains("Infinity");
    });
  }

  private JSONObject executeCypher(final int serverIndex, final String command, final int expectedStatus) throws Exception {
    return postCommand(serverIndex, "cypher", command, expectedStatus);
  }

  private JSONObject executeOpenCypher(final int serverIndex, final String command, final int expectedStatus) throws Exception {
    return postCommand(serverIndex, "opencypher", command, expectedStatus);
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
