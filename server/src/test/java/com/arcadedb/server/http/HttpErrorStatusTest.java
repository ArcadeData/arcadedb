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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The HTTP status and message a failed command or query answers with: a caller's mistake is a 4xx, and a failure is
 * classified by what actually failed.
 */
class HttpErrorStatusTest {
  // Regression: #11211 a Cypher type error answered 500 instead of 400
  /**
   * Issue #11211 over HTTP, the surface it was reported from: {@code r.weight} on a variable-length relationship answered
   * 500 "Cannot execute command" from both the command and the query endpoint. It is the caller's error, refused before
   * the query runs as Neo4j refuses it; and a type error met only while the rows are serialized is the caller's too.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CypherTypeError extends BaseGraphServerTest {

    @Test
    void propertyAccessOnAVariableLengthRelationshipIsAClientError() throws Exception {
      testEachServer(serverIndex -> {
        executeCommand(serverIndex, "opencypher", "CREATE (:N11211 {id: 1})-[:R11211 {weight: 7}]->(:N11211 {id: 2})");

        for (final String endpoint : new String[] { "command", "query" }) {
          final JSONObject error = post(serverIndex, endpoint, 400,
              "MATCH (n:N11211)<-[r:R11211*1..3]-(m:N11211) RETURN r.weight AS w");
          assertThat(error.getString("exception")).as(endpoint).isEqualTo(CommandSemanticException.class.getName());
          assertThat(error.getString("detail")).as(endpoint).contains("List<Relationship>");
        }

        // the control: the relationship list itself, and a fixed-length relationship's property, still answer 200
        post(serverIndex, "query", 200, "MATCH (n:N11211)<-[r:R11211*1..3]-(m:N11211) RETURN size(r) AS s");
        post(serverIndex, "query", 200, "MATCH (n:N11211)<-[r:R11211]-(m:N11211) RETURN r.weight AS w");
      });
    }

    @Test
    void aTypeErrorMetWhileTheRowsAreSerializedIsAClientError() throws Exception {
      testEachServer(serverIndex -> {
        for (final String endpoint : new String[] { "command", "query" }) {
          final JSONObject error = post(serverIndex, endpoint, 400, "UNWIND [[1, 2]] AS l RETURN l.x AS v");
          assertThat(error.getString("exception")).as(endpoint).isEqualTo(TypeMismatchException.class.getName());
          assertThat(error.getString("detail")).as(endpoint).contains("TypeError: Cannot access property 'x'");
          assertThat(error.getString("error")).as(endpoint).isEqualTo("Cannot execute command");
        }
      });
    }

    /** Posts a Cypher statement to an endpoint, asserts the HTTP status, and returns the parsed body. */
    private JSONObject post(final int serverIndex, final String endpoint, final int expectedStatus, final String command)
        throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          getServerHttpUrl(serverIndex, "/api/v1/" + endpoint + "/graph")).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      final JSONObject payload = new JSONObject();
      payload.put("language", "opencypher");
      payload.put("command", command);
      formatPayload(connection, payload);
      connection.connect();

      try {
        final int status = connection.getResponseCode();
        final String body = status < 400 ? readResponse(connection) : readError(connection);
        assertThat(status).as("HTTP status of /%s for `%s`; body: %s", endpoint, command, body).isEqualTo(expectedStatus);
        return new JSONObject(body);
      } finally {
        connection.disconnect();
      }
    }
  }

  // Regression: #11589 a function given a wrong argument answered 500
  /**
   * Issue #11589 over HTTP, the surface it was reported from: SQL and openCypher functions given an argument of the wrong kind or a
   * null answered 500, some with a raw {@code NullPointerException}, {@code ClassCastException} or {@code DateTimeParseException}.
   * Every statement of the report now answers below 500: 400 for the argument the caller got wrong, 200 for the two that were
   * never wrong ({@code out('E', null)}, whose null names no edge type, and {@code intersect(@this)}).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FunctionArgumentError extends BaseGraphServerTest {
    private static final List<String> SQL_ANSWERED = List.of("SELECT out('E11589', null) AS v FROM V11589 WHERE id = 1",
        "SELECT outE('E11589', null) AS v FROM V11589 WHERE id = 1", "SELECT in('E11589', null) AS v FROM V11589 WHERE id = 2",
        "SELECT both('E11589', null) AS v FROM V11589 WHERE id = 1", "SELECT intersect(@this) AS v FROM V11589 WHERE id = 1");

    private static final List<String> SQL_REFUSED = List.of("SELECT geo.lineString(1) AS v FROM V11589 WHERE id = 1",
        "SELECT geo.polygon(@rid) AS v FROM V11589 WHERE id = 1", "SELECT ts.rate(1, 'abc') AS v FROM V11589 WHERE id = 1",
        "SELECT ts.last(1, 'x') AS v FROM V11589 WHERE id = 1", "SELECT duration(1, '') AS v FROM V11589 WHERE id = 1",
        "SELECT cypherrid('a', 'b') AS v FROM V11589 WHERE id = 1",
        "SELECT fulltext.searchIndexMore('x', 1, 2, 3) AS v FROM V11589 WHERE id = 1");

    private static final List<String> CYPHER_REFUSED = List.of("MATCH (n:V11589 {id: 1}) RETURN date.fields('abc') AS v",
        "MATCH (n:V11589 {id: 1}) RETURN duration(1) AS v", "MATCH (n:V11589 {id: 1}) RETURN duration.between(n, date()) AS v",
        "MATCH (n:V11589 {id: 1}) RETURN duration.inDays(1, 2) AS v", "MATCH (n:V11589 {id: 1}) RETURN toIntegerList(1) AS v",
        "MATCH (n:V11589 {id: 1}) RETURN toStringList('a') AS v", "MATCH (n:V11589 {id: 1}) RETURN point.distance(1, 2) AS v",
        "MATCH (n:V11589 {id: 1}) RETURN percentileCont(n.id, 1.5) AS v", "MATCH (n:V11589 {id: 1}) RETURN format(1, 'yyyy') AS v");

    @Test
    void aWrongFunctionArgumentIsAClientError() throws Exception {
      testEachServer(serverIndex -> {
        executeCommand(serverIndex, "opencypher", "CREATE (:V11589 {id: 1})-[:E11589]->(:V11589 {id: 2})");

        for (final String sql : SQL_ANSWERED)
          post(serverIndex, "sql", sql, 200);
        for (final String sql : SQL_REFUSED)
          post(serverIndex, "sql", sql, 400);
        for (final String cypher : CYPHER_REFUSED)
          post(serverIndex, "opencypher", cypher, 400);
        // a null unit is a null answer, as a null temporal value is
        post(serverIndex, "opencypher", "MATCH (n:V11589 {id: 1}) RETURN time.truncate(null, true) AS v", 200);

        // the exception names the kind of mistake, which a Java client rebuilds
        assertThat(post(serverIndex, "opencypher", "MATCH (n:V11589 {id: 1}) RETURN toIntegerList(1) AS v", 400).getString("exception"))
            .isEqualTo(TypeMismatchException.class.getName());
        assertThat(post(serverIndex, "sql", "SELECT ts.rate(1, 'abc') AS v FROM V11589 WHERE id = 1", 400).getString("exception"))
            .isEqualTo(InvalidArgumentException.class.getName());
      });
    }

    /** Posts a statement to the command endpoint, asserts the HTTP status, and returns the parsed body. */
    private JSONObject post(final int serverIndex, final String language, final String command, final int expectedStatus)
        throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) new URL(getServerHttpUrl(serverIndex, "/api/v1/command/graph"))
          .openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      final JSONObject payload = new JSONObject();
      payload.put("language", language);
      payload.put("command", command);
      formatPayload(connection, payload);
      connection.connect();

      try {
        final int status = connection.getResponseCode();
        final String body = status < 400 ? readResponse(connection) : readError(connection);
        assertThat(status).as("HTTP status for `%s`; body: %s", command, body).isEqualTo(expectedStatus);
        return new JSONObject(body);
      } finally {
        connection.disconnect();
      }
    }
  }

  // Regression: #5898 a broken LINK dereference was reported as a commit failure
  /**
   * Issue #5898 over HTTP, which is the surface it was reported from. Dereferencing a Cypher property whose stored
   * LINK no longer resolves used to let a {@code RecordNotFoundException} out of the query engine; the auto-commit
   * wrapper in {@code DatabaseAbstractHandler} re-throws it inside a {@code TransactionException}, and
   * {@code AbstractServerHttpHandler} - which classifies by {@code getCause()} - has no arm for it, so the client was
   * told "Error on transaction commit" and learned nothing about what actually failed.
   * <p>
   * What this pins is the message, not the status: a Cypher {@code TypeError} is a 500 by deliberate design (the
   * #5219 rationale in {@code AbstractServerHttpHandler}, matching TinkerPop's {@code SERVER_ERROR_SCRIPT_EVALUATION}),
   * and this failure must be classified as one of those rather than as the catch-all commit failure. Asserting the
   * status alone would pass on the very bug being fixed, since both shapes answer 500.
   */
  @Nested
  class BrokenLinkPropertyAccess extends BaseGraphServerTest {

    @Test
    void aDanglingLinkIsReportedAsACypherTypeError() throws Exception {
      testEachServer(serverIndex -> {
        executeCommand(serverIndex, "opencypher",
            "CREATE (holder:LinkHolder {role: 'holder'}), (target:LinkHolder {role: 'target', id: 42}) "
                + "SET holder.ref = target");

        // Deleted in its own request, so the read below meets a genuinely unresolvable RID rather than the
        // in-transaction deleted-entity marker.
        executeCommand(serverIndex, "sql", "DELETE FROM LinkHolder WHERE role = 'target'");

        final JSONObject error = commandExpecting(serverIndex, 500, "opencypher",
            "MATCH (holder:LinkHolder {role: 'holder'}) RETURN holder.ref.id AS referencedId");

        assertThat(error.getString("detail"))
            .as("the client is told which property could not be read, and through which link")
            .contains("TypeError: Cannot access property 'id'");
        assertThat(error.getString("error"))
            .as("classified as a failed command, not as a failed commit")
            .isEqualTo("Cannot execute command");
      });
    }

    /**
     * The control: a link that still resolves must keep answering 200 with the referenced value, so the test above
     * cannot pass by breaking every dereference.
     */
    @Test
    void aLiveLinkStillDereferences() throws Exception {
      testEachServer(serverIndex -> {
        executeCommand(serverIndex, "opencypher",
            "CREATE (holder:LiveLinkHolder {role: 'holder'}), (target:LiveLinkHolder {role: 'target', id: 7}) "
                + "SET holder.ref = target");

        final JSONObject response = executeCommand(serverIndex, "opencypher",
            "MATCH (holder:LiveLinkHolder {role: 'holder'}) RETURN holder.ref.id AS referencedId");

        // The Cypher engine answers a records/columns envelope rather than the flat array SQL returns.
        final JSONObject result = response.getJSONObject("result");
        assertThat(result.getJSONArray("records").getJSONObject(0).getInt("referencedId")).isEqualTo(7);
      });
    }

    /** Posts a command, asserts the HTTP status, and returns the parsed body (the error body on a failure). */
    private JSONObject commandExpecting(final int serverIndex, final int expectedStatus, final String language,
        final String command) throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) new URL(getServerHttpUrl(serverIndex, "/api/v1/command/graph")).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      final JSONObject payload = new JSONObject();
      payload.put("language", language);
      payload.put("command", command);
      formatPayload(connection, payload);
      connection.connect();

      try {
        // The status is read BEFORE the body: which stream carries it depends on the status, and asking for the error
        // stream of a successful response hands back null.
        final int status = connection.getResponseCode();
        final String body = status < 400 ? readResponse(connection) : readError(connection);
        assertThat(status).as("HTTP status for `%s`; body: %s", command, body).isEqualTo(expectedStatus);
        return new JSONObject(body);
      } finally {
        connection.disconnect();
      }
    }
  }

  // Regression: #5675 an unsatisfiable CREATE INDEX IF NOT EXISTS answered 200
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
  @Nested
  class CreateIndexIfNotExists extends BaseGraphServerTest {

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
      final HttpURLConnection connection = (HttpURLConnection) new URL(getServerHttpUrl(serverIndex, "/api/v1/command/" + DATABASE_NAME)).openConnection();
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
}
