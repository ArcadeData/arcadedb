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

import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.remote.RemoteAlreadyExistsException;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A name that is already taken - a unique key, a user, an API token, a database, a type, property, index, bucket, view or
 * constraint - is answered with HTTP 409 Conflict and an exception that names it, so a client can treat "it already
 * exists" as such instead of matching on message text, and the Java client rebuilds a typed exception from it. A taken
 * database name has its own class, {@link HttpTakenDatabaseNameConflictIT}, because its import tests need local URLs allowed.
 * <p>
 * Each group of tests below keeps the explanation of the issue it was written for.
 */
class HttpConflictStatusIT extends BaseGraphServerTest {

  /*
   * Regression test for issue #4350: a unique-constraint violation must surface over HTTP as
   * 409 Conflict (RFC 9110 §15.5.10), never 503 Service Unavailable. 503 is retry-worthy and
   * caused upstream clients/load balancers to retry the bad write indefinitely, amplifying
   * the problem and triggering 5xx-rate alerts on what is really a client data conflict.
   */

  // Regression: #4350 a unique-key violation is a 409 Conflict, never a retry-worthy 503
  @Test
  void duplicateKeyReturns409Conflict() throws Exception {
    testEachServer(serverIndex -> {
      // Create the type, unique index, and the first row in one script. We intentionally do not
      // include the duplicate insert here, so that the duplicate is committed by its own request
      // and the server reports the error synchronously (not as part of a sqlScript batch).
      execCommand(serverIndex, "sqlScript", """
          CREATE VERTEX TYPE issue4350;
          CREATE PROPERTY issue4350.code STRING;
          CREATE INDEX ON issue4350 (code) UNIQUE;
          CREATE VERTEX issue4350 SET code = 'X-1';
          """, 200);

      // Now insert a duplicate row. This must yield HTTP 409 Conflict, NOT 503.
      final HttpURLConnection connection = openCommandConnection(serverIndex);
      try {
        final JSONObject payload = new JSONObject()
            .put("language", "sql")
            .put("command", "CREATE VERTEX issue4350 SET code = 'X-1'");

        try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
          pw.write(payload.toString());
        }

        final int statusCode = connection.getResponseCode();
        final String response = readError(connection);

        assertThat(statusCode)
            .as("duplicate-key violation must return 409 Conflict, got %d (body=%s)", statusCode, response)
            .isEqualTo(409);

        final JSONObject json = new JSONObject(response);
        assertThat(json.getString("exception")).isEqualTo(DuplicatedKeyException.class.getName());
        assertThat(json.getString("error")).isEqualTo("Found duplicate key in index");
        // exceptionArgs format is "<indexName>|<keys>|<currentIndexedRID>" - keep the existing
        // contract intact so RemoteHttpComponent can still reconstruct DuplicatedKeyException
        // client-side.
        assertThat(json.getString("exceptionArgs")).contains("|").contains("X-1");
      } finally {
        connection.disconnect();
      }
    });
  }

  // Regression: #4350 a unique-key violation is a 409 Conflict, never a retry-worthy 503
  /**
   * Same scenario but the duplicate is produced inside a multi-statement {@code sqlScript},
   * which is the exact shape that triggered the original bug report (batch MERGE statements
   * against UNIQUE indexes). The transactional wrapper must not down-grade the status code
   * to 5xx.
   */
  @Test
  void duplicateKeyInScriptReturns409Conflict() throws Exception {
    testEachServer(serverIndex -> {
      // Prepare type + unique index in a successful script first so the duplicate below
      // executes against an existing schema.
      execCommand(serverIndex, "sqlScript", """
          CREATE VERTEX TYPE issue4350_script;
          CREATE PROPERTY issue4350_script.code STRING;
          CREATE INDEX ON issue4350_script (code) UNIQUE;
          """, 200);

      final HttpURLConnection connection = openCommandConnection(serverIndex);
      try {
        final JSONObject payload = new JSONObject()
            .put("language", "sqlScript")
            .put("command", """
                CREATE VERTEX issue4350_script SET code = 'dup';
                CREATE VERTEX issue4350_script SET code = 'dup';
                """);

        try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
          pw.write(payload.toString());
        }

        final int statusCode = connection.getResponseCode();
        final String response = readError(connection);

        assertThat(statusCode)
            .as("duplicate-key violation inside sqlScript must return 409 Conflict, got %d (body=%s)", statusCode, response)
            .isEqualTo(409);
        assertThat(response).contains("DuplicatedKeyException");
      } finally {
        connection.disconnect();
      }
    });
  }

  private void execCommand(final int serverIndex, final String language, final String sql, final int expectedStatus)
      throws Exception {
    final HttpURLConnection connection = openCommandConnection(serverIndex);
    try {
      final JSONObject payload = new JSONObject().put("language", language).put("command", sql);
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(payload.toString());
      }
      assertThat(connection.getResponseCode()).isEqualTo(expectedStatus);
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection openCommandConnection(final int serverIndex) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:" + getServerHttpPort(serverIndex) + "/api/v1/command/graph").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    return connection;
  }

  /*
   * Issue #10073: creating a user whose name is taken answered 403 "Security error", the status a non-root caller gets, so
   * a client could not tell "this user already exists" from "you may not do this" without matching on the `detail` text.
   * The contract declares 409, and 409 is what an application provisioning a user per tenant needs to treat the conflict
   * as success.
   * <p>
   * One test per way in: the REST route and the `create user` server command, each driven against a real server. The
   * non-root test is the other half of the contract: a 403 must still mean "not authorized", or the fix would have moved
   * the ambiguity rather than removed it.
   */

  private static final String PASSWORD = "a-long-password";

  // Regression: #10073 a taken user name is a 409, not the 403 of a refusal
  @Test
  void aTakenNameOnTheRestRouteAnswers409AndLeavesTheFirstUserAlone() throws Exception {
    final String name = "dup_rest_user";
    assertThat(postUser(name, PASSWORD, basicAuth())).isEqualTo(201);

    final HttpURLConnection second = open("/api/v1/server/users", basicAuth(), toJson(name, "another-long-password"));
    try {
      assertThat(second.getResponseCode()).isEqualTo(409);
      assertThat(readError(second)).contains("already exists").contains(name);
    } finally {
      second.disconnect();
    }

    // The conflict must not have replaced the first user's credentials
    assertThat(getServer(0).getSecurity().authenticate(name, PASSWORD, null)).isNotNull();
  }

  // Regression: #10073 a taken user name is a 409, not the 403 of a refusal
  @Test
  void aTakenNameOnTheServerCommandAnswers409() throws Exception {
    final String name = "dup_command_user";
    assertThat(postUser(name, PASSWORD, basicAuth())).isEqualTo(201);

    final HttpURLConnection second = open("/api/v1/server", basicAuth(),
        new JSONObject().put("command", "create user " + toJson(name, PASSWORD)).toString());
    try {
      assertThat(second.getResponseCode()).isEqualTo(409);
      assertThat(readError(second)).contains("already exists");
    } finally {
      second.disconnect();
    }
  }

  // Regression: #10073 a taken user name is a 409, not the 403 of a refusal
  @Test
  void aNonRootCallerStillGets403EvenForATakenName() throws Exception {
    final String name = "dup_nonroot_user";
    assertThat(postUser(name, PASSWORD, basicAuth())).isEqualTo(201);

    final String asThatUser = "Basic " + Base64.getEncoder().encodeToString((name + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8));
    assertThat(postUser(name, PASSWORD, asThatUser)).isEqualTo(403);
    assertThat(postUser("dup_nonroot_other", PASSWORD, asThatUser)).isEqualTo(403);
  }

  // Regression: #10073 a taken user name is a 409, not the 403 of a refusal
  /**
   * The Java remote client used to surface the duplicate as a {@code SecurityException}, the type it gives a 403. It
   * arrives as a conflict instead, with its own type ({@link RemoteAlreadyExistsException}, issue #10075), so a caller can
   * tell it apart from a refusal without matching on the message.
   */
  @Test
  void theRemoteClientReportsATakenNameAsAnAlreadyExistsConflict() {
    final RemoteServer remote = new RemoteServer("127.0.0.1", getServerHttpPort(0), "root", DEFAULT_PASSWORD_FOR_TESTS);
    remote.createUser("dup_remote_user", PASSWORD, List.of());

    assertThatThrownBy(() -> remote.createUser("dup_remote_user", PASSWORD, List.of()))
        .isInstanceOf(RemoteAlreadyExistsException.class)
        .isNotInstanceOf(SecurityException.class)
        .hasMessageContaining("already exists");
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private int postUser(final String name, final String password, final String authorization) throws Exception {
    final HttpURLConnection connection = open("/api/v1/server/users", authorization, toJson(name, password));
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private static String toJson(final String name, final String password) {
    return new JSONObject().put("name", name).put("password", password).toString();
  }

  private HttpURLConnection open(final String path, final String authorization, final String body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) URI.create(getServerHttpUrl(0, path)).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", authorization);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    try (final OutputStream out = connection.getOutputStream()) {
      out.write(body.getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();
    return connection;
  }

  /*
   * Issue #10075: the server answers a taken user, API token or database name as a conflict (HTTP 409), but the Java HTTP
   * client had no type for it: it fell through to a generic {@link RemoteException} that only message text could tell
   * apart. {@link RemoteServer} now raises {@link RemoteAlreadyExistsException} for all of them. Driven against a real
   * server, one test per way the conflict reaches the client.
   */

  // Regression: #10075 the Java client raises a typed RemoteAlreadyExistsException on a 409
  @Test
  void aTakenUserNameIsATypedAlreadyExistsException() {
    final RemoteServer remote = remote();
    remote.createUser("dup_typed_user", PASSWORD, List.of());

    assertThatThrownBy(() -> remote.createUser("dup_typed_user", PASSWORD, List.of()))
        .isInstanceOf(RemoteAlreadyExistsException.class)
        .isInstanceOf(RemoteException.class)
        .isNotInstanceOf(SecurityException.class)
        .hasMessageContaining("already exists")
        .hasMessageContaining("dup_typed_user");
  }

  // Regression: #10075 the Java client raises a typed RemoteAlreadyExistsException on a 409
  @Test
  void aTakenDatabaseNameIsATypedAlreadyExistsException() {
    final RemoteServer remote = remote();
    final String name = "dup_typed_db_10075";
    remote.create(name);
    try {
      assertThatThrownBy(() -> remote.create(name))
          .isInstanceOf(RemoteAlreadyExistsException.class)
          .hasMessageContaining("already exists")
          .hasMessageContaining(name);
    } finally {
      remote.drop(name);
    }
  }

  // Regression: #10075 the Java client raises a typed RemoteAlreadyExistsException on a 409
  @Test
  void aTakenApiTokenNameIsATypedAlreadyExistsException() {
    final RemoteServer remote = remote();
    remote.createApiToken("dup_typed_token", "graph", 0, new JSONObject());

    assertThatThrownBy(() -> remote.createApiToken("dup_typed_token", "graph", 0, new JSONObject()))
        .isInstanceOf(RemoteAlreadyExistsException.class)
        .hasMessageContaining("already exists")
        .hasMessageContaining("dup_typed_token");
  }

  // Regression: #10075 the Java client raises a typed RemoteAlreadyExistsException on a 409
  /**
   * The token route answered 409 with only {@code {"error": ...}}: no exception name, so a client could not tell a taken
   * token name from any other conflict. The body now names the exception, like every other error the server sends.
   */
  @Test
  void theTokenConflictBodyNamesItsException() throws Exception {
    remote().createApiToken("dup_body_token", "graph", 0, new JSONObject());

    final JSONObject body = new JSONObject().put("name", "dup_body_token").put("database", "graph").put("expiresAt", 0)
        .put("permissions", new JSONObject());
    final HttpURLConnection connection = (HttpURLConnection) URI.create(getServerHttpUrl(0, "/api/v1/server/api-tokens"))
        .toURL().openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);
      try (final OutputStream out = connection.getOutputStream()) {
        out.write(body.toString().getBytes(StandardCharsets.UTF_8));
      }

      assertThat(connection.getResponseCode()).isEqualTo(409);
      try (final InputStream in = connection.getErrorStream()) {
        final JSONObject answer = new JSONObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        assertThat(answer.getString("exception", "")).isEqualTo("com.arcadedb.server.ServerControlPlane$AlreadyExistsException");
        assertThat(answer.getString("detail", "")).contains("already exists").contains("dup_body_token");
        assertThat(answer.getString("error", "")).contains("already exists");
      }
    } finally {
      connection.disconnect();
    }
  }

  // Regression: #10075 the Java client raises a typed RemoteAlreadyExistsException on a 409
  @Test
  void aDifferentTokenNameIsStillCreated() {
    final RemoteServer remote = remote();
    remote.createApiToken("dup_ok_token_a", "graph", 0, new JSONObject());

    assertThat(remote.createApiToken("dup_ok_token_b", "graph", 0, new JSONObject()).getString("token", "")).isNotEmpty();
  }

  private RemoteServer remote() {
    return new RemoteServer("127.0.0.1", getServerHttpPort(0), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  /*
   * Issue #10153: creating a type, property, index, bucket or materialized view under a taken name answered HTTP 500 - "Cannot
   * execute command", or "Internal error" for the SchemaException ones - and logged a SEVERE stack trace, while a taken database,
   * user or token name answered 409 (#10073, #10074). A retry policy keyed on 5xx re-sent a statement that can only be refused
   * again. Every one of them is a 409 now, with a label naming the kind (it survives production mode, which conceals 'detail'),
   * and the Java client rebuilds the {@link AlreadyExistsException}.
   */

  // Regression: #10153 a taken schema object name is a 409 naming its kind, not a 500
  @Test
  void everyTakenSchemaObjectNameAnswers409() throws Exception {
    assertConflict("sql", "create vertex type T10153", "Type already exists", "TYPE");
    // the name is taken by a vertex type
    assertThat(command("sql", "create edge type T10153").statusCode()).isEqualTo(409);
    assertConflict("sql", "create property T10153.p1 string", "Property already exists", "PROPERTY");
    assertConflict("sql", "create index on T10153 (p1) unique", "Index already exists", "INDEX");
    assertConflict("sql", "create bucket b10153", "Bucket already exists", "BUCKET");
    assertConflict("sql", "create materialized view mv10153 as select from T10153", "Materialized view already exists", "MATERIALIZED_VIEW");
    assertConflict("opencypher", "CREATE CONSTRAINT c10153 FOR (n:P) REQUIRE n.name IS UNIQUE", "Constraint already exists",
        "CONSTRAINT");

    // a rename onto a taken name was a 400, the status of a malformed request
    assertThat(command("sql", "create document type D10153").statusCode()).isEqualTo(200);
    final HttpResponse<String> rename = command("sql", "alter type D10153 name T10153");
    assertThat(rename.statusCode()).isEqualTo(409);
    assertThat(new JSONObject(rename.body()).getString("exception")).isEqualTo(AlreadyExistsException.class.getName());
  }

  // Regression: #10153 a taken schema object name is a 409 naming its kind, not a 500
  @Test
  void theJavaClientRebuildsTheConflictWithItsKind() {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(0), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {
      database.command("sql", "create vertex type Remote10153");

      assertThatThrownBy(() -> database.command("sql", "create vertex type Remote10153"))
          .isInstanceOf(AlreadyExistsException.class)
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("already exists")
          .satisfies(e -> assertThat(((AlreadyExistsException) e).getKind()).isEqualTo(AlreadyExistsException.Kind.TYPE));

      assertThatThrownBy(() -> database.getSchema().createDocumentType("Remote10153"))
          .isInstanceOf(AlreadyExistsException.class);
    }
  }

  // Regression: #10153 a taken schema object name is a 409 naming its kind, not a 500
  /** Not every refusal became a conflict: a statement naming a type that does not exist keeps its answer. */
  @Test
  void aMissingTypeIsNotAConflict() throws Exception {
    final HttpResponse<String> missing = command("sql", "create property NoSuchType10153.p string");
    assertThat(missing.statusCode()).isNotEqualTo(409);
  }

  private void assertConflict(final String language, final String statement, final String label, final String kind)
      throws Exception {
    assertThat(command(language, statement).statusCode()).as("first %s", statement).isEqualTo(200);

    final HttpResponse<String> second = command(language, statement);
    assertThat(second.statusCode()).as("second %s: %s", statement, second.body()).isEqualTo(409);
    final JSONObject body = new JSONObject(second.body());
    assertThat(body.getString("error")).isEqualTo(label);
    assertThat(body.getString("exception")).isEqualTo(AlreadyExistsException.class.getName());
    assertThat(body.getString("exceptionArgs", null)).isEqualTo(kind);
    assertThat(body.getString("detail", "")).contains("already exists");
  }

  private HttpResponse<String> command(final String language, final String command) throws Exception {
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
}
