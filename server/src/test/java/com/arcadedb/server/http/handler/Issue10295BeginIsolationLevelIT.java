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
package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.http.handler.openapi.CoreApiSpec;
import com.arcadedb.server.security.ServerSecurityUser;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static com.arcadedb.server.http.HttpSessionManager.ARCADEDB_SESSION_ID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10295: the optional {@code isolationLevel} body of {@code POST /api/v1/begin/{database}} that the OpenAPI spec now
 * declares. The values the spec advertises open a transaction at that level, an absent body opens one at the database
 * default, and a value outside the engine enum answers 400 without opening a session.
 */
class Issue10295BeginIsolationLevelIT extends BaseGraphServerTest {

  @Test
  void eachAdvertisedIsolationLevelOpensATransactionAtThatLevel() throws Exception {
    // Driven by the values the published contract advertises, so the spec cannot offer one the server refuses
    final OpenAPI openAPI = new OpenAPI();
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new CoreApiSpec().contribute(openAPI);
    final List<?> advertised = ((Schema<?>) openAPI.getComponents().getSchemas().get("BeginRequest").getProperties()
        .get("isolationLevel")).getEnum();
    assertThat(advertised).isNotEmpty();

    for (final Object value : advertised) {
      final String level = String.valueOf(value);
      final HttpURLConnection connection = begin("{\"isolationLevel\":\"" + level + "\"}");
      try {
        assertThat(connection.getResponseCode()).as(level).isEqualTo(204);
        final String sessionId = connection.getHeaderField(ARCADEDB_SESSION_ID);
        assertThat(sessionId).isNotNull();
        assertThat(sessionOf(sessionId.trim()).transaction.getIsolationLevel().name()).isEqualTo(level);
      } finally {
        connection.disconnect();
      }
    }
  }

  @Test
  void anAbsentIsolationLevelOpensATransactionAtTheDatabaseDefault() throws Exception {
    final Database.TRANSACTION_ISOLATION_LEVEL defaultLevel = getServer(0).getDatabase(getDatabaseName())
        .getTransactionIsolationLevel();

    // An explicit JSON null is the same as an absent member, not a value of the wrong type
    for (final String body : new String[] { null, "{}", "{\"isolationLevel\":null}" }) {
      final HttpURLConnection connection = begin(body);
      try {
        assertThat(connection.getResponseCode()).as(String.valueOf(body)).isEqualTo(204);
        final String sessionId = connection.getHeaderField(ARCADEDB_SESSION_ID);
        assertThat(sessionOf(sessionId.trim()).transaction.getIsolationLevel()).isEqualTo(defaultLevel);
      } finally {
        connection.disconnect();
      }
    }
  }

  @Test
  void malformedJsonAnswers400AndOpensNoSession() throws Exception {
    final HttpSessionManager sessions = getServer(0).getHttpServer().getSessionManager();
    final int before = sessions.getActiveSessions();
    final HttpURLConnection connection = begin("{\"isolationLevel\":");
    try {
      assertThat(connection.getResponseCode()).isEqualTo(400);
      assertThat(connection.getHeaderField(ARCADEDB_SESSION_ID)).isNull();
      assertThat(readError(connection)).contains("Invalid JSON payload").contains("valid JSON object");
    } finally {
      connection.disconnect();
    }
    assertThat(sessions.getActiveSessions()).as("a malformed body must not leave a session behind").isEqualTo(before);
  }

  @Test
  void aBodyThatIsNotAJsonObjectAnswers400AndOpensNoSession() throws Exception {
    // Truncated or garbled bodies, a scalar and a top-level array: none can carry the isolationLevel the client meant, so
    // none may fall back to the database default with a success status (issue #10337). The same check serves every shape
    final HttpSessionManager sessions = getServer(0).getHttpServer().getSessionManager();

    for (final String body : new String[] { "{\"isolationLevel\":\"REPEATABLE_READ\"", "{\"isolationLevel\":\"REPEATABLE_READ\",",
        "not json", "null", "123", "\"REPEATABLE_READ\"", "[", "[]", "[\"REPEATABLE_READ\"]" }) {
      final int before = sessions.getActiveSessions();
      final HttpURLConnection connection = begin(body);
      try {
        assertThat(connection.getResponseCode()).as(body).isEqualTo(400);
        assertThat(connection.getHeaderField(ARCADEDB_SESSION_ID)).as(body).isNull();
        assertThat(readError(connection)).as(body).contains("JSON object");
      } finally {
        connection.disconnect();
      }
      assertThat(sessions.getActiveSessions()).as(body).isEqualTo(before);
    }

    // The refused begins left nothing attached to the worker threads: a valid begin still succeeds
    final HttpURLConnection connection = begin("{\"isolationLevel\":\"REPEATABLE_READ\"}");
    try {
      assertThat(connection.getResponseCode()).isEqualTo(204);
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void aBlankBodyIsNoBody() throws Exception {
    // Whitespace only is an absent body, not a malformed one: it must keep opening a transaction at the database default
    for (final String body : new String[] { "", "  ", "\n" }) {
      final HttpURLConnection connection = begin(body);
      try {
        assertThat(connection.getResponseCode()).as("'" + body + "'").isEqualTo(204);
        assertThat(connection.getHeaderField(ARCADEDB_SESSION_ID)).as("'" + body + "'").isNotNull();
      } finally {
        connection.disconnect();
      }
    }
  }

  @Test
  void anUnknownIsolationLevelAnswers400AndOpensNoSession() throws Exception {
    final HttpSessionManager sessions = getServer(0).getHttpServer().getSessionManager();

    for (final String value : new String[] { "SERIALIZABLE", "read_committed", "" }) {
      final int before = sessions.getActiveSessions();
      final HttpURLConnection connection = begin("{\"isolationLevel\":\"" + value + "\"}");
      try {
        assertThat(connection.getResponseCode()).as("'" + value + "'").isEqualTo(400);
        assertThat(connection.getHeaderField(ARCADEDB_SESSION_ID)).isNull();
        final String error = readError(connection);
        assertThat(error).as("the error names the parameter and what it accepts").contains("isolationLevel")
            .contains("READ_COMMITTED").contains("REPEATABLE_READ");
      } finally {
        connection.disconnect();
      }
      assertThat(sessions.getActiveSessions()).as("a refused begin must not leave a session behind").isEqualTo(before);
    }

    // The refused begins left nothing attached to the worker threads: a valid begin still succeeds
    final HttpURLConnection connection = begin("{\"isolationLevel\":\"REPEATABLE_READ\"}");
    try {
      assertThat(connection.getResponseCode()).isEqualTo(204);
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void aNonStringIsolationLevelAnswers400AndOpensNoSession() throws Exception {
    // The spec declares a string. A one-element array is the case that matters: JSONObject.getString() unwraps it, so it
    // used to begin a transaction at the level it carried
    final HttpSessionManager sessions = getServer(0).getHttpServer().getSessionManager();

    for (final String value : new String[] { "1", "true", "{\"level\":\"READ_COMMITTED\"}", "[\"READ_COMMITTED\"]", "[]" }) {
      final int before = sessions.getActiveSessions();
      final HttpURLConnection connection = begin("{\"isolationLevel\":" + value + "}");
      try {
        assertThat(connection.getResponseCode()).as(value).isEqualTo(400);
        assertThat(connection.getHeaderField(ARCADEDB_SESSION_ID)).as(value).isNull();
        assertThat(readError(connection)).as(value).contains("isolationLevel");
      } finally {
        connection.disconnect();
      }
      assertThat(sessions.getActiveSessions()).as(value).isEqualTo(before);
    }
  }

  private HttpSession sessionOf(final String sessionId) {
    final ServerSecurityUser root = getServer(0).getSecurity().getUser("root");
    final HttpSession session = getServer(0).getHttpServer().getSessionManager().getSessionById(root, sessionId);
    assertThat(session).as("session " + sessionId).isNotNull();
    return session;
  }

  private HttpURLConnection begin(final String body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        getServerHttpUrl("/api/v1/begin/") + getDatabaseName()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    if (body != null) {
      final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setRequestProperty("Content-Length", Integer.toString(bytes.length));
      connection.setDoOutput(true);
      try (final OutputStream out = connection.getOutputStream()) {
        out.write(bytes);
      }
    }
    connection.connect();
    return connection;
  }
}
