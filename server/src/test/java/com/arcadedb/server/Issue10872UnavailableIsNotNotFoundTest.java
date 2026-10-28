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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.DatabaseNotFoundException;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10872: HTTP answered "the database exists and cannot serve you now" - closed by {@code close database}, held by a
 * restore, being dropped - with the {@code 404 Database not found} of a name that is no database, with the same exception
 * class, so in production mode, which conceals the detail, the two bodies were identical. The resolver now raises
 * {@link DatabaseNotFoundException} for a name that is no database and keeps {@link DatabaseNotAvailableException}, with the
 * state as its reason, for one that is there; HTTP answers the first 404 and the second 503 under a label naming the state,
 * and the remote client rebuilds both types.
 * <p>
 * Run in production mode on purpose: the free-form detail is concealed there, so what tells the two apart has to be the
 * status, the label, the exception class and its arguments.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10872UnavailableIsNotNotFoundTest extends BaseGraphServerTest {
  private static final String NAME    = "Issue10872Held";
  private static final String MISSING = "Issue10872NoSuchDatabase";

  private ArcadeDBServer server;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_MODE, "production");
  }

  @BeforeEach
  void createHeldDatabase() {
    server = getServer(0);
    final ServerDatabase created = server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
    created.getSchema().createDocumentType("T");
    created.transaction(() -> created.command("sql", "INSERT INTO T SET k = 1"));
  }

  @AfterEach
  void dropHeldDatabase() {
    server.releaseDatabaseNameReservedForRestore(NAME);
    server.unmarkClosedByCommand(NAME);
    if (server.existsDatabase(NAME))
      server.dropLocalDatabase(server.getDatabaseForOperator(NAME).getEmbedded(), NAME);
    FileUtils.deleteRecursively(new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
  }

  @Test
  void aNameThatIsNoDatabaseIsNotFound() throws Exception {
    for (final HttpResponse<String> response : new HttpResponse[] { post("query", MISSING, "select 1"),
        post("command", MISSING, "insert into T set k = 2") }) {
      assertThat(response.statusCode()).isEqualTo(404);
      final JSONObject body = new JSONObject(response.body());
      assertThat(body.getString("error")).isEqualTo("Database not found");
      assertThat(body.getString("exception")).isEqualTo(DatabaseNotFoundException.class.getName());
      assertThat(body.has("detail")).as("production mode conceals the detail").isFalse();
    }

    // the resolver itself: a name with no database on disk reaches the allowLoad=false refusal
    assertThatThrownBy(() -> server.getDatabaseForRequest(MISSING)).isInstanceOf(DatabaseNotFoundException.class);
    assertThatThrownBy(() -> server.getDatabaseForRequest("..")).as("a name that leaves the database directory")
        .isInstanceOf(DatabaseNotFoundException.class);
  }

  /** The report's run 1: a database a {@code close database} command closed is there, and says so. */
  @Test
  void aDatabaseClosedByCommandIsUnavailableNotMissing() throws Exception {
    assertThat(serverCommand("close database " + NAME).statusCode()).isEqualTo(200);

    final HttpResponse<String> closed = post("query", NAME, "select from T");
    final HttpResponse<String> missing = post("query", MISSING, "select from T");
    assertThat(closed.statusCode()).isEqualTo(503);
    assertThat(missing.statusCode()).isEqualTo(404);

    final JSONObject body = new JSONObject(closed.body());
    assertThat(body.getString("error")).isEqualTo("Database is closed");
    assertThat(body.getString("exception")).isEqualTo(DatabaseNotAvailableException.class.getName());
    assertThat(body.getString("exceptionArgs")).isEqualTo("CLOSED");
    assertThat(body.has("detail")).as("production mode conceals the detail").isFalse();
    assertThat(closed.headers().firstValue("Retry-After")).as("how long an operator keeps it closed is unknown").isEmpty();

    assertThat(post("command", NAME, "insert into T set k = 2").statusCode()).isEqualTo(503);
    assertThatThrownBy(() -> server.getDatabaseForRequest(NAME)).isInstanceOf(DatabaseNotAvailableException.class)
        .isNotInstanceOf(DatabaseNotFoundException.class)
        .satisfies(e -> assertThat(((DatabaseNotAvailableException) e).getReason()).isEqualTo(DatabaseNotAvailableException.Reason.CLOSED));

    // the remote client is told the state, typed, and does not spin retrying a database closed on purpose
    try (final RemoteDatabase remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(0), NAME, "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {
      assertThatThrownBy(() -> remote.query("sql", "select from T").close()).isInstanceOf(DatabaseNotAvailableException.class)
          .isNotInstanceOf(DatabaseNotFoundException.class)
          .satisfies(e -> assertThat(((DatabaseNotAvailableException) e).getReason()).isEqualTo(DatabaseNotAvailableException.Reason.CLOSED));
    }
    try (final RemoteDatabase remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(0), MISSING, "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {
      assertThatThrownBy(() -> remote.query("sql", "select 1").close()).isInstanceOf(DatabaseNotFoundException.class);
    }

    assertThat(serverCommand("open database " + NAME).statusCode()).isEqualTo(200);
    assertThat(post("query", NAME, "select from T").statusCode()).isEqualTo(200);
  }

  /** The report's run 2: in the same second a read of a database a restore holds answers 200 and a write 503, never 404. */
  @Test
  void aWriteToADatabaseARestoreHoldsIsUnavailableNotMissing() throws Exception {
    server.reserveDatabaseNameForRestore(NAME);
    try {
      assertThat(post("query", NAME, "select count(*) as c from T").statusCode()).isEqualTo(200);

      final HttpResponse<String> write = post("command", NAME, "insert into T set k = 2");
      assertThat(write.statusCode()).isEqualTo(503);
      final JSONObject body = new JSONObject(write.body());
      assertThat(body.getString("error")).isEqualTo("Database does not accept writes");
      assertThat(body.getString("exceptionArgs")).isEqualTo("WRITES_REFUSED");

      assertThat(post("command", MISSING, "insert into T set k = 2").statusCode()).isEqualTo(404);
    } finally {
      server.releaseDatabaseNameReservedForRestore(NAME);
    }
    assertThat(post("command", NAME, "insert into T set k = 3").statusCode()).isEqualTo(200);
  }

  /** A restore holding a name that is not open: no client opens it, and the refusal names the restore. */
  @Test
  void aDatabaseARestoreHoldsClosedIsBeingRestored() throws Exception {
    server.getDatabaseForOperator(NAME).getEmbedded().close();
    server.removeDatabase(NAME);
    server.reserveDatabaseNameForRestore(NAME);
    try {
      final HttpResponse<String> response = post("query", NAME, "select 1");
      assertThat(response.statusCode()).isEqualTo(503);
      assertThat(new JSONObject(response.body()).getString("error")).isEqualTo("Database is being restored");
    } finally {
      server.releaseDatabaseNameReservedForRestore(NAME);
    }
  }

  private HttpResponse<String> post(final String endpoint, final String database, final String command) throws Exception {
    return send(HttpRequest.newBuilder().uri(URI.create(getServerHttpUrl(0, "/api/v1/" + endpoint + "/" + database)))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", "sql").put("command", command).toString())));
  }

  private HttpResponse<String> serverCommand(final String command) throws Exception {
    return send(HttpRequest.newBuilder().uri(URI.create(getServerHttpUrl(0, "/api/v1/server")))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString())));
  }

  private static HttpResponse<String> send(final HttpRequest.Builder builder) throws Exception {
    final HttpRequest request = builder.header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json").build();
    try (final HttpClient client = HttpClient.newHttpClient()) {
      return client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }
  }
}
