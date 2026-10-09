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
package com.arcadedb.server.ws;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8749: production mode conceals the engine's error text on HTTP, gRPC, PostgreSQL, MongoDB and Gremlin, but the
 * {@code /ws} insert session sent it in every mode - in the error frame of a {@code per_stream} commit refused on a
 * duplicated key and in the per-row {@code errors} entries of a {@code batchAck}, both of which carry the stored key
 * VALUES. The code ({@code CONFLICT}) and the exception class are what a client branches on and stay; the text the
 * protocol words about the request itself stays too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8749WebSocketErrorConcealmentIT extends BaseGraphServerTest {
  private static final String TYPE   = "Keyed8749";
  private static final String SECRET = "stored-secret-8749";

  private Database database;

  @BeforeEach
  void createKeyedType() {
    database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".name STRING");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (name) UNIQUE");
    database.transaction(() -> database.newDocument(TYPE).set("name", SECRET).save());
  }

  @AfterEach
  void dropKeyedType() {
    database.command("sql", "DROP TYPE " + TYPE + " IF EXISTS UNSAFE");
  }

  @Test
  void perRowConflictIsConcealedInProductionMode() throws Exception {
    final JSONObject error = withMode("production", this::perRowConflict);

    assertThat(error.getString("code", "")).isEqualTo("CONFLICT");
    assertThat(error.getString("exception", "")).endsWith("DuplicatedKeyException");
    assertThat(error.getString("message", "")).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
    assertThat(error.toString()).doesNotContain(SECRET);
  }

  @Test
  void perRowConflictKeepsItsTextInDevelopmentMode() throws Exception {
    final JSONObject error = withMode("development", this::perRowConflict);

    assertThat(error.getString("code", "")).isEqualTo("CONFLICT");
    assertThat(error.getString("message", "")).contains(SECRET);
  }

  @Test
  void refusedCommitIsConcealedInProductionMode() throws Exception {
    final JSONObject refused = withMode("production", this::refusedCommit);

    assertThat(refused.getString("result", "")).isEqualTo("error");
    assertThat(refused.getString("error", "")).isEqualTo("Insert session error");
    assertThat(refused.getString("exception", "")).endsWith("DuplicatedKeyException");
    assertThat(refused.getString("detail", "")).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
    assertThat(refused.toString()).doesNotContain(SECRET);
  }

  @Test
  void refusedCommitKeepsItsTextInDevelopmentMode() throws Exception {
    final JSONObject refused = withMode("development", this::refusedCommit);

    assertThat(refused.getString("exception", "")).endsWith("DuplicatedKeyException");
    assertThat(refused.getString("detail", "")).contains(SECRET);
  }

  /** The text the protocol words about the request is what the client needs to fix it, and stays in production. */
  @Test
  void requestValidationTextIsKeptInProductionMode() throws Exception {
    final JSONObject refused = withMode("production", () -> {
      try (final WebSocketClientHelper client = newClient()) {
        return new JSONObject(client.send(chunkOf("no-such-session-8749", 1, new JSONArray().put(new JSONObject().put("name", "x")))));
      }
    });

    assertThat(refused.getString("result", "")).isEqualTo("error");
    assertThat(refused.getString("detail", "")).contains("not found or expired");
  }

  /** The default conflict mode with key columns reports a duplicate as a failed row: its errors entry. */
  private JSONObject perRowConflict() throws Exception {
    try (final WebSocketClientHelper client = newClient()) {
      final JSONObject options = new JSONObject().put("targetType", TYPE).put("keyColumns", new JSONArray().put("name"));
      final String sessionId = new JSONObject(client.send(start(options))).getString("sessionId");
      final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1, new JSONArray()
          .put(new JSONObject().put("name", SECRET)).put(new JSONObject().put("name", "fresh-8749")))));
      assertThat(ack.getLong("failed", -1)).isEqualTo(1);
      client.send(control("rollback", sessionId));
      return ack.getJSONArray("errors").getJSONObject(0);
    }
  }

  /** {@code per_stream} without key columns finds the duplicate at the session's commit: the error frame. */
  private JSONObject refusedCommit() throws Exception {
    try (final WebSocketClientHelper client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(new JSONObject().put("targetType", TYPE)))).getString("sessionId");
      final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1, new JSONArray().put(new JSONObject().put("name", SECRET)))));
      assertThat(ack.getLong("inserted", -1)).isEqualTo(1);
      return new JSONObject(client.send(control("commit", sessionId)));
    }
  }

  private <T> T withMode(final String mode, final Callable<T> work) throws Exception {
    final Object previous = getServer(0).getConfiguration().getValue(GlobalConfiguration.SERVER_MODE);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, mode);
    try {
      return work.call();
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, previous);
    }
  }

  private WebSocketClientHelper newClient() throws Exception {
    return new WebSocketClientHelper("ws://localhost:" + getServer(0).getHttpServer().getPort() + "/ws", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  private String start(final JSONObject options) {
    return new JSONObject().put("action", "start").put("database", getDatabaseName()).put("options", options).toString();
  }

  private static String chunkOf(final String sessionId, final long chunkSeq, final JSONArray records) {
    return new JSONObject().put("action", "chunk").put("sessionId", sessionId).put("chunkSeq", chunkSeq).put("records", records)
        .toString();
  }

  private static String control(final String action, final String sessionId) {
    return new JSONObject().put("action", action).put("sessionId", sessionId).toString();
  }
}
