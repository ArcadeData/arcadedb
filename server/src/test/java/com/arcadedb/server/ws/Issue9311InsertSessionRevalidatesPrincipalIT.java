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

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9311: the {@code /ws} insert session kept the principal captured at the handshake for the life of the
 * connection, so a user dropped, re-passworded or stripped of the database kept inserting. The principal is now
 * re-resolved on every frame.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9311InsertSessionRevalidatesPrincipalIT extends BaseGraphServerTest {
  private static final String TYPE = "Doc9311";
  private static final String USER = "ws9311";
  private static final String PWD  = "ws9311-password";

  private Database database;

  @BeforeEach
  void setUp() {
    database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(USER))
      security.dropUser(USER);
    security.createUser(userConfiguration(security.encodePassword(PWD), new JSONArray().put("admin")));
  }

  @AfterEach
  void tearDown() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(USER))
      security.dropUser(USER);
    database.command("sql", "DROP TYPE " + TYPE + " IF EXISTS UNSAFE");
  }

  @Test
  void aDroppedUserCannotKeepInserting() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = open(client);
      assertThat(ack(client, sessionId, 1).getString("action", "")).isEqualTo("batchAck");

      getServer(0).getSecurity().dropUser(USER);

      assertRefused(new JSONObject(client.send(chunk(sessionId, 2))));
    }
    // Chunk 1 was committed before the revocation (per_batch), chunk 2 must not be
    assertThat(database.countType(TYPE, false)).isEqualTo(1);
  }

  @Test
  void aRevokedGrantCannotKeepInsertingNorStartANewSession() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = open(client);
      assertThat(ack(client, sessionId, 1).getString("action", "")).isEqualTo("batchAck");

      final ServerSecurity security = getServer(0).getSecurity();
      security.updateUser(userConfiguration(security.getUser(USER).getPassword(), new JSONArray()));

      assertRefused(new JSONObject(client.send(chunk(sessionId, 2))));
    }
    // Chunk 1 was committed before the revocation (per_batch), chunk 2 must not be
    assertThat(database.countType(TYPE, false)).isEqualTo(1);
  }

  @Test
  void aRevokedGrantRefusesANewStartOnTheSameChannel() throws Throwable {
    try (final var client = newClient()) {
      final ServerSecurity security = getServer(0).getSecurity();
      security.updateUser(userConfiguration(security.getUser(USER).getPassword(), new JSONArray()));

      assertRefused(new JSONObject(client.send(start())));
    }
  }

  @Test
  void aChangedPasswordCannotKeepInserting() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = open(client);

      final ServerSecurity security = getServer(0).getSecurity();
      security.updateUser(userConfiguration(security.encodePassword("another-password-1"), new JSONArray().put("admin")));

      assertRefused(new JSONObject(client.send(chunk(sessionId, 1))));
    }
    assertThat(database.countType(TYPE, false)).isZero();
  }

  @Test
  void anUnchangedUserKeepsWorkingAcrossAnUnrelatedUpdate() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = open(client);
      assertThat(ack(client, sessionId, 1).getString("action", "")).isEqualTo("batchAck");

      // A metadata-only update replaces the principal object but keeps the credentials: the connection must carry on
      final ServerSecurity security = getServer(0).getSecurity();
      security.updateUser(userConfiguration(security.getUser(USER).getPassword(), new JSONArray().put("admin")));

      assertThat(ack(client, sessionId, 2).getString("action", "")).isEqualTo("batchAck");
      assertThat(new JSONObject(client.send(control("commit", sessionId))).getString("action", "")).isEqualTo("committed");
    }
    assertThat(database.countType(TYPE, false)).isEqualTo(2);
  }

  private static void assertRefused(final JSONObject answer) {
    assertThat(answer.getString("result", "")).isEqualTo("error");
    assertThat(answer.getString("error", "")).isEqualTo("Security error");
  }

  private JSONObject userConfiguration(final String password, final JSONArray groups) {
    final JSONObject databases = new JSONObject();
    if (groups.length() > 0)
      databases.put(getDatabaseName(), groups);
    return new JSONObject().put("name", USER).put("password", password).put("databases", databases);
  }

  private WebSocketClientHelper newClient() throws Exception {
    return new WebSocketClientHelper("ws://localhost:" + getServer(0).getHttpServer().getPort() + "/ws", USER, PWD);
  }

  private String open(final WebSocketClientHelper client) throws Exception {
    return new JSONObject(client.send(start())).getString("sessionId");
  }

  private JSONObject ack(final WebSocketClientHelper client, final String sessionId, final long seq) throws Exception {
    return new JSONObject(client.send(chunk(sessionId, seq)));
  }

  private String start() {
    final JSONObject message = new JSONObject();
    message.put("action", "start");
    message.put("database", getDatabaseName());
    message.put("options", new JSONObject().put("targetType", TYPE).put("transactionMode", "per_batch"));
    return message.toString();
  }

  private static String chunk(final String sessionId, final long chunkSeq) {
    final JSONObject message = new JSONObject();
    message.put("action", "chunk");
    message.put("sessionId", sessionId);
    message.put("chunkSeq", chunkSeq);
    message.put("records", new JSONArray().put(new JSONObject().put("name", "n" + chunkSeq)));
    return message.toString();
  }

  private static String control(final String action, final String sessionId) {
    return new JSONObject().put("action", action).put("sessionId", sessionId).toString();
  }
}
