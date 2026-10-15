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
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10084: the {@code /ws} insert session counted a row a before-create or before-update listener vetoed as inserted or
 * updated, because it called the {@code void} {@code save()} and threw the engine's answer away - so a {@code batchAck} told a
 * client driving the load off its counters that rows the engine refused were durable. A vetoed row is {@code ignored}, as in the
 * gRPC loader this session is the twin of, and a vetoed upsert does not fall through to inserting a second record for the key.
 * The edge arm hit a {@link NullPointerException} out of the adjacency append (issue #10085) and answered {@code DB_ERROR}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10084InsertSessionVetoIT extends BaseGraphServerTest {
  private static final String DOC  = "Doc10084";
  private static final String VTX  = "Vtx10084";
  private static final String EDGE = "Link10084";

  private Database database;

  @BeforeEach
  void createTypes() {
    database = getServerDatabase(0, getDatabaseName());
    database.getSchema().createDocumentType(DOC).getEvents().registerListener(refuseCreate());
    database.getSchema().getType(DOC).getEvents().registerListener(refuseUpdate());
    database.getSchema().createVertexType(VTX).getEvents().registerListener(refuseCreate());
    database.getSchema().createEdgeType(EDGE).getEvents().registerListener(refuseCreate());
    database.getSchema().getType(EDGE).getEvents().registerListener(refuseUpdate());
  }

  @AfterEach
  void dropTypes() {
    database.command("sql", "DROP TYPE " + EDGE + " IF EXISTS UNSAFE");
    database.command("sql", "DROP TYPE " + VTX + " IF EXISTS UNSAFE");
    database.command("sql", "DROP TYPE " + DOC + " IF EXISTS UNSAFE");
  }

  /** The reporter's first repro: a chunk the create listener refuses is acknowledged as ignored, and nothing is stored. */
  @Test
  void aRefusedCreateIsIgnoredAndNotInserted() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(new JSONObject().put("targetType", DOC)
          .put("transactionMode", "per_batch")))).getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1,
          row("refused-1"), row("kept"), row("refused-2"))));
      assertThat(ack.getLong("received", -1)).isEqualTo(3);
      assertThat(ack.getLong("inserted", -1)).isEqualTo(1);
      assertThat(ack.getLong("ignored", -1)).isEqualTo(2);
      assertThat(ack.getLong("updated", -1)).isZero();
      assertThat(ack.getLong("failed", -1)).isZero();

      final JSONObject committed = new JSONObject(client.send(control("commit", sessionId)));
      assertThat(committed.getJSONObject("summary").getLong("inserted", -1)).isEqualTo(1);
      assertThat(committed.getJSONObject("summary").getLong("ignored", -1)).isEqualTo(2);
    }

    assertThat(database.countType(DOC, false)).isEqualTo(1);
  }

  @Test
  void aRefusedVertexCreateIsIgnoredAndNotInserted() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(new JSONObject().put("targetType", VTX)
          .put("transactionMode", "per_batch")))).getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1, row("refused"), row("kept"))));
      assertThat(ack.getLong("inserted", -1)).isEqualTo(1);
      assertThat(ack.getLong("ignored", -1)).isEqualTo(1);
      assertThat(ack.getLong("failed", -1)).isZero();
      new JSONObject(client.send(control("commit", sessionId)));
    }

    assertThat(database.countType(VTX, false)).isEqualTo(1);
  }

  /** The reporter's second repro: a refused upsert is ignored, the stored value stays, and no second record is inserted. */
  @Test
  void aRefusedUpdateIsIgnoredAndDoesNotFallThroughToAnInsert() throws Throwable {
    database.transaction(() -> database.newDocument(DOC).set("name", "probe0").set("city", "before").save());

    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(new JSONObject().put("targetType", DOC)
          .put("transactionMode", "per_batch").put("conflictMode", "update").put("keyColumns", new JSONArray().put("name")))))
          .getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1,
          new JSONObject().put("name", "probe0").put("city", "refused"))));
      assertThat(ack.getLong("received", -1)).isEqualTo(1);
      assertThat(ack.getLong("updated", -1)).isZero();
      assertThat(ack.getLong("inserted", -1)).isZero();
      assertThat(ack.getLong("ignored", -1)).isEqualTo(1);
      assertThat(ack.getLong("failed", -1)).isZero();
      new JSONObject(client.send(control("commit", sessionId)));
    }

    assertThat(database.countType(DOC, false)).isEqualTo(1);
    try (final ResultSet rs = database.query("sql", "select from " + DOC + " where name = 'probe0'")) {
      assertThat(rs.next().<String>getProperty("city")).isEqualTo("before");
    }
  }

  /** An update the listener allows is still counted as an update. */
  @Test
  void anAllowedUpdateIsStillCountedAsUpdated() throws Throwable {
    database.transaction(() -> database.newDocument(DOC).set("name", "probe1").set("city", "before").save());

    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(new JSONObject().put("targetType", DOC)
          .put("transactionMode", "per_batch").put("conflictMode", "update").put("keyColumns", new JSONArray().put("name")))))
          .getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1,
          new JSONObject().put("name", "probe1").put("city", "after"))));
      assertThat(ack.getLong("updated", -1)).isEqualTo(1);
      assertThat(ack.getLong("ignored", -1)).isZero();
      new JSONObject(client.send(control("commit", sessionId)));
    }

    try (final ResultSet rs = database.query("sql", "select from " + DOC + " where name = 'probe1'")) {
      assertThat(rs.next().<String>getProperty("city")).isEqualTo("after");
    }
  }

  /** The edge arm: a refused edge is ignored, not a {@code DB_ERROR} carrying the JDK's NullPointerException text. */
  @Test
  void aRefusedEdgeIsIgnoredAndNotADatabaseError() throws Throwable {
    final String[] rids = new String[3];
    database.transaction(() -> {
      rids[0] = database.newVertex(VTX).set("name", "kept").save().getIdentity().toString();
      rids[1] = database.newVertex(VTX).set("name", "kept").save().getIdentity().toString();
      rids[2] = database.newVertex(VTX).set("name", "kept").save().getIdentity().toString();
    });
    // The source owns a committed edge segment: the ordinary case, where the adjacency append dereferenced the null RID
    database.transaction(() -> database.lookupByRID(database.newRID(rids[0]), false).asVertex().newEdge(EDGE, database.newRID(rids[1]),
        "name", "kept"));

    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(new JSONObject().put("targetType", EDGE)
          .put("transactionMode", "per_batch")))).getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1,
          new JSONObject().put("name", "refused").put("@from", rids[0]).put("@to", rids[2]),
          new JSONObject().put("name", "kept").put("@from", rids[0]).put("@to", rids[2]))));
      assertThat(ack.getLong("received", -1)).isEqualTo(2);
      assertThat(ack.getLong("inserted", -1)).isEqualTo(1);
      assertThat(ack.getLong("ignored", -1)).isEqualTo(1);
      assertThat(ack.getLong("failed", -1)).isZero();
      assertThat(ack.has("errors")).isFalse();
      new JSONObject(client.send(control("commit", sessionId)));
    }

    assertThat(database.countType(EDGE, false)).isEqualTo(2);
  }

  /** An edge upsert the listener refuses is ignored too, and the edge keeps its stored value. */
  @Test
  void aRefusedEdgeUpdateIsIgnored() throws Throwable {
    final String[] rids = new String[2];
    database.transaction(() -> {
      final var from = database.newVertex(VTX).set("name", "kept").save();
      final var to = database.newVertex(VTX).set("name", "kept").save();
      rids[0] = from.getIdentity().toString();
      rids[1] = to.getIdentity().toString();
      from.newEdge(EDGE, to, "name", "e1", "weight", 1);
    });

    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(new JSONObject().put("targetType", EDGE)
          .put("transactionMode", "per_batch").put("conflictMode", "update").put("keyColumns", new JSONArray().put("name")))))
          .getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1,
          new JSONObject().put("name", "e1").put("weight", 2).put("flag", "refused").put("@from", rids[0]).put("@to", rids[1]))));
      assertThat(ack.getLong("updated", -1)).isZero();
      assertThat(ack.getLong("inserted", -1)).isZero();
      assertThat(ack.getLong("ignored", -1)).isEqualTo(1);
      new JSONObject(client.send(control("commit", sessionId)));
    }

    assertThat(database.countType(EDGE, false)).isEqualTo(1);
    try (final ResultSet rs = database.query("sql", "select from " + EDGE + " where name = 'e1'")) {
      assertThat(rs.next().<Integer>getProperty("weight")).isEqualTo(1);
    }
  }

  // --- helpers ---

  private static BeforeRecordCreateListener refuseCreate() {
    return record -> !isRefused(record.asDocument().getString("name"));
  }

  private static BeforeRecordUpdateListener refuseUpdate() {
    return record -> !"refused".equals(record.asDocument().getString("city"))
        && !"refused".equals(record.asDocument().getString("flag"));
  }

  private static boolean isRefused(final String name) {
    return name != null && name.startsWith("refused");
  }

  private static JSONObject row(final String name) {
    return new JSONObject().put("name", name);
  }

  private WebSocketClientHelper newClient() throws Exception {
    return new WebSocketClientHelper("ws://localhost:" + getServer(0).getHttpServer().getPort() + "/ws", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  private String start(final JSONObject options) {
    final JSONObject message = new JSONObject();
    message.put("action", "start");
    message.put("database", getDatabaseName());
    message.put("options", options);
    return message.toString();
  }

  private static String chunkOf(final String sessionId, final long chunkSeq, final JSONObject... records) {
    final JSONObject message = new JSONObject();
    message.put("action", "chunk");
    message.put("sessionId", sessionId);
    message.put("chunkSeq", chunkSeq);
    message.put("records", new JSONArray(records));
    return message.toString();
  }

  private static String control(final String action, final String sessionId) {
    final JSONObject message = new JSONObject();
    message.put("action", action);
    message.put("sessionId", sessionId);
    return message.toString();
  }
}
