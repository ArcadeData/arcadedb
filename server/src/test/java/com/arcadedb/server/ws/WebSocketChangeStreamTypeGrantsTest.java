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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The record changes pushed on {@code /ws} follow the per-type read grants of the group of the subscriber, the same grants the
 * record reads of every other transport are answered under: a subscriber that may not read a type is sent none of its changes,
 * whichever way it subscribed (every type, or the type by name) and whichever kind of record the change is about, while the
 * changes of the types it may read keep flowing on the same subscription.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class WebSocketChangeStreamTypeGrantsTest extends BaseGraphServerTest {
  private static final String GROUP        = "streamTypeGrants";
  private static final String USER         = "streamTypeGrantsUser";
  private static final String PASSWORD     = "streamTypeGrantsPwd1";
  private static final String READABLE     = "StreamReadableDoc";
  private static final String UNREADABLE   = "StreamUnreadableDoc";
  private static final String LATER        = "StreamLaterDoc";
  private static final String VERTEX       = "StreamReadableVertex";
  private static final String UNREADABLE_V = "StreamUnreadableVertex";
  private static final String UNREADABLE_E = "StreamUnreadableLink";
  private static final int    QUEUE        = 200;
  private static final int    WAIT_MS      = 5_000;

  private Database database;

  @BeforeEach
  void createTypesGroupAndUser() {
    database = getServerDatabase(0, getDatabaseName());
    database.getSchema().createDocumentType(READABLE);
    database.getSchema().createDocumentType(UNREADABLE);
    database.getSchema().createDocumentType(LATER);
    database.getSchema().createVertexType(VERTEX);
    database.getSchema().createVertexType(UNREADABLE_V);
    database.getSchema().createEdgeType(UNREADABLE_E);

    final ServerSecurity security = getServer(0).getSecurity();
    new ServerControlPlane(getServer(0)).saveGroup(getDatabaseName(), GROUP, group(UNREADABLE, UNREADABLE_V, UNREADABLE_E));

    if (security.existsUser(USER))
      security.dropUser(USER);
    security.createUser(new JSONObject().put("name", USER).put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(GROUP))));
  }

  @AfterEach
  void dropTypesGroupAndUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(USER))
      security.dropUser(USER);
    new ServerControlPlane(getServer(0)).deleteGroup(getDatabaseName(), GROUP);

    for (final String type : new String[] { READABLE, UNREADABLE, LATER, VERTEX, UNREADABLE_V, UNREADABLE_E })
      database.command("sql", "DROP TYPE " + type + " IF EXISTS UNSAFE");
  }

  @Test
  void aSubscriberToEveryTypeIsSentTheChangesOfTheTypesItMayRead() throws Exception {
    try (final WebSocketClientHelper restricted = client(USER, PASSWORD);
        final WebSocketClientHelper full = client("root", DEFAULT_PASSWORD_FOR_TESTS)) {
      assertOk(restricted.send(subscribe(null)));
      assertOk(full.send(subscribe(null)));

      changeEveryWayAndThenCreate(UNREADABLE, READABLE);

      // What the group may read is delivered, and the stream is still there after the changes it may not read were passed over
      final Set<String> seenByRestricted = typesSeenUntilCreated(restricted, READABLE);
      assertThat(seenByRestricted).contains(READABLE).doesNotContain(UNREADABLE);

      // The same changes are delivered to a principal that reads everything: the filter is per subscriber, not per change
      final Set<String> seenByFull = typesSeenUntilCreated(full, READABLE);
      assertThat(seenByFull).contains(UNREADABLE, READABLE);
    }
  }

  @Test
  void aSubscriberToATypeItMayNotReadIsSentNoneOfItsChanges() throws Exception {
    try (final WebSocketClientHelper restricted = client(USER, PASSWORD)) {
      // Both on one channel: the readable subscription is the proof that passing over the other one leaves the channel open
      assertOk(restricted.send(subscribe(UNREADABLE)));
      assertOk(restricted.send(subscribe(READABLE)));

      changeEveryWayAndThenCreate(UNREADABLE, READABLE);

      final Set<String> seen = typesSeenUntilCreated(restricted, READABLE);
      assertThat(seen).contains(READABLE).doesNotContain(UNREADABLE);
    }
  }

  @Test
  void aSubscriberIsSentNoVertexOfATypeItMayNotRead() throws Exception {
    try (final WebSocketClientHelper restricted = client(USER, PASSWORD);
        final WebSocketClientHelper full = client("root", DEFAULT_PASSWORD_FOR_TESTS)) {
      assertOk(restricted.send(subscribe(null)));
      assertOk(full.send(subscribe(null)));

      database.transaction(() -> database.newVertex(UNREADABLE_V).set("name", "hidden").save());
      database.transaction(() -> database.newVertex(VERTEX).set("name", "shown").save());
      database.transaction(() -> database.newDocument(READABLE).set("name", "after").save());

      final Set<String> seenByRestricted = typesSeenUntilCreated(restricted, READABLE);
      assertThat(seenByRestricted).contains(VERTEX, READABLE).doesNotContain(UNREADABLE_V);

      final Set<String> seenByFull = typesSeenUntilCreated(full, READABLE);
      assertThat(seenByFull).contains(UNREADABLE_V, VERTEX, READABLE);
    }
  }

  @Test
  void aSubscriberIsSentNoRelationshipOfATypeItMayNotRead() throws Exception {
    try (final WebSocketClientHelper restricted = client(USER, PASSWORD);
        final WebSocketClientHelper full = client("root", DEFAULT_PASSWORD_FOR_TESTS)) {
      assertOk(restricted.send(subscribe(null)));
      assertOk(full.send(subscribe(null)));

      database.transaction(() -> {
        final MutableVertex from = database.newVertex(VERTEX).set("name", "from").save();
        final MutableVertex to = database.newVertex(VERTEX).set("name", "to").save();
        from.newEdge(UNREADABLE_E, to).set("weight", 1).save();
      });
      database.transaction(() -> database.newDocument(READABLE).set("name", "after").save());

      final Set<String> seenByRestricted = typesSeenUntilCreated(restricted, READABLE);
      assertThat(seenByRestricted).contains(VERTEX, READABLE).doesNotContain(UNREADABLE_E);

      final Set<String> seenByFull = typesSeenUntilCreated(full, READABLE);
      assertThat(seenByFull).contains(VERTEX, UNREADABLE_E, READABLE);
    }
  }

  @Test
  void aGrantRemovedAfterTheSubscriptionIsHonouredByTheNextChanges() throws Exception {
    try (final WebSocketClientHelper restricted = client(USER, PASSWORD)) {
      assertOk(restricted.send(subscribe(null)));

      // Readable now: its change is delivered
      database.transaction(() -> database.newDocument(LATER).set("name", "before").save());
      assertThat(typesSeenUntilCreated(restricted, LATER)).contains(LATER);

      // The group loses the read grant on the type while the subscription is open
      new ServerControlPlane(getServer(0)).saveGroup(getDatabaseName(), GROUP, group(UNREADABLE, UNREADABLE_V, UNREADABLE_E, LATER));

      database.transaction(() -> database.newDocument(LATER).set("name", "after").save());
      database.transaction(() -> database.newDocument(READABLE).set("name", "barrier").save());

      final Set<String> seen = typesSeenUntilCreated(restricted, READABLE);
      assertThat(seen).contains(READABLE).doesNotContain(LATER);
    }
  }

  /** A create, an update and a delete of {@code type}, then a create of {@code lastType}: one change of each kind, in order. */
  private void changeEveryWayAndThenCreate(final String type, final String lastType) {
    database.begin();
    final MutableDocument document = database.newDocument(type).set("name", "created");
    document.save();
    database.commit();

    database.begin();
    document.set("name", "updated").save();
    database.commit();

    database.begin();
    document.delete();
    database.commit();

    database.transaction(() -> database.newDocument(lastType).set("name", "last").save());
  }

  /**
   * The names of the types of every change delivered on {@code client} up to, and including, the creation of a {@code lastType}
   * record: changes of one database reach the stream in the order they were made, so everything the subscriber was sent before
   * that one is in the answer.
   */
  private static Set<String> typesSeenUntilCreated(final WebSocketClientHelper client, final String lastType) {
    final Set<String> types = new HashSet<>();
    String message;
    while ((message = client.popMessage(WAIT_MS)) != null) {
      final JSONObject change = new JSONObject(message);
      final String type = change.getJSONObject("record").getString("@type", "");
      types.add(type);
      if ("create".equals(change.getString("changeType", "")) && lastType.equals(type))
        break;
    }
    return types;
  }

  private WebSocketClientHelper client(final String user, final String password) throws Exception {
    return new WebSocketClientHelper(getServerWsUrl("/ws"), user, password, QUEUE);
  }

  private String subscribe(final String type) {
    final JSONObject message = new JSONObject().put("action", "subscribe").put("database", getDatabaseName());
    if (type != null)
      message.put("type", type);
    return message.toString();
  }

  private static void assertOk(final String answer) {
    assertThat(new JSONObject(answer).getString("result", "")).isEqualTo("ok");
  }

  /** Reads every type, writes every type, except {@code withoutRead}: those it writes and never reads. */
  private static JSONObject group(final String... withoutRead) {
    final JSONObject types = new JSONObject().put("*", new JSONObject().put("access",
        new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord")));
    for (final String type : withoutRead)
      types.put(type, new JSONObject().put("access", new JSONArray().put("createRecord").put("updateRecord").put("deleteRecord")));
    return new JSONObject().put("access", new JSONArray()).put("types", types);
  }
}
