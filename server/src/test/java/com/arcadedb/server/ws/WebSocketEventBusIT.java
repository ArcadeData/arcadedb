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

import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.utility.CallableNoReturn;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.xnio.http.UpgradeFailedException;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.arcadedb.schema.Property.RID_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WebSocketEventBusIT extends BaseGraphServerTest {
  @Test
  void closeUnsubscribesAll() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V1")));
        assertThat(result.get("result")).isEqualTo("ok");
        result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V2")));
        assertThat(result.get("result")).isEqualTo("ok");
      }
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .pollInterval(100, TimeUnit.MILLISECONDS)
          .until(() -> getServer(0).getHttpServer().getWebSocketEventBus().getDatabaseSubscriptions("graph").isEmpty());
    }, "closeUnsubscribesAll");
  }

  @Test
  void badCloseIsCleanedUp() throws Throwable {
    execute(() -> {
      {
        final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
            BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
        final var result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V1")));
        assertThat(result.get("result")).isEqualTo("ok");
        client.breakConnection();
      }

      try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final JSONObject result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V1")));
        assertThat(result.get("result")).isEqualTo("ok");

        getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();
        final var json = getJsonMessageOrFail(client);
        assertThat(json.get("changeType")).isEqualTo("create");

        // The sending thread should have detected and removed the zombie connection.
        Awaitility.await()
            .atMost(5, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .until(() -> getServer(0).getHttpServer().getWebSocketEventBus().getDatabaseSubscriptions("graph").size() == 1);
      }

      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .pollInterval(100, TimeUnit.MILLISECONDS)
          .until(() -> getServer(0).getHttpServer().getWebSocketEventBus().getDatabaseSubscriptions(getDatabaseName()).isEmpty());
    }, "badCloseIsCleanedUp");
  }

  @Test
  void invalidJsonReturnsError() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = new JSONObject(client.send("42"));
        assertThat(result.get("result")).isEqualTo("error");
        assertThat(result.get("exception")).isEqualTo("com.arcadedb.serializer.json.JSONException");
      }
    }, "invalidJsonReturnsError");
  }

  @Test
  void authenticationFailureReturns401() throws Throwable {
    // Wrong Basic credentials answer 401, as on every other route (#10180)
    execute(() ->
      assertThatThrownBy(() -> {
        new WebSocketClientHelper(getServerWsUrl("/ws"), "root", "bad");
      }).isInstanceOf(UpgradeFailedException.class)
          .hasMessageContaining("401"), "authenticationFailureReturns401");
  }

  @Test
  void invalidDatabaseReturnsError() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = new JSONObject(client.send(buildActionMessage("subscribe", "invalid")));
        assertThat(result.get("result")).isEqualTo("error");
        // The narrower type since issue #7874: a name that does not exist is reported as such rather than as
        // the generic DatabaseOperationException it used to be indistinguishable from. It is a SUBTYPE, so the
        // expectation is tightened rather than relaxed - a client keyed on the parent still matches.
        assertThat(result.get("exception")).isEqualTo("com.arcadedb.exception.DatabaseNotFoundException");
      }
    }, "invalidDatabaseReturnsError");
  }

  @Test
  void unsubscribeWithoutSubscribeDoesNothing() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = new JSONObject(client.send(buildActionMessage("unsubscribe", "graph")));
        assertThat(result.get("result")).isEqualTo("ok");
      }
    }, "unsubscribeWithoutSubscribeDoesNothing");
  }

  @Test
  void invalidActionReturnsError() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = new JSONObject(client.send(buildActionMessage("invalid", "graph")));
        assertThat(result.get("result")).isEqualTo("error");
        assertThat(result.get("detail")).isEqualTo("invalid is not a valid action.");
      }
    }, "invalidActionReturnsError");
  }

  @Test
  void missingActionReturnsError() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = new JSONObject(client.send("{\"database\": \"graph\"}"));
        assertThat(result.get("result")).isEqualTo("error");
        assertThat(result.get("detail")).isEqualTo("Property 'action' is required.");
      }
    }, "missingActionReturnsError");
  }

  @Test
  void subscribeDatabaseWorks() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = client.send(buildActionMessage("subscribe", "graph"));
        assertThat(new JSONObject(result).get("result")).isEqualTo("ok");

        final MutableVertex v = getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

        final var json = getJsonMessageOrFail(client);
        assertThat(json.get("changeType")).isEqualTo("create");
        final var record = json.getJSONObject("record");
        assertThat(record.get("name")).isEqualTo("test");
        assertThat(record.get("@type")).isEqualTo("V1");
      }
    }, "subscribeDatabaseWorks");
  }

  @Test
  void twoSubscribersAreServiced() throws Throwable {
    execute(() -> {
      final var clients = new WebSocketClientHelper[] {
          new WebSocketClientHelper(getServerWsUrl("/ws"), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS),
          new WebSocketClientHelper(getServerWsUrl("/ws"), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS) };

      for (final var client : clients) {
        final var result = client.send(buildActionMessage("subscribe", "graph"));
        assertThat(new JSONObject(result).get("result")).isEqualTo("ok");
      }

      getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

      for (final var client : clients) {
        final var json = getJsonMessageOrFail(client);
        assertThat(json.get("changeType")).isEqualTo("create");
        final var record = json.getJSONObject("record");
        assertThat(record.get("name")).isEqualTo("test");
        assertThat(record.get("@type")).isEqualTo("V1");

        client.close();
      }
    }, "twoSubscribersAreServiced");
  }

  @Test
  @Tag("slow")
  void concurrentSubscribesCreateSingleWatcherAndDeliverOnce() throws Throwable {
    // ISSUE #5024 (defect 2): subscribe() used a non-atomic containsKey/startDatabaseWatcher check-then-act. Many
    // clients subscribing to the same database at once each saw the watcher absent and started their own
    // DatabaseEventWatcherThread; every extra watcher registered as a record-event listener, so a single change was
    // published once per watcher and each subscriber received duplicate frames. After the fix exactly one watcher is
    // created and every subscriber receives each event exactly once.
    execute(() -> {
      final int clientCount = 8;
      final var clients = new WebSocketClientHelper[clientCount];
      for (int i = 0; i < clientCount; i++)
        clients[i] = new WebSocketClientHelper(getServerWsUrl("/ws"), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      final ExecutorService pool = Executors.newFixedThreadPool(clientCount);
      try {
        // Fire all subscribes at the same instant to maximise the race on the (previously) unguarded watcher creation.
        final var barrier = new CyclicBarrier(clientCount);
        final var errors = new CopyOnWriteArrayList<Throwable>();
        final var futures = new ArrayList<Future<?>>();
        for (final var client : clients) {
          futures.add(pool.submit(() -> {
            try {
              barrier.await();
              final var result = new JSONObject(client.send(buildActionMessage("subscribe", "graph")));
              assertThat(result.get("result")).isEqualTo("ok");
            } catch (final Throwable t) {
              errors.add(t);
            }
          }));
        }
        for (final var future : futures)
          future.get(20, TimeUnit.SECONDS);
        assertThat(errors).as("all concurrent subscribes must succeed").isEmpty();

        // A single change must reach each subscriber exactly once. Use a unique marker so the count is unaffected by any
        // unrelated change events, and a duplicate watcher is caught as the marker arriving twice.
        final String marker = "once-" + UUID.randomUUID();
        getServerDatabase(0, "graph").newVertex("V1").set("name", marker).save();

        for (final var client : clients) {
          int mine = 0;
          String message;
          while ((message = client.popMessage(1500)) != null) {
            final var record = new JSONObject(message).getJSONObject("record");
            if (marker.equals(record.getString("name", null)))
              mine++;
          }
          assertThat(mine).as("subscriber must receive its change event exactly once: a duplicate watcher delivers it twice").isEqualTo(1);
        }
      } finally {
        pool.shutdownNow();
        for (final var client : clients)
          client.close();
      }
    }, "concurrentSubscribesCreateSingleWatcherAndDeliverOnce");
  }

  @Test
  void subscribeTypeWorks() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = client.send(buildActionMessage("subscribe", "graph", "V1"));
        assertThat(new JSONObject(result).get("result")).isEqualTo("ok");

        getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

        final var json = getJsonMessageOrFail(client);
        assertThat(json.get("changeType")).isEqualTo("create");
        final var record = json.getJSONObject("record");
        assertThat(record.get("name")).isEqualTo("test");
        assertThat(record.get("@type")).isEqualTo("V1");
      }
    }, "subscribeTypeWorks");
  }

  @Test
  void subscribeChangeTypeWorks() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = client.send(buildActionMessage("subscribe", "graph", null, new String[] { "create" }));
        assertThat(new JSONObject(result).get("result")).isEqualTo("ok");

        getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

        final var json = getJsonMessageOrFail(client);
        assertThat(json.get("changeType")).isEqualTo("create");
        final var record = json.getJSONObject("record");
        assertThat(record.get("name")).isEqualTo("test");
        assertThat(record.get("@type")).isEqualTo("V1");
      }
    }, "subscribeChangeTypeWorks");
  }

  @Test
  void subscribeMultipleChangeTypesWorks() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = client.send(
            buildActionMessage("subscribe", "graph", null, new String[] { "create", "update", "delete" }));
        assertThat(new JSONObject(result).get("result")).isEqualTo("ok");

        final var v1 = getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

        var json = getJsonMessageOrFail(client);
        assertThat(json.get("changeType")).isEqualTo("create");
        var record = json.getJSONObject("record");
        assertThat(record.get("name")).isEqualTo("test");
        assertThat(record.get("@type")).isEqualTo("V1");

        v1.set("updated", true).save();

        json = getJsonMessageOrFail(client);
        assertThat(json.get("changeType")).isEqualTo("update");
        record = json.getJSONObject("record");
        assertThat(record.get(RID_PROPERTY)).isEqualTo(v1.getIdentity().toString());
        assertThat(record.getBoolean("updated")).isTrue();

        v1.delete();

        json = getJsonMessageOrFail(client);
        assertThat(json.get("changeType")).isEqualTo("delete");
        record = json.getJSONObject("record");
        assertThat(record.get("name")).isEqualTo("test");
        assertThat(record.get("@type")).isEqualTo("V1");
      }
    }, "subscribeMultipleChangeTypesWorks");
  }

  @Test
  void subscribeChangeTypeDoesNotPushOtherChangeTypes() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = client.send(buildActionMessage("subscribe", "graph", null, new String[] { "update" }));
        assertThat(new JSONObject(result).get("result")).isEqualTo("ok");

        getServerDatabase(0, "graph").newVertex("V2").save();

        assertThat(client.popMessage(500)).isNull();
      }
    }, "subscribeChangeTypeDoesNotPushOtherChangeTypes");
  }

  @Test
  void subscribeTypeDoesNotPushOtherTypes() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = client.send(buildActionMessage("subscribe", "graph", "V1"));
        assertThat(new JSONObject(result).get("result")).isEqualTo("ok");

        getServerDatabase(0, "graph").newVertex("V2").save();

        assertThat(client.popMessage(500)).isNull();
      }
    }, "subscribeTypeDoesNotPushOtherTypes");
  }

  @Test
  void edgeChangeDoesNotCrashEventBus() throws Throwable {
    // ISSUE #4479: creating an edge produced a ChangeEvent whose record is an Edge. isMatch() called
    // record.asDocument(), which throws ClassCastException for edges, killing the watcher thread and
    // permanently stopping the change stream (only an ArcadeDB restart recovered it). After the fix the
    // edge event must be delivered and the stream must keep working for subsequent changes.
    execute(() -> {
      try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = client.send(buildActionMessage("subscribe", "graph"));
        assertThat(new JSONObject(result).get("result")).isEqualTo("ok");

        final var db = getServerDatabase(0, "graph");
        final MutableVertex v1 = db.newVertex("V1").set("name", "src").save();
        final MutableVertex v2 = db.newVertex("V1").set("name", "dst").save();

        // Creating the edge generates a ChangeEvent backed by an Edge record: this used to crash the bus.
        v1.newEdge("E1", v2).save();

        // Drain messages until the edge create event arrives (edge creation also fires vertex update events,
        // so the ordering is not deterministic).
        var sawEdgeCreate = false;
        String message;
        while ((message = client.popMessage(2000)) != null) {
          final var json = new JSONObject(message);
          if ("create".equals(json.get("changeType")) && "E1".equals(json.getJSONObject("record").get("@type"))) {
            sawEdgeCreate = true;
            break;
          }
        }
        assertThat(sawEdgeCreate).as("Edge create event must be delivered without crashing the change stream").isTrue();

        // The stream must still be alive after the edge event: a subsequent vertex creation must be delivered.
        db.newVertex("V1").set("name", "after").save();
        var sawAfter = false;
        while ((message = client.popMessage(2000)) != null) {
          final var json = new JSONObject(message);
          final var record = json.getJSONObject("record");
          if ("create".equals(json.get("changeType")) && "after".equals(record.getString("name", null))) {
            sawAfter = true;
            break;
          }
        }
        assertThat(sawAfter).as("Change stream must stay alive after an edge change event").isTrue();
      }
    }, "edgeChangeDoesNotCrashEventBus");
  }

  @Test
  void unsubscribeDatabaseWorks() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = client.send(buildActionMessage("subscribe", "graph"));
        assertThat(new JSONObject(result).get("result")).isEqualTo("ok");

        result = client.send(buildActionMessage("unsubscribe", "graph"));
        assertThat(new JSONObject(result).get("result")).isEqualTo("ok");

        getServerDatabase(0, "graph").newVertex("V1").save();

        assertThat(client.popMessage(500)).isNull();
      }
    }, "unsubscribeDatabaseWorks");
  }

  @Test
  void subscribeToUnauthorizedDatabaseIsRejected() throws Throwable {
    // ISSUE #5021: the WebSocket change-stream authenticated the user only during the HTTP handshake and
    // then dropped the identity, so any authenticated user could subscribe to (and stream the full record
    // contents of) a database they had no rights to read. After the fix the SUBSCRIBE must be rejected with
    // an error frame and zero change events must be delivered for the unauthorized database.
    execute(() -> {
      final ServerSecurity security = getServer(0).getSecurity();
      final String user = "eve";
      final String password = "evepassword";
      // eve is a valid user (handshake succeeds) but is authorized only for an unrelated database.
      security.createUser(new JSONObject().put("name", user).put("password", security.encodePassword(password))
          .put("databases", new JSONObject().put("otherdb", new JSONArray(new String[] { "admin" }))));
      try {
        try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), user, password)) {
          final var result = new JSONObject(client.send(buildActionMessage("subscribe", "graph")));
          assertThat(result.get("result")).isEqualTo("error");
          assertThat(result.getString("error", "")).contains("Security");

          // A change in the unauthorized database must not reach eve.
          getServerDatabase(0, "graph").newVertex("V1").set("name", "secret").save();
          assertThat(client.popMessage(1000)).isNull();
        }
      } finally {
        security.dropUser(user);
      }
    }, "subscribeToUnauthorizedDatabaseIsRejected");
  }

  @Test
  void subscribeToAuthorizedDatabaseWorksForNonRootUser() throws Throwable {
    // ISSUE #5021 (no-regression): a non-root user explicitly authorized for the database must still be able
    // to subscribe and receive change events.
    execute(() -> {
      final ServerSecurity security = getServer(0).getSecurity();
      final String user = "alice";
      final String password = "alicepassword";
      security.createUser(new JSONObject().put("name", user).put("password", security.encodePassword(password))
          .put("databases", new JSONObject().put("graph", new JSONArray(new String[] { "admin" }))));
      try {
        try (final var client = new WebSocketClientHelper(getServerWsUrl("/ws"), user, password)) {
          final var result = new JSONObject(client.send(buildActionMessage("subscribe", "graph")));
          assertThat(result.get("result")).isEqualTo("ok");

          getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

          final var json = getJsonMessageOrFail(client);
          assertThat(json.get("changeType")).isEqualTo("create");
          final var record = json.getJSONObject("record");
          assertThat(record.get("name")).isEqualTo("test");
          assertThat(record.get("@type")).isEqualTo("V1");
        }
      } finally {
        security.dropUser(user);
      }
    }, "subscribeToAuthorizedDatabaseWorksForNonRootUser");
  }

  private static String buildActionMessage(final String action, final String database) {
    return buildActionMessage(action, database, null, null);
  }

  private static String buildActionMessage(final String action, final String database, final String type) {
    return buildActionMessage(action, database, type, null);
  }

  private static JSONObject getJsonMessageOrFail(final WebSocketClientHelper client) {
    final var message = client.popMessage();
    assertThat(message).as("No message received from the server.").isNotNull();
    return new JSONObject(message);
  }

  private static String buildActionMessage(final String action, final String database, final String type,
      final String[] changeTypes) {
    final var obj = new JSONObject();
    obj.put("action", action);
    obj.put("database", database);
    obj.put("type", type);
    obj.put("changeTypes", changeTypes);
    return obj.toString();
  }

  private void execute(final CallableNoReturn callback, final String testName) throws Throwable {
    LogManager.instance().log(this, Level.FINE, "BEGIN " + testName);
    try {
      callback.call();
    } catch (final Throwable e) {
      LogManager.instance().log(this, Level.SEVERE, "ERROR in " + testName, e);
      throw e;
    } finally {
      LogManager.instance().log(this, Level.FINE, "END " + testName);
    }
  }

  /*
   * Issue #10690: a {@code /ws} SUBSCRIBE frame resolved its database through {@code ArcadeDBServer.getDatabase(String)}, the
   * operator's entry point, which opens a database from disk past the {@code close database} mark and clears the mark on the way.
   * One subscribe frame from any user with access to the name reopened a database the operator had closed (for a cold backup, a
   * copy of the files) and served it again to every protocol. The subscribe now resolves through
   * {@code getDatabaseForClient}, like every other client surface, and is refused.
   */

  // Regression: #10690 a SUBSCRIBE frame must not reopen a database a close command closed
  @Test
  void aSubscribeDoesNotReopenADatabaseACloseCommandClosed() throws Exception {
    assertThat(postServerCommand("close database " + getDatabaseName()).statusCode()).isEqualTo(200);
    try {
      try (final WebSocketClientHelper client = newClient()) {
        final JSONObject answer = new JSONObject(client.send(subscribe()));
        assertThat(answer.getString("result", "")).as("the subscribe is refused").isEqualTo("error");
        assertThat(answer.getString("exception", "")).isEqualTo(DatabaseNotAvailableException.class.getName());
      }

      assertThat(getServer(0).existsDatabase(getDatabaseName())).as("the subscribe opened nothing").isFalse();
      // The mark survives the frame: every other client is still refused
      assertThatThrownBy(() -> getServer(0).getDatabaseForClient(getDatabaseName())).isInstanceOf(DatabaseNotAvailableException.class)
          .hasMessageContaining("close database");
      assertThat(getServer(0).getHttpServer().getWebSocketEventBus().getDatabaseSubscriptions(getDatabaseName()))
          .as("a refused subscribe leaves no subscription behind").isEmpty();
    } finally {
      assertThat(postServerCommand("open database " + getDatabaseName()).statusCode()).isEqualTo(200);
    }

    // Once the operator opens it again the subscribe is served as before
    try (final WebSocketClientHelper client = newClient()) {
      assertThat(new JSONObject(client.send(subscribe())).getString("result", "")).isEqualTo("ok");
    }
  }

  // Regression: #10690 a SUBSCRIBE frame must not reopen a database a close command closed
  /**
   * The watcher an earlier subscribe started is no pass for a later one: the database is resolved for the client on every
   * subscribe, not only when the first one creates the watcher.
   */
  @Test
  void aSubscribeIsRefusedEvenWhileAWatcherFromAnEarlierOneIsRunning() throws Exception {
    try (final WebSocketClientHelper first = newClient()) {
      assertThat(new JSONObject(first.send(subscribe())).getString("result", "")).isEqualTo("ok");

      assertThat(postServerCommand("close database " + getDatabaseName()).statusCode()).isEqualTo(200);
      try {
        try (final WebSocketClientHelper second = newClient()) {
          final JSONObject answer = new JSONObject(second.send(subscribe()));
          assertThat(answer.getString("result", "")).isEqualTo("error");
          assertThat(answer.getString("exception", "")).isEqualTo(DatabaseNotAvailableException.class.getName());
        }
        assertThat(getServer(0).existsDatabase(getDatabaseName())).isFalse();
      } finally {
        assertThat(postServerCommand("open database " + getDatabaseName()).statusCode()).isEqualTo(200);
      }
    }
  }

  /*
   * Issue #10801: a {@code /ws} change-stream watcher listens on the database INSTANCE it was started on, and outlived a
   * {@code close database} / {@code open database} cycle of its name. Every subscriber, the ones from before the close and every new
   * one after the reopen, was answered {@code ok} and then received nothing: {@code subscribe} found the old watcher in the map and
   * never started one on the new instance. The watcher of a name now follows the instance the server registers under it.
   */

  private static final int EVENT_WAIT_MS = 5_000;

  // Regression: #10801 the change stream must follow a database closed and opened again
  @Test
  void aSubscriberAfterTheReopenAndOneFromBeforeTheCloseBothReceiveTheNextCommit() throws Exception {
    try (final WebSocketClientHelper before = newClient()) {
      assertThat(new JSONObject(before.send(subscribe())).getString("result", "")).isEqualTo("ok");

      closeAndOpen();

      try (final WebSocketClientHelper after = newClient()) {
        assertThat(new JSONObject(after.send(subscribe())).getString("result", "")).isEqualTo("ok");

        getServerDatabase(0, getDatabaseName()).newVertex("V1").set("name", "issue10801-both").save();

        assertCreated(after, "issue10801-both", "a subscriber that subscribed after the reopen");
        assertCreated(before, "issue10801-both", "a subscriber from before the close, which the reopen did not end");
      }
    }
  }

  // Regression: #10801 the change stream must follow a database closed and opened again
  /** No subscribe follows the reopen: the registration of the new instance alone gives the subscriber already there its stream. */
  @Test
  void aSubscriberFromBeforeTheCloseReceivesTheNextCommitWithoutSubscribingAgain() throws Exception {
    try (final WebSocketClientHelper client = newClient()) {
      assertThat(new JSONObject(client.send(subscribe())).getString("result", "")).isEqualTo("ok");

      closeAndOpen();
      getServerDatabase(0, getDatabaseName()).newVertex("V1").set("name", "issue10801-kept").save();

      assertCreated(client, "issue10801-kept", "the subscription is to the name, and the name was opened again");
    }
  }

  // Regression: #10801 the change stream must follow a database closed and opened again
  @Test
  void theStreamSurvivesSeveralCyclesAndDeliversEachCommitOnce() throws Exception {
    try (final WebSocketClientHelper client = newClient()) {
      assertThat(new JSONObject(client.send(subscribe())).getString("result", "")).isEqualTo("ok");

      for (int cycle = 0; cycle < 3; cycle++) {
        closeAndOpen();
        getServerDatabase(0, getDatabaseName()).newVertex("V1").set("name", "issue10801-cycle-" + cycle).save();
        assertCreated(client, "issue10801-cycle-" + cycle, "cycle " + cycle);
        assertThat(client.popMessage(500)).as("one commit, one event: no watcher of an earlier instance publishes it too").isNull();
      }
    }
  }

  private void assertCreated(final WebSocketClientHelper client, final String name, final String who) {
    final String message = client.popMessage(EVENT_WAIT_MS);
    assertThat(message).as("%s receives the commit", who).isNotNull();
    final JSONObject json = new JSONObject(message);
    assertThat(json.getString("changeType")).isEqualTo("create");
    assertThat(json.getJSONObject("record").getString("name")).isEqualTo(name);
  }

  private void closeAndOpen() throws Exception {
    assertThat(postServerCommand("close database " + getDatabaseName()).statusCode()).isEqualTo(200);
    assertThat(postServerCommand("open database " + getDatabaseName()).statusCode()).isEqualTo(200);
  }

  private WebSocketClientHelper newClient() throws Exception {
    return new WebSocketClientHelper(getServerWsUrl(0, "/ws"), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private String subscribe() {
    return new JSONObject().put("action", "subscribe").put("database", getDatabaseName()).toString();
  }

  private HttpResponse<String> postServerCommand(final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(0, "/api/v1/server")))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .build();
    try (final HttpClient client = HttpClient.newHttpClient()) {
      return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
  }
}
