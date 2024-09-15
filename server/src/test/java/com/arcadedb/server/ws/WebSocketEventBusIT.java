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

import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.CallableNoReturn;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.xnio.http.UpgradeFailedException;

import java.util.logging.*;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;

public class WebSocketEventBusIT extends BaseGraphServerTest {
  private static final int DELAY_MS = 1000;

  @Test
  public void closeUnsubscribesAll() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V1")));
        assertThat(result.get("result")).isEqualTo("ok");
        result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V2")));
        assertThat(result.get("result")).isEqualTo("ok");
      }
      Thread.sleep(DELAY_MS);
      assertThat(getServer(0).getHttpServer().getWebSocketEventBus().getDatabaseSubscriptions("graph")).isEmpty();
    }, "closeUnsubscribesAll");
  }

  @Test
  public void badCloseIsCleanedUp() throws Throwable {
    execute(() -> {
      {
        final var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root",
            BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
        final var result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V1")));
        assertThat(result.get("result")).isEqualTo("ok");
        client.breakConnection();
      }

      try (final var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final JSONObject result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V1")));
        assertThat(result.get("result")).isEqualTo("ok");

        getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();
        final var json = getJsonMessageOrFail(client);
        assertThat(json.get("changeType")).isEqualTo("create");

        // The sending thread should have detected and removed the zombie connection.
        Thread.sleep(DELAY_MS);
        assertThat(getServer(0).getHttpServer().getWebSocketEventBus().getDatabaseSubscriptions("graph")).hasSize(1);
      }

      Thread.sleep(DELAY_MS);
      assertThat(
          getServer(0).getHttpServer().getWebSocketEventBus().getDatabaseSubscriptions(getDatabaseName()).isEmpty()).isTrue();
    }, "badCloseIsCleanedUp");
  }

  @Test
  public void invalidJsonReturnsError() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = new JSONObject(client.send("42"));
        assertThat(result.get("result")).isEqualTo("error");
        assertThat(result.get("exception")).isEqualTo("com.arcadedb.serializer.json.JSONException");
      }
    }, "invalidJsonReturnsError");
  }

  @Test
  public void authenticationFailureReturns403() throws Throwable {
    execute(() -> {
      assertThatThrownBy(() -> {
        new WebSocketClientHelper("ws://localhost:2480/ws", "root", "bad");
      }).isInstanceOf(UpgradeFailedException.class)
          .hasMessageContaining("403");

    }, "authenticationFailureReturns403");
  }

  @Test
  public void invalidDatabaseReturnsError() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = new JSONObject(client.send(buildActionMessage("subscribe", "invalid")));
        assertThat(result.get("result")).isEqualTo("error");
        assertThat(result.get("exception")).isEqualTo("com.arcadedb.exception.DatabaseOperationException");
      }
    }, "invalidDatabaseReturnsError");
  }

  @Test
  public void unsubscribeWithoutSubscribeDoesNothing() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = new JSONObject(client.send(buildActionMessage("unsubscribe", "graph")));
        assertThat(result.get("result")).isEqualTo("ok");
      }
    }, "unsubscribeWithoutSubscribeDoesNothing");
  }

  @Test
  public void invalidActionReturnsError() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = new JSONObject(client.send(buildActionMessage("invalid", "graph")));
        assertThat(result.get("result")).isEqualTo("error");
        assertThat(result.get("detail")).isEqualTo("invalid is not a valid action.");
      }
    }, "invalidActionReturnsError");
  }

  @Test
  public void missingActionReturnsError() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = new JSONObject(client.send("{\"database\": \"graph\"}"));
        assertThat(result.get("result")).isEqualTo("error");
        assertThat(result.get("detail")).isEqualTo("Property 'action' is required.");
      }
    }, "missingActionReturnsError");
  }

  @Test
  public void subscribeDatabaseWorks() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root",
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
  public void twoSubscribersAreServiced() throws Throwable {
    execute(() -> {
      final var clients = new WebSocketClientHelper[] {
          new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS),
          new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS) };

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
  public void subscribeTypeWorks() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root",
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
  public void subscribeChangeTypeWorks() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root",
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
  public void subscribeMultipleChangeTypesWorks() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root",
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
        assertThat(record.get("@rid")).isEqualTo(v1.getIdentity().toString());
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
  public void subscribeChangeTypeDoesNotPushOtherChangeTypes() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = client.send(buildActionMessage("subscribe", "graph", null, new String[] { "update" }));
        assertThat(new JSONObject(result).get("result")).isEqualTo("ok");

        getServerDatabase(0, "graph").newVertex("V2").save();

        assertThat(client.popMessage(500)).isNull();
      }
    }, "subscribeChangeTypeDoesNotPushOtherChangeTypes");
  }

  @Test
  public void subscribeTypeDoesNotPushOtherTypes() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final var result = client.send(buildActionMessage("subscribe", "graph", "V1"));
        assertThat(new JSONObject(result).get("result")).isEqualTo("ok");

        getServerDatabase(0, "graph").newVertex("V2").save();

        assertThat(client.popMessage(500)).isNull();
      }
    }, "subscribeTypeDoesNotPushOtherTypes");
  }

  @Test
  public void unsubscribeDatabaseWorks() throws Throwable {
    execute(() -> {
      try (final var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root",
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
}
