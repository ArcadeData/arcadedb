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
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.utility.CallableNoReturn;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.xnio.http.UpgradeFailedException;

import java.util.logging.Level;

public class WebSocketEventBusIT extends StaticBaseServerTest {
  private static final int DELAY_MS = 1000;

  @Test
  public void closeUnsubscribesAll() throws Throwable {
    execute(() -> {
      try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V1")));
        Assertions.assertEquals("ok", result.get("result"));
        result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V2")));
        Assertions.assertEquals("ok", result.get("result"));
      }
      Thread.sleep(DELAY_MS);
      Assertions.assertEquals(0, getServer(0).getHttpServer().getWebSocketEventBus().getDatabaseSubscriptions("graph").size());
    }, "closeUnsubscribesAll");
  }

  @Test
  public void badCloseIsCleanedUp() throws Throwable {
    execute(() -> {
      {
        var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
        var result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V1")));
        Assertions.assertEquals("ok", result.get("result"));
        client.breakConnection();
      }

      try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        JSONObject result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V1")));
        Assertions.assertEquals("ok", result.get("result"));

        getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();
        var json = getJsonMessageOrFail(client);
        Assertions.assertEquals("create", json.get("changeType"));

        // The sending thread should have detected and removed the zombie connection.
        Thread.sleep(DELAY_MS);
        Assertions.assertEquals(1, getServer(0).getHttpServer().getWebSocketEventBus().getDatabaseSubscriptions("graph").size());
      }

      Thread.sleep(DELAY_MS);
      Assertions.assertTrue(getServer(0).getHttpServer().getWebSocketEventBus().getDatabaseSubscriptions(getDatabaseName()).isEmpty());
    }, "badCloseIsCleanedUp");
  }

  @Test
  public void invalidJsonReturnsError() throws Throwable {
    execute(() -> {
      try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = new JSONObject(client.send("42"));
        Assertions.assertEquals("error", result.get("result"));
        Assertions.assertEquals("org.json.JSONException", result.get("exception"));
      }
    }, "invalidJsonReturnsError");
  }

  @Test
  public void authenticationFailureReturns403() throws Throwable {
    execute(() -> {
      var thrown = Assertions.assertThrows(UpgradeFailedException.class, () -> new WebSocketClientHelper("ws://localhost:2480/ws", "root", "bad"));
      Assertions.assertTrue(thrown.getMessage().contains("403"));
    }, "authenticationFailureReturns403");
  }

  @Test
  public void invalidDatabaseReturnsError() throws Throwable {
    execute(() -> {
      try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = new JSONObject(client.send(buildActionMessage("subscribe", "invalid")));
        Assertions.assertEquals("error", result.get("result"));
        Assertions.assertEquals("com.arcadedb.exception.DatabaseOperationException", result.get("exception"));
      }
    }, "invalidDatabaseReturnsError");
  }

  @Test
  public void unsubscribeWithoutSubscribeDoesNothing() throws Throwable {
    execute(() -> {
      try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = new JSONObject(client.send(buildActionMessage("unsubscribe", "graph")));
        Assertions.assertEquals("ok", result.get("result"));
      }
    }, "unsubscribeWithoutSubscribeDoesNothing");
  }

  @Test
  public void invalidActionReturnsError() throws Throwable {
    execute(() -> {
      try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = new JSONObject(client.send(buildActionMessage("invalid", "graph")));
        Assertions.assertEquals("error", result.get("result"));
        Assertions.assertEquals("invalid is not a valid action.", result.get("detail"));
      }
    }, "invalidActionReturnsError");
  }

  @Test
  public void missingActionReturnsError() throws Throwable {
    execute(() -> {
      try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = new JSONObject(client.send("{\"database\": \"graph\"}"));
        Assertions.assertEquals("error", result.get("result"));
        Assertions.assertEquals("Property 'action' is required.", result.get("detail"));
      }
    }, "missingActionReturnsError");
  }

  @Test
  public void subscribeDatabaseWorks() throws Throwable {
    execute(() -> {
      try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = client.send(buildActionMessage("subscribe", "graph"));
        Assertions.assertEquals("ok", new JSONObject(result).get("result"));

        final MutableVertex v = getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

        var json = getJsonMessageOrFail(client);
        Assertions.assertEquals("create", json.get("changeType"));
        var record = json.getJSONObject("record");
        Assertions.assertEquals("test", record.get("name"));
        Assertions.assertEquals("V1", record.get("@type"));
      }
    }, "subscribeDatabaseWorks");
  }

  @Test
  public void twoSubscribersAreServiced() throws Throwable {
    execute(() -> {
      var clients = new WebSocketClientHelper[] { new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS),
          new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS) };

      for (var client : clients) {
        var result = client.send(buildActionMessage("subscribe", "graph"));
        Assertions.assertEquals("ok", new JSONObject(result).get("result"));
      }

      getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

      for (var client : clients) {
        var json = getJsonMessageOrFail(client);
        Assertions.assertEquals("create", json.get("changeType"));
        var record = json.getJSONObject("record");
        Assertions.assertEquals("test", record.get("name"));
        Assertions.assertEquals("V1", record.get("@type"));

        client.close();
      }
    }, "twoSubscribersAreServiced");
  }

  @Test
  public void subscribeTypeWorks() throws Throwable {
    execute(() -> {
      try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = client.send(buildActionMessage("subscribe", "graph", "V1"));
        Assertions.assertEquals("ok", new JSONObject(result).get("result"));

        getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

        var json = getJsonMessageOrFail(client);
        Assertions.assertEquals("create", json.get("changeType"));
        var record = json.getJSONObject("record");
        Assertions.assertEquals("test", record.get("name"));
        Assertions.assertEquals("V1", record.get("@type"));
      }
    }, "subscribeTypeWorks");
  }

  @Test
  public void subscribeChangeTypeWorks() throws Throwable {
    execute(() -> {
      try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = client.send(buildActionMessage("subscribe", "graph", null, new String[] { "create" }));
        Assertions.assertEquals("ok", new JSONObject(result).get("result"));

        getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

        var json = getJsonMessageOrFail(client);
        Assertions.assertEquals("create", json.get("changeType"));
        var record = json.getJSONObject("record");
        Assertions.assertEquals("test", record.get("name"));
        Assertions.assertEquals("V1", record.get("@type"));
      }
    }, "subscribeChangeTypeWorks");
  }

  @Test
  public void subscribeMultipleChangeTypesWorks() throws Throwable {
    execute(() -> {
      try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = client.send(buildActionMessage("subscribe", "graph", null, new String[] { "create", "update", "delete" }));
        Assertions.assertEquals("ok", new JSONObject(result).get("result"));

        var v1 = getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

        var json = getJsonMessageOrFail(client);
        Assertions.assertEquals("create", json.get("changeType"));
        var record = json.getJSONObject("record");
        Assertions.assertEquals("test", record.get("name"));
        Assertions.assertEquals("V1", record.get("@type"));

        v1.set("updated", true).save();

        json = getJsonMessageOrFail(client);
        Assertions.assertEquals("update", json.get("changeType"));
        record = json.getJSONObject("record");
        Assertions.assertEquals(v1.getIdentity().toString(), record.get("@rid"));
        Assertions.assertTrue(record.getBoolean("updated"));

        v1.delete();

        json = getJsonMessageOrFail(client);
        Assertions.assertEquals("delete", json.get("changeType"));
        record = json.getJSONObject("record");
        Assertions.assertEquals("test", record.get("name"));
        Assertions.assertEquals("V1", record.get("@type"));
      }
    }, "subscribeMultipleChangeTypesWorks");
  }

  @Test
  public void subscribeChangeTypeDoesNotPushOtherChangeTypes() throws Throwable {
    execute(() -> {
      try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = client.send(buildActionMessage("subscribe", "graph", null, new String[] { "update" }));
        Assertions.assertEquals("ok", new JSONObject(result).get("result"));

        getServerDatabase(0, "graph").newVertex("V2").save();

        Assertions.assertNull(client.popMessage(500));
      }
    }, "subscribeChangeTypeDoesNotPushOtherChangeTypes");
  }

  @Test
  public void subscribeTypeDoesNotPushOtherTypes() throws Throwable {
    execute(() -> {
      try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = client.send(buildActionMessage("subscribe", "graph", "V1"));
        Assertions.assertEquals("ok", new JSONObject(result).get("result"));

        getServerDatabase(0, "graph").newVertex("V2").save();

        Assertions.assertNull(client.popMessage(500));
      }
    }, "subscribeTypeDoesNotPushOtherTypes");
  }

  @Test
  public void unsubscribeDatabaseWorks() throws Throwable {
    execute(() -> {
      try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        var result = client.send(buildActionMessage("subscribe", "graph"));
        Assertions.assertEquals("ok", new JSONObject(result).get("result"));

        result = client.send(buildActionMessage("unsubscribe", "graph"));
        Assertions.assertEquals("ok", new JSONObject(result).get("result"));

        getServerDatabase(0, "graph").newVertex("V1").save();

        Assertions.assertNull(client.popMessage(500));
      }
    }, "unsubscribeDatabaseWorks");
  }

  private static String buildActionMessage(String action, String database) {
    return buildActionMessage(action, database, null, null);
  }

  private static String buildActionMessage(String action, String database, String type) {
    return buildActionMessage(action, database, type, null);
  }

  private static JSONObject getJsonMessageOrFail(WebSocketClientHelper client) {
    var message = client.popMessage();
    Assertions.assertNotNull(message, "No message received from the server.");
    return new JSONObject(message);
  }

  private static String buildActionMessage(String action, String database, String type, String[] changeTypes) {
    var obj = new JSONObject();
    obj.put("action", action);
    obj.putOpt("database", database);
    obj.putOpt("type", type);
    obj.putOpt("changeTypes", changeTypes);
    return obj.toString();
  }

  private void execute(final CallableNoReturn callback, final String testName) throws Throwable {
    LogManager.instance().log(this, Level.FINE, "BEGIN " + testName);
    try {
      callback.call();
    } catch (Throwable e) {
      LogManager.instance().log(this, Level.SEVERE, "ERROR in " + testName, e);
      throw e;
    } finally {
      LogManager.instance().log(this, Level.FINE, "END " + testName);
    }
  }
}
