package com.arcadedb.server.ws;

import com.arcadedb.log.LogManager;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.StaticBaseServerTest;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.xnio.http.UpgradeFailedException;

import java.util.logging.*;

public class WebSocketEventBusIT extends StaticBaseServerTest {
  private static final int DELAY_MS = 1000;

  @Test
  public void closeUnsubscribesAll() throws Exception {
    LogManager.instance().log(this, Level.INFO, "BEGIN closeUnsubscribesAll");

    try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      var result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V1")));
      Assertions.assertEquals("ok", result.get("result"));
      result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V2")));
      Assertions.assertEquals("ok", result.get("result"));
    }
    Thread.sleep(DELAY_MS);
    Assertions.assertEquals(0, this.getServer(0).getHttpServer().getWebSocketEventBus().getDatabaseSubscriptions("graph").size());

    LogManager.instance().log(this, Level.INFO, "END closeUnsubscribesAll");
  }

  @Test
  public void badCloseIsCleanedUp() throws Exception {
    LogManager.instance().log(this, Level.INFO, "BEGIN badCloseIsCleanedUp");

    {
      var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      var result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V1")));
      Assertions.assertEquals("ok", result.get("result"));
      client.breakConnection();
    }

    try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      JSONObject result = new JSONObject(client.send(buildActionMessage("subscribe", "graph", "V1")));
      Assertions.assertEquals("ok", result.get("result"));

      this.getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();
      var json = getJsonMessageOrFail(client);
      Assertions.assertEquals("create", json.get("changeType"));

      // The sending thread should have detected and removed the zombie connection.
      Thread.sleep(DELAY_MS);
      Assertions.assertEquals(1, this.getServer(0).getHttpServer().getWebSocketEventBus().getDatabaseSubscriptions("graph").size());
    }

    Thread.sleep(DELAY_MS);
    Assertions.assertTrue(getServer(0).getHttpServer().getWebSocketEventBus().getDatabaseSubscriptions(getDatabaseName()).isEmpty());

    LogManager.instance().log(this, Level.INFO, "END badCloseIsCleanedUp");
  }

  @Test
  public void invalidJsonReturnsError() throws Exception {
    LogManager.instance().log(this, Level.INFO, "BEGIN invalidJsonReturnsError");

    try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      var result = new JSONObject(client.send("42"));
      Assertions.assertEquals("error", result.get("result"));
      Assertions.assertEquals("org.json.JSONException", result.get("exception"));
    }

    LogManager.instance().log(this, Level.INFO, "END invalidJsonReturnsError");
  }

  @Test
  public void authenticationFailureReturns403() {
    LogManager.instance().log(this, Level.INFO, "BEGIN authenticationFailureReturns403");

    var thrown = Assertions.assertThrows(UpgradeFailedException.class, () -> new WebSocketClientHelper("ws://localhost:2480/ws", "root", "bad"));
    Assertions.assertTrue(thrown.getMessage().contains("403"));

    LogManager.instance().log(this, Level.INFO, "END authenticationFailureReturns403");
  }

  @Test
  public void invalidDatabaseReturnsError() throws Exception {
    LogManager.instance().log(this, Level.INFO, "BEGIN invalidDatabaseReturnsError");

    try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      var result = new JSONObject(client.send(buildActionMessage("subscribe", "invalid")));
      Assertions.assertEquals("error", result.get("result"));
      Assertions.assertEquals("com.arcadedb.exception.DatabaseOperationException", result.get("exception"));
    }

    LogManager.instance().log(this, Level.INFO, "END invalidDatabaseReturnsError");
  }

  @Test
  public void unsubscribeWithoutSubscribeDoesNothing() throws Exception {
    LogManager.instance().log(this, Level.INFO, "BEGIN unsubscribeWithoutSubscribeDoesNothing");

    try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      var result = new JSONObject(client.send(buildActionMessage("unsubscribe", "graph")));
      Assertions.assertEquals("ok", result.get("result"));
    }

    LogManager.instance().log(this, Level.INFO, "END unsubscribeWithoutSubscribeDoesNothing");
  }

  @Test
  public void invalidActionReturnsError() throws Exception {
    LogManager.instance().log(this, Level.INFO, "BEGIN invalidActionReturnsError");

    try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      var result = new JSONObject(client.send(buildActionMessage("invalid", "graph")));
      Assertions.assertEquals("error", result.get("result"));
      Assertions.assertEquals("invalid is not a valid action.", result.get("detail"));
    }

    LogManager.instance().log(this, Level.INFO, "BEGIN invalidActionReturnsError");
  }

  @Test
  public void missingActionReturnsError() throws Exception {
    LogManager.instance().log(this, Level.INFO, "BEGIN missingActionReturnsError");

    try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      var result = new JSONObject(client.send("{\"database\": \"graph\"}"));
      Assertions.assertEquals("error", result.get("result"));
      Assertions.assertEquals("Property 'action' is required.", result.get("detail"));
    }

    LogManager.instance().log(this, Level.INFO, "END missingActionReturnsError");
  }

  @Test
  public void subscribeDatabaseWorks() throws Exception {
    LogManager.instance().log(this, Level.INFO, "BEGIN subscribeDatabaseWorks");

    try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      var result = client.send(buildActionMessage("subscribe", "graph"));
      Assertions.assertEquals("ok", new JSONObject(result).get("result"));

      this.getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

      var json = getJsonMessageOrFail(client);
      Assertions.assertEquals("create", json.get("changeType"));
      var record = json.getJSONObject("record");
      Assertions.assertEquals("test", record.get("name"));
      Assertions.assertEquals("V1", record.get("@type"));
    }

    LogManager.instance().log(this, Level.INFO, "END subscribeDatabaseWorks");
  }

  @Test
  public void twoSubscribersAreServiced() throws Exception {
    LogManager.instance().log(this, Level.INFO, "BEGIN twoSubscribersAreServiced");

    var clients = new WebSocketClientHelper[] { new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS),
        new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS) };

    for (var client : clients) {
      var result = client.send(buildActionMessage("subscribe", "graph"));
      Assertions.assertEquals("ok", new JSONObject(result).get("result"));
    }

    this.getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

    for (var client : clients) {
      var json = getJsonMessageOrFail(client);
      Assertions.assertEquals("create", json.get("changeType"));
      var record = json.getJSONObject("record");
      Assertions.assertEquals("test", record.get("name"));
      Assertions.assertEquals("V1", record.get("@type"));

      client.close();
    }

    LogManager.instance().log(this, Level.INFO, "END twoSubscribersAreServiced");
  }

  @Test
  public void subscribeTypeWorks() throws Exception {
    LogManager.instance().log(this, Level.INFO, "BEGIN subscribeTypeWorks");

    try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      var result = client.send(buildActionMessage("subscribe", "graph", "V1"));
      Assertions.assertEquals("ok", new JSONObject(result).get("result"));

      this.getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

      var json = getJsonMessageOrFail(client);
      Assertions.assertEquals("create", json.get("changeType"));
      var record = json.getJSONObject("record");
      Assertions.assertEquals("test", record.get("name"));
      Assertions.assertEquals("V1", record.get("@type"));
    }

    LogManager.instance().log(this, Level.INFO, "END subscribeTypeWorks");
  }

  @Test
  public void subscribeChangeTypeWorks() throws Exception {
    LogManager.instance().log(this, Level.INFO, "BEGIN subscribeChangeTypeWorks");

    try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      var result = client.send(buildActionMessage("subscribe", "graph", null, new String[] { "create" }));
      Assertions.assertEquals("ok", new JSONObject(result).get("result"));

      this.getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

      var json = getJsonMessageOrFail(client);
      Assertions.assertEquals("create", json.get("changeType"));
      var record = json.getJSONObject("record");
      Assertions.assertEquals("test", record.get("name"));
      Assertions.assertEquals("V1", record.get("@type"));
    }

    LogManager.instance().log(this, Level.INFO, "END subscribeChangeTypeWorks");
  }

  @Test
  public void subscribeMultipleChangeTypesWorks() throws Exception {
    LogManager.instance().log(this, Level.INFO, "BEGIN subscribeMultipleChangeTypesWorks");

    try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      var result = client.send(buildActionMessage("subscribe", "graph", null, new String[] { "create", "delete" }));
      Assertions.assertEquals("ok", new JSONObject(result).get("result"));

      var v1 = this.getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

      var json = getJsonMessageOrFail(client);
      Assertions.assertEquals("create", json.get("changeType"));
      var record = json.getJSONObject("record");
      Assertions.assertEquals("test", record.get("name"));
      Assertions.assertEquals("V1", record.get("@type"));

      v1.delete();

      json = getJsonMessageOrFail(client);
      Assertions.assertEquals("delete", json.get("changeType"));
      record = json.getJSONObject("record");
      Assertions.assertEquals("test", record.get("name"));
      Assertions.assertEquals("V1", record.get("@type"));
    }

    LogManager.instance().log(this, Level.INFO, "END subscribeMultipleChangeTypesWorks");
  }

  @Test
  public void subscribeChangeTypeDoesNotPushOtherChangeTypes() throws Exception {
    LogManager.instance().log(this, Level.INFO, "BEGIN subscribeChangeTypeDoesNotPushOtherChangeTypes");

    try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      var result = client.send(buildActionMessage("subscribe", "graph", null, new String[] { "update" }));
      Assertions.assertEquals("ok", new JSONObject(result).get("result"));

      this.getServerDatabase(0, "graph").newVertex("V2").save();

      Assertions.assertNull(client.popMessage(500));
    }

    LogManager.instance().log(this, Level.INFO, "END subscribeChangeTypeDoesNotPushOtherChangeTypes");
  }

  @Test
  public void subscribeTypeDoesNotPushOtherTypes() throws Exception {
    LogManager.instance().log(this, Level.INFO, "BEGIN subscribeTypeDoesNotPushOtherTypes");

    try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      var result = client.send(buildActionMessage("subscribe", "graph", "V1"));
      Assertions.assertEquals("ok", new JSONObject(result).get("result"));

      this.getServerDatabase(0, "graph").newVertex("V2").save();

      Assertions.assertNull(client.popMessage(500));
    }

    LogManager.instance().log(this, Level.INFO, "END subscribeTypeDoesNotPushOtherTypes");
  }

  @Test
  public void unsubscribeDatabaseWorks() throws Exception {
    LogManager.instance().log(this, Level.INFO, "BEGIN unsubscribeDatabaseWorks");

    try (var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      var result = client.send(buildActionMessage("subscribe", "graph"));
      Assertions.assertEquals("ok", new JSONObject(result).get("result"));

      result = client.send(buildActionMessage("unsubscribe", "graph"));
      Assertions.assertEquals("ok", new JSONObject(result).get("result"));

      this.getServerDatabase(0, "graph").newVertex("V1").save();

      Assertions.assertNull(client.popMessage(500));
    }

    LogManager.instance().log(this, Level.INFO, "END unsubscribeDatabaseWorks");
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
}
