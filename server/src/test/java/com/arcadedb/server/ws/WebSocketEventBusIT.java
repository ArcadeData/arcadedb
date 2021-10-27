package com.arcadedb.server.ws;

import com.arcadedb.server.BaseGraphServerTest;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WebSocketEventBusIT extends BaseGraphServerTest {

  @Test
  public void invalidJsonReturnsError() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = new JSONObject(client.send("42"));
    Assertions.assertEquals("error", result.get("result"));
    Assertions.assertEquals("org.json.JSONException", result.get("exception"));
  }

  @Test
  public void invalidDatabaseReturnsError() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = new JSONObject(client.send(buildActionMessage("subscribe", "invalid")));
    Assertions.assertEquals("error", result.get("result"));
    Assertions.assertEquals("com.arcadedb.exception.DatabaseOperationException", result.get("exception"));
  }

  @Test
  public void unsubscribeWithoutSubscribeDoesNothing() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = new JSONObject(client.send(buildActionMessage("unsubscribe", "graph")));
    Assertions.assertEquals("ok", result.get("result"));
  }

  @Test
  public void invalidActionReturnsError() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = new JSONObject(client.send(buildActionMessage("invalid", "graph")));
    Assertions.assertEquals("error", result.get("result"));
    Assertions.assertEquals("invalid is not a valid action.", result.get("detail"));
  }

  @Test
  public void missingActionReturnsError() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = new JSONObject(client.send("{\"database\": \"graph\"}"));
    Assertions.assertEquals("error", result.get("result"));
    Assertions.assertEquals("Property 'action' is required.", result.get("detail"));
  }

  @Test
  public void subscribeDatabaseWorks() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = client.send(buildActionMessage("subscribe", "graph"));
    Assertions.assertEquals("ok", new JSONObject(result).get("result"));

    this.getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

    var json = new JSONObject(client.popMessage());
    Assertions.assertEquals("create", json.get("changeType"));
    var record = json.getJSONObject("record");
    Assertions.assertEquals("test", record.get("name"));
    Assertions.assertEquals("V1", record.get("@type"));

    client.close();
  }

  @Test
  public void subscribeTypeWorks() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = client.send(buildActionMessage("subscribe", "graph", "V1"));
    Assertions.assertEquals("ok", new JSONObject(result).get("result"));

    this.getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

    var json = new JSONObject(client.popMessage());
    Assertions.assertEquals("create", json.get("changeType"));
    var record = json.getJSONObject("record");
    Assertions.assertEquals("test", record.get("name"));
    Assertions.assertEquals("V1", record.get("@type"));

    client.close();
  }

  @Test
  public void subscribeChangeTypeWorks() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = client.send(buildActionMessage("subscribe", "graph", null, new String[]{"create"}));
    Assertions.assertEquals("ok", new JSONObject(result).get("result"));

    this.getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

    var json = new JSONObject(client.popMessage());
    Assertions.assertEquals("create", json.get("changeType"));
    var record = json.getJSONObject("record");
    Assertions.assertEquals("test", record.get("name"));
    Assertions.assertEquals("V1", record.get("@type"));

    client.close();
  }

  @Test
  public void subscribeMultipleChangeTypesWorks() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = client.send(buildActionMessage("subscribe", "graph", null, new String[]{"create", "delete"}));
    Assertions.assertEquals("ok", new JSONObject(result).get("result"));

    var v1 = this.getServerDatabase(0, "graph").newVertex("V1").set("name", "test").save();

    var json = new JSONObject(client.popMessage());
    Assertions.assertEquals("create", json.get("changeType"));
    var record = json.getJSONObject("record");
    Assertions.assertEquals("test", record.get("name"));
    Assertions.assertEquals("V1", record.get("@type"));

    v1.delete();

    json = new JSONObject(client.popMessage());
    Assertions.assertEquals("delete", json.get("changeType"));
    record = json.getJSONObject("record");
    Assertions.assertEquals("test", record.get("name"));
    Assertions.assertEquals("V1", record.get("@type"));

    client.close();
  }

  @Test
  public void subscribeChangeTypeDoesNotPushOtherChangeTypes() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = client.send(buildActionMessage("subscribe", "graph", null, new String[]{"update"}));
    Assertions.assertEquals("ok", new JSONObject(result).get("result"));

    this.getServerDatabase(0, "graph").newVertex("V2").save();

    Assertions.assertNull(client.popMessage());

    client.close();
  }

  @Test
  public void subscribeTypeDoesNotPushOtherTypes() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = client.send(buildActionMessage("subscribe", "graph", "V1"));
    Assertions.assertEquals("ok", new JSONObject(result).get("result"));

    this.getServerDatabase(0, "graph").newVertex("V2").save();

    Assertions.assertNull(client.popMessage());

    client.close();
  }

  @Test
  public void unsubscribeDatabaseWorks() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = client.send(buildActionMessage("subscribe", "graph"));
    Assertions.assertEquals("ok", new JSONObject(result).get("result"));

    result = client.send(buildActionMessage("unsubscribe", "graph"));
    Assertions.assertEquals("ok", new JSONObject(result).get("result"));

    this.getServerDatabase(0, "graph").newVertex("V1").save();

    Assertions.assertNull(client.popMessage());

    client.close();
  }

  private static String buildActionMessage(String action, String database) {
    return buildActionMessage(action, database, null, null);
  }

  private static String buildActionMessage(String action, String database, String type) {
    return buildActionMessage(action, database, type, null);
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
