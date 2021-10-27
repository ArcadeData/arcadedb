package com.arcadedb.server.ws;

import com.arcadedb.server.BaseGraphServerTest;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class WebSocketEventBusIT extends BaseGraphServerTest {

  @Test
  public void subscribeDatabaseWorks() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = client.send(buildActionMessage("subscribe", "graph"));
    var json = new JSONObject(result.get(1, TimeUnit.SECONDS));
    Assertions.assertEquals("ok", json.get("result"));

    result = client.get();
    var db = this.getServerDatabase(0, "graph");
    var v1 = db.newVertex("V1").set("name", "test");
    v1.save();

    json = new JSONObject(result.get(1, TimeUnit.SECONDS));
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
    var json = new JSONObject(result.get(1, TimeUnit.SECONDS));
    Assertions.assertEquals("ok", json.get("result"));

    result = client.get();
    var db = this.getServerDatabase(0, "graph");
    var v1 = db.newVertex("V1").set("name", "test");
    v1.save();

    json = new JSONObject(result.get(1, TimeUnit.SECONDS));
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
    var json = new JSONObject(result.get(1, TimeUnit.SECONDS));
    Assertions.assertEquals("ok", json.get("result"));

    result = client.get();
    var db = this.getServerDatabase(0, "graph");
    var v1 = db.newVertex("V1").set("name", "test");
    v1.save();

    json = new JSONObject(result.get(1, TimeUnit.SECONDS));
    Assertions.assertEquals("create", json.get("changeType"));
    var record = json.getJSONObject("record");
    Assertions.assertEquals("test", record.get("name"));
    Assertions.assertEquals("V1", record.get("@type"));

    client.close();
  }

  @Test
  public void subscribeChangeTypeDoesNotPushOtherChangeTypes() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = client.send(buildActionMessage("subscribe", "graph", null, new String[]{"update"}));
    var json = new JSONObject(result.get(1, TimeUnit.SECONDS));
    Assertions.assertEquals("ok", json.get("result"));

    result = client.get();
    var db = this.getServerDatabase(0, "graph");
    var v2 = db.newVertex("V2").set("name", "test");
    v2.save();

    try {
      result.get(100, TimeUnit.MILLISECONDS);
    } catch (TimeoutException ignored) {
    }

    Assertions.assertFalse(result.isDone());

    client.close();
  }

  @Test
  public void subscribeTypeDoesNotPushOtherTypes() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = client.send(buildActionMessage("subscribe", "graph", "V1"));
    var json = new JSONObject(result.get(1, TimeUnit.SECONDS));
    Assertions.assertEquals("ok", json.get("result"));

    result = client.get();
    var db = this.getServerDatabase(0, "graph");
    var v2 = db.newVertex("V2").set("name", "test");
    v2.save();

    try {
      result.get(100, TimeUnit.MILLISECONDS);
    } catch (TimeoutException ignored) {
    }

    Assertions.assertFalse(result.isDone());

    client.close();
  }

  @Test
  public void unsubscribeDatabaseWorks() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = client.send(buildActionMessage("subscribe", "graph"));
    var json = new JSONObject(result.get(1, TimeUnit.SECONDS));
    Assertions.assertEquals("ok", json.get("result"));

    result = client.send(buildActionMessage("unsubscribe", "graph"));
    json = new JSONObject(result.get(1, TimeUnit.SECONDS));
    Assertions.assertEquals("ok", json.get("result"));

    result = client.get();
    var db = this.getServerDatabase(0, "graph");
    var v1 = db.newVertex("V1").set("name", "test");
    v1.save();

    try {
      result.get(100, TimeUnit.MILLISECONDS);
    } catch (TimeoutException ignored) {
    }

    Assertions.assertFalse(result.isDone());

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
