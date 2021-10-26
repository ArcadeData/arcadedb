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

    var result = client.send(buildActionMessage("subscribe", "graph", null));
    var json = new JSONObject(result.get(1, TimeUnit.SECONDS));
    Assertions.assertEquals("ok", json.get("result"));

    result = client.get();
    var db = this.getServerDatabase(0, "graph");
    db.execute("sql", "INSERT INTO V1 content {name: 'Test'};");
    json = new JSONObject(result.get(1, TimeUnit.SECONDS));
    Assertions.assertEquals("create", json.get("changeType"));

    client.close();
  }

  @Test
  public void unsubscribeDatabaseWorks() throws Exception {
    var client = new WebSocketClientHelper("ws://localhost:2480/ws", "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    var result = client.send(buildActionMessage("subscribe", "graph", null));
    var json = new JSONObject(result.get(1, TimeUnit.SECONDS));
    Assertions.assertEquals("ok", json.get("result"));

    result = client.send(buildActionMessage("unsubscribe", "graph", null));
    json = new JSONObject(result.get(1, TimeUnit.SECONDS));
    Assertions.assertEquals("ok", json.get("result"));

    result = client.get();
    var db = this.getServerDatabase(0, "graph");
    db.execute("sql", "INSERT INTO V1 content {name: 'Test'};");

    try {
      result.get(100, TimeUnit.MILLISECONDS);
    } catch (TimeoutException ignored) {
    }

    Assertions.assertFalse(result.isDone());

    client.close();
  }

  private static String buildActionMessage(String action, String database, String type) {
    var obj = new JSONObject();
    obj.put("action", action);
    if (database != null) obj.put("database", database);
    if (type != null) obj.put("type", type);
    return obj.toString();
  }
}
