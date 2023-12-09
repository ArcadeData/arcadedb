package com.arcadedb.server;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

public class HTTPGraphIT extends BaseGraphServerTest {
  @Test
  public void checkAuthenticationError() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:wrong".getBytes()));
      try {
        connection.connect();
        readResponse(connection);
        Assertions.fail("Authentication was bypassed!");
      } catch (final IOException e) {
        Assertions.assertTrue(e.toString().contains("403"));
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkNoAuthentication() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      try {
        connection.connect();
        readResponse(connection);
        Assertions.fail("Authentication was bypassed!");
      } catch (final IOException e) {
        Assertions.assertTrue(e.toString().contains("401"));
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkQueryInGet() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertTrue(response.contains("V1"));

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkQueryInPost() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "select from V1 limit 1", null, new HashMap<>());
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertTrue(response.contains("V1"));
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkCommand() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "select from V1 limit 1", null, new HashMap<>());
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertTrue(response.contains("V1"));
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkCommandLoadByRIDWithParameters() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "SELECT FROM :rid", null, Collections.singletonMap("rid", "#1:0"));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertTrue(response.contains("V1"));
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkCommandLoadByRIDInWhereWithParameters() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "SELECT FROM " + VERTEX1_TYPE_NAME + " where @rid = :rid", null,
          Collections.singletonMap("rid", "#1:0"));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertTrue(response.contains("V1"));
      } finally {
        connection.disconnect();
      }
    });
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/discussions/468
   */
  @Test
  public void checkCommandLoadByRIDIn() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "SELECT FROM " + VERTEX1_TYPE_NAME + " where @rid in (#1:0)", null, Collections.emptyMap());
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertTrue(response.contains("V1"));
      } finally {
        connection.disconnect();
      }
    });
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/discussions/468
   */
  @Test
  public void checkCommandLet() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql",
          "SELECT $p from " + VERTEX1_TYPE_NAME + " let pid = @rid, p = (select from " + VERTEX1_TYPE_NAME
              + " where @rid = $parent.pid)", null, Collections.emptyMap());
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertTrue(response.contains("#1:0"), response);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkCommandNoDuplication() throws Exception {
    testEachServer((serverIndex) -> {
      final JSONObject responseAsJson = executeCommand(serverIndex, "sql", "SELECT FROM E1");

      final List<Object> vertices = responseAsJson.getJSONObject("result").getJSONArray("vertices").toList();
      Assertions.assertEquals(2, vertices.size());
      for (final Object o : vertices)
        Assertions.assertTrue(((Map) o).get("t").equals("V1") || ((Map) o).get("t").equals("V2"));

      final List<Object> records = responseAsJson.getJSONObject("result").getJSONArray("records").toList();
      Assertions.assertEquals(1, records.size());
      for (final Object o : records)
        Assertions.assertTrue(
            ((Map) o).get("@type").equals("V1") || ((Map) o).get("@type").equals("V2") || ((Map) o).get("@type").equals("E1"));

      final List<Object> edges = responseAsJson.getJSONObject("result").getJSONArray("edges").toList();
      Assertions.assertEquals(1, edges.size());
      for (final Object o : edges)
        Assertions.assertTrue(((Map) o).get("t").equals("E1"));
    });
  }

  @Test
  public void checkDatabaseExists() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/exists/graph/").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertTrue(new JSONObject(response).getBoolean("result"));
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkDatabaseList() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/databases").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        final JSONArray databases = new JSONObject(response).getJSONArray("result");
        Assertions.assertEquals(1, databases.length(), "Found the following databases: " + databases);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void createAndDropDatabase() throws Exception {
    testEachServer((serverIndex) -> {
      // CREATE THE DATABASE 'JUSTFORFUN'
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, new JSONObject().put("command", "create database justforfun"));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertEquals("ok", new JSONObject(response).getString("result"));

      } finally {
        connection.disconnect();
      }

      // CHECK EXISTENCE
      connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/exists/justforfun").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertTrue(new JSONObject(response).getBoolean("result"));

      } finally {
        connection.disconnect();
      }

      // DROP DATABASE
      connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, new JSONObject().put("command", "drop database justforfun"));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertEquals("ok", new JSONObject(response).getString("result"));

      } finally {
        connection.disconnect();
      }

      // CHECK NOT EXISTENCE
      connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/exists/justforfun").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertFalse(new JSONObject(response).getBoolean("result"));

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void closeAndReopenDatabase() throws Exception {
    testEachServer((serverIndex) -> {
      // CREATE THE DATABASE 'JUSTFORFUN'
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, new JSONObject().put("command", "create database closeAndReopen"));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertEquals("ok", new JSONObject(response).getString("result"));

      } finally {
        connection.disconnect();
      }

      // CLOSE DATABASE
      connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, new JSONObject().put("command", "close database closeAndReopen"));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertEquals("ok", new JSONObject(response).getString("result"));

      } finally {
        connection.disconnect();
      }

      // RE-OPEN DATABASE
      connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, new JSONObject().put("command", "open database closeAndReopen"));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertEquals("ok", new JSONObject(response).getString("result"));

      } finally {
        connection.disconnect();
      }

      // CHECK EXISTENCE
      connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/exists/closeAndReopen").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertTrue(new JSONObject(response).getBoolean("result"));

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void testEmptyDatabaseName() throws Exception {
    testEachServer((serverIndex) -> {
      // CREATE THE DATABASE ''
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, new JSONObject().put("command", "create database "));
      connection.connect();

      try {
        readResponse(connection);
        Assertions.fail("Empty database should be an error");
      } catch (final Exception e) {
        Assertions.assertEquals(400, connection.getResponseCode());

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void testOneEdgePerTx() throws Exception {
    testEachServer((serverIndex) -> {
      executeCommand(serverIndex, "sqlscript", "create vertex type Photos;create vertex type Users;create edge type HasUploaded;");
      executeCommand(serverIndex, "sql", "create vertex Users set id = 'u1111'");

      executeCommand(serverIndex, "sqlscript", //
          "BEGIN;" //
              + "LET photo = CREATE vertex Photos SET id = \"p12345\", name = \"download1.jpg\";" //
              + "LET user = SELECT * FROM Users WHERE id = \"u1111\";" //
              + "LET userEdge = Create edge HasUploaded FROM $user to $photo set type = \"User_Photos\";" //
              + "SLEEP randomInt( 500 );" //
              + "commit retry 30;return $photo;");

      executeCommand(serverIndex, "sqlscript", //
          "BEGIN;" //
              + "LET photo = CREATE vertex Photos SET id = \"p2222\", name = \"download2.jpg\";" //
              + "LET user = SELECT * FROM Users WHERE id = \"u1111\";" //
              + "LET userEdge = Create edge HasUploaded FROM $user to $photo set type = \"User_Photos\";" //
              + "commit retry 30;return $photo;");

      executeCommand(serverIndex, "sqlscript", // //
          "BEGIN;" + "LET photo = CREATE vertex Photos SET id = \"p5555\", name = \"download3.jpg\";" //
              + "LET user = SELECT * FROM Users WHERE id = \"u1111\";" //
              + "LET userEdge = Create edge HasUploaded FROM $user to $photo set type = \"User_Photos\";" //
              + "commit retry 30;return $photo;");

      final JSONObject responseAsJsonSelect = executeCommand(serverIndex, "sql", //
          "SELECT id FROM ( SELECT expand( outE('HasUploaded') ) FROM Users WHERE id = \"u1111\" )");

      Assertions.assertEquals(3, responseAsJsonSelect.getJSONObject("result").getJSONArray("records").length());
    });
  }

  @Test
  public void testOneEdgePerTxMultiThreads() throws Exception {
    testEachServer((serverIndex) -> {
      executeCommand(serverIndex, "sqlscript", "create vertex type Photos;create vertex type Users;create edge type HasUploaded;");

      executeCommand(serverIndex, "sql", "create vertex Users set id = 'u1111'");

      final int THREADS = 4;
      final int SCRIPTS = 100;
      final AtomicInteger atomic = new AtomicInteger();

      final Thread[] threads = new Thread[THREADS];
      for (int i = 0; i < THREADS; i++) {
        threads[i] = new Thread(() -> {
          for (int j = 0; j < SCRIPTS; j++) {
            try {
              final JSONObject responseAsJson = executeCommand(serverIndex, "sqlscript", //
                  "BEGIN ISOLATION REPEATABLE_READ;" //
                      + "LET photo = CREATE vertex Photos SET id = uuid(), name = \"downloadX.jpg\";" //
                      + "LET user = SELECT * FROM Users WHERE id = \"u1111\";" //
                      + "LET userEdge = Create edge HasUploaded FROM $user to $photo set type = \"User_Photos\";" //
                      + "commit retry 100;return $photo;");

              atomic.incrementAndGet();

              if (responseAsJson == null) {
                LogManager.instance().log(this, Level.SEVERE, "Error on execution from thread %d", Thread.currentThread().getId());
                continue;
              }

              Assertions.assertNotNull(responseAsJson.getJSONObject("result").getJSONArray("records"));

            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        });
        threads[i].start();
      }

      for (int i = 0; i < THREADS; i++)
        threads[i].join(60 * 1_000);

      Assertions.assertEquals(THREADS * SCRIPTS, atomic.get());

      final JSONObject responseAsJsonSelect = executeCommand(serverIndex, "sql", //
          "SELECT id FROM ( SELECT expand( outE('HasUploaded') ) FROM Users WHERE id = \"u1111\" )");

      Assertions.assertEquals(THREADS * SCRIPTS, responseAsJsonSelect.getJSONObject("result").getJSONArray("records").length());
    });
  }
}
