package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class RemoteDatabaseTest {
  class MockRemoteDatabase extends RemoteDatabase {
    MockRemoteDatabase() {
      super("localhost", 1234, "testdb", "user", "password", new ContextConfiguration());
    }

    void requestClusterConfiguration() {
    }
  }

  @Test
  void testCreate() throws Exception {
    HttpURLConnection connection = mock(HttpURLConnection.class);
    doNothing().when(connection).connect();
    when(connection.getResponseCode()).thenReturn(200);
    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream("{}".getBytes()));

    RemoteDatabase database = spy(new MockRemoteDatabase());
    doNothing().when(database).setRequestPayload(any(), any());
    doReturn(connection).when(database).createConnection(any(), any());

    database.create();
    verify(database).httpCommand("POST", null, "server", null, "create database testdb", null, true, true, null);
    verify(database).createConnection("POST", "http://localhost:1234/api/v1/server");
    JSONObject payload = new JSONObject("{\"command\":\"create database testdb\",\"serializer\":\"record\"}");
    verify(database).setRequestPayload(connection, payload);
  }

  @Test
  void testExists() throws Exception {
    HttpURLConnection connection = mock(HttpURLConnection.class);
    doNothing().when(connection).connect();
    when(connection.getResponseCode()).thenReturn(200);
    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream("{\"result\": true}".getBytes()));

    RemoteDatabase database = spy(new MockRemoteDatabase());
    doReturn(connection).when(database).createConnection(any(), any());

    database.exists();
    verify(database).httpCommand(eq("GET"), eq("testdb"), eq("exists"), eq("SQL"), eq(null), eq(null), eq(false), eq(true), any());
    verify(database).createConnection("GET", "http://localhost:1234/api/v1/exists/testdb");
    verify(database, never()).setRequestPayload(any(), any());
  }

  @Test
  void testDrop() throws Exception {
    HttpURLConnection connection = mock(HttpURLConnection.class);
    doNothing().when(connection).connect();
    when(connection.getResponseCode()).thenReturn(200);

    RemoteDatabase database = spy(new MockRemoteDatabase());
    doNothing().when(database).setRequestPayload(any(), any());
    doReturn(connection).when(database).createConnection(any(), any());

    database.drop();
    verify(database).createConnection("POST", "http://localhost:1234/api/v1/server");
    JSONObject payload = new JSONObject("{\"command\":\"drop database testdb\"}");
    verify(database).setRequestPayload(connection, payload);
  }

  @Test
  void testDatabases() throws Exception {
    OutputStream outputStream = mock(OutputStream.class);
    HttpURLConnection connection = mock(HttpURLConnection.class);
    doNothing().when(connection).connect();
    when(connection.getResponseCode()).thenReturn(200);
    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream("{\"result\": []}".getBytes()));
    when(connection.getOutputStream()).thenReturn(outputStream);
    doNothing().when(connection).setDoOutput(anyBoolean());
    doNothing().when(connection).disconnect();

    RemoteDatabase database = spy(new MockRemoteDatabase());
    doReturn(connection).when(database).createConnection(any(), any());

    database.databases();
    verify(database).httpCommand(eq("POST"), eq(null), eq("server"), eq(null), eq("list databases"), eq(null), eq(true), eq(true), any());
    verify(database).createConnection("POST", "http://localhost:1234/api/v1/server");
    JSONObject payload = new JSONObject("{\"command\":\"list databases\",\"serializer\":\"record\"}");
    verify(database).setRequestPayload(connection, payload);
    byte[] payloadAsByteArray = payload.toString().getBytes(StandardCharsets.UTF_8);
    verify(outputStream).write(payloadAsByteArray, 0, payloadAsByteArray.length);
  }

  @Test
  void testBegin() throws Exception {
    HttpURLConnection connection = mock(HttpURLConnection.class);
    doNothing().when(connection).connect();
    when(connection.getResponseCode()).thenReturn(204);
    when(connection.getHeaderField(RemoteDatabase.ARCADEDB_SESSION_ID)).thenReturn("1234");

    RemoteDatabase database = spy(new MockRemoteDatabase());
    doReturn(connection).when(database).createConnection(any(), any());

    database.begin();
    verify(database).createConnection("POST", "http://localhost:1234/api/v1/begin/testdb");
    verify(database, never()).setRequestPayload(any(), any());
    verify(connection).getHeaderField(RemoteDatabase.ARCADEDB_SESSION_ID);
    assertTrue(database.isTransactionActive());
  }

  @Test
  void testCommit() throws Exception {
    HttpURLConnection connection = mock(HttpURLConnection.class);
    doNothing().when(connection).connect();
    when(connection.getResponseCode()).thenReturn(204);

    RemoteDatabase database = spy(new MockRemoteDatabase());
    doReturn(connection).when(database).createConnection(any(), any());
    doReturn("1234").when(database).getSessionId();

    database.commit();
    verify(database).createConnection("POST", "http://localhost:1234/api/v1/commit/testdb");
    verify(database, never()).setRequestPayload(any(), any());
    verify(database).setSessionId(null);
  }

  @Test
  void testRollback() throws Exception {
    HttpURLConnection connection = mock(HttpURLConnection.class);
    doNothing().when(connection).connect();
    when(connection.getResponseCode()).thenReturn(204);

    RemoteDatabase database = spy(new MockRemoteDatabase());
    doReturn(connection).when(database).createConnection(any(), any());
    doReturn("1234").when(database).getSessionId();

    database.rollback();
    verify(database).createConnection("POST", "http://localhost:1234/api/v1/rollback/testdb");
    verify(database, never()).setRequestPayload(any(), any());
    verify(database).setSessionId(null);
  }

  @Test
  void testTransactionNotJoined() throws Exception {
    RemoteDatabase database = spy(new MockRemoteDatabase());
    doNothing().when(database).begin();
    doNothing().when(database).commit();

    boolean createdNewTx = database.transaction(() -> {
    }, false, 1);
    verify(database).begin();
    verify(database).commit();
    assertTrue(createdNewTx);
  }

  @Test
  void testTransactionJoined() throws Exception {
    RemoteDatabase database = spy(new MockRemoteDatabase());
    doReturn(true).when(database).isTransactionActive();

    boolean createdNewTx = database.transaction(() -> {
    }, true, 1);
    verify(database, never()).begin();
    verify(database, never()).commit();
    assertFalse(createdNewTx);
  }

  @Test
  void testTransactionRollback() throws Exception {
    RemoteDatabase database = spy(new MockRemoteDatabase());
    doNothing().when(database).begin();
    doThrow(new RuntimeException()).when(database).commit();

    assertThrows(RuntimeException.class, () -> database.transaction(() -> {
    }, false, 1));
    verify(database).begin();
    verify(database).rollback();
  }

  @Test
  void testCommand() throws Exception {
    OutputStream outputStream = mock(OutputStream.class);
    HttpURLConnection connection = mock(HttpURLConnection.class);
    doNothing().when(connection).connect();
    when(connection.getResponseCode()).thenReturn(200);
    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream("{\"result\": []}".getBytes()));
    when(connection.getOutputStream()).thenReturn(outputStream);
    doNothing().when(connection).setDoOutput(anyBoolean());
    doNothing().when(connection).disconnect();

    RemoteDatabase database = spy(new MockRemoteDatabase());
    doReturn(connection).when(database).createConnection(any(), any());

    String command = "create vertex type Customer";
    database.command("SQL", command);
    verify(database).httpCommand(eq("POST"), eq("testdb"), eq("command"), eq("SQL"), eq(command), eq(null), eq(true), eq(true), any());
    verify(database).createConnection("POST", "http://localhost:1234/api/v1/command/testdb");
    JSONObject payload = new JSONObject("{\"language\":\"SQL\",\"command\":\"" + command + "\",\"serializer\":\"record\"}");
    verify(database).setRequestPayload(connection, payload);
    byte[] payloadAsByteArray = payload.toString().getBytes(StandardCharsets.UTF_8);
    verify(outputStream).write(payloadAsByteArray, 0, payloadAsByteArray.length);
  }

  @Test
  void testCommandWithParameters() throws Exception {
    OutputStream outputStream = mock(OutputStream.class);
    HttpURLConnection connection = mock(HttpURLConnection.class);
    doNothing().when(connection).connect();
    when(connection.getResponseCode()).thenReturn(200);
    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream("{\"result\": []}".getBytes()));
    when(connection.getOutputStream()).thenReturn(outputStream);
    doNothing().when(connection).setDoOutput(anyBoolean());
    doNothing().when(connection).disconnect();

    RemoteDatabase database = spy(new MockRemoteDatabase());
    doReturn(connection).when(database).createConnection(any(), any());

    String command = "insert into Customer(name, surname) values(:name, :surname)";
    Map<String, Object> paramsMap = new HashMap<>();
    paramsMap.put("name", "Jay");
    paramsMap.put("surname", "Miner");
    database.command("SQL", command, paramsMap);
    verify(database).httpCommand(eq("POST"), eq("testdb"), eq("command"), eq("SQL"), eq(command), eq(paramsMap), eq(true), eq(true), any());
    verify(database).createConnection("POST", "http://localhost:1234/api/v1/command/testdb");
    JSONObject payload = new JSONObject(
        "{\"language\":\"SQL\",\"command\":\"" + command + "\",\"serializer\":\"record\",\"params\":" + (new JSONObject(paramsMap)).toString() + "}");
    verify(database).setRequestPayload(connection, payload);
    byte[] payloadAsByteArray = payload.toString().getBytes(StandardCharsets.UTF_8);
    verify(outputStream).write(payloadAsByteArray, 0, payloadAsByteArray.length);
  }

  @Test
  void testQuery() throws Exception {
    OutputStream outputStream = mock(OutputStream.class);
    HttpURLConnection connection = mock(HttpURLConnection.class);
    doNothing().when(connection).connect();
    when(connection.getResponseCode()).thenReturn(200);
    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream("{\"result\": []}".getBytes()));
    when(connection.getOutputStream()).thenReturn(outputStream);
    doNothing().when(connection).setDoOutput(anyBoolean());
    doNothing().when(connection).disconnect();

    RemoteDatabase database = spy(new MockRemoteDatabase());
    doReturn(connection).when(database).createConnection(any(), any());

    String query = "select from Customer where name = :name";
    Map<String, Object> paramsMap = new HashMap<>();
    paramsMap.put("name", "Jay");
    database.command("SQL", query, paramsMap);
    verify(database).httpCommand(eq("POST"), eq("testdb"), eq("command"), eq("SQL"), eq(query), eq(paramsMap), eq(true), eq(true), any());
    verify(database).createConnection("POST", "http://localhost:1234/api/v1/command/testdb");
    JSONObject payload = new JSONObject(
        "{\"language\":\"SQL\",\"command\":\"" + query + "\",\"serializer\":\"record\",\"params\":" + (new JSONObject(paramsMap)).toString() + "}");
    verify(database).setRequestPayload(connection, payload);
    byte[] payloadAsByteArray = payload.toString().getBytes(StandardCharsets.UTF_8);
    verify(outputStream).write(payloadAsByteArray, 0, payloadAsByteArray.length);
  }
}
