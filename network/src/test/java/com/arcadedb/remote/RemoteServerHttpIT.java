package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RemoteServerHttpIT {
  class MockRemoteServer extends RemoteServer {
    MockRemoteServer() {
      super("localhost", 1234, "user", "password", new ContextConfiguration());
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

    RemoteServer server = spy(new MockRemoteServer());
    doNothing().when(server).setRequestPayload(any(), any());
    doReturn(connection).when(server).createConnection(any(), any());

    server.create("testdb");
    verify(server).httpCommand("POST", null, "server", null, "create database testdb", null, true, true, null);
    verify(server).createConnection("POST", "http://localhost:1234/api/v1/server");
    JSONObject payload = new JSONObject("{\"command\":\"create database testdb\",\"serializer\":\"record\"}");
    verify(server).setRequestPayload(connection, payload);
  }

  @Test
  void testExists() throws Exception {
    HttpURLConnection connection = mock(HttpURLConnection.class);
    doNothing().when(connection).connect();
    when(connection.getResponseCode()).thenReturn(200);
    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream("{\"result\": true}".getBytes()));

    RemoteServer server = spy(new MockRemoteServer());
    doReturn(connection).when(server).createConnection(any(), any());

    server.exists("testdb");
    verify(server).httpCommand(eq("GET"), eq("testdb"), eq("exists"), eq("SQL"), eq(null), eq(null), eq(false), eq(true), any());
    verify(server).createConnection("GET", "http://localhost:1234/api/v1/exists/testdb");
    verify(server, never()).setRequestPayload(any(), any());
  }

  @Test
  void testDrop() throws Exception {
    HttpURLConnection connection = mock(HttpURLConnection.class);
    doNothing().when(connection).connect();
    when(connection.getResponseCode()).thenReturn(200);

    RemoteServer server = spy(new MockRemoteServer());
    doNothing().when(server).setRequestPayload(any(), any());
    doReturn(connection).when(server).createConnection(any(), any());

    server.drop("testdb");
    verify(server).createConnection("POST", "http://localhost:1234/api/v1/server");
    JSONObject payload = new JSONObject("{\"command\":\"drop database testdb\"}");
    verify(server).setRequestPayload(connection, payload);
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

    RemoteServer server = spy(new MockRemoteServer());
    doReturn(connection).when(server).createConnection(any(), any());

    server.databases();
    verify(server).httpCommand(eq("POST"), eq(null), eq("server"), eq(null), eq("list databases"), eq(null), eq(true), eq(true),
        any());
    verify(server).createConnection("POST", "http://localhost:1234/api/v1/server");
    JSONObject payload = new JSONObject("{\"command\":\"list databases\",\"serializer\":\"record\"}");
    verify(server).setRequestPayload(connection, payload);
    byte[] payloadAsByteArray = payload.toString().getBytes(StandardCharsets.UTF_8);
    verify(outputStream).write(payloadAsByteArray, 0, payloadAsByteArray.length);
  }
}
