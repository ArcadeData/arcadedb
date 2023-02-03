package com.arcadedb.remote;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RemoteDatabaseTest {

    @Test
    void testCreate() throws Exception {
        HttpURLConnection connection = mock(HttpURLConnection.class);
        doNothing().when(connection).connect();
        when(connection.getResponseCode()).thenReturn(200);
        when(connection.getInputStream()).thenReturn(new ByteArrayInputStream("{}".getBytes()));

        RemoteDatabase database = mock(RemoteDatabase.class, CALLS_REAL_METHODS);
        database.setApiVersion(1);
        doNothing().when(database).setRequestPayload(any(), any());
        doReturn(connection).when(database).createConnection(any(), any());
        database.create();
        verify(database).httpCommand("POST", null, "server", null,
                "create database null", null, true, true, null);
        verify(database).createConnection("POST", "http://null:0/api/v1/server");
        JSONObject payload = new JSONObject("{\"command\":\"create database null\",\"serializer\":\"record\"}");
        verify(database).setRequestPayload(connection, payload);
    }

    @Test
    void testExists() throws Exception {
        HttpURLConnection connection = mock(HttpURLConnection.class);
        doNothing().when(connection).connect();
        when(connection.getResponseCode()).thenReturn(200);
        when(connection.getInputStream()).thenReturn(new ByteArrayInputStream("{}".getBytes()));

        RemoteDatabase database = mock(RemoteDatabase.class, CALLS_REAL_METHODS);
        database.setApiVersion(1);
        doNothing().when(database).setRequestPayload(any(), any());
        doReturn(connection).when(database).createConnection(any(), any());
        database.exists();
        verify(database).httpCommand("POST", null, "server", null,
                "create database null", null, true, true, null);
        verify(database).createConnection("POST", "http://null:0/api/v1/server");
        JSONObject payload = new JSONObject("{\"command\":\"create database null\",\"serializer\":\"record\"}");
        verify(database).setRequestPayload(connection, payload);
    }
}