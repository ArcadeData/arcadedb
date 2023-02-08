package com.arcadedb.remote;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.serializer.json.JSONObject;

@ExtendWith(MockitoExtension.class)
class RemoteDatabaseTest {
    class MockRemoteDatabase extends RemoteDatabase {
        MockRemoteDatabase() {
            super("localhost", 1234, "testdb", "user", "password",
                    new ContextConfiguration());
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
        verify(database).httpCommand("POST", null, "server", null,
                "create database testdb", null, true, true, null);
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
        verify(database).httpCommand(eq("GET"), eq("testdb"), eq("exists"), eq("SQL"),
                eq(null), eq(null), eq(false), eq(true), any());
        verify(database).createConnection("GET", "http://localhost:1234/api/v1/exists/testdb");
        verify(database, never()).setRequestPayload(any(), any());
    }
}
