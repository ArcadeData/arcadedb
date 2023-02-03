package com.arcadedb.remote;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedConstruction;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        when(connection.getInputStream()).thenReturn(new ByteArrayInputStream("{\"result\": true}".getBytes()));

        try (MockedConstruction<RemoteDatabase> ignored = mockConstruction(RemoteDatabase.class,
                withSettings()
                        .useConstructor("localhost", 1234, "testdb", "user", "password")
                        .defaultAnswer(CALLS_REAL_METHODS),
                (mock, context) -> {
                    doNothing().when(mock).setRequestPayload(any(), any());
                    doReturn(connection).when(mock).createConnection(any(), any());
                    doReturn(new ArrayList<>()).when(mock).getReplicaServerList();
                })) {
            RemoteDatabase database = new RemoteDatabase("localhost", 1234, "testdb", "user", "password");
            assertEquals(1, ignored.constructed().size());
            database.setApiVersion(1);
            database.exists();
            verify(database).httpCommand(eq("GET"), eq(null), eq("exists"), eq("SQL"),
                    eq(null), eq(null), eq(false), eq(true), any());
            verify(database).createConnection("GET", "http://null:0/api/v1/exists");
            verify(database, never()).setRequestPayload(any(), any());
        }
    }
}
