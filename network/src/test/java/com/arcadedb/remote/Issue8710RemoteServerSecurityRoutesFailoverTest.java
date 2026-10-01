/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8710: the ten security control-plane calls of {@link RemoteServer} ({@code /server/users},
 * {@code /server/groups}, {@code /server/api-tokens}) sent one request to one server, so they had no election retry
 * and no failover, unlike {@code createUser}/{@code dropUser}/{@code drop} (issue #7796). Each call is driven against
 * a loopback server that answers from a script, one response per request.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8710RemoteServerSecurityRoutesFailoverTest {

  private static final String OK_LIST = "HTTP/1.1 200 OK\r\n" + json(new JSONObject().put("result", List.of()));
  private static final String OK_OBJECT = "HTTP/1.1 200 OK\r\n" + json(new JSONObject().put("result", new JSONObject()));
  private static final String CREATED = "HTTP/1.1 201 Created\r\n" + json(new JSONObject().put("result", new JSONObject().put("token", "AU-x")));
  private static final String OK_EMPTY = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

  private static final String ELECTION_IN_PROGRESS = "HTTP/1.1 503 Service Unavailable\r\n" + json(new JSONObject()
      .put("error", "Election in progress")
      .put("detail", "Election in progress")
      .put("exception", "com.arcadedb.exception.NeedRetryException"));

  private static final String SECURITY_REFUSAL = "HTTP/1.1 403 Forbidden\r\n" + json(new JSONObject()
      .put("error", "Security error")
      .put("detail", "Not allowed")
      .put("exception", "com.arcadedb.server.security.ServerSecurityException"));

  private static final String DROP = "DROP";

  /** One entry point per call, with the request line it must send and the response a healthy server gives. */
  private record EntryPoint(String name, String requestLine, String success, boolean write, Consumer<RemoteServer> call) {
  }

  private static final List<EntryPoint> ENTRY_POINTS = List.of(
      new EntryPoint("listUsers", "GET /api/v1/server/users", OK_LIST, false, RemoteServer::listUsers),
      new EntryPoint("updateUser", "PUT /api/v1/server/users?name=bob", OK_EMPTY, true,
          s -> s.updateUser("bob", "pw", Map.of("db", List.of("admin")))),
      new EntryPoint("updateUserPassword", "PUT /api/v1/server/users?name=bob", OK_EMPTY, true,
          s -> s.updateUserPassword("bob", "pw")),
      new EntryPoint("updateUserGrants", "PUT /api/v1/server/users?name=bob", OK_EMPTY, true,
          s -> s.updateUserGrants("bob", Map.of("db", List.of("admin")))),
      new EntryPoint("listGroups", "GET /api/v1/server/groups", OK_OBJECT, false, RemoteServer::listGroups),
      new EntryPoint("saveGroup", "POST /api/v1/server/groups", OK_EMPTY, true, s -> s.saveGroup("db", "g", new JSONObject())),
      new EntryPoint("deleteGroup", "DELETE /api/v1/server/groups?database=db&name=g", OK_EMPTY, true, s -> s.deleteGroup("db", "g")),
      new EntryPoint("listApiTokens", "GET /api/v1/server/api-tokens", OK_LIST, false, RemoteServer::listApiTokens),
      new EntryPoint("createApiToken", "POST /api/v1/server/api-tokens", CREATED, true,
          s -> s.createApiToken("t", "db", 0L, null)),
      new EntryPoint("deleteApiToken", "DELETE /api/v1/server/api-tokens?token=abc", OK_EMPTY, true, s -> s.deleteApiToken("abc")));

  @Test
  void everyCallSendsItsRouteAndMethod() throws Exception {
    for (final EntryPoint entry : ENTRY_POINTS) {
      try (final ScriptedServer server = new ScriptedServer(entry.success())) {
        final RemoteServer client = client(server.port(), new ContextConfiguration());
        try {
          entry.call().accept(client);
          assertThat(server.requestLines()).as(entry.name()).hasSize(1);
          assertThat(server.requestLines().getFirst()).as(entry.name()).startsWith(entry.requestLine() + " ");
        } finally {
          client.close();
        }
      }
    }
  }

  @Test
  void everyCallWaitsOutAnElection() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_CLIENT_ELECTION_RETRY_DELAY_MS, 10L);

    for (final EntryPoint entry : ENTRY_POINTS) {
      try (final ScriptedServer server = new ScriptedServer(ELECTION_IN_PROGRESS, entry.success())) {
        final RemoteServer client = client(server.port(), cfg);
        try {
          entry.call().accept(client);
          assertThat(server.requestLines()).as(entry.name()).hasSize(2);
        } finally {
          client.close();
        }
      }
    }
  }

  @Test
  void everyCallFailsOverFromAnUnreachableServer() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.NETWORK_SAME_SERVER_ERROR_RETRIES, 2);

    for (final EntryPoint entry : ENTRY_POINTS) {
      final int closedPort;
      try (final ServerSocket probe = new ServerSocket(0)) {
        closedPort = probe.getLocalPort();
      }
      try (final ScriptedServer second = new ScriptedServer(entry.success())) {
        final RemoteServer client = failoverClient(closedPort, second.port(), cfg);
        try {
          entry.call().accept(client);
          assertThat(second.requestLines()).as(entry.name()).hasSize(1);
        } finally {
          client.close();
        }
      }
    }
  }

  /** A write whose response was lost may have been applied, so it is not sent again (issue #8136); a read is. */
  @Test
  void aWriteWhoseResponseWasLostIsNotResentButAReadIs() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.NETWORK_SAME_SERVER_ERROR_RETRIES, 3);

    for (final EntryPoint entry : ENTRY_POINTS) {
      try (final ScriptedServer server = new ScriptedServer(DROP, entry.success())) {
        final RemoteServer client = client(server.port(), cfg);
        client.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
        try {
          if (entry.write()) {
            assertThatThrownBy(() -> entry.call().accept(client)).as(entry.name()).isInstanceOf(RemoteException.class)
                .hasMessageContaining("may already have applied it");
            assertThat(server.requestLines()).as(entry.name()).hasSize(1);
          } else {
            entry.call().accept(client);
            assertThat(server.requestLines()).as(entry.name()).hasSize(2);
          }
        } finally {
          client.close();
        }
      }
    }
  }

  @Test
  void aTypedRefusalReachesTheCallerUnwrapped() throws Exception {
    for (final EntryPoint entry : ENTRY_POINTS) {
      try (final ScriptedServer server = new ScriptedServer(SECURITY_REFUSAL)) {
        final RemoteServer client = client(server.port(), new ContextConfiguration());
        try {
          assertThatThrownBy(() -> entry.call().accept(client)).as(entry.name()).isExactlyInstanceOf(SecurityException.class);
        } finally {
          client.close();
        }
      }
    }
  }

  /** The cleartext guard is applied to the server actually dialled, not only the one first configured. */
  @Test
  void createApiTokenStillRefusesACleartextNonLoopbackServer() {
    final RemoteServer client = new RemoteServer("192.0.2.1", 2480, "root", "test", new ContextConfiguration()) {
      @Override
      void requestClusterConfiguration() {
        // no cluster
      }
    };
    try {
      assertThatThrownBy(() -> client.createApiToken("t", "db", 0L, null)).isInstanceOf(SecurityException.class)
          .hasMessageContaining("cleartext");
    } finally {
      client.close();
    }
  }

  private static String json(final JSONObject body) {
    final byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
    return "Content-Type: application/json\r\nContent-Length: " + bytes.length + "\r\nConnection: close\r\n\r\n" + body;
  }

  private static RemoteServer client(final int port, final ContextConfiguration cfg) {
    return new RemoteServer("127.0.0.1", port, "root", "test", cfg) {
      @Override
      void requestClusterConfiguration() {
        // no cluster behind the scripted server
      }
    };
  }

  private static RemoteServer failoverClient(final int firstPort, final int secondPort, final ContextConfiguration cfg) {
    return new RemoteServer("127.0.0.1", firstPort, "root", "test", cfg) {
      @Override
      void requestClusterConfiguration() {
        // no cluster behind the scripted server
      }

      @Override
      boolean reloadClusterConfiguration() {
        final List<Pair<String, Integer>> replicas = getReplicaServerList();
        if (replicas.isEmpty())
          replicas.add(new Pair<>("127.0.0.1", secondPort));
        return true;
      }
    };
  }

  /** Loopback server recording each request line and answering with the next scripted response, repeating the last. */
  private static final class ScriptedServer implements AutoCloseable {
    private final ServerSocket                  socket;
    private final ConcurrentLinkedQueue<String> script       = new ConcurrentLinkedQueue<>();
    private final List<String>                  requestLines = new CopyOnWriteArrayList<>();
    private volatile String                     last;

    ScriptedServer(final String... responses) throws IOException {
      script.addAll(List.of(responses));
      socket = new ServerSocket(0);
      final Thread t = new Thread(() -> {
        while (!socket.isClosed()) {
          try (final Socket client = socket.accept()) {
            requestLines.add(readRequestLine(client.getInputStream()));
            final String next = script.poll();
            if (next != null)
              last = next;
            if (last == null || DROP.equals(last))
              continue; // the request was read, and so applied; the response never makes it back
            final OutputStream out = client.getOutputStream();
            out.write(last.getBytes(StandardCharsets.UTF_8));
            out.flush();
          } catch (final IOException ignored) {
            // socket closed at the end of the test
          }
        }
      });
      t.setDaemon(true);
      t.start();
    }

    int port() {
      return socket.getLocalPort();
    }

    List<String> requestLines() {
      return requestLines;
    }

    @Override
    public void close() throws IOException {
      socket.close();
    }

    private static String readRequestLine(final InputStream in) throws IOException {
      final StringBuilder head = new StringBuilder();
      while (head.indexOf("\r\n\r\n") < 0) {
        final int b = in.read();
        if (b < 0)
          return head.toString();
        head.append((char) b);
      }
      int contentLength = 0;
      for (final String line : head.toString().split("\r\n"))
        if (line.toLowerCase(Locale.ROOT).startsWith("content-length:"))
          contentLength = Integer.parseInt(line.substring("content-length:".length()).trim());
      in.readNBytes(contentLength);
      return head.substring(0, head.indexOf("\r\n"));
    }
  }
}
