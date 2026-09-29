/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
 * Issue #7796: {@link RemoteServer#drop}, {@link RemoteServer#createUser} and {@link RemoteServer#dropUser} built the
 * {@code POST /server} request by hand instead of going through {@code httpCommand} the way
 * {@link RemoteServer#create} does. They therefore had no election retry and no failover, and the typed exception
 * {@code manageException} rebuilt from the server's answer was immediately re-wrapped into a
 * {@code DatabaseOperationException} (or a {@code RemoteException}), so {@code catch (SecurityException e)} never
 * fired.
 * <p>
 * Each entry point is driven against a loopback server that answers from a script, one response per request.
 */
class Issue7796RemoteServerAdminCommandsTest {

  private static final String PASSWORD = "s3cr3t-Pa55word";

  private static final String SECURITY_REFUSAL = "HTTP/1.1 403 Forbidden\r\n" + json(new JSONObject()
      .put("error", "Security error")
      .put("detail", "Password does not satisfy the policy")
      .put("exception", "com.arcadedb.server.security.ServerSecurityException"));

  private static final String ELECTION_IN_PROGRESS = "HTTP/1.1 503 Service Unavailable\r\n" + json(new JSONObject()
      .put("error", "Election in progress")
      .put("detail", "Election in progress")
      .put("exception", "com.arcadedb.exception.NeedRetryException"));

  private static final String OK = "HTTP/1.1 200 OK\r\n" + json(new JSONObject().put("result", "ok"));

  private static final String UNTYPED_BAD_REQUEST = "HTTP/1.1 400 Bad Request\r\n" + json(new JSONObject()
      .put("error", "Cannot create user")
      .put("detail", "Cannot create user"));

  /** Scripted "response" that reads the request and closes the connection without answering it. */
  private static final String DROP = "DROP";

  /** One entry point per method the issue names, with the command it must send. */
  private static final List<Pair<String, Consumer<RemoteServer>>> ENTRY_POINTS = List.of(
      new Pair<>("drop database accounts", s -> s.drop("accounts")),
      new Pair<>("create user ", s -> s.createUser("u7796", PASSWORD, Map.of("accounts", "admin"))),
      new Pair<>("drop user u7796", s -> s.dropUser("u7796")));

  // ------------------------------------------------------------------------------------------------------------
  // The typed exception reaches the caller
  // ------------------------------------------------------------------------------------------------------------

  /** The repro of the issue: a password-policy refusal must be catchable as the SecurityException it is. */
  @Test
  void createUserRefusedByTheServerSurfacesTheSecurityException() throws Exception {
    try (final ScriptedServer server = new ScriptedServer(SECURITY_REFUSAL)) {
      final RemoteServer client = client(server.port(), new ContextConfiguration());
      try {
        assertThatThrownBy(() -> client.createUser("u7796", PASSWORD, List.of("accounts")))
            .isExactlyInstanceOf(SecurityException.class)
            .hasMessageContaining("Password does not satisfy the policy");
      } finally {
        client.close();
      }
    }
  }

  @Test
  void everyAdminCommandSurfacesTheTypedExceptionUnwrapped() throws Exception {
    for (final Pair<String, Consumer<RemoteServer>> entry : ENTRY_POINTS) {
      try (final ScriptedServer server = new ScriptedServer(SECURITY_REFUSAL)) {
        final RemoteServer client = client(server.port(), new ContextConfiguration());
        try {
          assertThatThrownBy(() -> entry.getSecond().accept(client))
              .as(entry.getFirst())
              .isExactlyInstanceOf(SecurityException.class);
        } finally {
          client.close();
        }
      }
    }
  }

  // ------------------------------------------------------------------------------------------------------------
  // The election-retry budget applies
  // ------------------------------------------------------------------------------------------------------------

  @Test
  void everyAdminCommandWaitsOutAnElection() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_CLIENT_ELECTION_RETRY_DELAY_MS, 10L);

    for (final Pair<String, Consumer<RemoteServer>> entry : ENTRY_POINTS) {
      try (final ScriptedServer server = new ScriptedServer(ELECTION_IN_PROGRESS, OK)) {
        final RemoteServer client = client(server.port(), cfg);
        try {
          entry.getSecond().accept(client);

          assertThat(server.bodies()).as(entry.getFirst()).hasSize(2);
          for (final String body : server.bodies())
            assertThat(new JSONObject(body).getString("command")).as(entry.getFirst()).startsWith(entry.getFirst());
        } finally {
          client.close();
        }
      }
    }
  }

  // ------------------------------------------------------------------------------------------------------------
  // Failover applies
  // ------------------------------------------------------------------------------------------------------------

  /** The failover half of the issue: a node that cannot be reached is replaced by the next one, as for create(). */
  @Test
  void everyAdminCommandFailsOverFromAnUnreachableServer() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.NETWORK_SAME_SERVER_ERROR_RETRIES, 2);

    for (final Pair<String, Consumer<RemoteServer>> entry : ENTRY_POINTS) {
      final int closedPort;
      try (final ServerSocket probe = new ServerSocket(0)) {
        closedPort = probe.getLocalPort();
      }

      try (final ScriptedServer second = new ScriptedServer(OK)) {
        final RemoteServer client = failoverClient(closedPort, second.port(), cfg);
        try {
          entry.getSecond().accept(client);

          assertThat(second.bodies()).as(entry.getFirst()).hasSize(1);
          assertThat(new JSONObject(second.bodies().getFirst()).getString("command")).as(entry.getFirst())
              .startsWith(entry.getFirst());
        } finally {
          client.close();
        }
      }
    }
  }

  // ------------------------------------------------------------------------------------------------------------
  // The replay guard of #8136 applies: a write whose response was lost is not sent again
  // ------------------------------------------------------------------------------------------------------------

  @Test
  void everyAdminCommandIsNotResentWhenItsResponseWasLost() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.NETWORK_SAME_SERVER_ERROR_RETRIES, 3);

    for (final Pair<String, Consumer<RemoteServer>> entry : ENTRY_POINTS) {
      try (final ScriptedServer server = new ScriptedServer(DROP)) {
        final RemoteServer client = client(server.port(), cfg);
        client.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
        try {
          assertThatThrownBy(() -> entry.getSecond().accept(client))
              .as(entry.getFirst())
              .isInstanceOf(RemoteException.class)
              .hasMessageContaining("may already have applied it");

          assertThat(server.bodies()).as(entry.getFirst()).hasSize(1);
        } finally {
          client.close();
        }
      }
    }
  }

  // ------------------------------------------------------------------------------------------------------------
  // RemoteDatabase.drop() sends the same command and had the same hand-rolled request
  // ------------------------------------------------------------------------------------------------------------

  @Test
  void remoteDatabaseDropSurfacesTheTypedExceptionUnwrapped() throws Exception {
    try (final ScriptedServer server = new ScriptedServer(SECURITY_REFUSAL)) {
      final RemoteDatabase db = database(server.port(), new ContextConfiguration());
      try {
        assertThatThrownBy(db::drop).isExactlyInstanceOf(SecurityException.class);
        assertThat(db.isOpen()).as("a refused drop leaves the handle open").isTrue();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
  }

  @Test
  void remoteDatabaseDropWaitsOutAnElection() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_CLIENT_ELECTION_RETRY_DELAY_MS, 10L);

    try (final ScriptedServer server = new ScriptedServer(ELECTION_IN_PROGRESS, OK)) {
      final RemoteDatabase db = database(server.port(), cfg);
      db.drop();

      assertThat(db.isOpen()).isFalse();
      assertThat(server.bodies()).hasSize(2);
      for (final String body : server.bodies())
        assertThat(new JSONObject(body).getString("command")).isEqualTo("drop database accounts");
    }
  }

  // ------------------------------------------------------------------------------------------------------------
  // What travels, and what does not travel back in an error message
  // ------------------------------------------------------------------------------------------------------------

  @Test
  void createUserSendsTheUserDocument() throws Exception {
    try (final ScriptedServer server = new ScriptedServer(OK)) {
      final RemoteServer client = client(server.port(), new ContextConfiguration());
      try {
        client.createUser("u7796", PASSWORD, Map.of("accounts", "reader"));

        final String command = new JSONObject(server.bodies().getFirst()).getString("command");
        assertThat(command).startsWith("create user ");
        final JSONObject user = new JSONObject(command.substring("create user ".length()));
        assertThat(user.getString("name")).isEqualTo("u7796");
        assertThat(user.getString("password")).isEqualTo(PASSWORD);
        assertThat(user.getJSONObject("databases").getJSONArray("accounts").getString(0)).isEqualTo("reader");
      } finally {
        client.close();
      }
    }
  }

  /**
   * httpCommand names the failed command in the message of an untyped error. For {@code create user} the command
   * text carries the password, which must not end up in an exception message and from there in a log.
   * <p>
   * Only the untyped path needs this check: a typed exception ({@code SecurityException}, ...) is rebuilt from the
   * server's {@code detail} alone and never carries the label, so the label reaches a message only here.
   */
  @Test
  void createUserErrorDoesNotEchoThePassword() throws Exception {
    try (final ScriptedServer server = new ScriptedServer(UNTYPED_BAD_REQUEST)) {
      final RemoteServer client = client(server.port(), new ContextConfiguration());
      try {
        assertThatThrownBy(() -> client.createUser("u7796", PASSWORD, List.of("accounts")))
            .isInstanceOf(RemoteException.class)
            .hasMessageContaining("create user u7796")
            .hasMessageContaining("Cannot create user")
            .hasMessageNotContaining(PASSWORD);
      } finally {
        client.close();
      }
    }
  }

  // ------------------------------------------------------------------------------------------------------------
  // Fixtures
  // ------------------------------------------------------------------------------------------------------------

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

  private static RemoteDatabase database(final int port, final ContextConfiguration cfg) {
    return new RemoteDatabase("127.0.0.1", port, "accounts", "root", "test", cfg) {
      @Override
      void requestClusterConfiguration() {
        // no cluster behind the scripted server
      }
    };
  }

  /**
   * A client whose cluster reload reveals {@code secondPort} as the next server, as an HA topology refresh would.
   */
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

  /**
   * Loopback server that records the body of each request and answers it with the next scripted response,
   * repeating the last one once the script runs out. {@link #DROP} closes the connection without an answer.
   */
  private static final class ScriptedServer implements AutoCloseable {
    private final ServerSocket                  socket;
    private final ConcurrentLinkedQueue<String> script = new ConcurrentLinkedQueue<>();
    private final List<String>                  bodies = new CopyOnWriteArrayList<>();
    private volatile String                     last;

    ScriptedServer(final String... responses) throws IOException {
      script.addAll(List.of(responses));
      socket = new ServerSocket(0);
      final Thread t = new Thread(() -> {
        while (!socket.isClosed()) {
          try (final Socket client = socket.accept()) {
            bodies.add(readBody(client.getInputStream()));
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

    List<String> bodies() {
      return bodies;
    }

    @Override
    public void close() throws IOException {
      socket.close();
    }

    private static String readBody(final InputStream in) throws IOException {
      final StringBuilder head = new StringBuilder();
      while (head.indexOf("\r\n\r\n") < 0) {
        final int b = in.read();
        if (b < 0)
          return "";
        head.append((char) b);
      }
      int contentLength = 0;
      for (final String line : head.toString().split("\r\n"))
        if (line.toLowerCase(Locale.ROOT).startsWith("content-length:"))
          contentLength = Integer.parseInt(line.substring("content-length:".length()).trim());
      return new String(in.readNBytes(contentLength), StandardCharsets.UTF_8);
    }
  }
}
