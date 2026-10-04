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
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8526: a write whose response was lost can be sent again when it carries an {@code X-Request-Id} and the server has said it
 * answers a replay from its cache, instead of failing with "the server may already have applied it" (issue #8136).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8526RemoteRequestIdReplayTest {

  private static final String INSERT = "INSERT INTO Account SET balance = 100";

  @Test
  void aCommandWhoseResponseWasLostIsSentAgainWithTheSameRequestId() throws Exception {
    // answer the priming query (and advertise replay protection), drop the first attempt of the command, answer the second
    try (final ScriptedServer server = new ScriptedServer(true, "answer", "drop", "answer")) {
      final RemoteHttpComponentTest.TestableRemoteHttpComponent c = component(server.port());
      try {
        c.httpCommand("POST", "db", "query", "sql", "SELECT 1", null, false, true, null);

        final Object result = c.httpCommand("POST", "db", "command", "sql", INSERT, null, false, true, (response, json) -> json.getString("result"));
        assertThat(result).isEqualTo("ok");

        assertThat(server.received()).isEqualTo(3);
        final List<String> ids = server.requestIds();
        assertThat(ids.get(1)).isNotNull().isEqualTo(ids.get(2));
        // the retry names the server process that advertised replay protection, the first attempt does not
        assertThat(server.instances().get(1)).isNull();
        assertThat(server.instances().get(2)).isEqualTo("proc-1");
      } finally {
        c.close();
      }
    }
  }

  @Test
  void eachLogicalCallGetsItsOwnRequestId() throws Exception {
    try (final ScriptedServer server = new ScriptedServer(true, "answer", "answer", "answer")) {
      final RemoteHttpComponentTest.TestableRemoteHttpComponent c = component(server.port());
      try {
        c.httpCommand("POST", "db", "query", "sql", "SELECT 1", null, false, true, null);
        c.httpCommand("POST", "db", "command", "sql", INSERT, null, false, true, null);
        c.httpCommand("POST", "db", "command", "sql", INSERT, null, false, true, null);

        final List<String> ids = server.requestIds();
        // a read-only query is replayable without any id
        assertThat(ids.get(0)).isNull();
        assertThat(ids.get(1)).isNotNull();
        assertThat(ids.get(2)).isNotNull().isNotEqualTo(ids.get(1));
      } finally {
        c.close();
      }
    }
  }

  @Test
  void aServerThatNeverAdvertisedReplayProtectionKeepsTheRefusal() throws Exception {
    try (final ScriptedServer server = new ScriptedServer(false, "answer", "drop", "answer")) {
      final RemoteHttpComponentTest.TestableRemoteHttpComponent c = component(server.port());
      try {
        c.httpCommand("POST", "db", "query", "sql", "SELECT 1", null, false, true, null);

        assertThatThrownBy(() -> c.httpCommand("POST", "db", "command", "sql", INSERT, null, false, true, null))
            .isInstanceOf(RemoteException.class)
            .hasMessageContaining("may already have applied it");
        assertThat(server.received()).isEqualTo(2);
      } finally {
        c.close();
      }
    }
  }

  @Test
  void aRestartedServerAfterAConnectFailureStillRunsTheCommand() throws Exception {
    // the client saw process proc-old, the server restarted (now another process): the first attempt is refused at connect, so nothing
    // was sent and the retry must not insist on the old process
    final int port;
    try (final ServerSocket probe = new ServerSocket(0)) {
      port = probe.getLocalPort();
    }
    final ScriptedServer[] restarted = new ScriptedServer[1];
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.NETWORK_SAME_SERVER_ERROR_RETRIES, 3);
    final RemoteHttpComponentTest.TestableRemoteHttpComponent c = new RemoteHttpComponentTest.TestableRemoteHttpComponent("127.0.0.1", port,
        "root", "test", cfg) {
      @Override
      HttpResponse<String> sendWithWatchdog(final HttpRequest request, final long watchdogMs)
          throws IOException, InterruptedException {
        if (restarted[0] == null) {
          try {
            return super.sendWithWatchdog(request, watchdogMs);
          } finally {
            // the server comes back right after the refused attempt
            restarted[0] = new ScriptedServer(port, true, "answer");
          }
        }
        return super.sendWithWatchdog(request, watchdogMs);
      }
    };
    c.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    try {
      c.serverReplayInstances.put("127.0.0.1:" + port, "proc-old");
      final Object result = c.httpCommand("POST", "db", "command", "sql", INSERT, null, false, true, (response, json) -> json.getString("result"));
      assertThat(result).isEqualTo("ok");
      assertThat(restarted[0].instances().get(0)).isNull();
    } finally {
      c.close();
      if (restarted[0] != null)
        restarted[0].close();
    }
  }

  @Test
  void failoverAfterAConnectFailureDoesNotCarryTheOtherServersInstance() throws Exception {
    final int closedPort;
    try (final ServerSocket probe = new ServerSocket(0)) {
      closedPort = probe.getLocalPort();
    }
    try (final ScriptedServer second = new ScriptedServer(true, "answer")) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_ERROR_RETRIES, 3);
      final RemoteHttpComponentTest.TestableRemoteHttpComponent c = new RemoteHttpComponentTest.TestableRemoteHttpComponent("127.0.0.1",
          closedPort, "root", "test", cfg) {
        @Override
        boolean reloadClusterConfiguration() {
          try {
            final Field f = RemoteHttpComponent.class.getDeclaredField("replicaServerList");
            f.setAccessible(true);
            @SuppressWarnings("unchecked") final List<Pair<String, Integer>> replicas = (List<Pair<String, Integer>>) f.get(this);
            if (replicas.isEmpty())
              replicas.add(new Pair<>("127.0.0.1", second.port()));
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
          return true;
        }
      };
      try {
        // the first server was seen once, with its own instance, then refuses connections
        c.serverReplayInstances.put("127.0.0.1:" + closedPort, "proc-closed");
        final Object result = c.httpCommand("POST", "db", "command", "sql", INSERT, null, false, true, (response, json) -> json.getString("result"));
        assertThat(result).isEqualTo("ok");
        assertThat(second.instances().get(0)).isNull();
      } finally {
        c.close();
      }
    }
  }

  private static RemoteHttpComponentTest.TestableRemoteHttpComponent component(final int port) {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.NETWORK_SAME_SERVER_ERROR_RETRIES, 3);
    final RemoteHttpComponentTest.TestableRemoteHttpComponent c = new RemoteHttpComponentTest.TestableRemoteHttpComponent("127.0.0.1", port,
        "root", "test", cfg);
    c.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    return c;
  }

  /** Loopback server that plays one scripted action per request: answer 200 or drop the connection after reading the request. */
  private static final class ScriptedServer implements AutoCloseable {
    private final ServerSocket  socket;
    private final List<String>  requestIds = Collections.synchronizedList(new ArrayList<>());
    private final List<String>  instances  = Collections.synchronizedList(new ArrayList<>());
    private final List<String>  script;
    private final boolean       advertise;

    ScriptedServer(final boolean advertise, final String... script) throws IOException {
      this(0, advertise, script);
    }

    ScriptedServer(final int port, final boolean advertise, final String... script) throws IOException {
      this.advertise = advertise;
      this.script = List.of(script);
      socket = new ServerSocket(port);
      final Thread t = new Thread(() -> {
        while (!socket.isClosed()) {
          try (final Socket client = socket.accept()) {
            final String id = readRequest(client.getInputStream(), instances);
            final int n = requestIds.size();
            requestIds.add(id);
            if (n < this.script.size() && "answer".equals(this.script.get(n)))
              answer(client);
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

    int received() {
      return requestIds.size();
    }

    List<String> instances() {
      return new ArrayList<>(instances);
    }

    List<String> requestIds() {
      return new ArrayList<>(requestIds);
    }

    private void answer(final Socket client) throws IOException {
      final byte[] body = "{\"result\":\"ok\"}".getBytes(StandardCharsets.UTF_8);
      client.getOutputStream().write(("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
          + (advertise ? "X-ArcadeDB-Replay-Protection: proc-1\r\n" : "") + "Content-Length: " + body.length + "\r\nConnection: close\r\n\r\n")
          .getBytes(StandardCharsets.ISO_8859_1));
      client.getOutputStream().write(body);
      client.getOutputStream().flush();
    }

    /** Reads the request in full and returns its X-Request-Id, or null. */
    private static String readRequest(final InputStream in, final List<String> instances) throws IOException {
      final StringBuilder head = new StringBuilder();
      while (head.indexOf("\r\n\r\n") < 0) {
        final int b = in.read();
        if (b < 0)
          return null;
        head.append((char) b);
      }
      int contentLength = 0;
      String requestId = null;
      String instance = null;
      for (final String line : head.toString().split("\r\n")) {
        final String lower = line.toLowerCase(Locale.ROOT);
        if (lower.startsWith("content-length:"))
          contentLength = Integer.parseInt(line.substring("content-length:".length()).trim());
        else if (lower.startsWith("x-request-id:"))
          requestId = line.substring("x-request-id:".length()).trim();
        else if (lower.startsWith("x-arcadedb-replay-instance:"))
          instance = line.substring("x-arcadedb-replay-instance:".length()).trim();
      }
      in.readNBytes(contentLength);
      instances.add(instance);
      return requestId;
    }

    @Override
    public void close() throws IOException {
      socket.close();
    }
  }
}
