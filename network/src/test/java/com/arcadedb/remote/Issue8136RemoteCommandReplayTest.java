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
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8136: {@code RemoteHttpComponent.httpCommand} retried a request on any {@link IOException}, and an
 * {@code IOException} is raised as readily for a response lost after the server applied the request as for a
 * server that was never reached. A {@code POST} that is not provably read-only must therefore be re-sent only
 * when the failure proves the request never left the client.
 * <p>
 * The "server" here reads each request in full, counts it - the point at which a real server would have applied
 * it - and then drops the connection without answering, which is the response-side failure of the issue.
 */
class Issue8136RemoteCommandReplayTest {

  private static final String INSERT = "INSERT INTO Account SET balance = 100";

  /**
   * The FIXED arm of the issue: the same request re-sent to the same server once per configured retry.
   */
  @Test
  void fixedStrategyDoesNotResendACommandWhoseResponseWasLost() throws Exception {
    try (final DroppingServer server = new DroppingServer()) {
      final RemoteHttpComponentTest.TestableRemoteHttpComponent c = component(server.port(), sameServerRetries(3));
      c.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
      try {
        assertThatThrownBy(() -> c.httpCommand("POST", "db", "command", "sql", INSERT, null, true, true, null))
            .isInstanceOf(RemoteException.class)
            .hasMessageContaining("may already have applied it");

        assertThat(server.received()).isEqualTo(1);
      } finally {
        c.close();
      }
    }
  }

  /**
   * The same arm, reached through a STICKY pin (the server a remote transaction is bound to) rather than FIXED.
   */
  @Test
  void stickyPinnedStrategyDoesNotResendACommandWhoseResponseWasLost() throws Exception {
    try (final DroppingServer server = new DroppingServer()) {
      final RemoteHttpComponentTest.TestableRemoteHttpComponent c = component(server.port(), sameServerRetries(3));
      c.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.STICKY);
      c.setStickyTransactionServer(new Pair<>("127.0.0.1", server.port()));
      try {
        assertThatThrownBy(() -> c.httpCommand("POST", "db", "command", "sql", INSERT, null, false, true, null))
            .isInstanceOf(RemoteException.class)
            .hasMessageContaining("may already have applied it");

        assertThat(server.received()).isEqualTo(1);
      } finally {
        c.close();
      }
    }
  }

  /**
   * The failover arm, outside a transaction: the request the first server may have applied was sent on to the
   * next server of the cluster, which in an HA cluster applies the same write a second time.
   */
  @Test
  void failoverDoesNotForwardACommandTheFirstServerMayHaveApplied() throws Exception {
    try (final DroppingServer first = new DroppingServer(); final AnsweringServer second = new AnsweringServer()) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_ERROR_RETRIES, 3);
      final RemoteHttpComponentTest.TestableRemoteHttpComponent c = failoverComponent(first.port(), second.port(), cfg);
      try {
        assertThatThrownBy(() -> c.httpCommand("POST", "db", "command", "sql", INSERT, null, false, true, null))
            .isInstanceOf(RemoteException.class)
            .hasMessageContaining("may already have applied it");

        assertThat(first.received()).isEqualTo(1);
        assertThat(second.received()).isZero();
      } finally {
        c.close();
      }
    }
  }

  /**
   * The server-level admin commands of {@link RemoteServer} ({@code create database}, {@code drop user}, ...) are
   * POSTs to {@code /server} and go through the same loop.
   */
  @Test
  void serverAdminCommandIsNotResentWhenItsResponseWasLost() throws Exception {
    try (final DroppingServer server = new DroppingServer()) {
      final RemoteHttpComponentTest.TestableRemoteHttpComponent c = component(server.port(), sameServerRetries(3));
      c.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
      try {
        assertThatThrownBy(() -> c.httpCommand("POST", null, "server", null, "create database accounts", null, true, true, null))
            .isInstanceOf(RemoteException.class)
            .hasMessageContaining("may already have applied it");

        assertThat(server.received()).isEqualTo(1);
      } finally {
        c.close();
      }
    }
  }

  /**
   * A {@code query} is refused by every query engine unless it is idempotent, so replaying one is harmless and the
   * retry budget the application configured still applies to it.
   */
  @Test
  void readOnlyQueryIsStillRetriedWhenItsResponseWasLost() throws Exception {
    try (final DroppingServer server = new DroppingServer()) {
      final RemoteHttpComponentTest.TestableRemoteHttpComponent c = component(server.port(), sameServerRetries(3));
      c.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
      try {
        assertThatThrownBy(() -> c.httpCommand("POST", "db", "query", "sql", "SELECT FROM Account", null, false, true, null))
            .isInstanceOf(RemoteException.class)
            .hasMessageNotContaining("may already have applied it");

        assertThat(server.received()).isEqualTo(3);
      } finally {
        c.close();
      }
    }
  }

  /**
   * Issue #8570: {@code RemoteServer.databases()} is a POST to {@code /server} like {@code create database}, but it
   * changes nothing, so the route name alone wrongly put it in the may-have-been-applied bucket. It states that it is
   * replayable, and the retry budget the application configured applies to it.
   */
  @Test
  void readOnlyServerCommandIsStillRetriedWhenItsResponseWasLost() throws Exception {
    try (final DroppingServer server = new DroppingServer()) {
      final ContextConfiguration cfg = sameServerRetries(3);
      final RemoteServer remoteServer = new RemoteServer("127.0.0.1", server.port(), "root", "test", cfg);
      try {
        remoteServer.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
        // the constructor already asked the server for the cluster configuration
        final int beforeCall = server.received();
        assertThatThrownBy(remoteServer::databases)
            .isInstanceOf(RemoteException.class)
            .hasMessageNotContaining("may already have applied it");

        assertThat(server.received() - beforeCall).isEqualTo(3);
      } finally {
        remoteServer.close();
      }
    }
  }

  /**
   * A write on the same route keeps being refused after the first attempt: the classification is per command.
   */
  @Test
  void createDatabaseOnTheSameRouteIsNotResent() throws Exception {
    try (final DroppingServer server = new DroppingServer()) {
      final RemoteServer remoteServer = new RemoteServer("127.0.0.1", server.port(), "root", "test", sameServerRetries(3));
      try {
        remoteServer.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
        final int beforeCall = server.received();
        assertThatThrownBy(() -> remoteServer.create("accounts"))
            .isInstanceOf(RemoteException.class)
            .hasMessageContaining("may already have applied it");

        assertThat(server.received() - beforeCall).isEqualTo(1);
      } finally {
        remoteServer.close();
      }
    }
  }

  /**
   * A command that never reached a server - connection refused - is still failed over: nothing ran, so sending it
   * to the next server runs it for the first time.
   */
  @Test
  void commandThatNeverConnectedIsStillFailedOver() throws Exception {
    final int closedPort;
    try (final ServerSocket probe = new ServerSocket(0)) {
      closedPort = probe.getLocalPort();
    }

    try (final AnsweringServer second = new AnsweringServer()) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_ERROR_RETRIES, 3);
      final RemoteHttpComponentTest.TestableRemoteHttpComponent c = failoverComponent(closedPort, second.port(), cfg);
      try {
        final Object result = c.httpCommand("POST", "db", "command", "sql", INSERT, null, false, true,
            (response, json) -> json.getString("result"));
        assertThat(result).isEqualTo("ok");
        assertThat(second.received()).isEqualTo(1);
      } finally {
        c.close();
      }
    }
  }

  /**
   * The classification on its own, for the failures a loopback socket cannot produce on demand.
   */
  @Test
  void onlyAFailureToConnectProvesTheRequestNeverLeft() {
    assertThat(RemoteHttpComponent.provablyNeverSent(new ConnectException("Connection refused"))).isTrue();
    assertThat(RemoteHttpComponent.provablyNeverSent(new HttpConnectTimeoutException("connect timed out"))).isTrue();
    assertThat(RemoteHttpComponent.provablyNeverSent(new IOException("wrapped", new ConnectException("refused")))).isTrue();

    assertThat(RemoteHttpComponent.provablyNeverSent(new IOException("Connection reset"))).isFalse();
    assertThat(RemoteHttpComponent.provablyNeverSent(new HttpTimeoutException("request timed out"))).isFalse();
    assertThat(RemoteHttpComponent.provablyNeverSent(
        new IOException("HTTP request watchdog timeout after 30000ms", new TimeoutException()))).isFalse();
  }

  private static ContextConfiguration sameServerRetries(final int retries) {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.NETWORK_SAME_SERVER_ERROR_RETRIES, retries);
    return cfg;
  }

  private static RemoteHttpComponentTest.TestableRemoteHttpComponent component(final int port, final ContextConfiguration cfg) {
    return new RemoteHttpComponentTest.TestableRemoteHttpComponent("127.0.0.1", port, "root", "test", cfg);
  }

  /**
   * A ROUND_ROBIN component whose cluster reload reveals {@code secondPort} as the next server, as an HA topology
   * refresh would.
   */
  @SuppressWarnings("unchecked")
  private static RemoteHttpComponentTest.TestableRemoteHttpComponent failoverComponent(final int firstPort,
      final int secondPort, final ContextConfiguration cfg) {
    return new RemoteHttpComponentTest.TestableRemoteHttpComponent("127.0.0.1", firstPort, "root", "test", cfg) {
      @Override
      boolean reloadClusterConfiguration() {
        try {
          final Field f = RemoteHttpComponent.class.getDeclaredField("replicaServerList");
          f.setAccessible(true);
          final List<Pair<String, Integer>> replicas = (List<Pair<String, Integer>>) f.get(this);
          if (replicas.isEmpty())
            replicas.add(new Pair<>("127.0.0.1", secondPort));
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
        return true;
      }
    };
  }

  /**
   * Loopback server that reads each request in full, counts it, and then either drops the connection without a
   * response or answers 200, depending on the subclass.
   */
  private abstract static class CountingServer implements AutoCloseable {
    private final ServerSocket  socket;
    private final AtomicInteger received = new AtomicInteger();

    CountingServer() throws IOException {
      socket = new ServerSocket(0);
      final Thread t = new Thread(() -> {
        while (!socket.isClosed()) {
          try (final Socket client = socket.accept()) {
            readRequest(client.getInputStream());
            received.incrementAndGet();
            handle(client);
          } catch (final IOException ignored) {
            // socket closed at the end of the test, or the client gave up first
          }
        }
      });
      t.setDaemon(true);
      t.start();
    }

    abstract void handle(Socket client) throws IOException;

    int port() {
      return socket.getLocalPort();
    }

    int received() {
      return received.get();
    }

    @Override
    public void close() throws IOException {
      socket.close();
    }

    private static void readRequest(final InputStream in) throws IOException {
      final StringBuilder head = new StringBuilder();
      while (head.indexOf("\r\n\r\n") < 0) {
        final int b = in.read();
        if (b < 0)
          return;
        head.append((char) b);
      }
      int contentLength = 0;
      for (final String line : head.toString().split("\r\n"))
        if (line.toLowerCase(Locale.ROOT).startsWith("content-length:"))
          contentLength = Integer.parseInt(line.substring("content-length:".length()).trim());
      in.readNBytes(contentLength);
    }
  }

  private static final class DroppingServer extends CountingServer {
    DroppingServer() throws IOException {
      super();
    }

    @Override
    void handle(final Socket client) {
      // the request was applied; the response never makes it back
    }
  }

  private static final class AnsweringServer extends CountingServer {
    AnsweringServer() throws IOException {
      super();
    }

    @Override
    void handle(final Socket client) throws IOException {
      final byte[] body = "{\"result\":\"ok\"}".getBytes(StandardCharsets.UTF_8);
      client.getOutputStream().write(("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + body.length
          + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1));
      client.getOutputStream().write(body);
      client.getOutputStream().flush();
    }
  }
}
