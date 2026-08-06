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
import com.arcadedb.database.Database;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteDatabaseTest {

  private TestableRemoteDatabase database;

  /**
   * Testable subclass that overrides requestClusterConfiguration() to avoid HTTP calls.
   */
  static class TestableRemoteDatabase extends RemoteDatabase {
    TestableRemoteDatabase(final String server, final int port, final String databaseName,
        final String userName, final String userPassword) {
      super(server, port, databaseName, userName, userPassword, new ContextConfiguration());
    }

    @Override
    void requestClusterConfiguration() {
      // No-op to avoid HTTP calls during tests
    }
  }

  @BeforeEach
  void setUp() {
    database = new TestableRemoteDatabase("localhost", 2480, "testdb", "root", "test");
  }

  @AfterEach
  void tearDown() {
    if (database.isOpen())
      database.close();
  }

  @Test
  void getName() {
    assertThat(database.getName()).isEqualTo("testdb");
  }

  @Test
  void getDatabasePath() {
    assertThat(database.getDatabasePath()).isEqualTo("http://localhost:2480/testdb");
  }

  @Test
  void toStringReturnsDatabaseName() {
    assertThat(database.toString()).isEqualTo("testdb");
  }

  @Test
  void isOpen() {
    assertThat(database.isOpen()).isTrue();
  }

  @Test
  void isOpenAfterClose() {
    database.close();
    assertThat(database.isOpen()).isFalse();
  }

  @Test
  void getSchema() {
    assertThat(database.getSchema()).isNotNull();
    assertThat(database.getSchema()).isInstanceOf(RemoteSchema.class);
  }

  @Test
  void getTransactionIsolationLevel() {
    assertThat(database.getTransactionIsolationLevel()).isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
  }

  @Test
  void setTransactionIsolationLevel() {
    database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    assertThat(database.getTransactionIsolationLevel()).isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
  }

  @Test
  void hashCodeIsConsistent() {
    final int hash1 = database.hashCode();
    final int hash2 = database.hashCode();
    assertThat(hash1).isEqualTo(hash2);
  }

  @Test
  void hashCodeUsesPath() {
    assertThat(database.hashCode()).isEqualTo(database.getDatabasePath().hashCode());
  }

  @Test
  void isTransactionActive() {
    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void getNestedTransactions() {
    assertThat(database.getNestedTransactions()).isEqualTo(0);
  }

  @Test
  void checkDatabaseIsOpenWhenClosed() {
    database.close();
    assertThatThrownBy(() -> database.command("sql", "select 1"))
        .isInstanceOf(DatabaseIsClosedException.class);
  }

  // mapArgs tests

  @Test
  void mapArgsWithNull() {
    final Map<String, Object> result = database.mapArgs(null);
    assertThat(result).isNull();
  }

  @Test
  void mapArgsWithEmpty() {
    final Map<String, Object> result = database.mapArgs(new Object[]{});
    assertThat(result).isNull();
  }

  @Test
  void mapArgsWithSingleMap() {
    final Map<String, Object> params = new HashMap<>();
    params.put("name", "test");
    params.put("value", 42);

    final Map<String, Object> result = database.mapArgs(new Object[]{params});
    assertThat(result).isEqualTo(params);
  }

  @Test
  void mapArgsWithMultipleArgs() {
    final Map<String, Object> result = database.mapArgs(new Object[]{"a", "b", "c"});
    assertThat(result).hasSize(3);
    assertThat(result.get("0")).isEqualTo("a");
    assertThat(result.get("1")).isEqualTo("b");
    assertThat(result.get("2")).isEqualTo("c");
  }

  @Test
  void closeIsIdempotent() {
    database.close();
    database.close(); // Should not throw
    assertThat(database.isOpen()).isFalse();
  }

  @Test
  void acquireLock() {
    assertThat(database.acquireLock()).isNotNull();
    assertThat(database.acquireLock()).isInstanceOf(RemoteTransactionExplicitLock.class);
  }

  @Test
  void acquireLockReturnsSameInstance() {
    final var lock1 = database.acquireLock();
    final var lock2 = database.acquireLock();
    assertThat(lock1).isSameAs(lock2);
  }

  @Test
  void getSerializer() {
    assertThat(database.getSerializer()).isNotNull();
  }

  // STICKY strategy session-pinning tests — regression for issue #4273

  @Test
  void stickyStrategyUrlUsesPinnedServerDuringActiveSession() {
    database.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.STICKY);
    database.setStickyTransactionServer(new Pair<>("leader-pod", 2480));
    database.setSessionId("test-session-id");

    assertThat(database.getUrl("command")).isEqualTo("http://leader-pod:2480/api/v1/command");
    assertThat(database.isTransactionActive()).isTrue();
  }

  @Test
  void stickyStrategySessionClearClearsStickyServer() {
    database.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.STICKY);
    database.setStickyTransactionServer(new Pair<>("leader-pod", 2480));
    database.setSessionId("test-session-id");
    assertThat(database.getUrl("command")).contains("leader-pod");

    // Clearing the session must also release the sticky pin
    database.setSessionId(null);

    assertThat(database.getUrl("command")).isEqualTo("http://localhost:2480/api/v1/command");
    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void resolveStickyTargetServerFallsBackToCurrentServerWhenNoLeader() {
    // No leader resolved by the testable cluster-config override - falls back to currentServer/currentPort
    final Pair<String, Integer> target = database.resolveStickyTargetServer();
    assertThat(target.getFirst()).isEqualTo("localhost");
    assertThat(target.getSecond()).isEqualTo(2480);
  }

  @Test
  void resolveStickyTargetServerPrefersLeader() {
    // Simulate cluster-configuration discovery placing a concrete leader pod address;
    // begin() will pin to this leader rather than the load-balancer hostname.
    final Pair<String, Integer> leader = new Pair<>("leader-pod", 2481);
    final TestableRemoteDatabaseWithLeader db = new TestableRemoteDatabaseWithLeader(
        "localhost", 2480, "testdb", "root", "test", leader);
    try {
      assertThat(db.resolveStickyTargetServer()).isEqualTo(leader);
    } finally {
      db.close();
    }
  }

  @Test
  void stickyStrategyClearsPinWhenBeginFails() throws Exception {
    // Bind a ServerSocket on an OS-assigned port, immediately close it: connect attempts to
    // that port will fail fast with ConnectException. The finally block in begin() must
    // release the sticky pin because no session was established.
    final int closedPort;
    try (final ServerSocket probe = new ServerSocket(0)) {
      closedPort = probe.getLocalPort();
    }

    final TestableRemoteDatabase failDb = new TestableRemoteDatabase("127.0.0.1", closedPort, "testdb", "root", "test");
    try {
      failDb.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.STICKY);

      assertThatThrownBy(failDb::begin).isInstanceOf(TransactionException.class);

      // Pin released - URL falls back to the configured host, not a stale pinned address
      assertThat(failDb.isTransactionActive()).isFalse();
      assertThat(failDb.getUrl("command")).isEqualTo("http://127.0.0.1:" + closedPort + "/api/v1/command");
    } finally {
      failDb.close();
    }
  }

  @Test
  void haFailoverDuringActiveTransactionThrowsTransactionException() throws Exception {
    // Bind an ephemeral port then immediately close it; connect attempts will fail fast.
    final int unreachablePort;
    try (final ServerSocket probe = new ServerSocket(0)) {
      unreachablePort = probe.getLocalPort();
    }

    final TestableRemoteDatabase db = new TestableRemoteDatabase("127.0.0.1", unreachablePort, "testdb", "root", "test");
    try {
      // Expose a replica entry so maxRetry = 2, allowing the server-switch retry path.
      db.getReplicaServerList().add(new Pair<>("127.0.0.1", 9999));
      // Simulate an in-flight transaction (session issued by the original server).
      db.setSessionId("live-session-id");

      // query() uses leaderIsPreferable=false, so a replica-sized retry budget applies.
      // The HTTP call fails (ConnectException), the retry path detects an active session,
      // and must reject the server switch with TransactionException.
      assertThatThrownBy(() -> db.query("sql", "select 1"))
          .isInstanceOf(TransactionException.class)
          .hasMessageContaining("failover");

      // Session must be cleared so the object is in a consistent, retryable state.
      assertThat(db.isTransactionActive()).isFalse();
    } finally {
      db.close();
    }
  }

  @Test
  void haFailoverWithoutActiveTransactionContinuesRetry() throws Exception {
    // With no active session the retry path should proceed to a normal connectivity error,
    // not a TransactionException.
    final int unreachablePort;
    try (final ServerSocket probe = new ServerSocket(0)) {
      unreachablePort = probe.getLocalPort();
    }

    final TestableRemoteDatabase db = new TestableRemoteDatabase("127.0.0.1", unreachablePort, "testdb", "root", "test");
    try {
      db.getReplicaServerList().add(new Pair<>("127.0.0.1", 9999));
      // No setSessionId - no active transaction.

      assertThatThrownBy(() -> db.query("sql", "select 1"))
          .isNotInstanceOf(TransactionException.class);
    } finally {
      db.close();
    }
  }

  // commit-index bookmark capture tests, regression for issue #5845

  @Test
  void commitCapturesCommitIndexHeaderForReadYourWrites() throws Exception {
    withHttpServer(List.of(
        new StubResponse(204, "", Map.of(RemoteDatabase.ARCADEDB_SESSION_ID, "test-session")),
        new StubResponse(204, "", Map.of("X-ArcadeDB-Commit-Index", "42"))
    ), port -> {
      final TestableRemoteDatabase db = new TestableRemoteDatabase("127.0.0.1", port, "testdb", "root", "test");
      try {
        db.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);

        db.begin();
        db.commit();

        // The X-ArcadeDB-Commit-Index header on the commit response is the only place the server
        // publishes the bookmark for the just-committed transaction: a raw commit() that ignores it
        // silently degrades READ_YOUR_WRITES to EVENTUAL for the next read.
        assertThat(db.getLastCommitIndex())
            .as("commit() must capture the X-ArcadeDB-Commit-Index response header")
            .isEqualTo(42);
      } finally {
        db.close();
      }
    });
  }

  @Test
  void commitWithoutCommitIndexHeaderLeavesLastCommitIndexUnchanged() throws Exception {
    withHttpServer(List.of(
        new StubResponse(204, "", Map.of(RemoteDatabase.ARCADEDB_SESSION_ID, "test-session")),
        new StubResponse(204, "", Map.of())
    ), port -> {
      final TestableRemoteDatabase db = new TestableRemoteDatabase("127.0.0.1", port, "testdb", "root", "test");
      try {
        db.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);

        db.begin();
        db.commit();

        assertThat(db.getLastCommitIndex()).isEqualTo(-1L);
      } finally {
        db.close();
      }
    });
  }

  /**
   * Testable subclass that injects a fake leader address via the cluster-configuration hook.
   */
  static class TestableRemoteDatabaseWithLeader extends TestableRemoteDatabase {
    private final Pair<String, Integer> fakeLeader;

    TestableRemoteDatabaseWithLeader(final String server, final int port, final String databaseName,
        final String userName, final String userPassword, final Pair<String, Integer> fakeLeader) {
      super(server, port, databaseName, userName, userPassword);
      this.fakeLeader = fakeLeader;
    }

    @Override
    Pair<String, Integer> getLeaderServer() {
      return fakeLeader;
    }
  }

  private record StubResponse(int statusCode, String body, Map<String, String> headers) {
  }

  @FunctionalInterface
  private interface ServerAction {
    void run(int port) throws Exception;
  }

  /**
   * Starts a loopback HTTP server that answers the given responses in order, one per connection, then runs
   * {@code action} against its port. Used to test raw-send call sites (begin/commit/rollback) that bypass
   * httpCommand() and must capture response headers on their own.
   */
  private void withHttpServer(final List<StubResponse> responses, final ServerAction action) throws Exception {
    try (final ServerSocket serverSocket = new ServerSocket(0)) {
      final int port = serverSocket.getLocalPort();

      final Thread serverThread = new Thread(() -> {
        for (final StubResponse stub : responses) {
          try (final Socket client = serverSocket.accept()) {
            final InputStream in = client.getInputStream();
            final byte[] buf = new byte[8192];
            int total = 0;
            // Stops at the header terminator without draining a request body (begin() sends one): safe here
            // because the client never reuses the connection (Connection: close) and does not wait for us to
            // read the body before it reads our response.
            while (total < buf.length) {
              final int n = in.read(buf, total, buf.length - total);
              if (n < 0)
                break;
              total += n;
              if (new String(buf, 0, total, StandardCharsets.ISO_8859_1).contains("\r\n\r\n"))
                break;
            }
            final byte[] body = stub.body().getBytes(StandardCharsets.UTF_8);
            final StringBuilder header = new StringBuilder("HTTP/1.1 " + stub.statusCode() + " \r\nContent-Type: application/json\r\n");
            for (final Map.Entry<String, String> h : stub.headers().entrySet())
              header.append(h.getKey()).append(": ").append(h.getValue()).append("\r\n");
            header.append("Content-Length: ").append(body.length).append("\r\nConnection: close\r\n\r\n");
            client.getOutputStream().write(header.toString().getBytes(StandardCharsets.ISO_8859_1));
            client.getOutputStream().write(body);
            client.getOutputStream().flush();
          } catch (final Exception ignored) {
            // best-effort: the test assertions cover the client side
          }
        }
      });
      serverThread.setDaemon(true);
      serverThread.start();

      action.run(port);
    }
  }
}
