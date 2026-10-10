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
package com.arcadedb.postgres;

import com.arcadedb.database.Database;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9689 over the Postgres protocol: a statement is listed in the server's running statements with its
 * connection's process id, and stopping it works the way PostgreSQL clients expect - a {@code CancelRequest}
 * ({@code Statement.cancel()}, a query timeout) or {@code pg_cancel_backend} stops the statement with
 * {@code 57014 query_canceled} and leaves the connection usable, {@code pg_terminate_backend} closes the connection.
 * Before, a CancelRequest closed the whole connection, and since every connection had process id 0 it reached whichever
 * connection had registered last.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9689PostgresCancelIT extends PostgresWireProtocolTestBase {
  /** One full scan of 10,000 records per record, 100 million comparisons: minutes when left alone, on any machine. */
  private static final String LONG_SQL =
      "SELECT FROM Node9689 WHERE (SELECT count(*) AS c FROM Node9689 WHERE v = $parent.$current.v + 1000000)[0].c > 0";
  private static final String OTHER    = "other9689";
  private static final String PASSWORD = "pwd9689-secret";

  @BeforeEach
  void createData() {
    final Database database = getServerDatabase(0, getDatabaseName());
    if (!database.getSchema().existsType("Node9689")) {
      database.getSchema().createDocumentType("Node9689");
      database.transaction(() -> {
        for (int i = 0; i < 10_000; i++)
          database.newDocument("Node9689").set("v", i).save();
      });
    }
  }

  @AfterEach
  @Override
  public void endTest() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(OTHER) != null)
      security.dropUser(OTHER);
    super.endTest();
  }

  @Test
  void statementCancelStopsTheStatementAndKeepsTheConnection() throws Exception {
    for (final String mode : new String[] { "simple", "extended" }) {
      try (final Connection conn = connect("root", DEFAULT_PASSWORD_FOR_TESTS, mode); final Statement st = conn.createStatement()) {
        final int pid = backendPid(conn);
        final CompletableFuture<Void> running = runAsync(st);

        final RunningQuery entry = awaitRunning(pid);
        assertThat(entry.getProtocol()).isEqualTo("postgres");
        assertThat(entry.getUser()).isEqualTo("root");
        assertThat(entry.getText()).isEqualTo(LONG_SQL);

        st.cancel();
        assertCanceled(running, mode);
        assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);

        // The connection is still there and serves the next statement
        assertThat(backendPid(conn)).as(mode).isEqualTo(pid);
      }
    }
  }

  /** The bug behind it all: every connection had pid 0, so a cancel reached whichever one had registered last. */
  @Test
  void aCancelReachesItsOwnConnectionAndNoOther() throws Exception {
    try (final Connection first = connect("root", DEFAULT_PASSWORD_FOR_TESTS, "simple");
        final Statement firstSt = first.createStatement();
        final Connection second = connect("root", DEFAULT_PASSWORD_FOR_TESTS, "simple");
        final Statement secondSt = second.createStatement()) {
      final int firstPid = backendPid(first);
      final int secondPid = backendPid(second);
      assertThat(firstPid).isNotEqualTo(secondPid);

      final CompletableFuture<Void> firstRunning = runAsync(firstSt);
      final CompletableFuture<Void> secondRunning = runAsync(secondSt);
      final RunningQuery firstEntry = awaitRunning(firstPid);
      final RunningQuery secondEntry = awaitRunning(secondPid);

      // The connection registered first is cancelled: the one registered after it, which used to take the cancel, runs on
      firstSt.cancel();
      assertCanceled(firstRunning, "the cancelled connection");
      assertThat(secondEntry.isTerminated()).as("the other connection's statement must keep running").isFalse();
      assertThat(secondRunning).isNotDone();

      secondSt.cancel();
      assertCanceled(secondRunning, "the second connection");
      assertThat(firstEntry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
    }
  }

  @Test
  void aQueryTimeoutCancelsTheStatementNotTheConnection() throws Exception {
    // pgjdbc enforces setQueryTimeout by sending a CancelRequest when it expires
    try (final Connection conn = connect("root", DEFAULT_PASSWORD_FOR_TESTS, "extended"); final Statement st = conn.createStatement()) {
      st.setQueryTimeout(1);
      assertThatThrownBy(() -> st.executeQuery(LONG_SQL)).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("57014"));
      st.setQueryTimeout(0);
      try (final ResultSet rs = st.executeQuery("SELECT 1 AS one")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt("one")).isEqualTo(1);
      }
    }
  }

  @Test
  void pgCancelBackendStopsAnotherConnectionsStatement() throws Exception {
    try (final Connection victim = connect("root", DEFAULT_PASSWORD_FOR_TESTS, "simple");
        final Statement st = victim.createStatement();
        final Connection admin = connect("root", DEFAULT_PASSWORD_FOR_TESTS, "extended")) {
      final int pid = backendPid(victim);
      assertThat(backendPid(admin)).as("every connection has a process id of its own").isNotEqualTo(pid);

      final CompletableFuture<Void> running = runAsync(st);
      awaitRunning(pid);

      try (final PreparedStatement cancel = admin.prepareStatement("SELECT pg_cancel_backend(?) AS cancelled")) {
        cancel.setInt(1, pid);
        try (final ResultSet rs = cancel.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getBoolean("cancelled")).isTrue();
        }
      }
      assertCanceled(running, "pg_cancel_backend");
      assertThat(backendPid(victim)).isEqualTo(pid);
    }
  }

  @Test
  void aUserSignalsOnlyTheirOwnConnections() throws Exception {
    createUser(OTHER);
    try (final Connection rootConnection = connect("root", DEFAULT_PASSWORD_FOR_TESTS, "simple");
        final Statement st = rootConnection.createStatement();
        final Connection other = connect(OTHER, PASSWORD, "simple");
        final Statement otherSt = other.createStatement()) {
      final int pid = backendPid(rootConnection);
      final CompletableFuture<Void> running = runAsync(st);
      final RunningQuery entry = awaitRunning(pid);

      // Somebody else's connection is answered as one that does not exist
      try (final ResultSet rs = otherSt.executeQuery("SELECT pg_cancel_backend(" + pid + ") AS cancelled")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getBoolean("cancelled")).isFalse();
      }
      try (final ResultSet rs = otherSt.executeQuery("SELECT pg_terminate_backend(" + pid + ") AS terminated")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getBoolean("terminated")).isFalse();
      }
      assertThat(entry.isTerminated()).isFalse();

      // Its own: the user may
      final int otherPid = backendPid(other);
      try (final ResultSet rs = otherSt.executeQuery("SELECT pg_cancel_backend(" + otherPid + ") AS cancelled")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getBoolean("cancelled")).isTrue();
      }

      st.cancel();
      assertCanceled(running, "root's own cancel");
    }
  }

  @Test
  void pgTerminateBackendClosesTheConnection() throws Exception {
    try (final Connection victim = connect("root", DEFAULT_PASSWORD_FOR_TESTS, "simple");
        final Statement st = victim.createStatement();
        final Connection admin = connect("root", DEFAULT_PASSWORD_FOR_TESTS, "simple");
        final Statement adminSt = admin.createStatement()) {
      final int pid = backendPid(victim);
      final CompletableFuture<Void> running = runAsync(st);
      final RunningQuery entry = awaitRunning(pid);

      try (final ResultSet rs = adminSt.executeQuery("SELECT pg_terminate_backend(" + pid + ") AS terminated")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getBoolean("terminated")).isTrue();
      }
      assertThatThrownBy(() -> running.get(30, TimeUnit.SECONDS)).isNotNull();
      assertThat(entry.awaitEnd(30_000)).isTrue();
      assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);

      // The connection is gone
      await().atMost(Duration.ofSeconds(30)).until(() -> PostgresNetworkExecutor.getBackend(pid) == null);
      assertThatThrownBy(() -> st.executeQuery("SELECT 1")).isInstanceOf(SQLException.class);
      // The caller's is not
      assertThat(backendPid(admin)).isNotEqualTo(pid);
    }
  }

  @Test
  void pgTerminateBackendRollsBackTheTransactionItCloses() throws Exception {
    try (final Connection victim = connect("root", DEFAULT_PASSWORD_FOR_TESTS, "extended");
        final Connection admin = connect("root", DEFAULT_PASSWORD_FOR_TESTS, "simple");
        final Statement adminSt = admin.createStatement()) {
      final int pid = backendPid(victim);
      adminSt.execute("CREATE DOCUMENT TYPE Written9689 IF NOT EXISTS");

      // A transaction with a write in it, and a statement still running in it
      victim.setAutoCommit(false);
      try (final Statement st = victim.createStatement()) {
        st.execute("INSERT INTO Written9689 SET x = 1");
      }
      final Statement st = victim.createStatement();
      final CompletableFuture<Void> running = runAsync(st);
      final RunningQuery entry = awaitRunning(pid);

      try (final ResultSet rs = adminSt.executeQuery("SELECT pg_terminate_backend(" + pid + ") AS terminated")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getBoolean("terminated")).isTrue();
      }
      assertThatThrownBy(() -> running.get(30, TimeUnit.SECONDS)).isNotNull();
      assertThat(entry.awaitEnd(30_000)).isTrue();

      // The connection is gone, and with it the transaction: nothing it wrote was committed, nothing of it is left behind
      await().atMost(Duration.ofSeconds(30)).until(() -> PostgresNetworkExecutor.getBackend(pid) == null);
      try (final ResultSet rs = adminSt.executeQuery("SELECT count(*) AS c FROM Written9689")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getLong("c")).isZero();
      }
    }
  }

  @Test
  void aCancelRequestWithTheWrongSecretStopsNothing() throws Exception {
    try (final Connection conn = connect("root", DEFAULT_PASSWORD_FOR_TESTS, "simple"); final Statement st = conn.createStatement()) {
      final int pid = backendPid(conn);
      final CompletableFuture<Void> running = runAsync(st);
      final RunningQuery entry = awaitRunning(pid);

      // The pid of a live connection, quoted with a secret that is not its own: no answer, and nothing stopped
      try (final Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 5_000);
        final DataOutputStream cancel = new DataOutputStream(socket.getOutputStream());
        cancel.writeInt(16);
        cancel.writeInt(80877102); // CancelRequest
        cancel.writeInt(pid);
        cancel.writeInt(0x5ec2e7);
        cancel.flush();
        socket.setSoTimeout(10_000);
        assertThat(socket.getInputStream().read()).as("a CancelRequest is never answered").isEqualTo(-1);
      }
      Thread.sleep(500);
      assertThat(entry.isTerminated()).as("a wrong secret must not stop the statement").isFalse();

      // The connection's own key still works
      st.cancel();
      assertCanceled(running, "the right key");
      assertThat(backendPid(conn)).isEqualTo(pid);
    }
  }

  @Test
  void aPidThatNamesNoConnectionIsAnsweredFalse() throws Exception {
    try (final Connection conn = connect("root", DEFAULT_PASSWORD_FOR_TESTS, "simple"); final Statement st = conn.createStatement()) {
      // Beyond the int range of a pid, a pid nobody holds: no connection, no error. A NULL is answered NULL, as PostgreSQL does
      for (final String function : new String[] { "pg_cancel_backend", "pg_terminate_backend" }) {
        for (final String argument : new String[] { "9999999999", "2147483647" })
          try (final ResultSet rs = st.executeQuery("SELECT " + function + "(" + argument + ") AS signalled")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBoolean("signalled")).as("%s(%s)", function, argument).isFalse();
          }
        try (final ResultSet rs = st.executeQuery("SELECT " + function + "(null) AS signalled")) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getObject("signalled")).as("%s(null)", function).isNull();
        }
        // A fractional pid is refused, never rounded to some other connection's
        final int pid = backendPid(conn);
        assertThatThrownBy(() -> st.executeQuery("SELECT " + function + "(" + pid + ".5) AS signalled").close())
            .as("%s(%s.5)", function, pid).isInstanceOf(SQLException.class);
      }
      assertThat(backendPid(conn)).isPositive();
    }
  }

  @Test
  void aConnectionCanTerminateItself() throws Exception {
    try (final Connection conn = connect("root", DEFAULT_PASSWORD_FOR_TESTS, "simple"); final Statement st = conn.createStatement()) {
      final int pid = backendPid(conn);
      // The connection being answered on is the one closed: the client sees it go, and the server forgets it
      assertThatThrownBy(() -> {
        try (final ResultSet rs = st.executeQuery("SELECT pg_terminate_backend(pg_backend_pid()) AS terminated")) {
          rs.next();
        }
        st.executeQuery("SELECT 1").close();
      }).isInstanceOf(SQLException.class);
      await().atMost(Duration.ofSeconds(30)).until(() -> PostgresNetworkExecutor.getBackend(pid) == null);
    }
    // The server serves the next connection
    try (final Connection next = connect("root", DEFAULT_PASSWORD_FOR_TESTS, "simple")) {
      assertThat(backendPid(next)).isPositive();
    }
  }

  @Test
  void anIdleConnectionIsNotDisturbedByACancel() throws Exception {
    try (final Connection conn = connect("root", DEFAULT_PASSWORD_FOR_TESTS, "simple"); final Statement st = conn.createStatement()) {
      final int pid = backendPid(conn);
      // Nothing running: nothing to stop, and the connection stays (it used to be closed)
      st.cancel();
      assertThat(backendPid(conn)).isEqualTo(pid);
    }
  }

  // ---------------------------------------------------------------------------------------------

  private RunningQuery awaitRunning(final int pid) {
    final RunningQuery[] found = new RunningQuery[1];
    await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).until(() -> {
      for (final RunningQuery q : getServer(0).getRunningQueries().getRunning())
        if (String.valueOf(pid).equals(q.getConnectionId()) && LONG_SQL.equals(q.getText())) {
          found[0] = q;
          return true;
        }
      return false;
    });
    return found[0];
  }

  private static void assertCanceled(final CompletableFuture<Void> running, final String what) {
    assertThatThrownBy(() -> running.get(30, TimeUnit.SECONDS)).as(what).satisfies(e -> {
      Throwable cause = e.getCause() instanceof CompletionException ? e.getCause().getCause() : e.getCause();
      while (cause != null && !(cause instanceof SQLException))
        cause = cause.getCause();
      assertThat(cause).as(what + ": %s", e).isInstanceOf(SQLException.class);
      assertThat(((SQLException) cause).getSQLState()).as(what).isEqualTo("57014");
    });
  }

  private static CompletableFuture<Void> runAsync(final Statement st) {
    return CompletableFuture.runAsync(() -> {
      try (final ResultSet rs = st.executeQuery(LONG_SQL)) {
        while (rs.next())
          rs.getObject(1);
      } catch (final SQLException e) {
        throw new CompletionException(e);
      }
    });
  }

  private static int backendPid(final Connection conn) throws SQLException {
    try (final Statement st = conn.createStatement(); final ResultSet rs = st.executeQuery("SELECT pg_backend_pid() AS pid")) {
      assertThat(rs.next()).isTrue();
      return rs.getInt("pid");
    }
  }

  private void createUser(final String name) {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(name) == null)
      security.createUser(new JSONObject().put("name", name).put("password", security.encodePassword(PASSWORD))
          .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { "admin" }))));
  }

  private Connection connect(final String user, final String password, final String mode) throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    props.setProperty("ssl", "false");
    props.setProperty("preferQueryMode", mode);
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), props);
  }
}
