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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readyForQueryStatusOf;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireProtocolTestBase.readMessage;
import static com.arcadedb.postgres.PostgresWireProtocolTestBase.readMessageOfType;
import static com.arcadedb.postgres.PostgresWireProtocolTestBase.sendPasswordMessage;
import static com.arcadedb.postgres.PostgresWireProtocolTestBase.sendStartupMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A Postgres client connected to an HA follower: a write it sends is forwarded to the leader, or refused naming the
 * leader when the forward cannot reach it, on both the simple and the extended query protocol; inside a transaction the
 * client opened, or the implicit transaction of a multi-statement Query message, the writes run on the follower and
 * commit or roll back as one block.
 */
@Tag("slow")
class PostgresFollowerForwardingIT extends BaseRaftHATest {
  // One Postgres port per server, drawn free from the same ledger as the Raft ports
  private final int[] postgresPorts = allocateFixturePorts(3);

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    final int index = serverIndexOf(config);
    config.setValue(GlobalConfiguration.SERVER_PLUGINS.getKey(), "Postgres:com.arcadedb.postgres.PostgresProtocolPlugin");
    config.setValue(GlobalConfiguration.POSTGRES_PORT.getKey(), String.valueOf(postgresPorts[index]));
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  /*
   * Issue #9870: a real follower refusing a real Postgres client's write. Issue #9845 kept the SQLSTATE at {@code 40001},
   * the wire having no redirect code, and named the leader in the message, tested on the mapping helpers alone; this
   * drives it through the JDBC driver, the executor's error arms and the cluster's own refusal, on both the simple and the
   * extended query protocol, which reach the database through different arms.
   */
  private static final String LEADER_REFUSAL_TYPE = "Issue9870Pg";

  // Regression: #9870
  /**
   * The simple protocol goes through the follower's forward, which a correctly configured cluster answers by running the
   * write on the leader. The follower is put in the state a same-host cluster that declares no HTTP ports is in (see
   * {@link BaseRaftHATest#refuseForwardedWritesOn}): the leader address it resolves is its own, so it refuses the forward,
   * naming the leader, rather than POSTing the write back to itself.
   */
  @Test
  @Timeout(120)
  void aWriteOnTheSimpleQueryProtocolNamesTheLeader() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader, LEADER_REFUSAL_TYPE);
    final Runnable restore = refuseForwardedWritesOn(follower);
    try (final Connection connection = connect(follower, "simple"); final Statement statement = connection.createStatement()) {
      final Throwable refused = catchThrowable(() -> statement.execute("INSERT INTO " + LEADER_REFUSAL_TYPE + " SET name = 'simple'"));
      assertRefusalNaming(refused, getRaftPlugin(follower).getRaftHAServer().getLeaderName());
    } finally {
      restore.run();
    }
  }

  // Regression: #9870
  /**
   * The extended protocol reaches the same forward since issue #9954 (it used to run a parsed statement on the node the
   * client reached, so a schema change was refused by the follower's schema layer instead), so it needs the same
   * injection as the simple-protocol case above and is refused the same way, naming the leader.
   */
  @Test
  @Timeout(120)
  void aSchemaChangeOnTheExtendedQueryProtocolNamesTheLeader() throws Exception {
    final int follower = firstFollowerOf(requireLeaderIndex());
    final Runnable restore = refuseForwardedWritesOn(follower);
    try (final Connection connection = connect(follower, "extended");
        final PreparedStatement statement = connection.prepareStatement("CREATE DOCUMENT TYPE Issue9870PgExtended")) {
      final Throwable refused = catchThrowable(statement::execute);
      assertRefusalNaming(refused, getRaftPlugin(follower).getRaftHAServer().getLeaderName());
    } finally {
      restore.run();
    }
  }

  /** {@code leader} is how the refusal names the leader: its server name for a refused forward, its address otherwise. */
  private static void assertRefusalNaming(final Throwable refused, final String leader) {
    assertThat(leader).as("the follower must know which node leads").isNotBlank();
    assertThat(refused).as("the follower must refuse the write").isInstanceOf(SQLException.class);
    assertThat(((SQLException) refused).getSQLState())
        .as("the Postgres wire has no redirect code: the refusal stays the retryable serialization_failure (issue #9845)")
        .isEqualTo("40001");
    assertThat(refused.getMessage())
        .as("the message is the only place a Postgres client or an operator can find the leader in")
        .contains("The leader is server '" + leader + "'");
  }

  /*
   * Issue #9954: the Postgres extended query protocol ran a follower's parsed write or schema change locally, where the
   * simple query protocol forwards it to the leader through {@code RaftReplicatedDatabase.command(...)}. The same statement
   * succeeded or failed on a follower depending only on the driver's query mode. Every test here uses
   * {@code preferQueryMode=extended}, pgjdbc's default and what every driver that binds parameters sends.
   */
  private static final String EXTENDED_FORWARD_TYPE = "Issue9954Pg";

  // Regression: #9954
  /**
   * A prepared INSERT reaches the follower's forward: with the forward made to refuse (the #6191 self-address refusal),
   * the write is refused naming the leader, exactly as the same INSERT is over the simple protocol. Before the fix it
   * ran on the follower and committed through Raft from there, so it succeeded.
   */
  @Test
  @Timeout(120)
  void aPreparedWriteGoesThroughTheFollowersForward() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader, EXTENDED_FORWARD_TYPE);
    final Runnable restore = refuseForwardedWritesOn(follower);
    try (final Connection connection = connect(follower, "extended");
        final PreparedStatement statement = connection.prepareStatement("INSERT INTO " + EXTENDED_FORWARD_TYPE + " SET name = ?")) {
      statement.setString(1, "refused");
      final Throwable refused = catchThrowable(statement::execute);
      assertThat(refused).as("the write must reach the forward, which refuses it").isInstanceOf(SQLException.class);
      assertThat(((SQLException) refused).getSQLState()).isEqualTo("40001");
      assertThat(refused.getMessage()).contains("The leader is server '" + getRaftPlugin(follower).getRaftHAServer().getLeaderName() + "'");

      // The refusal leaves the connection usable: the next statement on it is answered
      try (final PreparedStatement read = connection.prepareStatement("SELECT count(*) AS c FROM " + EXTENDED_FORWARD_TYPE + " WHERE name = ?")) {
        read.setString(1, "refused");
        try (final java.sql.ResultSet rs = read.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getLong("c")).isZero();
        }
      }
    } finally {
      restore.run();
    }

    try (final ResultSet rs = getServerDatabase(leader, getDatabaseName()).query("sql",
        "SELECT count(*) AS c FROM " + EXTENDED_FORWARD_TYPE + " WHERE name = 'refused'")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).as("a refused forward must not have written locally").isZero();
    }
  }

  // Regression: #9954
  /** A prepared UPDATE and DELETE reach the follower's forward too, the other two writes a driver binds parameters to. */
  @Test
  @Timeout(120)
  void aPreparedUpdateAndDeleteGoThroughTheFollowersForward() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader, EXTENDED_FORWARD_TYPE);
    final String leaderName = getRaftPlugin(follower).getRaftHAServer().getLeaderName();
    final Runnable restore = refuseForwardedWritesOn(follower);
    try (final Connection connection = connect(follower, "extended")) {
      for (final String sql : new String[] { "UPDATE " + EXTENDED_FORWARD_TYPE + " SET n = 1 WHERE name = ?", "DELETE FROM " + EXTENDED_FORWARD_TYPE + " WHERE name = ?" }) {
        try (final PreparedStatement statement = connection.prepareStatement(sql)) {
          statement.setString(1, "any");
          final Throwable refused = catchThrowable(statement::execute);
          assertThat(refused).as("'%s' must reach the forward, which refuses it", sql).isInstanceOf(SQLException.class);
          assertThat(((SQLException) refused).getSQLState()).isEqualTo("40001");
          assertThat(refused.getMessage()).contains("The leader is server '" + leaderName + "'");
        }
      }
    } finally {
      restore.run();
    }
  }

  // Regression: #9954
  /** A prepared INSERT with a bound parameter, forwarded for real, lands on the leader with the parameter's value. */
  @Test
  @Timeout(120)
  void aPreparedWriteWithABoundParameterIsWrittenByTheLeader() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader, EXTENDED_FORWARD_TYPE);
    try (final Connection connection = connect(follower, "extended");
        final PreparedStatement statement = connection.prepareStatement("INSERT INTO " + EXTENDED_FORWARD_TYPE + " SET name = ?, n = ?")) {
      statement.setString(1, "forwarded");
      statement.setInt(2, 42);
      statement.execute();
      // A second forwarded write on the same connection: the first left no implicit transaction behind to trip it
      statement.setString(1, "forwardedAgain");
      statement.setInt(2, 43);
      statement.execute();
    }

    try (final ResultSet rs = getServerDatabase(leader, getDatabaseName()).query("sql",
        "SELECT n FROM " + EXTENDED_FORWARD_TYPE + " WHERE name = 'forwarded'")) {
      assertThat(rs.hasNext()).as("the forwarded write must be on the leader").isTrue();
      assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(42);
    }
    try (final ResultSet rs = getServerDatabase(leader, getDatabaseName()).query("sql",
        "SELECT n FROM " + EXTENDED_FORWARD_TYPE + " WHERE name = 'forwardedAgain'")) {
      assertThat(rs.hasNext()).as("the second forwarded write must be on the leader").isTrue();
      assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(43);
    }
  }

  // Regression: #9954
  /**
   * A prepared schema change on a follower is forwarded and succeeds, as over the simple protocol. Before the fix it ran
   * on the follower and was refused by the schema layer ("Changes to the schema must be executed on the leader server").
   */
  @Test
  @Timeout(120)
  void aPreparedSchemaChangeOnAFollowerSucceeds() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final String type = EXTENDED_FORWARD_TYPE + "Ddl";
    try (final Connection connection = connect(follower, "extended");
        final PreparedStatement statement = connection.prepareStatement("CREATE DOCUMENT TYPE " + type)) {
      statement.execute();
    }
    assertThat(getServerDatabase(leader, getDatabaseName()).getSchema().existsType(type))
        .as("the schema change must have run on the leader").isTrue();
    waitForAllServers();
    assertThat(getServerDatabase(follower, getDatabaseName()).getSchema().existsType(type))
        .as("and replicated back to the follower").isTrue();
  }

  // Regression: #9954
  /**
   * Inside an explicit transaction block a prepared write is NOT forwarded: the forward would commit it on the leader as
   * a request of its own, so a ROLLBACK could not undo it (issue #10040, which the simple protocol still has). It runs in
   * the follower's own transaction instead, which a ROLLBACK discards and a COMMIT replicates through Raft.
   */
  @Test
  @Timeout(120)
  void aPreparedWriteInsideAnExplicitBlockKeepsTheBlockAtomic() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader, EXTENDED_FORWARD_TYPE);
    try (final Connection connection = connect(follower, "extended")) {
      connection.setAutoCommit(false);
      try (final PreparedStatement statement = connection.prepareStatement("INSERT INTO " + EXTENDED_FORWARD_TYPE + " SET name = ?")) {
        statement.setString(1, "rolledBack");
        statement.execute();
        connection.rollback();

        statement.setString(1, "committed");
        statement.execute();
        connection.commit();
      }
    }

    waitForAllServers();
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    try (final ResultSet rs = leaderDb.query("sql", "SELECT count(*) AS c FROM " + EXTENDED_FORWARD_TYPE + " WHERE name = 'rolledBack'")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).as("a rolled-back write must not reach the leader").isZero();
    }
    try (final ResultSet rs = leaderDb.query("sql", "SELECT count(*) AS c FROM " + EXTENDED_FORWARD_TYPE + " WHERE name = 'committed'")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).as("a committed write must reach the leader").isEqualTo(1L);
    }
  }

  // Regression: #9954
  /** A prepared read stays on the follower: it still answers with the forward refusing every write. */
  @Test
  @Timeout(120)
  void aPreparedReadStillRunsOnTheFollower() throws Exception {
    final int leader = requireLeaderIndex();
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + EXTENDED_FORWARD_TYPE + " IF NOT EXISTS");
    leaderDb.transaction(() -> leaderDb.command("sql", "INSERT INTO " + EXTENDED_FORWARD_TYPE + " SET name = 'read'"));
    waitForAllServers();
    final int follower = firstFollowerOf(leader);

    final Runnable restore = refuseForwardedWritesOn(follower);
    try (final Connection connection = connect(follower, "extended");
        final PreparedStatement statement = connection.prepareStatement("SELECT name FROM " + EXTENDED_FORWARD_TYPE + " WHERE name = ?")) {
      statement.setString(1, "read");
      try (final java.sql.ResultSet rs = statement.executeQuery()) {
        assertThat(rs.next()).as("the read must be answered locally").isTrue();
        assertThat(rs.getString("name")).isEqualTo("read");
      }
    } finally {
      restore.run();
    }
  }

  /*
   * Issue #10040: a follower forwarded every write it received to the leader, where it committed as a request of its own,
   * even inside a transaction the client had opened with BEGIN. A ROLLBACK then discarded an empty local transaction and
   * the row stayed on the leader, and a block of several writes was not atomic. Driven over the Postgres simple query
   * protocol, the shape of the report: the write now runs in the client's transaction on the follower, and its COMMIT
   * replicates it through Raft.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String EXPLICIT_TRANSACTION_TYPE = "Issue10040Pg";

  // Regression: #10040
  @Test
  @Timeout(180)
  void aRollbackOnAFollowerUndoesTheWritesOfItsBlock() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader, EXPLICIT_TRANSACTION_TYPE);

    try (final Connection connection = connect(follower, "simple"); final Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false); // pgjdbc sends BEGIN with the first statement
      statement.execute("INSERT INTO " + EXPLICIT_TRANSACTION_TYPE + " SET name = 'rb'");
      statement.execute("INSERT INTO " + EXPLICIT_TRANSACTION_TYPE + " SET name = 'rb'");
      connection.rollback();
    }

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, EXPLICIT_TRANSACTION_TYPE, "rb")).as("server %d must hold no row of the rolled back block", i).isZero();
  }

  // Regression: #10040
  @Test
  @Timeout(180)
  void aCommitOnAFollowerPublishesTheWholeBlockEverywhere() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader, EXPLICIT_TRANSACTION_TYPE);

    try (final Connection connection = connect(follower, "simple"); final Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      statement.execute("INSERT INTO " + EXPLICIT_TRANSACTION_TYPE + " SET name = 'block'");
      statement.execute("INSERT INTO " + EXPLICIT_TRANSACTION_TYPE + " SET name = 'block'");
      // inside the block, the follower already sees its own writes: they are in its transaction, not on the leader
      try (final java.sql.ResultSet rs = statement.executeQuery("SELECT count(@rid) AS c FROM " + EXPLICIT_TRANSACTION_TYPE + " WHERE name = 'block'")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getLong(1)).isEqualTo(2L);
      }
      assertThat(count(leader, EXPLICIT_TRANSACTION_TYPE, "block")).as("nothing reaches the leader before COMMIT").isZero();
      connection.commit();
    }

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, EXPLICIT_TRANSACTION_TYPE, "block")).as("server %d must hold the committed block", i).isEqualTo(2L);
  }

  // Regression: #10040
  /**
   * A write that runs on the follower can add a name to the schema dictionary, which every node must agree on: the
   * reason non-idempotent statements were forwarded in the first place (#4039). The leader writes a new name of its own
   * first, so the follower's dictionary page is the one the leader moved on from unless the follower applied it. The
   * block must either commit with every node agreeing on both names, or fail with a retryable conflict, never diverge.
   */
  @Test
  @Timeout(180)
  void aNewPropertyNameWrittenOnAFollowerIsAgreedByEveryNode() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader, EXPLICIT_TRANSACTION_TYPE);

    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    leaderDb.transaction(() -> leaderDb.command("sql", "INSERT INTO " + EXPLICIT_TRANSACTION_TYPE + " SET name = 'leader', leaderOnlyProp = 1"));

    boolean committed = false;
    for (int attempt = 0; attempt < 5 && !committed; attempt++) {
      try (final Connection connection = connect(follower, "simple"); final Statement statement = connection.createStatement()) {
        connection.setAutoCommit(false);
        statement.execute("INSERT INTO " + EXPLICIT_TRANSACTION_TYPE + " SET name = 'dict', followerOnlyProp = 'x'");
        connection.commit();
        committed = true;
      } catch (final SQLException e) {
        // a stale dictionary page is refused by the leader as the retryable serialization_failure, the same as any conflict
        assertThat(e.getSQLState()).as("a conflict must be retryable: %s", e.getMessage()).isEqualTo("40001");
      }
    }
    assertThat(committed).as("the block must commit within a few retries").isTrue();

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, getDatabaseName());
      try (final ResultSet rs = db.query("sql", "SELECT followerOnlyProp FROM " + EXPLICIT_TRANSACTION_TYPE + " WHERE name = 'dict'")) {
        assertThat(rs.next().<String>getProperty("followerOnlyProp")).as("server %d", i).isEqualTo("x");
      }
      try (final ResultSet rs = db.query("sql", "SELECT leaderOnlyProp FROM " + EXPLICIT_TRANSACTION_TYPE + " WHERE name = 'leader'")) {
        assertThat(rs.next().<Number>getProperty("leaderOnlyProp").intValue()).as("server %d", i).isEqualTo(1);
      }
    }
  }

  // Regression: #10040
  /** Outside a block nothing changes: an auto-commit write is still forwarded and committed on the leader. */
  @Test
  @Timeout(180)
  void anAutoCommitWriteOnAFollowerIsStillCommitted() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader, EXPLICIT_TRANSACTION_TYPE);

    try (final Connection connection = connect(follower, "simple"); final Statement statement = connection.createStatement()) {
      statement.execute("INSERT INTO " + EXPLICIT_TRANSACTION_TYPE + " SET name = 'auto'");
    }

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, EXPLICIT_TRANSACTION_TYPE, "auto")).as("server %d must hold the auto-committed row", i).isEqualTo(1L);
  }

  /*
   * Issue #11483 on an HA follower: the statements of one simple Query message run in a single implicit transaction, and a
   * follower forwards a write to the leader as a request of its own unless the transaction it runs in is the client's
   * (issue #10040). Left unmarked, the implicit transaction of a message let a write commit on the leader before a later
   * statement of the same message failed, and the rollback that followed found nothing to undo.
   */
  private static final String MULTI_STATEMENT_TYPE = "Issue11483Ha";

  // Regression: #11483
  @Test
  @Timeout(180)
  void aFailureLaterInTheMessageUndoesAWriteSentToAFollower() throws Exception {
    final int follower = prepareFollower(requireLeaderIndex(), MULTI_STATEMENT_TYPE);

    try (final Socket socket = connectSocket(follower)) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      sendSimpleQuery(out, "INSERT INTO " + MULTI_STATEMENT_TYPE + " SET name = 'undone'; SET x");
      final List<WireMessage> response = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(response)).contains('E');
      assertThat(readyForQueryStatusOf(response)).isEqualTo('I');
    }

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, MULTI_STATEMENT_TYPE, "undone")).as("server %d must hold no row of the failed message", i).isZero();
  }

  // Regression: #11483
  @Test
  @Timeout(180)
  void aMessageThatSucceedsOnAFollowerIsCommittedEverywhere() throws Exception {
    final int follower = prepareFollower(requireLeaderIndex(), MULTI_STATEMENT_TYPE);

    try (final Socket socket = connectSocket(follower)) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      sendSimpleQuery(out, "INSERT INTO " + MULTI_STATEMENT_TYPE + " SET name = 'kept'; INSERT INTO " + MULTI_STATEMENT_TYPE + " SET name = 'kept'");
      final List<WireMessage> response = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(response)).doesNotContain('E');
      assertThat(readyForQueryStatusOf(response)).isEqualTo('I');
    }

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, MULTI_STATEMENT_TYPE, "kept")).as("server %d must hold both rows of the message", i).isEqualTo(2L);
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }

  /** Creates the target type through the leader and waits for the follower to see it, then returns the follower. */
  private int prepareFollower(final int leader, final String type) {
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + type + " IF NOT EXISTS");
    waitForAllServers();
    return firstFollowerOf(leader);
  }

  private long count(final int serverIndex, final String type, final String name) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    try (final ResultSet rs = db.query("sql", "SELECT count(@rid) AS c FROM " + type + " WHERE name = ?", name)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private Connection connect(final int serverIndex, final String queryMode) throws SQLException {
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("sslmode", "disable");
    props.setProperty("preferQueryMode", queryMode);
    return DriverManager.getConnection("jdbc:postgresql://localhost:" + postgresPorts[serverIndex] + "/" + getDatabaseName(), props);
  }

  private Socket connectSocket(final int serverIndex) throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", postgresPorts[serverIndex]), 2000);
    return socket;
  }
}
