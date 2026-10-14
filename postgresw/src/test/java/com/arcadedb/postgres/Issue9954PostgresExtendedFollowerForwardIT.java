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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #9954: the Postgres extended query protocol ran a follower's parsed write or schema change locally, where the
 * simple query protocol forwards it to the leader through {@code RaftReplicatedDatabase.command(...)}. The same statement
 * succeeded or failed on a follower depending only on the driver's query mode. Every test here uses
 * {@code preferQueryMode=extended}, pgjdbc's default and what every driver that binds parameters sends.
 */
@Tag("slow")
class Issue9954PostgresExtendedFollowerForwardIT extends BaseRaftHATest {
  private static final String TYPE = "Issue9954Pg";

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

  /**
   * A prepared INSERT reaches the follower's forward: with the forward made to refuse (the #6191 self-address refusal),
   * the write is refused naming the leader, exactly as the same INSERT is over the simple protocol. Before the fix it
   * ran on the follower and committed through Raft from there, so it succeeded.
   */
  @Test
  @Timeout(120)
  void aPreparedWriteGoesThroughTheFollowersForward() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader);
    final Runnable restore = refuseForwardedWritesOn(follower);
    try (final Connection connection = connect(follower);
        final PreparedStatement statement = connection.prepareStatement("INSERT INTO " + TYPE + " SET name = ?")) {
      statement.setString(1, "refused");
      final Throwable refused = catchThrowable(statement::execute);
      assertThat(refused).as("the write must reach the forward, which refuses it").isInstanceOf(SQLException.class);
      assertThat(((SQLException) refused).getSQLState()).isEqualTo("40001");
      assertThat(refused.getMessage()).contains("The leader is server '" + getRaftPlugin(follower).getRaftHAServer().getLeaderName() + "'");

      // The refusal leaves the connection usable: the next statement on it is answered
      try (final PreparedStatement read = connection.prepareStatement("SELECT count(*) AS c FROM " + TYPE + " WHERE name = ?")) {
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
        "SELECT count(*) AS c FROM " + TYPE + " WHERE name = 'refused'")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).as("a refused forward must not have written locally").isZero();
    }
  }

  /** A prepared UPDATE and DELETE reach the follower's forward too, the other two writes a driver binds parameters to. */
  @Test
  @Timeout(120)
  void aPreparedUpdateAndDeleteGoThroughTheFollowersForward() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader);
    final String leaderName = getRaftPlugin(follower).getRaftHAServer().getLeaderName();
    final Runnable restore = refuseForwardedWritesOn(follower);
    try (final Connection connection = connect(follower)) {
      for (final String sql : new String[] { "UPDATE " + TYPE + " SET n = 1 WHERE name = ?", "DELETE FROM " + TYPE + " WHERE name = ?" }) {
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

  /** A prepared INSERT with a bound parameter, forwarded for real, lands on the leader with the parameter's value. */
  @Test
  @Timeout(120)
  void aPreparedWriteWithABoundParameterIsWrittenByTheLeader() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader);
    try (final Connection connection = connect(follower);
        final PreparedStatement statement = connection.prepareStatement("INSERT INTO " + TYPE + " SET name = ?, n = ?")) {
      statement.setString(1, "forwarded");
      statement.setInt(2, 42);
      statement.execute();
      // A second forwarded write on the same connection: the first left no implicit transaction behind to trip it
      statement.setString(1, "forwardedAgain");
      statement.setInt(2, 43);
      statement.execute();
    }

    try (final ResultSet rs = getServerDatabase(leader, getDatabaseName()).query("sql",
        "SELECT n FROM " + TYPE + " WHERE name = 'forwarded'")) {
      assertThat(rs.hasNext()).as("the forwarded write must be on the leader").isTrue();
      assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(42);
    }
    try (final ResultSet rs = getServerDatabase(leader, getDatabaseName()).query("sql",
        "SELECT n FROM " + TYPE + " WHERE name = 'forwardedAgain'")) {
      assertThat(rs.hasNext()).as("the second forwarded write must be on the leader").isTrue();
      assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(43);
    }
  }

  /**
   * A prepared schema change on a follower is forwarded and succeeds, as over the simple protocol. Before the fix it ran
   * on the follower and was refused by the schema layer ("Changes to the schema must be executed on the leader server").
   */
  @Test
  @Timeout(120)
  void aPreparedSchemaChangeOnAFollowerSucceeds() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final String type = TYPE + "Ddl";
    try (final Connection connection = connect(follower);
        final PreparedStatement statement = connection.prepareStatement("CREATE DOCUMENT TYPE " + type)) {
      statement.execute();
    }
    assertThat(getServerDatabase(leader, getDatabaseName()).getSchema().existsType(type))
        .as("the schema change must have run on the leader").isTrue();
    waitForAllServers();
    assertThat(getServerDatabase(follower, getDatabaseName()).getSchema().existsType(type))
        .as("and replicated back to the follower").isTrue();
  }

  /**
   * Inside an explicit transaction block a prepared write is NOT forwarded: the forward would commit it on the leader as
   * a request of its own, so a ROLLBACK could not undo it (issue #10040, which the simple protocol still has). It runs in
   * the follower's own transaction instead, which a ROLLBACK discards and a COMMIT replicates through Raft.
   */
  @Test
  @Timeout(120)
  void aPreparedWriteInsideAnExplicitBlockKeepsTheBlockAtomic() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader);
    try (final Connection connection = connect(follower)) {
      connection.setAutoCommit(false);
      try (final PreparedStatement statement = connection.prepareStatement("INSERT INTO " + TYPE + " SET name = ?")) {
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
    try (final ResultSet rs = leaderDb.query("sql", "SELECT count(*) AS c FROM " + TYPE + " WHERE name = 'rolledBack'")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).as("a rolled-back write must not reach the leader").isZero();
    }
    try (final ResultSet rs = leaderDb.query("sql", "SELECT count(*) AS c FROM " + TYPE + " WHERE name = 'committed'")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).as("a committed write must reach the leader").isEqualTo(1L);
    }
  }

  /** A prepared read stays on the follower: it still answers with the forward refusing every write. */
  @Test
  @Timeout(120)
  void aPreparedReadStillRunsOnTheFollower() throws Exception {
    final int leader = requireLeaderIndex();
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
    leaderDb.transaction(() -> leaderDb.command("sql", "INSERT INTO " + TYPE + " SET name = 'read'"));
    waitForAllServers();
    final int follower = firstFollowerOf(leader);

    final Runnable restore = refuseForwardedWritesOn(follower);
    try (final Connection connection = connect(follower);
        final PreparedStatement statement = connection.prepareStatement("SELECT name FROM " + TYPE + " WHERE name = ?")) {
      statement.setString(1, "read");
      try (final java.sql.ResultSet rs = statement.executeQuery()) {
        assertThat(rs.next()).as("the read must be answered locally").isTrue();
        assertThat(rs.getString("name")).isEqualTo("read");
      }
    } finally {
      restore.run();
    }
  }

  /** Creates the target type through the leader and waits for the follower to see it, then returns the follower. */
  private int prepareFollower(final int leader) {
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
    waitForAllServers();
    return firstFollowerOf(leader);
  }

  private Connection connect(final int serverIndex) throws SQLException {
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("sslmode", "disable");
    props.setProperty("preferQueryMode", "extended");
    return DriverManager.getConnection("jdbc:postgresql://localhost:" + postgresPorts[serverIndex] + "/" + getDatabaseName(), props);
  }
}
