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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10040: a follower forwarded every write it received to the leader, where it committed as a request of its own,
 * even inside a transaction the client had opened with BEGIN. A ROLLBACK then discarded an empty local transaction and
 * the row stayed on the leader, and a block of several writes was not atomic. Driven over the Postgres simple query
 * protocol, the shape of the report: the write now runs in the client's transaction on the follower, and its COMMIT
 * replicates it through Raft.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue10040PostgresFollowerExplicitTransactionIT extends BaseRaftHATest {
  private static final String TYPE = "Issue10040Pg";

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

  @Test
  @Timeout(180)
  void aRollbackOnAFollowerUndoesTheWritesOfItsBlock() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader);

    try (final Connection connection = connect(follower); final Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false); // pgjdbc sends BEGIN with the first statement
      statement.execute("INSERT INTO " + TYPE + " SET name = 'rb'");
      statement.execute("INSERT INTO " + TYPE + " SET name = 'rb'");
      connection.rollback();
    }

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, "rb")).as("server %d must hold no row of the rolled back block", i).isZero();
  }

  @Test
  @Timeout(180)
  void aCommitOnAFollowerPublishesTheWholeBlockEverywhere() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader);

    try (final Connection connection = connect(follower); final Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      statement.execute("INSERT INTO " + TYPE + " SET name = 'block'");
      statement.execute("INSERT INTO " + TYPE + " SET name = 'block'");
      // inside the block, the follower already sees its own writes: they are in its transaction, not on the leader
      try (final java.sql.ResultSet rs = statement.executeQuery("SELECT count(*) AS c FROM " + TYPE + " WHERE name = 'block'")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getLong(1)).isEqualTo(2L);
      }
      assertThat(count(leader, "block")).as("nothing reaches the leader before COMMIT").isZero();
      connection.commit();
    }

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, "block")).as("server %d must hold the committed block", i).isEqualTo(2L);
  }

  /** Outside a block nothing changes: an auto-commit write is still forwarded and committed on the leader. */
  @Test
  @Timeout(180)
  void anAutoCommitWriteOnAFollowerIsStillCommitted() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader);

    try (final Connection connection = connect(follower); final Statement statement = connection.createStatement()) {
      statement.execute("INSERT INTO " + TYPE + " SET name = 'auto'");
    }

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, "auto")).as("server %d must hold the auto-committed row", i).isEqualTo(1L);
  }

  private long count(final int serverIndex, final String name) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS c FROM " + TYPE + " WHERE name = ?", name)) {
      return rs.next().<Number>getProperty("c").longValue();
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
    props.setProperty("preferQueryMode", "simple");
    return DriverManager.getConnection("jdbc:postgresql://localhost:" + postgresPorts[serverIndex] + "/" + getDatabaseName(), props);
  }
}
