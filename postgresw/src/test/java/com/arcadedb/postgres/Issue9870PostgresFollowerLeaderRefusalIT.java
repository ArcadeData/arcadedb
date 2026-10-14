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
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #9870: a real follower refusing a real Postgres client's write. Issue #9845 kept the SQLSTATE at {@code 40001},
 * the wire having no redirect code, and named the leader in the message, tested on the mapping helpers alone; this
 * drives it through the JDBC driver, the executor's error arms and the cluster's own refusal, on both the simple and the
 * extended query protocol, which reach the database through different arms.
 */
@Tag("slow")
class Issue9870PostgresFollowerLeaderRefusalIT extends BaseRaftHATest {
  private static final String TYPE = "Issue9870Pg";

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
   * The simple protocol goes through the follower's forward, which a correctly configured cluster answers by running the
   * write on the leader. The follower is put in the state a same-host cluster that declares no HTTP ports is in (see
   * {@link BaseRaftHATest#refuseForwardedWritesOn}): the leader address it resolves is its own, so it refuses the forward,
   * naming the leader, rather than POSTing the write back to itself.
   */
  @Test
  @Timeout(120)
  void aWriteOnTheSimpleQueryProtocolNamesTheLeader() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = prepareFollower(leader);
    final Runnable restore = refuseForwardedWritesOn(follower);
    try (final Connection connection = connect(follower, "simple"); final Statement statement = connection.createStatement()) {
      final Throwable refused = catchThrowable(() -> statement.execute("INSERT INTO " + TYPE + " SET name = 'simple'"));
      assertRefusalNaming(refused, getRaftPlugin(follower).getRaftHAServer().getLeaderName());
    } finally {
      restore.run();
    }
  }

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

  /** Creates the target type through the leader and waits for the follower to see it, then returns the follower. */
  private int prepareFollower(final int leader) {
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
    waitForAllServers();
    return firstFollowerOf(leader);
  }

  private Connection connect(final int serverIndex, final String queryMode) throws SQLException {
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("sslmode", "disable");
    props.setProperty("preferQueryMode", queryMode);
    return DriverManager.getConnection("jdbc:postgresql://localhost:" + postgresPorts[serverIndex] + "/" + getDatabaseName(), props);
  }
}
