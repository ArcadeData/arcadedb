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
package com.arcadedb.bolt;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10040 over Bolt: a write sent to a follower inside an explicit transaction ({@code BEGIN}/{@code RUN}/
 * {@code COMMIT}) used to be forwarded to the leader and committed there on its own, so a ROLLBACK could not undo it.
 * It now runs in the client's transaction on the follower, and the COMMIT replicates it through Raft.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue10040BoltFollowerExplicitTransactionIT extends BaseRaftHATest {
  private static final String LABEL = "Issue10040Bolt";

  // One Bolt port per server, drawn free from the same ledger as the Raft ports (issue #8203)
  private final int[] boltPorts = allocateFixturePorts(3);

  private Driver driver;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    final int index = serverIndexOf(config);
    config.setValue(GlobalConfiguration.SERVER_PLUGINS.getKey(), "Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
    config.setValue(GlobalConfiguration.BOLT_PORT.getKey(), String.valueOf(boltPorts[index]));
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void closeDriver() {
    if (driver != null) {
      driver.close();
      driver = null;
    }
  }

  @Test
  @Timeout(180)
  void aRollbackOnAFollowerUndoesItsWritesAndACommitPublishesThem() {
    final int leader = requireLeaderIndex();
    getServerDatabase(leader, getDatabaseName()).command("sql", "CREATE VERTEX TYPE " + LABEL + " IF NOT EXISTS");
    waitForAllServers();
    final int follower = firstFollowerOf(leader);

    try (final Session session = followerSession(follower)) {
      try (final Transaction tx = session.beginTransaction()) {
        tx.run("CREATE (:" + LABEL + " {name: 'rb'})").consume();
        tx.run("CREATE (:" + LABEL + " {name: 'rb'})").consume();
        assertThat(count(leader, "rb")).as("nothing reaches the leader before the transaction ends").isZero();
        tx.rollback();
      }

      waitForAllServers();
      for (int i = 0; i < getServerCount(); i++)
        assertThat(count(i, "rb")).as("server %d must hold no node of the rolled back transaction", i).isZero();

      try (final Transaction tx = session.beginTransaction()) {
        tx.run("CREATE (:" + LABEL + " {name: 'ok'})").consume();
        tx.run("CREATE (:" + LABEL + " {name: 'ok'})").consume();
        tx.commit();
      }
    }

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, "ok")).as("server %d must hold the committed transaction", i).isEqualTo(2L);
  }

  private long count(final int serverIndex, final String name) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    try (final ResultSet rs = db.query("sql", "SELECT count(@rid) AS c FROM " + LABEL + " WHERE name = ?", name)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private Session followerSession(final int follower) {
    driver = GraphDatabase.driver("bolt://localhost:" + boltPorts[follower], AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
    return driver.session(SessionConfig.builder().withDatabase(getDatabaseName()).build());
  }
}
