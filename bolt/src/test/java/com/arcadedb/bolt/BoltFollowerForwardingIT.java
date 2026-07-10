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
import com.arcadedb.server.ha.raft.BaseRaftHATest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: a Bolt write issued to a follower must be forwarded to the leader.
 *
 * Before the fix, BoltNetworkExecutor.ensureDatabase() resolved the database but never
 * propagated the authenticated user onto DatabaseContext, so
 * RaftReplicatedDatabase.forwardCommandToLeaderViaRaft threw
 * "Cannot forward command to leader: no authenticated user in the current security context"
 * and every write hitting a follower failed.
 */
@Tag("slow")
class BoltFollowerForwardingIT extends BaseRaftHATest {

  private static final int    BASE_BOLT_PORT = 57687;
  private static final String VERTEX_TYPE    = "BoltFollowerWrite";

  private Driver driver;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));

    config.setValue(GlobalConfiguration.SERVER_PLUGINS.getKey(), "Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
    config.setValue(GlobalConfiguration.BOLT_PORT.getKey(), String.valueOf(BASE_BOLT_PORT + index));
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void teardownBoltClient() {
    if (driver != null) {
      driver.close();
      driver = null;
    }
  }

  @Test
  void writeCommandThroughFollowerIsForwardedToLeader() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    int followerIndex = -1;
    for (int i = 0; i < getServerCount(); i++) {
      if (i != leaderIndex) {
        followerIndex = i;
        break;
      }
    }
    assertThat(followerIndex).as("At least one follower must exist").isGreaterThanOrEqualTo(0);

    // Schema creation goes through the leader to avoid mixing schema replication with the forwarding under test.
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
        leaderDb.getSchema().createVertexType(VERTEX_TYPE);
    });
    waitForAllServers();

    // Open the Bolt driver against the follower deliberately.
    driver = GraphDatabase.driver(
        "bolt://localhost:" + (BASE_BOLT_PORT + followerIndex),
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());

    try (Session session = driver.session(SessionConfig.builder().withDatabase(getDatabaseName()).build())) {
      // Write via Cypher command - the follower must forward to leader, which requires an authenticated
      // user on the DatabaseContext. Before the fix this surfaced a SecurityException to the caller.
      session.run("CREATE (n:" + VERTEX_TYPE + " {name: 'forwarded'})").consume();
    }

    waitForAllServers();

    for (int i = 0; i < getServerCount(); i++) {
      final Database serverDb = getServerDatabase(i, getDatabaseName());
      assertThat(serverDb.countType(VERTEX_TYPE, true))
          .as("Server %d (leader=%d, write went through Bolt follower=%d) must see the forwarded vertex",
              i, leaderIndex, followerIndex)
          .isEqualTo(1);
    }
  }
}
