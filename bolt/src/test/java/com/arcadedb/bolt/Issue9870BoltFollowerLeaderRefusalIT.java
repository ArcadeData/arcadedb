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
import org.neo4j.driver.exceptions.Neo4jException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #9870: a real follower refusing a real Bolt client's write. Issue #9845 made the refusal a NotALeader carrying
 * the leader's address, tested on the mapping helpers alone; this drives it through the driver, the executor's failure
 * arms and the cluster's own refusal.
 * <p>
 * The follower is put in the state a same-host cluster that declares no HTTP ports is in (see
 * {@link BaseRaftHATest#refuseForwardedWritesOn}): the leader address it resolves is its own, so it refuses the write
 * it would forward, naming the leader, rather than forwarding it. Against a correctly configured cluster the same write
 * is forwarded and succeeds ({@link BoltFollowerForwardingIT}).
 */
@Tag("slow")
class Issue9870BoltFollowerLeaderRefusalIT extends BaseRaftHATest {

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
  @Timeout(120)
  void anAutoCommitWriteOnAFollowerIsAnsweredNotALeaderNamingTheLeader() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Runnable restore = refuseForwardedWritesOn(follower);
    try (final Session session = followerSession(follower)) {
      final Throwable refused = catchThrowable(() -> session.run("CREATE (n:Issue9870AutoCommit {name: 'refused'})").consume());
      assertNotALeaderNaming(refused, follower);
    } finally {
      restore.run();
    }
  }

  @Test
  @Timeout(120)
  void aWriteInAnExplicitTransactionOnAFollowerIsAnsweredNotALeaderNamingTheLeader() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Runnable restore = refuseForwardedWritesOn(follower);
    try (final Session session = followerSession(follower)) {
      // Since issue #10040 the write runs in the client's transaction on the follower rather than being forwarded. The label
      // has no type yet, and only the leader may create it: the follower asks it to (issue #10067), and that forward is the
      // one refused here. The refusal must name the leader, not the follower's own address it resolved for it (issues #10060,
      // #10066)
      final Throwable refused = catchThrowable(() -> {
        try (final Transaction tx = session.beginTransaction()) {
          tx.run("CREATE (n:Issue9870Explicit {name: 'refused'})").consume();
          tx.commit();
        }
      });
      assertNotALeaderNaming(refused, follower);
    } finally {
      restore.run();
    }
  }

  private void assertNotALeaderNaming(final Throwable refused, final int follower) {
    assertThat(refused).as("the follower must refuse the write").isInstanceOf(Neo4jException.class);
    assertThat(((Neo4jException) refused).code())
        .as("a routing driver re-routes on NotALeader, and retries DeadlockDetected on the same follower (issue #9845)")
        .isEqualTo(BoltErrorCodes.NOT_A_LEADER_ERROR);
    // The routing table resolves the leader's Bolt endpoint from the same peer HTTP addresses the injection cleared, so
    // it has none here, and the text falls back to naming the leader the way the refusal does (its server name)
    final String leaderName = getRaftPlugin(follower).getRaftHAServer().getLeaderName();
    assertThat(leaderName).as("the follower must know which node leads").isNotBlank();
    assertThat(refused.getMessage())
        .as("a bolt:// client has only the text to find the leader in")
        .contains("The leader is server '" + leaderName + "'");
  }

  private Session followerSession(final int follower) {
    driver = GraphDatabase.driver("bolt://localhost:" + boltPorts[follower], AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
    return driver.session(SessionConfig.builder().withDatabase(getDatabaseName()).build());
  }
}
