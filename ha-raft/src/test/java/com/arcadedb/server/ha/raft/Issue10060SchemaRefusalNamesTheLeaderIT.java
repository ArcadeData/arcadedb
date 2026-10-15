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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #10060: a follower refusing a schema change named the leader by the HTTP address it resolved for it, even when
 * that address was the follower's OWN - what a same-host cluster that declares no HTTP ports resolves for every peer
 * (issue #6191). The refusal then sent the client back to the node that had just refused it. The forwarding refusals
 * already fall back to the leader's server name in that state; the schema refusal now does the same.
 * <p>
 * Reached the way a Bolt or Postgres client reaches it since issue #10040: an openCypher {@code CREATE} with a label no
 * type exists for yet, inside a transaction the client opened on the follower. The statement runs in that transaction
 * instead of being forwarded, and creating the label's type is a schema change only the leader may make.
 */
class Issue10060SchemaRefusalNamesTheLeaderIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(120)
  void aSchemaRefusalOnAFollowerThatCannotTellItsPeersApartNamesTheLeaderByName() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final RaftHAServer raft = getRaftPlugin(follower).getRaftHAServer();
    final Runnable restore = refuseForwardedWritesOn(follower);
    try {
      final ServerIsNotTheLeaderException refusal = schemaRefusalOn(follower, "Issue10060Ambiguous");
      final String leaderName = raft.getLeaderName();
      assertThat(leaderName).as("the follower must know which node leads").isNotBlank();
      assertThat(raft.isOwnHttpAddress(refusal.getLeaderAddress()))
          .as("the refusal must not send the client back to the follower that refused it (named '%s')", refusal.getLeaderAddress())
          .isFalse();
      assertThat(refusal.getLeaderAddress()).isEqualTo(leaderName);
    } finally {
      restore.run();
    }
  }

  @Test
  @Timeout(120)
  void aSchemaRefusalOnAFollowerOfAWellConfiguredClusterNamesTheLeadersHttpAddress() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final RaftHAServer raft = getRaftPlugin(follower).getRaftHAServer();
    final String leaderHttpAddress = raft.getLeaderHttpAddress();
    assertThat(raft.isOwnHttpAddress(leaderHttpAddress)).as("the fixture declares every node's HTTP port").isFalse();

    final ServerIsNotTheLeaderException refusal = schemaRefusalOn(follower, "Issue10060Configured");
    assertThat(refusal.getLeaderAddress()).isEqualTo(leaderHttpAddress);
  }

  private ServerIsNotTheLeaderException schemaRefusalOn(final int follower, final String label) {
    final Database db = getServerDatabase(follower, getDatabaseName());
    db.begin();
    try {
      // What the Bolt and Postgres executors mark on the transaction a client opens with BEGIN (issue #10040)
      ((DatabaseInternal) db).getTransaction().setClientManaged(true);
      final Throwable error = catchThrowable(() -> db.command("opencypher", "CREATE (n:" + label + " {name: 'refused'})"));
      final ServerIsNotTheLeaderException refusal = CauseChain.find(error, ServerIsNotTheLeaderException.class);
      assertThat(refusal).as("creating a type on a follower must be refused as a leader refusal, got %s", error).isNotNull();
      assertThat(refusal.getMessage()).contains("Changes to the schema must be executed on the leader server");
      return refusal;
    } finally {
      if (db.isTransactionActive())
        db.rollback();
    }
  }
}
