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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9995: a follower refuses a schema change and names the leader, and that answer holds on the
 * follower until leadership moves. {@code Database.transaction} retried it {@code txRetries} times anyway, with the retry
 * delays, before the refusal reached the caller - on every transport that runs work through it (the HTTP auto-retry, the MCP
 * tools). It now reaches the caller on the first attempt. A refusal raised mid-election, naming no leader, is still retried:
 * this very node may win the election.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9995FollowerSchemaRefusalNotRetriedTest {
  private static final String LEADER = "leader-host:2480";

  @TempDir
  Path tempDir;

  private LocalDatabase proxied;

  @AfterEach
  void tearDown() {
    if (proxied != null && proxied.isOpen()) {
      proxied.rollbackAllNested();
      proxied.drop();
    }
  }

  private RaftReplicatedDatabase follower(final String leaderHttpAddress) {
    proxied = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue9995").toString()).create();
    return new RaftReplicatedDatabase(null, proxied, FakeRaftHAServer.detached().leader(false).leaderHttpAddress(leaderHttpAddress));
  }

  @Test
  void aSchemaChangeRefusedByAFollowerNamingTheLeaderIsNotRetriedLocally() {
    final RaftReplicatedDatabase database = follower(LEADER);
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      database.getSchema().createDocumentType("Doc9995");
    }, false, 5)).isInstanceOfSatisfying(ServerIsNotTheLeaderException.class, e -> {
      assertThat(e.getLeaderAddress()).isEqualTo(LEADER);
      assertThat(e.isRetryableOnThisNode()).isFalse();
    });

    assertThat(attempts.get()).as("the refusal holds on this node: one attempt answers it").isEqualTo(1);
    assertThat(proxied.getSchema().existsType("Doc9995")).isFalse();
  }

  @Test
  void aSchemaChangeRefusedWhileNoLeaderIsKnownIsStillRetried() {
    final RaftReplicatedDatabase database = follower(null);
    final AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(() -> database.transaction(() -> {
      attempts.incrementAndGet();
      database.getSchema().createDocumentType("Doc9995");
    }, false, 3)).isInstanceOfSatisfying(ServerIsNotTheLeaderException.class, e -> {
      assertThat(e.namesLeader()).isFalse();
      assertThat(e.isRetryableOnThisNode()).isTrue();
    });

    assertThat(attempts.get()).as("mid-election this node may become the leader: every attempt runs").isEqualTo(3);
  }
}
