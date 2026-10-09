/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/**
 * Issue #9590: the branch matrix of {@code RaftReplicatedDatabase.waitForReadConsistency()} on a node removed from the
 * Raft configuration, in the regular lane. {@link Issue9590RemovedNodeRefusesConsistentReadsIT} drives the same gate
 * against real Ratis.
 */
class Issue9590ReadConsistencyGateOnRemovedNodeTest {

  private static final String DB_NAME  = "readgate9590";
  private static final long   APPLIED  = 100L;
  private static final String QUERY    = "SELECT FROM V";

  @AfterEach
  void clearContext() {
    RaftReplicatedDatabase.removeReadConsistencyContext();
  }

  private static RaftReplicatedDatabase databaseWith(final RaftHAServer raft) {
    final LocalDatabase proxied = mock(LocalDatabase.class);
    when(proxied.getName()).thenReturn(DB_NAME);
    final ArcadeDBServer server = TestServerHelper.unstartedServer();
    return new RaftReplicatedDatabase(server, proxied, raft);
  }

  private static FakeRaftHAServer node(final boolean leader, final boolean removed) {
    return FakeRaftHAServer.detached().leader(leader).returns("isRemovedFromConfiguration", removed)
        .returns("getTrustedAppliedIndex", APPLIED);
  }

  @Test
  void aRemovedFollowerRefusesAReadYourWritesBookmarkAheadOfIt() {
    final FakeRaftHAServer raft = node(false, true);
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, APPLIED + 1);

    assertThatThrownBy(() -> databaseWith(raft).query("sql", QUERY)).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("not a member").hasMessageContaining("READ_YOUR_WRITES");
    assertThat(raft.calls("waitForAppliedIndex")).isEmpty();
  }

  @Test
  void aRemovedFollowerServesAReadYourWritesBookmarkItAlreadyApplied() {
    final FakeRaftHAServer raft = node(false, true);
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, APPLIED);

    assertThatNoException().isThrownBy(() -> databaseWith(raft).query("sql", QUERY));
    // Already satisfied: membership is never consulted.
    assertThat(raft.calls("isRemovedFromConfiguration")).isEmpty();
    assertThat(raft.calls("waitForAppliedIndex")).containsExactly(List.of(DB_NAME, APPLIED, false));
  }

  @Test
  void aMemberFollowerStillWaitsForAReadYourWritesBookmarkAheadOfIt() {
    final FakeRaftHAServer raft = node(false, false);
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, APPLIED + 1);

    assertThatNoException().isThrownBy(() -> databaseWith(raft).query("sql", QUERY));
    assertThat(raft.calls("waitForAppliedIndex")).containsExactly(List.of(DB_NAME, APPLIED + 1, false));
  }

  @Test
  void aLeaderNeverWaitsNorChecksMembershipForReadYourWrites() {
    final FakeRaftHAServer raft = node(true, true);
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, APPLIED + 1);

    assertThatNoException().isThrownBy(() -> databaseWith(raft).query("sql", QUERY));
    assertThat(raft.calls("isRemovedFromConfiguration")).isEmpty();
    assertThat(raft.calls("waitForAppliedIndex")).isEmpty();
  }

  @Test
  void aRemovedNodeRefusesALinearizableReadBeforeAskingForAReadIndex() {
    for (final boolean leader : new boolean[] { false, true }) {
      final FakeRaftHAServer raft = node(leader, true);
      RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.LINEARIZABLE, -1L);

      assertThatThrownBy(() -> databaseWith(raft).query("sql", QUERY)).as("leader=%s", leader)
          .isInstanceOf(NeedRetryException.class).hasMessageContaining("not a member").hasMessageContaining("LINEARIZABLE");
      assertThat(raft.calls("ensureLinearizableRead")).isEmpty();
      assertThat(raft.calls("ensureLinearizableFollowerRead")).isEmpty();
    }
  }

  @Test
  void aMemberRunsTheLinearizableBarrierOfItsRole() {
    final FakeRaftHAServer follower = node(false, false);
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.LINEARIZABLE, -1L);
    assertThatNoException().isThrownBy(() -> databaseWith(follower).query("sql", QUERY));
    assertThat(follower.calls("ensureLinearizableFollowerRead")).containsExactly(List.of(DB_NAME));

    final FakeRaftHAServer leader = node(true, false);
    assertThatNoException().isThrownBy(() -> databaseWith(leader).query("sql", QUERY));
    assertThat(leader.calls("ensureLinearizableRead")).containsExactly(List.of(DB_NAME));
  }

  @Test
  void anUnknownConfigurationFailsOpen() {
    // Unanswered, a detached server cannot read its configuration and so never refuses (the #9510 fail-open rule).
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(false).returns("getTrustedAppliedIndex", APPLIED);
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.LINEARIZABLE, -1L);

    assertThatNoException().isThrownBy(() -> databaseWith(raft).query("sql", QUERY));
    assertThat(raft.calls("isRemovedFromConfiguration")).hasSize(1);
    assertThat(raft.calls("ensureLinearizableFollowerRead")).containsExactly(List.of(DB_NAME));
  }

  @Test
  void anEventualReadIsServedOnARemovedNode() {
    final FakeRaftHAServer raft = node(false, true);
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.EVENTUAL, APPLIED + 1);

    assertThatNoException().isThrownBy(() -> databaseWith(raft).query("sql", QUERY));
    assertThat(raft.calls("isRemovedFromConfiguration")).isEmpty();
  }
}
