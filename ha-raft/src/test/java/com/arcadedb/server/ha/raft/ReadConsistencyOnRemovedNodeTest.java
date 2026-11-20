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
import com.arcadedb.utility.StallAwareStopwatch;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;

/**
 * Read-consistency gate and read waits on a node that is no longer a cluster member.
 */
class ReadConsistencyOnRemovedNodeTest {
  // ---- #9590 ----
  // Issue #9590: the branch matrix of {@code RaftReplicatedDatabase.waitForReadConsistency()} on a node removed from the
  // Raft configuration, in the regular lane. {@link HaRemovedNodeIT} drives the same gate
  // against real Ratis.

  private static final String DB_NAME = "readgate9590";
  private static final long   APPLIED = 100L;
  private static final String QUERY   = "SELECT FROM V";

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

  // Regression for #9590: a removed follower refuses A read your writes bookmark ahead of it
  @Test
  void aRemovedFollowerRefusesAReadYourWritesBookmarkAheadOfIt() {
    final FakeRaftHAServer raft = node(false, true);
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, APPLIED + 1);

    assertThatThrownBy(() -> databaseWith(raft).query("sql", QUERY)).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("not a member").hasMessageContaining("READ_YOUR_WRITES");
    assertThat(raft.calls("waitForAppliedIndexForRead")).isEmpty();
  }

  // Regression for #9590: a removed follower serves A read your writes bookmark it already applied
  @Test
  void aRemovedFollowerServesAReadYourWritesBookmarkItAlreadyApplied() {
    final FakeRaftHAServer raft = node(false, true);
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, APPLIED);

    assertThatNoException().isThrownBy(() -> databaseWith(raft).query("sql", QUERY));
    // Already satisfied: neither membership nor the wait is consulted.
    assertThat(raft.calls("isRemovedFromConfiguration")).isEmpty();
    assertThat(raft.calls("waitForAppliedIndexForRead")).isEmpty();
  }

  // Regression for #9590: a follower removed while waiting refuses instead of serving data missing the write
  @Test
  void aFollowerRemovedWhileWaitingRefusesInsteadOfServingDataMissingTheWrite() {
    // A member when the read arrives, removed during the wait, which then gives up short of the bookmark.
    final FakeRaftHAServer raft = node(false, false);
    raft.on("waitForAppliedIndexForRead", args -> {
      raft.returns("isRemovedFromConfiguration", true);
      return null;
    });
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, APPLIED + 1);

    assertThatThrownBy(() -> databaseWith(raft).query("sql", QUERY)).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("not a member").hasMessageContaining("READ_YOUR_WRITES");
    assertThat(raft.calls("waitForAppliedIndexForRead")).containsExactly(List.of(DB_NAME, APPLIED + 1, Database.READ_CONSISTENCY.READ_YOUR_WRITES));
  }

  // Regression for #9590: a follower removed after its wait reached the bookmark serves the read
  @Test
  void aFollowerRemovedAfterItsWaitReachedTheBookmarkServesTheRead() {
    final FakeRaftHAServer raft = node(false, false);
    raft.on("waitForAppliedIndexForRead", args -> {
      raft.returns("isRemovedFromConfiguration", true).returns("getTrustedAppliedIndex", APPLIED + 1);
      return null;
    });
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, APPLIED + 1);

    assertThatNoException().isThrownBy(() -> databaseWith(raft).query("sql", QUERY));
  }

  // Regression for #9590: a member follower still waits for A read your writes bookmark ahead of it
  @Test
  void aMemberFollowerStillWaitsForAReadYourWritesBookmarkAheadOfIt() {
    final FakeRaftHAServer raft = node(false, false);
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, APPLIED + 1);

    assertThatNoException().isThrownBy(() -> databaseWith(raft).query("sql", QUERY));
    assertThat(raft.calls("waitForAppliedIndexForRead")).containsExactly(List.of(DB_NAME, APPLIED + 1, Database.READ_CONSISTENCY.READ_YOUR_WRITES));
  }

  // Regression for #9590: a leader never waits nor checks membership for read your writes
  @Test
  void aLeaderNeverWaitsNorChecksMembershipForReadYourWrites() {
    final FakeRaftHAServer raft = node(true, true);
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, APPLIED + 1);

    assertThatNoException().isThrownBy(() -> databaseWith(raft).query("sql", QUERY));
    assertThat(raft.calls("isRemovedFromConfiguration")).isEmpty();
    assertThat(raft.calls("waitForAppliedIndexForRead")).isEmpty();
  }

  // Regression for #9590: a removed node refuses A linearizable read before asking for A read index
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

  // Regression for #9590: a member runs the linearizable barrier of its role
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

  // Regression for #9590: an unknown configuration fails open
  @Test
  void anUnknownConfigurationFailsOpen() {
    // Unanswered, a detached server cannot read its configuration and so never refuses (the #9510 fail-open rule).
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(false).returns("getTrustedAppliedIndex", APPLIED);
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.LINEARIZABLE, -1L);

    assertThatNoException().isThrownBy(() -> databaseWith(raft).query("sql", QUERY));
    assertThat(raft.calls("isRemovedFromConfiguration")).hasSize(1);
    assertThat(raft.calls("ensureLinearizableFollowerRead")).containsExactly(List.of(DB_NAME));
  }

  // Regression for #9590: an eventual read is served on A removed node
  @Test
  void anEventualReadIsServedOnARemovedNode() {
    final FakeRaftHAServer raft = node(false, true);
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.EVENTUAL, APPLIED + 1);

    assertThatNoException().isThrownBy(() -> databaseWith(raft).query("sql", QUERY));
    assertThat(raft.calls("isRemovedFromConfiguration")).isEmpty();
  }
  // ---- #9606 ----
  // Issue #9606: a consistent read whose apply wait began while its node was still a member of the Raft configuration
  // blocked the whole quorum timeout (10 s by default) after the node was removed, since a removed node receives no more
  // entries and never reaches the index. The read's wait now gives up as soon as it learns of the removal: at once when
  // the node is told of a configuration change, and within one recheck interval when it is not (a removed node is usually
  // cut off before it applies its own removal). The commit-path wait, whose entry is already committed cluster-wide, is
  // not refused: a retryable exception there would invite a duplicate retry of a durable write.

  private static final String DB_NAME9606 = "readwait9606";

  // Regression for #9606: a read your writes wait is refused as soon as the node is told of its removal
  @Test
  void aReadYourWritesWaitIsRefusedAsSoonAsTheNodeIsToldOfItsRemoval() throws Exception {
    final FakeRaftHAServer raft = member();
    final CompletableFuture<Throwable> read = waitInBackground(raft, Database.READ_CONSISTENCY.READ_YOUR_WRITES);
    awaitWaiting(raft);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    raft.returns("isRemovedFromConfiguration", true);
    raft.notifyMembershipChanged();

    assertRefused(read.get(30, TimeUnit.SECONDS), Database.READ_CONSISTENCY.READ_YOUR_WRITES);
    // Well under the one-second recheck the next test relies on: the notification is what woke it
    stopwatch.assertGaveUpWithin(900, "a wait woken by the configuration change vs one left to the periodic recheck");
  }

  // Regression for #9606: a linearizable wait is refused within one recheck when nothing tells the node
  @Test
  void aLinearizableWaitIsRefusedWithinOneRecheckWhenNothingTellsTheNode() throws Exception {
    final FakeRaftHAServer raft = member();
    final CompletableFuture<Throwable> read = waitInBackground(raft, Database.READ_CONSISTENCY.LINEARIZABLE);
    awaitWaiting(raft);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    raft.returns("isRemovedFromConfiguration", true);

    // A NeedRetryException rather than the ReplicationException of a timeout: a member of the cluster can serve it
    assertRefused(read.get(30, TimeUnit.SECONDS), Database.READ_CONSISTENCY.LINEARIZABLE);
    stopwatch.assertGaveUpWithin(5_000, "a wait that rechecks the membership every second vs the 10 s quorum timeout");
  }

  // Regression for #9606: a read on A removed node is refused before it waits
  @Test
  void aReadOnARemovedNodeIsRefusedBeforeItWaits() {
    final FakeRaftHAServer raft = member().returns("isRemovedFromConfiguration", true);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertRefused(waitFor(raft, Database.READ_CONSISTENCY.READ_YOUR_WRITES), Database.READ_CONSISTENCY.READ_YOUR_WRITES);
    stopwatch.assertGaveUpWithin(900, "a refusal on entry vs a wait of any length");
  }

  // Regression for #9606: a read whose index is already applied never asks for the membership
  @Test
  void aReadWhoseIndexIsAlreadyAppliedNeverAsksForTheMembership() {
    final FakeRaftHAServer raft = member().returns("isRemovedFromConfiguration", true);

    raft.awaitAppliedIndex(DB_NAME9606, APPLIED, false, Database.READ_CONSISTENCY.READ_YOUR_WRITES);
    assertThat(raft.calls("isRemovedFromConfiguration")).isEmpty();
  }

  // Regression for #9606: the commit path wait is not refused on removal
  /** The commit-path wait: its entry is committed cluster-wide, so it waits for the apply and never asks for membership. */
  @Test
  void theCommitPathWaitIsNotRefusedOnRemoval() throws Exception {
    final AtomicLong applied = new AtomicLong(APPLIED);
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(false).returns("isRemovedFromConfiguration", true)
        .on("getTrustedAppliedIndex", args -> applied.get());
    final CompletableFuture<Throwable> commit = CompletableFuture.supplyAsync(() -> {
      try {
        raft.awaitAppliedIndex(DB_NAME9606, APPLIED + 1, true, null);
        return null;
      } catch (final Throwable t) {
        return t;
      }
    });
    await().atMost(30, TimeUnit.SECONDS).until(() -> raft.calls("getTrustedAppliedIndex").size() >= 1);
    raft.notifyMembershipChanged();

    applied.set(APPLIED + 1);
    assertThat(commit.get(30, TimeUnit.SECONDS)).isNull();
    assertThat(raft.calls("isRemovedFromConfiguration")).isEmpty();
  }

  private static FakeRaftHAServer member() {
    return FakeRaftHAServer.detached().leader(false).returns("isRemovedFromConfiguration", false)
        .returns("getTrustedAppliedIndex", APPLIED);
  }

  private static CompletableFuture<Throwable> waitInBackground(final FakeRaftHAServer raft,
      final Database.READ_CONSISTENCY consistency) {
    return CompletableFuture.supplyAsync(() -> waitFor(raft, consistency));
  }

  private static Throwable waitFor(final FakeRaftHAServer raft, final Database.READ_CONSISTENCY consistency) {
    try {
      raft.awaitAppliedIndex(DB_NAME9606, APPLIED + 1, consistency == Database.READ_CONSISTENCY.LINEARIZABLE, consistency);
      return null;
    } catch (final Throwable t) {
      return t;
    }
  }

  /** The read asked for the membership on entry, so it is now parked on the apply notifier. */
  private static void awaitWaiting(final FakeRaftHAServer raft) {
    await().atMost(30, TimeUnit.SECONDS).until(() -> raft.calls("isRemovedFromConfiguration").size() >= 1);
  }

  private static void assertRefused(final Throwable failure, final Database.READ_CONSISTENCY consistency) {
    assertThat(failure).isInstanceOf(NeedRetryException.class).hasMessageContaining("not a member")
        .hasMessageContaining(consistency.name()).hasMessageContaining(DB_NAME9606);
  }
}
