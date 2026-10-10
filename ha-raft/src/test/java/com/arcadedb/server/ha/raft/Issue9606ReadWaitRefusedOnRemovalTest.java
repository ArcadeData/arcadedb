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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9606: a consistent read whose apply wait began while its node was still a member of the Raft configuration
 * blocked the whole quorum timeout (10 s by default) after the node was removed, since a removed node receives no more
 * entries and never reaches the index. The read's wait now gives up as soon as it learns of the removal: at once when
 * the node is told of a configuration change, and within one recheck interval when it is not (a removed node is usually
 * cut off before it applies its own removal). The commit-path wait, whose entry is already committed cluster-wide, is
 * not refused: a retryable exception there would invite a duplicate retry of a durable write.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9606ReadWaitRefusedOnRemovalTest {

  private static final String DB_NAME = "readwait9606";
  private static final long   APPLIED = 100L;

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

  @Test
  void aReadOnARemovedNodeIsRefusedBeforeItWaits() {
    final FakeRaftHAServer raft = member().returns("isRemovedFromConfiguration", true);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertRefused(waitFor(raft, Database.READ_CONSISTENCY.READ_YOUR_WRITES), Database.READ_CONSISTENCY.READ_YOUR_WRITES);
    stopwatch.assertGaveUpWithin(900, "a refusal on entry vs a wait of any length");
  }

  @Test
  void aReadWhoseIndexIsAlreadyAppliedNeverAsksForTheMembership() {
    final FakeRaftHAServer raft = member().returns("isRemovedFromConfiguration", true);

    raft.awaitAppliedIndex(DB_NAME, APPLIED, false, Database.READ_CONSISTENCY.READ_YOUR_WRITES);
    assertThat(raft.calls("isRemovedFromConfiguration")).isEmpty();
  }

  /** The commit-path wait: its entry is committed cluster-wide, so it waits for the apply and never asks for membership. */
  @Test
  void theCommitPathWaitIsNotRefusedOnRemoval() throws Exception {
    final AtomicLong applied = new AtomicLong(APPLIED);
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(false).returns("isRemovedFromConfiguration", true)
        .on("getTrustedAppliedIndex", args -> applied.get());
    final CompletableFuture<Throwable> commit = CompletableFuture.supplyAsync(() -> {
      try {
        raft.awaitAppliedIndex(DB_NAME, APPLIED + 1, true, null);
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
      raft.awaitAppliedIndex(DB_NAME, APPLIED + 1, consistency == Database.READ_CONSISTENCY.LINEARIZABLE, consistency);
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
        .hasMessageContaining(consistency.name()).hasMessageContaining(DB_NAME);
  }
}
