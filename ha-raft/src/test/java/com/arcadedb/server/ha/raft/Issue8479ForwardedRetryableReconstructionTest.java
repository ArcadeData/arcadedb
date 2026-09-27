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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ReplicationQueueFullException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8479: a follower that forwarded a write to the leader rebuilt the leader's refusal only when its exception class
 * was an exact key of {@code LEADER_EXCEPTION_FACTORIES}. A {@link ReplicatedPageConflictException} (issue #6965), which
 * the leader answers with 503 as the retryable conflict it is, missed that lookup and came back as a plain
 * {@link TransactionException}: {@code database.transaction(..., retries)} on the follower did not retry it, and the
 * follower's HTTP client got a non-retryable error for a conflict a plain retry resolves.
 * <p>
 * The same exact-name lookup dropped the other refusals the leader's group committer raises BEFORE the entry reaches
 * the Raft log - {@link ReplicationQueueFullException} and a plain {@link QuorumNotReachedException} - so they are
 * rebuilt too. The two {@link QuorumNotReachedException} subtypes that mean the entry did, or may have, reached the log
 * ({@link MajorityCommittedAllFailedException}, {@link ReplicationDispatchedTimeoutException}) are deliberately NOT: a
 * retry of either can apply the write twice, so they stay non-retryable on the follower.
 */
class Issue8479ForwardedRetryableReconstructionTest {

  private static final String CONFLICT_DETAIL = "[6965 db='graph' page=28/25 base=75 cluster=76] Concurrent modification on "
      + "page 28/25 of database 'graph': the transaction was validated against version 75 but the cluster is at version 76. "
      + "Please retry the operation";

  private static String leaderBody(final String exceptionClass, final String detail) {
    return "{\"error\":\"Cannot execute command\",\"requestId\":\"r-1\",\"exception\":\"" + exceptionClass + "\",\"detail\":\""
        + detail + "\"}";
  }

  @Test
  void aPageConflictFromTheLeaderIsRebuiltAsTheRetryablePageConflict() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503,
        leaderBody(ReplicatedPageConflictException.class.getName(), CONFLICT_DETAIL), null);

    assertThat(rebuilt).isInstanceOf(ReplicatedPageConflictException.class);
    assertThat(rebuilt).isInstanceOf(ConcurrentModificationException.class);
    assertThat(rebuilt).isInstanceOf(NeedRetryException.class);
    assertThat(rebuilt.getMessage()).isEqualTo(CONFLICT_DETAIL);

    // The (String) constructor parses the machine-readable header back, so the page and version survive the hop.
    final ReplicatedPageConflictException conflict = (ReplicatedPageConflictException) rebuilt;
    assertThat(conflict.getDatabaseName()).isEqualTo("graph");
    assertThat(conflict.getFileId()).isEqualTo(28);
    assertThat(conflict.getPageNumber()).isEqualTo(25);
    assertThat(conflict.getClusterVersion()).isEqualTo(76);
  }

  /** In production mode the leader may conceal 'detail': the rebuilt conflict is still retryable, only without its page. */
  @Test
  void aPageConflictWithoutItsDetailIsStillRetryable() {
    final String body = "{\"error\":\"Cannot execute command\",\"exception\":\"" + ReplicatedPageConflictException.class.getName()
        + "\"}";

    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503, body, null);

    assertThat(rebuilt).isInstanceOf(ReplicatedPageConflictException.class);
    assertThat(((ReplicatedPageConflictException) rebuilt).getClusterVersion()).isEqualTo(-1);
  }

  @Test
  void aFullReplicationQueueOnTheLeaderIsRebuiltAsRetryable() {
    final String detail = "Replication queue is full (0 remaining of 1024 max). Server is overloaded, retry later";
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503,
        leaderBody(ReplicationQueueFullException.class.getName(), detail), null);

    assertThat(rebuilt).isInstanceOf(ReplicationQueueFullException.class);
    assertThat(rebuilt).isInstanceOf(NeedRetryException.class);
    assertThat(rebuilt.getMessage()).isEqualTo(detail);
  }

  @Test
  void aQuorumNotReachedBeforeDispatchIsRebuiltAsRetryable() {
    final String detail = "Group commit timed out after 20000ms (cancelled before dispatch)";
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503,
        leaderBody(QuorumNotReachedException.class.getName(), detail), null);

    assertThat(rebuilt).isInstanceOf(QuorumNotReachedException.class);
    assertThat(rebuilt).isInstanceOf(NeedRetryException.class);
    assertThat(rebuilt.getMessage()).isEqualTo(detail);
  }

  /**
   * MAJORITY committed the entry: the write is durable cluster-wide. Retrying the forwarded command would run it a second
   * time, so the follower must not rebuild it as a NeedRetryException - even though the leader answers it with 503.
   */
  @Test
  void aMajorityCommittedRefusalStaysNonRetryable() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503,
        leaderBody(MajorityCommittedAllFailedException.class.getName(), "ALL quorum not reached"), null);

    assertThat(rebuilt).isNotInstanceOf(NeedRetryException.class);
    assertThat(rebuilt).isInstanceOf(TransactionException.class);
  }

  /** The entry was dispatched and may still commit: an indeterminate outcome is not safe to retry either. */
  @Test
  void aDispatchedTimeoutStaysNonRetryable() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503,
        leaderBody(ReplicationDispatchedTimeoutException.class.getName(), "Group commit timed out (entry was dispatched to Raft)"),
        null);

    assertThat(rebuilt).isNotInstanceOf(NeedRetryException.class);
    assertThat(rebuilt).isInstanceOf(TransactionException.class);
  }

  /**
   * The caller the issue is about: {@code database.transaction(..., retries)} on the follower. The rebuilt page conflict
   * must make it run the block again, and the rebuilt MAJORITY-committed refusal must make it stop at the first attempt.
   */
  @Test
  void theFollowersTransactionRetryLoopRetriesTheRebuiltConflictOnly() {
    final String path = "target/databases/Issue8479ForwardedRetryableReconstructionTest";
    FileUtils.deleteRecursively(new File(path));
    try (final DatabaseFactory factory = new DatabaseFactory(path); final Database db = factory.create()) {
      final AtomicInteger conflictAttempts = new AtomicInteger();
      db.transaction(() -> {
        if (conflictAttempts.incrementAndGet() == 1)
          throw RaftReplicatedDatabase.reconstructLeaderException(503,
              leaderBody(ReplicatedPageConflictException.class.getName(), CONFLICT_DETAIL), null);
      }, false, 3);
      assertThat(conflictAttempts.get()).isEqualTo(2);

      final AtomicInteger committedAttempts = new AtomicInteger();
      assertThatThrownBy(() -> db.transaction(() -> {
        committedAttempts.incrementAndGet();
        throw RaftReplicatedDatabase.reconstructLeaderException(503,
            leaderBody(MajorityCommittedAllFailedException.class.getName(), "ALL quorum not reached"), null);
      }, false, 3)).isInstanceOf(TransactionException.class);
      assertThat(committedAttempts.get()).isEqualTo(1);
    } finally {
      FileUtils.deleteRecursively(new File(path));
    }
  }
}
