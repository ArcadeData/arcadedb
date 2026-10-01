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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8481: {@link MajorityCommittedAllFailedException} and {@link ReplicationDispatchedTimeoutException} extended
 * {@code QuorumNotReachedException}, and therefore {@link NeedRetryException}, although both are raised AFTER the entry
 * was dispatched to Raft: the first once the MAJORITY committed it (the leader completes its local commit before
 * rethrowing it), the second when the outcome is unknown and the entry may still commit. Every retry contract read them
 * as "nothing happened, retry" - {@code Database.transaction(block, joinTx, retries)} re-ran the block, the HTTP layer
 * answered 503, and the Java remote client resent the request - so a committed write ran a second time.
 * <p>
 * The end-to-end paths (the leader-local retry loop against a real cluster, and the HTTP answer read by the remote
 * client) are driven by {@link Issue8481CommittedWriteIsNotRetriedIT}; this class pins the contract each of them keys
 * on without a cluster.
 */
class Issue8481CommittedReplicationOutcomeIsNotRetriedTest {

  private static final String PATH = "target/databases/Issue8481CommittedReplicationOutcomeIsNotRetriedTest";

  @AfterEach
  void cleanup() {
    FileUtils.deleteRecursively(new File(PATH));
  }

  @Test
  void aMajorityCommittedOutcomeIsACommittedRemotelyOutcomeAndNotRetryable() {
    final MajorityCommittedAllFailedException e = new MajorityCommittedAllFailedException("ALL quorum not reached");

    assertThat(e).isNotInstanceOf(NeedRetryException.class);
    // The type the HTTP layer answers 409 "do not retry" for, and the one the remote client rebuilds.
    assertThat(e).isInstanceOf(TransactionCommittedRemotelyException.class);
  }

  @Test
  void aDispatchedTimeoutIsANonRetryableTransactionFailure() {
    final ReplicationDispatchedTimeoutException e = new ReplicationDispatchedTimeoutException("outcome unknown");

    assertThat(e).isNotInstanceOf(NeedRetryException.class);
    assertThat(e).isInstanceOf(TransactionException.class);
    // Not a committed-remotely outcome either: the entry may NOT have committed, so it must not claim it did.
    assertThat(e).isNotInstanceOf(TransactionCommittedRemotelyException.class);
  }

  /**
   * The retry loop the issue names: {@code database.transaction(block, false, retries)} catches NeedRetryException and
   * runs the block again. On the leader these two surface from {@code commit()} inside that loop.
   */
  @Test
  void theTransactionRetryLoopRunsTheBlockOnceForAMajorityCommittedOutcome() {
    assertBlockRunsOnce(() -> new MajorityCommittedAllFailedException("ALL quorum not reached after MAJORITY commit"),
        MajorityCommittedAllFailedException.class);
  }

  @Test
  void theTransactionRetryLoopRunsTheBlockOnceForADispatchedTimeout() {
    assertBlockRunsOnce(() -> new ReplicationDispatchedTimeoutException("Group commit timed out (entry was dispatched to Raft)"),
        ReplicationDispatchedTimeoutException.class);
  }

  /**
   * A follower forwarding a write rebuilds the leader's answer by class name. The leader now answers the MAJORITY
   * committed outcome 409, and the follower must rebuild it as the committed-remotely type so it answers its own client
   * 409 too, rather than the 500 a plain TransactionException leaves as.
   */
  @Test
  void aForwardedMajorityCommittedOutcomeIsRebuiltAsCommittedRemotely() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(409,
        leaderBody(MajorityCommittedAllFailedException.class.getName(), "ALL quorum not reached"), null);

    assertThat(rebuilt).isInstanceOf(TransactionCommittedRemotelyException.class);
    assertThat(rebuilt).isNotInstanceOf(NeedRetryException.class);
    assertThat(rebuilt.getMessage()).isEqualTo("ALL quorum not reached");
  }

  @Test
  void aForwardedDispatchedTimeoutStaysANonRetryableTransactionFailure() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(500,
        leaderBody(ReplicationDispatchedTimeoutException.class.getName(), "outcome unknown"), null);

    assertThat(rebuilt).isInstanceOf(TransactionException.class);
    assertThat(rebuilt).isNotInstanceOf(NeedRetryException.class);
    assertThat(rebuilt).isNotInstanceOf(TransactionCommittedRemotelyException.class);
  }

  private static void assertBlockRunsOnce(final Supplier<RuntimeException> outcome, final Class<? extends RuntimeException> expected) {
    FileUtils.deleteRecursively(new File(PATH));
    try (final DatabaseFactory factory = new DatabaseFactory(PATH); final Database db = factory.create()) {
      final AtomicInteger attempts = new AtomicInteger();
      assertThatThrownBy(() -> db.transaction(() -> {
        attempts.incrementAndGet();
        throw outcome.get();
      }, false, 3)).isInstanceOf(expected);
      assertThat(attempts.get()).as("a committed or possibly committed write must not be run again").isEqualTo(1);
    }
  }

  private static String leaderBody(final String exceptionClass, final String detail) {
    return "{\"error\":\"Cannot execute command\",\"requestId\":\"r-1\",\"exception\":\"" + exceptionClass + "\",\"detail\":\""
        + detail + "\"}";
  }
}
