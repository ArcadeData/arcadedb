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

import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.CallLog;
import com.arcadedb.server.TestServerHelper;
import org.junit.jupiter.api.Test;

import java.util.List;


import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/**
 * Issue #7641 (review of PR #7649): {@code RaftReplicatedDatabase.dropInReplicas()} used to return as soon as the
 * drop-database entry was Raft-COMMITTED, which {@code RaftGroupCommitter.submitAndWait}'s own javadoc is explicit
 * is not the same as APPLIED - the state machine applies entries asynchronously on every node, leader included, so
 * the directory could still be mid-delete on this node when the call returned. {@code ServerControlPlane.dropDatabase}
 * releases the {@link com.arcadedb.engine.MaintenanceCoordinator.Operation#DROP} slot in a {@code finally} right
 * after {@code dropInReplicas()} returns, so a backup, restore, import or export admitted into the now-free slot
 * could start touching a directory the apply thread had not finished deleting - the same async-apply gap issue
 * #5503 closed for transaction commits, on the drop path this time.
 * <p>
 * {@code dropInReplicas()} now waits for its own committed index to be locally applied before returning, with
 * {@code throwOnTimeout = true}: since the caller's {@code finally} releases the slot regardless of the outcome
 * here, a caller that cannot confirm the local delete finished must be told so loudly rather than have the slot
 * released as if the drop had definitely completed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7641DropInReplicasWaitsForLocalApplyTest {

  private static final String DB_NAME       = "dropinreplicas7641";
  private static final long   COMMITTED_LOG_INDEX = 42L;

  private static RaftReplicatedDatabase databaseWith(final RaftHAServer raft) {
    final LocalDatabase proxied = mock(LocalDatabase.class);
    when(proxied.getName()).thenReturn(DB_NAME);
    final ArcadeDBServer server = TestServerHelper.unstartedServer();
    return new RaftReplicatedDatabase(server, proxied, raft);
  }

  @Test
  void waitsForTheCommittedIndexToBeLocallyAppliedBeforeReturning() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.localDropVerbs(new LocalDropVerbs());
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    raft.transactionBroker(broker);
    broker.returns("replicateDropDatabase", COMMITTED_LOG_INDEX);

    databaseWith(raft).dropInReplicas();

    assertThat(broker.calls("replicateDropDatabase")).containsOnlyOnce(List.of(DB_NAME));
    // throwOnTimeout = true: a caller that cannot confirm the local delete finished must be told, not left
    // to assume it did because nothing complained.
    assertThat(raft.calls("waitForAppliedIndex")).containsOnlyOnce(List.of(DB_NAME, COMMITTED_LOG_INDEX, true));
  }

  /**
   * The wait must happen AFTER the entry commits, using THAT entry's own index - waiting on a stale or
   * default index would not prove anything about this drop specifically.
   */
  @Test
  void theWaitRunsAfterReplicationUsingTheCommittedEntrysOwnIndex() {
    // One log for both fakes, so the order of their calls is observable
    final CallLog log = new CallLog();
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().recordingOn(log);
    raft.localDropVerbs(new LocalDropVerbs());
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker(log);
    raft.transactionBroker(broker);
    broker.returns("replicateDropDatabase", COMMITTED_LOG_INDEX);

    databaseWith(raft).dropInReplicas();

    // Relative order only: other recorded calls (the #9510 membership check) may come before the two that matter here.
    assertThat(log.methods()).containsSubsequence("replicateDropDatabase", "waitForAppliedIndex")
        .containsOnlyOnce("replicateDropDatabase", "waitForAppliedIndex");
    assertThat(raft.calls("waitForAppliedIndex")).containsExactly(List.of(DB_NAME, COMMITTED_LOG_INDEX, true));
  }

  /**
   * A timeout proving the local apply cannot be confirmed must surface as a failure, not as a silent success -
   * the caller's maintenance slot is released either way, so this is the only signal it gets that the drop's
   * local effect is unconfirmed.
   */
  @Test
  void aTimeoutConfirmingTheLocalApplyThrowsRatherThanReturningSilently() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.localDropVerbs(new LocalDropVerbs());
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    raft.transactionBroker(broker);
    broker.returns("replicateDropDatabase", COMMITTED_LOG_INDEX);
    raft.fails("waitForAppliedIndex", new ReplicationException("local apply did not catch up in time"));

    assertThatThrownBy(() -> databaseWith(raft).dropInReplicas())
        .isInstanceOf(TransactionException.class)
        .hasCauseInstanceOf(ReplicationException.class);
  }

  /** A failure to even replicate the entry must not reach the wait at all - there is no index to wait on. */
  @Test
  void aReplicationFailureNeverReachesTheApplyWait() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.localDropVerbs(new LocalDropVerbs());
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    raft.transactionBroker(broker);
    broker.fails("replicateDropDatabase", new ReplicationException("quorum not reached"));

    assertThatThrownBy(() -> databaseWith(raft).dropInReplicas())
        .isInstanceOf(TransactionException.class)
        .hasCauseInstanceOf(ReplicationException.class);

    assertThat(raft.calls("waitForAppliedIndex")).isEmpty();
  }

  /**
   * Issue #8035: the local apply of the drop must know this node's verb holds the maintenance slot for it, so the
   * verb is registered for the whole wait - and withdrawn afterwards, so a later peer-style apply reserves the slot.
   */
  @Test
  void theVerbIsRegisteredAsAwaitingTheDropForTheLengthOfTheWait() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    final LocalDropVerbs verbs = new LocalDropVerbs();
    raft.localDropVerbs(verbs);
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    raft.transactionBroker(broker);
    broker.on("replicateDropDatabase", args -> {
      assertThat(verbs.isAwaiting(DB_NAME)).as("registered before the entry is submitted").isTrue();
      return COMMITTED_LOG_INDEX;
    });
    final boolean[] registeredDuringWait = new boolean[1];
    raft.on("waitForAppliedIndex", args -> {
      registeredDuringWait[0] = verbs.isAwaiting(DB_NAME);
      return null;
    });

    databaseWith(raft).dropInReplicas();

    assertThat(registeredDuringWait[0]).isTrue();
    assertThat(verbs.isAwaiting(DB_NAME)).isFalse();
  }

  /** A failed drop withdraws the registration too. */
  @Test
  void aFailedDropWithdrawsTheRegistration() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    final LocalDropVerbs verbs = new LocalDropVerbs();
    raft.localDropVerbs(verbs);
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    raft.transactionBroker(broker);
    broker.returns("replicateDropDatabase", COMMITTED_LOG_INDEX);
    raft.fails("waitForAppliedIndex", new ReplicationException("local apply did not catch up in time"));

    assertThatThrownBy(() -> databaseWith(raft).dropInReplicas()).isInstanceOf(TransactionException.class);
    assertThat(verbs.isAwaiting(DB_NAME)).isFalse();
  }
}
