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
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    return new RaftReplicatedDatabase(server, proxied, raft);
  }

  @Test
  void waitsForTheCommittedIndexToBeLocallyAppliedBeforeReturning() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    when(raft.getTransactionBroker()).thenReturn(broker);
    when(broker.replicateDropDatabase(DB_NAME)).thenReturn(COMMITTED_LOG_INDEX);

    databaseWith(raft).dropInReplicas();

    verify(broker).replicateDropDatabase(DB_NAME);
    // throwOnTimeout = true: a caller that cannot confirm the local delete finished must be told, not left
    // to assume it did because nothing complained.
    verify(raft).waitForAppliedIndex(DB_NAME, COMMITTED_LOG_INDEX, true);
  }

  /**
   * The wait must happen AFTER the entry commits, using THAT entry's own index - waiting on a stale or
   * default index would not prove anything about this drop specifically.
   */
  @Test
  void theWaitRunsAfterReplicationUsingTheCommittedEntrysOwnIndex() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    when(raft.getTransactionBroker()).thenReturn(broker);
    when(broker.replicateDropDatabase(DB_NAME)).thenReturn(COMMITTED_LOG_INDEX);

    databaseWith(raft).dropInReplicas();

    final InOrder order = inOrder(broker, raft);
    order.verify(broker).replicateDropDatabase(DB_NAME);
    order.verify(raft).waitForAppliedIndex(DB_NAME, COMMITTED_LOG_INDEX, true);
  }

  /**
   * A timeout proving the local apply cannot be confirmed must surface as a failure, not as a silent success -
   * the caller's maintenance slot is released either way, so this is the only signal it gets that the drop's
   * local effect is unconfirmed.
   */
  @Test
  void aTimeoutConfirmingTheLocalApplyThrowsRatherThanReturningSilently() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    when(raft.getTransactionBroker()).thenReturn(broker);
    when(broker.replicateDropDatabase(DB_NAME)).thenReturn(COMMITTED_LOG_INDEX);
    doThrow(new ReplicationException("local apply did not catch up in time"))
        .when(raft).waitForAppliedIndex(DB_NAME, COMMITTED_LOG_INDEX, true);

    assertThatThrownBy(() -> databaseWith(raft).dropInReplicas())
        .isInstanceOf(TransactionException.class)
        .hasCauseInstanceOf(ReplicationException.class);
  }

  /** A failure to even replicate the entry must not reach the wait at all - there is no index to wait on. */
  @Test
  void aReplicationFailureNeverReachesTheApplyWait() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    when(raft.getTransactionBroker()).thenReturn(broker);
    doThrow(new ReplicationException("quorum not reached")).when(broker).replicateDropDatabase(DB_NAME);

    assertThatThrownBy(() -> databaseWith(raft).dropInReplicas())
        .isInstanceOf(TransactionException.class)
        .hasCauseInstanceOf(ReplicationException.class);

    verify(raft, never()).waitForAppliedIndex(anyString(), anyLong(), anyBoolean());
  }
}
