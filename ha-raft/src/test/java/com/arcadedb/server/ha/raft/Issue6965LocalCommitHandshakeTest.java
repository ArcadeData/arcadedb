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

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The committing thread's side of the leader commit since issue #6965: the transaction is registered with the state
 * machine before the entry is dispatched, the apply thread publishes its pages at the entry's log position, and every
 * exit of {@link RaftReplicatedDatabase#replicateAndCommitLocally} either withdraws the registration (and rolls
 * back) or, when the apply thread got there first, completes the commit with the outcome the apply thread recorded.
 * <p>
 * The apply thread is played by the mocked broker: what it does inside {@code replicateTransaction} is what the state
 * machine would have done before the acknowledgement (or the failure) reached the committing thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6965LocalCommitHandshakeTest {
  private static final String DB_NAME  = "issue6965";
  private static final long   WAL_TX_ID = 4242L;

  private final String dbPath = "issue6965-local-commit-handshake-" + UUID.randomUUID();

  private LocalDatabase          proxied;
  private RaftHAServer           raftServer;
  private TransactionContext     tx;
  private ArcadeStateMachine     stateMachine;
  private RaftTransactionBroker  broker;
  private RaftReplicatedDatabase database;
  private RaftReplicatedDatabase.ReplicationPayload payload;

  @BeforeEach
  void setUp() {
    // Subclass mocks for every class the commit path is handed: see SubclassMocks (issue #8021).
    proxied = SubclassMocks.mock(LocalDatabase.class);
    when(proxied.getDatabasePath()).thenReturn(dbPath);
    when(proxied.getName()).thenReturn(DB_NAME);
    when(proxied.getTransactionManager()).thenReturn(SubclassMocks.mock(TransactionManager.class));
    when(proxied.getSchema()).thenReturn(mock(Schema.class, RETURNS_DEEP_STUBS));
    when(proxied.executeInReadLock(any())).thenAnswer(inv -> ((Callable<?>) inv.getArgument(0)).call());

    broker = SubclassMocks.mock(RaftTransactionBroker.class);
    raftServer = SubclassMocks.mock(RaftHAServer.class, RETURNS_DEEP_STUBS);
    when(raftServer.isLeader()).thenReturn(true);
    when(raftServer.getTransactionBroker()).thenReturn(broker);
    when(raftServer.getQuorumTimeout()).thenReturn(1_000L);
    stateMachine = new ArcadeStateMachine();
    when(raftServer.getStateMachine()).thenReturn(stateMachine);

    tx = SubclassMocks.mock(TransactionContext.class);
    DatabaseContext.INSTANCE.init(proxied, tx);
    database = new RaftReplicatedDatabase(null, proxied, raftServer);
    payload = new RaftReplicatedDatabase.ReplicationPayload(tx, null, walTransactionBytes(WAL_TX_ID), Map.of());
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.removeContext(dbPath);
  }

  /**
   * Issue #8021: an inline mock IS an instance of the mocked class, and the Graal JIT can keep code compiled against
   * its unmocked methods; a subclass mock is a generated class. Fails on every JVM if an inline mock creeps back in.
   */
  @Test
  void theCommitPathIsHandedSubclassMocks() {
    assertThat(proxied.getClass()).isNotEqualTo(LocalDatabase.class);
    assertThat(tx.getClass()).isNotEqualTo(TransactionContext.class);
    assertThat(proxied.getTransactionManager().getClass()).isNotEqualTo(TransactionManager.class);
    assertThat(broker.getClass()).isNotEqualTo(RaftTransactionBroker.class);
    assertThat(raftServer.getClass()).isNotEqualTo(RaftHAServer.class);
  }

  /** The common case: the entry is acknowledged after the apply thread published its pages. */
  @Test
  void anAcknowledgedEntryCompletesTheCommitPublishedByTheApplyThread() {
    when(broker.replicateTransaction(anyString(), any(), any())).thenAnswer(inv -> {
      applyThreadPublishes();
      return 7L;
    });

    database.replicateAndCommitLocally(payload, true, stateMachine);

    verify(tx).setRemotelyCommitted(true);
    verify(tx).completeCommit();
    verify(tx, never()).commit2ndPhase(any());
    verify(proxied, never()).rollback();
    assertThat(stateMachine.pendingLocalCommits()).as("the claim consumed the registration").isZero();
  }

  /** The apply thread must be the one publishing the pages: the committing thread never does it itself. */
  @Test
  void theCommittingThreadNeverPublishesWhenAStateMachineIsWired() {
    when(broker.replicateTransaction(anyString(), any(), any())).thenAnswer(inv -> {
      applyThreadPublishes();
      return 7L;
    });

    database.replicateAndCommitLocally(payload, true, stateMachine);

    verify(tx, never()).publishCommittedPages(any());
    verify(tx, never()).commit2ndPhase(any());
  }

  /**
   * Indeterminate outcome BEFORE the apply thread reached the entry (issue #4790): the transaction is withdrawn and
   * rolled back, and the error is surfaced. Should the entry commit regardless, the apply thread will find no claim
   * and apply it from its WAL bytes.
   */
  @Test
  void aTimeoutBeforeTheClaimWithdrawsAndRollsBack() {
    when(broker.replicateTransaction(anyString(), any(), any()))
        .thenThrow(new ReplicationDispatchedTimeoutException("dispatched, outcome unknown"));

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
        .isInstanceOf(ReplicationDispatchedTimeoutException.class);

    verify(proxied).rollback();
    verify(tx, never()).completeCommit();
    assertThat(stateMachine.pendingLocalCommits()).as("the withdrawal removed the registration").isZero();
    assertThat(stateMachine.claimLocalCommit(DB_NAME, WAL_TX_ID, payload.walData())).as("a late apply finds nothing to claim").isNull();
  }

  /**
   * Indeterminate outcome AFTER the apply thread claimed the entry (issue #6848): the entry committed, its pages are
   * published, so the commit completes as a success - the timeout was only late news.
   */
  @Test
  void aTimeoutAfterTheClaimCompletesTheCommit() {
    when(broker.replicateTransaction(anyString(), any(), any())).thenAnswer(inv -> {
      applyThreadPublishes();
      throw new ReplicationDispatchedTimeoutException("dispatched, outcome unknown");
    });

    database.replicateAndCommitLocally(payload, true, stateMachine);

    verify(tx).completeCommit();
    verify(proxied, never()).rollback();
  }

  /** The leader refused the entry before appending it (page-version conflict): definite, retryable, rolled back. */
  @Test
  void aRefusedEntryIsRolledBackAndTheConflictSurfaced() {
    when(broker.replicateTransaction(anyString(), any(), any()))
        .thenThrow(new ConcurrentModificationException("Concurrent modification on page 3/0"));

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
        .isInstanceOf(ConcurrentModificationException.class)
        .hasMessageContaining("page 3/0");

    verify(proxied).rollback();
    verify(tx, never()).completeCommit();
    assertThat(stateMachine.pendingLocalCommits()).isZero();
  }

  /**
   * The apply thread claimed the entry but publishing its pages failed (issue #5064): the transaction is released
   * without touching the record identities the cluster committed, the leader steps down, and the caller learns that
   * the transaction IS committed and must not be retried.
   */
  @Test
  void aFailedPublicationIsSurfacedAsCommittedRemotely() {
    final ConcurrentModificationException cause = new ConcurrentModificationException("simulated phase-2 failure");
    when(broker.replicateTransaction(anyString(), any(), any())).thenAnswer(inv -> {
      final LocalCommit claimed = stateMachine.claimLocalCommit(DB_NAME, WAL_TX_ID, payload.walData());
      assertThat(claimed).isNotNull();
      claimed.failed(cause, true);
      return 7L;
    });

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
        .isInstanceOf(TransactionCommittedRemotelyException.class)
        .hasMessageContaining("committed cluster-wide")
        .hasMessageContaining("reconciled")
        .hasCause(cause);

    verify(tx).setRemotelyCommitted(true);
    verify(tx).concludeFailedPhase2(cause);
    verify(tx, never()).completeCommit();
    verify(proxied, never()).rollback();
    verify(raftServer).stepDown();
  }

  /** MAJORITY committed, ALL watch failed: the local commit completes and the watch failure is still reported. */
  @Test
  void aMajorityCommitWhoseAllWatchFailedCompletesLocallyAndRethrows() {
    when(broker.replicateTransaction(anyString(), any(), any())).thenAnswer(inv -> {
      applyThreadPublishes();
      throw new MajorityCommittedAllFailedException("ALL quorum not reached");
    });

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
        .isInstanceOf(MajorityCommittedAllFailedException.class);

    verify(tx).completeCommit();
    verify(proxied, never()).rollback();
  }

  /**
   * Ratis acknowledges only after the apply, so an acknowledged entry nobody claimed with no live state machine left
   * (here: a Ratis restart replaced the one the commit registered with) means no apply thread will ever run for it: the
   * committing thread must publish itself rather than wait for a publication that will never come.
   */
  @Test
  void anAcknowledgedButUnclaimedEntryIsPublishedByTheCommittingThread() {
    when(broker.replicateTransaction(anyString(), any(), any())).thenReturn(7L);
    stateMachineReplacedByARestart();

    database.replicateAndCommitLocally(payload, true, stateMachine);

    verify(tx).commit2ndPhase(any());
    verify(tx, never()).completeCommit();
    verify(proxied, never()).rollback();
    assertThat(stateMachine.pendingLocalCommits()).as("the withdrawal removed the registration").isZero();
  }

  /**
   * Issue #8781: a leader deposed mid-commit has its entry acknowledged by the NEW leader, after that leader's own apply,
   * while this node's apply thread is alive and behind. The registration is unclaimed at the acknowledgement, but the
   * apply thread will apply the entry from its WAL bytes, so publishing on the committing thread as well would fold the
   * transaction's record delta into the bucket counters twice. The committing thread must wait for the entry's index
   * and only release the transaction - and must not publish even when that wait runs out (the mocked wait returns at
   * once, which is what a timed-out lenient wait does).
   */
  @Test
  void anUnclaimedEntryALiveStateMachineWillApplyIsNeverPublishedByTheCommittingThread() {
    when(broker.replicateTransaction(anyString(), any(), any())).thenReturn(7L);

    database.replicateAndCommitLocally(payload, true, stateMachine);

    verify(raftServer).waitForAppliedIndex(DB_NAME, 7L);
    verify(tx, never()).commit2ndPhase(any());
    verify(tx, never()).publishCommittedPages(any());
    verify(tx).reset();
    verify(proxied, never()).rollback();
    assertThat(stateMachine.pendingLocalCommits()).isZero();
  }

  /** Issue #8781, MAJORITY-committed variant: the exception carries the entry's index, which the committing thread awaits. */
  @Test
  void aMajorityCommittedUnclaimedEntryALiveStateMachineWillApplyIsNeverPublishedByTheCommittingThread() {
    when(broker.replicateTransaction(anyString(), any(), any()))
        .thenThrow(new MajorityCommittedAllFailedException("ALL quorum not reached", null, 7L));

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
        .isInstanceOf(MajorityCommittedAllFailedException.class);

    verify(raftServer).waitForAppliedIndex(DB_NAME, 7L);
    verify(tx, never()).commit2ndPhase(any());
    verify(tx, never()).publishCommittedPages(any());
    verify(tx).reset();
    verify(proxied, never()).rollback();
  }

  /**
   * Issue #8781: a MAJORITY-committed exception that lost its index still leaves the pages to a live state machine, and
   * waits for the local commit index rather than releasing the commit locks at once (#5503).
   */
  @Test
  void aMajorityCommittedUnclaimedEntryWithoutAnIndexIsNeverPublishedWhileTheStateMachineLives() {
    when(broker.replicateTransaction(anyString(), any(), any()))
        .thenThrow(new MajorityCommittedAllFailedException("ALL quorum not reached"));
    when(raftServer.getCommitIndex()).thenReturn(11L);

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
        .isInstanceOf(MajorityCommittedAllFailedException.class);

    verify(raftServer).waitForAppliedIndex(DB_NAME, 11L);
    verify(tx, never()).commit2ndPhase(any());
    verify(tx).reset();
  }

  /** Issue #8781: with neither the entry's index nor a commit index, a replica still never publishes. */
  @Test
  void aReplicaWithoutAnyIndexStillNeverPublishes() {
    when(broker.replicateTransaction(anyString(), any(), any()))
        .thenThrow(new MajorityCommittedAllFailedException("ALL quorum not reached"));
    when(raftServer.getCommitIndex()).thenReturn(-1L);

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, false, null))
        .isInstanceOf(MajorityCommittedAllFailedException.class);

    verify(raftServer, never()).waitForAppliedIndex(anyString(), anyLong());
    verify(tx, never()).commit2ndPhase(any());
    verify(tx).reset();
  }

  /**
   * Issue #8781, replica variant: a forwarded commit the leader answers MAJORITY-committed is rebuilt here from the
   * message, and this replica's state machine applies the entry from the leader's log. Publishing on the committing
   * thread as well folded the record delta twice; the replica only waits for the entry and releases.
   */
  @Test
  void aReplicaNeverPublishesAForwardedMajorityCommittedEntry() {
    when(broker.replicateTransaction(anyString(), any(), any()))
        .thenThrow(new MajorityCommittedAllFailedException("ALL quorum not reached after MAJORITY commit at logIndex=9"));

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, false, null))
        .isInstanceOf(MajorityCommittedAllFailedException.class);

    verify(raftServer).waitForAppliedIndex(DB_NAME, 9L);
    verify(tx, never()).commit2ndPhase(any());
    verify(tx).reset();
  }

  /** The index survives the hop to a follower: the message every leader-side constructor call writes carries it. */
  @Test
  void theLogIndexIsReadBackFromTheMessage() {
    assertThat(new MajorityCommittedAllFailedException("ALL quorum not reached after MAJORITY commit at logIndex=42").getLogIndex())
        .isEqualTo(42L);
    assertThat(new MajorityCommittedAllFailedException("ALL quorum watch failed after MAJORITY commit at logIndex=5: boom",
        new RuntimeException()).getLogIndex()).isEqualTo(5L);
    assertThat(new MajorityCommittedAllFailedException("ALL quorum not reached").getLogIndex()).isEqualTo(-1L);
    // A garbled remote message must not turn the "committed, do not retry" signal into a NumberFormatException.
    assertThat(new MajorityCommittedAllFailedException("at logIndex=99999999999999999999999").getLogIndex()).isEqualTo(-1L);
  }

  /** Issue #8781: a closed state machine applies nothing more, so the committing thread publishes. */
  @Test
  void anUnclaimedEntryIsPublishedByTheCommittingThreadWhenTheStateMachineIsClosed() {
    final ArcadeStateMachine closed = SubclassMocks.spy(stateMachine);
    doReturn(true).when(closed).isClosed();
    when(raftServer.getStateMachine()).thenReturn(closed);
    when(broker.replicateTransaction(anyString(), any(), any())).thenReturn(7L);

    database.replicateAndCommitLocally(payload, true, closed);

    verify(tx).commit2ndPhase(any());
    verify(tx, never()).reset();
  }

  /** Issue #8781: a shutdown in progress stops the state machine applying, so the committing thread publishes. */
  @Test
  void anUnclaimedEntryIsPublishedByTheCommittingThreadWhenAShutdownIsRequested() {
    when(broker.replicateTransaction(anyString(), any(), any())).thenReturn(7L);
    when(raftServer.isShutdownRequested()).thenReturn(true);

    database.replicateAndCommitLocally(payload, true, stateMachine);

    verify(tx).commit2ndPhase(any());
    verify(tx, never()).reset();
  }

  /** MAJORITY committed, ALL watch failed, and no apply thread ever claimed the entry: the committing thread publishes. */
  @Test
  void aMajorityCommitNobodyClaimedIsPublishedByTheCommittingThread() {
    when(broker.replicateTransaction(anyString(), any(), any()))
        .thenThrow(new MajorityCommittedAllFailedException("ALL quorum not reached"));
    stateMachineReplacedByARestart();

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
        .isInstanceOf(MajorityCommittedAllFailedException.class);

    verify(tx).commit2ndPhase(any());
    verify(tx, never()).completeCommit();
    verify(proxied, never()).rollback();
    assertThat(stateMachine.pendingLocalCommits()).isZero();
  }

  /**
   * A publication that failed during MAJORITY-commit recovery stays silent to the caller - who is told about the ALL
   * watch failure - but still releases the transaction and steps this leader down (the rule the retired
   * applyLocallyAfterMajorityCommit pinned).
   */
  @Test
  void aFailedPublicationDuringMajorityRecoveryStaysSilentButStepsDown() {
    final ConcurrentModificationException cause = new ConcurrentModificationException("simulated phase-2 failure");
    when(broker.replicateTransaction(anyString(), any(), any())).thenAnswer(inv -> {
      final LocalCommit claimed = stateMachine.claimLocalCommit(DB_NAME, WAL_TX_ID, payload.walData());
      assertThat(claimed).isNotNull();
      claimed.failed(cause, true);
      throw new MajorityCommittedAllFailedException("ALL quorum not reached");
    });

    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
        .isInstanceOf(MajorityCommittedAllFailedException.class);

    verify(tx).concludeFailedPhase2(cause);
    verify(tx, never()).completeCommit();
    verify(proxied, never()).rollback();
    verify(raftServer).stepDown();
  }

  /** Without a state machine nobody can publish at the log position, so the committing thread does, as before. */
  @Test
  void withoutAStateMachineTheCommittingThreadPublishes() {
    when(broker.replicateTransaction(anyString(), any(), any())).thenReturn(7L);

    database.replicateAndCommitLocally(payload, true, null);

    verify(tx).commit2ndPhase(any());
    verify(proxied, never()).rollback();
  }

  /** A replica registers nothing: the state machine applies its entry from the WAL bytes (#5503). */
  @Test
  void aReplicaRegistersNothing() {
    when(broker.replicateTransaction(anyString(), any(), any())).thenReturn(7L);

    database.replicateAndCommitLocally(payload, false, null);

    assertThat(stateMachine.pendingLocalCommits()).isZero();
    verify(tx, never()).completeCommit();
    verify(tx, never()).commit2ndPhase(any());
    verify(tx).reset();
  }

  /** A Ratis restart builds a new state machine: the one the commit registered with applies nothing more. */
  private void stateMachineReplacedByARestart() {
    when(raftServer.getStateMachine()).thenReturn(new ArcadeStateMachine());
  }

  private void applyThreadPublishes() {
    final LocalCommit claimed = stateMachine.claimLocalCommit(DB_NAME, WAL_TX_ID, payload.walData());
    assertThat(claimed).as("the transaction must be registered before the entry is dispatched").isNotNull();
    claimed.published();
  }

  /** The wire layout {@code ArcadeStateMachine.deserializeWalTransaction} reads: id, timestamp, no pages. */
  private static byte[] walTransactionBytes(final long txId) {
    final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES);
    buf.putLong(txId);
    buf.putLong(System.currentTimeMillis());
    buf.putInt(0);
    buf.putInt(0);
    return buf.array();
  }
}
