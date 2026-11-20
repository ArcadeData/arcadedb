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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.schema.Schema;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.SubclassMocks;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The committing thread's side of the leader commit: the handshake with the state machine, the position at which a commit is
 * published, the validation before the append, and the messages and warnings of the local phase 2.
 */
class LocalCommitHandshakeTest {

  /**
   * The committing thread's side of the leader commit since issue #6965: the transaction is registered with the state
   * machine before the entry is dispatched, the apply thread publishes its pages at the entry's log position, and every
   * exit of {@link RaftReplicatedDatabase#replicateAndCommitLocally} either withdraws the registration (and rolls
   * back) or, when the apply thread got there first, completes the commit with the outcome the apply thread recorded.
   * <p>
   * The apply thread is played by the mocked broker: what it does inside {@code replicateTransaction} is what the state
   * machine would have done before the acknowledgement (or the failure) reached the committing thread.

   */
  @Nested
  class LocalCommitHandshake {
    private static final String DB_NAME  = "issue6965";
    private static final long   WAL_TX_ID = 4242L;

    private final String dbPath = "issue6965-local-commit-handshake-" + UUID.randomUUID();

    private LocalDatabase             proxied;
    private FakeRaftHAServer          raftServer;
    private TransactionContext        tx;
    private ArcadeStateMachine        stateMachine;
    private FakeRaftTransactionBroker broker;
    private RaftReplicatedDatabase    database;
    private RaftReplicatedDatabase.ReplicationPayload payload;

    @BeforeEach
    void setUp() {
      // Subclass mocks for every class the commit path is handed: see SubclassMocks (issue #8021).
      proxied = SubclassMocks.mock(LocalDatabase.class);
      when(proxied.getDatabasePath()).thenReturn(dbPath);
      when(proxied.getName()).thenReturn(DB_NAME);
      when(proxied.getTransactionManager()).thenReturn(SubclassMocks.mock(TransactionManager.class));
      // An interface mock is already a generated class, so the inline maker's retransformation never applies to it.
      when(proxied.getSchema()).thenReturn(mock(Schema.class, RETURNS_DEEP_STUBS));
      when(proxied.executeInReadLock(any())).thenAnswer(inv -> ((Callable<?>) inv.getArgument(0)).call());

      broker = new FakeRaftTransactionBroker();
      raftServer = FakeRaftHAServer.detached();
      raftServer.leader(true);
      raftServer.transactionBroker(broker);
      raftServer.quorumTimeout(1_000L);
      // No peer advertised the tx-prepared-at section: commits take the three-argument form, as the mocked server answered
      raftServer.txPreparedAtCapable(false);
      stateMachine = new ArcadeStateMachine();
      raftServer.stateMachine(stateMachine);

      tx = SubclassMocks.mock(TransactionContext.class);
      DatabaseContext.INSTANCE.init(proxied, tx);
      database = new RaftReplicatedDatabase(null, proxied, raftServer);
      payload = new RaftReplicatedDatabase.ReplicationPayload(tx, null, walTransactionBytes(WAL_TX_ID), Map.of());
    }

    @AfterEach
    void tearDown() {
      // A commit records the index it was acknowledged at on its thread (issue #11742): leave none for the next test
      database.clearReadConsistencyContext();
      DatabaseContext.INSTANCE.removeContext(dbPath);
    }

    /**
     * Issue #11742: the bookmark of the response covers the entry the cluster acknowledged the write at, even though this node
     * has not applied it (here the state machine and Ratis are both at index 3 while the entry is 7): a read that waited for the
     * applied index alone would not see the write.
     */
    // Regression for #11742: AcknowledgedWriteBookmark
    @Test
    void theBookmarkOfACommitCoversTheIndexTheClusterAcknowledgedItAt() {
      raftServer.lastAppliedIndex(3L);
      stateMachine.lastAppliedIndex.set(3L);
      broker.on("replicateTransaction", args -> 7L);

      database.replicateAndCommitLocally(payload, true, stateMachine);

      assertThat(database.getCommitIndexBookmark()).as("the entry the write was acknowledged at, not the applied index").isEqualTo(7L);
    }

    /**
     * Issue #8021: an inline mock IS an instance of the mocked class, and the Graal JIT can keep code compiled against
     * its unmocked methods; a subclass mock is a generated class. Fails on every JVM if an inline mock creeps back in.
     */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void theCommitPathIsHandedSubclassMocks() {
      assertThat(proxied.getClass()).isNotEqualTo(LocalDatabase.class);
      assertThat(tx.getClass()).isNotEqualTo(TransactionContext.class);
      assertThat(proxied.getTransactionManager().getClass()).isNotEqualTo(TransactionManager.class);
    }

    /** The common case: the entry is acknowledged after the apply thread published its pages. */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void anAcknowledgedEntryCompletesTheCommitPublishedByTheApplyThread() {
      broker.on("replicateTransaction", args -> {
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
    // Regression for #6965: LocalCommitHandshake
    @Test
    void theCommittingThreadNeverPublishesWhenAStateMachineIsWired() {
      broker.on("replicateTransaction", args -> {
        applyThreadPublishes();
        return 7L;
      });

      database.replicateAndCommitLocally(payload, true, stateMachine);

      verify(tx, never()).publishCommittedPages(any());
      verify(tx, never()).publishReplicatedCommittedPages(any());
      verify(tx, never()).commit2ndPhase(any());
    }

    /**
     * Indeterminate outcome BEFORE the apply thread reached the entry (issue #4790): the transaction is withdrawn and
     * rolled back, and the error is surfaced. Should the entry commit regardless, the apply thread will find no claim
     * and apply it from its WAL bytes.
     */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void aTimeoutBeforeTheClaimWithdrawsAndRollsBack() {
      broker.fails("replicateTransaction", new ReplicationDispatchedTimeoutException("dispatched, outcome unknown"));

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
    // Regression for #6965: LocalCommitHandshake
    @Test
    void aTimeoutAfterTheClaimCompletesTheCommit() {
      broker.on("replicateTransaction", args -> {
        applyThreadPublishes();
        throw new ReplicationDispatchedTimeoutException("dispatched, outcome unknown");
      });

      database.replicateAndCommitLocally(payload, true, stateMachine);

      verify(tx).completeCommit();
      verify(proxied, never()).rollback();
    }

    /** The leader refused the entry before appending it (page-version conflict): definite, retryable, rolled back. */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void aRefusedEntryIsRolledBackAndTheConflictSurfaced() {
      broker.fails("replicateTransaction", new ConcurrentModificationException("Concurrent modification on page 3/0"));

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
    // Regression for #6965: LocalCommitHandshake
    @Test
    void aFailedPublicationIsSurfacedAsCommittedRemotely() {
      final ConcurrentModificationException cause = new ConcurrentModificationException("simulated phase-2 failure");
      broker.on("replicateTransaction", args -> {
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
      assertThat(raftServer.calls("stepDown")).hasSize(1);
    }

    /** MAJORITY committed, ALL watch failed: the local commit completes and the watch failure is still reported. */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void aMajorityCommitWhoseAllWatchFailedCompletesLocallyAndRethrows() {
      broker.on("replicateTransaction", args -> {
        applyThreadPublishes();
        throw new MajorityCommittedAllFailedException("ALL quorum not reached");
      });

      assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
          .isInstanceOf(MajorityCommittedAllFailedException.class);

      verify(tx).completeCommit();
      verify(proxied, never()).rollback();
    }

    /**
     * Issue #8785: an in-place Ratis restart replaced the state machine the commit registered with. The replacement
     * recovers the log from its last applied index, which is below this entry since the old one never claimed it, and
     * applies the entry from its WAL bytes, possibly while this thread would still be inside phase 2: publishing here as
     * well could fold the record delta into the bucket counters twice. The committing thread waits for the entry's index,
     * which the waits read from the CURRENT division, and only releases.
     */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void anUnclaimedEntryTheReplacementStateMachineWillApplyIsNeverPublishedByTheCommittingThread() {
      broker.returns("replicateTransaction", 7L);
      stateMachineReplacedByARestart();

      database.replicateAndCommitLocally(payload, true, stateMachine);

      assertThat(waitedFor()).containsExactly(List.of(DB_NAME, 7L));
      verify(tx, never()).commit2ndPhase(any());
      verify(tx, never()).publishCommittedPages(any());
      verify(tx, never()).publishReplicatedCommittedPages(any());
      verify(tx, never()).completeCommit();
      verify(tx).concludeCommitWithoutPublishing();
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
    // Regression for #6965: LocalCommitHandshake
    @Test
    void anUnclaimedEntryALiveStateMachineWillApplyIsNeverPublishedByTheCommittingThread() {
      broker.returns("replicateTransaction", 7L);

      database.replicateAndCommitLocally(payload, true, stateMachine);

      assertThat(waitedFor()).containsExactly(List.of(DB_NAME, 7L));
      verify(tx, never()).commit2ndPhase(any());
      verify(tx, never()).publishCommittedPages(any());
      verify(tx, never()).publishReplicatedCommittedPages(any());
      verify(tx).concludeCommitWithoutPublishing();
      verify(proxied, never()).rollback();
      assertThat(stateMachine.pendingLocalCommits()).isZero();
    }

    /** Issue #8781, MAJORITY-committed variant: the exception carries the entry's index, which the committing thread awaits. */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void aMajorityCommittedUnclaimedEntryALiveStateMachineWillApplyIsNeverPublishedByTheCommittingThread() {
      broker.fails("replicateTransaction", new MajorityCommittedAllFailedException("ALL quorum not reached", null, 7L));

      assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
          .isInstanceOf(MajorityCommittedAllFailedException.class);

      assertThat(waitedFor()).containsExactly(List.of(DB_NAME, 7L));
      verify(tx, never()).commit2ndPhase(any());
      verify(tx, never()).publishCommittedPages(any());
      verify(tx, never()).publishReplicatedCommittedPages(any());
      verify(tx).concludeCommitWithoutPublishing();
      verify(proxied, never()).rollback();
    }

    /**
     * Issue #8781: a MAJORITY-committed exception that lost its index still leaves the pages to a live state machine, and
     * waits for the local commit index rather than releasing the commit locks at once (#5503).
     */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void aMajorityCommittedUnclaimedEntryWithoutAnIndexIsNeverPublishedWhileTheStateMachineLives() {
      broker.fails("replicateTransaction", new MajorityCommittedAllFailedException("ALL quorum not reached"));
      raftServer.commitIndex(11L);

      assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
          .isInstanceOf(MajorityCommittedAllFailedException.class);

      assertThat(waitedFor()).containsExactly(List.of(DB_NAME, 11L));
      verify(tx, never()).commit2ndPhase(any());
      verify(tx).concludeCommitWithoutPublishing();
    }

    /** Issue #8781: with neither the entry's index nor a commit index, a replica still never publishes. */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void aReplicaWithoutAnyIndexStillNeverPublishes() {
      broker.fails("replicateTransaction", new MajorityCommittedAllFailedException("ALL quorum not reached"));
      raftServer.commitIndex(-1L);

      assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, false, null))
          .isInstanceOf(MajorityCommittedAllFailedException.class);

      assertThat(waitedFor()).isEmpty();
      verify(tx, never()).commit2ndPhase(any());
      verify(tx).concludeCommitWithoutPublishing();
    }

    /**
     * Issue #8781, replica variant: a forwarded commit the leader answers MAJORITY-committed is rebuilt here from the
     * message, and this replica's state machine applies the entry from the leader's log. Publishing on the committing
     * thread as well folded the record delta twice; the replica only waits for the entry and releases.
     */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void aReplicaNeverPublishesAForwardedMajorityCommittedEntry() {
      broker.fails("replicateTransaction", new MajorityCommittedAllFailedException("ALL quorum not reached after MAJORITY commit at logIndex=9"));

      assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, false, null))
          .isInstanceOf(MajorityCommittedAllFailedException.class);

      assertThat(waitedFor()).containsExactly(List.of(DB_NAME, 9L));
      verify(tx, never()).commit2ndPhase(any());
      verify(tx).concludeCommitWithoutPublishing();
    }

    /** The index survives the hop to a follower: the message every leader-side constructor call writes carries it. */
    // Regression for #6965: LocalCommitHandshake
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

    /**
     * Issue #8785: a state machine closed without a shutdown is the window of an in-place Ratis restart before the
     * replacement is wired (or a division Ratis closed itself, which the health monitor restarts in place). The replacement
     * applies the entry from the log, so the committing thread must not publish it as well. The applied index reads -1
     * here, as it does while a restart re-initializes the division, which is the branch that warns the machine is still
     * closed after the wait. The race itself (the replacement applying the entry exactly once) needs a real Ratis restart
     * mid-commit and is not reproduced deterministically by these mocks.
     */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void anUnclaimedEntryIsNeverPublishedByTheCommittingThreadWhileAClosedStateMachineAwaitsItsReplacement() {
      final FakeArcadeStateMachine closed = new FakeArcadeStateMachine().closed(true);
      raftServer.stateMachine(closed);
      raftServer.lastAppliedIndex(-1L);
      broker.returns("replicateTransaction", 7L);

      database.replicateAndCommitLocally(payload, true, closed);

      assertThat(waitedFor()).containsExactly(List.of(DB_NAME, 7L));
      verify(tx, never()).commit2ndPhase(any());
      verify(tx).concludeCommitWithoutPublishing();
    }

    /** Issue #8781: a shutdown in progress stops the state machine applying, so the committing thread publishes. */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void anUnclaimedEntryIsPublishedByTheCommittingThreadWhenAShutdownIsRequested() {
      broker.returns("replicateTransaction", 7L);
      raftServer.shutdownRequested(true);

      database.replicateAndCommitLocally(payload, true, stateMachine);

      verify(tx).commit2ndPhase(any());
      verify(tx, never()).concludeCommitWithoutPublishing();
    }

    /** Issue #8785, MAJORITY-committed variant: the replacement state machine applies the entry, this thread only releases. */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void aMajorityCommitNobodyClaimedIsNeverPublishedAfterTheStateMachineWasReplaced() {
      broker.fails("replicateTransaction", new MajorityCommittedAllFailedException("ALL quorum not reached", null, 7L));
      stateMachineReplacedByARestart();

      assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
          .isInstanceOf(MajorityCommittedAllFailedException.class);

      assertThat(waitedFor()).containsExactly(List.of(DB_NAME, 7L));
      verify(tx, never()).commit2ndPhase(any());
      verify(tx, never()).completeCommit();
      verify(tx).concludeCommitWithoutPublishing();
      verify(proxied, never()).rollback();
      assertThat(stateMachine.pendingLocalCommits()).isZero();
    }

    /** MAJORITY committed, ALL watch failed, nobody claimed it and a shutdown stops every apply: the committing thread publishes. */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void aMajorityCommitNobodyClaimedIsPublishedByTheCommittingThreadDuringAShutdown() {
      broker.fails("replicateTransaction", new MajorityCommittedAllFailedException("ALL quorum not reached", null, 7L));
      raftServer.shutdownRequested(true);

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
    // Regression for #6965: LocalCommitHandshake
    @Test
    void aFailedPublicationDuringMajorityRecoveryStaysSilentButStepsDown() {
      final ConcurrentModificationException cause = new ConcurrentModificationException("simulated phase-2 failure");
      broker.on("replicateTransaction", args -> {
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
      assertThat(raftServer.calls("stepDown")).hasSize(1);
    }

    /** Without a state machine nobody can publish at the log position, so the committing thread does, as before. */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void withoutAStateMachineTheCommittingThreadPublishes() {
      broker.returns("replicateTransaction", 7L);

      database.replicateAndCommitLocally(payload, true, null);

      verify(tx).commit2ndPhase(any());
      verify(proxied, never()).rollback();
    }

    /** A replica registers nothing: the state machine applies its entry from the WAL bytes (#5503). */
    // Regression for #6965: LocalCommitHandshake
    @Test
    void aReplicaRegistersNothing() {
      broker.returns("replicateTransaction", 7L);

      database.replicateAndCommitLocally(payload, false, null);

      assertThat(stateMachine.pendingLocalCommits()).isZero();
      verify(tx, never()).completeCommit();
      verify(tx, never()).commit2ndPhase(any());
      verify(tx).concludeCommitWithoutPublishing();
    }

    /** A Ratis restart builds a new state machine, which recovers the log the one the commit registered with stopped applying. */
    private void stateMachineReplacedByARestart() {
      raftServer.stateMachine(new ArcadeStateMachine());
    }

    private void applyThreadPublishes() {
      final LocalCommit claimed = stateMachine.claimLocalCommit(DB_NAME, WAL_TX_ID, payload.walData());
      assertThat(claimed).as("the transaction must be registered before the entry is dispatched").isNotNull();
      claimed.published();
    }

    /** The wire layout {@code CommittedEntryDecoding.deserializeWalTransaction} reads: id, timestamp, no pages. */
    private static byte[] walTransactionBytes(final long txId) {
      final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES);
      buf.putLong(txId);
      buf.putLong(System.currentTimeMillis());
      buf.putInt(0);
      buf.putInt(0);
      return buf.array();
    }

    /**
     * The (database, index) of every wait for the local apply. The fake records the canonical (database, index,
     * throwOnTimeout) form the two-argument one delegates to; a recording of any other shape fails here by name.
     */
    private List<List<Object>> waitedFor() {
      final List<List<Object>> calls = raftServer.calls("waitForAppliedIndex");
      assertThat(calls).as("waitForAppliedIndex recorded as (database, index, throwOnTimeout)").allSatisfy(args -> assertThat(args).hasSize(3));
      return calls.stream().map(args -> args.subList(0, 2)).toList();
    }
  }

  /**
   * The ordering invariant that replaced the #5407 snapshot clamp, driven through the real
   * {@link ArcadeStateMachine#applyTransaction} and {@link ArcadeStateMachine#takeSnapshot} path: a transaction this
   * node originated is published at its entry's log position, BEFORE the applied index moves past the entry, so a
   * checkpoint taken afterwards may cover the entry and a restart never has to replay it to recover its pages.
   * <p>
   * The entry is fed back with a fresh context and no origin marker of any kind, as Ratis does after a step-down: the
   * state machine recognises its own transaction by the bytes it registered.

   */
  @Nested
  class PublishAtLogPosition {
    private static final long ENTRY_INDEX = 7L;

    @TempDir
    Path tempDir;

    private LocalDatabase      db;
    private ArcadeStateMachine sm;
    private RaftStorage        storage;

    @AfterEach
    void tearDown() throws IOException {
      RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = null;
      if (sm != null)
        sm.close();
      if (storage != null)
        storage.close();
      if (db != null && db.isOpen())
        db.drop();
    }

    // Regression for #6965: PublishAtLogPosition
    @Test
    void theApplyThreadPublishesBeforeTheAppliedIndexMovesPastTheEntry() throws Exception {
      db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("db").toString()).create();
      db.getSchema().createDocumentType("Doc", 1);
      storage = RaftStorage.newBuilder().setDirectory(tempDir.resolve("raft").toFile())
          .setOption(RaftStorage.StartupOption.FORMAT).build();
      sm = new ArcadeStateMachine() {
        @Override
        DatabaseInternal databaseFor(final String databaseName) {
          return db;
        }
      };
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);

      // Phase 1 of a real transaction, registered exactly as the committing thread does before dispatching it.
      db.begin();
      db.newDocument("Doc").set("v", 1).save();
      final TransactionContext tx = db.getTransaction();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      final byte[] walData = phase1.result.toByteArray();
      final LocalCommit local = new LocalCommit(db.getName(), CommittedEntryDecoding.peekWalTransactionId(walData), tx, phase1, walData);
      assertThat(sm.registerLocalCommit(local)).isTrue();

      // The entry as Ratis feeds it to the state machine.
      final ByteString payload = RaftLogEntryCodec.encodeTxEntry(db.getName(), walData, tx.getBucketRecordDelta());
      final LogEntryProto logEntry = LogEntryProto.newBuilder().setTerm(1L).setIndex(ENTRY_INDEX)
          .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build()).build();

      // Observed from inside the publication: the applied index must still be behind the entry at that moment.
      final AtomicLong appliedWhilePublishing = new AtomicLong(Long.MIN_VALUE);
      RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = name -> appliedWhilePublishing.set(indexOf(sm.getLastAppliedTermIndex()));

      sm.applyTransaction(org.apache.ratis.statemachine.TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build())
          .get();

      assertThat(local.awaitOutcome(1_000)).as("the apply thread claimed and published the registered transaction")
          .isEqualTo(LocalCommit.Outcome.PUBLISHED);
      assertThat(appliedWhilePublishing.get()).as("the pages were published while the applied index was still behind the entry")
          .isLessThan(ENTRY_INDEX);
      assertThat(indexOf(sm.getLastAppliedTermIndex())).isEqualTo(ENTRY_INDEX);
      assertThat(sm.takeSnapshot()).as("with the pages published, the checkpoint may cover the entry").isEqualTo(ENTRY_INDEX);
      assertThat(sm.pendingLocalCommits()).isZero();
      assertThat(sm.reservedPageVersions(db.getName())).isZero();

      // The committing thread's half, once the entry is acknowledged.
      tx.completeCommit();
      assertThat(db.countType("Doc", true)).isEqualTo(1L);
    }

    private static long indexOf(final TermIndex termIndex) {
      return termIndex != null ? termIndex.getIndex() : -1L;
    }

    private RaftServer stubServer() {
      return (RaftServer) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { RaftServer.class },
          (proxy, method, args) -> {
            if ("getId".equals(method.getName()))
              return RaftPeerId.valueOf("test-peer");
            if ("close".equals(method.getName()) || "start".equals(method.getName()))
              return null;
            throw new UnsupportedOperationException("Stub: " + method.getName());
          });
    }
  }

  /**
   * The pre-append validation of issue #6965 against a real database: two transactions validated against the same
   * page version are the collision the issue reports (two nodes, one document each, one shared page), and the second
   * one must be refused at the point that decides the log order, not merged region by region on apply. Also covers the
   * engine-side half: while an entry's versions are reserved, a local transaction on the same page fails its phase 1
   * with the same retryable conflict, without a Raft round trip.

   */
  @Nested
  class PreAppendValidation {
    private static final String TYPE = "Counter";

    @TempDir
    Path tempDir;

    private LocalDatabase      db;
    private ArcadeStateMachine stateMachine;
    private RID                leaderCounter;
    private RID                replicaCounter;

    @BeforeEach
    void setUp() {
      db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue6965").toString()).create();
      db.getSchema().createDocumentType(TYPE, 1); // one bucket: both documents share a page
      db.transaction(() -> {
        leaderCounter = db.newDocument(TYPE).set("name", "leader").set("value", 0L).save().getIdentity();
        replicaCounter = db.newDocument(TYPE).set("name", "replica").set("value", 0L).save().getIdentity();
      });
      stateMachine = new ArcadeStateMachine();
    }

    @AfterEach
    void tearDown() {
      if (db != null && db.isOpen())
        db.drop();
    }

    // Regression for #6965: PreAppendValidation
    @Test
    void theSecondWriterOnASharedPageIsRefusedNotMerged() throws Exception {
      // Two writers, one per document, both validated against the same page version: what the leader and a replica do
      // concurrently in the issue's scenario.
      final byte[] leaderEntry = prepareIncrement(leaderCounter);
      final byte[] replicaEntry = prepareIncrement(replicaCounter);

      stateMachine.validateBeforeAppend(db, leaderEntry, entry(1));
      assertThat(stateMachine.reservedPageVersions(db.getName())).isGreaterThan(0);

      assertThatThrownBy(() -> stateMachine.validateBeforeAppend(db, replicaEntry, entry(2)))
          .as("the second entry was validated against a version the log already gave away")
          .isInstanceOf(ConcurrentModificationException.class)
          .hasMessageContaining("retry");

      // Once the first entry is applied, the reservation is released and the local copy carries the version: the
      // replica's entry is still stale and still refused, now against the local copy.
      db.getTransactionManager().applyChanges(CommittedEntryDecoding.deserializeWalTransaction(leaderEntry), Map.of(), false);
      stateMachine.releaseReservedVersions(db.getName(), leaderEntry);
      assertThat(stateMachine.reservedPageVersions(db.getName())).isZero();
      assertThatThrownBy(() -> stateMachine.validateBeforeAppend(db, replicaEntry, entry(2)))
          .isInstanceOf(ConcurrentModificationException.class);

      // The replica retries: a fresh transaction against the applied page is accepted.
      stateMachine.validateBeforeAppend(db, prepareIncrement(replicaCounter), entry(3));
      assertThat(db.lookupByRID(leaderCounter, true).asDocument().getLong("value")).isEqualTo(1L);
    }

    // Regression for #6965: PreAppendValidation
    @Test
    void aReservedPageRefusesLocalTransactionsInPhaseOne() throws Exception {
      // A replica's entry has been accepted into the log but not applied here yet.
      final byte[] replicaEntry = prepareIncrement(replicaCounter);
      stateMachine.validateBeforeAppend(db, replicaEntry, entry(1));

      // A local transaction on the same page fails its own phase 1: the page it validated against is superseded, and
      // shipping it would only get it refused at append time one round trip later.
      assertThatThrownBy(() -> db.transaction(() -> {
        final Document counter = db.lookupByRID(leaderCounter, true).asDocument();
        counter.modify().set("value", counter.getLong("value") + 1).save();
      }, false, 0)).isInstanceOf(ConcurrentModificationException.class);

      // The entry is applied and its reservation released: the same local transaction now goes through.
      db.getTransactionManager().applyChanges(CommittedEntryDecoding.deserializeWalTransaction(replicaEntry), Map.of(), false);
      stateMachine.releaseReservedVersions(db.getName(), replicaEntry);
      db.transaction(() -> {
        final Document counter = db.lookupByRID(leaderCounter, true).asDocument();
        counter.modify().set("value", counter.getLong("value") + 1).save();
      }, false, 0);

      assertThat(db.lookupByRID(leaderCounter, true).asDocument().getLong("value")).isEqualTo(1L);
      assertThat(db.lookupByRID(replicaCounter, true).asDocument().getLong("value")).isEqualTo(1L);
    }

    /**
     * The validation never lets anything but a retryable refusal out of {@code startTransaction}: a file that vanished
     * under the entry is the engine's own conflict, and a database that cannot be read at all is a retryable refusal
     * with the cause attached.
     */
    // Regression for #6965: PreAppendValidation
    @Test
    void anEntryThatCannotBeValidatedIsRefusedNotThrown() throws Exception {
      final byte[] entry = prepareIncrement(leaderCounter);

      db.getSchema().dropType(TYPE);
      assertThatThrownBy(() -> stateMachine.validateBeforeAppend(db, entry, entry(1)))
          .isInstanceOf(ConcurrentModificationException.class)
          .hasMessageContaining("does not exist anymore");

      db.close();
      assertThatThrownBy(() -> stateMachine.validateBeforeAppend(db, entry, entry(2)))
          .isInstanceOf(NeedRetryException.class)
          .hasMessageContaining("Cannot validate the transaction on the leader");
    }

    private static PageVersionLedger.EntryId entry(final long callId) {
      return new PageVersionLedger.EntryId("client", callId);
    }

    /**
     * Runs phase 1 of an increment of the given counter and returns the WAL bytes it would ship, then abandons the
     * transaction: the bytes describe a delta validated against the page version current at that moment, exactly what
     * a node ships to the leader.
     */
    private byte[] prepareIncrement(final RID rid) {
      db.begin();
      final Document counter = db.lookupByRID(rid, true).asDocument();
      counter.modify().set("value", counter.getLong("value") + 1).save();
      final TransactionContext tx = db.getTransaction();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      final byte[] walData = phase1.result.toByteArray();
      tx.rollback();
      return walData;
    }
  }

  /**
   * Regression test for issue #5018.
   * <p>
   * The phase-2 commit-failure handler in {@link RaftReplicatedDatabase} logs a more specific
   * "page version conflict" message when the failure is the engine's page-version conflict
   * ({@link com.arcadedb.exception.ConcurrentModificationException}). The original bug was that the
   * {@code instanceof} test bound to {@link java.util.ConcurrentModificationException} - an unrelated
   * JDK type the engine never throws - so the specific branch was dead and every page-version conflict
   * was logged with the generic wording, hiding the "this may indicate a locking bug" signal operators
   * rely on.
   * <p>
   * This test pins {@link RaftReplicatedDatabase#phase2CommitFailureMessage(Throwable)} to the engine
   * type so a future edit cannot re-bind the check to the JDK type without failing here. It needs no
   * cluster and creates no database.

   */
  @Nested
  class Phase2ConflictMessage {

    private static final String PAGE_CONFLICT_MARKER = "page version conflict";

    // Regression for #5018: Phase2ConflictMessage
    @Test
    void engineConcurrentModificationExceptionSelectsPageConflictMessage() {
      final String message = RaftReplicatedDatabase.phase2CommitFailureMessage(
          new com.arcadedb.exception.ConcurrentModificationException("page 1 v2 != v1"));

      assertThat(message).contains(PAGE_CONFLICT_MARKER);
      assertThat(message).contains("this may indicate a locking bug");
    }

    // Regression for #5018: Phase2ConflictMessage
    @Test
    void jdkConcurrentModificationExceptionDoesNotSelectPageConflictMessage() {
      // The JDK type is what the buggy instanceof used to bind to; the engine never throws it here,
      // so it must fall through to the generic message - proving the check no longer matches it.
      final String message = RaftReplicatedDatabase.phase2CommitFailureMessage(
          new java.util.ConcurrentModificationException("iterator modified"));

      assertThat(message).doesNotContain(PAGE_CONFLICT_MARKER);
      assertThat(message).contains("Phase 2 commit failed AFTER successful Raft replication (db=");
    }

    // Regression for #5018: Phase2ConflictMessage
    @Test
    void unrelatedExceptionSelectsGenericMessage() {
      final String message = RaftReplicatedDatabase.phase2CommitFailureMessage(
          new RuntimeException("boom"));

      assertThat(message).doesNotContain(PAGE_CONFLICT_MARKER);
      assertThat(message).contains("Phase 2 commit failed AFTER successful Raft replication (db=");
    }
  }

  /**
   * Issue #9548, the committing thread's half: a close of the database on another thread (the engine's JVM shutdown hook
   * in the report) removes every thread's transaction context while the committer waits for the quorum or for the apply
   * thread. The commit tail then required that context to pop it and threw "Transaction context not found on current
   * thread", which replaced the outcome the caller is owed. Every tail that runs after the entry left this node must
   * conclude without it: completed when the pages were published, {@link TransactionCommittedRemotelyException} when they
   * were not.
   * <p>
   * The close is simulated on the committing thread itself, from inside the broker call, by removing its context: that is
   * exactly what {@code DatabaseContext.removeAllContexts} does to it from the closing thread.
   */
  @Nested
  class CommitTailAfterContextRemoved {
    private static final String DB_NAME   = "issue9548";
    private static final long   WAL_TX_ID = 9548L;

    private final String dbPath = "issue9548-commit-tail-" + UUID.randomUUID();

    private LocalDatabase                             proxied;
    private FakeRaftHAServer                          raftServer;
    private TransactionContext                        tx;
    private ArcadeStateMachine                        stateMachine;
    private FakeRaftTransactionBroker                 broker;
    private RaftReplicatedDatabase                    database;
    private RaftReplicatedDatabase.ReplicationPayload payload;

    @BeforeEach
    void setUp() {
      proxied = SubclassMocks.mock(LocalDatabase.class);
      when(proxied.getDatabasePath()).thenReturn(dbPath);
      when(proxied.getName()).thenReturn(DB_NAME);
      when(proxied.getTransactionManager()).thenReturn(SubclassMocks.mock(TransactionManager.class));
      when(proxied.getSchema()).thenReturn(mock(Schema.class, RETURNS_DEEP_STUBS));
      when(proxied.executeInReadLock(any())).thenAnswer(inv -> ((Callable<?>) inv.getArgument(0)).call());

      broker = new FakeRaftTransactionBroker();
      raftServer = FakeRaftHAServer.detached();
      raftServer.leader(true);
      raftServer.transactionBroker(broker);
      raftServer.quorumTimeout(1_000L);
      raftServer.txPreparedAtCapable(false);
      stateMachine = new ArcadeStateMachine();
      raftServer.stateMachine(stateMachine);

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
     * The report's exact sequence: the apply thread claimed the entry and failed to publish it (the database was closing),
     * the quorum wait timed out, and the committing thread - its context gone - must still tell the caller that the
     * transaction is committed cluster-wide and must not be retried.
     */
    // Regression for #9548: CommitTailAfterContextRemoved
    @Test
    void aTimedOutCommitWhosePublicationFailedReportsCommittedRemotelyWithoutAContext() {
      final DatabaseIsClosedException closed = new DatabaseIsClosedException(DB_NAME);
      broker.on("replicateTransaction", args -> {
        databaseClosedOnAnotherThread();
        claim().failed(closed, false);
        throw new ReplicationDispatchedTimeoutException("dispatched, outcome unknown");
      });

      assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
          .isInstanceOf(TransactionCommittedRemotelyException.class)
          .hasMessageContaining("committed cluster-wide")
          .hasCause(closed);

      verify(tx).setRemotelyCommitted(true);
      verify(tx).concludeFailedPhase2(closed);
      verify(proxied, never()).rollback();
    }

    /** Acknowledged, failed publication: same answer as above through the acknowledged exit. */
    // Regression for #9548: CommitTailAfterContextRemoved
    @Test
    void anAcknowledgedCommitWhosePublicationFailedReportsCommittedRemotelyWithoutAContext() {
      final ConcurrentModificationException cause = new ConcurrentModificationException("simulated phase-2 failure");
      broker.on("replicateTransaction", args -> {
        databaseClosedOnAnotherThread();
        claim().failed(cause, true);
        return 7L;
      });

      assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
          .isInstanceOf(TransactionCommittedRemotelyException.class)
          .hasCause(cause);

      verify(tx).concludeFailedPhase2(cause);
    }

    /** Published by the apply thread: the commit completes, the missing context is not an error. */
    // Regression for #9548: CommitTailAfterContextRemoved
    @Test
    void aPublishedCommitCompletesWithoutAContext() {
      broker.on("replicateTransaction", args -> {
        databaseClosedOnAnotherThread();
        claim().published();
        return 7L;
      });

      database.replicateAndCommitLocally(payload, true, stateMachine);

      verify(tx).completeCommit();
      verify(proxied, never()).rollback();
    }

    /** Unclaimed with a live state machine: the commit waits for the apply and releases, without a context. */
    // Regression for #9548: CommitTailAfterContextRemoved
    @Test
    void anUnclaimedCommitALiveStateMachineAppliesIsReleasedWithoutAContext() {
      broker.on("replicateTransaction", args -> {
        databaseClosedOnAnotherThread();
        return 7L;
      });

      database.replicateAndCommitLocally(payload, true, stateMachine);

      verify(tx).concludeCommitWithoutPublishing();
      verify(tx, never()).commit2ndPhase(any());
    }

    /** Unclaimed during a shutdown: the committing thread publishes the pages itself, without a context. */
    // Regression for #9548: CommitTailAfterContextRemoved
    @Test
    void anUnclaimedCommitPublishedByTheCommittingThreadDuringAShutdownCompletesWithoutAContext() {
      raftServer.shutdownRequested(true);
      broker.on("replicateTransaction", args -> {
        databaseClosedOnAnotherThread();
        return 7L;
      });

      database.replicateAndCommitLocally(payload, true, stateMachine);

      verify(tx).commit2ndPhase(any());
      verify(tx).setRemotelyCommitted(true);
    }

    /** A replica waits for its own apply and releases, without a context. */
    // Regression for #9548: CommitTailAfterContextRemoved
    @Test
    void aReplicaCommitIsReleasedWithoutAContext() {
      broker.on("replicateTransaction", args -> {
        databaseClosedOnAnotherThread();
        return 7L;
      });

      database.replicateAndCommitLocally(payload, false, null);

      verify(tx).concludeCommitWithoutPublishing();
    }

    /** What {@code DatabaseContext.removeAllContexts} does to this thread when another thread closes the database. */
    private void databaseClosedOnAnotherThread() {
      DatabaseContext.INSTANCE.removeContext(dbPath);
      assertThat(DatabaseContext.INSTANCE.getContextIfExists(dbPath)).isNull();
    }

    private LocalCommit claim() {
      final LocalCommit claimed = stateMachine.claimLocalCommit(DB_NAME, WAL_TX_ID, payload.walData());
      assertThat(claimed).as("the transaction must be registered before the entry is dispatched").isNotNull();
      return claimed;
    }

    /** The wire layout {@code CommittedEntryDecoding.deserializeWalTransaction} reads: id, timestamp, no pages. */
    private static byte[] walTransactionBytes(final long txId) {
      final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES);
      buf.putLong(txId);
      buf.putLong(System.currentTimeMillis());
      buf.putInt(0);
      buf.putInt(0);
      return buf.array();
    }
  }

  /**
   * Issue #9411: the warning a committing thread logs when it releases a committed entry unpublished because the local
   * state machine is closed (#8785).
   * <ul>
   *   <li>It fired once per commit during a prolonged in-place restart; it is now said once per throttle window, with the
   *   number of commits it stands for.</li>
   *   <li>A closed machine is the restart window whatever the old division still reports: a stale applied index must not
   *   turn it into the #5503 apply-lag alarm, which points the operator at the wrong problem.</li>
   * </ul>
   * Real objects throughout: an unstarted {@link RaftHAServer} with its own state machine, closed, and a real database.
   * The wait for the local apply runs against a quorum timeout of a few milliseconds.
   */
  @Nested
  class UnpublishedReleaseWarning {

    private static final String SERVER_LIST       = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
    private static final String UNPUBLISHED       = "is releasing entry %d unpublished";
    private static final String APPLY_LAG_ALARM   = "the condition behind issue #5503";
    private static final long   COMMITTED_INDEX   = 5L;

    @TempDir
    Path dir;

    private RaftHAServer           raft;
    private LocalDatabase          database;
    private RaftReplicatedDatabase replicated;
    private CapturingTestLogger    logger;

    @BeforeEach
    void setUp() throws Exception {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
      config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 5L);
      raft = new RaftHAServer(TestServerHelper.unstartedServer("ArcadeDB_0"), config);

      database = (LocalDatabase) new DatabaseFactory(dir.resolve("issue9411").toString()).create();
      replicated = new RaftReplicatedDatabase(null, database, raft);
      logger = CapturingTestLogger.install();
    }

    @AfterEach
    void tearDown() throws Exception {
      logger.uninstall();
      if (!raft.getStateMachine().isClosed())
        raft.getStateMachine().close();
      if (database != null && database.isOpen())
        database.drop();
    }

    // Regression for #9411: UnpublishedReleaseWarning
    @Test
    void aClosedStateMachineIsReportedOncePerWindowNotOncePerCommit() throws Exception {
      raft.getStateMachine().close();

      for (int i = 0; i < 5; i++)
        releaseOneCommit();

      assertThat(logger.countContaining(UNPUBLISHED)).as("one warning for five releases inside one window").isEqualTo(1);
      assertThat(logger.countContaining(APPLY_LAG_ALARM)).isZero();
    }

    // Regression for #9411: UnpublishedReleaseWarning
    @Test
    void aClosedStateMachineIsNotReportedAsApplyLagWhenTheOldDivisionStillAnswers() throws Exception {
      raft.getStateMachine().close();
      // The closed division still answers with the index it reached before closing, behind the committed entry.
      final RaftServer ratis = mock(RaftServer.class, RETURNS_DEEP_STUBS);
      when(ratis.getDivision(any()).getInfo().getLastAppliedIndex()).thenReturn(COMMITTED_INDEX - 2);
      final Field field = RaftHAServer.class.getDeclaredField("raftServer");
      field.setAccessible(true);
      field.set(raft, ratis);

      releaseOneCommit();

      assertThat(logger.countContaining(UNPUBLISHED)).isEqualTo(1);
      assertThat(logger.countContaining(APPLY_LAG_ALARM)).as("a closed machine is the restart window, not apply lag").isZero();
    }

    // Regression for #9411: UnpublishedReleaseWarning
    @Test
    void aLiveStateMachineWithAnUnknownAppliedIndexWarnsAboutNothing() {
      releaseOneCommit();

      assertThat(logger.countContaining(UNPUBLISHED)).isZero();
      assertThat(logger.countContaining(APPLY_LAG_ALARM)).isZero();
    }

    /** Begins a transaction and releases it the way a committing thread does after an unclaimed acknowledged entry. */
    private void releaseOneCommit() {
      database.begin();
      final var tx = DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).getLastTransaction();
      replicated.awaitLocalApplyAndRelease(new RaftReplicatedDatabase.ReplicationPayload(tx, null, new byte[0], Map.of()),
          COMMITTED_INDEX);
    }
  }
}
