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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #6111.
 * <p>
 * {@link ArcadeStateMachine#reinitialize()} seeds the applied position from the on-disk Ratis snapshot
 * marker. When the marker index runs ahead of the persisted applied index by more than
 * {@link GlobalConfiguration#HA_SNAPSHOT_GAP_TOLERANCE}, the entries it covers were never applied on this
 * node: the method flags {@code needsSnapshotDownload} and schedules an asynchronous re-download - but it
 * used to advance the applied position and (since issue #6110) call {@code notifyApplied()} anyway.
 * <p>
 * Ratis derives {@link RaftHAServer#getLastAppliedIndex()} from that same marker, so the whole gap looked
 * applied to {@link RaftHAServer#waitForAppliedIndex(long, boolean)} and
 * {@link RaftHAServer#waitForLocalApply()}: a LINEARIZABLE or READ_YOUR_WRITES read targeting an index
 * inside the gap was released immediately and served from the stale local databases, before the flagged
 * re-download had run.
 * <p>
 * The fix publishes an honest read floor while the re-download is outstanding, keeps the ArcadeDB-side
 * applied counter on the persisted position, withholds the notification, and clamps both apply waiters to
 * the floor. The Ratis-facing applied {@code TermIndex} is deliberately still seeded from the marker: it
 * is the only replay position Ratis has.
 */
class Issue6111StaleSnapshotReadFloorTest {

  /** Comfortably beyond {@link GlobalConfiguration#HA_SNAPSHOT_GAP_TOLERANCE} (10). */
  private static final long PERSISTED_APPLIED = 100L;
  private static final long SNAPSHOT_INDEX    = 5_000L;
  private static final long SNAPSHOT_TERM     = 7L;
  private static final String DB_NAME         = "db-a";

  // ---------------------------------------------------------------------------------------------
  // reinitialize(): what the stale marker may and may not do
  // ---------------------------------------------------------------------------------------------

  /**
   * The core regression: with the gap flagged, {@code reinitialize()} must neither wake apply waiters nor
   * advance the ArcadeDB-side applied counter onto the marker, and it must publish the persisted position
   * as the read floor.
   */
  @Test
  void staleMarkerPublishesAFloorAndWithholdsTheAppliedNotification(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir.resolve("raft-storage"));
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    final RaftHAServer mockRaft = mock(RaftHAServer.class);
    try {
      sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);

      // Only entries up to PERSISTED_APPLIED were ever applied here...
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, DB_NAME);
      // ...but the marker on disk claims SNAPSHOT_INDEX.
      registerMarkerAt(sm, SNAPSHOT_TERM, SNAPSHOT_INDEX);

      sm.setRaftHAServer(mockRaft);
      sm.reinitialize();

      assertThat(sm.getStaleSnapshotAppliedFloor())
          .as("the read floor is the position the databases really carry, not the marker")
          .isEqualTo(PERSISTED_APPLIED);
      assertThat(sm.isSnapshotDownloadPending())
          .as("the gap is flagged for re-download")
          .isTrue();
      assertThat(sm.isResyncInProgress())
          .as("a node holding an unfilled gap must not advertise itself as ready")
          .isTrue();
      assertThat(readLastAppliedIndex(sm))
          .as("the ArcadeDB-side applied counter stays on the honest persisted position")
          .isEqualTo(PERSISTED_APPLIED);
      verify(mockRaft, never()).notifyApplied();
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /**
   * The Ratis contract is unchanged: {@code StateMachineUpdater.reload()} reads
   * {@code getLatestSnapshot().getIndex()} straight after {@code reinitialize()} and requires the applied
   * {@code TermIndex} to match it, so the marker must still seed it even on the stale branch.
   */
  @Test
  void staleMarkerStillSeedsTheRatisFacingAppliedTermIndex(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir.resolve("raft-storage"));
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, DB_NAME);
      registerMarkerAt(sm, SNAPSHOT_TERM, SNAPSHOT_INDEX);

      sm.reinitialize();

      assertThat(sm.getLastAppliedTermIndex().getIndex())
          .as("Ratis' replay position still comes from the marker")
          .isEqualTo(SNAPSHOT_INDEX);
      assertThat(sm.getLastAppliedTermIndex().getTerm()).isEqualTo(SNAPSHOT_TERM);
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /**
   * Control: a marker within the gap tolerance is trustworthy, so the pre-existing behaviour is intact -
   * the counter is seeded from the marker, waiters are woken (issue #5846), and no floor is published.
   */
  @Test
  void markerWithinToleranceSeedsAndNotifiesAsBefore(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir.resolve("raft-storage"));
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    final RaftHAServer mockRaft = mock(RaftHAServer.class);
    try {
      sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);

      // Gap of exactly HA_SNAPSHOT_GAP_TOLERANCE (10): the check is strictly greater-than, so no flag.
      sm.writePersistedAppliedIndex(SNAPSHOT_INDEX - 10, DB_NAME);
      registerMarkerAt(sm, SNAPSHOT_TERM, SNAPSHOT_INDEX);

      sm.setRaftHAServer(mockRaft);
      sm.reinitialize();

      assertThat(sm.getStaleSnapshotAppliedFloor()).isEqualTo(-1L);
      assertThat(sm.isSnapshotDownloadPending()).isFalse();
      assertThat(readLastAppliedIndex(sm)).isEqualTo(SNAPSHOT_INDEX);
      verify(mockRaft, times(1)).notifyApplied();
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /**
   * A later {@code reinitialize()} that no longer sees a gap - the resync landed and persisted its
   * position - drops the floor, so reads are unclamped again.
   */
  @Test
  void aLaterReinitializeWithoutAGapDropsTheFloor(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir.resolve("raft-storage"));
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, DB_NAME);
      registerMarkerAt(sm, SNAPSHOT_TERM, SNAPSHOT_INDEX);
      sm.reinitialize();
      assertThat(sm.getStaleSnapshotAppliedFloor()).isEqualTo(PERSISTED_APPLIED);

      // The resync recorded the marker index for every database, exactly as a full install does.
      sm.writePersistedAppliedIndexForAllDatabases(SNAPSHOT_INDEX);
      sm.reinitialize();

      assertThat(sm.getStaleSnapshotAppliedFloor())
          .as("no gap remains, so nothing clamps the readers")
          .isEqualTo(-1L);
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /**
   * {@code raftHAServer} is null during the startup {@code initialize() -> reinitialize()} path; the stale
   * branch must not throw there either.
   */
  @Test
  void staleMarkerDoesNotThrowWhenRaftHAServerNotYetAttached(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir.resolve("raft-storage"));
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, DB_NAME);
      registerMarkerAt(sm, SNAPSHOT_TERM, SNAPSHOT_INDEX);

      assertThatNoException().isThrownBy(sm::reinitialize);
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The waiters: what the floor does to a read
  // ---------------------------------------------------------------------------------------------

  /**
   * The observable half of the bug. Ratis reports the marker index as applied, so before the fix the
   * LINEARIZABLE waiter's predicate was satisfied at once and the read proceeded against databases that
   * stop at the floor. It must now fail instead - the LINEARIZABLE contract is "fail rather than serve a
   * value older than an already-committed write".
   */
  @Test
  void linearizableReadFailsWhileTheFloorIsOutstanding() throws Exception {
    final AtomicLong ratisApplied = new AtomicLong(SNAPSHOT_INDEX);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);
    final RaftHAServer raft = newDetachedRaftHAServer(ratisApplied, SNAPSHOT_INDEX, sm, 500L);

    assertThat(raft.getLastAppliedIndex())
        .as("Ratis still advertises the marker index")
        .isEqualTo(SNAPSHOT_INDEX);
    assertThat(raft.getTrustedAppliedIndex())
        .as("but only the floor is backed by local data")
        .isEqualTo(PERSISTED_APPLIED);

    assertThatThrownBy(() -> raft.waitForAppliedIndex(PERSISTED_APPLIED + 1, true))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("LINEARIZABLE read timed out");
  }

  /** A target at or below the floor is genuinely on disk and must still be served without waiting. */
  @Test
  void linearizableReadBelowTheFloorStillSucceeds() throws Exception {
    final AtomicLong ratisApplied = new AtomicLong(SNAPSHOT_INDEX);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);
    final RaftHAServer raft = newDetachedRaftHAServer(ratisApplied, SNAPSHOT_INDEX, sm, 500L);

    assertThatNoException().isThrownBy(() -> raft.waitForAppliedIndex(PERSISTED_APPLIED, true));
  }

  /**
   * Once the resync clears the floor, a blocked LINEARIZABLE waiter is released. This also pins that the
   * resync path notifies: the waiter is woken by {@code notifyApplied()} rather than by its own bounded
   * re-check, which is why the assertion allows well under one re-check interval.
   */
  @Test
  void linearizableReadIsReleasedWhenTheResyncClearsTheFloor() throws Exception {
    final AtomicLong ratisApplied = new AtomicLong(SNAPSHOT_INDEX);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);
    final RaftHAServer raft = newDetachedRaftHAServer(ratisApplied, SNAPSHOT_INDEX, sm, 10_000L);

    assertThat(raft.getTrustedAppliedIndex())
        .as("the waiter must actually block on the floor, otherwise this test proves nothing")
        .isEqualTo(PERSISTED_APPLIED);

    final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    try {
      scheduler.schedule(() -> {
        sm.clearStaleSnapshotFloor();
        raft.notifyApplied();
      }, 300, TimeUnit.MILLISECONDS);

      final long start = System.currentTimeMillis();
      assertThatNoException().isThrownBy(() -> raft.waitForAppliedIndex(SNAPSHOT_INDEX, true));
      final long elapsed = System.currentTimeMillis() - start;

      assertThat(elapsed)
          .as("the wait really was held by the floor until the resync cleared it")
          .isGreaterThanOrEqualTo(250L);
      assertThat(elapsed)
          .as("clearing the floor releases the waiter promptly, not at the quorum timeout")
          .isLessThan(5_000L);
    } finally {
      scheduler.shutdownNow();
    }
  }

  /**
   * The lenient (READ_YOUR_WRITES / bookmark) waiter keeps its "degrade, do not fail" contract, but must
   * not burn the whole quorum timeout on a gap only the pending resync can close.
   */
  @Test
  void readYourWritesDegradesImmediatelyInsteadOfBurningTheQuorumTimeout() throws Exception {
    final AtomicLong ratisApplied = new AtomicLong(SNAPSHOT_INDEX);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);
    final RaftHAServer raft = newDetachedRaftHAServer(ratisApplied, SNAPSHOT_INDEX, sm, 10_000L);

    assertThat(raft.getTrustedAppliedIndex())
        .as("the clamp must be in force, otherwise the waiter never enters the loop and proves nothing")
        .isEqualTo(PERSISTED_APPLIED);

    final long start = System.currentTimeMillis();
    assertThatNoException().isThrownBy(() -> raft.waitForAppliedIndex(SNAPSHOT_INDEX, false));
    assertThat(System.currentTimeMillis() - start)
        .as("waiting cannot help while the floor stands, so the lenient waiter degrades at once")
        .isLessThan(2_000L);
  }

  /** Same for {@link RaftHAServer#waitForLocalApply()}, which is always lenient. */
  @Test
  void waitForLocalApplyDegradesImmediatelyWhileTheFloorIsOutstanding() throws Exception {
    final AtomicLong ratisApplied = new AtomicLong(SNAPSHOT_INDEX);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);
    final RaftHAServer raft = newDetachedRaftHAServer(ratisApplied, SNAPSHOT_INDEX, sm, 10_000L);

    assertThat(raft.getTrustedAppliedIndex())
        .as("the clamp must be in force, otherwise the waiter never enters the loop and proves nothing")
        .isEqualTo(PERSISTED_APPLIED);

    final long start = System.currentTimeMillis();
    raft.waitForLocalApply();
    assertThat(System.currentTimeMillis() - start).isLessThan(2_000L);
  }

  /** With no floor outstanding the trusted index is the raw Ratis one - no behaviour change at all. */
  @Test
  void withoutAFloorTheTrustedIndexIsTheRatisIndex() throws Exception {
    final AtomicLong ratisApplied = new AtomicLong(SNAPSHOT_INDEX);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final RaftHAServer raft = newDetachedRaftHAServer(ratisApplied, SNAPSHOT_INDEX, sm, 500L);

    assertThat(sm.getStaleSnapshotAppliedFloor()).isEqualTo(-1L);
    assertThat(raft.getTrustedAppliedIndex()).isEqualTo(SNAPSHOT_INDEX);
    assertThatNoException().isThrownBy(() -> raft.waitForAppliedIndex(SNAPSHOT_INDEX, true));
  }

  // ---------------------------------------------------------------------------------------------
  // The retry backstop: an unfilled gap must not need a leader election to recover
  // ---------------------------------------------------------------------------------------------

  /**
   * The lag-driven backstop is structurally blind here - Ratis reports the marker index as applied, so a
   * node with an open gap shows zero lag - and a re-armed {@code needsSnapshotDownload} additionally
   * makes {@code isSnapshotDownloadPending()} true, which stands {@code recoverFromPersistentLag()} and
   * {@code isFollowerLaggingBeyond()} down. So the {@link HealthMonitor} must drive a dedicated hook, or
   * a node whose download keeps failing stays clamped until an election happens.
   */
  @Test
  void healthMonitorTickDrivesTheUnfilledGapRetry() {
    final AtomicInteger retries = new AtomicInteger();
    final HealthMonitor.HealthTarget target = new HealthMonitor.HealthTarget() {
      @Override
      public LifeCycle.State getRaftLifeCycleState() {
        return LifeCycle.State.RUNNING;
      }

      @Override
      public boolean isShutdownRequested() {
        return false;
      }

      @Override
      public void restartRatisIfNeeded() {
      }

      @Override
      public void retryUnfilledSnapshotGap() {
        retries.incrementAndGet();
      }
    };

    final HealthMonitor monitor = new HealthMonitor(target, 1_000L, 0L, 0L, false, 0);
    monitor.tick();
    monitor.tick();

    assertThat(retries.get()).as("every health tick offers the unfilled-gap retry").isEqualTo(2);
  }

  /** No gap outstanding: the backstop must not touch anything. */
  @Test
  void retryIsANoOpWithoutAnOutstandingFloor(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    sm.setRaftHAServer(followerRaftHAServerMock());
    try {
      sm.retryUnfilledSnapshotGap();
      assertThat(readLastRetryMs(sm)).as("nothing was submitted").isZero();
    } finally {
      sm.close();
    }
  }

  /** A download that is genuinely running will resolve or re-arm the request itself; do not pile on. */
  @Test
  void retryIsANoOpWhileADownloadIsRunning(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    sm.setRaftHAServer(followerRaftHAServerMock());
    try {
      setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);
      setAtomicBoolean(sm, "snapshotDownloadInProgress", true);

      sm.retryUnfilledSnapshotGap();

      assertThat(readLastRetryMs(sm)).as("the in-flight download owns this resync").isZero();
    } finally {
      sm.close();
    }
  }

  /**
   * The retry throttle allows one attempt per watchdog interval, so an attempt the resync will refuse anyway
   * costs a whole interval of not retrying. The precheck therefore asks the same questions
   * {@code resolveSnapshotSource()} asks, including the one it used to skip: an address that resolves to this
   * node's own is not somewhere to download from (issue #6202).
   */
  @Test
  void retryStandsDownWhenTheResolvedLeaderAddressIsOurOwn(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    final RaftHAServer mockRaft = mock(RaftHAServer.class);
    when(mockRaft.isLeader()).thenReturn(false);
    when(mockRaft.getLeaderId()).thenReturn(LEADER_PEER_ID);
    when(mockRaft.getUnambiguousPeerHttpAddress(LEADER_PEER_ID)).thenReturn("localhost:2480");
    when(mockRaft.isOwnHttpAddress("localhost:2480")).thenReturn(true);
    sm.setRaftHAServer(mockRaft);
    try {
      setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);

      sm.retryUnfilledSnapshotGap();

      assertThat(readLastRetryMs(sm))
          .as("a doomed attempt must not consume the one retry slot this watchdog interval has")
          .isZero();
    } finally {
      sm.close();
    }
  }

  /**
   * The case the backstop exists for: a floor left behind by a failed download, nothing running, a
   * leader reachable. The retry must fire - and reach {@code triggerSnapshotDownload()}, which with no
   * databases present completes and clears the floor.
   */
  @Test
  void retryFiresAndResolvesTheFloorWhenNothingIsRunning(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    sm.setRaftHAServer(followerRaftHAServerMock());
    try {
      setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);

      sm.retryUnfilledSnapshotGap();

      assertThat(readLastRetryMs(sm)).as("a retry was submitted").isNotZero();
      // The submission runs on the single-threaded lifecycleExecutor; give it a bounded moment.
      for (int i = 0; i < 100 && sm.getStaleSnapshotAppliedFloor() >= 0; i++)
        Thread.sleep(20);
      assertThat(sm.getStaleSnapshotAppliedFloor())
          .as("the retry really reached triggerSnapshotDownload(), which resolved the floor")
          .isEqualTo(-1L);
    } finally {
      sm.close();
    }
  }

  /**
   * A full resync pulls every database from the leader while the HealthMonitor ticks every few seconds,
   * so a persistently failing download must not be retried on every tick.
   */
  @Test
  void retryIsThrottledBetweenAttempts(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    sm.setRaftHAServer(followerRaftHAServerMock());
    try {
      setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);
      // A retry that has just been attempted, as a previous tick would have left it.
      final long justNow = System.currentTimeMillis();
      setLastRetryMs(sm, justNow);

      sm.retryUnfilledSnapshotGap();

      assertThat(readLastRetryMs(sm))
          .as("within the retry interval the tick is swallowed, leaving the previous attempt's stamp")
          .isEqualTo(justNow);
      assertThat(sm.getStaleSnapshotAppliedFloor())
          .as("and no download ran, so the floor still stands")
          .isEqualTo(PERSISTED_APPLIED);
    } finally {
      sm.close();
    }
  }

  /** Nowhere to download from yet: the first attempt belongs to {@code notifyLeaderChanged()}. */
  @Test
  void retryIsANoOpWhileNoLeaderIsKnown(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    final RaftHAServer mockRaft = mock(RaftHAServer.class); // getLeaderHttpAddress() defaults to null
    sm.setRaftHAServer(mockRaft);
    try {
      setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);

      sm.retryUnfilledSnapshotGap();

      assertThat(readLastRetryMs(sm)).isZero();
    } finally {
      sm.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // A node must never "resolve" its own gap by downloading from itself
  // ---------------------------------------------------------------------------------------------

  /**
   * The dangerous sequence: restart onto a stale marker, then win the next election before the resync
   * lands. {@code notifyLeaderChanged()} submits {@code triggerSnapshotDownload()} unconditionally, on
   * the new leader too, and {@code getLeaderHttpAddress()} then resolves to this node's own address - so
   * the resync would copy this node's incomplete databases onto themselves, report success, and let the
   * floor be dropped and the marker index durably recorded as applied. That reopens #6111 on a leader,
   * and survives the next restart because the persisted position no longer shows the gap.
   */
  @Test
  void aLeaderMustNotResolveItsOwnFloorByDownloadingFromItself(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    final RaftHAServer mockRaft = mock(RaftHAServer.class);
    when(mockRaft.isLeader()).thenReturn(true);
    when(mockRaft.getUnambiguousPeerHttpAddress(LEADER_PEER_ID)).thenReturn("localhost:2480");
    sm.setRaftHAServer(mockRaft);
    try {
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, DB_NAME);
      setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);

      sm.triggerSnapshotDownload();

      assertThat(sm.getStaleSnapshotAppliedFloor())
          .as("a self-download resolves nothing, so the read floor must stand")
          .isEqualTo(PERSISTED_APPLIED);
      assertThat(sm.readPersistedAppliedIndex())
          .as("and the marker index must not be durably recorded as applied")
          .isEqualTo(PERSISTED_APPLIED);
      assertThat(sm.isResyncInProgress())
          .as("the node keeps itself out of the ready set instead of pretending it caught up")
          .isTrue();
    } finally {
      sm.close();
    }
  }

  /**
   * Same refusal via the address comparison rather than the role flag: leadership can move between the
   * {@code isLeader()} check and the address resolution, and {@code getLeaderId()} can report this node
   * while {@code isLeader()} has not caught up.
   * <p>
   * The comparison itself moved into {@code RaftHAServer.isSameHttpEndpoint} when the write-forwarding path
   * started asking the same question (issue #6191), and its edge cases are unit-tested there; here it runs
   * for real against the two addresses the mock reports.
   */
  @Test
  void aResolvedLeaderAddressEqualToOurOwnIsAlsoRefused(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    final RaftHAServer mockRaft = mock(RaftHAServer.class);
    when(mockRaft.isLeader()).thenReturn(false); // role flag has not caught up...
    when(mockRaft.getLeaderId()).thenReturn(LEADER_PEER_ID);
    when(mockRaft.getUnambiguousPeerHttpAddress(LEADER_PEER_ID)).thenReturn("localhost:2480");
    when(mockRaft.getLocalHttpAddress()).thenReturn("localhost:2480"); // ...but it is us
    sm.setRaftHAServer(mockRaft);
    try {
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, DB_NAME);
      setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);

      sm.triggerSnapshotDownload();

      assertThat(sm.getStaleSnapshotAppliedFloor()).isEqualTo(PERSISTED_APPLIED);
      assertThat(sm.readPersistedAppliedIndex()).isEqualTo(PERSISTED_APPLIED);
    } finally {
      sm.close();
    }
  }

  /**
   * Control: a genuine peer leader is downloaded from, and the floor resolves. Without it the two tests
   * above would also pass against a {@code triggerSnapshotDownload()} that refused everything.
   */
  @Test
  void aPeerLeaderIsStillDownloadedFromAndResolvesTheFloor(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    sm.setRaftHAServer(followerRaftHAServerMock());
    try {
      setStaleSnapshotAppliedFloor(sm, PERSISTED_APPLIED);

      sm.triggerSnapshotDownload();

      assertThat(sm.getStaleSnapshotAppliedFloor())
          .as("a resync from a real peer resolves the floor")
          .isEqualTo(-1L);
    } finally {
      sm.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static RaftHAServer followerRaftHAServerMock() {
    final RaftHAServer mockRaft = mock(RaftHAServer.class);
    when(mockRaft.isLeader()).thenReturn(false);
    when(mockRaft.getLeaderId()).thenReturn(LEADER_PEER_ID);
    // The resolver every resync path now goes through: it withholds an address that does not identify one
    // peer on its own, and refusing is the whole point of it (issue #6202).
    when(mockRaft.getUnambiguousPeerHttpAddress(LEADER_PEER_ID)).thenReturn("peer-b:2480");
    return mockRaft;
  }

  /** The peer id the mocked leader answers to. */
  private static final RaftPeerId LEADER_PEER_ID = RaftPeerId.valueOf("peer-b_2434");

  private static long readLastRetryMs(final ArcadeStateMachine sm) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("lastStaleSnapshotRetryMs");
    f.setAccessible(true);
    return ((AtomicLong) f.get(sm)).get();
  }

  private static void setLastRetryMs(final ArcadeStateMachine sm, final long value) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("lastStaleSnapshotRetryMs");
    f.setAccessible(true);
    ((AtomicLong) f.get(sm)).set(value);
  }

  private static void setAtomicBoolean(final ArcadeStateMachine sm, final String name, final boolean value) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField(name);
    f.setAccessible(true);
    ((AtomicBoolean) f.get(sm)).set(value);
  }

  /**
   * A real (unstarted) {@link ArcadeDBServer} rooted at {@code tempDir} so the state machine can resolve
   * {@code .raft/applied-index} and read the HA configuration.
   */
  private static ArcadeStateMachine newStateMachine(final Path tempDir) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(new ArcadeDBServer(config));
    return sm;
  }

  /**
   * Writes a real {@code snapshot.<term>_<index>} marker through the state machine's own registration
   * path, so {@code storage.getLatestSnapshot()} rediscovers it the way a restart would.
   */
  private static void registerMarkerAt(final ArcadeStateMachine sm, final long term, final long index) throws Exception {
    final Method m = ArcadeStateMachine.class.getDeclaredMethod("registerSnapshotMarker", long.class, long.class);
    m.setAccessible(true);
    assertThat((Boolean) m.invoke(sm, term, index)).as("snapshot marker written").isTrue();
  }

  private static long readLastAppliedIndex(final ArcadeStateMachine sm) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("lastAppliedIndex");
    f.setAccessible(true);
    return ((AtomicLong) f.get(sm)).get();
  }

  private static void setStaleSnapshotAppliedFloor(final ArcadeStateMachine sm, final long floor) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("staleSnapshotAppliedFloor");
    f.setAccessible(true);
    ((AtomicLong) f.get(sm)).set(floor);
  }

  private static RaftStorage newFormattedStorage(final Path dir) throws IOException {
    return RaftStorage.newBuilder()
        .setDirectory(dir.toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
  }

  /**
   * Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null
   * {@code getId()}; nothing else is reached on this path.
   */
  private static RaftServer stubRaftServer() {
    return (RaftServer) Proxy.newProxyInstance(
        Issue6111StaleSnapshotReadFloorTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }

  /**
   * A {@link RaftHAServer} without Ratis started (no gRPC, no election), with a stubbed division whose
   * applied/commit index is test-controlled, and with {@code stateMachine} wired to {@code sm} so the
   * trusted-index clamp can see its floor. Mirrors {@link ApplyWaitBoundedRecheckTest}'s harness.
   */
  private static RaftHAServer newDetachedRaftHAServer(final AtomicLong appliedIndex, final long commitIndex,
      final ArcadeStateMachine sm, final long quorumTimeoutMs) throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, quorumTimeoutMs);

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("localhost");

    final RaftHAServer raft = new RaftHAServer(mockServer, config);

    final DivisionInfo mockInfo = mock(DivisionInfo.class);
    when(mockInfo.getLastAppliedIndex()).thenAnswer(inv -> appliedIndex.get());

    final RaftLog mockRaftLog = mock(RaftLog.class);
    when(mockRaftLog.getLastCommittedIndex()).thenReturn(commitIndex);

    final RaftServer.Division mockDivision = mock(RaftServer.Division.class);
    when(mockDivision.getInfo()).thenReturn(mockInfo);
    when(mockDivision.getRaftLog()).thenReturn(mockRaftLog);

    final RaftServer mockRaftServer = mock(RaftServer.class);
    when(mockRaftServer.getDivision(any())).thenReturn(mockDivision);

    setRaftHAServerField(raft, "raftServer", mockRaftServer);
    setRaftHAServerField(raft, "stateMachine", sm);
    return raft;
  }

  private static void setRaftHAServerField(final Object target, final String name, final Object value) throws Exception {
    final Field f = RaftHAServer.class.getDeclaredField(name);
    f.setAccessible(true);
    f.set(target, value);
  }
}
