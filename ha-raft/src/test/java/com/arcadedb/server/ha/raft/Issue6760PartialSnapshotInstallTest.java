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
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #6760: a leader-driven snapshot install that GIVES UP on a database must not record
 * that database as installed.
 * <p>
 * The reconciler stops failing the whole install for a database that has failed {@code ACQUIRE_GIVE_UP_AFTER}
 * times in a row, so Ratis is not made to re-download every healthy database on the node in a tight loop. That is
 * the right call for the retry - but the install then went on to write {@code snapshotIndex} as applied for EVERY
 * database, clear the read floor and the diverged marks, and return the installed {@code TermIndex} to Ratis,
 * which purges the log. The node re-entered the ready set advertising itself fully caught up while one database
 * was still on its old copy, and a LINEARIZABLE (or read-your-writes) read of that database passed the apply wait
 * instantly and was served from stale state.
 * <p>
 * What must hold instead: the given-up database keeps its diverged mark and gets a read floor of its own, its
 * persisted applied position is left where it really is, and the healthy co-located databases are untouched -
 * they still serve unclamped reads, which is the whole reason the give-up threshold exists.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6760PartialSnapshotInstallTest {
  private static final String LEADER_PEER_ID    = "peer-b_2434";
  private static final String LOCAL_HTTP        = "localhost:2480";
  private static final long   FIRST_LOG_INDEX   = 5_000L;
  private static final long   SNAPSHOT_INDEX    = FIRST_LOG_INDEX - 1;
  private static final long   PERSISTED_APPLIED = 100L;
  private static final String STALE_DB          = "db-stale";
  private static final String HEALTHY_DB        = "db-ok";

  // -----------------------------------------------------------------------------------------------
  // The install itself
  // -----------------------------------------------------------------------------------------------

  /**
   * The core regression. The install completes - it must, or the give-up threshold buys nothing - but the
   * database it gave up on is left visibly behind instead of being laundered into "applied".
   */
  @Test
  void aGivenUpDatabaseIsNotRecordedAsInstalled(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(STALE_DB, HEALTHY_DB));
    replaceReconciler(sm, new StubReconciler(Set.of(STALE_DB)));
    sm.setRaftHAServer(followerRaft());
    try {
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, STALE_DB);
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, HEALTHY_DB);

      final TermIndex installed = sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(),
          TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();

      assertThat(installed.getIndex())
          .as("Ratis is still told what it needs, so it stops re-driving the install")
          .isEqualTo(SNAPSHOT_INDEX);

      assertThat(sm.readPersistedAppliedIndex(HEALTHY_DB))
          .as("the database that WAS refreshed really is at the snapshot index")
          .isEqualTo(SNAPSHOT_INDEX);
      assertThat(sm.readPersistedAppliedIndex(STALE_DB))
          .as("the one that was not stays at the position it genuinely reached")
          .isEqualTo(PERSISTED_APPLIED);

      assertThat(sm.getDatabaseAppliedFloor(STALE_DB))
          .as("and publishes a read floor there, so reads of it cannot be served from the stale copy")
          .isEqualTo(PERSISTED_APPLIED);
      assertThat(sm.getDatabaseAppliedFloor(HEALTHY_DB))
          .as("the healthy database is clamped by nothing")
          .isEqualTo(-1L);

      assertThat(sm.isDatabaseDiverged(STALE_DB))
          .as("it is still diverged: the install did not restore it")
          .isTrue();
      assertThat(sm.isDatabaseDiverged(HEALTHY_DB)).isFalse();
      assertThat(sm.isResyncInProgress())
          .as("so the node must not advertise itself ready")
          .isTrue();
    } finally {
      sm.close();
    }
  }

  /**
   * Control: an install that gave up on nothing behaves exactly as before - every database at the snapshot index,
   * no floors, nothing diverged, node ready.
   */
  @Test
  void aCleanInstallIsUnchanged(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(STALE_DB, HEALTHY_DB));
    replaceReconciler(sm, new StubReconciler(Set.of()));
    sm.setRaftHAServer(followerRaft());
    try {
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, STALE_DB);
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, HEALTHY_DB);

      sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();

      assertThat(sm.readPersistedAppliedIndex(STALE_DB)).isEqualTo(SNAPSHOT_INDEX);
      assertThat(sm.readPersistedAppliedIndex(HEALTHY_DB)).isEqualTo(SNAPSHOT_INDEX);
      assertThat(sm.getDatabaseAppliedFloor(STALE_DB)).isEqualTo(-1L);
      assertThat(sm.isResyncInProgress()).isFalse();
    } finally {
      sm.close();
    }
  }

  /** A later install that does refresh the database resolves everything the partial one left behind. */
  @Test
  void aLaterSuccessfulInstallClearsTheFloor(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(STALE_DB, HEALTHY_DB));
    final StubReconciler reconciler = new StubReconciler(Set.of(STALE_DB));
    replaceReconciler(sm, reconciler);
    sm.setRaftHAServer(followerRaft());
    try {
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, STALE_DB);
      sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();
      assertThat(sm.getDatabaseAppliedFloor(STALE_DB)).isEqualTo(PERSISTED_APPLIED);

      // The leader's copy was fixed: this pass gives up on nothing.
      reconciler.givenUp = Set.of();
      sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), TermIndex.valueOf(9L, FIRST_LOG_INDEX + 1)).get();

      assertThat(sm.getDatabaseAppliedFloor(STALE_DB)).isEqualTo(-1L);
      assertThat(sm.isDatabaseDiverged(STALE_DB)).isFalse();
      assertThat(sm.isResyncInProgress()).isFalse();
    } finally {
      sm.close();
    }
  }

  /**
   * The floor is a per-database read floor, so the HealthMonitor backstop has to treat it as an unfilled gap:
   * nothing else re-arms a resync for a database the install stopped retrying.
   */
  @Test
  void aPerDatabaseFloorKeepsTheResyncBackstopArmed(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(STALE_DB));
    replaceReconciler(sm, new StubReconciler(Set.of(STALE_DB)));
    sm.setRaftHAServer(followerRaft());
    try {
      sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();
      assertThat(sm.getStaleSnapshotAppliedFloor()).as("the GLOBAL floor is resolved, only the database is not")
          .isEqualTo(-1L);

      sm.retryUnfilledSnapshotGap();

      assertThat(readLastRetryMs(sm))
          .as("the backstop must submit a resync for the database still short of the snapshot index")
          .isNotZero();
    } finally {
      sm.close();
    }
  }

  /**
   * The per-database floor must be published BEFORE any waiter is woken (PR #6783 review).
   * <p>
   * {@code notifyApplied()} holds {@code applyNotifier} only long enough to {@code notifyAll()}, so a waiter can
   * reacquire it and re-check {@code getTrustedAppliedIndex(db)} at once. Notifying before the re-arm leaves a
   * window where the global floor is already cleared and the per-database one is not yet published, so the waiter
   * sees the raw Ratis index - which already equals snapshotIndex - and the stale read #6760 exists to prevent
   * happens anyway, just in a narrower window.
   * <p>
   * Driven by observing the state at the instant of the notify rather than by racing threads, so it is
   * deterministic: a regression flips the captured floor to -1 every time.
   */
  @Test
  void theDatabaseFloorIsPublishedBeforeAnyWaiterIsWoken(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir, Set.of(STALE_DB, HEALTHY_DB));
    replaceReconciler(sm, new StubReconciler(Set.of(STALE_DB)));

    final AtomicLong floorSeenByAWokenWaiter = new AtomicLong(Long.MIN_VALUE);
    final RaftHAServer raft = followerRaft();
    // Stands in for the woken waiter: notifyApplied() is the moment it can re-check, so whatever the floor reads
    // here is exactly what that waiter would have based its decision on.
    org.mockito.Mockito.doAnswer(inv -> {
      floorSeenByAWokenWaiter.set(sm.getDatabaseAppliedFloor(STALE_DB));
      return null;
    }).when(raft).notifyApplied();
    sm.setRaftHAServer(raft);

    try {
      sm.writePersistedAppliedIndex(PERSISTED_APPLIED, STALE_DB);

      sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();

      assertThat(floorSeenByAWokenWaiter.get())
          .as("a waiter woken by this install must already see the floor that keeps it off the stale copy")
          .isEqualTo(PERSISTED_APPLIED);
    } finally {
      sm.close();
    }
  }

  // -----------------------------------------------------------------------------------------------
  // The read gate
  // -----------------------------------------------------------------------------------------------

  /** The observable half: a LINEARIZABLE read of the stale database fails, while the healthy one is served. */
  @Test
  void theReadGateClampsOnlyTheStaleDatabase() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    publishDatabaseFloor(sm, STALE_DB, PERSISTED_APPLIED);
    final RaftHAServer raft = newDetachedRaftHAServer(new AtomicLong(SNAPSHOT_INDEX), SNAPSHOT_INDEX, sm, 500L);

    assertThat(raft.getLastAppliedIndex())
        .as("Ratis still advertises the snapshot index for the whole node")
        .isEqualTo(SNAPSHOT_INDEX);
    assertThat(raft.getTrustedAppliedIndex(STALE_DB))
        .as("but only the floor is backed by local data for this database")
        .isEqualTo(PERSISTED_APPLIED);
    assertThat(raft.getTrustedAppliedIndex(HEALTHY_DB))
        .as("and the co-located healthy database is not clamped at all")
        .isEqualTo(SNAPSHOT_INDEX);

    assertThatThrownBy(() -> raft.waitForAppliedIndex(STALE_DB, PERSISTED_APPLIED + 1, true))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("LINEARIZABLE read timed out");

    assertThatNoException()
        .as("the whole point of the per-database floor: one bad database does not stop the others")
        .isThrownBy(() -> raft.waitForAppliedIndex(HEALTHY_DB, SNAPSHOT_INDEX, true));
  }

  /** A target at or below the database's own floor is genuinely on disk and must still be served. */
  @Test
  void aReadBelowTheDatabaseFloorStillSucceeds() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    publishDatabaseFloor(sm, STALE_DB, PERSISTED_APPLIED);
    final RaftHAServer raft = newDetachedRaftHAServer(new AtomicLong(SNAPSHOT_INDEX), SNAPSHOT_INDEX, sm, 500L);

    assertThatNoException().isThrownBy(() -> raft.waitForAppliedIndex(STALE_DB, PERSISTED_APPLIED, true));
  }

  /**
   * The lenient (READ_YOUR_WRITES / bookmark) waiter keeps its degrade-do-not-fail contract, and must not burn
   * the whole quorum timeout on a gap only a resync can close - the same treatment the global floor gets.
   */
  @Test
  void readYourWritesDegradesImmediatelyOnAPerDatabaseFloor() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    publishDatabaseFloor(sm, STALE_DB, PERSISTED_APPLIED);
    final RaftHAServer raft = newDetachedRaftHAServer(new AtomicLong(SNAPSHOT_INDEX), SNAPSHOT_INDEX, sm, 10_000L);

    assertThat(raft.getTrustedAppliedIndex(STALE_DB))
        .as("the clamp must be in force, otherwise the waiter never enters the loop and proves nothing")
        .isEqualTo(PERSISTED_APPLIED);

    final long start = System.currentTimeMillis();
    assertThatNoException().isThrownBy(() -> raft.waitForAppliedIndex(STALE_DB, SNAPSHOT_INDEX, false));
    assertThat(System.currentTimeMillis() - start)
        .as("waiting cannot help while the floor stands, so the lenient waiter degrades at once")
        .isLessThan(2_000L);
  }

  /** With no floor anywhere the trusted index is the raw Ratis one, named database or not. */
  @Test
  void withoutAnyFloorTheTrustedIndexIsTheRatisIndex() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final RaftHAServer raft = newDetachedRaftHAServer(new AtomicLong(SNAPSHOT_INDEX), SNAPSHOT_INDEX, sm, 500L);

    assertThat(raft.getTrustedAppliedIndex()).isEqualTo(SNAPSHOT_INDEX);
    assertThat(raft.getTrustedAppliedIndex(STALE_DB)).isEqualTo(SNAPSHOT_INDEX);
  }

  // -----------------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------------

  /** A reconciler that touches nothing and reports exactly the databases the test wants given up on. */
  private static class StubReconciler extends DatabaseReconciler {
    private volatile Set<String> givenUp;

    private StubReconciler(final Set<String> givenUp) {
      this.givenUp = givenUp;
    }

    @Override
    Set<String> reconcileDatabasesFromLeader(final String leaderHttpAddr, final String leaderHttpsAddr,
        final String clusterToken) {
      return givenUp;
    }
  }

  private static RaftHAServer followerRaft() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(false);
    when(raft.getLeaderId()).thenReturn(RaftPeerId.valueOf(LEADER_PEER_ID));
    when(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID))).thenReturn("peer-b:2480");
    when(raft.getLocalHttpAddress()).thenReturn(LOCAL_HTTP);
    return raft;
  }

  private static RaftProtos.RoleInfoProto leaderRoleInfo() {
    return RaftProtos.RoleInfoProto.newBuilder()
        .setFollowerInfo(RaftProtos.FollowerInfoProto.newBuilder()
            .setLeaderInfo(RaftProtos.ServerRpcProto.newBuilder()
                .setId(RaftProtos.RaftPeerProto.newBuilder()
                    .setId(ByteString.copyFromUtf8(LEADER_PEER_ID)))))
        .build();
  }

  /**
   * A state machine with real Ratis storage (the install registers a snapshot marker through it) and a server
   * that reports {@code databaseNames} as present, which is what the applied-index bookkeeping iterates.
   */
  private static ArcadeStateMachine newStateMachine(final Path tempDir, final Set<String> databaseNames)
      throws IOException {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    config.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.getDatabaseNames()).thenReturn(databaseNames);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()),
        newFormattedStorage(tempDir.resolve("raft")));
    return sm;
  }

  private static RaftStorage newFormattedStorage(final Path dir) throws IOException {
    return RaftStorage.newBuilder()
        .setDirectory(dir.toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
  }

  private static RaftServer stubRaftServer() {
    return (RaftServer) Proxy.newProxyInstance(
        Issue6760PartialSnapshotInstallTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }

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

    setField(RaftHAServer.class, raft, "raftServer", mockRaftServer);
    setField(RaftHAServer.class, raft, "stateMachine", sm);
    return raft;
  }

  @SuppressWarnings("unchecked")
  private static void publishDatabaseFloor(final ArcadeStateMachine sm, final String dbName, final long floor)
      throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("staleDatabaseAppliedFloors");
    f.setAccessible(true);
    ((java.util.Map<String, Long>) f.get(sm)).put(dbName, floor);
  }

  private static long readLastRetryMs(final ArcadeStateMachine sm) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("lastStaleSnapshotRetryMs");
    f.setAccessible(true);
    return ((AtomicLong) f.get(sm)).get();
  }

  private static void replaceReconciler(final ArcadeStateMachine sm, final DatabaseReconciler reconciler)
      throws Exception {
    setField(ArcadeStateMachine.class, sm, "reconciler", reconciler);
  }

  private static void setField(final Class<?> owner, final Object target, final String name, final Object value)
      throws Exception {
    final Field f = owner.getDeclaredField(name);
    f.setAccessible(true);
    f.set(target, value);
  }
}
