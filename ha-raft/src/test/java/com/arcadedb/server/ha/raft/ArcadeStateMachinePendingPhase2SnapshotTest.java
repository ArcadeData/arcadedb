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

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5407.
 * <p>
 * On the leader a locally-originated transaction is written locally by
 * {@code RaftReplicatedDatabase.commit}'s phase 2, which runs AFTER Raft has committed the entry.
 * The state machine therefore origin-skips the data apply but still advances {@code lastAppliedIndex}
 * the moment Ratis commits. If a snapshot checkpoint is taken in that window - and Ratis takes one on
 * shutdown - the marker records an index whose pages were never written. Because
 * {@code reinitialize()} seeds the replay position exclusively from that marker, a restart starts
 * replaying above the entry and the write is dropped on this node permanently.
 * <p>
 * The fix registers a phase-2 "replay floor" before replication and clamps {@code takeSnapshot()} to
 * the oldest in-flight floor, so such an entry stays inside the replay window until phase 2 confirms.
 */
class ArcadeStateMachinePendingPhase2SnapshotTest {

  /**
   * The core lost-write scenario: phase 2 is in flight when the entry commits, so the checkpoint must
   * stay BELOW the committed-but-unapplied entry. Before the fix {@code takeSnapshot()} reported the
   * entry's own index, which a restart then treated as durable.
   */
  @Test
  void takeSnapshotClampsToPendingPhase2Floor(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);

      // Baseline transactions are applied and durable up to index 8.
      setLastApplied(sm, 1L, 8L);

      // A locally-originated commit starts replicating: its entry does not exist yet, so the floor is 8.
      final long ticket = sm.beginLocalPhase2();

      // Raft commits the entry at index 9; applyTransaction origin-skips it and advances the index,
      // but phase 2 has not written the pages yet.
      setLastApplied(sm, 1L, 9L);

      assertThat(sm.takeSnapshot())
          .as("checkpoint must not cover index 9, whose phase 2 has not confirmed")
          .isEqualTo(8L);

      final SingleFileSnapshotInfo marker = (SingleFileSnapshotInfo) sm.getStateMachineStorage().getLatestSnapshot();
      assertThat(marker).as("a marker must still be written at the clamped index").isNotNull();
      assertThat(marker.getIndex())
          .as("the persisted marker is the only replay position a restart trusts, so it must be 8")
          .isEqualTo(8L);

      sm.endLocalPhase2(ticket);
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /**
   * Once phase 2 confirms, the clamp must lift so log compaction is not stalled for the node's lifetime.
   */
  @Test
  void takeSnapshotAdvancesOncePhase2Completes(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);

      setLastApplied(sm, 1L, 8L);
      final long ticket = sm.beginLocalPhase2();
      setLastApplied(sm, 1L, 9L);
      assertThat(sm.takeSnapshot()).isEqualTo(8L);

      sm.endLocalPhase2(ticket);

      assertThat(sm.takeSnapshot())
          .as("with no phase 2 in flight the checkpoint must reach the applied index again")
          .isEqualTo(9L);
      assertThat(sm.getStateMachineStorage().getLatestSnapshot().getIndex()).isEqualTo(9L);
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /**
   * The clamp is the minimum across all in-flight commits: a single laggard must hold the checkpoint
   * back even when newer commits have already drained.
   */
  @Test
  void takeSnapshotClampsToTheOldestInFlightCommit(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);

      setLastApplied(sm, 1L, 5L);
      final long oldest = sm.beginLocalPhase2();

      setLastApplied(sm, 1L, 12L);
      final long newer = sm.beginLocalPhase2();

      setLastApplied(sm, 1L, 20L);

      sm.endLocalPhase2(newer);

      assertThat(sm.takeSnapshot())
          .as("the oldest unconfirmed phase 2 governs the checkpoint")
          .isEqualTo(5L);

      sm.endLocalPhase2(oldest);
      assertThat(sm.takeSnapshot()).isEqualTo(20L);
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /**
   * The clamp must never move the marker backwards: a previous checkpoint already authorised Ratis to
   * purge the log up to it, so replaying from below it could hit purged entries. When the clamp would
   * regress, no checkpoint is reported and the existing marker is left untouched.
   */
  @Test
  void takeSnapshotNeverRegressesBelowAnExistingMarker(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);

      setLastApplied(sm, 1L, 30L);
      assertThat(sm.takeSnapshot()).isEqualTo(30L);

      // Reproduces the narrow interleaving that can strand a floor below an existing marker: a commit
      // reads the applied index, and the checkpoint above completes before the commit registers its
      // floor. Only the counter beginLocalPhase2 samples is rewound - the Ratis term/index is
      // strictly monotonic and is left where the checkpoint saw it.
      setLastAppliedIndexOnly(sm, 10L);
      final long ticket = sm.beginLocalPhase2();
      setLastApplied(sm, 1L, 31L);

      assertThat(sm.takeSnapshot())
          .as("a clamp below the existing marker must not authorise a purge")
          .isEqualTo(RaftLog.INVALID_LOG_INDEX);
      assertThat(sm.getStateMachineStorage().getLatestSnapshot().getIndex())
          .as("the existing marker must be left untouched")
          .isEqualTo(30L);

      sm.endLocalPhase2(ticket);
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /**
   * A phase 2 that starts before anything has ever been applied floors at -1, which must suppress the
   * checkpoint entirely rather than report a negative purge index to Ratis.
   */
  @Test
  void takeSnapshotIsSuppressedWhenPhase2StartsBeforeAnyApply(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = newFormattedStorage(tempDir);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);

      final long ticket = sm.beginLocalPhase2();
      setLastApplied(sm, 1L, 1L);

      assertThat(sm.takeSnapshot())
          .as("the very first write must stay replayable until its phase 2 confirms")
          .isEqualTo(RaftLog.INVALID_LOG_INDEX);
      assertThat(sm.getStateMachineStorage().getLatestSnapshot())
          .as("no marker may be written while the first write is unconfirmed")
          .isNull();

      sm.endLocalPhase2(ticket);
      assertThat(sm.takeSnapshot()).isEqualTo(1L);
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  private static RaftStorage newFormattedStorage(final Path dir) throws IOException {
    return RaftStorage.newBuilder()
        .setDirectory(dir.toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
  }

  /**
   * Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null
   * {@code getId()}; all other calls are unused on this path.
   */
  private RaftServer stubServer() {
    return (RaftServer) Proxy.newProxyInstance(
        getClass().getClassLoader(),
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
   * Advances the applied position the way {@code applyTransaction} does: the private
   * {@code lastAppliedIndex} counter that {@code takeSnapshot()} reads, plus the BaseStateMachine
   * last-applied term/index that supplies the snapshot term.
   */
  private static void setLastApplied(final ArcadeStateMachine sm, final long term, final long index) throws Exception {
    setLastAppliedIndexOnly(sm, index);

    final Method m = findMethod(sm.getClass(), "updateLastAppliedTermIndex", long.class, long.class);
    m.setAccessible(true);
    m.invoke(sm, term, index);
  }

  /** Sets only the counter {@code takeSnapshot()} and {@code beginLocalPhase2()} read. */
  private static void setLastAppliedIndexOnly(final ArcadeStateMachine sm, final long index) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("lastAppliedIndex");
    f.setAccessible(true);
    ((AtomicLong) f.get(sm)).set(index);
  }

  private static Method findMethod(final Class<?> type, final String name, final Class<?>... params)
      throws NoSuchMethodException {
    for (Class<?> c = type; c != null; c = c.getSuperclass()) {
      try {
        return c.getDeclaredMethod(name, params);
      } catch (final NoSuchMethodException ignored) {
        // walk up to the superclass
      }
    }
    throw new NoSuchMethodException(name);
  }
}
