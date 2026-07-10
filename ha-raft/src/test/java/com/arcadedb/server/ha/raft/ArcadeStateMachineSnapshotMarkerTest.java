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
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4829.
 * <p>
 * {@link ArcadeStateMachine#takeSnapshot()} returns {@code lastAppliedIndex} - the Ratis contract
 * for "state up to this index is durable, you may purge the log up to it". Before the fix it wrote
 * <b>no</b> {@code snapshot.<term>_<index>} file, so {@link SimpleStateMachineStorage#getLatestSnapshot()}
 * (which discovers snapshots by scanning for those files) always returned {@code null}. With
 * auto-snapshot + {@code purgeUptoSnapshotIndex} enabled, Ratis purged log entries up to the returned
 * index even though no snapshot existed; after a restart the node seeded {@code lastAppliedIndex = -1}
 * and tried to replay from the start of a log whose early entries were already purged, so it could
 * never self-recover.
 * <p>
 * The fix persists a real (empty) snapshot marker before returning the purge index, mirroring what
 * {@link ArcadeStateMachine#notifyInstallSnapshotFromLeader} already does on the follower install path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ArcadeStateMachineSnapshotMarkerTest {

  /**
   * After {@code takeSnapshot()} reports a purge index, a freshly-opened {@link SimpleStateMachineStorage}
   * (simulating a process restart) must rediscover a snapshot at exactly that index. Before the fix the
   * fresh storage found nothing and {@code getLatestSnapshot()} returned {@code null}.
   */
  @Test
  void takeSnapshotWritesDiscoverableSnapshotMarker(@TempDir final Path tempDir) throws Exception {
    final long appliedIndex = 4242L;
    final long appliedTerm = 7L;

    final RaftGroupId groupId = RaftGroupId.valueOf(UUID.randomUUID());
    final RaftStorage raftStorage = newFormattedStorage(tempDir);

    try (final ArcadeStateMachine sm = new ArcadeStateMachine()) {
      sm.initialize(stubServer(), groupId, raftStorage);

      // Simulate having applied entries up to (appliedTerm, appliedIndex), as applyTransaction would.
      setLastApplied(sm, appliedTerm, appliedIndex);

      final long reported = sm.takeSnapshot();
      assertThat(reported)
          .as("takeSnapshot() must report the last-applied index as the purge point")
          .isEqualTo(appliedIndex);

      // The live storage must now expose the snapshot (StateMachineUpdater.reload() relies on this).
      final SingleFileSnapshotInfo live = (SingleFileSnapshotInfo) sm.getStateMachineStorage().getLatestSnapshot();
      assertThat(live).as("takeSnapshot() must register a snapshot in the live storage").isNotNull();
      assertThat(live.getIndex()).isEqualTo(appliedIndex);
      assertThat(live.getTerm()).isEqualTo(appliedTerm);
    } finally {
      raftStorage.close();
    }

    // Simulate a restart: a brand-new storage opened on the same directory must rediscover the marker.
    try (final RaftStorage reopened = newRecoveredStorage(tempDir)) {
      final SimpleStateMachineStorage freshStorage = new SimpleStateMachineStorage();
      freshStorage.init(reopened);
      final SingleFileSnapshotInfo discovered = freshStorage.getLatestSnapshot();
      assertThat(discovered)
          .as("""
              after restart the snapshot marker written by takeSnapshot() must be rediscovered \
              (was null before the #4829 fix, orphaning purged log state)""")
          .isNotNull();
      assertThat(discovered.getIndex()).isEqualTo(appliedIndex);
      assertThat(discovered.getTerm()).isEqualTo(appliedTerm);
    }
  }

  /**
   * When nothing has been applied yet ({@code lastAppliedIndex == -1}) {@code takeSnapshot()} must
   * report {@link RaftLog#INVALID_LOG_INDEX} and must NOT write a marker - returning a real index
   * without a backing snapshot is exactly the bug #4829 fixes.
   */
  @Test
  void takeSnapshotWithoutAppliedEntriesWritesNoMarker(@TempDir final Path tempDir) throws Exception {
    final RaftGroupId groupId = RaftGroupId.valueOf(UUID.randomUUID());
    final RaftStorage raftStorage = newFormattedStorage(tempDir);

    try (final ArcadeStateMachine sm = new ArcadeStateMachine()) {
      sm.initialize(stubServer(), groupId, raftStorage);

      final long reported = sm.takeSnapshot();
      assertThat(reported)
          .as("with nothing applied, takeSnapshot() must not authorise a log purge")
          .isEqualTo(RaftLog.INVALID_LOG_INDEX);
      assertThat(sm.getStateMachineStorage().getLatestSnapshot())
          .as("no snapshot marker must be written when nothing has been applied")
          .isNull();
    } finally {
      raftStorage.close();
    }
  }

  private static RaftStorage newFormattedStorage(final Path dir) throws IOException {
    return RaftStorage.newBuilder()
        .setDirectory(dir.toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
  }

  private static RaftStorage newRecoveredStorage(final Path dir) throws IOException {
    return RaftStorage.newBuilder()
        .setDirectory(dir.toFile())
        .setOption(RaftStorage.StartupOption.RECOVER)
        .build();
  }

  /**
   * Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null
   * {@code getId()}; all other calls are unused on this path.
   */
  private RaftServer stubServer() {
    return (RaftServer) java.lang.reflect.Proxy.newProxyInstance(
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
   * Advances the state machine's applied position the way {@code applyTransaction} does: the private
   * {@code lastAppliedIndex} counter that {@code takeSnapshot()} reads, plus the BaseStateMachine
   * last-applied term/index that supplies the snapshot term.
   */
  private static void setLastApplied(final ArcadeStateMachine sm, final long term, final long index) throws Exception {
    final java.lang.reflect.Field f = ArcadeStateMachine.class.getDeclaredField("lastAppliedIndex");
    f.setAccessible(true);
    ((AtomicLong) f.get(sm)).set(index);

    final Method m = findMethod(sm.getClass(), "updateLastAppliedTermIndex", long.class, long.class);
    m.setAccessible(true);
    m.invoke(sm, term, index);
  }

  private static Method findMethod(Class<?> type, final String name, final Class<?>... params) throws NoSuchMethodException {
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
