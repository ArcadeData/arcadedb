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
import org.apache.ratis.server.storage.RaftStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Regression tests for issue #5846.
 * <p>
 * A follower that catches up through a snapshot install advances {@code lastAppliedIndex} directly in
 * {@link ArcadeStateMachine#reinitialize()} - called by Ratis's {@code StateMachineUpdater.reload()}
 * after the snapshot is installed - without going through {@link ArcadeStateMachine#applyTransaction},
 * the only method that used to call {@link RaftHAServer#notifyApplied()}. Any thread blocked in
 * {@link RaftHAServer#waitForAppliedIndex(long, boolean)} or {@link RaftHAServer#waitForLocalApply()}
 * for an index the snapshot already covers was never woken, and slept until the full quorum timeout:
 * a spurious {@code ReplicationException} (HTTP 503) for LINEARIZABLE reads, or a bogus "consistency
 * degraded to EVENTUAL" warning for READ_YOUR_WRITES reads.
 * <p>
 * The fix makes {@code reinitialize()} call {@code notifyApplied()} whenever it seeds
 * {@code lastAppliedIndex} from a snapshot. This test drives {@code reinitialize()} directly against a
 * real {@link org.apache.ratis.statemachine.impl.SimpleStateMachineStorage} snapshot marker (registered
 * via the real {@link ArcadeStateMachine#takeSnapshot()}) and a mocked {@link RaftHAServer}, verifying
 * the notify call fires - deterministically, without depending on cluster timing.
 */
class ArcadeStateMachineReinitializeNotifiesTest {

  @Test
  void reinitializeNotifiesWaitersWhenASnapshotIsPresent(@TempDir final Path tempDir) throws Exception {
    final long appliedIndex = 4242L;
    final long appliedTerm = 7L;

    final RaftGroupId groupId = RaftGroupId.valueOf(UUID.randomUUID());
    final RaftStorage raftStorage = newFormattedStorage(tempDir);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final RaftHAServer mockRaft = mock(RaftHAServer.class);
    try {
      sm.initialize(stubServer(), groupId, raftStorage);

      // Simulate having applied entries up to (appliedTerm, appliedIndex), as applyTransaction would,
      // then register a real snapshot marker for it - the state reinitialize() will rediscover.
      setLastApplied(sm, appliedTerm, appliedIndex);
      sm.takeSnapshot();

      sm.setRaftHAServer(mockRaft);

      // reinitialize() must both re-seed lastAppliedIndex from the snapshot AND notify: this is the
      // call StateMachineUpdater.reload() makes right after installing a snapshot.
      sm.reinitialize();

      verify(mockRaft, times(1)).notifyApplied();
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /**
   * A plain startup reinitialize() with no snapshot on disk (fresh cluster / fresh database) must not
   * call notifyApplied() - there is nothing for a waiter to have missed, and BaseStateMachine's
   * lifecycle transition (PAUSED -> RUNNING) is not exercised here either.
   */
  @Test
  void reinitializeDoesNotNotifyWhenNoSnapshotExists(@TempDir final Path tempDir) throws Exception {
    final RaftGroupId groupId = RaftGroupId.valueOf(UUID.randomUUID());
    final RaftStorage raftStorage = newFormattedStorage(tempDir);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final RaftHAServer mockRaft = mock(RaftHAServer.class);
    try {
      sm.initialize(stubServer(), groupId, raftStorage);
      sm.setRaftHAServer(mockRaft);

      sm.reinitialize();

      verify(mockRaft, never()).notifyApplied();
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /**
   * {@code raftHAServer} is null until {@link RaftHAServer} finishes wiring itself up
   * ({@link ArcadeStateMachine#setRaftHAServer}), and {@code reinitialize()} also runs during the
   * normal startup path (from {@code initialize()}) before that wiring happens. It must not throw.
   */
  @Test
  void reinitializeDoesNotThrowWhenRaftHAServerNotYetAttached(@TempDir final Path tempDir) throws Exception {
    final RaftGroupId groupId = RaftGroupId.valueOf(UUID.randomUUID());
    final RaftStorage raftStorage = newFormattedStorage(tempDir);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubServer(), groupId, raftStorage);
      setLastApplied(sm, 7L, 4242L);
      sm.takeSnapshot();

      assertThatNoException().isThrownBy(sm::reinitialize);
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
  private static RaftServer stubServer() {
    return (RaftServer) java.lang.reflect.Proxy.newProxyInstance(
        ArcadeStateMachineReinitializeNotifiesTest.class.getClassLoader(),
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

  private static Method findMethod(final Class<?> type, final String name, final Class<?>... params) throws NoSuchMethodException {
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
