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
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for issue #575.
 * <p>
 * At leader-transition boundaries, Ratis can call
 * {@link ArcadeStateMachine#notifyTermIndexUpdated(long, long)} with a term that is lower than
 * the term of the last snapshot. This happens because
 * {@link ArcadeStateMachine#notifyInstallSnapshotFromLeader} records the snapshot term as
 * {@code firstTermIndexInLog.getTerm()} (an upper-bound from the first log entry after the
 * snapshot), which can exceed the term of committed entries at nearby indices from the old leader.
 * <p>
 * Ratis 3.2.2's {@code BaseStateMachine.updateLastAppliedTermIndex} throws
 * {@link IllegalStateException} when its monotonicity assertion detects such a backward term.
 * Before the fix this exception propagated to {@code StateMachineUpdater.run()}, which responded
 * by calling {@code raftServer.shutdown()} - turning a transient edge case into an unclean
 * shutdown that left all cluster nodes stuck in split-vote ({@code VOTING_FOR_ME}) after restart.
 */
class Issue575BackwardTermNotifyTest {

  /**
   * Reproduces the exact error reported in issue #575:
   * {@code newTI = (t:10, i:39707284) < oldTI = (t:11, i:39707283)}.
   * <p>
   * Before the fix, calling {@code notifyTermIndexUpdated(10, 39707284)} after
   * {@code lastAppliedTermIndex} was set to {@code (t:11, i:39707283)} threw
   * {@link IllegalStateException} and aborted the StateMachineUpdater thread.
   * After the fix the call completes normally, the applied index advances, and the
   * state machine remains usable.
   */
  @Test
  void backwardTermAtLeaderTransitionBoundaryDoesNotThrow(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), newFormattedStorage(tempDir));

      // Simulate what notifyInstallSnapshotFromLeader does: it sets lastAppliedTermIndex to
      // (snapshotTerm=11, snapshotIndex=39707283) using firstTermIndexInLog.getTerm()=11
      // even though committed entries at nearby indices may still carry old term=10.
      advanceApplied(sm, 11L, 39707283L);
      assertThat(sm.getLastAppliedTermIndex())
          .as("precondition: lastAppliedTermIndex must be (t:11, i:39707283)")
          .isEqualTo(TermIndex.valueOf(11L, 39707283L));

      // The exact call that triggered the IllegalStateException in issue #575:
      // a committed entry from the old leader (term=10) at the next index (39707284).
      assertThatCode(() -> sm.notifyTermIndexUpdated(10L, 39707284L))
          .as("notifyTermIndexUpdated must not throw when term goes backward at a leader-transition boundary")
          .doesNotThrowAnyException();

      // Ratis's getAndSet already advanced the value before the assertion; verify it is correct.
      assertThat(sm.getLastAppliedTermIndex())
          .as("lastAppliedTermIndex must reflect the entry that was just applied")
          .isEqualTo(TermIndex.valueOf(10L, 39707284L));

    } finally {
      sm.close();
    }
  }

  /**
   * Verifies the normal (non-backward) case still works: a monotonically-advancing
   * term-index sequence must succeed without any exception.
   */
  @Test
  void forwardTermNotifySucceeds(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), newFormattedStorage(tempDir));

      advanceApplied(sm, 5L, 100L);

      assertThatCode(() -> sm.notifyTermIndexUpdated(5L, 101L))
          .as("same-term forward entry must succeed")
          .doesNotThrowAnyException();
      assertThat(sm.getLastAppliedTermIndex()).isEqualTo(TermIndex.valueOf(5L, 101L));

      assertThatCode(() -> sm.notifyTermIndexUpdated(6L, 102L))
          .as("higher-term forward entry must succeed")
          .doesNotThrowAnyException();
      assertThat(sm.getLastAppliedTermIndex()).isEqualTo(TermIndex.valueOf(6L, 102L));

    } finally {
      sm.close();
    }
  }

  // -- helpers --

  private static RaftStorage newFormattedStorage(final Path dir) throws IOException {
    return RaftStorage.newBuilder()
        .setDirectory(dir.toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
  }

  private static RaftServer stubServer() {
    return (RaftServer) java.lang.reflect.Proxy.newProxyInstance(
        Issue575BackwardTermNotifyTest.class.getClassLoader(),
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
   * Advances the state machine's applied position the way {@code applyTransaction} does:
   * sets both the private {@code lastAppliedIndex} counter and the inherited
   * {@code BaseStateMachine.lastAppliedTermIndex}.
   */
  private static void advanceApplied(final ArcadeStateMachine sm, final long term, final long index) throws Exception {
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
      }
    }
    throw new NoSuchMethodException(name);
  }
}
