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
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4754.
 * <p>
 * {@code StateMachineUpdater.reload()} (Ratis 3.2.2, line 230) asserts
 * {@code getLifeCycleState() == PAUSED} before calling {@code reinitialize()},
 * then asserts {@code getLatestSnapshot() != null} after. Before this fix,
 * {@code BaseStateMachine.pause()} was a no-op so the lifecycle stayed in {@code NEW}
 * and the precondition threw {@code IllegalStateException}.
 */
class ArcadeStateMachineLifecycleTest {

  /**
   * Verifies that {@link ArcadeStateMachine#initialize} starts the {@link LifeCycle} so that
   * subsequent {@link ArcadeStateMachine#pause()} calls can transition to {@code PAUSED}. This
   * covers the regression path: if the lifecycle-start lines are removed from {@code initialize()},
   * the lifecycle stays {@code NEW} and {@code pause()} silently skips the transition.
   */
  @Test
  void initializeStartsLifecycleToRunning(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    assertThat(sm.getLifeCycleState())
        .as("fresh state machine must start in NEW")
        .isEqualTo(LifeCycle.State.NEW);

    final RaftGroupId groupId = RaftGroupId.valueOf(UUID.randomUUID());
    // RaftStorage.newBuilder requires an existing directory; FORMAT creates any missing subdirs.
    final RaftStorage raftStorage = RaftStorage.newBuilder()
        .setDirectory(tempDir.toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
    // Minimal RaftServer stub using a JDK dynamic proxy: BaseStateMachine.initialize() only
    // calls server.complete(raftServer) and then toString() → getId() → server.join().getId(),
    // so getId() must return non-null. All other method calls throw UnsupportedOperationException.
    final RaftServer raftServer = (RaftServer) Proxy.newProxyInstance(
        getClass().getClassLoader(),
        new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
    try (sm) {
      sm.initialize(raftServer, groupId, raftStorage);

      assertThat(sm.getLifeCycleState())
          .as("initialize() must start the lifecycle so pause() can later transition to PAUSED")
          .isEqualTo(LifeCycle.State.RUNNING);
    } finally {
      raftStorage.close();
    }
  }

  /**
   * Simulates the sequence Ratis drives when it needs the state machine to reload
   * after a snapshot install:
   * <ol>
   *   <li>{@link ArcadeStateMachine#pause()} -called by {@code SnapshotInstallationHandler}
   *       before signalling {@code StateMachineUpdater} to enter RELOAD mode.</li>
   *   <li>{@code StateMachineUpdater.reload()} checks {@code getLifeCycleState() == PAUSED}
   *       at line 230. Before the fix this assertion failed with {@code IllegalStateException}
   *       because the lifecycle stayed in {@code NEW}.</li>
   *   <li>{@link ArcadeStateMachine#reinitialize()} -called by {@code reload()} to restore
   *       the state machine from the installed snapshot. Must transition back to RUNNING.</li>
   * </ol>
   * The lifecycle is started manually rather than via {@code initialize()} to keep this test
   * self-contained (no filesystem setup needed). {@link #initializeStartsLifecycleToRunning}
   * separately verifies that {@code initialize()} itself performs this start.
   */
  @Test
  void pauseThenReinitializeFollowsLifecycleContractRequiredByReload() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    // Simulate initialize(): start the lifecycle (NEW -> STARTING -> RUNNING)
    sm.getLifeCycle().transition(LifeCycle.State.STARTING);
    sm.getLifeCycle().transition(LifeCycle.State.RUNNING);
    assertThat(sm.getLifeCycleState()).isEqualTo(LifeCycle.State.RUNNING);

    // Simulate SnapshotInstallationHandler calling pause() before reloadStateMachine()
    sm.pause();

    // This is the exact check at StateMachineUpdater.reload() line 230.
    // Before the fix, this would be NEW (not PAUSED) and StateMachineUpdater.reload() would
    // throw IllegalStateException, killing the updater thread and closing the Raft division.
    assertThat(sm.getLifeCycleState())
        .as("lifecycle must be PAUSED so StateMachineUpdater.reload() passes its precondition")
        .isEqualTo(LifeCycle.State.PAUSED);

    // Simulate StateMachineUpdater.reload() calling reinitialize()
    sm.reinitialize();

    assertThat(sm.getLifeCycleState())
        .as("lifecycle must return to RUNNING after reinitialize() so the state machine can apply log entries again")
        .isEqualTo(LifeCycle.State.RUNNING);
  }

  @Test
  void pauseIsIdempotentAfterAlreadyPaused() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.getLifeCycle().transition(LifeCycle.State.STARTING);
    sm.getLifeCycle().transition(LifeCycle.State.RUNNING);

    sm.pause();
    assertThat(sm.getLifeCycleState()).isEqualTo(LifeCycle.State.PAUSED);

    // A second pause() call must not throw even though the lifecycle is already PAUSED
    sm.pause();
    assertThat(sm.getLifeCycleState()).isEqualTo(LifeCycle.State.PAUSED);
  }

  @Test
  void reinitializeWithoutSnapshotLeavesLifecycleRunning() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.getLifeCycle().transition(LifeCycle.State.STARTING);
    sm.getLifeCycle().transition(LifeCycle.State.RUNNING);

    sm.pause();
    sm.reinitialize();

    // Even with no snapshot in storage, the lifecycle must reach RUNNING after reinitialize()
    assertThat(sm.getLifeCycleState()).isEqualTo(LifeCycle.State.RUNNING);
  }
}
