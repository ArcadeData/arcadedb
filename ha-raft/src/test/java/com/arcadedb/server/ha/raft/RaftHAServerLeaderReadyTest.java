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

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the leaderReady stuck-at-false bug: if the lifecycle executor is shut
 * down during a leadership transition (e.g. concurrent restartRatisIfNeeded), the
 * RejectedExecutionException must be caught and leaderReady restored to true.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftHAServerLeaderReadyTest {

  /**
   * Subclass that overrides isLeader() to simulate the leader state without a real Ratis server.
   */
  static class TestableRaftHAServer extends RaftHAServer {
    @Override
    public boolean isLeader() {
      return true;
    }
  }

  @Test
  void notifyLeaderChangedRestoresLeaderReadyWhenExecutorIsShutDown() throws Exception {
    final TestableRaftHAServer server = new TestableRaftHAServer();

    // Create a state machine with null server/raftHA (only the executor matters for this test)
    final ArcadeDBStateMachine stateMachine = new ArcadeDBStateMachine(null, null);
    // Shut down the executor to simulate concurrent restartRatisIfNeeded()
    stateMachine.close();

    // Inject the shut-down state machine via reflection
    final Field smField = RaftHAServer.class.getDeclaredField("stateMachine");
    smField.setAccessible(true);
    smField.set(server, stateMachine);

    // Before the fix, this would throw RejectedExecutionException and leave leaderReady=false.
    // After the fix, the exception is caught and leaderReady is restored to true.
    server.notifyLeaderChanged();

    assertThat(server.isLeaderReady()).isTrue();
  }

  @Test
  void notifyLeaderChangedSetsLeaderReadyTrueForNonLeader() {
    // No-arg constructor: raftServer is null, so isLeader() returns false.
    final RaftHAServer server = new RaftHAServer();

    server.notifyLeaderChanged();

    assertThat(server.isLeaderReady()).isTrue();
  }
}
