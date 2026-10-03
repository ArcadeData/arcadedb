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
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #8900: the in-place restart now waits for a close another thread has in flight on the old Ratis server. A
 * shutdown, or an interrupt of the health-monitor thread, during that wait must abandon the restart: no failure
 * counted toward stopping the node, no close that would interrupt the close in flight, and no new server.
 */
class Issue8900RestartAbandonedOnShutdownTest {

  private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";

  @Test
  @Timeout(value = 25, unit = TimeUnit.SECONDS) // hang detector: the close-in-progress wait alone is 30s
  void aShutdownDuringTheCloseInProgressWaitAbandonsTheRestart() throws Exception {
    final RaftHAServer raft = detachedServer();
    final CountDownLatch waiting = new CountDownLatch(1);
    final AtomicInteger stateReads = new AtomicInteger();
    final RaftServer old = mock(RaftServer.class);
    when(old.getDivision(any())).thenThrow(new IllegalStateException("closing"));
    // A close running on another thread that never finishes by itself.
    when(old.getLifeCycleState()).thenAnswer(invocation -> {
      if (stateReads.incrementAndGet() >= 2)
        waiting.countDown();
      return LifeCycle.State.CLOSING;
    });
    setField(raft, "raftServer", old);

    final Thread restart = new Thread(raft::restartRatisIfNeeded, "Issue8900-restart");
    restart.start();
    assertThat(waiting.await(10, TimeUnit.SECONDS)).as("the restart must be waiting on the close in flight").isTrue();

    setField(raft, "shutdownRequested", true);
    restart.join();

    verify(old, never()).close();
    assertThat(getField(raft, "raftServer")).as("no new server").isSameAs(old);
    assertThat(getField(raft, "restartFailureCount")).as("not a failed restart").isEqualTo(0);
    assertThat(raft.getRecoverRestartCount()).isZero();
  }

  @Test
  void anInterruptedThreadWithoutAShutdownLeavesEverythingAsItWas() throws Exception {
    final RaftHAServer raft = detachedServer();
    final RaftServer old = mock(RaftServer.class);
    when(old.getLifeCycleState()).thenReturn(LifeCycle.State.CLOSED);
    setField(raft, "raftServer", old);

    final Thread restart = new Thread(() -> {
      Thread.currentThread().interrupt();
      raft.restartRatisIfNeeded();
    }, "Issue8900-interrupted-restart");
    restart.start();
    restart.join(10_000L);

    assertThat(restart.isAlive()).isFalse();
    verify(old, never()).close();
    assertThat(getField(raft, "raftServer")).isSameAs(old);
    assertThat(getField(raft, "restartFailureCount")).isEqualTo(0);
  }

  private static RaftHAServer detachedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("ArcadeDB_0");
    return new RaftHAServer(mockServer, config);
  }

  private static void setField(final Object target, final String name, final Object value) throws Exception {
    final Field field = RaftHAServer.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static Object getField(final Object target, final String name) throws Exception {
    final Field field = RaftHAServer.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }
}
