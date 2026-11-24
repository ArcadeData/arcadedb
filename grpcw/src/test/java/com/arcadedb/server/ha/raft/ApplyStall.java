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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Holds one server's apply position still: while it is open, the state machine of that server applies the entries it
 * receives but cannot move its applied index past them, the way an apply that outlasts the wait of a commit does. Lives in the
 * package of the state machine because the lock is package-private.
 */
public final class ApplyStall implements AutoCloseable {
  private final CountDownLatch held    = new CountDownLatch(1);
  private final CountDownLatch release = new CountDownLatch(1);
  private final Thread         holder;

  private ApplyStall(final ArcadeStateMachine stateMachine) {
    holder = new Thread(() -> {
      synchronized (stateMachine.appliedPositionLock) {
        held.countDown();
        try {
          release.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }, "apply-stall");
    holder.setDaemon(true);
    holder.start();
  }

  /** Stalls the apply position of the server of {@code plugin}; returns once the stall is in force. */
  public static ApplyStall on(final RaftHAPlugin plugin) throws InterruptedException {
    final ApplyStall stall = new ApplyStall(plugin.getRaftHAServer().getStateMachine());
    if (!stall.held.await(10, TimeUnit.SECONDS))
      throw new IllegalStateException("the apply position lock was not taken");
    return stall;
  }

  /** The index of the last entry the state machine of {@code plugin} applied, the counter a response bookmark reads. */
  public static long appliedCounter(final RaftHAPlugin plugin) {
    return plugin.getRaftHAServer().getStateMachine().readAppliedIndexCounter();
  }

  @Override
  public void close() throws InterruptedException {
    release.countDown();
    holder.join(10_000);
  }
}
