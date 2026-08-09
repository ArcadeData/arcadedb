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

import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5890 (code review follow-up): {@code ArcadeDBServer.stopInternal()} stops
 * plugins - closing {@code SnapshotHttpHandler}'s {@code watchdogExecutor} via {@code close()} - before it
 * stops the HTTP server, so a snapshot request already in flight, or one accepted in that window, can reach
 * the watchdog scheduling call after the executor is shut down. {@code ScheduledExecutorService.scheduleWithFixedDelay}
 * throws {@link java.util.concurrent.RejectedExecutionException} synchronously in that case - a failure mode
 * that could not previously occur, since the executor was never shut down while the handler was alive.
 * {@link SnapshotHttpHandler#scheduleWatchdogOrSkip} must degrade to an unmonitored transfer instead of
 * letting the exception escape {@code handleRequest()} uncaught.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotHttpHandlerRejectedExecutionTest {

  @Test
  void scheduleWatchdogOrSkipReturnsNullWhenThePoolIsAlreadyShutDown() {
    final SnapshotHttpHandler handler = new SnapshotHttpHandler(null);
    handler.close(); // shuts down watchdogExecutor, simulating a concurrent stopService()

    final ScheduledFuture<?> watchdog = handler.scheduleWatchdogOrSkip(
        new AtomicBoolean(false), new AtomicLong(System.currentTimeMillis()), 30_000L, 5_000L, "mydb", () -> { });

    assertThat(watchdog == null).isTrue();
  }
}
