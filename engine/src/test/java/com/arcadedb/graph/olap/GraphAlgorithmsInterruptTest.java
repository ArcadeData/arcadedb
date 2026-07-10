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
package com.arcadedb.graph.olap;

import com.arcadedb.exception.CommandExecutionException;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #4951: {@code GraphAlgorithms.awaitFutures} used to swallow an interrupt (set the flag,
 * keep looping - every remaining {@code get()} threw immediately) and return normally, so callers (PageRank/BFS
 * chunk fan-outs, the GAV fused-chain operator, the partitioned triangle count) merged partial per-chunk results
 * and reported them as a successful, complete answer on query kill/timeout. It must instead cancel the
 * outstanding futures and throw, preserving the interrupt flag.
 */
class GraphAlgorithmsInterruptTest {

  @Test
  void awaitFuturesThrowsOnInterruptAndCancelsOutstanding() throws Exception {
    final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
      final Thread t = new Thread(r, "GraphAlgorithmsInterruptTest-worker");
      t.setDaemon(true);
      return t;
    });

    final CountDownLatch release = new CountDownLatch(1);
    try {
      // First future occupies the single worker until released; the second waits behind it. Neither can
      // complete before awaitFutures gives up, so a normal return would mean partial results were merged.
      final Future<?> running = executor.submit(() -> {
        release.await();
        return null;
      });
      final Future<?> queued = executor.submit(() -> null);
      final Future<?>[] futures = new Future<?>[] { running, queued };

      // Pre-set the interrupt flag: the first get() observes it immediately, deterministic with no timing.
      Thread.currentThread().interrupt();
      try {
        assertThatThrownBy(() -> GraphAlgorithms.awaitFutures(futures, 2))
            .as("an interrupted await must abort, never return normally with partial chunks")
            .isInstanceOf(CommandExecutionException.class);
        assertThat(Thread.currentThread().isInterrupted())
            .as("the interrupt flag must be preserved for the caller").isTrue();
      } finally {
        Thread.interrupted(); // leave the test framework uninterrupted
      }

      assertThat(running.isCancelled()).as("in-flight chunk must be cancelled").isTrue();
      assertThat(queued.isCancelled()).as("queued chunk must be cancelled").isTrue();
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }
}
