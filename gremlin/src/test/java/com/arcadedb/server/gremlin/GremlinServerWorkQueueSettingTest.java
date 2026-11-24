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
package com.arcadedb.server.gremlin;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.TestServerHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Gremlin execution pool built by the plugin has a work queue of {@code maxWorkQueueSize} entries, like the pool
 * TinkerPop builds when none is supplied: a task that finds every worker busy and the queue full is refused with a
 * {@link RejectedExecutionException}, which the eval and traversal processors answer with the "too many requests" status.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinServerWorkQueueSettingTest {

  @TempDir
  Path root;

  @Test
  void theWorkQueueHoldsAtMostTheConfiguredNumberOfTasks() throws Exception {
    final ExecutorService pool = GremlinServerPlugin.newGremlinPool(1, 2);
    try {
      final CountDownLatch release = new CountDownLatch(1);
      final CountDownLatch started = new CountDownLatch(1);
      final AtomicInteger completed = new AtomicInteger();

      final Future<?> running = pool.submit(() -> {
        started.countDown();
        await(release);
        completed.incrementAndGet();
      });
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

      // TWO WAIT IN THE QUEUE, THE NEXT FINDS IT FULL
      final Future<?> firstQueued = pool.submit(completed::incrementAndGet);
      final Future<?> secondQueued = pool.submit(completed::incrementAndGet);
      assertThatThrownBy(() -> pool.submit(completed::incrementAndGet)).isInstanceOf(RejectedExecutionException.class);

      release.countDown();
      running.get(10, TimeUnit.SECONDS);
      firstQueued.get(10, TimeUnit.SECONDS);
      secondQueued.get(10, TimeUnit.SECONDS);
      assertThat(completed.get()).as("the refused task never runs").isEqualTo(3);

      // ONCE THE QUEUE HAS DRAINED THE POOL ACCEPTS TASKS AGAIN
      assertThat(pool.submit(() -> "served").get(10, TimeUnit.SECONDS)).isEqualTo("served");
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void theRefusalReachesTheCallerOfThePrincipalPropagatingPoolUnchanged() throws Exception {
    final ExecutorService pool = GremlinServerPlugin.newGremlinPool(1, 1);
    try {
      final GremlinPrincipalPropagatingExecutorService executor = new GremlinPrincipalPropagatingExecutorService(pool,
          TestServerHelper.unstartedServer(root, new ContextConfiguration()));
      final CountDownLatch release = new CountDownLatch(1);
      final CountDownLatch started = new CountDownLatch(1);

      final Future<?> running = executor.submit(() -> {
        started.countDown();
        await(release);
      });
      assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
      final Future<?> queued = executor.submit(() -> "queued");

      // TINKERPOP CATCHES THIS TYPE, NOT A WRAPPER OF IT, TO ANSWER "TOO MANY REQUESTS"
      assertThatThrownBy(() -> executor.submit(() -> "refused")).isExactlyInstanceOf(RejectedExecutionException.class);

      release.countDown();
      running.get(10, TimeUnit.SECONDS);
      assertThat(queued.get(10, TimeUnit.SECONDS)).isEqualTo("queued");
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void aWorkQueueSizeThatIsNotPositiveFailsTheStart() {
    assertThatThrownBy(() -> GremlinServerPlugin.newGremlinPool(1, 0)).isInstanceOf(ServerException.class)
        .hasMessageContaining("maxWorkQueueSize").hasMessageContaining("'0'");
    assertThatThrownBy(() -> GremlinServerPlugin.newGremlinPool(1, -5)).isInstanceOf(ServerException.class)
        .hasMessageContaining("maxWorkQueueSize").hasMessageContaining("'-5'");
  }

  private static void await(final CountDownLatch latch) {
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
