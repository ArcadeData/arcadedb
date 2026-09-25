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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8356: {@code TransactionManager.cleanWALFiles()} manually iterated
 * {@code inactiveWALFilePool} (a {@code Collections.synchronizedList}) with a raw iterator instead of
 * synchronizing on the list, as the list's own contract requires for manual iteration. In production this
 * pool is mutated from two callers that are never coordinated with each other: the once-a-second WAL
 * housekeeping timer ({@code checkWALFiles()} retiring a file into the pool, then this same method removing
 * it) and {@code close()} (reachable from either of two independent JVM shutdown hooks). A {@code close()}
 * landing mid-tick raced the timer's own {@code cleanWALFiles()} pass and threw
 * {@code ConcurrentModificationException} out of {@code TransactionManager.close()}, aborting WAL cleanup
 * and leaving the lock-file release right after it to fail too ({@code ClosedChannelException}).
 * <p>
 * Reproduced here at the {@code TransactionManager} level: many threads run {@code cleanWALFiles()}
 * concurrently against one shared, populated pool - the same shape as two callers landing on the same tick
 * - without needing the real once-a-second timer or a 64MB WAL file to trigger rotation.
 */
class Issue8356TransactionManagerCleanWALFilesRaceTest extends TestHelper {

  private static final int FILE_COUNT   = 200;
  private static final int THREAD_COUNT = 8;

  @Test
  void concurrentCleanWALFilesDoesNotThrowConcurrentModificationException() throws Exception {
    final TransactionManager tm = ((DatabaseInternal) database).getTransactionManager();

    final File dir = new File(database.getDatabasePath());
    for (int i = 0; i < FILE_COUNT; i++)
      tm.addInactiveWALFileForTesting(new WALFile(new File(dir, "issue8356-" + i + ".wal").getAbsolutePath()));

    final ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
    final CountDownLatch ready = new CountDownLatch(THREAD_COUNT);
    final CountDownLatch go = new CountDownLatch(1);
    final List<Throwable> failures = new CopyOnWriteArrayList<>();

    for (int i = 0; i < THREAD_COUNT; i++) {
      pool.submit(() -> {
        try {
          ready.countDown();
          go.await();
          tm.cleanWALFilesForTesting(true, true, false);
        } catch (final Throwable t) {
          failures.add(t);
        }
      });
    }

    assertThat(ready.await(10, TimeUnit.SECONDS)).as("every worker must reach the starting gate").isTrue();
    go.countDown();
    pool.shutdown();
    assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).as("all workers must finish").isTrue();

    assertThat(failures)
        .as("concurrent cleanWALFiles() passes over the same pool must not throw; got: %s", failures)
        .isEmpty();
  }
}
