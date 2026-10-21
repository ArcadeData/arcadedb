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
package com.arcadedb.database.async;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10537: the async barrier ({@code waitForAsyncCompletion}) returns at once for the thread that holds a quiescence, because the
 * quiescence committed every worker's batch and holds the workers parked, so the marker the barrier would enqueue behind them is never
 * dequeued. The exemption is the holder's alone: the lock it is read from is reentrant and owned by one thread, so another thread, which
 * the workers are parked against as well, is not exempted and keeps its barrier.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10537AsyncBarrierUnderQuiescenceTest extends TestHelper {

  /** A database that never touched the async API has no executor: the quiescence and the barrier are free, and create none. */
  @Test
  void withoutAnExecutorTheQuiescenceAndTheBarrierAreFreeAndCreateNone() {
    assertThat(((LocalDatabase) database).getAsyncIfExists()).isNull();

    try (final AsyncQuiesce asyncPaused = ((DatabaseInternal) database).quiesceAsync()) {
      ((DatabaseInternal) database).waitForAsyncCompletion();
    }

    assertThat(((LocalDatabase) database).getAsyncIfExists()).as("no worker pool was started to be parked").isNull();
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void onlyTheThreadThatHoldsTheQuiescenceIsExemptedFromTheBarrier() throws Exception {
    database.getSchema().createVertexType("Batch");
    for (int i = 0; i < 100; i++)
      database.async().createRecord(database.newVertex("Batch").set("i", i), null);
    final DatabaseAsyncExecutorImpl executor = ((LocalDatabase) database).getAsyncIfExists();
    assertThat(executor).isNotNull();
    assertThat(executor.isQuiescedByCurrentThread()).as("nothing is quiesced yet").isFalse();

    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      try (final AsyncQuiesce asyncPaused = ((DatabaseInternal) database).quiesceAsync()) {
        assertThat(executor.isQuiescedByCurrentThread()).as("the holder").isTrue();
        assertThat(other.submit(executor::isQuiescedByCurrentThread).get()).as("another thread").isFalse();

        // The holder does not wait on workers it has parked, and a nested quiescence rides on its own
        ((DatabaseInternal) database).waitForAsyncCompletion();
        try (final AsyncQuiesce nested = ((DatabaseInternal) database).quiesceAsync()) {
          assertThat(executor.isQuiescedByCurrentThread()).isTrue();
        }
        assertThat(executor.isQuiescedByCurrentThread()).as("the outer quiescence is still held").isTrue();
      }

      assertThat(executor.isQuiescedByCurrentThread()).as("released").isFalse();
    } finally {
      other.shutdownNow();
    }

    // Released, the barrier is the real one again and sees what the async side wrote
    ((DatabaseInternal) database).waitForAsyncCompletion();
    assertThat(database.countType("Batch", false)).isEqualTo(100L);
  }
}
