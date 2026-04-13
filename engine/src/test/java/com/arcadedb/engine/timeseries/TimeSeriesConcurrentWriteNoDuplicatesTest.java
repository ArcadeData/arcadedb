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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/discussions/3860 .
 * <p>
 * Verifies that concurrent writes to the same shard do NOT produce MVCC
 * {@code ConcurrentModificationException} errors and do NOT cause data duplication
 * (which would occur if the HTTP client retried after receiving a 503 for a partially
 * committed batch).
 * <p>
 * Prior to the fix, the per-shard {@code appendLock} was absent and two threads racing
 * to commit page 0 of the same shard would get a version mismatch at commit time:
 * "current v.X &lt;&gt; database v.Y. Please retry the operation".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesConcurrentWriteNoDuplicatesTest extends TestHelper {

  /**
   * Launches multiple threads that all write to a single-shard engine simultaneously.
   * With one shard every append competes for the same mutable-bucket page.
   * The test asserts that:
   * <ul>
   *   <li>no exception is thrown by any writer thread;</li>
   *   <li>the total sample count equals exactly threadCount × samplesPerThread
   *       (i.e. no duplicates and no missing writes).</li>
   * </ul>
   */
  @Test
  void concurrentWritesToSameShardNoDuplicatesAndNoMvccErrors() throws Exception {
    final int THREAD_COUNT = 8;
    final int SAMPLES_PER_THREAD = 200;

    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );

    // Use shardCount=1 to force maximum contention: every append goes to the same shard.
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "test_concurrent_no_dup", columns, 1);

    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    for (int t = 0; t < THREAD_COUNT; t++) {
      final int threadIdx = t;
      final Thread thread = new Thread(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < SAMPLES_PER_THREAD; i++) {
            // Assign unique timestamps per thread so there are no intentional duplicates.
            final long ts = (long) threadIdx * SAMPLES_PER_THREAD + i;
            engine.appendSamples(new long[] { ts }, new Object[][] { { (double) ts } });
          }
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        } finally {
          doneLatch.countDown();
        }
      }, "writer-" + t);
      thread.setDaemon(true);
      thread.start();
    }

    startLatch.countDown();
    assertThat(doneLatch.await(30, java.util.concurrent.TimeUnit.SECONDS))
        .as("All writer threads should finish within 30 seconds")
        .isTrue();

    assertThat(failure.get())
        .as("No writer thread should throw an exception (MVCC or otherwise)")
        .isNull();

    // Total samples must equal exactly threadCount × samplesPerThread.
    // Any MVCC exception that slipped through without the appendLock would have caused
    // the client (HTTP handler) to return 503; the client would then retry the whole
    // batch — duplicating every sample that was already committed before the failure.
    final long totalSamples = engine.countSamples();
    assertThat(totalSamples)
        .as("Sample count must be exactly %d (no duplicates, no lost writes)", THREAD_COUNT * SAMPLES_PER_THREAD)
        .isEqualTo((long) THREAD_COUNT * SAMPLES_PER_THREAD);

    engine.close();
  }
}
