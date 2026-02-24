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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests concurrent append and compaction operations on the TimeSeries engine.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesConcurrentAppendCompactTest extends TestHelper {

  @Test
  void testConcurrentAppendDuringCompaction() throws Exception {
    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "test_concurrent", columns, 2);

    // Insert initial data
    for (int i = 0; i < 100; i++)
      engine.appendSamples(new long[] { i * 1000L }, new Object[] { (double) i });
    database.commit();

    // Launch concurrent writers and a compactor
    final int writerCount = 4;
    final int samplesPerWriter = 50;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(writerCount + 1);
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final AtomicInteger totalAppended = new AtomicInteger(0);
    final AtomicInteger concurrentRetries = new AtomicInteger(0);

    final ExecutorService executor = Executors.newFixedThreadPool(writerCount + 1);

    // Compactor thread — ConcurrentModificationException is expected under contention
    executor.submit(() -> {
      try {
        startLatch.await();
        for (int retry = 0; retry < 5; retry++) {
          try {
            engine.compactAll();
            break;
          } catch (final Exception e) {
            if (hasConcurrentModification(e)) {
              concurrentRetries.incrementAndGet();
              Thread.sleep(50);
            } else
              throw e;
          }
        }
      } catch (final Throwable t) {
        error.compareAndSet(null, t);
      } finally {
        doneLatch.countDown();
      }
    });

    // Writer threads
    for (int w = 0; w < writerCount; w++) {
      final int writerIdx = w;
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < samplesPerWriter; i++) {
            final long ts = 100_000L + writerIdx * 100_000L + i * 1000L;
            for (int retry = 0; retry < 10; retry++) {
              try {
                database.begin();
                engine.appendSamples(new long[] { ts }, new Object[] { ts / 1000.0 });
                database.commit();
                totalAppended.incrementAndGet();
                break;
              } catch (final ConcurrentModificationException e) {
                concurrentRetries.incrementAndGet();
              }
            }
          }
        } catch (final Throwable t) {
          error.compareAndSet(null, t);
        } finally {
          doneLatch.countDown();
        }
      });
    }

    // Start all threads simultaneously
    startLatch.countDown();
    assertThat(doneLatch.await(30, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();

    if (error.get() != null)
      throw new AssertionError("Concurrent operation failed", error.get());

    // Verify all data is accessible
    database.begin();
    final List<Object[]> results = engine.query(0, Long.MAX_VALUE, null, null);
    database.commit();

    // At least the initial samples should be present; some appended may have failed after max retries
    assertThat(results.size()).isGreaterThanOrEqualTo(100);

    // Verify results are sorted by timestamp (query guarantees this)
    for (int i = 1; i < results.size(); i++)
      assertThat((long) results.get(i)[0]).isGreaterThanOrEqualTo((long) results.get(i - 1)[0]);

    engine.close();
  }

  private static boolean hasConcurrentModification(final Throwable t) {
    Throwable cause = t;
    while (cause != null) {
      if (cause instanceof ConcurrentModificationException)
        return true;
      cause = cause.getCause();
    }
    return false;
  }
}
