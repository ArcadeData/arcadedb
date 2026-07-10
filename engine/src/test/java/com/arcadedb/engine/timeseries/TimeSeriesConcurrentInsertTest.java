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
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for silent sample loss when many concurrent single-row INSERTs target the same
 * time series type. Each INSERT runs in its own transaction (as it does over the HTTP/wire path),
 * and the engine append must not drop a sample under concurrency.
 * <p>
 * The loss is a compaction Phase-4 race: the background TimeSeries maintenance scheduler runs an
 * automatic compaction a few seconds into the run (well within this test's runtime), so the
 * lock-free Phase 4b overlaps the concurrent inserts.  Before the Phase-4 fix the last partial data
 * page was read in Phase 4a and then cleared, dropping any samples written to it during Phase 4b;
 * the fix defers reading that page to Phase 4c under the write lock so those samples are captured.
 */
@Tag("slow")
class TimeSeriesConcurrentInsertTest extends TestHelper {

  @Test
  void concurrentSingleRowInsertsDoNotLoseSamples() throws Exception {
    // One shard maximizes contention: every concurrent append serializes on the same shard and
    // contends for the same mutable-bucket data page, which is exactly where the lost update occurs.
    database.command("sql",
        "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (sid STRING) FIELDS (v DOUBLE) SHARDS 1");

    final int threads = 48;
    final int perThread = 5000;
    final int expected = threads * perThread;

    final ExecutorService executor = Executors.newFixedThreadPool(threads);
    final List<Throwable> errors = new ArrayList<>();
    final AtomicInteger committed = new AtomicInteger();

    for (int t = 0; t < threads; t++) {
      final int threadIndex = t;
      executor.submit(() -> {
        final long base = 1_000_000_000L + (long) threadIndex * 10_000_000L;
        for (int i = 0; i < perThread; i++) {
          final long ts = base + i;
          final double v = i;
          try {
            // Each INSERT is its own transaction, mirroring the per-command transaction the wire
            // protocols use. Unique (sid, ts) per row means none should overwrite another.
            database.transaction(() ->
                database.command("sql", "INSERT INTO Reading SET ts = ?, sid = ?, v = ?", ts, "s" + threadIndex, v));
            committed.incrementAndGet();
          } catch (final Throwable e) {
            synchronized (errors) {
              errors.add(e);
            }
          }
        }
      });
    }

    executor.shutdown();
    assertThat(executor.awaitTermination(5, TimeUnit.MINUTES)).isTrue();

    assertThat(errors).as("no insert should fail").isEmpty();
    assertThat(committed.get()).as("all inserts reported committed").isEqualTo(expected);

    final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM Reading");
    final long actual = ((Number) rs.next().getProperty("cnt")).longValue();

    assertThat(actual).as("no samples lost under concurrent single-row inserts").isEqualTo(expected);
  }
}
