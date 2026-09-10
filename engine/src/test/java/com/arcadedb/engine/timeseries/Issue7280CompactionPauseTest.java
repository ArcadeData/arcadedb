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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7280 - the two engine-side primitives a consistent backup of a TimeSeries database needs.
 * <p>
 * {@link TimeSeriesSealedStore#listSealedFiles(File)} is the enumeration the page-file layer cannot provide,
 * because a sealed store is raw {@code FileChannel} I/O and is never registered with the {@code FileManager}.
 * {@link TimeSeriesCompactionPause} is what stops the enumeration from being a trap: without it a sealed image
 * read after the page image can carry samples the page image still holds in the mutable bucket, which restores
 * as duplicates.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7280CompactionPauseTest extends TestHelper {
  private static final int  SAMPLES = 30_000;
  private static final long BASE_TS = 1_700_000_000_000L;

  /**
   * A wait that is EXPECTED to expire: it is the assertion that compaction did not get through the pause. Kept
   * short because a stall can only make it more true, never less.
   */
  private static final long BLOCKED_PROBE_MS = 2_000L;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
  }

  @Test
  void listSealedFilesReturnsTheSealedStoresAndNothingElse() throws Exception {
    ingest(SAMPLES);
    engine().compactAll();

    final File dir = new File(database.getDatabasePath());

    // Scratch and staging files sit in the same directory and must never reach an archive: the first is a
    // half-written compaction, the second a half-received HA install.
    assertThat(new File(dir, "Reading_shard_0.ts.sealed.tmp").createNewFile()).isTrue();
    assertThat(new File(dir, "Reading_shard_1.ts.sealed.incoming").createNewFile()).isTrue();

    final List<String> names = Arrays.stream(TimeSeriesSealedStore.listSealedFiles(dir)).map(File::getName).sorted()
        .toList();

    assertThat(names).containsExactly("Reading_shard_0.ts.sealed", "Reading_shard_1.ts.sealed");
    for (final String name : names)
      assertThat(new File(dir, name)).exists().isFile();
  }

  @Test
  void listSealedFilesOnADirectoryWithoutTimeSeriesIsEmptyRatherThanNull() {
    assertThat(TimeSeriesSealedStore.listSealedFiles(new File(database.getDatabasePath(), "does-not-exist"))).isEmpty();
  }

  /**
   * The property the backup relies on: while the pause is held no compaction can COMPLETE, so a sealed image
   * read inside it still pairs with the mutable bucket the page image carries.
   */
  @Test
  void aHeldPauseBlocksCompactionUntilItIsReleased() throws Exception {
    ingest(SAMPLES);

    final long blocksBefore = totalSealedBlocks();
    assertThat(blocksBefore).as("the test must start with nothing sealed, or it proves nothing").isZero();

    final CountDownLatch compacted = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
      final Thread compactor = new Thread(() -> {
        DatabaseContext.INSTANCE.init((DatabaseInternal) database);
        try {
          engine().compactAll();
        } catch (final Throwable e) {
          failure.set(e);
        } finally {
          DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
          compacted.countDown();
        }
      }, "issue7280-compactor");
      compactor.setDaemon(true);
      compactor.start();

      assertThat(compacted.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS))
          .as("compaction must not complete while the pause is held").isFalse();
      assertThat(totalSealedBlocks()).as("no sealed block may appear while the pause is held").isZero();

      pause.close();

      assertThat(compacted.await(60, TimeUnit.SECONDS)).as("compaction must complete once the pause is released")
          .isTrue();
      assertThat(failure.get()).isNull();
      compactor.join(60_000);
    }

    assertThat(totalSealedBlocks()).as("the released compaction must actually have sealed something")
        .isGreaterThan(blocksBefore);
  }

  /** Closing twice is a no-op: the backup releases the pause early and the try-with-resources closes it again. */
  @Test
  void closingThePauseTwiceIsANoOp() throws Exception {
    ingest(1_000);

    final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L);
    pause.close();
    pause.close();

    // A double release would have thrown above on an unheld lock; compaction still works afterwards.
    engine().compactAll();
    assertThat(totalSealedBlocks()).isGreaterThan(0);
  }

  /**
   * The pause covers every shard of every TimeSeries type and nothing else, so a database with no TimeSeries
   * type pays nothing for it and one with several is covered whole.
   */
  @Test
  void thePauseCoversEveryShardOfEveryTimeSeriesTypeAndNothingElse() {
    database.command("sql", "CREATE DOCUMENT TYPE Plain");
    try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
      assertThat(pause.getPausedShards()).isEqualTo(engine().getShardCount());
    }

    database.command("sql", "CREATE TIMESERIES TYPE Second TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 3");
    try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
      assertThat(pause.getPausedShards()).isEqualTo(engine().getShardCount() + 3);
    }
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType("Reading")).getEngine();
  }

  private long totalSealedBlocks() {
    long total = 0;
    for (int i = 0; i < engine().getShardCount(); i++)
      total += engine().getShard(i).getSealedStore().getBlockCount();
    return total;
  }

  private void ingest(final int samples) throws Exception {
    final long[] timestamps = new long[samples];
    final Object[] hosts = new Object[samples];
    final Object[] values = new Object[samples];
    for (int i = 0; i < samples; i++) {
      timestamps[i] = BASE_TS + i * 1_000L;
      hosts[i] = "host_" + (i % 4);
      values[i] = (double) i;
    }
    engine().appendBatch(timestamps, new Object[][] { hosts, values });
  }
}
