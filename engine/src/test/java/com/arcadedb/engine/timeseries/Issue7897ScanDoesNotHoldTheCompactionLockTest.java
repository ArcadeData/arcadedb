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
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7897: {@code forEachRow} ran the caller's visitor while the shard held {@code compactionLock.readLock()}
 * and the sealed store held {@code directoryLock.readLock()}, so the time a compaction - and therefore an append,
 * which queues behind the waiting writer - spent waiting was the caller's TOTAL work rather than one block's.
 * <p>
 * The regression that made it visible was the #7697 fix: {@code JsonlExporterFormat} swapped {@code iterateQuery}
 * for {@code forEachRow} and its visitor writes a gzip chunk to the archive every {@code TIMESERIES_CHUNK_SIZE}
 * rows, so an {@code EXPORT DATABASE} held a shard's compaction read lock across its own file I/O for the whole
 * export and ingest for the type stalled for exactly that long.
 * <p>
 * These tests pin the property the fix establishes rather than a duration: with a visitor that is blocked and
 * stays blocked, an append still completes, and a compaction that lands DURING such a scan neither loses a row
 * nor hands one over twice.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7897ScanDoesNotHoldTheCompactionLockTest extends TestHelper {

  private static final long BASE_TS      = 1_700_000_000_000L;
  private static final int  SEALED_ROWS  = 400;
  private static final int  MUTABLE_ROWS = 40;

  private TimeSeriesEngine engine;

  @BeforeEach
  void populate() throws IOException {
    // One shard: the scan then blocks inside the only shard there is, which is what makes the append it must not
    // block a deterministic thing to ask for.
    database.command("sql",
        "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

    engine = ((LocalTimeSeriesType) database.getSchema().getType("Reading")).getEngine();

    appendRange(0, SEALED_ROWS);
    // Sealed, so the walk blocks in the sealed layer - the half that used to hold BOTH locks across the visitor.
    engine.compactAll();
    appendRange(SEALED_ROWS, MUTABLE_ROWS);
  }

  private void appendRange(final int from, final int count) throws IOException {
    final long[] timestamps = new long[count];
    final Object[] hosts = new Object[count];
    final Object[] values = new Object[count];
    for (int i = 0; i < count; i++) {
      timestamps[i] = BASE_TS + (from + i) * 1_000L;
      hosts[i] = "host_" + ((from + i) % 4);
      values[i] = (double) (from + i);
    }
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
  }

  /**
   * The reported defect, through {@code forEachRow}: a visitor that is parked mid-scan must not keep an append
   * out. A compaction is started first because that is what turns a shared read lock into a barrier - a queued
   * writer makes {@code appendSamples}' own read lock wait.
   */
  @Test
  @Timeout(180)
  void anAppendCompletesWhileAForEachRowVisitorIsParked() throws Exception {
    assertAppendCompletesWhileVisitorIsParked(
        visitor -> engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, null, visitor));
  }

  /**
   * The same shape, one method over: {@code forEachTagCombination} held the identical pair of locks across the
   * identical caller-supplied visitor, and {@code GET /prom/api/v1/series} is its caller.
   */
  @Test
  @Timeout(180)
  void anAppendCompletesWhileAForEachTagCombinationVisitorIsParked() throws Exception {
    final int[] tagOnly = { 0 };
    assertAppendCompletesWhileVisitorIsParked(
        visitor -> engine.forEachTagCombination(Long.MIN_VALUE, Long.MAX_VALUE, tagOnly, null, visitor));
  }

  private interface Scan {
    boolean run(TimeSeriesRowVisitor visitor) throws IOException;
  }

  private void assertAppendCompletesWhileVisitorIsParked(final Scan scan) throws Exception {
    final CountDownLatch visitorParked = new CountDownLatch(1);
    final CountDownLatch releaseVisitor = new CountDownLatch(1);
    final AtomicBoolean parkedOnce = new AtomicBoolean();
    final AtomicReference<Throwable> scanFailure = new AtomicReference<>();

    final Thread scanThread = new Thread(() -> {
      try {
        scan.run(row -> {
          if (parkedOnce.compareAndSet(false, true)) {
            visitorParked.countDown();
            try {
              // Stands in for the exporter's gzip chunk write: work the caller does per row that the engine
              // knows nothing about. It is released by the assertions below, or by the timeout, never by itself.
              releaseVisitor.await(150, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          return true;
        });
      } catch (final Throwable t) {
        scanFailure.set(t);
      }
    }, "issue7897-scan");
    scanThread.start();

    final AtomicReference<Throwable> compactFailure = new AtomicReference<>();
    final Thread compactThread = new Thread(() -> {
      try {
        engine.compactAll();
      } catch (final Throwable t) {
        compactFailure.set(t);
      }
    }, "issue7897-compact");

    final CountDownLatch appendDone = new CountDownLatch(1);
    final AtomicReference<Throwable> appendFailure = new AtomicReference<>();
    final Thread appendThread = new Thread(() -> {
      try {
        appendRange(SEALED_ROWS + MUTABLE_ROWS, 1);
      } catch (final Throwable t) {
        appendFailure.set(t);
      } finally {
        appendDone.countDown();
      }
    }, "issue7897-append");

    try {
      assertThat(visitorParked.await(60, TimeUnit.SECONDS)).as("the scan must reach its first row").isTrue();

      compactThread.start();
      // Give the compaction long enough to reach - and, once fixed, to get past - the write lock it needs. Not
      // asserted on: before the fix it parks there for the whole scan, after the fix it simply runs.
      compactThread.join(TimeUnit.SECONDS.toMillis(10));

      appendThread.start();
      assertThat(appendDone.await(60, TimeUnit.SECONDS))
          .as("an append must not wait for a parked scan visitor: the compaction read lock is what it queues behind")
          .isTrue();
      assertThat(appendFailure.get()).isNull();
    } finally {
      releaseVisitor.countDown();
      scanThread.join(TimeUnit.SECONDS.toMillis(60));
      compactThread.join(TimeUnit.SECONDS.toMillis(60));
      appendThread.join(TimeUnit.SECONDS.toMillis(60));
    }

    assertThat(scanFailure.get()).isNull();
    assertThat(compactFailure.get()).isNull();
  }

  /**
   * The other thing a walk that releases the directory lock between blocks has to survive: a RETENTION pass, which
   * rewrites the sealed file with the surviving blocks only and so moves every remaining block's offsets. A
   * snapshot entry read through its own offsets afterwards would be reading the wrong bytes of a shorter file;
   * {@code resolveLiveBlock} is what turns it back into the block it names, or into nothing when retention dropped
   * it.
   */
  @Test
  @Timeout(180)
  void aRetentionPassDuringTheScanMovesEveryOffsetAndTheWalkSurvivesIt() throws Exception {
    // A fresh type: one block per compaction, so the walk has blocks to be between.
    database.command("sql",
        "CREATE TIMESERIES TYPE Blocky TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
    final TimeSeriesEngine blocky = ((LocalTimeSeriesType) database.getSchema().getType("Blocky")).getEngine();

    final int blocks = 6;
    final int perBlock = 500;
    final List<Long> originals = new ArrayList<>();
    for (int b = 0; b < blocks; b++) {
      final long[] timestamps = new long[perBlock];
      final Object[] hosts = new Object[perBlock];
      final Object[] values = new Object[perBlock];
      for (int i = 0; i < perBlock; i++) {
        timestamps[i] = BASE_TS + ((long) b * perBlock + i) * 1_000L;
        hosts[i] = "host_" + (i % 4);
        values[i] = (double) i;
        originals.add(timestamps[i]);
      }
      blocky.appendBatch(timestamps, new Object[][] { hosts, values });
      blocky.compactAll();
    }

    // Drops the first half of the blocks, so the second half's offsets all move.
    final long cutoff = BASE_TS + ((long) blocks / 2 * perBlock) * 1_000L;

    final List<Long> seen = new ArrayList<>();
    final AtomicBoolean retainedOnce = new AtomicBoolean();
    final AtomicReference<Throwable> retentionFailure = new AtomicReference<>();

    blocky.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, null, row -> {
      seen.add((Long) row[0]);
      if (retainedOnce.compareAndSet(false, true)) {
        final Thread retention = new Thread(() -> {
          try {
            blocky.applyRetention(cutoff);
          } catch (final Throwable t) {
            retentionFailure.set(t);
          }
        }, "issue7897-retention");
        retention.start();
        try {
          retention.join(TimeUnit.SECONDS.toMillis(60));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return true;
    });

    assertThat(retentionFailure.get()).isNull();
    assertThat(seen).as("the walk must not invent a timestamp out of a block it read at the wrong offset")
        .isSubsetOf(originals);
    assertThat(new HashSet<>(seen)).as("nor hand one over twice").hasSize(seen.size());

    final List<Long> survivors = new ArrayList<>();
    for (final Long ts : originals)
      if (ts >= cutoff)
        survivors.add(ts);
    assertThat(seen).as("every sample retention KEPT is still handed over, at its new offset")
        .containsAll(survivors);
  }

  /**
   * The third directory-rewriting writer, and the one the other two tests leave unexercised against a walk in
   * flight: downsampling REPLACES blocks rather than dropping them, so the rows a snapshot entry names may still
   * be in the store under a different sample count - which is precisely the shape {@code resolveLiveBlock}'s
   * identity does NOT match, and has to decline rather than read at the old offsets (code review on PR #7970).
   */
  @Test
  @Timeout(180)
  void aDownsamplingPassDuringTheScanReplacesBlocksAndTheWalkSurvivesIt() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Coarse TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
    final TimeSeriesEngine coarse = ((LocalTimeSeriesType) database.getSchema().getType("Coarse")).getEngine();

    final int blocks = 6;
    final int perBlock = 500;
    final List<Long> originals = new ArrayList<>();
    for (int b = 0; b < blocks; b++) {
      final long[] timestamps = new long[perBlock];
      final Object[] hosts = new Object[perBlock];
      final Object[] values = new Object[perBlock];
      for (int i = 0; i < perBlock; i++) {
        timestamps[i] = BASE_TS + ((long) b * perBlock + i) * 1_000L;
        hosts[i] = "host_" + (i % 4);
        values[i] = (double) i;
        originals.add(timestamps[i]);
      }
      coarse.appendBatch(timestamps, new Object[][] { hosts, values });
      coarse.compactAll();
    }

    final long newest = BASE_TS + ((long) blocks * perBlock - 1) * 1_000L;
    // Everything older than the last block's worth of samples is folded to one point per minute.
    final List<DownsamplingTier> tiers = List.of(new DownsamplingTier((long) perBlock * 1_000L, 60_000L));

    final List<Long> seen = new ArrayList<>();
    final AtomicBoolean downsampledOnce = new AtomicBoolean();
    final AtomicReference<Throwable> downsampleFailure = new AtomicReference<>();

    coarse.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, null, row -> {
      seen.add((Long) row[0]);
      if (downsampledOnce.compareAndSet(false, true)) {
        final Thread downsampler = new Thread(() -> {
          try {
            coarse.applyDownsampling(tiers, newest);
          } catch (final Throwable t) {
            downsampleFailure.set(t);
          }
        }, "issue7897-downsample");
        downsampler.start();
        try {
          downsampler.join(TimeUnit.SECONDS.toMillis(60));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return true;
    });

    assertThat(downsampleFailure.get()).isNull();
    // The walk must come back with real samples, not with whatever a stale offset into a rewritten file decodes
    // to. A downsampled block is declined by resolveLiveBlock, so its rows are absent rather than wrong - which
    // is the contract, and the reason this asserts a SUBSET and not equality.
    assertThat(seen).as("no timestamp the store never held").isSubsetOf(originals);
    assertThat(new HashSet<>(seen)).as("and none of them twice").hasSize(seen.size());
    assertThat(seen).as("the walk still produced the rows it read before the rewrite landed").isNotEmpty();
  }

  /**
   * The other half of releasing the locks: the walk no longer sees one frozen shard, so it has to be shown that a
   * compaction landing in the middle of it neither drops a row into the gap between the two layers nor hands one
   * over from both.
   */
  @Test
  @Timeout(180)
  void aCompactionDuringTheScanNeitherLosesNorDuplicatesARow() throws Exception {
    final List<Long> seen = new ArrayList<>();
    final AtomicBoolean compactedOnce = new AtomicBoolean();
    final AtomicReference<Throwable> compactFailure = new AtomicReference<>();

    engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, null, row -> {
      seen.add((Long) row[0]);
      if (compactedOnce.compareAndSet(false, true)) {
        // Runs to completion before the scan resumes, so the mutable bucket the scan still has to read has been
        // sealed and cleared underneath it - the exact window the old whole-scan read lock made unreachable.
        final Thread compactor = new Thread(() -> {
          try {
            engine.compactAll();
          } catch (final Throwable t) {
            compactFailure.set(t);
          }
        }, "issue7897-inline-compact");
        compactor.start();
        try {
          compactor.join(TimeUnit.SECONDS.toMillis(60));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return true;
    });

    assertThat(compactFailure.get()).isNull();
    assertThat(compactedOnce.get()).as("the scan must have visited at least one row").isTrue();

    final List<Long> expected = new ArrayList<>();
    for (int i = 0; i < SEALED_ROWS + MUTABLE_ROWS; i++)
      expected.add(BASE_TS + i * 1_000L);

    assertThat(seen).as("every row exactly once, whatever the compaction did while the walk was in flight")
        .containsExactlyInAnyOrderElementsOf(expected);
  }
}
