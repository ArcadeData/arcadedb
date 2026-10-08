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
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore.BlockDirectorySnapshot;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #9488: a merge of small sealed blocks keeps every row, yet a walk that had read some of the old blocks and met one
 * the merge replaced raised {@link TimeSeriesWalkCoarsenedException} - an error the caller of a plain aggregate read
 * cannot do anything about, raised by the engine's own maintenance. The walk now follows the merge to the block that
 * holds the rows and emits exactly the rows it had not handed over yet: each row once, none lost.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9488WalkAcrossMergeTest extends TestHelper {
  private static final long T0 = 1_700_000_000_000L;

  private TimeSeriesEngine engine(final String type) {
    return ((LocalTimeSeriesType) database.getSchema().getType(type)).getEngine();
  }

  /** {@code passes} small blocks, each sealed by itself. Odd passes are written BEFORE the even ones in time, so the blocks overlap. */
  private long feedAndCompact(final TimeSeriesEngine engine, final int passes, final int samplesPerPass, final long firstValue,
      final boolean interleave) throws Exception {
    long value = firstValue;
    for (int p = 0; p < passes; p++) {
      final long[] timestamps = new long[samplesPerPass];
      final Object[] tags = new Object[samplesPerPass];
      final Object[] values = new Object[samplesPerPass];
      for (int i = 0; i < samplesPerPass; i++) {
        // interleaved: every pass spans the whole window, so the merged block is NOT in source order
        timestamps[i] = interleave ? T0 + (long) i * 1_000L * passes + p * 1_000L : T0 + (value - firstValue) * 1_000L;
        tags[i] = "series-" + (i % 3);
        values[i] = (double) value++;
      }
      engine.appendSamples(timestamps, tags, values);
      engine.compactAll();
    }
    return value;
  }

  private static List<Double> values(final List<Object[]> rows) {
    final List<Double> out = new ArrayList<>(rows.size());
    for (final Object[] r : rows)
      out.add((Double) r[2]);
    out.sort(null);
    return out;
  }

  private List<Double> expected(final int count) {
    final List<Double> out = new ArrayList<>(count);
    for (int i = 0; i < count; i++)
      out.add((double) i);
    return out;
  }

  @Test
  void aWalkOverAMergedRunAnswersTheWholeRows() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = engine("Slow");
    feedAndCompact(engine, 5, 20, 0, false);

    final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
    final BlockDirectorySnapshot snapshot = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);
    assertThat(snapshot.blocks()).hasSize(5);

    engine.mergeSmallBlocks();
    assertThat(sealed.getBlockCount()).isEqualTo(1);

    final List<Object[]> rows = new ArrayList<>();
    sealed.forEachRow(snapshot, Long.MIN_VALUE, Long.MAX_VALUE, null, null, new AggregationMetrics(), rows::add);
    assertThat(values(rows)).isEqualTo(expected(100));
  }

  @Test
  void aWalkThatIsHalfWayWhenTheMergeLandsEmitsEachRowOnce() throws Exception {
    for (final boolean interleave : new boolean[] { false, true }) {
      final String name = interleave ? "Interleaved" : "Sequential";
      database.command("sql", "CREATE TIMESERIES TYPE " + name + " TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
      final TimeSeriesEngine engine = engine(name);
      feedAndCompact(engine, 6, 20, 0, interleave);

      final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
      final BlockDirectorySnapshot snapshot = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);
      assertThat(snapshot.blocks()).hasSize(6);

      final List<Object[]> rows = new ArrayList<>();
      final boolean[] merged = { false };
      sealed.forEachRow(snapshot, Long.MIN_VALUE, Long.MAX_VALUE, null, null, new AggregationMetrics(), row -> {
        rows.add(row);
        // the merge lands in the middle of the third block (sequential) or of the walk's first block (interleaved)
        if (!merged[0] && rows.size() == 45) {
          merged[0] = true;
          try {
            engine.mergeSmallBlocks();
          } catch (final Exception e) {
            throw new IllegalStateException(e);
          }
        }
        return true;
      });

      assertThat(merged[0]).isTrue();
      assertThat(sealed.getBlockCount()).as(name).isEqualTo(1);
      assertThat(values(rows)).as(name).isEqualTo(expected(120));
    }
  }

  @Test
  void aLazyIteratorThatIsHalfWayWhenTheMergeLandsEmitsEachRowOnce() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = engine("Slow");
    feedAndCompact(engine, 6, 20, 0, false);

    final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
    final Iterator<Object[]> it = sealed.iterateRange(sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE), Long.MIN_VALUE,
        Long.MAX_VALUE, null, null, null);

    final List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < 45; i++)
      rows.add(it.next());
    engine.mergeSmallBlocks();
    while (it.hasNext())
      rows.add(it.next());

    assertThat(values(rows)).isEqualTo(expected(120));
  }

  @Test
  void aWalkSurvivesTwoMergesAndIgnoresTheBlocksAppendedAfterItsSnapshot() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = engine("Slow");
    final long next = feedAndCompact(engine, 6, 20, 0, false);

    final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
    final BlockDirectorySnapshot snapshot = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);

    final List<Object[]> rows = new ArrayList<>();
    final int[] stage = { 0 };
    sealed.forEachRow(snapshot, Long.MIN_VALUE, Long.MAX_VALUE, null, null, new AggregationMetrics(), row -> {
      rows.add(row);
      try {
        if (stage[0] == 0 && rows.size() == 30) {
          stage[0] = 1;
          engine.mergeSmallBlocks();
        } else if (stage[0] == 1 && rows.size() == 50) {
          stage[0] = 2;
          // new small blocks, then a second merge that folds the first merge's block together with them
          feedAndCompact(engine, 3, 20, next, false);
          engine.mergeSmallBlocks();
        }
      } catch (final Exception e) {
        throw new IllegalStateException(e);
      }
      return true;
    });

    assertThat(stage[0]).isEqualTo(2);
    assertThat(sealed.getBlockCount()).isEqualTo(1);
    assertThat(values(rows)).as("only the rows of the snapshot, each once").isEqualTo(expected(120));
  }

  @Test
  void sqlReadsRacingWithTheMaintenanceNeverFail() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = engine("Slow");
    final long next = feedAndCompact(engine, 10, 20, 0, false);
    final long lastInRange = T0 + (next - 1) * 1_000L;

    // The reader asks for the rows of the first 200 samples while the writer keeps adding small blocks AFTER them and merging
    // them into the one the reader is walking: its answer cannot change, and it must never be refused
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicInteger reads = new AtomicInteger();
    final Thread reader = new Thread(() -> {
      try {
        while (!stop.get()) {
          long rows = 0;
          try (final ResultSet rs = database.query("sql", "SELECT FROM Slow WHERE ts BETWEEN " + T0 + " AND " + lastInRange)) {
            while (rs.hasNext()) {
              rs.next();
              rows++;
            }
          }
          if (rows != 200)
            throw new AssertionError("the read returned " + rows + " rows instead of 200");
          reads.incrementAndGet();
        }
      } catch (final Throwable t) {
        failure.compareAndSet(null, t);
      }
    }, "reader");
    reader.start();
    try {
      long value = next;
      long ts = lastInRange + 1_000L;
      for (int round = 0; round < 40 && failure.get() == null; round++) {
        final long[] timestamps = new long[20];
        final Object[] tags = new Object[20];
        final Object[] values = new Object[20];
        for (int i = 0; i < 20; i++) {
          timestamps[i] = ts;
          tags[i] = "series-" + (i % 3);
          values[i] = (double) value++;
          ts += 1_000L;
        }
        engine.appendSamples(timestamps, tags, values);
        engine.compactAll();
        engine.mergeSmallBlocks();
      }
    } finally {
      stop.set(true);
      reader.join();
    }

    assertThat(failure.get()).isNull();
    assertThat(reads.get()).isPositive();
  }

  @Test
  void aTagFilteredWalkAcrossAMergeKeepsOnlyTheMatchingRows() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = engine("Slow");
    feedAndCompact(engine, 6, 21, 0, false);

    final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
    final BlockDirectorySnapshot snapshot = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);
    final TagFilter filter = TagFilter.eq(0, "series-1");

    final List<Object[]> rows = new ArrayList<>();
    final boolean[] merged = { false };
    sealed.forEachRow(snapshot, Long.MIN_VALUE, Long.MAX_VALUE, null, filter, new AggregationMetrics(), row -> {
      rows.add(row);
      if (!merged[0] && rows.size() == 10) {
        merged[0] = true;
        try {
          engine.mergeSmallBlocks();
        } catch (final Exception e) {
          throw new IllegalStateException(e);
        }
      }
      return true;
    });

    final List<Double> expected = new ArrayList<>();
    for (int i = 0; i < 126; i++)
      if (i % 21 % 3 == 1)
        expected.add((double) i);
    assertThat(merged[0]).isTrue();
    assertThat(values(rows)).isEqualTo(expected);
  }
}
