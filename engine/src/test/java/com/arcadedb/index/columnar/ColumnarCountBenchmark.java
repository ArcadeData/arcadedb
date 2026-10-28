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
package com.arcadedb.index.columnar;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.block.BlockCache;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Spike observations 7 and 8 (spec 5.2 and 10), report only: the cost of a facet count through {@link ColumnReader#forEachOrdinal}
 * over the plan 3 layout, for brand and tags (BY ITEM) and a 1% and a 70% match set, in four phases:
 * <ul>
 * <li>(a) one merged segment with a dead ratio of 0.1;</li>
 * <li>(b) 1,048,576 records built into 16 segments plus an overlay of 5,000 rows, dead ratio 0.1 (the build cuts a segment at the
 * first page boundary past {@code flushMaxRows} 65,536, so segments hold about 59,000 to 66,000 rows; the layout is logged);</li>
 * <li>(c) as (b), with {@link BlockCache#clear()} before each measured round (cold block cache, OS cache warm);</li>
 * <li>(d) as (c), with every segment opened afresh through {@link SegmentReader#open} for each round.</li>
 * </ul>
 * Each round is timed with {@code System.nanoTime()} minus the JVM stall observed in the same window
 * ({@link StallAwareStopwatch#jvmStallNanos()}, the stopwatch's own discount at nanosecond resolution: a 1% count takes well under a
 * millisecond, below {@link StallAwareStopwatch#effectiveMs()}'s resolution). The medians are logged through {@link LogManager};
 * no time is asserted. One correctness guard: (b), (c) and (d) count the same match set over the same snapshot, so their count totals
 * must be equal; a mismatch fails the run, as its timings would then compare different work.
 */
@Tag("benchmark")
class ColumnarCountBenchmark extends TestHelper {
  private static final String   TYPE             = "Asset";
  private static final int      ROWS_PER_SEGMENT = 65_536;
  private static final int      SEGMENTS         = 16;
  private static final int      RECORDS          = SEGMENTS * ROWS_PER_SEGMENT;
  private static final int      OVERLAY_ROWS     = 5_000;
  private static final double   DEAD_RATIO       = 0.1;
  private static final double[] SELECTIVITIES    = { 0.01, 0.70 };
  private static final String[] PROPERTIES       = { "brand", "tags" };
  private static final int      BATCH            = 5_000;
  private static final int      WARMUP_ROUNDS    = 3;
  private static final int      MEASURED_ROUNDS  = 11;
  private static final long     CATCH_UP_MS      = TimeUnit.MINUTES.toMillis(10);

  private final Random random = new Random(42);

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Test
  void countOverSegmentsOverlayAndColdCache() throws Exception {
    // no flush by time, one flush per 65,536 rows (the build cuts its segments at the same row count), no merge by count
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, TimeUnit.HOURS.toMillis(1));
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_FLUSH_MAX_ROWS, ROWS_PER_SEGMENT);
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_MERGE_MAX_SEGMENTS, 1_000);

    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".brand STRING");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".tags LIST OF STRING");
    final List<RID> segmentRecords = insert(RECORDS);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) COLUMNAR");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (tags BY ITEM) COLUMNAR");
    awaitCaughtUp();
    final List<RID> overlayRecords = insert(OVERLAY_ROWS);
    delete(segmentRecords, (int) (RECORDS * DEAD_RATIO));
    awaitCaughtUp();

    final ColumnarProjection projection = projection();
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      logLayout("b-d", snapshot);
      final PositionBitmap[] matchSets = matchSets(snapshot);
      for (final String property : PROPERTIES)
        for (int s = 0; s < SELECTIVITIES.length; s++) {
          final long[] warm = measure(snapshot, property, matchSets[s], false, false);
          final long[] cold = measure(snapshot, property, matchSets[s], true, false);
          final long[] reopened = measure(snapshot, property, matchSets[s], true, true);
          report("b 16 segments + overlay", property, s, matchSets[s], warm);
          report("c as b, cold block cache", property, s, matchSets[s], cold);
          report("d as c, fresh SegmentReader.open", property, s, matchSets[s], reopened);
          if (warm[1] != cold[1] || warm[1] != reopened[1])
            throw new IllegalStateException("Count totals of " + property + " differ across phases b, c, d: " + warm[1] + ", " + cold[1]
                + ", " + reopened[1]);
        }
    }

    // (a): the overlay flushed, every segment merged into one, then a dead ratio of 0.1 again on the merged segment
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_MERGE_MAX_SEGMENTS, 1);
    manager().flushNow();
    final int bucketId = projection.bucketId();
    while (projection.manifest().segments().size() > 1)
      manager().maintainForTesting(bucketId);
    final List<RID> remaining = new ArrayList<>(segmentRecords);
    remaining.addAll(overlayRecords);
    delete(remaining, (int) (remaining.size() * DEAD_RATIO));
    awaitCaughtUp();
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      logLayout("a", snapshot);
      final PositionBitmap[] matchSets = matchSets(snapshot);
      for (final String property : PROPERTIES)
        for (int s = 0; s < SELECTIVITIES.length; s++)
          report("a one merged segment", property, s, matchSets[s], measure(snapshot, property, matchSets[s], false, false));
    }
  }

  /** The median effective nanoseconds of a count, and the count total of the last round (compared across phases b, c, d). */
  private long[] measure(final ColumnarSnapshot snapshot, final String property, final PositionBitmap matches, final boolean coldCache,
      final boolean freshReaders) throws IOException {
    final int[] counts = new int[snapshot.dictionary(property).size() + 1];
    final long[] effective = new long[MEASURED_ROUNDS];
    long total = 0;
    for (int round = 0; round < WARMUP_ROUNDS + MEASURED_ROUNDS; round++) {
      Arrays.fill(counts, 0);
      if (coldCache)
        BlockCache.instance().clear();
      final long startStall = StallAwareStopwatch.jvmStallNanos();
      final long start = System.nanoTime();
      final List<ColumnarSegmentView> views = freshReaders ? reopen(snapshot.segments()) : snapshot.segments();
      try {
        for (final ColumnarSegmentView view : views) {
          final ColumnReader reader = view.column(property);
          if (reader != null)
            reader.forEachOrdinal(matches, (position, ordinal) -> counts[ordinal]++);
        }
      } finally {
        if (freshReaders)
          for (final ColumnarSegmentView view : views)
            if (view instanceof SegmentReader reader)
              reader.close();
      }
      final long nanos = Math.max(0, System.nanoTime() - start - (StallAwareStopwatch.jvmStallNanos() - startStall));
      if (round >= WARMUP_ROUNDS)
        effective[round - WARMUP_ROUNDS] = nanos;
      total = 0;
      for (final int c : counts)
        total += c;
    }
    Arrays.sort(effective);
    return new long[] { effective[MEASURED_ROUNDS / 2], total };
  }

  /** The views with every segment opened afresh on its file and dead set; the overlay view is kept. */
  private static List<ColumnarSegmentView> reopen(final List<ColumnarSegmentView> views) throws IOException {
    final List<ColumnarSegmentView> fresh = new ArrayList<>(views.size());
    for (final ColumnarSegmentView view : views)
      if (view instanceof SegmentReader reader)
        fresh.add(SegmentReader.open(reader.file().path(), reader.deadSet() == null ? null : reader.deadSet().copy()));
      else
        fresh.add(view);
    return fresh;
  }

  /** One match set per selectivity, drawn from the live positions of the snapshot. */
  private PositionBitmap[] matchSets(final ColumnarSnapshot snapshot) {
    final int maxRecordsInPage = projection().maxRecordsInPage();
    final PositionBitmap live = new PositionBitmap(maxRecordsInPage);
    for (final ColumnarSegmentView view : snapshot.segments())
      live.or(view.live());
    final PositionBitmap[] sets = new PositionBitmap[SELECTIVITIES.length];
    for (int s = 0; s < sets.length; s++) {
      final PositionBitmap set = new PositionBitmap(maxRecordsInPage);
      final double selectivity = SELECTIVITIES[s];
      live.forEach(position -> {
        if (random.nextDouble() < selectivity)
          set.set(position);
      });
      sets[s] = set;
    }
    return sets;
  }

  private void report(final String phase, final String property, final int selectivity, final PositionBitmap matches, final long[] result) {
    LogManager.instance().log(this, Level.INFO, String.format(Locale.ROOT, "COUNT %-34s %-5s %3.0f%% (%,d matches): median %8.3f ms, total %,d",
        phase, property, SELECTIVITIES[selectivity] * 100, matches.cardinality(), result[0] / 1_000_000.0, result[1]));
  }

  private void logLayout(final String phases, final ColumnarSnapshot snapshot) {
    final StringBuilder rows = new StringBuilder();
    long dead = 0;
    for (final ColumnarSegmentView view : snapshot.segments())
      if (view instanceof SegmentReader reader) {
        rows.append(reader.rowCount()).append(' ');
        dead += reader.deadCount();
      } else
        rows.append("overlay:").append(view.live().cardinality()).append(' ');
    LogManager.instance().log(this, Level.INFO, "COUNT layout of phase(s) %s: %d views, rows %s, %d dead segment rows", phases,
        snapshot.segments().size(), rows.toString().trim(), dead);
  }

  private List<RID> insert(final int count) {
    final List<RID> rids = new ArrayList<>(count);
    for (int done = 0; done < count; done += BATCH) {
      final int to = Math.min(done + BATCH, count);
      database.transaction(() -> {
        while (rids.size() < to) {
          final MutableDocument record = database.newDocument(TYPE);
          record.set("brand", "brand-" + random.nextInt(200));
          final int tagCount = random.nextInt(9);
          final List<String> tags = new ArrayList<>(tagCount);
          for (int t = 0; t < tagCount; t++)
            tags.add("tag-" + random.nextInt(2_000));
          record.set("tags", tags);
          record.save();
          rids.add(record.getIdentity());
        }
      });
    }
    return rids;
  }

  /** Deletes {@code count} records drawn at random from {@code rids}, which loses them. */
  private void delete(final List<RID> rids, final int count) {
    for (int done = 0; done < count; done += BATCH) {
      final int batch = Math.min(BATCH, count - done);
      database.transaction(() -> {
        for (int i = 0; i < batch; i++) {
          final int index = random.nextInt(rids.size());
          final RID rid = rids.get(index);
          rids.set(index, rids.getLast());
          rids.removeLast();
          database.deleteRecord(database.lookupByRID(rid, false));
        }
      });
    }
  }

  private void awaitCaughtUp() throws InterruptedException {
    if (!manager().awaitCaughtUp(CATCH_UP_MS))
      throw new IllegalStateException("The columnar projections did not catch up within " + CATCH_UP_MS + " ms");
  }

  private ColumnarProjection projection() {
    return manager().projection(database.getSchema().getType(TYPE).getBuckets(false).getFirst().getFileId());
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }
}
