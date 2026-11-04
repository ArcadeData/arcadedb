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
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Task 2 of plan 7 (#10662), first lever (allocation-free old-value lookups): the apply pass reads the old value of a re-read record
 * through a {@link ColumnarProjection.LiveLookup}, which locates the position once per record and reads the segment blocks through a
 * pass-owned memo instead of the per-call allocating point reads. It must answer exactly what {@link ColumnarProjection#liveValueLookup}
 * (the reference: column readers) answers, for every column kind and wherever the live value is.
 */
class ColumnarOldValueLookupTest extends TestHelper {
  private static final String TYPE        = ColumnarTestData.TYPE;
  private static final String[] PROPERTIES = { "brand", "tags", "fileSize", "publishDate", "author", "score", "day" };
  private static final int    LOOKUPS     = 10_000;

  @Override
  protected void beginTest() {
    // no background flush or merge: the test moves rows between the overlay and the segments itself
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, TimeUnit.HOURS.toMillis(1));
  }

  @Test
  void oldValueLookupEqualsTheAllocatingLookupForEveryColumnKind() throws Exception {
    final ColumnarTestData data = data(11);
    data.insertAssets(database, 3_000);
    caughtUp();
    assertThat(assertLookupsAgree("inserted, overlay only").get("overlay")).isPositive();

    manager().flushNow();
    data.mutate(database, 1_500);
    caughtUp();
    TreeMap<String, Integer> seen = assertLookupsAgree("one segment, dead rows, overlay rows");
    assertThat(seen.get("overlay")).isPositive();
    assertThat(seen.get("segment")).isPositive();
    assertThat(seen.get("absent")).as("deleted positions").isPositive();

    manager().flushNow();
    data.mutate(database, 1_500);
    caughtUp();
    seen = assertLookupsAgree("two segments");
    assertThat(seen.get("segments")).as("distinct segments answering").isEqualTo(2);

    // every segment merged into one: each row moved to a new segment
    final long[] ids = segmentIds();
    assertThat(ids).hasSize(2);
    ColumnarFlusher.merge(projection(), ids);
    assertThat(segmentIds()).hasSize(1).doesNotContain(ids);
    data.mutate(database, 500);
    caughtUp();
    seen = assertLookupsAgree("merged");
    assertThat(seen.get("segment")).isPositive();

    // a frozen overlay of a flush stopped after its segment is written, then newer values superseding some of its rows
    final KillPoint killPoint = KillPoint.arm(ColumnarFlusher.FLUSH_SEGMENT_WRITTEN);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> flush = executor.submit(() -> {
        manager().flushNow();
        return null;
      });
      assertThat(killPoint.awaitReached(10_000)).isTrue();
      assertThat(projection().hasFrozenOverlay()).isTrue();
      data.mutate(database, 300);
      caughtUp();
      seen = assertLookupsAgree("frozen overlay");
      assertThat(seen.get("frozen")).isPositive();
      assertThat(seen.get("overlay")).isPositive();
      killPoint.releaseAsCrashed();
      try {
        flush.get(30, TimeUnit.SECONDS);
      } catch (final Exception expected) {
        // the simulated crash of the flush
      }
    } finally {
      killPoint.disarm();
      executor.shutdownNow();
    }
    manager().flushNow();
    assertLookupsAgree("after the retried flush");
  }

  @Test
  void applyComparesOldValuesExactly() throws Exception {
    final ColumnarTestData data = data(12);
    final List<RID> rids = data.insertAssets(database, 2_000);
    caughtUp();
    for (final String round : new String[] { "first", "second" }) {
      manager().flushNow();
      assertThat(projection().overlay().rows()).isZero();

      // a change outside the projected properties is a no-op for every column: no row may be staged
      database.transaction(() -> {
        for (int i = 0; i < rids.size(); i += 7)
          if (database.existsRecord(rids.get(i)))
            database.lookupByRID(rids.get(i), true).asDocument().modify().set("body", "unprojected " + round).save();
      });
      caughtUp();
      assertThat(projection().overlay().rows()).as(round + ": unprojected changes stage nothing").isZero();

      // one pass: new dictionary values appended by early records, compared for later ones; updates, deletes and inserts
      try (final AutoCloseable hold = manager().suspendApply()) {
        database.transaction(() -> {
          for (int i = 1; i < rids.size(); i += 5)
            if (database.existsRecord(rids.get(i)))
              database.lookupByRID(rids.get(i), true).asDocument().modify().set("brand", "fresh-" + (i % 3) + "-" + round).save();
        });
        data.mutate(database, 400);
      }
      caughtUp();
      for (final String property : PROPERTIES)
        FacetOracle.assertEquivalent(database, TYPE, property, null);
      assertLookupsAgree(round);
    }
  }

  @Test
  void lookupAllocatesNothingPerCall() throws Exception {
    final ColumnarTestData data = data(13);
    data.insertAssets(database, 3_000);
    caughtUp();
    manager().flushNow();
    data.mutate(database, 600);
    caughtUp();
    manager().flushNow();

    final ColumnarProjection projection = projection();
    final int columns = projection.columns().length;
    final long[] positions = livePositions();
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final long[] allocated = new long[2];
    projection.applyLock().lock();
    try {
      for (int round = 0; round < 3; round++)
        for (final boolean pass : new boolean[] { true, false }) {
          final ColumnarProjection.LiveLookup lookup = new ColumnarProjection.LiveLookup(projection);
          final RowValues out = new RowValues(columns);
          // the first record allocates the list arrays of the RowValues and loads the blocks: outside the measure
          for (int k = 0; k < columns; k++)
            lookup.lookup(positions[0], k, out);
          final long before = threads.getCurrentThreadAllocatedBytes();
          for (int i = 0; i < LOOKUPS; i++) {
            final long position = positions[(i / columns) % positions.length];
            if (pass)
              lookup.lookup(position, i % columns, out);
            else
              projection.liveValueLookup(position, i % columns, out);
          }
          allocated[pass ? 0 : 1] = threads.getCurrentThreadAllocatedBytes() - before;
          lookup.clear();
        }
    } finally {
      projection.applyLock().unlock();
    }

    // one object is at least 16 bytes: below one object per lookup on the pass's lookup, above on the reference (a lookup per call),
    // which proves the measure can see an allocation
    assertThat(allocated[0]).as("bytes of %d lookups, the pass's lookup", LOOKUPS).isLessThan(16L * LOOKUPS);
    assertThat(allocated[1]).as("bytes of %d lookups, liveValueLookup", LOOKUPS).isGreaterThan(16L * LOOKUPS);
  }

  /**
   * Under the apply lock: every position of the bucket (and the page after the last) looked up for every column, ascending and then
   * shuffled, through one {@link ColumnarProjection.LiveLookup}, equals {@link ColumnarProjection#liveValueLookup}.
   * Returns how many positions each source answered ({@code overlay}, {@code frozen}, {@code segment}, {@code absent}) and the number of
   * distinct {@code segments}.
   */
  private TreeMap<String, Integer> assertLookupsAgree(final String label) {
    final ColumnarProjection projection = projection();
    final int columns = projection.columns().length;
    assertThat(columns).isEqualTo(PROPERTIES.length);
    final long end = (bucket().getTotalPages() + 1L) * bucket().getMaxRecordsInPage();
    final List<Long> order = new ArrayList<>();
    for (long position = 0; position < end; position++)
      order.add(position);
    final List<Long> shuffled = new ArrayList<>(order);
    Collections.shuffle(shuffled, new Random(label.hashCode()));

    final TreeMap<String, Integer> seen = new TreeMap<>();
    final Set<Long> segments = new HashSet<>();
    projection.applyLock().lock();
    try {
      final ColumnarProjection.LiveLookup lookup = new ColumnarProjection.LiveLookup(projection);
      final RowValues expected = new RowValues(columns);
      final RowValues actual = new RowValues(columns);
      for (final List<Long> positions : List.of(order, shuffled)) {
        for (final long position : positions)
          for (int k = 0; k < columns; k++) {
            final long where = projection.liveValueLookup(position, k, expected);
            assertThat(lookup.lookup(position, k, actual)).as("%s: source of position %d, column %d", label, position, k).isEqualTo(where);
            assertSameValue(projection.columns()[k], k, expected, actual, label + ": position " + position);
            if (positions == order && k == 0) {
              seen.merge(source(where), 1, Integer::sum);
              if (where >= 0)
                segments.add(where);
            }
          }
        lookup.clear();
      }
    } finally {
      projection.applyLock().unlock();
    }
    seen.put("segments", segments.size());
    for (final String source : List.of("overlay", "frozen", "segment", "absent"))
      seen.putIfAbsent(source, 0);
    return seen;
  }

  private static void assertSameValue(final ColumnDefinition column, final int k, final RowValues expected, final RowValues actual,
      final String label) {
    assertThat(actual.present[k]).as("%s, %s present", label, column.property()).isEqualTo(expected.present[k]);
    if (!expected.present[k])
      return;
    switch (column.kind()) {
    case ORDINAL -> assertThat(actual.ordinal[k]).as("%s, %s", label, column.property()).isEqualTo(expected.ordinal[k]);
    case LONG -> assertThat(actual.longs[k]).as("%s, %s", label, column.property()).isEqualTo(expected.longs[k]);
    case DOUBLE -> assertThat(Double.doubleToLongBits(actual.doubles[k])).as("%s, %s", label, column.property())
        .isEqualTo(Double.doubleToLongBits(expected.doubles[k]));
    case ORDINAL_LIST -> {
      assertThat(actual.listCount[k]).as("%s, %s count", label, column.property()).isEqualTo(expected.listCount[k]);
      for (int i = 0; i < expected.listCount[k]; i++)
        assertThat(actual.list[k][i]).as("%s, %s item %d", label, column.property(), i).isEqualTo(expected.list[k][i]);
    }
    }
  }

  private static String source(final long where) {
    if (where == ColumnarProjection.LOOKUP_OVERLAY)
      return "overlay";
    if (where == ColumnarProjection.LOOKUP_FROZEN)
      return "frozen";
    return where == ColumnarProjection.LOOKUP_ABSENT ? "absent" : "segment";
  }

  /** The positions a segment answers, ascending. */
  private long[] livePositions() {
    final ColumnarProjection projection = projection();
    final RowValues out = new RowValues(projection.columns().length);
    final long end = bucket().getTotalPages() * (long) bucket().getMaxRecordsInPage();
    final List<Long> found = new ArrayList<>();
    projection.applyLock().lock();
    try {
      for (long position = 0; position < end; position++)
        if (projection.liveValueLookup(position, 0, out) >= 0)
          found.add(position);
    } finally {
      projection.applyLock().unlock();
    }
    assertThat(found).hasSizeGreaterThan(1_000);
    final long[] positions = new long[found.size()];
    for (int i = 0; i < positions.length; i++)
      positions[i] = found.get(i);
    return positions;
  }

  private ColumnarTestData data(final long seed) {
    final ColumnarTestData data = new ColumnarTestData(seed);
    data.doubleColumn = true;
    data.dateColumn = true;
    data.deleteRatio = 0.1;
    data.reuseRatio = 0.05;
    data.createSchema(database);
    return data;
  }

  private long[] segmentIds() {
    final List<ColumnarManifest.SegmentEntry> segments = projection().manifest().segments();
    final long[] ids = new long[segments.size()];
    for (int i = 0; i < ids.length; i++)
      ids[i] = segments.get(i).segmentId();
    return ids;
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private LocalBucket bucket() {
    return (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
  }

  private ColumnarProjection projection() {
    return manager().projection(bucket().getFileId());
  }

  private void caughtUp() throws InterruptedException {
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();
  }
}
