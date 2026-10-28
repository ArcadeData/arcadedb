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
package com.arcadedb.query.facet;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.index.columnar.ColumnReader;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.PositionBitmap;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;

/**
 * Counts a range facet for one declaring type (D17): the bounds are converted once into the encoding of the type's column (a LONG
 * column, dates included, is counted exactly in longs, a DOUBLE column in doubles, see {@link RangeBucketer}), and every matched value
 * is located among the elementary intervals of the sorted distinct bounds. A segment view whose {@code [minLong, maxLong]} lies in one
 * interval is counted by presence alone, without decoding a value. Ranges are half-open and may overlap; a present value in no range
 * counts in the {@code DEFAULT} bucket, a matched row without a value as missing.
 * <p>
 * Collectors of several declaring types merge by summing per range: each converted the bounds with its own column, so a DATE and a
 * DATETIME declaration of the same property count the same literal bound at their own precision.
 * <p>
 * Not thread safe: a collector is driven by one query thread.
 */
public final class RangeCollector implements FacetCollector {
  private final RangeFacetSpec        spec;
  /** Null when no bucket is counted: every range answers 0. */
  private final ColumnDefinition      column;
  private final boolean               doubles;
  private final CollectorReservations budget;
  private final FacetExecutionStats   stats  = new FacetExecutionStats();
  private final List<RangeCollector>  others = new ArrayList<>();
  private final Rows                  rows   = new Rows();
  private final Presence              presence = new Presence();
  // the bounds in the column's encoding, until the bucketer is allocated
  private long[]                      longFrom;
  private long[]                      longTo;
  private boolean[]                   fromUnbounded;
  private boolean[]                   toUnbounded;
  private double[]                    doubleFrom;
  private double[]                    doubleTo;

  private RangeBucketer bucketer;
  private long          missing;
  private boolean       unavailable;
  private boolean       finished;

  /**
   * @param column    the column of the property in the declaring type, null when no bucket is counted
   * @param database  converts the date literals of a date column
   * @param extractor the declaring type's extractor: converts a literal bound as stored values are converted
   *
   * @throws CommandExecutionException  when the column is not a number or date column, or a bound is not a value of it
   * @throws CommandSQLParsingException when a range is empty once its bounds are in the column's encoding
   */
  RangeCollector(final RangeFacetSpec spec, final ColumnDefinition column, final long budgetWaitMs, final DatabaseInternal database,
      final ColumnConversion extractor) {
    this.spec = spec;
    this.column = column;
    this.doubles = column != null && column.kind() == ColumnKind.DOUBLE;
    this.budget = new CollectorReservations(budgetWaitMs);
    if (column != null)
      convertBounds(database, extractor);
  }

  @Override
  public void beginBucket(final int bucketId, final ColumnarSnapshot snapshot, final CrossBucketOrdinalMap map) {
    if (bucketer != null || unavailable || column == null)
      return;
    // reserved before it is allocated (D13)
    final FacetMemoryBudget.Reservation granted = budget.reserve(bucketerBytes(spec.ranges().size()), true);
    if (granted == null) {
      unavailable = true;
      return;
    }
    stats.reserved(granted.bytes());
    bucketer = doubles ? new RangeBucketer(doubleFrom, doubleTo) : new RangeBucketer(longFrom, longTo, fromUnbounded, toUnbounded);
  }

  @Override
  public void collect(final ColumnarSegmentView segment, final PositionBitmap matches) {
    if (unavailable)
      return;
    final ColumnReader reader = segment.column(spec.property());
    if (reader == null)
      throw new IllegalStateException("FACETS " + spec.alias() + ": a segment view without the column '" + spec.property() + "'");
    final PositionBitmap live = segment.live();
    final long matchedLive = matches.andCardinality(live);
    if (matchedLive == 0) {
      stats.segmentSkipped();
      return;
    }
    // a DOUBLE view's bounds leave NaN out (NaN is never a bound), so only a LONG view can be counted by its bounds
    if (!doubles) {
      final long min = segment.minLong(spec.property());
      final long max = segment.maxLong(spec.property());
      if (min == Long.MAX_VALUE && max == Long.MIN_VALUE) {
        // the view holds no value at all
        missing += matchedLive;
        stats.segmentSkipped();
        return;
      }
      final int interval = bucketer.singleIntervalOf(min, max);
      if (interval >= 0) {
        presence.reader = reader;
        presence.present = 0;
        matches.forEachAnd(live, presence);
        presence.reader = null;
        bucketer.addInterval(interval, presence.present);
        missing += matchedLive - presence.present;
        stats.segmentSkipped();
        return;
      }
    }
    stats.segmentScanned();
    rows.reader = reader;
    matches.forEachAnd(live, rows);
    rows.reader = null;
  }

  @Override
  public void collectPatched(final long position, final PatchedValue value) {
    if (unavailable)
      return;
    if (value.isMissing())
      missing++;
    else if (value.hasDouble())
      bucketer.add(value.doubleValue());
    else if (value.hasLong()) {
      if (doubles)
        bucketer.add((double) value.longValue());
      else
        bucketer.add(value.longValue());
    }
  }

  @Override
  public void merge(final FacetCollector other) {
    if (!(other instanceof RangeCollector collector))
      throw new IllegalArgumentException("FACETS " + spec.alias() + ": cannot merge a " + other.getClass().getSimpleName() + " into ranges");
    others.add(collector);
    stats.add(collector.stats);
  }

  @Override
  public FacetResult finish() {
    if (finished)
      throw new IllegalStateException("FACETS " + spec.alias() + ": already finished");
    try {
      for (final RangeCollector collector : all())
        if (collector.unavailable) {
          stats.source(FacetResult.UNAVAILABLE);
          return FacetResult.unavailable("range", TermsCollector.MEMORY_BUDGET);
        }
      stats.source("index");
      final List<RangeBound> ranges = spec.ranges();
      final long[] counts = new long[ranges.size()];
      long outside = 0;
      long missingTotal = 0;
      for (final RangeCollector collector : all()) {
        missingTotal += collector.missing;
        if (collector.bucketer == null)
          continue;
        for (int r = 0; r < counts.length; r++)
          counts[r] += collector.bucketer.rangeCount(r);
        outside += collector.bucketer.defaultCount();
      }
      final List<FacetBucket> buckets = new ArrayList<>(counts.length + 1);
      for (int r = 0; r < counts.length; r++) {
        final RangeBound range = ranges.get(r);
        buckets.add(FacetBucket.range(range.key(), range.from(), range.to(), counts[r]));
      }
      if (spec.defaultLabel() != null)
        buckets.add(FacetBucket.rangeDefault(spec.defaultLabel(), outside));
      return FacetResult.of("range", "index", buckets, 0L, missingTotal);
    } finally {
      finished = true;
      release();
      for (final RangeCollector collector : others)
        collector.release();
    }
  }

  @Override
  public void release() {
    budget.release();
  }

  @Override
  public FacetExecutionStats stats() {
    return stats;
  }

  @Override
  public int budgetWaits() {
    int waits = budget.waits();
    for (final RangeCollector collector : others)
      waits += collector.budget.waits();
    return waits;
  }

  /** An upper bound of the heap of a bucketer over {@code ranges} ranges: bounds, intervals, counters, prefix sums and layout. */
  static long bucketerBytes(final int ranges) {
    return 128L + 64L * ranges;
  }

  private List<RangeCollector> all() {
    final List<RangeCollector> all = new ArrayList<>(others.size() + 1);
    all.add(this);
    all.addAll(others);
    return all;
  }

  private void convertBounds(final DatabaseInternal database, final ColumnConversion extractor) {
    if (column.kind() != ColumnKind.LONG && column.kind() != ColumnKind.DOUBLE)
      throw new CommandExecutionException(
          "FACETS " + spec.alias() + ": RANGES needs a number or date property, '" + spec.property() + "' is " + column.propertyType());
    final List<RangeBound> ranges = spec.ranges();
    final int n = ranges.size();
    if (doubles) {
      doubleFrom = new double[n];
      doubleTo = new double[n];
    } else {
      longFrom = new long[n];
      longTo = new long[n];
      fromUnbounded = new boolean[n];
      toUnbounded = new boolean[n];
    }
    for (int r = 0; r < n; r++) {
      final RangeBound range = ranges.get(r);
      // an infinite end is unbounded, not saturated: [x, +Infinity) holds every value from x on, Long.MAX_VALUE included
      final boolean noFrom = range.from() == null || FacetBounds.infinity(range.from()) < 0;
      final boolean noTo = range.to() == null || FacetBounds.infinity(range.to()) > 0;
      if (doubles) {
        doubleFrom[r] = noFrom ? Double.NaN : FacetBounds.toColumnDouble(spec.alias(), column, range.from());
        doubleTo[r] = noTo ? Double.NaN : FacetBounds.toColumnDouble(spec.alias(), column, range.to());
        if (!noFrom && !noTo && doubleFrom[r] >= doubleTo[r])
          throw FacetSpecCompiler.emptyRange(spec.alias(), range.from(), range.to());
      } else {
        fromUnbounded[r] = noFrom;
        toUnbounded[r] = noTo;
        if (!fromUnbounded[r])
          longFrom[r] = FacetBounds.toColumnLong(spec.alias(), database, extractor, column, range.from());
        if (!toUnbounded[r])
          longTo[r] = FacetBounds.toColumnLong(spec.alias(), database, extractor, column, range.to());
        // re-checked in the column's encoding: [1.2, 1.5) on an integral column holds no value
        if (!fromUnbounded[r] && !toUnbounded[r] && longFrom[r] >= longTo[r])
          throw FacetSpecCompiler.emptyRange(spec.alias(), range.from(), range.to());
      }
    }
  }

  /** Locates the matched live values of a segment view among the intervals ({@link PositionBitmap#forEachAnd}); reused. */
  private final class Rows implements LongConsumer {
    private ColumnReader reader;

    @Override
    public void accept(final long position) {
      if (!reader.isPresent(position))
        missing++;
      else if (doubles)
        bucketer.add(reader.doubleValue(position));
      else
        bucketer.add(reader.longValue(position));
    }
  }

  /** Counts the matched live rows that hold a value, without decoding it; reused (also by {@link HistogramCollector}). */
  static final class Presence implements LongConsumer {
    ColumnReader reader;
    long         present;

    @Override
    public void accept(final long position) {
      if (reader.isPresent(position))
        present++;
    }
  }
}
