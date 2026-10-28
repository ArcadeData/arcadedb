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
import com.arcadedb.function.sql.time.SQLFunctionTimeBucket;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.index.columnar.ColumnReader;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.ColumnarValueExtractor;
import com.arcadedb.index.columnar.PositionBitmap;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

/**
 * Counts a histogram facet for one declaring type (D18, D19). The bucket boundaries depend on every matched value, of every declaring
 * type of the facet, so {@link #collect} only keeps the segment views with matched live rows and their matches (they stay valid
 * until the executor returns), {@link #collectPatched} buffers the patched values (charged to the budget), and {@link #finish} builds
 * the boundaries once and counts.
 * <p>
 * Boundaries: from {@code BOUNDS} (extended bounds: the buckets exist, values outside are still counted) together with the matched
 * values, taken from the views' {@code minLong/maxLong} and the patched values. Those bounds also cover rows the query did not match;
 * when they would need more than 100,000 buckets, a precise first pass reads the minimum and maximum of the matched values before the
 * histogram is refused. A time interval (CALENDAR, or FIXED {@code N(s|m|h|d|w)}) buckets epoch milliseconds through
 * {@link CalendarBoundaries}: a date column's epoch counts are converted with its own unit, an integer column is read as epoch
 * milliseconds. A FIXED number buckets the numbers themselves, starting at {@code OFFSET + k * step} (ES {@code histogram.offset}):
 * exactly in longs when every column is a LONG and the step and offset are integral, else in doubles.
 * <p>
 * Collectors of several declaring types merge on these converted values (a DATE and a DATETIME declaration share a day's bucket).
 * A view whose values all fall in one bucket is counted by presence alone; a NaN of a DOUBLE column is in no bucket and is counted
 * as missing (a histogram has no default bucket), so the buckets plus {@code missing} always add up to the matched rows.
 * <p>
 * Not thread safe: a collector is driven by one query thread.
 */
public final class HistogramCollector implements FacetCollector {
  static final         int      MAX_BUCKETS = 100_000;
  private static final long[]   NO_LONGS    = new long[0];
  private static final double[] NO_DOUBLES  = new double[0];
  private static final int      FIRST_PATCH = 64;

  /** What the buckets are laid over, one for every collector of the facet. */
  private enum Grid {
    /** Epoch milliseconds, through {@link CalendarBoundaries}. */
    TIME,
    /** Longs with an integral step, through {@link CalendarBoundaries#fixed}. */
    LONG,
    /** Doubles. */
    DOUBLE
  }

  private final HistogramFacetSpec        spec;
  /** Null when no bucket is counted. */
  private final ColumnDefinition          column;
  private final boolean                   doubles;
  /** The unit of a date column's epoch counts, null for a column that is not a date. */
  private final TimeUnit                  dateUnit;
  private final DatabaseInternal          database;
  private final ColumnarValueExtractor    extractor;
  private final CollectorReservations     budget;
  private final FacetExecutionStats       stats       = new FacetExecutionStats();
  private final List<HistogramCollector>  others      = new ArrayList<>();
  private final List<ColumnarSegmentView> views       = new ArrayList<>();
  private final List<PositionBitmap>      viewMatches = new ArrayList<>();

  private long[]   patchedLongs   = NO_LONGS;
  private double[] patchedDoubles = NO_DOUBLES;
  private int      patchedCount;
  private long     missing;
  private boolean  unavailable;
  private boolean  finished;

  /**
   * @param column    the column of the property in the declaring type, null when no bucket is counted
   * @param database  converts the date literals of BOUNDS on a date column
   * @param extractor the declaring type's extractor: converts BOUNDS as stored values are converted
   *
   * @throws CommandExecutionException when the column cannot be bucketed by the interval
   */
  HistogramCollector(final HistogramFacetSpec spec, final ColumnDefinition column, final long budgetWaitMs, final DatabaseInternal database,
      final ColumnarValueExtractor extractor) {
    this.spec = spec;
    this.column = column;
    this.database = database;
    this.extractor = extractor;
    this.budget = new CollectorReservations(budgetWaitMs);
    this.doubles = column != null && column.kind() == ColumnKind.DOUBLE;
    this.dateUnit = column != null && column.kind() == ColumnKind.LONG ? extractor.dateUnit(column.property()) : null;
    if (column != null)
      validate();
  }

  @Override
  public void beginBucket(final int bucketId, final ColumnarSnapshot snapshot, final CrossBucketOrdinalMap map) {
    // nothing per bucket: positions are only read at finish
  }

  @Override
  public void collect(final ColumnarSegmentView segment, final PositionBitmap matches) {
    if (unavailable)
      return;
    if (segment.column(spec.property()) == null)
      throw new IllegalStateException("FACETS " + spec.alias() + ": a segment view without the column '" + spec.property() + "'");
    if (matches.andCardinality(segment.live()) == 0) {
      stats.segmentSkipped();
      return;
    }
    views.add(segment);
    viewMatches.add(matches);
  }

  @Override
  public void collectPatched(final long position, final PatchedValue value) {
    if (unavailable)
      return;
    if (value.isMissing()) {
      missing++;
      return;
    }
    if (!value.hasLong() && !value.hasDouble())
      return;
    if (patchedCount == Math.max(patchedLongs.length, patchedDoubles.length) && !growPatched())
      return;
    if (doubles)
      patchedDoubles[patchedCount++] = value.hasDouble() ? value.doubleValue() : value.longValue();
    else
      patchedLongs[patchedCount++] = value.longValue();
  }

  @Override
  public void merge(final FacetCollector other) {
    if (!(other instanceof HistogramCollector collector))
      throw new IllegalArgumentException(
          "FACETS " + spec.alias() + ": cannot merge a " + other.getClass().getSimpleName() + " into a histogram");
    others.add(collector);
    stats.add(collector.stats);
  }

  @Override
  public FacetResult finish() {
    if (finished)
      throw new IllegalStateException("FACETS " + spec.alias() + ": already finished");
    try {
      for (final HistogramCollector collector : all())
        if (collector.unavailable)
          return unavailableResult();
      final Grid grid = grid();
      // boundaries (just built, at most the bucket cap) are as large as the counts: both are charged before the counts are allocated; a
      // double grid has no boundary array
      final Layout layout = layout(grid);
      final int size = layout == null ? 0 : layout.size();
      if (size > 0) {
        final long bytes = 16L + (layout.boundaries != null ? 16L : 8L) * size;
        final FacetMemoryBudget.Reservation granted = budget.reserve(bytes, true);
        if (granted == null)
          return unavailableResult();
        stats.reserved(granted.bytes());
        stats.counterBytes(bytes);
      }
      final long[] counts = new long[size];
      // without a layout no matched row holds a value: the rows are still counted, as missing
      for (final HistogramCollector collector : all())
        collector.countInto(counts, layout, grid, stats);

      final List<FacetBucket> buckets = new ArrayList<>();
      for (int i = 0; i < counts.length; i++)
        if (counts[i] >= spec.minCount())
          buckets.add(FacetBucket.histogram(layout.key(i), layout.from(i), layout.to(i), counts[i]));
      stats.source("index");
      return FacetResult.of("histogram", "index", buckets, 0L, missingTotal());
    } finally {
      finished = true;
      release();
      for (final HistogramCollector collector : others)
        collector.release();
    }
  }

  @Override
  public void release() {
    budget.release();
    patchedLongs = NO_LONGS;
    patchedDoubles = NO_DOUBLES;
    patchedCount = 0;
  }

  @Override
  public FacetExecutionStats stats() {
    return stats;
  }

  @Override
  public int budgetWaits() {
    int waits = budget.waits();
    for (final HistogramCollector collector : others)
      waits += collector.budget.waits();
    return waits;
  }

  // ------------------------------------------------------------------------------------------------------------- boundaries

  private void validate() {
    final boolean time = isTimeInterval();
    if (column.kind() != ColumnKind.LONG && column.kind() != ColumnKind.DOUBLE)
      throw new CommandExecutionException(
          "FACETS " + spec.alias() + ": HISTOGRAM needs a number or date property, '" + spec.property() + "' is " + column.propertyType());
    if (time && doubles)
      throw new CommandExecutionException(
          "FACETS " + spec.alias() + ": the time interval '" + spec.interval().text() + "' needs a date or integer property, '" + spec.property()
              + "' is " + column.propertyType());
    if (!time && dateUnit != null)
      throw new CommandExecutionException(
          "FACETS " + spec.alias() + ": FIXED " + spec.interval().text() + " needs a number property, '" + spec.property() + "' is "
              + column.propertyType() + ": use a time interval such as FIXED '1d' or CALENDAR '1M'");
  }

  private boolean isTimeInterval() {
    return spec.interval().calendar() || spec.interval().fixedMillis() > 0;
  }

  private Grid grid() {
    if (isTimeInterval())
      return Grid.TIME;
    final double step = spec.interval().fixedNumber();
    final double offset = numericOffset();
    if (step != Math.rint(step) || step >= Long.MAX_VALUE || offset != Math.rint(offset) || Math.abs(offset) >= Long.MAX_VALUE)
      return Grid.DOUBLE;
    for (final HistogramCollector collector : all())
      if (collector.doubles)
        return Grid.DOUBLE;
    return Grid.LONG;
  }

  /**
   * The bucket layout over BOUNDS and the matched values, null when there is neither.
   *
   * @throws CommandExecutionException above 100,000 buckets even over the matched values alone
   */
  private Layout layout(final Grid grid) {
    final Span span = new Span();
    boolean fromViews = false;
    for (final HistogramCollector collector : all()) {
      fromViews |= collector.addViewBounds(span, grid);
      collector.addPatched(span, grid);
    }
    addBounds(span, grid);
    if (span.empty)
      return null;
    try {
      return layout(grid, span);
    } catch (final CommandExecutionException e) {
      if (!fromViews)
        throw e;
      // the views' bounds also cover rows the query did not match, and dead ones: the matched values may need fewer buckets
      final Span exact = new Span();
      for (final HistogramCollector collector : all()) {
        collector.addMatchedValues(exact, grid);
        collector.addPatched(exact, grid);
      }
      addBounds(exact, grid);
      if (exact.empty)
        return null;
      return layout(grid, exact);
    }
  }

  private Layout layout(final Grid grid, final Span span) {
    final HistogramInterval interval = spec.interval();
    return switch (grid) {
      case TIME -> {
        final ZoneId zone = spec.timezone() != null ? ZoneId.of(spec.timezone().trim()) : ZoneOffset.UTC;
        if (interval.calendar()) {
          final long offsetMs = spec.offset() != null ? SQLFunctionTimeBucket.offsetToMs(spec.offset()) : 0L;
          yield new Layout(CalendarBoundaries.calendar(spec.alias(), interval.calendarUnit(), zone, Duration.ofMillis(offsetMs), span.low,
              span.high, MAX_BUCKETS), false);
        }
        yield new Layout(CalendarBoundaries.fixed(spec.alias(), zone, interval.fixedMillis(), fixedOffsetMs(), span.low, span.high,
            MAX_BUCKETS), false);
      }
      // fixed() reduces the offset modulo the step: the buckets start at offset + k * step
      case LONG -> new Layout(CalendarBoundaries.fixed(spec.alias(), ZoneOffset.UTC, (long) interval.fixedNumber(), (long) numericOffset(),
          span.low, span.high, MAX_BUCKETS), true);
      case DOUBLE -> {
        final double step = interval.fixedNumber();
        final double offset = numericOffset();
        final double first = Math.floor((span.lowDouble - offset) / step);
        final double last = Math.floor((span.highDouble - offset) / step);
        // false for NaN and the infinities too
        if (!(last - first < MAX_BUCKETS))
          throw CalendarBoundaries.tooMany(spec.alias());
        yield new Layout(step, offset, first, (int) (last - first) + 1);
      }
    };
  }

  /** The OFFSET of a numeric FIXED histogram, 0 for none (the compiler checked it is a number). */
  private double numericOffset() {
    return spec.offset() == null ? 0d : Double.parseDouble(spec.offset().trim());
  }

  /** The grid offset of a FIXED time histogram, resolved as {@code ts.timeBucket} resolves its options (D18). */
  private long fixedOffsetMs() {
    final Map<String, Object> options = new HashMap<>(2);
    if (spec.timezone() != null)
      options.put("timezone", spec.timezone());
    if (spec.offset() != null)
      options.put("offset", spec.offset());
    try {
      return SQLFunctionTimeBucket.resolveOffset(options, spec.interval().text(), spec.interval().fixedMillis());
    } catch (final IllegalArgumentException e) {
      throw new CommandExecutionException("FACETS " + spec.alias() + ": " + e.getMessage());
    }
  }

  /**
   * Adds BOUNDS, converted by the first collector that has a column. A facet whose match set holds no bucket still gets the column of
   * the property's declaring type in the type the query selects from ({@code FacetExecutor.execute} with a target), so it converts
   * exactly as a counted facet does. Only without one (no target, or no COLUMNAR index on the property there) are the BOUNDS converted
   * by their own type: a number as is, a date or date string as epoch milliseconds. Either way MIN_COUNT 0 answers every bucket of the
   * bounds at 0.
   */
  private void addBounds(final Span span, final Grid grid) {
    if (spec.boundsFrom() == null && spec.boundsTo() == null)
      return;
    HistogramCollector converter = null;
    for (final HistogramCollector collector : all())
      if (collector.column != null) {
        converter = collector;
        break;
      }
    for (final Object bound : new Object[] { spec.boundsFrom(), spec.boundsTo() }) {
      if (bound == null)
        continue;
      if (grid == Grid.DOUBLE)
        span.add(converter != null ? FacetBounds.toColumnDouble(spec.alias(), converter.column, bound) : FacetBounds.toDouble(spec.alias(), bound));
      else if (converter != null)
        span.add(converter.gridValue(FacetBounds.toColumnLong(spec.alias(), converter.database, converter.extractor, converter.column, bound),
            grid));
      else
        span.add(grid == Grid.TIME ? FacetBounds.toEpochMs(spec.alias(), bound) : FacetBounds.toLong(spec.alias(), bound));
    }
  }

  /** Adds the views' {@code [minLong, maxLong]}; true when a view had a value. */
  private boolean addViewBounds(final Span span, final Grid grid) {
    boolean added = false;
    for (final ColumnarSegmentView view : views) {
      final long min = view.minLong(spec.property());
      final long max = view.maxLong(spec.property());
      if (min == Long.MAX_VALUE && max == Long.MIN_VALUE)
        continue;
      if (doubles) {
        span.add(Double.longBitsToDouble(min));
        span.add(Double.longBitsToDouble(max));
      } else {
        addRaw(span, min, grid);
        addRaw(span, max, grid);
      }
      added = true;
    }
    return added;
  }

  /** The precise pass: adds every matched live value of the views. */
  private void addMatchedValues(final Span span, final Grid grid) {
    for (int v = 0; v < views.size(); v++) {
      final ColumnarSegmentView view = views.get(v);
      final ColumnReader reader = view.column(spec.property());
      viewMatches.get(v).forEachAnd(view.live(), position -> {
        if (!reader.isPresent(position))
          return;
        if (doubles)
          span.add(reader.doubleValue(position));
        else
          addRaw(span, reader.longValue(position), grid);
      });
    }
  }

  private void addPatched(final Span span, final Grid grid) {
    for (int i = 0; i < patchedCount; i++)
      if (doubles)
        span.add(patchedDoubles[i]);
      else
        addRaw(span, patchedLongs[i], grid);
  }

  private void addRaw(final Span span, final long raw, final Grid grid) {
    if (grid == Grid.DOUBLE)
      span.add((double) raw);
    else
      span.add(gridValue(raw, grid));
  }

  /** A stored long on a TIME or LONG grid: epoch milliseconds for a time interval, the value itself for a number step. */
  private long gridValue(final long raw, final Grid grid) {
    return grid == Grid.TIME ? FacetBounds.toEpochMs(raw, dateUnit) : raw;
  }

  // --------------------------------------------------------------------------------------------------------------- counting

  /**
   * Counts the collector's views and patched values into {@code counts}.
   *
   * @param layout null when no matched value exists (nor BOUNDS): every matched row is then missing (or a NaN, missing too)
   */
  private void countInto(final long[] counts, final Layout layout, final Grid grid, final FacetExecutionStats into) {
    final Rows rows = new Rows(counts, layout, grid);
    final RangeCollector.Presence presence = new RangeCollector.Presence();
    for (int v = 0; v < views.size(); v++) {
      final ColumnarSegmentView view = views.get(v);
      final PositionBitmap matches = viewMatches.get(v);
      final PositionBitmap live = view.live();
      final ColumnReader reader = view.column(spec.property());
      // a DOUBLE view's bounds leave NaN out, so only a LONG view can be counted by its bounds
      if (!doubles && layout != null) {
        final long min = view.minLong(spec.property());
        final long max = view.maxLong(spec.property());
        final int bucket = min == Long.MAX_VALUE && max == Long.MIN_VALUE ? -1 : bucketOfRaw(layout, grid, min);
        if (bucket >= 0 && bucket == bucketOfRaw(layout, grid, max)) {
          presence.reader = reader;
          presence.present = 0;
          matches.forEachAnd(live, presence);
          counts[bucket] += presence.present;
          missing += matches.andCardinality(live) - presence.present;
          into.segmentSkipped();
          continue;
        }
      }
      into.segmentScanned();
      rows.reader = reader;
      matches.forEachAnd(live, rows);
    }
    for (int i = 0; i < patchedCount; i++) {
      if (doubles && Double.isNaN(patchedDoubles[i])) {
        missing++;
        continue;
      }
      final int bucket = doubles ? layout.bucketOf(patchedDoubles[i]) : bucketOfRaw(layout, grid, patchedLongs[i]);
      if (bucket >= 0)
        counts[bucket]++;
    }
  }

  private int bucketOfRaw(final Layout layout, final Grid grid, final long raw) {
    return grid == Grid.DOUBLE ? layout.bucketOf((double) raw) : layout.bucketOf(gridValue(raw, grid));
  }

  /** Locates the matched live values of a view among the buckets ({@link PositionBitmap#forEachAnd}); one per collector count. */
  private final class Rows implements LongConsumer {
    private final long[] counts;
    private final Layout layout;
    private final Grid   grid;
    private ColumnReader reader;

    private Rows(final long[] counts, final Layout layout, final Grid grid) {
      this.counts = counts;
      this.layout = layout;
      this.grid = grid;
    }

    @Override
    public void accept(final long position) {
      if (!reader.isPresent(position)) {
        missing++;
        return;
      }
      final int bucket;
      if (doubles) {
        final double value = reader.doubleValue(position);
        // a NaN belongs to no bucket: it is counted as missing, so buckets + missing = matched rows
        if (Double.isNaN(value)) {
          missing++;
          return;
        }
        bucket = layout.bucketOf(value);
      } else
        bucket = bucketOfRaw(layout, grid, reader.longValue(position));
      if (bucket >= 0)
        counts[bucket]++;
    }
  }

  // ---------------------------------------------------------------------------------------------------------------- helpers

  /** Doubles the patched-value buffer, reserving the growth first; refused, the facet is unavailable and everything is released. */
  private boolean growPatched() {
    final int capacity = Math.max(patchedLongs.length, patchedDoubles.length);
    final int grown = capacity == 0 ? FIRST_PATCH : (int) Math.min(Integer.MAX_VALUE - 8, capacity * 2L);
    final FacetMemoryBudget.Reservation granted = budget.reserve(8L * (grown - capacity), true);
    if (granted == null) {
      unavailable = true;
      release();
      return false;
    }
    stats.reserved(granted.bytes());
    if (doubles)
      patchedDoubles = Arrays.copyOf(patchedDoubles, grown);
    else
      patchedLongs = Arrays.copyOf(patchedLongs, grown);
    return true;
  }

  private FacetResult unavailableResult() {
    stats.source(FacetResult.UNAVAILABLE);
    return FacetResult.unavailable("histogram", TermsCollector.MEMORY_BUDGET);
  }

  private long missingTotal() {
    long total = 0;
    for (final HistogramCollector collector : all())
      total += collector.missing;
    return total;
  }

  private List<HistogramCollector> all() {
    final List<HistogramCollector> all = new ArrayList<>(others.size() + 1);
    all.add(this);
    all.addAll(others);
    return all;
  }

  /** The smallest and largest value to cover, as longs (TIME and LONG grids) or doubles (DOUBLE grid). */
  private static final class Span {
    private boolean empty = true;
    private long    low;
    private long    high;
    private double  lowDouble;
    private double  highDouble;

    void add(final long value) {
      if (empty) {
        low = high = value;
        empty = false;
      } else {
        low = Math.min(low, value);
        high = Math.max(high, value);
      }
    }

    void add(final double value) {
      if (Double.isNaN(value))
        return;
      if (empty) {
        lowDouble = highDouble = value;
        empty = false;
      } else {
        lowDouble = Math.min(lowDouble, value);
        highDouble = Math.max(highDouble, value);
      }
    }
  }

  /** The buckets of one facet: boundaries of a TIME or LONG grid, or a regular double grid. */
  private static final class Layout {
    private final CalendarBoundaries boundaries;
    private final boolean            numericKeys;
    private final double             step;
    private final double             offset;
    private final double             firstIndex;
    private final int                size;

    private Layout(final CalendarBoundaries boundaries, final boolean numericKeys) {
      this.boundaries = boundaries;
      this.numericKeys = numericKeys;
      this.step = 0d;
      this.offset = 0d;
      this.firstIndex = 0d;
      this.size = boundaries.size();
    }

    /** A double grid: bucket {@code i} is {@code [offset + (firstIndex + i) * step, offset + (firstIndex + i + 1) * step)}. */
    private Layout(final double step, final double offset, final double firstIndex, final int size) {
      this.boundaries = null;
      this.numericKeys = true;
      this.step = step;
      this.offset = offset;
      this.firstIndex = firstIndex;
      this.size = size;
    }

    int size() {
      return size;
    }

    int bucketOf(final long value) {
      return boundaries.bucketOf(value);
    }

    int bucketOf(final double value) {
      final double index = Math.floor((value - offset) / step) - firstIndex;
      // false for NaN
      return index >= 0 && index < size ? (int) index : -1;
    }

    Object from(final int i) {
      return boundaries != null ? (Object) boundaries.from(i) : start(i);
    }

    Object to(final int i) {
      return boundaries != null ? (Object) boundaries.to(i) : start(i + 1);
    }

    String key(final int i) {
      if (boundaries == null)
        return Double.toString(start(i));
      return numericKeys ? Long.toString(boundaries.from(i)) : boundaries.key(i);
    }

    private double start(final int i) {
      return offset + (firstIndex + i) * step;
    }
  }
}
