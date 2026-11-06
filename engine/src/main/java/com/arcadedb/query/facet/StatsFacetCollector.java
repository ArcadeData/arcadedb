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
import com.arcadedb.function.sql.math.NumericSum;
import com.arcadedb.function.sql.math.SQLFunctionAverage;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.index.columnar.ColumnReader;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.OrdinalConsumer;
import com.arcadedb.index.columnar.PositionBitmap;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.utility.DateUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;

/**
 * Computes a STATS facet for one declaring type (spec 7.7): the count, missing, min, max, sum and average of the matched live records'
 * values, as {@code count(p)}, {@code min(p)}, {@code max(p)}, {@code sum(p)} and {@code avg(p)} give them, value and Java type: min and
 * max in the type a row of the property holds (an {@code Integer} for INTEGER, a {@code Float} for FLOAT, the temporal a row of a date
 * property reads back as), sum and average as those functions build them ({@link NumericSum}, {@link SQLFunctionAverage#average}): an
 * INTEGER sum widening to {@code Long}, a {@code long} overflow to {@code BigDecimal}, a FLOAT sum in float arithmetic. Over dates the
 * sum and average are null (SQL refuses to sum dates), and so they are over no value.
 * <p>
 * A LONG or DOUBLE column visits {@code matches AND live} ({@link PositionBitmap#forEachAnd}); a segment with no matched live row is
 * skipped without decoding a value. On a LONG column, a segment whose bounds ({@link ColumnarSegmentView#minLong}, which include its dead
 * rows) lie inside the current min and max only feeds the count and the sum; a view whose column holds no value carries the sentinels
 * {@code [Long.MAX_VALUE, Long.MIN_VALUE]} and never skips, and a DOUBLE view never skips (its bounds are bit patterns, which do not
 * order negative doubles). A bucket the fallback scan reads delivers its values through {@link #collectPatched}: the column encoding of
 * its declared type (epoch counts at the declared precision for a date), or, for a property its type does not declare, the raw value.
 * <p>
 * The collectors of several declaring types (and of scanned buckets) are merged as {@code min()} and {@code max()} merge partial values
 * (numbers cast to a common class with {@link Type#castComparableNumber}, ordered by {@link BinaryComparator}), sums folded exactly
 * ({@link NumericSum#merge}). What SQL refuses makes the facet {@code unavailable}: a value that is no number and no date
 * ({@value #NOT_A_NUMBER_OR_DATE}), numbers and dates in one facet ({@value #MIXED_KINDS}).
 * <p>
 * Memory: the accumulators are reserved ({@value #STATE_BYTES} bytes, an upper bound of the sum, a widened decimal total and the
 * returned extremes) from {@link FacetMemoryBudget} at the first bucket, before they are allocated; a refusal makes the facet
 * {@code unavailable} ({@code memory budget}) and resets every accumulator. Not thread safe: driven by one query thread.
 */
public final class StatsFacetCollector implements FacetCollector {
  static final String KIND                 = "stats";
  /** The reason of a facet that met a value SQL cannot sum or order with numbers and dates (a string, a list, a map). */
  static final String NOT_A_NUMBER_OR_DATE = "not a number or a date";
  /** The reason of a facet that met numbers and dates: SQL's {@code min()} cannot compare them. */
  static final String MIXED_KINDS          = "numbers and dates mixed";
  /** The heap of the accumulators: the {@link NumericSum}, a {@code BigDecimal} total, the boxed or temporal min and max. */
  static final long   STATE_BYTES          = 256;

  // how a column value feeds the sum
  private static final byte NONE   = 0;
  private static final byte INT    = 1;
  private static final byte SHORT  = 2;
  private static final byte BYTE   = 3;
  private static final byte LONG   = 4;
  private static final byte DOUBLE = 5;
  private static final byte FLOAT  = 6;

  private final StatsFacetSpec              spec;
  private final ColumnKind                  kind;
  /** The type of the property in the column: the Java type of min and max. */
  private final Type                        propertyType;
  /** A LONG column of epoch counts (days, or the precision of a DATETIME): no sum. */
  private final boolean                     dates;
  private final byte                        adder;
  private final DatabaseInternal            db;
  private final CollectorReservations       budget;
  private final FacetExecutionStats         stats      = new FacetExecutionStats();
  private final List<StatsFacetCollector>   others     = new ArrayList<>();
  private final LongRows                    longRows   = new LongRows();
  private final DoubleRows                  doubleRows = new DoubleRows();
  private final Ordinals                    ordinals   = new Ordinals();

  /** Why the facet is unavailable, null while it is not. */
  private String     unavailable;
  private boolean    finished;
  private boolean    reserved;
  /** Allocated once {@link #STATE_BYTES} are reserved. */
  private NumericSum sum;
  private long       count;
  private long       missing;

  // the column values: their count and extremes in the column's encoding
  private long   columnCount;
  private long   minL = Long.MAX_VALUE;
  private long   maxL = Long.MIN_VALUE;
  private double minD;
  private double maxD;

  // the raw values (a property the bucket's type does not declare), as SQL folds them
  private Object  rawMin;
  private Object  rawMax;
  private boolean rawNumbers;
  private boolean rawDates;
  private boolean decimalInput;

  /**
   * @param column       the column of the property in the declaring type (or the synthetic column, of the declared type, of the values
   *                     a fallback scan reads), null for none: validation only, an empty match set, or the raw values of a property the
   *                     scanned type does not declare
   * @param matchCount   the records the facet counts in this declaring type; STATS keeps a fixed state whatever their number
   * @param db           the database the temporal min and max are read back with
   * @param budgetWaitMs the longest this collector waits for the budget
   */
  StatsFacetCollector(final StatsFacetSpec spec, final ColumnDefinition column, final long matchCount, final DatabaseInternal db,
      final long budgetWaitMs) {
    this.spec = spec;
    this.kind = column == null ? null : column.kind();
    this.propertyType = column == null ? null : column.propertyType();
    this.dates = kind == ColumnKind.LONG && propertyType.isDateOrDateTime();
    this.adder = adderOf(kind, propertyType, dates);
    this.db = db;
    this.budget = new CollectorReservations(budgetWaitMs);
  }

  private static byte adderOf(final ColumnKind kind, final Type propertyType, final boolean dates) {
    if (kind == ColumnKind.LONG && !dates)
      return switch (propertyType) {
        case INTEGER -> INT;
        case SHORT -> SHORT;
        case BYTE -> BYTE;
        default -> LONG;
      };
    if (kind == ColumnKind.DOUBLE)
      return propertyType == Type.FLOAT ? FLOAT : DOUBLE;
    return NONE;
  }

  @Override
  public void beginBucket(final int bucketId, final ColumnarSnapshot snapshot, final CrossBucketOrdinalMap map) {
    if (unavailable != null)
      return;
    if (snapshot != null && kind == null)
      throw new IllegalArgumentException("FACETS " + spec.alias() + ": a bucket counted from its projection needs the column '"
          + spec.property() + "'");
    if (reserved)
      return;
    final FacetMemoryBudget.Reservation granted = budget.reserve(STATE_BYTES, true);
    if (granted == null) {
      refuse();
      return;
    }
    stats.reserved(granted.bytes());
    reserved = true;
    sum = new NumericSum();
  }

  @Override
  public void collect(final ColumnarSegmentView segment, final PositionBitmap matches) {
    if (unavailable != null)
      return;
    final ColumnReader column = segment.column(spec.property());
    if (column == null)
      throw new IllegalStateException("FACETS " + spec.alias() + ": a segment view without the column '" + spec.property() + "'");
    final PositionBitmap live = segment.live();
    final long matchedLive = matches.andCardinality(live);
    if (matchedLive == 0) {
      stats.segmentSkipped();
      return;
    }
    stats.segmentScanned();
    if (kind == ColumnKind.LONG) {
      longRows.column = column;
      longRows.extremes = !boundsInside(segment.minLong(spec.property()), segment.maxLong(spec.property()));
      matches.forEachAnd(live, longRows);
      longRows.column = null;
    } else if (kind == ColumnKind.DOUBLE) {
      // never skipped by bounds: the bounds of a DOUBLE view are bit patterns
      doubleRows.column = column;
      matches.forEachAnd(live, doubleRows);
      doubleRows.column = null;
    } else {
      // a dictionary column (a type declaring the property as a string or a list): SQL cannot sum its values
      ordinals.seen = false;
      column.forEachOrdinal(matches, ordinals);
      if (!ordinals.seen)
        missing += matchedLive;
    }
  }

  /**
   * Whether a LONG view's bounds (dead rows included) lie inside the current extremes: then no row of it moves them. A view whose column
   * holds no value carries the sentinels {@code [Long.MAX_VALUE, Long.MIN_VALUE]}, which would pass the test: it never skips.
   */
  private boolean boundsInside(final long segmentMin, final long segmentMax) {
    return columnCount > 0 && segmentMin != Long.MAX_VALUE && segmentMin >= minL && segmentMax <= maxL;
  }

  @Override
  public void collectPatched(final long position, final PatchedValue value) {
    if (unavailable != null)
      return;
    if (value.isMissing()) {
      missing++;
      return;
    }
    if (value.ordinalCount() > 0) {
      unavailable = NOT_A_NUMBER_OR_DATE;
      return;
    }
    if (value.hasLong())
      addColumnLong(value.longValue(), true);
    else if (value.hasDouble())
      addColumnDouble(value.doubleValue());
    else if (value.unknownCount() > 0)
      for (int i = 0; i < value.unknownCount() && unavailable == null; i++)
        addRaw(value.unknown(i));
    else
      missing++;
  }

  @Override
  public void merge(final FacetCollector other) {
    if (!(other instanceof StatsFacetCollector collector))
      throw new IllegalArgumentException("FACETS " + spec.alias() + ": cannot merge a " + other.getClass().getSimpleName() + " into stats");
    others.add(collector);
    stats.add(collector.stats);
  }

  @Override
  public FacetResult finish() {
    if (finished)
      throw new IllegalStateException("FACETS " + spec.alias() + ": already finished");
    try {
      final String reason = unavailableReason();
      final FacetStats result = reason == null ? combine() : null;
      if (result != null) {
        stats.source("index");
        return FacetResult.ofStats("index", result);
      }
      stats.source(FacetResult.UNAVAILABLE);
      return FacetResult.unavailable(KIND, reason != null ? reason : MIXED_KINDS);
    } finally {
      finished = true;
      release();
      for (final StatsFacetCollector collector : others)
        collector.release();
    }
  }

  @Override
  public void release() {
    budget.release();
  }

  /** The statistics of the facet, the other declaring types merged in included. */
  @Override
  public FacetExecutionStats stats() {
    return stats;
  }

  @Override
  public int budgetWaits() {
    int waits = budget.waits();
    for (final StatsFacetCollector collector : others)
      waits += collector.budget.waits();
    return waits;
  }

  // ------------------------------------------------------------------------------------------------------------------- counting

  /** Visits the matched live positions of a LONG segment view; reused. */
  private final class LongRows implements LongConsumer {
    private ColumnReader column;
    /** False when the view's bounds lie inside the current extremes: only the count and the sum move. */
    private boolean      extremes;

    @Override
    public void accept(final long position) {
      if (!column.isPresent(position)) {
        missing++;
        return;
      }
      addColumnLong(column.longValue(position), extremes);
    }
  }

  /** Visits the matched live positions of a DOUBLE segment view; reused. */
  private final class DoubleRows implements LongConsumer {
    private ColumnReader column;

    @Override
    public void accept(final long position) {
      if (!column.isPresent(position)) {
        missing++;
        return;
      }
      addColumnDouble(column.doubleValue(position));
    }
  }

  /** The ordinals of a dictionary column: any value makes the facet unavailable, as SQL refuses to sum it. */
  private final class Ordinals implements OrdinalConsumer {
    private boolean seen;

    @Override
    public void accept(final long position, final int ordinal) {
      seen = true;
      unavailable = NOT_A_NUMBER_OR_DATE;
    }
  }

  /** A value in the encoding of the LONG column: an integral value, or an epoch count of a date column. */
  private void addColumnLong(final long value, final boolean extremes) {
    if (kind != ColumnKind.LONG) {
      addRaw(value);
      return;
    }
    count++;
    columnCount++;
    if (extremes) {
      if (value < minL)
        minL = value;
      if (value > maxL)
        maxL = value;
    }
    switch (adder) {
    case INT -> sum.addInt((int) value);
    case SHORT -> sum.addShort((short) value);
    case BYTE -> sum.addByte((byte) value);
    case LONG -> sum.addLong(value);
    default -> {
      // a date: no sum
    }
    }
  }

  /** A value of the DOUBLE column: a DOUBLE, or the widened value of a FLOAT, summed as the float it was. */
  private void addColumnDouble(final double value) {
    if (kind != ColumnKind.DOUBLE) {
      addRaw(value);
      return;
    }
    count++;
    // the order of SQL's min() and max(): Double.compareTo
    if (++columnCount == 1) {
      minD = value;
      maxD = value;
    } else {
      if (Double.compare(value, minD) < 0)
        minD = value;
      if (Double.compare(value, maxD) > 0)
        maxD = value;
    }
    if (adder == FLOAT)
      sum.addFloat((float) value);
    else
      sum.addDouble(value);
  }

  /** A raw value: a number summed as {@code sum()} adds it, a date only ordered, anything else refused as SQL refuses it. */
  private void addRaw(final Object raw) {
    if (raw == null) {
      missing++;
      return;
    }
    final boolean number = raw instanceof Number;
    if (!number && !DateUtils.isDate(raw)) {
      unavailable = NOT_A_NUMBER_OR_DATE;
      return;
    }
    try {
      rawMin = fold(rawMin, raw, true);
      rawMax = fold(rawMax, raw, false);
      if (number) {
        sum.add((Number) raw);
        decimalInput |= raw instanceof BigDecimal;
      }
    } catch (final IllegalArgumentException e) {
      // a date against a number, or a number class Type.increment() does not add
      unavailable = number && rawDates || !number && rawNumbers ? MIXED_KINDS : NOT_A_NUMBER_OR_DATE;
      return;
    }
    count++;
    if (number)
      rawNumbers = true;
    else
      rawDates = true;
  }

  /**
   * Folds {@code candidate} into the min (or max) {@code current} as {@code min()} and {@code max()} fold a row into their running value:
   * two numbers cast to a common class first ({@link Type#castComparableNumber}), the result in that class, a tie keeps the current
   * one.
   *
   * @throws IllegalArgumentException for two values SQL does not compare (a number and a date)
   */
  static Object fold(Object current, Object candidate, final boolean min) {
    if (candidate == null)
      return current;
    if (current == null)
      return candidate;
    if (current instanceof Number left && candidate instanceof Number right) {
      final Number[] cast = Type.castComparableNumber(left, right);
      current = cast[0];
      candidate = cast[1];
    } else if (current.getClass() != candidate.getClass() && !(DateUtils.isDate(current) && DateUtils.isDate(candidate)))
      throw new IllegalArgumentException("Cannot compare a " + current.getClass().getSimpleName() + " with a " + candidate.getClass()
          .getSimpleName());
    final int comparison;
    try {
      comparison = BinaryComparator.compareTo(current, candidate);
    } catch (final ClassCastException e) {
      throw new IllegalArgumentException("Cannot compare a " + current.getClass().getSimpleName() + " with a " + candidate.getClass()
          .getSimpleName(), e);
    }
    return min ? comparison > 0 ? candidate : current : comparison < 0 ? candidate : current;
  }

  /** The memory budget refused the accumulators: the facet is unavailable and every accumulator is reset. */
  private void refuse() {
    unavailable = TermsCollector.MEMORY_BUDGET;
    sum = null;
    count = 0;
    missing = 0;
    columnCount = 0;
    minL = Long.MAX_VALUE;
    maxL = Long.MIN_VALUE;
    minD = 0;
    maxD = 0;
    rawMin = null;
    rawMax = null;
    rawNumbers = false;
    rawDates = false;
    decimalInput = false;
    release();
  }

  // ------------------------------------------------------------------------------------------------------------------- merging

  private String unavailableReason() {
    if (unavailable != null)
      return unavailable;
    for (final StatsFacetCollector collector : others)
      if (collector.unavailable != null)
        return collector.unavailable;
    return null;
  }

  /** The stats of this collector and the merged ones, null when numbers met dates (two extremes SQL cannot compare). */
  private FacetStats combine() {
    long totalCount = count;
    long totalMissing = missing;
    Object min;
    Object max;
    boolean anyDates = hasDates();
    boolean anyNumbers = hasNumbers();
    boolean decimal = decimalInput;
    NumericSum total = sum;
    try {
      min = ownExtreme(true);
      max = ownExtreme(false);
      for (final StatsFacetCollector other : others) {
        min = fold(min, other.ownExtreme(true), true);
        max = fold(max, other.ownExtreme(false), false);
      }
    } catch (final IllegalArgumentException e) {
      return null;
    }
    for (final StatsFacetCollector other : others) {
      totalCount += other.count;
      totalMissing += other.missing;
      anyDates |= other.hasDates();
      anyNumbers |= other.hasNumbers();
      decimal |= other.decimalInput;
      if (other.sum != null) {
        if (total == null)
          total = other.sum;
        else
          total.merge(other.sum);
      }
    }
    if (anyDates && anyNumbers)
      return null;
    if (reserved)
      stats.counterBytes(STATE_BYTES);
    for (final StatsFacetCollector other : others)
      if (other.reserved)
        stats.counterBytes(STATE_BYTES);
    final boolean summed = !anyDates && total != null && totalCount > 0;
    return new FacetStats(totalCount, totalMissing, min, max, summed ? total.get() : null,
        summed ? SQLFunctionAverage.average(total, totalCount, decimal) : null);
  }

  private boolean hasDates() {
    return dates && columnCount > 0 || rawDates;
  }

  private boolean hasNumbers() {
    return !dates && columnCount > 0 || rawNumbers;
  }

  /** The min (or max) of this collector alone, in the Java type a row of the property holds. */
  private Object ownExtreme(final boolean min) {
    Object columnValue = null;
    if (columnCount > 0) {
      if (kind == ColumnKind.LONG) {
        final long value = min ? minL : maxL;
        columnValue = dates ? db.getSerializer().deserializeTemporal(db, value, propertyType.getBinaryType()) : integral(value);
      } else {
        final double value = min ? minD : maxD;
        columnValue = propertyType == Type.FLOAT ? (Object) (float) value : (Object) value;
      }
    }
    return fold(columnValue, min ? rawMin : rawMax, min);
  }

  /** An integral column value as the property's Java type. */
  private Object integral(final long value) {
    return switch (propertyType) {
      case INTEGER -> (int) value;
      case SHORT -> (short) value;
      case BYTE -> (byte) value;
      default -> value;
    };
  }
}
