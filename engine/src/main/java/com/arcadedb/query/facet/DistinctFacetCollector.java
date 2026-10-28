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
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.index.columnar.ColumnReader;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.ColumnarValueExtractor;
import com.arcadedb.index.columnar.OrdinalConsumer;
import com.arcadedb.index.columnar.PositionBitmap;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.HyperLogLog;
import com.arcadedb.utility.LongHashSet;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

/**
 * Counts a DISTINCT facet for one declaring type (spec 7.7): the number of distinct values the matched live records hold, as
 * {@code count(DISTINCT p)} counts them: values keyed by {@link Type#normalizeForKey}, a missing value or a null is no value, a list
 * counts each distinct item.
 * <p>
 * <b>Dictionary columns</b> (ORDINAL, ORDINAL_LIST) are always exact: a bitset over the merged ordinals of the type's
 * {@link CrossBucketOrdinalMap}, sized from {@link CrossBucketOrdinalMap#size()} at the first bucket, so a value held by several buckets
 * counts once; segment ordinals come through {@link ColumnReader#forEachOrdinal} (live rows only). A value with no merged ordinal of the
 * query's map (a patched record's new value, every value of a bucket the fallback scan reads) is looked up by value
 * ({@link CrossBucketOrdinalMap#mergedOf}) and, absent, kept in a value set under its key.
 * <p>
 * <b>Numbers and dates</b> (LONG and DOUBLE columns, the numbers and temporals of a scanned bucket) visit {@code matches AND live} and
 * are exact by default, in {@link LongHashSet}s of four key spaces that never hold equal values, as {@link Type#normalizeForKey} keys
 * them:
 * <ul>
 *   <li>integral numbers, by their long (an integral double up to 2^53 included, and a larger one or a DECIMAL whose key is a long);</li>
 *   <li>other numbers a double holds, by the double's bits (a DECIMAL equal to the shortest decimal form of a double is that double; any
 *   other DECIMAL, such as 0.10000000000000000001, is kept in the value set as its stripped value);</li>
 *   <li>DATE values, by their epoch day;</li>
 *   <li>DATETIME values of every precision (and the instants a scanned bucket reads raw), by their epoch count in the finest unit the
 *   facet met, so one instant at two precisions is one value.</li>
 * </ul>
 * Only with {@code DISTINCT PRECISION n} do they switch to one {@link HyperLogLog} (each key space hashed with its own salt, instants at
 * nanoseconds), when they hold more than {@code n} values or when the budget refuses them; the facet is then not exact.
 * <p>
 * <b>Memory.</b> The bitset, the value set (charged per key in steps of {@link TermsCollector#FOREIGN_STEP_BYTES}), every numeric
 * set's table and the HyperLogLog ({@value #HLL_BYTES} bytes) are reserved from {@link FacetMemoryBudget} before they are allocated,
 * through the collector's own {@link CollectorReservations}. A set's table is mirrored: before the add that doubles it, the doubled
 * table is reserved while the old one stays reserved, and the old one is given back after the copy, so the charge covers the heap at
 * every moment. A refusal makes the facet {@code unavailable} ({@code memory budget}) and releases everything, except a refused set
 * table with PRECISION, which switches to the HyperLogLog (the sets' tables are then given back): without PRECISION a count is never
 * approximate.
 * <p>
 * The collectors of several declaring types (and of scanned buckets) are merged by value into this one: bitsets are ORed when both
 * collectors count with the same map ({@link CrossBucketOrdinalMap#version()}), else the other's ordinals are folded in through its own
 * map's values; sets are united key space by key space, instants rescaled to the finest unit of any collector, sketches merged. Not
 * thread safe: driven by one query thread.
 */
public final class DistinctFacetCollector implements FacetCollector, OrdinalConsumer {
  static final         String   KIND                     = "distinct";
  /** The heap of the HyperLogLog: its registers, the array and object headers. */
  static final         long     HLL_BYTES                = (1L << HyperLogLog.PRECISION) + 32;
  /** The table a numeric set starts with at most: a small facet does not reserve for its matches, a large one grows by doubling. */
  private static final int      MAX_INITIAL_CAPACITY     = 1024;
  /** 2^53: up to it an integral double is keyed as its long ({@link Type#normalizeNumberForKey}). */
  private static final long     MAX_EXACT_DOUBLE_INTEGER = 1L << 53;
  private static final long     NANOS_PER_SECOND         = 1_000_000_000L;

  // the key spaces of the numeric sets
  private static final int      NUMBERS                  = 0;
  private static final int      DOUBLES                  = 1;
  private static final int      DATES                    = 2;
  private static final int      INSTANTS                 = 3;
  private static final int      SPACES                   = 4;
  /** XORed into the keys of each space before they are hashed into the sketch, so equal longs of two spaces are two values. */
  private static final long[]   SALTS                    = { 0L, 0x9E3779B97F4A7C15L, 0xC2B2AE3D27D4EB4FL, 0x165667B19E3779F9L };

  private final DistinctFacetSpec                   spec;
  private final ColumnKind                          kind;
  /** The key space of the longs of a LONG column: integral numbers, dates or instants. */
  private final int                                 columnSpace;
  /** The unit of the epoch counts of a DATETIME column, null for any other column. */
  private final TimeUnit                            columnUnit;
  private final int                                 initialCapacity;
  private final String                              declaringType;
  private final CollectorReservations               budget;
  private final FacetExecutionStats                 stats       = new FacetExecutionStats();
  private final List<DistinctFacetCollector>        others      = new ArrayList<>();
  private final NumericRows                         numericRows = new NumericRows();
  private final KeySet[]                            sets        = new KeySet[SPACES];

  /** Why the facet is unavailable, null while it is not. */
  private String  unavailable;
  private boolean finished;
  private long    missing;

  // by merged ordinal
  private CrossBucketOrdinalMap map;
  private long[]                bits;

  // by value: the values without a merged ordinal or a numeric key, charged in steps of TermsCollector.FOREIGN_STEP_BYTES
  private HashSet<Object> values;
  private long            valueBytes;
  private long            valueReserved;

  /** The unit of the epoch counts of the instants key space, null before the first instant. */
  private TimeUnit    instantUnit;
  private HyperLogLog sketch;

  // the bucket being counted
  private ColumnarSnapshot snapshot;
  private int[]            toMerged;
  // the segment being counted: positions arrive in ascending order, so a change of position is a new document
  private long             lastPosition;
  private long             positions;

  /**
   * @param column        the column of the property in the declaring type (or the synthetic column of the raw numbers or dates a
   *                      fallback scan reads), null when no projected bucket is counted (validation only, an empty match set, or the
   *                      raw values of a fallback scan)
   * @param matchCount    the records the facet counts in this declaring type, to size the first table of the numeric sets
   * @param db            the database of the counted buckets; DISTINCT reads nothing from it (the shape of every plan 5 collector)
   * @param declaringType the type declaring the COLUMNAR index of the property, named by errors; null when unknown
   * @param budgetWaitMs  the longest this collector waits for the budget, all its reservations together (one deadline)
   */
  DistinctFacetCollector(final DistinctFacetSpec spec, final ColumnDefinition column, final long matchCount, final DatabaseInternal db,
      final String declaringType, final long budgetWaitMs) {
    this.spec = spec;
    this.kind = column == null ? null : column.kind();
    final TimeUnit dateUnit = kind == ColumnKind.LONG ? ColumnarValueExtractor.dateUnit(column.propertyType()) : null;
    this.columnSpace = dateUnit == null ? NUMBERS : dateUnit == TimeUnit.DAYS ? DATES : INSTANTS;
    this.columnUnit = columnSpace == INSTANTS ? dateUnit : null;
    this.instantUnit = columnUnit;
    this.initialCapacity = tableCapacity((int) Math.max(16L, Math.min(MAX_INITIAL_CAPACITY, matchCount)));
    this.declaringType = declaringType;
    this.budget = new CollectorReservations(budgetWaitMs);
  }

  @Override
  public void beginBucket(final int bucketId, final ColumnarSnapshot snapshot, final CrossBucketOrdinalMap map) {
    if (unavailable != null)
      return;
    this.snapshot = snapshot;
    toMerged = null;
    // a bucket the fallback scan reads: every value arrives by value through collectPatched
    if (snapshot == null)
      return;
    if (kind == null)
      throw new IllegalArgumentException("FACETS " + spec.alias() + ": a bucket of " + declaringType + " counted from its projection needs the column '"
          + spec.property() + "'");
    if (!isDictionaryKind())
      return;
    if (map == null)
      throw new IllegalArgumentException("FACETS " + spec.alias() + ": the dictionary column '" + spec.property() + "' needs an ordinal map");
    if (this.map == null) {
      final int words = words(map.size());
      final FacetMemoryBudget.Reservation granted = budget.reserve(16L + (long) Long.BYTES * words, true);
      if (granted == null) {
        refuse();
        return;
      }
      stats.reserved(granted.bytes());
      this.map = map;
      bits = new long[words];
    } else if (this.map != map)
      throw new IllegalArgumentException("FACETS " + spec.alias() + ": every bucket of a declaring type is counted with the same ordinal map");
    toMerged = map.bucketArray(bucketId);
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
    if (isDictionaryKind()) {
      lastPosition = -1;
      positions = 0;
      // live rows only: the matches are not ANDed with live by the caller
      column.forEachOrdinal(matches, this);
      missing += matchedLive - positions;
      return;
    }
    numericRows.column = column;
    numericRows.doubles = kind == ColumnKind.DOUBLE;
    matches.forEachAnd(live, numericRows);
    numericRows.column = null;
  }

  /** {@link OrdinalConsumer}: one ordinal of a matched live row, ascending positions. */
  @Override
  public void accept(final long position, final int ordinal) {
    if (unavailable != null)
      return;
    if (position != lastPosition) {
      lastPosition = position;
      positions++;
    }
    setBit(toMerged == null ? ordinal : toMerged[ordinal]);
  }

  @Override
  public void collectPatched(final long position, final PatchedValue value) {
    if (unavailable != null)
      return;
    if (value.isMissing()) {
      missing++;
      return;
    }
    for (int i = 0; i < value.ordinalCount() && unavailable == null; i++) {
      // only a projected bucket delivers ordinals (a scanned one delivers values): its snapshot is set
      final int ordinal = value.ordinalAt(i);
      final int merged = toMerged == null ? ordinal : ordinal < toMerged.length ? toMerged[ordinal] : -1;
      if (map != null && merged >= 0 && merged < map.size())
        setBit(merged);
      else
        // a value added to the bucket's dictionary after the map was taken
        addValue(snapshot.dictionary(spec.property()).valueOf(ordinal));
    }
    if (value.hasLong())
      addColumnLong(value.longValue());
    else if (value.hasDouble())
      addDouble(value.doubleValue());
    for (int i = 0; i < value.unknownCount() && unavailable == null; i++)
      addValue(value.unknown(i));
  }

  @Override
  public void merge(final FacetCollector other) {
    if (!(other instanceof DistinctFacetCollector collector))
      throw new IllegalArgumentException("FACETS " + spec.alias() + ": cannot merge a " + other.getClass().getSimpleName() + " into distinct");
    others.add(collector);
    stats.add(collector.stats);
  }

  @Override
  public FacetResult finish() {
    if (finished)
      throw new IllegalStateException("FACETS " + spec.alias() + ": already finished");
    try {
      String reason = unavailableReason();
      if (reason == null) {
        long missingTotal = missing;
        stats.counterBytes(counterBytes());
        for (final DistinctFacetCollector collector : others) {
          stats.counterBytes(collector.counterBytes());
          missingTotal += collector.missing;
        }
        // instants meet in the finest unit any collector counts in, whatever their order: a finer one rescales those folded before
        for (final DistinctFacetCollector collector : others)
          if (unavailable == null)
            absorb(collector);
        if (map != null && unavailable == null && (exactSize() > 0 || sketch != null || values != null))
          moveKeyedOrdinals();
        reason = unavailable;
        if (reason == null) {
          stats.source("index");
          return FacetResult.ofDistinct("index", new DistinctValue(count(), sketch == null), missingTotal);
        }
      }
      stats.source(FacetResult.UNAVAILABLE);
      return FacetResult.unavailable(KIND, reason);
    } finally {
      finished = true;
      release();
      for (final DistinctFacetCollector collector : others)
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
    for (final DistinctFacetCollector collector : others)
      waits += collector.budget.waits();
    return waits;
  }

  // ------------------------------------------------------------------------------------------------------------------- counting

  /** Visits the matched live positions of a LONG or DOUBLE segment view ({@link PositionBitmap#forEachAnd}); reused. */
  private final class NumericRows implements LongConsumer {
    private ColumnReader column;
    private boolean      doubles;

    @Override
    public void accept(final long position) {
      if (unavailable != null)
        return;
      if (!column.isPresent(position))
        missing++;
      else if (doubles)
        addDouble(column.doubleValue(position));
      else
        addColumnLong(column.longValue(position));
    }
  }

  private boolean isDictionaryKind() {
    return kind == ColumnKind.ORDINAL || kind == ColumnKind.ORDINAL_LIST;
  }

  private static int words(final int size) {
    return (size + 63) >>> 6;
  }

  private void setBit(final int merged) {
    bits[merged >>> 6] |= 1L << merged;
  }

  /** A long of the LONG column (or of the synthetic column of a scan): an integral number, an epoch day or an epoch count. */
  private void addColumnLong(final long value) {
    if (columnSpace == INSTANTS)
      addInstant(value, columnUnit);
    else
      addKey(columnSpace, value);
  }

  /**
   * A value that has no merged ordinal in hand: its merged ordinal when the map holds it, a number or a temporal into its key space,
   * anything else into the value set by its key (charged before it grows).
   */
  private void addValue(final Object raw) {
    if (unavailable != null)
      return;
    if (map != null) {
      final int merged = map.mergedOf(raw);
      if (merged >= 0 && merged < map.size()) {
        setBit(merged);
        return;
      }
    }
    addUnmapped(raw);
  }

  /** A value counted outside the ordinal map: a number or a temporal into its key space, anything else into the value set. */
  private void addUnmapped(final Object raw) {
    if (raw instanceof Number number) {
      addNumber(number);
      return;
    }
    if (raw instanceof LocalDate date) {
      addKey(DATES, date.toEpochDay());
      return;
    }
    if (isInstant(raw)) {
      addRawInstant(toInstant(raw));
      return;
    }
    addValueKey(Type.normalizeForKey(raw));
  }

  /** Whether a dictionary value has a numeric or temporal key space, where another collector may count it as a key. */
  private static boolean hasKeySpace(final Object value) {
    return value instanceof Number || value instanceof LocalDate || isInstant(value);
  }

  /**
   * Moves the merged ordinals whose values are numbers or temporals (a numeric list indexed BY ITEM) into their key spaces, so a
   * value another collector counted as a key is one value. Only needed when there are keys (or a value set or sketch) to meet.
   */
  private void moveKeyedOrdinals() {
    for (int word = 0; word < bits.length && unavailable == null; word++)
      for (long remaining = bits[word]; remaining != 0 && unavailable == null; remaining &= remaining - 1) {
        final int merged = (word << 6) + Long.numberOfTrailingZeros(remaining);
        if (merged >= map.size())
          break;
        final Object value = map.value(merged);
        if (hasKeySpace(value)) {
          bits[word] &= ~(1L << merged);
          addUnmapped(value);
        }
      }
  }

  /**
   * A raw instant: at nanoseconds when a long holds them, else (after 2262) in the instants' current unit, or the finest unit that
   * holds it exactly; kept by its key only when none does.
   */
  private void addRawInstant(final Instant instant) {
    try {
      addInstant(epochIn(instant, TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
      return;
    } catch (final ArithmeticException e) {
      // beyond the nanosecond range of a long
    }
    if (instantUnit != null)
      try {
        addInstant(epochIn(instant, instantUnit), instantUnit);
        return;
      } catch (final ArithmeticException e) {
        // not exact in the current unit
      }
    for (final TimeUnit unit : new TimeUnit[] { TimeUnit.MICROSECONDS, TimeUnit.MILLISECONDS, TimeUnit.SECONDS })
      try {
        addInstant(epochIn(instant, unit), unit);
        return;
      } catch (final ArithmeticException e) {
        // not exact in this unit either
      }
    addValueKey(instant.atZone(ZoneOffset.UTC));
  }

  private void addValueKey(final Object key) {
    if (unavailable != null)
      return;
    if (values == null)
      values = new HashSet<>();
    else if (values.contains(key))
      return;
    valueBytes += TermsCollector.foreignEntryBytes(key);
    if (valueBytes > valueReserved) {
      final FacetMemoryBudget.Reservation more = budget.reserve(Math.max(TermsCollector.FOREIGN_STEP_BYTES, valueBytes - valueReserved), true);
      if (more == null) {
        refuse();
        return;
      }
      valueReserved += more.bytes();
      stats.reserved(more.bytes());
    }
    values.add(key);
  }

  /** A double as {@link Type#normalizeNumberForKey} keys it, without boxing below 2^53: an integral value there is its long. */
  private void addDouble(final double value) {
    if (Double.isFinite(value) && value == Math.rint(value)) {
      if (Math.abs(value) <= MAX_EXACT_DOUBLE_INTEGER)
        addKey(NUMBERS, (long) value);
      else
        // keyed by its shortest decimal form, a long when that is one
        addNumber(value);
    } else
      addKey(DOUBLES, Double.doubleToLongBits(value));
  }

  /** A number as {@link Type#normalizeNumberForKey} keys it: a long, a double's bits, or (a DECIMAL no double holds) the value set. */
  private void addNumber(final Number raw) {
    final Object key = Type.normalizeNumberForKey(raw);
    if (key instanceof Long integral)
      addKey(NUMBERS, integral);
    else if (key instanceof BigDecimal decimal) {
      // the key of a non-integral double is its shortest decimal form, stripped: a DECIMAL equal to it is that double
      final double asDouble = decimal.doubleValue();
      if (Double.isFinite(asDouble) && BigDecimal.valueOf(asDouble).stripTrailingZeros().equals(decimal))
        addKey(DOUBLES, Double.doubleToLongBits(asDouble));
      else
        addValueKey(decimal);
    } else if (key instanceof Double || key instanceof Float)
      // NaN and the infinities
      addKey(DOUBLES, Double.doubleToLongBits(((Number) key).doubleValue()));
    else
      addValueKey(key);
  }

  /** The temporals {@link Type#normalizeForKey} keys as a UTC instant. */
  private static boolean isInstant(final Object value) {
    return value instanceof LocalDateTime || value instanceof Date || value instanceof Calendar || value instanceof Instant
        || value instanceof ZonedDateTime zoned && ZoneOffset.UTC.equals(zoned.getZone());
  }

  private static Instant toInstant(final Object value) {
    return switch (value) {
      case LocalDateTime local -> local.toInstant(ZoneOffset.UTC);
      case Date date -> Instant.ofEpochMilli(date.getTime());
      case Calendar calendar -> Instant.ofEpochMilli(calendar.getTimeInMillis());
      case ZonedDateTime zoned -> zoned.toInstant();
      default -> (Instant) value;
    };
  }

  /** @throws ArithmeticException beyond the range of a long in {@code unit}, or when the instant is not a whole count of it */
  private static long epochIn(final Instant instant, final TimeUnit unit) {
    final long perSecond = unit.convert(1, TimeUnit.SECONDS);
    final long nanosPerUnit = NANOS_PER_SECOND / perSecond;
    if (instant.getNano() % nanosPerUnit != 0)
      throw new ArithmeticException("not a whole count of " + unit);
    return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), perSecond), instant.getNano() / nanosPerUnit);
  }

  /** The key of an instant kept by value: the UTC instant {@link Type#normalizeForKey} keys a stored DATETIME as. */
  private static Object instantKey(final long epoch, final TimeUnit unit) {
    final long perSecond = unit.convert(1, TimeUnit.SECONDS);
    final long nanos = Math.floorMod(epoch, perSecond) * (NANOS_PER_SECOND / perSecond);
    return Instant.ofEpochSecond(Math.floorDiv(epoch, perSecond), nanos).atZone(ZoneOffset.UTC);
  }

  /**
   * An instant counted {@code epoch} in {@code unit}: into the instants key space in its unit, a coarser count multiplied exactly into it
   * (beyond the range of a long: kept by value), a finer one moving the key space to that unit first.
   */
  private void addInstant(final long epoch, final TimeUnit unit) {
    if (instantUnit == null)
      instantUnit = unit;
    if (unit.compareTo(instantUnit) < 0)
      useInstantUnit(unit);
    if (unit == instantUnit) {
      addKey(INSTANTS, epoch);
      return;
    }
    try {
      addKey(INSTANTS, Math.multiplyExact(epoch, instantUnit.convert(1, unit)));
    } catch (final ArithmeticException e) {
      addValueKey(instantKey(epoch, unit));
    }
  }

  /**
   * Moves the instants key space to {@code unit}, finer than the current one, rescaling the instants already counted (the new set is
   * charged while the old one is still held). The sketch hashes instants at nanoseconds, so it is not touched.
   */
  private void useInstantUnit(final TimeUnit unit) {
    final KeySet coarser = sets[INSTANTS];
    final TimeUnit from = instantUnit;
    sets[INSTANTS] = null;
    instantUnit = unit;
    if (coarser != null) {
      coarser.forEach(epoch -> addInstant(epoch, from));
      coarser.drop();
    }
  }

  /** Counts a key of a key space: in its exact set, or in the sketch once there is one. */
  private void addKey(final int space, final long key) {
    if (unavailable != null)
      return;
    if (sketch != null) {
      sketch.addLong(hash(space, key, instantUnit));
      return;
    }
    KeySet keys = sets[space];
    if (keys == null)
      keys = sets[space] = new KeySet();
    if (!keys.add(key)) {
      // the budget refused the set's table: approximate only when PRECISION asked for it
      if (!hasPrecision()) {
        refuse();
        return;
      }
      switchToSketch();
      if (sketch != null)
        sketch.addLong(hash(space, key, instantUnit));
      return;
    }
    if (hasPrecision() && exactSize() > spec.precisionThreshold())
      switchToSketch();
  }

  /** The value a key is hashed into the sketch as: salted by its key space, an instant at nanoseconds whatever its unit. */
  private static long hash(final int space, final long key, final TimeUnit unit) {
    return (space == INSTANTS ? TimeUnit.NANOSECONDS.convert(key, unit) : key) ^ SALTS[space];
  }

  private long exactSize() {
    long size = 0;
    for (final KeySet keys : sets)
      if (keys != null)
        size += keys.size();
    return size;
  }

  private boolean hasPrecision() {
    return spec.precisionThreshold() != Long.MAX_VALUE;
  }

  /** Pours the numeric sets into a HyperLogLog, reserved before it is allocated, and gives the sets' tables back. */
  private void switchToSketch() {
    final FacetMemoryBudget.Reservation granted = budget.reserve(HLL_BYTES, true);
    if (granted == null) {
      refuse();
      return;
    }
    stats.reserved(granted.bytes());
    final HyperLogLog poured = new HyperLogLog();
    final TimeUnit unit = instantUnit;
    for (int space = 0; space < SPACES; space++)
      if (sets[space] != null) {
        final int pouring = space;
        sets[space].forEach(key -> poured.addLong(hash(pouring, key, unit)));
      }
    sketch = poured;
    dropSets();
  }

  private void dropSets() {
    for (int space = 0; space < SPACES; space++)
      if (sets[space] != null) {
        sets[space].drop();
        sets[space] = null;
      }
  }

  /** The memory budget refused a reservation: the facet is unavailable and everything is given back now. */
  private void refuse() {
    unavailable = TermsCollector.MEMORY_BUDGET;
    bits = null;
    values = null;
    sketch = null;
    dropSets();
    release();
  }

  /** The table capacity {@link LongHashSet} gives a set presized for {@code entries}: a power of two, 16 at least. */
  private static int tableCapacity(final int entries) {
    return Math.max(16, Integer.highestOneBit(Math.max(1, entries - 1)) << 1);
  }

  /** The heap of a set whose table has {@code capacity} slots: the table and its header, the set object. */
  private static long tableBytes(final long capacity) {
    return 48L + Long.BYTES * capacity;
  }

  /**
   * One key space's {@link LongHashSet} with its table charged: the set's capacity is mirrored (it doubles when an add brings its size
   * to 3/4 of the capacity), the doubled table is reserved before that add while the old one stays reserved, and the old one is given
   * back after it.
   */
  private final class KeySet {
    private LongHashSet                   set;
    private int                           capacity;
    /** {@link Long#MIN_VALUE} was counted: the set's empty slot, which it cannot hold. */
    private boolean                       minValue;
    private FacetMemoryBudget.Reservation table;

    /** @return false when the budget refused the table the key needs */
    private boolean add(final long key) {
      if (key == Long.MIN_VALUE) {
        minValue = true;
        return true;
      }
      if (set == null) {
        final FacetMemoryBudget.Reservation first = budget.reserve(tableBytes(initialCapacity), true);
        if (first == null)
          return false;
        stats.reserved(first.bytes());
        table = first;
        capacity = initialCapacity;
        set = new LongHashSet(capacity);
      } else if (set.size() + 1 >= (int) (capacity * 0.75f) && !set.contains(key)) {
        // this add doubles the table: both live while it copies
        final FacetMemoryBudget.Reservation doubled = budget.reserve(tableBytes(2L * capacity), true);
        if (doubled == null)
          return false;
        stats.reserved(doubled.bytes());
        set.add(key);
        budget.release(table);
        table = doubled;
        capacity *= 2;
        return true;
      }
      set.add(key);
      return true;
    }

    private long size() {
      return (set == null ? 0 : set.size()) + (minValue ? 1 : 0);
    }

    private long bytes() {
      return set == null ? 0 : tableBytes(capacity);
    }

    private void forEach(final LongHashSet.LongConsumer consumer) {
      if (set != null)
        set.forEach(consumer);
      if (minValue)
        consumer.accept(Long.MIN_VALUE);
    }

    private void drop() {
      set = null;
      minValue = false;
      if (table != null)
        budget.release(table);
      table = null;
    }
  }

  // ------------------------------------------------------------------------------------------------------------------- merging

  private String unavailableReason() {
    if (unavailable != null)
      return unavailable;
    for (final DistinctFacetCollector collector : others)
      if (collector.unavailable != null)
        return collector.unavailable;
    return null;
  }

  /** Folds the values of another collector of the facet into this one, charged to this one's budget. */
  private void absorb(final DistinctFacetCollector other) {
    if (other.bits != null) {
      if (map != null && other.map.version() == map.version())
        orBits(other.bits);
      else
        for (int word = 0; word < other.bits.length && unavailable == null; word++)
          for (long remaining = other.bits[word]; remaining != 0 && unavailable == null; remaining &= remaining - 1)
            addValue(other.map.value((word << 6) + Long.numberOfTrailingZeros(remaining)));
    }
    if (other.values != null)
      for (final Object value : other.values)
        addValue(value);
    if (other.sketch != null) {
      if (sketch == null)
        switchToSketch();
      if (unavailable != null)
        return;
      sketch.merge(other.sketch);
    }
    for (int space = 0; space < SPACES; space++) {
      final KeySet keys = other.sets[space];
      if (keys == null)
        continue;
      final int folded = space;
      if (space == INSTANTS)
        keys.forEach(epoch -> addInstant(epoch, other.instantUnit));
      else
        keys.forEach(key -> addKey(folded, key));
    }
  }

  /** ORs the bitset of a collector counting with the same map (same version): the merged ordinals mean the same values. */
  private void orBits(final long[] other) {
    if (other.length > bits.length) {
      final FacetMemoryBudget.Reservation more = budget.reserve((long) Long.BYTES * (other.length - bits.length), true);
      if (more == null) {
        refuse();
        return;
      }
      stats.reserved(more.bytes());
      bits = Arrays.copyOf(bits, other.length);
    }
    for (int i = 0; i < other.length; i++)
      bits[i] |= other[i];
  }

  private long count() {
    long count = sketch != null ? sketch.estimate() : exactSize();
    if (bits != null)
      for (final long word : bits)
        count += Long.bitCount(word);
    return count + (values == null ? 0 : values.size());
  }

  /** The heap the counters held when the facet was finished. */
  private long counterBytes() {
    long bytes = (bits == null ? 0 : (long) Long.BYTES * bits.length) + valueBytes + (sketch == null ? 0 : HLL_BYTES);
    for (final KeySet keys : sets)
      if (keys != null)
        bytes += keys.bytes();
    return bytes;
  }
}
