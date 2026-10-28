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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.function.sql.math.NumericSum;
import com.arcadedb.function.sql.math.SQLFunctionAverage;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.index.columnar.ColumnReader;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.PositionBitmap;
import com.arcadedb.query.facet.CrossBucketOrdinalMap;
import com.arcadedb.query.facet.FacetMemoryBudget;
import com.arcadedb.query.sql.executor.ColumnarGroupByPlanner.AggregateSpec;
import com.arcadedb.query.sql.executor.ColumnarGroupByPlanner.ColumnarGroupByPlan;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.IntHashSet;
import com.arcadedb.utility.LongLongHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The groups of one {@link ColumnarGroupByStep} execution and their aggregates, fed row by row from the columns of the projection
 * ({@link #countSegment}) or from records ({@link #countRecord}), then turned into the rows the normal aggregation chain returns
 * ({@link #emit}): the projection's aliases, the values in the Java types {@code count()}, {@code min()}, {@code max()},
 * {@code sum()} and {@code avg()} give them, the groups in the order a scan of the buckets (ascending bucket id, then position) first
 * meets them, and only the groups that hold a row.
 * <p>
 * A STRING key groups by merged ordinal ({@link CrossBucketOrdinalMap}) in dense arrays, the null group last, plus a group per value a
 * record holds and no dictionary does; an integral key groups through a primitive hash map. Missing and null keys are one null group,
 * as the normal plan's GROUP BY forms them. Every array, map and per-group object is charged to {@link FacetMemoryBudget} before it is
 * allocated, as an upper bound, never waiting: a refusal, or a record whose value is not of the declared type (written before the
 * property was typed), ends the execution with a {@link Decline} and the step runs the normal chain.
 * <p>
 * Not thread safe: one query thread drives it.
 */
final class ColumnarGroupByAggregator implements AutoCloseable {
  static final String BUDGET = "query memory budget not granted";

  /** Positions are below 2^40: the bucket's rank in the scan goes above them in the first-seen order. */
  private static final int  RANK_SHIFT              = 40;
  private static final long NOT_SEEN                = Long.MAX_VALUE;
  /** An open-addressing table entry with its share of the empty slots and of the copy a resize makes. */
  private static final long HASH_ENTRY_BYTES        = 64;
  private static final long OBJECT_ENTRY_BYTES      = 96;
  private static final long SUM_BYTES               = 48;
  /** An IntHashSet of 16 slots; then 16 bytes per element bound its table, the old and the new one during a resize. */
  private static final long INT_SET_BYTES           = 128;
  private static final long INT_SET_ELEMENT_BYTES   = 16;
  private static final int  INITIAL_LONG_KEY_GROUPS = 64;

  /** The projection cannot answer this execution; the message is the reason the step reports. */
  static final class Decline extends RuntimeException {
    Decline(final String reason) {
      super(reason, null, false, false);
    }
  }

  /** {@link FacetMemoryBudget} reservations taken in steps, never waiting, released together. */
  static final class Charges {
    private static final long                         STEP_BYTES   = 64 * 1024;
    private final        List<FacetMemoryBudget.Reservation> reservations = new ArrayList<>();
    private              long                         charged;
    private              long                         reserved;

    /** @throws Decline when the budget refuses */
    void charge(final long bytes) {
      charged += bytes;
      while (charged > reserved) {
        final FacetMemoryBudget.Reservation reservation = FacetMemoryBudget.instance()
            .reserve(Math.max(STEP_BYTES, charged - reserved), 0);
        if (reservation == null)
          throw new Decline(BUDGET);
        reservations.add(reservation);
        reserved += reservation.bytes();
      }
    }

    void release() {
      for (final FacetMemoryBudget.Reservation reservation : reservations)
        reservation.close();
      reservations.clear();
      charged = 0;
      reserved = 0;
    }
  }

  private final DatabaseInternal        db;
  private final ColumnarGroupByPlan     plan;
  private final AggregateSpec[]         aggregates;
  private final boolean                 ordinalKey;
  private final Class<?>                keyClass;
  private final CrossBucketOrdinalMap   keyMap;
  /** By projection item: the merged ordinals of a {@code count(DISTINCT q)}, null for the other items. */
  private final CrossBucketOrdinalMap[] distinctMaps;
  private final Charges                 charges  = new Charges();
  private final long                    groupBytes;
  private final WorkGuard               guard;
  private       int                     ticks;

  private int    size;
  private int    capacity;
  private long[] rows;
  private long[] firstSeen;
  private int    nullGroup     = -1;
  /** STRING key: the groups of the merged ordinals and the null group, allocated up front; 0 for an integral key. */
  private int    denseGroups;
  /** LONG key: the group of {@code Long.MIN_VALUE}, the empty marker of the hash map. */
  private int    minValueGroup = -1;

  private LongLongHashMap          longGroups;
  private long[]                   longKeys;
  /** STRING key: the groups of values no dictionary holds, by value. */
  private HashMap<Object, Integer> objectGroups;
  private Object[]                 objectKeys;

  /** By projection item, per group: the values counted (count(q), and the presence of min, max, sum and avg). */
  private final long[][]       present;
  private final long[][]       longValues;
  private final double[][]     doubleValues;
  private final NumericSum[][] sums;
  private final IntHashSet[][] distinct;
  private final Object[][]     distinctObjects;

  private long segmentsCounted;
  private long recordsCounted;

  /**
   * @param keyMap       the merged ordinals of the key, null for an integral key
   * @param distinctMaps by projection item, the merged ordinals of a {@code count(DISTINCT q)}
   *
   * @throws Decline when the budget refuses the initial group arrays
   */
  ColumnarGroupByAggregator(final DatabaseInternal db, final ColumnarGroupByPlan plan, final CrossBucketOrdinalMap keyMap,
      final CrossBucketOrdinalMap[] distinctMaps, final WorkGuard guard) {
    this.db = db;
    this.plan = plan;
    this.aggregates = plan.aggregates();
    this.ordinalKey = plan.groupKind() == ColumnKind.ORDINAL;
    this.keyClass = plan.groupType().getDefaultJavaType();
    this.keyMap = keyMap;
    this.distinctMaps = distinctMaps;
    this.guard = guard;
    final int items = aggregates.length;
    present = new long[items][];
    longValues = new long[items][];
    doubleValues = new double[items][];
    sums = new NumericSum[items][];
    distinct = new IntHashSet[items][];
    distinctObjects = new Object[items][];

    // rows and firstSeen, then the key: longKeys and the hash map entry, or the slot of objectKeys (allocated, at the capacity charged
    // here, once a record holds a value outside the dictionaries)
    long bytes = 8 + 8 + (ordinalKey ? 8 : 8 + HASH_ENTRY_BYTES);
    for (final AggregateSpec aggregate : aggregates)
      if (aggregate != null)
        bytes += switch (aggregate.kind()) {
          case COUNT_STAR -> 0;
          case COUNT -> 8;
          default -> 16;
        };
    this.groupBytes = bytes;

    if (ordinalKey) {
      // a group per merged ordinal, then the null group
      nullGroup = keyMap.size();
      size = nullGroup + 1;
      denseGroups = size;
      grow(size);
    } else {
      charges.charge(HASH_ENTRY_BYTES * INITIAL_LONG_KEY_GROUPS);
      longGroups = new LongLongHashMap(INITIAL_LONG_KEY_GROUPS);
      grow(INITIAL_LONG_KEY_GROUPS);
    }
  }

  /**
   * Counts the rows of one segment view: the positions of {@code matches} the view holds live, except those on pending pages (their
   * records are counted instead).
   *
   * @param rank the bucket's place in the scan order
   */
  void countSegment(final int rank, final int bucketId, final ColumnarSegmentView view, final PositionBitmap matches,
      final PositionBitmap pending) {
    if (view.live().isEmpty())
      return;
    final ColumnReader key = view.column(plan.groupProperty());
    final ColumnReader[] columns = new ColumnReader[aggregates.length];
    for (int i = 0; i < aggregates.length; i++)
      if (aggregates[i] != null && aggregates[i].property() != null && (columns[i] = view.column(aggregates[i].property())) == null)
        throw new Decline("a segment without the column '" + aggregates[i].property() + "'");
    if (key == null)
      throw new Decline("a segment without the column '" + plan.groupProperty() + "'");
    final long rankBits = (long) rank << RANK_SHIFT;
    segmentsCounted++;
    matches.forEachAnd(view.live(), position -> {
      if (pending.get(position))
        return;
      guard.checkPeriodically(++ticks);
      final int g = ordinalKey ? ordinalGroup(bucketId, key.ordinal(position)) :
          key.isPresent(position) ? longGroup(key.longValue(position)) : nullGroup();
      touch(g, rankBits | position);
      for (int i = 0; i < columns.length; i++) {
        final ColumnReader column = columns[i];
        if (column == null || !column.isPresent(position))
          continue;
        switch (aggregates[i].kind()) {
        case COUNT -> present[i][g]++;
        case MIN, MAX -> {
          if (aggregates[i].columnKind() == ColumnKind.DOUBLE)
            addDouble(i, g, column.doubleValue(position));
          else
            addLong(i, g, column.longValue(position));
        }
        case SUM, AVG -> addSum(i, g, column.longValue(position));
        case COUNT_DISTINCT -> addDistinct(i, g, merged(distinctMaps[i], bucketId, column.ordinal(position)));
        default -> {
        }
        }
      }
    });
  }

  /**
   * Counts one record by its raw values, what the normal plan groups on.
   *
   * @throws Decline when a value is not of its property's declared type
   */
  void countRecord(final int rank, final long position, final Document record) {
    guard.checkPeriodically(++ticks);
    if (position >>> RANK_SHIFT != 0)
      throw new Decline("a position beyond the range of the scan order");
    recordsCounted++;
    final int g = rawGroup(typed(record.get(plan.groupProperty()), keyClass, plan.groupProperty()));
    touch(g, ((long) rank << RANK_SHIFT) | position);
    for (int i = 0; i < aggregates.length; i++) {
      final AggregateSpec aggregate = aggregates[i];
      if (aggregate == null || aggregate.property() == null)
        continue;
      final Object value = typed(record.get(aggregate.property()), aggregate.propertyType().getDefaultJavaType(), aggregate.property());
      if (value == null)
        continue;
      switch (aggregate.kind()) {
      case COUNT -> present[i][g]++;
      case MIN, MAX -> {
        if (aggregate.columnKind() == ColumnKind.DOUBLE)
          addDouble(i, g, value instanceof Float f ? Type.widenFloat(f) : ((Number) value).doubleValue());
        else
          addLong(i, g, ((Number) value).longValue());
      }
      case SUM, AVG -> addSum(i, g, ((Number) value).longValue());
      case COUNT_DISTINCT -> {
        final int merged = distinctMaps[i].mergedOf(value);
        if (merged >= 0)
          addDistinct(i, g, merged);
        else
          addDistinctObject(i, g, value);
      }
      default -> {
      }
      }
    }
  }

  /** The value when it is null or of the declared Java type of its property. */
  private static Object typed(final Object value, final Class<?> expected, final String property) {
    if (value != null && value.getClass() != expected)
      throw new Decline("a record holds a " + value.getClass().getSimpleName() + " in '" + property + "'");
    return value;
  }

  private int ordinalGroup(final int bucketId, final int ordinal) {
    return ordinal < 0 ? nullGroup : merged(keyMap, bucketId, ordinal);
  }

  private static int merged(final CrossBucketOrdinalMap map, final int bucketId, final int ordinal) {
    final int merged = map.merged(bucketId, ordinal);
    if (merged < 0)
      throw new Decline("an ordinal outside the dictionaries of the query");
    return merged;
  }

  /** The group of a raw key value (null for a missing or null key). */
  private int rawGroup(final Object key) {
    if (key == null)
      return nullGroup();
    if (!ordinalKey)
      return longGroup(((Number) key).longValue());
    final int merged = keyMap.mergedOf(key);
    if (merged >= 0)
      return merged;
    if (objectGroups == null)
      objectGroups = new HashMap<>();
    final Integer existing = objectGroups.get(key);
    if (existing != null)
      return existing;
    charges.charge(OBJECT_ENTRY_BYTES);
    final int g = newGroup();
    if (objectKeys == null)
      // its 8 bytes per slot are in groupBytes, charged by every grow()
      objectKeys = new Object[capacity];
    objectKeys[g] = key;
    objectGroups.put(key, g);
    return g;
  }

  private int nullGroup() {
    if (nullGroup < 0)
      nullGroup = newGroup();
    return nullGroup;
  }

  private int longGroup(final long key) {
    if (key == Long.MIN_VALUE) {
      if (minValueGroup < 0) {
        minValueGroup = newGroup();
        longKeys[minValueGroup] = key;
      }
      return minValueGroup;
    }
    final long found = longGroups.get(key, -1L);
    if (found >= 0)
      return (int) found;
    final int g = newGroup();
    longKeys[g] = key;
    longGroups.put(key, g);
    return g;
  }

  private int newGroup() {
    if (size == capacity)
      // geometric in the groups past the dense part (all of them for an integral key): the first value outside the dictionaries of a
      // STRING key adds a few slots, not a copy of every dense array at twice its size
      grow(capacity + Math.max(INITIAL_LONG_KEY_GROUPS, capacity - denseGroups));
    return size++;
  }

  /** Grows every per-group array to {@code newCapacity} slots, charging the new arrays first (the old ones are still held). */
  private void grow(final int newCapacity) {
    if (newCapacity < 0 || newCapacity > Integer.MAX_VALUE - 8)
      throw new Decline(BUDGET);
    charges.charge(groupBytes * newCapacity);
    rows = rows == null ? new long[newCapacity] : Arrays.copyOf(rows, newCapacity);
    final int from = firstSeen == null ? 0 : firstSeen.length;
    firstSeen = firstSeen == null ? new long[newCapacity] : Arrays.copyOf(firstSeen, newCapacity);
    Arrays.fill(firstSeen, from, newCapacity, NOT_SEEN);
    if (!ordinalKey)
      longKeys = longKeys == null ? new long[newCapacity] : Arrays.copyOf(longKeys, newCapacity);
    if (objectKeys != null)
      objectKeys = Arrays.copyOf(objectKeys, newCapacity);
    for (int i = 0; i < aggregates.length; i++) {
      final AggregateSpec aggregate = aggregates[i];
      if (aggregate == null || aggregate.kind() == AggregateSpec.Kind.COUNT_STAR)
        continue;
      if (aggregate.kind() != AggregateSpec.Kind.COUNT_DISTINCT)
        present[i] = present[i] == null ? new long[newCapacity] : Arrays.copyOf(present[i], newCapacity);
      switch (aggregate.kind()) {
      case MIN, MAX -> {
        if (aggregate.columnKind() == ColumnKind.DOUBLE)
          doubleValues[i] = doubleValues[i] == null ? new double[newCapacity] : Arrays.copyOf(doubleValues[i], newCapacity);
        else
          longValues[i] = longValues[i] == null ? new long[newCapacity] : Arrays.copyOf(longValues[i], newCapacity);
      }
      case SUM, AVG -> sums[i] = sums[i] == null ? new NumericSum[newCapacity] : Arrays.copyOf(sums[i], newCapacity);
      case COUNT_DISTINCT -> {
        distinct[i] = distinct[i] == null ? new IntHashSet[newCapacity] : Arrays.copyOf(distinct[i], newCapacity);
        distinctObjects[i] = distinctObjects[i] == null ? new Object[newCapacity] : Arrays.copyOf(distinctObjects[i], newCapacity);
      }
      default -> {
      }
      }
    }
    capacity = newCapacity;
  }

  private void touch(final int g, final long seen) {
    rows[g]++;
    if (seen < firstSeen[g])
      firstSeen[g] = seen;
  }

  private void addLong(final int i, final int g, final long value) {
    final long[] values = longValues[i];
    if (present[i][g]++ == 0 || (aggregates[i].kind() == AggregateSpec.Kind.MIN ? value < values[g] : value > values[g]))
      values[g] = value;
  }

  /**
   * Ordered as {@code Double.compare} (NaN above every other value). SQL's {@code min()} and {@code max()} compare with
   * {@code BinaryComparator}, where -0.0 equals 0.0: a DOUBLE column never holds -0.0 (stored as 0.0), so the only difference is the sign of
   * a zero extreme, a documented limitation of the projection.
   */
  private void addDouble(final int i, final int g, final double value) {
    final double[] values = doubleValues[i];
    if (present[i][g]++ == 0) {
      values[g] = value;
      return;
    }
    final int compared = Double.compare(value, values[g]);
    if (aggregates[i].kind() == AggregateSpec.Kind.MIN ? compared < 0 : compared > 0)
      values[g] = value;
  }

  private void addSum(final int i, final int g, final long value) {
    NumericSum sum = sums[i][g];
    if (sum == null) {
      charges.charge(SUM_BYTES);
      sum = sums[i][g] = new NumericSum();
    }
    present[i][g]++;
    // the transitions of sum() over values of the declared type: an INTEGER total widens to Long, a LONG total to BigDecimal
    if (aggregates[i].propertyType() == Type.INTEGER)
      sum.addInt((int) value);
    else
      sum.addLong(value);
  }

  private void addDistinct(final int i, final int g, final int merged) {
    IntHashSet set = distinct[i][g];
    if (set == null) {
      charges.charge(INT_SET_BYTES);
      set = distinct[i][g] = new IntHashSet();
    }
    if (set.contains(merged))
      return;
    charges.charge(INT_SET_ELEMENT_BYTES);
    set.add(merged);
  }

  @SuppressWarnings("unchecked")
  private void addDistinctObject(final int i, final int g, final Object value) {
    Set<Object> set = (Set<Object>) distinctObjects[i][g];
    if (set == null) {
      charges.charge(OBJECT_ENTRY_BYTES);
      set = new HashSet<>();
      distinctObjects[i][g] = set;
    }
    if (set.contains(value))
      return;
    charges.charge(OBJECT_ENTRY_BYTES);
    set.add(value);
  }

  /** The segment views counted. */
  long segmentsCounted() {
    return segmentsCounted;
  }

  /** The records read and counted by their values. */
  long recordsCounted() {
    return recordsCounted;
  }

  /** The rows of the groups that hold a row, in the order the scan first meets them. */
  List<ResultInternal> emit() {
    int groups = 0;
    for (int g = 0; g < size; g++)
      if (rows[g] > 0)
        groups++;
    charges.charge((long) groups * (8 + HASH_ENTRY_BYTES));
    final long[] order = new long[groups];
    final LongLongHashMap groupOf = new LongLongHashMap(Math.max(16, groups));
    int n = 0;
    for (int g = 0; g < size; g++)
      if (rows[g] > 0) {
        order[n++] = firstSeen[g];
        groupOf.put(firstSeen[g], g);
      }
    Arrays.sort(order);

    final String[] aliases = plan.outputAliases();
    final List<ResultInternal> results = new ArrayList<>(groups);
    for (final long seen : order) {
      guard.checkPeriodically(++ticks);
      final int g = (int) groupOf.get(seen, -1L);
      final ResultInternal row = new ResultInternal(db);
      for (int i = 0; i < aliases.length; i++)
        row.setProperty(aliases[i], aggregates[i] == null ? key(g) : value(i, g));
      results.add(row);
    }
    return results;
  }

  private Object key(final int g) {
    if (g == nullGroup)
      return null;
    if (ordinalKey)
      return g < keyMap.size() ? keyMap.value(g) : objectKeys[g];
    return Type.convert(db, longKeys[g], keyClass);
  }

  private Object value(final int i, final int g) {
    final AggregateSpec aggregate = aggregates[i];
    return switch (aggregate.kind()) {
      case COUNT_STAR -> rows[g];
      case COUNT -> present[i][g];
      case MIN, MAX -> {
        if (present[i][g] == 0)
          yield null;
        if (aggregate.columnKind() != ColumnKind.DOUBLE)
          yield Type.convert(db, longValues[i][g], aggregate.propertyType().getDefaultJavaType());
        final double value = doubleValues[i][g];
        // a FLOAT column holds the shortest decimal widening of the float, which narrows back to it
        yield aggregate.propertyType() == Type.FLOAT ? (Object) (float) value : (Object) value;
      }
      case SUM -> sums[i][g] == null ? null : sums[i][g].get();
      case AVG -> sums[i][g] == null ? null : SQLFunctionAverage.average(sums[i][g], present[i][g]);
      case COUNT_DISTINCT -> {
        long count = distinct[i][g] == null ? 0 : distinct[i][g].size();
        if (distinctObjects[i][g] != null)
          count += ((Set<?>) distinctObjects[i][g]).size();
        yield count;
      }
    };
  }

  /** Releases every reservation; the groups must not be used afterwards. Idempotent. */
  @Override
  public void close() {
    charges.release();
  }
}
