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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.index.columnar.ColumnReader;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.index.columnar.GlobalDictionary;
import com.arcadedb.index.columnar.OrdinalConsumer;
import com.arcadedb.index.columnar.PositionBitmap;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * The DISTINCT collector: a dictionary column counts merged ordinals in a bitset (a value held by several buckets once), values with
 * no ordinal in a value set; a numeric column counts canonical values exactly in two long sets (integral values, non-integral double
 * bits) and switches to a HyperLogLog only when PRECISION asked for it (above the threshold, or when the budget refuses the sets). The
 * first tests drive a collector by hand over hand-built segment views and ordinal maps; the last ones go through SQL.
 */
class DistinctFacetCollectorTest extends TestHelper {
  private static final int              MAX_RECORDS = 128;
  private static final long             WAIT_MS     = 1_000L;
  private static final String           PROPERTY    = "p";
  private static final ColumnDefinition ORDINAL     = new ColumnDefinition(-1, PROPERTY, Type.STRING, ColumnKind.ORDINAL, false, null);
  private static final ColumnDefinition LIST        = new ColumnDefinition(-1, PROPERTY, Type.STRING, ColumnKind.ORDINAL_LIST, true, null);
  private static final ColumnDefinition LONG        = new ColumnDefinition(-1, PROPERTY, Type.LONG, ColumnKind.LONG, false, null);
  private static final ColumnDefinition DOUBLE      = new ColumnDefinition(-1, PROPERTY, Type.DOUBLE, ColumnKind.DOUBLE, false, null);
  private static final String           TYPE        = ColumnarTestData.TYPE;

  // ------------------------------------------------------------------------------------------------------- hand-built inputs

  private static DistinctFacetSpec spec(final long precision) {
    return new DistinctFacetSpec("d", PROPERTY, null, precision);
  }

  private static DistinctFacetCollector collector(final ColumnDefinition column, final long precision, final long matchCount) {
    return new DistinctFacetCollector(spec(precision), column, matchCount, null, null, WAIT_MS);
  }

  private static DistinctFacetCollector exact(final ColumnDefinition column) {
    return collector(column, Long.MAX_VALUE, 1_000);
  }

  private static GlobalDictionary dictionary(final String... values) {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    for (final String value : values)
      dictionary.getOrAdd(value);
    return dictionary;
  }

  /** {@code count} values {@code prefix0 ...}: ordinal i is {@code prefix + i}. */
  private static GlobalDictionary numbered(final String prefix, final int count) {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    for (int i = 0; i < count; i++)
      dictionary.getOrAdd(prefix + i);
    return dictionary;
  }

  private static PositionBitmap matches(final long... positions) {
    final PositionBitmap matches = new PositionBitmap(MAX_RECORDS);
    for (final long position : positions)
      matches.set(position);
    return matches;
  }

  private static PositionBitmap all(final int count) {
    final PositionBitmap matches = new PositionBitmap(MAX_RECORDS);
    for (int i = 0; i < count; i++)
      matches.set(i);
    return matches;
  }

  private static DistinctValue distinct(final FacetResult result) {
    assertThat(result.kind()).isEqualTo("distinct");
    assertThat(result.source()).as(result.reason()).isEqualTo("index");
    return result.distinct();
  }

  /** A segment view of hand-written rows of one column of the given kind: the live rows are those added by {@link #row}/{@link #value}. */
  private static final class Rows implements ColumnarSegmentView, ColumnReader {
    private final ColumnKind           kind;
    private final TreeMap<Long, int[]> ordinals = new TreeMap<>();
    private final TreeMap<Long, Long>  numbers  = new TreeMap<>();
    private final PositionBitmap       live     = new PositionBitmap(MAX_RECORDS);

    Rows(final ColumnKind kind) {
      this.kind = kind;
    }

    /** A live row with these ordinals (none: a row without a value). */
    Rows row(final long position, final int... values) {
      dead(position, values);
      live.set(position);
      return this;
    }

    /** A row the view still holds but that is dead (deleted, or superseded by an update in a later view). */
    Rows dead(final long position, final int... values) {
      final int[] sorted = values.clone();
      Arrays.sort(sorted);
      ordinals.put(position, sorted);
      return this;
    }

    /** A live row of a LONG (the value) or DOUBLE (its bits) column. */
    Rows value(final long position, final long value) {
      deadValue(position, value);
      live.set(position);
      return this;
    }

    Rows deadValue(final long position, final long value) {
      numbers.put(position, value);
      return this;
    }

    /** A live row of a numeric column without a value. */
    Rows empty(final long position) {
      live.set(position);
      return this;
    }

    @Override
    public PositionBitmap live() {
      return live;
    }

    @Override
    public PositionBitmap live(final int fromPage, final int toPageExclusive) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ColumnReader column(final String property) {
      return PROPERTY.equals(property) ? this : null;
    }

    @Override
    public long minLong(final String property) {
      return 0;
    }

    @Override
    public long maxLong(final String property) {
      return 0;
    }

    @Override
    public ColumnKind kind() {
      return kind;
    }

    @Override
    public boolean isPresent(final long position) {
      // a point read answers the stored row whatever the dead set says
      if (kind == ColumnKind.LONG || kind == ColumnKind.DOUBLE)
        return numbers.containsKey(position);
      return ordinals.containsKey(position) && ordinals.get(position).length > 0;
    }

    @Override
    public int ordinal(final long position) {
      return isPresent(position) ? ordinals.get(position)[0] : -1;
    }

    @Override
    public int ordinals(final long position, final int[] out) {
      final int[] values = isPresent(position) ? ordinals.get(position) : new int[0];
      System.arraycopy(values, 0, out, 0, Math.min(out.length, values.length));
      return values.length;
    }

    @Override
    public long longValue(final long position) {
      return numbers.getOrDefault(position, 0L);
    }

    @Override
    public double doubleValue(final long position) {
      return Double.longBitsToDouble(numbers.getOrDefault(position, 0L));
    }

    @Override
    public void forEachOrdinal(final PositionBitmap matches, final OrdinalConsumer consumer) {
      if (kind != ColumnKind.ORDINAL && kind != ColumnKind.ORDINAL_LIST)
        throw new IllegalStateException("not an ordinal column");
      for (final Map.Entry<Long, int[]> row : ordinals.entrySet())
        if (matches.get(row.getKey()) && live.get(row.getKey()))
          for (final int ordinal : row.getValue())
            consumer.accept(row.getKey(), ordinal);
    }
  }

  /** A numeric segment view of {@code count} live rows at positions 0..count-1, value i = {@code values[i]} (raw long or double bits). */
  private static final class Numbers implements ColumnarSegmentView, ColumnReader {
    private final ColumnKind     kind;
    private final long[]         values;
    private final PositionBitmap live;

    Numbers(final ColumnKind kind, final long[] values) {
      this.kind = kind;
      this.values = values;
      this.live = all(values.length);
    }

    @Override
    public PositionBitmap live() {
      return live;
    }

    @Override
    public PositionBitmap live(final int fromPage, final int toPageExclusive) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ColumnReader column(final String property) {
      return this;
    }

    @Override
    public long minLong(final String property) {
      return 0;
    }

    @Override
    public long maxLong(final String property) {
      return 0;
    }

    @Override
    public ColumnKind kind() {
      return kind;
    }

    @Override
    public boolean isPresent(final long position) {
      return position < values.length;
    }

    @Override
    public int ordinal(final long position) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int ordinals(final long position, final int[] out) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long longValue(final long position) {
      return values[(int) position];
    }

    @Override
    public double doubleValue(final long position) {
      return Double.longBitsToDouble(values[(int) position]);
    }

    @Override
    public void forEachOrdinal(final PositionBitmap matches, final OrdinalConsumer consumer) {
      throw new IllegalStateException("not an ordinal column");
    }
  }

  private static long[] range(final long from, final long toExclusive) {
    final long[] values = new long[(int) (toExclusive - from)];
    for (int i = 0; i < values.length; i++)
      values[i] = from + i;
    return values;
  }

  /** Counts {@code values} (one row each) in a fresh LONG collector of the given precision. */
  private static DistinctFacetCollector longs(final long precision, final long[] values) {
    final DistinctFacetCollector collector = collector(LONG, precision, values.length);
    collector.beginBucket(0, new Snapshot(0, null), null);
    collector.collect(new Numbers(ColumnKind.LONG, values), all(values.length));
    return collector;
  }

  /** A snapshot that only answers its bucket and its dictionary (the collector reads nothing else of it). */
  private record Snapshot(int bucketId, GlobalDictionary dictionary) implements ColumnarSnapshot {
    @Override
    public List<ColumnarSegmentView> segments() {
      return List.of();
    }

    @Override
    public PositionBitmap pendingPositions() {
      return new PositionBitmap(MAX_RECORDS);
    }

    @Override
    public long oldestPendingEpochStartMs() {
      return 0;
    }

    @Override
    public boolean isUsable() {
      return true;
    }

    @Override
    public GlobalDictionary dictionary(final String property) {
      return dictionary;
    }

    @Override
    public boolean publishes(final String property) {
      return true;
    }

    @Override
    public ColumnDefinition publishedColumn(final String property) {
      return ORDINAL;
    }

    @Override
    public void close() {
    }
  }

  /** The merged ordinals of hand-built bucket dictionaries (bucket i is {@code dictionaries[i]}), given by value in first-seen order. */
  private static final class Merged extends CrossBucketOrdinalMap {
    private final long                 version;
    private final int[][]              toMerged;
    private final List<Object>         values  = new ArrayList<>();
    private final Map<Object, Integer> byValue = new HashMap<>();

    Merged(final long version, final GlobalDictionary... dictionaries) {
      this.version = version;
      toMerged = new int[dictionaries.length][];
      for (int bucket = 0; bucket < dictionaries.length; bucket++) {
        toMerged[bucket] = new int[dictionaries[bucket].size()];
        for (int ordinal = 0; ordinal < toMerged[bucket].length; ordinal++) {
          final Object value = dictionaries[bucket].valueOf(ordinal);
          toMerged[bucket][ordinal] = byValue.computeIfAbsent(value, v -> {
            values.add(v);
            return values.size() - 1;
          });
        }
      }
    }

    @Override
    public int merged(final int bucketId, final int bucketOrdinal) {
      return toMerged[bucketId][bucketOrdinal];
    }

    @Override
    public Object value(final int mergedOrdinal) {
      return values.get(mergedOrdinal);
    }

    @Override
    public int size() {
      return values.size();
    }

    @Override
    public int mergedOf(final Object value) {
      return byValue.getOrDefault(value, -1);
    }

    @Override
    public long version() {
      return version;
    }

    @Override
    int[] bucketArray(final int bucketId) {
      return toMerged[bucketId];
    }
  }

  // ------------------------------------------------------------------------------------------------------- dictionary columns

  @Test
  void ordinalBitsetCountsEachValueOnce() {
    // ordinals 63 and 64 sit on both sides of a word boundary of the bitset
    final GlobalDictionary dictionary = numbered("v", 65);
    final DistinctFacetCollector collector = exact(ORDINAL);
    collector.beginBucket(0, new Snapshot(0, dictionary), new Merged(1, dictionary));
    collector.collect(new Rows(ColumnKind.ORDINAL).row(0, 1).row(1, 1).row(2, 5).row(3, 63).row(4, 64), matches(0, 1, 2, 3, 4));
    final FacetResult result = collector.finish();
    assertThat(distinct(result)).isEqualTo(new DistinctValue(4, true));
    assertThat(result.missing()).isZero();
  }

  @Test
  void sameValueInTwoBucketsCountsOnce() {
    // Nike is ordinal 0 in the first bucket, 2 in the second
    final GlobalDictionary first = dictionary("Nike", "Puma");
    final GlobalDictionary second = dictionary("Adidas", "Puma", "Nike");
    final Merged map = new Merged(1, first, second);
    final DistinctFacetCollector collector = exact(ORDINAL);
    collector.beginBucket(0, new Snapshot(0, first), map);
    collector.collect(new Rows(ColumnKind.ORDINAL).row(0, 0), matches(0));
    collector.beginBucket(1, new Snapshot(1, second), map);
    collector.collect(new Rows(ColumnKind.ORDINAL).row(0, 2).row(1, 2), matches(0, 1));
    assertThat(distinct(collector.finish())).isEqualTo(new DistinctValue(1, true));
  }

  @Test
  void listColumnCountsDistinctItems() {
    final GlobalDictionary dictionary = dictionary("a", "b", "c");
    final DistinctFacetCollector collector = exact(LIST);
    collector.beginBucket(0, new Snapshot(0, dictionary), new Merged(1, dictionary));
    collector.collect(new Rows(ColumnKind.ORDINAL_LIST).row(0, 0, 1).row(1, 1, 2), matches(0, 1));
    assertThat(distinct(collector.finish())).isEqualTo(new DistinctValue(3, true));
  }

  @Test
  void missingAndNullAreNotValues() {
    final GlobalDictionary dictionary = dictionary("a");
    final DistinctFacetCollector collector = exact(ORDINAL);
    collector.beginBucket(0, new Snapshot(0, dictionary), new Merged(1, dictionary));
    // row 1 has no value, a patched record without one is missing too
    collector.collect(new Rows(ColumnKind.ORDINAL).row(0, 0).row(1), matches(0, 1));
    collector.collectPatched(5, PatchedValue.ofValues(dictionary));
    // a scanned bucket: a record whose value is null reads as missing, never as a value
    collector.beginBucket(1, null, null);
    final PatchedValue missing = new PatchedValue();
    missing.missing();
    collector.collectPatched(0, missing);
    final FacetResult result = collector.finish();
    assertThat(distinct(result)).isEqualTo(new DistinctValue(1, true));
    assertThat(result.missing()).isEqualTo(3L);

    final DistinctFacetCollector numbers = exact(LONG);
    numbers.beginBucket(0, new Snapshot(0, null), null);
    numbers.collect(new Rows(ColumnKind.LONG).value(0, 7L).empty(1).empty(2), matches(0, 1, 2));
    final FacetResult numeric = numbers.finish();
    assertThat(distinct(numeric)).isEqualTo(new DistinctValue(1, true));
    assertThat(numeric.missing()).isEqualTo(2L);
  }

  @Test
  void patchedValueWithoutOrdinalIsCounted() {
    final GlobalDictionary dictionary = dictionary("existing");
    final DistinctFacetCollector collector = exact(ORDINAL);
    collector.beginBucket(0, new Snapshot(0, dictionary), new Merged(1, dictionary));
    collector.collect(new Rows(ColumnKind.ORDINAL).row(0, 0), matches(0));
    final PatchedValue fresh = PatchedValue.ofValues(dictionary, "new");
    assertThat(fresh.unknownCount()).isEqualTo(1);
    collector.collectPatched(3, fresh);
    collector.collectPatched(4, PatchedValue.ofValues(dictionary, "new"));
    // a value the map holds, arriving by value (a scanned record, an extraction against another dictionary): its merged ordinal
    final PatchedValue byValue = new PatchedValue();
    byValue.unknownValue("existing");
    collector.collectPatched(5, byValue);
    collector.collectPatched(6, PatchedValue.ofValues(dictionary, "existing"));
    assertThat(distinct(collector.finish())).isEqualTo(new DistinctValue(2, true));
  }

  // ------------------------------------------------------------------------------------------------------- numeric columns

  @Test
  void numericExactByDefault() {
    final DistinctFacetCollector collector = longs(Long.MAX_VALUE, range(-100_000, 100_000));
    assertThat(distinct(collector.finish())).isEqualTo(new DistinctValue(200_000, true));
  }

  @Test
  void integralDoubleAndLongAreOneValue() {
    // a DOUBLE column: 1.0 is the integral 1, -0.0 is 0, 1.5 stays a double
    final DistinctFacetCollector doubles = exact(DOUBLE);
    doubles.beginBucket(0, new Snapshot(0, null), null);
    doubles.collect(new Rows(ColumnKind.DOUBLE).value(0, Double.doubleToLongBits(1.0)).value(1, Double.doubleToLongBits(-0.0))
        .value(2, Double.doubleToLongBits(0.0)).value(3, Double.doubleToLongBits(1.5)), matches(0, 1, 2, 3));
    // patched and scanned values of the same column: 1L, 0L and 1.5 are no new values
    doubles.collectPatched(10, longValue(1L));
    doubles.collectPatched(11, longValue(0L));
    doubles.collectPatched(12, doubleValue(1.5));
    doubles.beginBucket(1, null, null);
    // keyed as count(DISTINCT) keys them (Type.normalizeForKey): 2^60 as a long and as a double are two values (the double keys as its
    // shortest decimal form), a DECIMAL no double holds is its own value (0.1 vs 0.10000000000000000001, 2^53 vs 2^53 + 1)
    final List<Object> raws = List.of(1L, 1, (short) 1, 1.0d, 1.0f, new BigDecimal("1.00"), -0.0d, 0L, 1.5d, new BigDecimal("1.5"), 1L << 60,
        (double) (1L << 60), 0.1d, new BigDecimal("0.1"), new BigDecimal("0.10000000000000000001"), new BigDecimal("9007199254740992"),
        new BigDecimal("9007199254740993"), 1e20d, new BigDecimal("1E+20"));
    for (final Object raw : raws)
      doubles.collectPatched(0, unknown(raw));
    final FacetResult result = doubles.finish();
    final Set<Object> keys = new HashSet<>();
    for (final Object raw : List.of(1.0d, -0.0d, 0.0d, 1.5d, 1L, 0L))
      keys.add(Type.normalizeForKey(raw));
    for (final Object raw : raws)
      keys.add(Type.normalizeForKey(raw));
    assertThat(keys).hasSize(10);
    assertThat(distinct(result)).isEqualTo(new DistinctValue(keys.size(), true));

    // Long.MIN_VALUE is a value like any other (the long set's empty slot is not)
    final DistinctFacetCollector extremes = longs(Long.MAX_VALUE, new long[] { Long.MIN_VALUE, Long.MIN_VALUE, 0L, Long.MAX_VALUE });
    assertThat(distinct(extremes.finish())).isEqualTo(new DistinctValue(3, true));
  }

  private static PatchedValue longValue(final long value) {
    final PatchedValue patched = new PatchedValue();
    patched.longValue(value);
    return patched;
  }

  private static PatchedValue doubleValue(final double value) {
    final PatchedValue patched = new PatchedValue();
    patched.doubleValue(value);
    return patched;
  }

  private static PatchedValue unknown(final Object value) {
    final PatchedValue patched = new PatchedValue();
    patched.unknownValue(value);
    return patched;
  }

  @Test
  void precisionThresholdSwitchesToHll() {
    final DistinctFacetCollector collector = longs(1_000, range(0, 100_000));
    final DistinctValue value = distinct(collector.finish());
    assertThat(value.exact()).isFalse();
    assertThat((double) value.value()).isCloseTo(100_000d, within(3_000d));

    // at the threshold it stays exact
    assertThat(distinct(longs(1_000, range(0, 1_000)).finish())).isEqualTo(new DistinctValue(1_000, true));
  }

  /**
   * Review I1: the charge of a numeric set covers its table, and both tables while it doubles. 3073 distinct longs reach the resize of
   * a 4096-slot table into 8192 slots: 96 KiB live during the copy, 64 KiB after, where 16 bytes an entry would be 48 KiB. Under 80 KiB
   * only the copy does not fit.
   */
  @Test
  void setChargeCoversTheTableAndItsResize() {
    final FacetResult refused = withBudget(80 * 1024, () -> countLongs(Long.MAX_VALUE, 3_073, 3_073));
    assertThat(refused.source()).isEqualTo(FacetResult.UNAVAILABLE);
    assertThat(refused.reason()).isEqualTo("memory budget");
    // one value fewer: no resize, the 4096-slot table fits
    assertThat(distinct(withBudget(80 * 1024, () -> countLongs(Long.MAX_VALUE, 3_073, 3_071)))).isEqualTo(new DistinctValue(3_071, true));
    assertThat(distinct(withBudget(128 * 1024, () -> countLongs(Long.MAX_VALUE, 3_073, 3_073)))).isEqualTo(new DistinctValue(3_073, true));
  }

  @Test
  void budgetRefusalWithoutPrecisionIsUnavailable() {
    final long before = FacetMemoryBudget.instance().available();
    final FacetResult result = countWithLittleBudget(Long.MAX_VALUE);
    assertThat(result.kind()).isEqualTo("distinct");
    assertThat(result.source()).isEqualTo(FacetResult.UNAVAILABLE);
    assertThat(result.reason()).isEqualTo("memory budget");
    assertThat(result.distinct()).isNull();
    assertThat(FacetMemoryBudget.instance().available()).isEqualTo(before);
  }

  @Test
  void budgetRefusalWithPrecisionSwitchesToHll() {
    final long before = FacetMemoryBudget.instance().available();
    final DistinctValue value = distinct(countWithLittleBudget(1_000_000));
    assertThat(value.exact()).isFalse();
    assertThat((double) value.value()).isCloseTo(5_000d, within(150d));
    assertThat(FacetMemoryBudget.instance().available()).isEqualTo(before);
  }

  /**
   * Counts 5000 distinct longs, sized for a million matches, under a budget of 64 KiB: the set's doubling to 8192 slots (96 KiB with the
   * table it replaces) is refused, the 16 KiB of a HyperLogLog are not.
   */
  private static FacetResult countWithLittleBudget(final long precision) {
    return withBudget(64 * 1024, () -> countLongs(precision, 1_000_000, 5_000));
  }

  private static FacetResult countLongs(final long precision, final long matchCount, final int distinct) {
    final DistinctFacetCollector collector = new DistinctFacetCollector(spec(precision), LONG, matchCount, null, null, 10L);
    collector.beginBucket(0, new Snapshot(0, null), null);
    collector.collect(new Numbers(ColumnKind.LONG, range(0, distinct)), all(distinct));
    return collector.finish();
  }

  /**
   * Runs {@code counting} under a budget of {@code bytes} for the whole JVM, restored afterwards; a test-scoped budget, so no other
   * reservation is needed to starve the collector (review M7).
   */
  private static FacetResult withBudget(final long bytes, final Supplier<FacetResult> counting) {
    final Object previous = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValue();
    GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(bytes);
    try {
      assertThat(FacetMemoryBudget.instance().available()).isEqualTo(bytes);
      final FacetResult result = counting.get();
      assertThat(FacetMemoryBudget.instance().available()).as("everything given back").isEqualTo(bytes);
      return result;
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(previous);
      FacetMemoryBudget.instance();
    }
  }

  @Test
  void mergeAcrossTasksExactAndHll() {
    // exact + exact: the union of the values
    final DistinctFacetCollector a = longs(Long.MAX_VALUE, range(0, 100));
    a.merge(longs(Long.MAX_VALUE, range(50, 150)));
    assertThat(distinct(a.finish())).isEqualTo(new DistinctValue(150, true));

    // exact + hll, both ways: an estimate of the union
    final DistinctFacetCollector exactFirst = longs(1_000, range(0, 100));
    exactFirst.merge(longs(1_000, range(0, 5_000)));
    final DistinctValue mixed = distinct(exactFirst.finish());
    assertThat(mixed.exact()).isFalse();
    assertThat((double) mixed.value()).isCloseTo(5_000d, within(150d));
    final DistinctFacetCollector hllFirst = longs(1_000, range(0, 5_000));
    hllFirst.merge(longs(1_000, range(4_950, 5_050)));
    final DistinctValue reversed = distinct(hllFirst.finish());
    assertThat(reversed.exact()).isFalse();
    assertThat((double) reversed.value()).isCloseTo(5_050d, within(152d));

    // hll + hll: register max, the overlap counted once
    final DistinctFacetCollector first = longs(1_000, range(0, 5_000));
    first.merge(longs(1_000, range(2_500, 7_500)));
    final DistinctValue union = distinct(first.finish());
    assertThat(union.exact()).isFalse();
    assertThat((double) union.value()).isCloseTo(7_500d, within(225d));
  }

  private static DistinctFacetCollector dateColumn(final Type type, final long... epochs) {
    final DistinctFacetCollector collector = exact(new ColumnDefinition(-1, PROPERTY, type, ColumnKind.LONG, false, null));
    collector.beginBucket(0, new Snapshot(0, null), null);
    for (final long epoch : epochs)
      collector.collectPatched(0, longValue(epoch));
    return collector;
  }

  /**
   * Review I3 and M5: count(DISTINCT) keys a DATE as its LocalDate and a DATETIME of any precision as its instant, so day 1 and the
   * instant 1970-01-02T00:00Z are two values, while the DATETIME family merges in its finest unit whatever the order of the collectors
   * (here a scanned collector without dates comes first).
   */
  @Test
  void dateAndDatetimeAreApartDatetimesMergeInTheFinest() {
    final DistinctFacetCollector scanned = exact(null);
    scanned.beginBucket(9, null, null);
    scanned.collectPatched(0, unknown("x"));
    scanned.merge(dateColumn(Type.DATE, 1L, 2L));
    scanned.merge(dateColumn(Type.DATETIME, 86_400_000L, 86_400_005L));
    // 86400000000 us is the first ms instant; 86400005001 us is 1 us past the second, which a merge in ms would fold into it
    scanned.merge(dateColumn(Type.DATETIME_MICROS, 86_400_000_000L, 86_400_005_001L));
    final Set<Object> keys = new HashSet<>();
    for (final Object value : List.of("x", LocalDate.ofEpochDay(1), LocalDate.ofEpochDay(2), LocalDateTime.of(1970, 1, 2, 0, 0),
        LocalDateTime.of(1970, 1, 2, 0, 0, 0, 5_000_000), LocalDateTime.of(1970, 1, 2, 0, 0, 0, 5_001_000)))
      keys.add(Type.normalizeForKey(value));
    assertThat(keys).hasSize(6);
    assertThat(distinct(scanned.finish())).isEqualTo(new DistinctValue(6, true));
  }

  /** Review r1 N1: a number held as a dictionary ordinal and as a numeric key of another collector is one value, in both merge orders. */
  @Test
  void numberInADictionaryAndInANumericSetCountsOnce() {
    for (final boolean dictionaryFirst : new boolean[] { true, false }) {
      final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
      for (final Object value : List.of(1L, 2L, 3L, 2.5d, "a"))
        dictionary.getOrAdd(value);
      final DistinctFacetCollector ordinals = exact(LIST);
      ordinals.beginBucket(0, new Snapshot(0, dictionary), new Merged(1, dictionary));
      ordinals.collect(new Rows(ColumnKind.ORDINAL_LIST).row(0, 0, 1, 2, 3, 4), matches(0));
      final DistinctFacetCollector numbers = exact(null);
      numbers.beginBucket(7, null, null);
      for (final Object raw : List.of(2, 3L, 4, 2.5d))
        numbers.collectPatched(0, unknown(raw));
      final DistinctFacetCollector first = dictionaryFirst ? ordinals : numbers;
      first.merge(dictionaryFirst ? numbers : ordinals);
      // 1, 2, 3, 4, 2.5 and "a"
      assertThat(distinct(first.finish())).as("dictionary first: " + dictionaryFirst).isEqualTo(new DistinctValue(6, true));
    }
  }

  /** Review M4: raw temporals (a property no type declares) are keyed into the same spaces as the date columns' epoch counts. */
  @Test
  void rawTemporalsMeetTheEpochCountsOfDateColumns() {
    final DistinctFacetCollector scanned = exact(null);
    scanned.beginBucket(9, null, null);
    final List<Object> raws = List.of(LocalDate.ofEpochDay(1), LocalDateTime.of(1970, 1, 2, 0, 0), new Date(86_400_000L),
        Instant.ofEpochMilli(86_400_000L), LocalDateTime.of(1970, 1, 2, 0, 0, 0, 1));
    for (final Object raw : raws)
      scanned.collectPatched(0, unknown(raw));
    scanned.merge(dateColumn(Type.DATE, 1L));
    scanned.merge(dateColumn(Type.DATETIME, 86_400_000L));
    final Set<Object> keys = new HashSet<>();
    for (final Object value : raws)
      keys.add(Type.normalizeForKey(value));
    keys.add(Type.normalizeForKey(LocalDate.ofEpochDay(1)));
    keys.add(Type.normalizeForKey(LocalDateTime.of(1970, 1, 2, 0, 0)));
    assertThat(keys).hasSize(3);
    assertThat(distinct(scanned.finish())).isEqualTo(new DistinctValue(3, true));
  }

  @Test
  void validationOnlyCollectorFinishesEmpty() {
    final FacetCollector collector = FacetExecutor.collectorFor(spec(Long.MAX_VALUE), null, null, 0);
    assertThat(collector).isInstanceOf(DistinctFacetCollector.class);
    collector.release();
    final FacetResult result = collector.finish();
    assertThat(distinct(result)).isEqualTo(new DistinctValue(0, true));
    assertThat(result.missing()).isZero();
    assertThat(result.buckets()).isEmpty();
  }

  @Test
  void deletedAndUpdatedRowsAreNotCounted() {
    // the matches are not ANDed with live by the caller: dead rows 1 (deleted) and 2 (superseded by the overlay below) are matched
    final GlobalDictionary dictionary = dictionary("a", "b", "c", "d");
    final DistinctFacetCollector strings = exact(ORDINAL);
    strings.beginBucket(0, new Snapshot(0, dictionary), new Merged(1, dictionary));
    strings.collect(new Rows(ColumnKind.ORDINAL).row(0, 0).dead(1, 1).dead(2, 2), matches(0, 1, 2));
    strings.collect(new Rows(ColumnKind.ORDINAL).row(2, 3), matches(0, 1, 2));
    final FacetResult byOrdinal = strings.finish();
    assertThat(distinct(byOrdinal)).isEqualTo(new DistinctValue(2, true));
    // a dead row is no missing value either
    assertThat(byOrdinal.missing()).isZero();

    final DistinctFacetCollector numbers = exact(LONG);
    numbers.beginBucket(0, new Snapshot(0, null), null);
    numbers.collect(new Rows(ColumnKind.LONG).value(0, 10L).deadValue(1, 11L).deadValue(2, 12L), matches(0, 1, 2));
    numbers.collect(new Rows(ColumnKind.LONG).value(2, 13L), matches(0, 1, 2));
    final FacetResult byValue = numbers.finish();
    assertThat(distinct(byValue)).isEqualTo(new DistinctValue(2, true));
    assertThat(byValue.missing()).isZero();
  }

  @Test
  void mergeAcrossDeclaringTypesByValue() {
    // two declaring types, two maps (other versions): Nike is merged ordinal 0 in the first, 1 in the second
    final GlobalDictionary first = dictionary("Nike", "Adidas");
    final GlobalDictionary second = dictionary("Puma", "Nike");
    final DistinctFacetCollector a = exact(ORDINAL);
    a.beginBucket(0, new Snapshot(0, first), new Merged(1, first));
    a.collect(new Rows(ColumnKind.ORDINAL).row(0, 0).row(1, 1), matches(0, 1));
    final DistinctFacetCollector b = exact(ORDINAL);
    b.beginBucket(0, new Snapshot(0, second), new Merged(2, second));
    b.collect(new Rows(ColumnKind.ORDINAL).row(0, 1).row(1, 0), matches(0, 1));
    // a scanned bucket of a third type: Nike and Reebok by value
    final DistinctFacetCollector scanned = exact(null);
    scanned.beginBucket(7, null, null);
    scanned.collectPatched(0, unknown("Nike"));
    scanned.collectPatched(1, unknown("Reebok"));
    a.merge(b);
    a.merge(scanned);
    // Nike, Adidas, Puma, Reebok: a positional OR of the two bitsets would answer 3
    assertThat(distinct(a.finish())).isEqualTo(new DistinctValue(4, true));

    // the same map (one version): the bitsets are ORed
    final Merged shared = new Merged(3, first);
    final DistinctFacetCollector left = exact(ORDINAL);
    left.beginBucket(0, new Snapshot(0, first), shared);
    left.collect(new Rows(ColumnKind.ORDINAL).row(0, 0), matches(0));
    final DistinctFacetCollector right = exact(ORDINAL);
    right.beginBucket(0, new Snapshot(0, first), shared);
    right.collect(new Rows(ColumnKind.ORDINAL).row(0, 0).row(1, 1), matches(0, 1));
    left.merge(right);
    assertThat(distinct(left.finish())).isEqualTo(new DistinctValue(2, true));
  }

  // ------------------------------------------------------------------------------------------------------- through SQL

  private FacetResult facet(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.getFacets().get("d");
    }
  }

  private long scalar(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @Test
  @Timeout(120)
  void missingIsWritten() {
    final ColumnarTestData data = new ColumnarTestData(11L);
    data.createSchema(database);
    data.insertAssets(database, 500);
    database.transaction(() -> {
      for (int i = 0; i < 30; i++)
        database.newDocument(TYPE).set("fileSize", 10L + i).save();
    });
    FacetOracle.awaitCaughtUp(database);
    for (final String property : List.of("brand", "author", "fileSize")) {
      final FacetResult result = facet("SELECT FROM " + TYPE + " FACETS d: " + property + " DISTINCT");
      assertThat(distinct(result).value()).as(property).isEqualTo(FacetOracle.distinctCount(database, TYPE, property, null));
      assertThat(result.missing()).as(property)
          .isEqualTo(scalar("SELECT count(*) AS c FROM " + TYPE) - scalar("SELECT count(" + property + ") AS c FROM " + TYPE));
    }
    // author is unset on every tenth asset and on the 30 extra records
    assertThat(facet("SELECT FROM " + TYPE + " FACETS d: author DISTINCT").missing()).isEqualTo(80L);

    // review M8, a list: a record without the property or with an empty list has no value
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newDocument(TYPE).set("tags", List.of()).save();
    });
    FacetOracle.awaitCaughtUp(database);
    final FacetResult tags = facet("SELECT FROM " + TYPE + " FACETS d: tags DISTINCT");
    assertThat(distinct(tags).value()).isEqualTo(FacetOracle.distinctCount(database, TYPE, "tags", null));
    final long withoutItems = scalar("SELECT count(*) AS c FROM " + TYPE + " WHERE tags IS NULL OR tags.size() = 0");
    assertThat(withoutItems).isGreaterThanOrEqualTo(35L);
    assertThat(tags.missing()).isEqualTo(withoutItems);
  }

  @Test
  @Timeout(120)
  void deletedAndUpdatedRowsAreNotCountedThroughSql() {
    final ColumnarTestData data = new ColumnarTestData(5L);
    data.createSchema(database);
    data.insertAssets(database, 600);
    FacetOracle.awaitCaughtUp(database);
    database.transaction(() -> {
      database.command("sql", "DELETE FROM " + TYPE + " WHERE brand = 'brand-1'");
      database.command("sql", "UPDATE " + TYPE + " SET brand = 'renamed', fileSize = -1 WHERE brand = 'brand-2'");
    });
    FacetOracle.awaitCaughtUp(database);
    for (final String property : List.of("brand", "fileSize", "tags")) {
      final FacetResult result = facet("SELECT FROM " + TYPE + " FACETS d: " + property + " DISTINCT");
      assertThat(result.source()).isEqualTo("index");
      assertThat(distinct(result)).as(property).isEqualTo(new DistinctValue(FacetOracle.distinctCount(database, TYPE, property, null), true));
    }
  }
}
