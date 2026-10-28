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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.IntBinaryOperator;
import java.util.function.LongConsumer;

/**
 * Counts a terms facet for one declaring type. A dictionary column (ORDINAL, ORDINAL_LIST) is counted per merged ordinal of the type's
 * {@link CrossBucketOrdinalMap}, in a dense array or a hash map ({@link FacetCounter#prefersDense}); only the top ordinals are resolved
 * to values at {@link #finish}. A LONG or DOUBLE column is counted per raw 64-bit term in a {@link LongTermsCounter}; a DATE or DATETIME
 * term is ranked on its epoch count and returned as the object the property reads back as (what GROUP BY returns). A record counts
 * once per distinct value (an ORDINAL_LIST is deduplicated at write time), and a matched live row without a value counts as missing.
 * <p>
 * Every byte a counter holds is reserved from {@link FacetMemoryBudget} before it is allocated: a dense array without waiting, else a
 * hash map waiting up to {@code arcadedb.columnar.queryBudgetWait}. A hash map that can grow past what it was presized for is topped up
 * first: a LONG or DOUBLE counter before each segment view, to the distinct terms it can reach there ({@code matchCount} at most), an
 * ORDINAL_LIST hash counter when it reaches the distinct ordinals reserved (the cardinality at most). Any refusal makes the facet
 * {@code unavailable} with the reason {@code memory budget} and releases everything it held. The reservations are released when the
 * facet is ranked or {@link #release released}.
 * <p>
 * Not thread safe: a collector is driven by one query thread.
 */
public final class TermsCollector implements FacetCollector, OrdinalConsumer {
  /** The distinct LONG or DOUBLE terms a counter is presized for at most; it is topped up as it grows past them. */
  static final int    NUMERIC_EXPECTED_TERMS = 1 << 10;
  static final String MEMORY_BUDGET          = "memory budget";

  private final TermsFacetSpec      spec;
  private final ColumnKind          kind;
  private final Type                propertyType;
  private final DatabaseInternal    database;
  private final ColumnarValueExtractor extractor;
  private final long                matchCount;
  /** Every byte a counter holds, reserved before it is allocated; all of them together wait one window. */
  private final CollectorReservations budget;
  private final FacetExecutionStats stats  = new FacetExecutionStats();
  private final List<TermsCollector> others = new ArrayList<>();
  private final NumericRows         numericRows = new NumericRows();

  private CrossBucketOrdinalMap                     map;
  private FacetCounter                              counter;
  /** The counter when it is a hash map over ORDINAL_LIST values: the one ordinal counter that can outgrow its presizing. */
  private HashFacetCounter                          growingHash;
  private int                                       cardinality;
  private LongTermsCounter                          terms;
  /** The distinct values of the hash counter the reservations cover. */
  private int                                       reservedDistinct;
  private boolean                                   unavailable;
  private boolean                                   finished;
  private long                                      missing;
  /** Values only the patched records have, by normalized value (rare). */
  private Map<Object, long[]>                       foreign;

  // the bucket being counted
  private ColumnarSnapshot snapshot;
  private int[]            toMerged;
  // the segment being counted: positions arrive in ascending order, so a change of position is a new document
  private long             lastPosition;
  private long             positions;

  /**
   * @param column     the column of the property in the declaring type, null when no bucket is counted (no match)
   * @param matchCount the records the facet counts in this declaring type, to size the counter
   * @param budgetWaitMs the longest this collector waits for the budget, all its reservations together (one deadline)
   * @param database     decodes DATE and DATETIME terms into the objects the property reads back as
   * @param extractor    the declaring type's extractor: converts an INCLUDE literal of a LONG column as stored values are converted
   */
  TermsCollector(final TermsFacetSpec spec, final ColumnDefinition column, final long matchCount, final long budgetWaitMs,
      final DatabaseInternal database, final ColumnarValueExtractor extractor) {
    this.spec = spec;
    this.kind = column == null ? null : column.kind();
    this.propertyType = column == null ? null : column.propertyType();
    this.database = database;
    this.extractor = extractor;
    this.matchCount = matchCount;
    this.budget = new CollectorReservations(budgetWaitMs);
  }

  @Override
  public void beginBucket(final int bucketId, final ColumnarSnapshot snapshot, final CrossBucketOrdinalMap map) {
    this.snapshot = snapshot;
    if (isDictionaryKind()) {
      if (map == null)
        throw new IllegalArgumentException("FACETS " + spec.alias() + ": the dictionary column '" + spec.property() + "' needs an ordinal map");
      if (this.map == null) {
        this.map = map;
        allocateOrdinalCounter();
      } else if (this.map != map)
        throw new IllegalArgumentException("FACETS " + spec.alias() + ": every bucket of a declaring type is counted with the same ordinal map");
      toMerged = map.bucketArray(bucketId);
    } else if (terms == null && !unavailable)
      allocateTermsCounter();
  }

  @Override
  public void collect(final ColumnarSegmentView segment, final PositionBitmap matches) {
    if (unavailable)
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
      column.forEachOrdinal(matches, this);
      missing += matchedLive - positions;
      return;
    }
    // at most one new term per matched live row
    if (!reserveDistinct((int) Math.min(matchCount, (long) terms.size() + matchedLive)))
      return;
    numericRows.column = column;
    numericRows.doubles = kind == ColumnKind.DOUBLE;
    matches.forEachAnd(live, numericRows);
    numericRows.column = null;
  }

  /** {@link OrdinalConsumer}: one ordinal of a matched live row, ascending positions. */
  @Override
  public void accept(final long position, final int ordinal) {
    if (unavailable)
      return;
    if (position != lastPosition) {
      lastPosition = position;
      positions++;
    }
    if (growingHash != null && growingHash.size() >= reservedDistinct && reservedDistinct < cardinality && !growHash())
      return;
    counter.increment(toMerged == null ? ordinal : toMerged[ordinal]);
  }

  @Override
  public void collectPatched(final long position, final PatchedValue value) {
    if (unavailable)
      return;
    if (value.isMissing()) {
      missing++;
      return;
    }
    if (terms != null && !reserveDistinct((int) Math.min(matchCount, (long) terms.size() + 1)))
      return;
    if (isDictionaryKind()) {
      for (int i = 0; i < value.ordinalCount(); i++) {
        final int ordinal = value.ordinalAt(i);
        final int merged = toMerged == null ? ordinal : ordinal < toMerged.length ? toMerged[ordinal] : -1;
        if (merged >= 0 && merged < cardinality)
          countOrdinal(merged);
        else
          addForeign(snapshot.dictionary(spec.property()).valueOf(ordinal));
        if (unavailable)
          return;
      }
    } else if (value.hasLong())
      terms.increment(value.longValue());
    else if (value.hasDouble())
      terms.increment(doubleKey(value.doubleValue()));
    for (int i = 0; i < value.unknownCount(); i++) {
      final int merged = map == null ? -1 : map.mergedOf(value.unknown(i));
      if (merged >= 0 && merged < cardinality)
        countOrdinal(merged);
      else
        addForeign(value.unknown(i));
      if (unavailable)
        return;
    }
  }

  @Override
  public void merge(final FacetCollector other) {
    if (!(other instanceof TermsCollector collector))
      throw new IllegalArgumentException("FACETS " + spec.alias() + ": cannot merge a " + other.getClass().getSimpleName() + " into terms");
    others.add(collector);
    stats.add(collector.stats);
  }

  @Override
  public FacetResult finish() {
    if (finished)
      throw new IllegalStateException("FACETS " + spec.alias() + ": already finished");
    try {
      if (unavailable || anyOtherUnavailable()) {
        stats.source(FacetResult.UNAVAILABLE);
        return FacetResult.unavailable("terms", MEMORY_BUDGET);
      }
      stats.source("index");
      for (final TermsCollector collector : all())
        stats.counterBytes(collector.counterBytes());
      if (others.isEmpty() && foreign == null && counter != null)
        return rankOrdinals();
      // collectors whose columns differ (sibling subtypes declaring the property with other types) merge on the values the records read
      // back as, each converting its own terms; otherwise date terms stay epoch counts until the returned buckets
      final boolean converted = !sameColumnType();
      final Map<Object, long[]> counts = new HashMap<>();
      long missingTotal = 0;
      for (final TermsCollector collector : all()) {
        collector.addValuesTo(counts, converted);
        missingTotal += collector.missing;
      }
      return rankValues(counts, missingTotal, converted);
    } finally {
      finished = true;
      release();
      for (final TermsCollector collector : others)
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

  /** Grants of the budget that were not immediate, this collector's and the merged ones'. */
  @Override
  public int budgetWaits() {
    int waits = budget.waits();
    for (final TermsCollector collector : others)
      waits += collector.budget.waits();
    return waits;
  }

  // ------------------------------------------------------------------------------------------------------------------ counting

  /** Visits the matched live positions of a LONG or DOUBLE segment view ({@link PositionBitmap#forEachAnd}); reused. */
  private final class NumericRows implements LongConsumer {
    private ColumnReader column;
    private boolean      doubles;

    @Override
    public void accept(final long position) {
      if (!column.isPresent(position))
        missing++;
      else if (doubles)
        terms.increment(doubleKey(column.doubleValue(position)));
      else
        terms.increment(column.longValue(position));
    }
  }

  /** The heap this collector's counter holds now. */
  private long counterBytes() {
    return counter != null ? counter.estimatedBytes() : terms != null ? terms.estimatedBytes() : 0;
  }

  private boolean isDictionaryKind() {
    return kind == ColumnKind.ORDINAL || kind == ColumnKind.ORDINAL_LIST;
  }

  private void countOrdinal(final int merged) {
    if (growingHash != null && growingHash.size() >= reservedDistinct && reservedDistinct < cardinality && !growHash())
      return;
    counter.increment(merged);
  }

  private void allocateOrdinalCounter() {
    final int size = map.size();
    if (FacetCounter.prefersDense(matchCount, size)) {
      // dense is tried without waiting: refused, a hash map of what is counted is much smaller
      final FacetMemoryBudget.Reservation dense = budget.reserve(DenseFacetCounter.bytesFor(size), false);
      if (dense != null) {
        granted(dense, FacetExecutionStats.CountingMode.DENSE, size, size);
        counter = new DenseFacetCounter(size);
        return;
      }
    }
    final int expected = (int) Math.min(matchCount, size);
    final FacetMemoryBudget.Reservation hash = budget.reserve(HashFacetCounter.bytesFor(expected), true);
    if (hash == null) {
      unavailable = true;
      return;
    }
    granted(hash, FacetExecutionStats.CountingMode.HASH, size, expected);
    final HashFacetCounter created = new HashFacetCounter(expected);
    counter = created;
    // a single value per record never holds more distinct ordinals than matched records; a list can, up to the cardinality
    if (kind == ColumnKind.ORDINAL_LIST && expected < size)
      growingHash = created;
  }

  private void allocateTermsCounter() {
    final int expected = (int) Math.min(matchCount, NUMERIC_EXPECTED_TERMS);
    final FacetMemoryBudget.Reservation granted = budget.reserve(LongTermsCounter.bytesFor(expected), true);
    if (granted == null) {
      unavailable = true;
      return;
    }
    granted(granted, FacetExecutionStats.CountingMode.HASH, 0, expected);
    terms = new LongTermsCounter(expected);
  }

  private void granted(final FacetMemoryBudget.Reservation granted, final FacetExecutionStats.CountingMode mode, final int size,
      final int distinct) {
    reservedDistinct = distinct;
    cardinality = size;
    stats.counter(mode, granted.bytes(), size);
  }

  /** The ORDINAL_LIST hash counter reached the distinct ordinals reserved: reserves for twice as many, the cardinality at most. */
  private boolean growHash() {
    return reserveDistinct((int) Math.min(cardinality, Math.max(reservedDistinct * 2L, reservedDistinct + 1L)));
  }

  /**
   * Tops the reservation up so it covers the hash counter holding {@code distinct} values.
   *
   * @return false when the budget refused: the facet is then unavailable and every reservation is released
   */
  private boolean reserveDistinct(final int distinct) {
    final long needed = terms != null ? terms.bytesAt(distinct) : growingHash.bytesAt(distinct);
    if (needed > budget.bytes()) {
      final FacetMemoryBudget.Reservation more = budget.reserve(needed - budget.bytes(), true);
      if (more == null) {
        unavailable = true;
        counter = null;
        growingHash = null;
        terms = null;
        release();
        return false;
      }
      stats.toppedUp(more.bytes());
    }
    reservedDistinct = Math.max(reservedDistinct, distinct);
    return true;
  }

  /** The 64-bit term of a double: one key for 0.0 and -0.0 and one for every NaN, as GROUP BY keys them. */
  private static long doubleKey(final double value) {
    return value == 0.0 ? 0L : Double.doubleToLongBits(value);
  }

  private void addForeign(final Object value) {
    if (foreign == null)
      foreign = new HashMap<>();
    foreign.computeIfAbsent(FacetValueComparator.normalize(value), k -> new long[1])[0]++;
  }

  private boolean anyOtherUnavailable() {
    for (final TermsCollector collector : others)
      if (collector.unavailable)
        return true;
    return false;
  }

  private List<TermsCollector> all() {
    final List<TermsCollector> all = new ArrayList<>(others.size() + 1);
    all.add(this);
    all.addAll(others);
    return all;
  }

  /** True when every merged collector counts a column of the same kind and property type as this one. */
  private boolean sameColumnType() {
    for (final TermsCollector collector : others)
      if (collector.kind != kind || collector.propertyType != propertyType)
        return false;
    return true;
  }

  /**
   * Adds this collector's counts by normalized value: the merge across declaring types, whose ordinals are unrelated.
   *
   * @param converted false: a date term stays its epoch count, so it ranks in time order, and {@link #bucketValue} turns it into the
   *                  property's object; true: it is converted here with this collector's own property type, so terms of columns of
   *                  different types merge exactly as GROUP BY groups the values they read back as
   */
  private void addValuesTo(final Map<Object, long[]> counts, final boolean converted) {
    if (counter != null)
      counter.forEachNonZero((ordinal, count) -> counts.computeIfAbsent(map.value(ordinal), k -> new long[1])[0] += count);
    if (terms != null)
      terms.forEachNonZero((key, count) -> counts.computeIfAbsent(converted ? convertedValue(key) : termValue(key), k -> new long[1])[0]
          += count);
    if (foreign != null)
      for (final Map.Entry<Object, long[]> entry : foreign.entrySet())
        counts.computeIfAbsent(entry.getKey(), k -> new long[1])[0] += entry.getValue()[0];
  }

  private Object termValue(final long key) {
    return kind == ColumnKind.DOUBLE ? FacetValueComparator.normalize(Double.longBitsToDouble(key)) : key;
  }

  private boolean isDateColumn() {
    return kind == ColumnKind.LONG && propertyType.isDateOrDateTime();
  }

  /** A term of this collector as the value its property reads back as, normalized: what GROUP BY groups on. */
  private Object convertedValue(final long key) {
    if (isDateColumn())
      return FacetValueComparator.normalize(database.getSerializer().deserializeTemporal(database, key, propertyType.getBinaryType()));
    return termValue(key);
  }

  /** The value a bucket returns: a date epoch term as the object the property reads back as, anything else as counted. */
  private Object bucketValue(final Object key, final boolean converted) {
    if (!converted && isDateColumn() && key instanceof Long epoch)
      return database.getSerializer().deserializeTemporal(database, epoch, propertyType.getBinaryType());
    return key;
  }

  /**
   * An INCLUDE literal as the converted term it matches in this collector (the merged path over columns of different types): its
   * single-type key ({@link #includeKey}), read back as the property's value.
   */
  private Object convertedIncludeKey(final Object value) {
    final Object key = includeKey(value);
    return kind == ColumnKind.LONG && key instanceof Long epoch ? convertedValue(epoch) : key;
  }

  /**
   * An INCLUDE value as the epoch-path key it is counted under: for a LONG column the literal converted as stored values are
   * ({@link ColumnarValueExtractor#toColumnLong}), else, or when it is unconvertible (counted 0), the literal itself, normalized.
   */
  private Object includeKey(final Object value) {
    final Object literal = schemaConverted(value);
    if (kind == ColumnKind.LONG && literal != null && extractor != null)
      try {
        return extractor.toColumnLong(spec.property(), literal);
      } catch (final IllegalArgumentException e) {
        // not a value of the column: counted 0
      }
    return FacetValueComparator.normalize(literal);
  }

  /**
   * An INCLUDE literal converted to the property's type as a WHERE comparing the property with it converts it
   * ({@link Type#convertOrNull}), so INCLUDE {@code '2020-01-01'} on a DATE property matches what {@code WHERE day = '2020-01-01'}
   * matches. The literal itself when it does not convert, and for a list indexed BY ITEM (the column knows no item type).
   */
  private Object schemaConverted(final Object value) {
    return kind == ColumnKind.ORDINAL_LIST ? value : FacetBounds.schemaConverted(database, propertyType, value);
  }

  // ------------------------------------------------------------------------------------------------------------------- ranking

  /** The fast path, one declaring type: ranks merged ordinals and resolves only the returned ones to values. */
  private FacetResult rankOrdinals() {
    final long[] total = new long[1];
    counter.forEachNonZero((ordinal, count) -> total[0] += count);

    final List<FacetBucket> buckets = new ArrayList<>();
    long returned = 0;
    if (hasInclude()) {
      final LinkedHashMap<Object, long[]> included = new LinkedHashMap<>();
      for (final Object value : spec.include()) {
        final Object normalized = FacetValueComparator.normalize(schemaConverted(value));
        final int merged = map.mergedOf(normalized);
        included.putIfAbsent(normalized, new long[] { merged >= 0 && merged < cardinality ? counter.get(merged) : 0L });
      }
      for (final Map.Entry<Object, Long> entry : FacetRanker.topValues(included, included.size(), 0, spec.order())) {
        buckets.add(FacetBucket.term(entry.getKey(), entry.getValue()));
        returned += entry.getValue();
      }
    } else
      for (final int ordinal : FacetRanker.topOrdinals(counter, spec.top(), spec.minCount(), spec.order(), valueOrder())) {
        final long count = counter.get(ordinal);
        buckets.add(FacetBucket.term(map.value(ordinal), count));
        returned += count;
      }
    returned -= addMissingBucket(buckets, missing);
    return FacetResult.of("terms", "index", buckets, total[0] - returned, missing);
  }

  private IntBinaryOperator valueOrder() {
    if (map instanceof CrossBucketOrdinalMap.IdentityOrdinalMap identity)
      return identity.dictionary()::compare;
    final CrossBucketOrdinalMap merged = map;
    final FacetValueComparator values = FacetValueComparator.INSTANCE;
    // the dictionaries hold normalized values
    return (a, b) -> values.compareNormalized(merged.value(a), merged.value(b));
  }

  /** The general path: counts by value (several declaring types, values only patched records have, LONG and DOUBLE terms). */
  private FacetResult rankValues(final Map<Object, long[]> counts, final long missingTotal, final boolean converted) {
    long total = 0;
    for (final long[] count : counts.values())
      total += count[0];

    final List<FacetBucket> buckets = new ArrayList<>();
    long returned = 0;
    final List<Map.Entry<Object, Long>> ranked;
    if (hasInclude()) {
      final LinkedHashMap<Object, long[]> included = new LinkedHashMap<>();
      for (final Object value : spec.include()) {
        if (!converted) {
          final Object key = includeKey(value);
          final long[] count = counts.get(key);
          included.putIfAbsent(key, new long[] { count == null ? 0L : count[0] });
          continue;
        }
        // each collector converts the literal as its own single-type path would, so it matches the same terms there; distinct
        // resulting values are summed under the first one
        final LinkedHashSet<Object> keys = new LinkedHashSet<>();
        for (final TermsCollector collector : all())
          keys.add(collector.convertedIncludeKey(value));
        long matched = 0;
        for (final Object key : keys) {
          final long[] count = counts.get(key);
          matched += count == null ? 0L : count[0];
        }
        included.putIfAbsent(keys.getFirst(), new long[] { matched });
      }
      ranked = FacetRanker.topValues(included, included.size(), 0, spec.order());
    } else
      ranked = FacetRanker.topValues(counts, spec.top(), spec.minCount(), spec.order());
    for (final Map.Entry<Object, Long> entry : ranked) {
      buckets.add(FacetBucket.term(bucketValue(entry.getKey(), converted), entry.getValue()));
      returned += entry.getValue();
    }
    returned -= addMissingBucket(buckets, missingTotal);
    return FacetResult.of("terms", "index", buckets, total - returned, missingTotal);
  }

  private boolean hasInclude() {
    return spec.include() != null && !spec.include().isEmpty();
  }

  /**
   * With MISSING, places the bucket of the records without a value where its count ranks it (null sorts last among equal counts and in
   * a value order). It takes a TOP slot like any value; with INCLUDE it is added on top of the included values.
   *
   * @return the count of the value bucket it displaced past TOP, 0 when none (that count joins {@code otherCount})
   */
  private long addMissingBucket(final List<FacetBucket> buckets, final long missingCount) {
    if (!spec.missingBucket() || missingCount == 0 || missingCount < spec.minCount())
      return 0;
    int at = buckets.size();
    for (int i = 0; i < buckets.size(); i++) {
      final long count = buckets.get(i).count();
      if (spec.order() == FacetOrder.COUNT_DESC ? count < missingCount : spec.order() == FacetOrder.COUNT_ASC && count > missingCount) {
        at = i;
        break;
      }
    }
    final int limit = hasInclude() ? buckets.size() + 1 : spec.top();
    if (at >= limit)
      return 0;
    buckets.add(at, FacetBucket.missing(missingCount));
    return buckets.size() > limit ? buckets.removeLast().count() : 0;
  }
}
