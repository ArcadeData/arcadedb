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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds multi-column aggregation results bucketed by timestamp.
 * Supports two modes:
 * <ul>
 *   <li><b>Flat mode</b>: pre-allocated arrays indexed by {@code (bucketTs - firstBucketTs) / bucketIntervalMs}.
 *       Zero HashMap overhead per sample. Used when bucket interval and data range are known.</li>
 *   <li><b>Map mode</b>: HashMap-based fallback for unknown ranges or zero-interval queries.</li>
 * </ul>
 * In flat mode the pre-allocated window is only as good as the caller's range estimate, so a bucket
 * that falls outside it is parked in a lazily created overflow map rather than discarded: an
 * under-sized window then costs a little speed, never a sample (issue #6937).
 * <p>
 * <b>Exact requests</b> (issues #9718, #9720). A request over a column that stores exact integers - an integral, boolean or
 * datetime column, see {@link ColumnDefinition#storesExactIntegers()} - can be declared EXACT at construction. Its MIN/MAX is
 * then kept as a {@code long} and its SUM/AVG total as a 128-bit integer ({@link ExactLongSum}), fed by the {@code long}
 * overloads of the accumulation methods, and read back through {@link #getExactValue}: a double holds every integer only up
 * to 2^53, so the double lane answered a {@code LONG} column of epoch nanoseconds, 64-bit ids or hashes with a rounded value.
 * {@link #getValue} still answers such a request, as the nearest double.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class MultiColumnAggregationResult {

  /** Maximum number of buckets allowed in flat mode before falling back to map mode. */
  static final int MAX_FLAT_BUCKETS = 10_000_000;

  private final int               requestCount;
  private final AggregationType[] types;
  /** Per request, whether it is exact (see the class comment); {@code null} when none is. */
  private final boolean[]         exact;

  // --- Map mode (fallback) ---
  private final Map<Long, double[]> valuesByBucket;
  private final Map<Long, long[]>   countsByBucket;
  // Per bucket, two longs per request for the exact lane: the MIN/MAX value or the low word of the SUM/AVG total, then its
  // high word. Only when a request is exact.
  private final Map<Long, long[]>   exactByBucket;
  private final List<Long>          orderedBuckets;

  // --- Flat mode ---
  private final boolean   flatMode;
  private final long      firstBucketTs;
  private final long      bucketIntervalMs;
  private final int       maxBuckets;
  private       double[][] flatValues;   // [bucketIdx][requestIdx]
  private       long[][]   flatCounts;   // [bucketIdx][requestIdx]
  private       long[][]   flatExact;    // [bucketIdx][2 * requestIdx (+1)], only when a request is exact
  private       boolean[]  bucketUsed;   // whether this bucket has been touched
  private       List<Long> cachedBucketTimestamps; // cached result for flat mode

  // --- Flat-mode overflow (issue #6937) ---
  // Sizing the flat array is a caller-side estimate of the data range. When it comes up short - the
  // mutable bucket growing past the sealed maximum while the aggregation runs, for instance - the
  // samples that fall outside the pre-allocated window land here instead of being silently dropped.
  // Lazily allocated, so a correctly sized flat result pays nothing.
  private Map<Long, double[]> overflowValues;
  private Map<Long, long[]>   overflowCounts;
  private Map<Long, long[]>   overflowExact;

  // The bucket the last resolve() found or created: its three arrays. A result is filled by one thread at a time.
  private double[] curValues;
  private long[]   curCounts;
  private long[]   curExact;
  // The timestamp the cursor above stands for, meaningful once curValues is set. The arrays of a bucket are created once and
  // never replaced, so while the next sample lands in the same bucket (the samples of a series arrive in time order, so for
  // most of them it does) the cursor is still the answer and resolve() does not divide, probe or box again (issue #10003).
  private long     curBucketTs;

  // --- Response ceiling (issue #7724) ---
  // The number of buckets the caller will accept, 0 for no ceiling, and the running count of the buckets
  // something has actually landed in. A scan asks isOverBucketCeiling() as it goes and stops the moment the
  // answer can no longer be sent, so an oversized request costs the work of reaching the ceiling rather than
  // the work of computing an answer that is thrown away. In flat mode the count cannot be read off
  // bucketUsed[] without walking it, hence the counter; in map mode orderedBuckets already is the count.
  private int bucketCeiling;
  private int flatUsedBuckets;

  /**
   * Map-mode constructor (original behavior).
   */
  public MultiColumnAggregationResult(final List<MultiColumnAggregationRequest> requests) {
    this(requests, (boolean[]) null);
  }

  /**
   * Map-mode constructor with exact requests.
   *
   * @param exact per request, whether it is exact (see the class comment); {@code null} for none
   */
  public MultiColumnAggregationResult(final List<MultiColumnAggregationRequest> requests, final boolean[] exact) {
    this.requestCount = requests.size();
    this.types = new AggregationType[requestCount];
    for (int i = 0; i < requestCount; i++)
      types[i] = requests.get(i).type();
    this.exact = normalizeExact(exact, types);
    this.valuesByBucket = new HashMap<>();
    this.countsByBucket = new HashMap<>();
    this.exactByBucket = this.exact != null ? new HashMap<>() : null;
    this.orderedBuckets = new ArrayList<>();
    this.flatMode = false;
    this.firstBucketTs = 0;
    this.bucketIntervalMs = 0;
    this.maxBuckets = 0;
  }

  /**
   * Flat-mode constructor. Pre-allocates arrays for direct-index access.
   * If {@code maxBuckets} exceeds {@link #MAX_FLAT_BUCKETS}, falls back to map mode
   * to avoid excessive memory allocation.
   *
   * @param requests         aggregation request definitions
   * @param firstBucketTs    timestamp of the first bucket (aligned to interval)
   * @param bucketIntervalMs bucket width in ms (must be > 0)
   * @param maxBuckets       number of buckets to pre-allocate
   */
  public MultiColumnAggregationResult(final List<MultiColumnAggregationRequest> requests,
      final long firstBucketTs, final long bucketIntervalMs, final int maxBuckets) {
    this(requests, firstBucketTs, bucketIntervalMs, maxBuckets, null);
  }

  /**
   * Flat-mode constructor with exact requests.
   *
   * @param exact per request, whether it is exact (see the class comment); {@code null} for none
   */
  public MultiColumnAggregationResult(final List<MultiColumnAggregationRequest> requests,
      final long firstBucketTs, final long bucketIntervalMs, final int maxBuckets, final boolean[] exact) {
    this.requestCount = requests.size();
    this.types = new AggregationType[requestCount];
    for (int i = 0; i < requestCount; i++)
      types[i] = requests.get(i).type();
    this.exact = normalizeExact(exact, types);

    if (maxBuckets > MAX_FLAT_BUCKETS) {
      // Fall back to map mode to avoid OOM
      this.flatMode = false;
      this.firstBucketTs = 0;
      this.bucketIntervalMs = 0;
      this.maxBuckets = 0;
      this.valuesByBucket = new HashMap<>();
      this.countsByBucket = new HashMap<>();
      this.exactByBucket = this.exact != null ? new HashMap<>() : null;
      this.orderedBuckets = new ArrayList<>();
    } else {
      this.flatMode = true;
      this.firstBucketTs = firstBucketTs;
      this.bucketIntervalMs = bucketIntervalMs;
      this.maxBuckets = maxBuckets;
      this.flatValues = new double[maxBuckets][];
      this.flatCounts = new long[maxBuckets][];
      this.flatExact = this.exact != null ? new long[maxBuckets][] : null;
      this.bucketUsed = new boolean[maxBuckets];
      this.valuesByBucket = null;
      this.countsByBucket = null;
      this.exactByBucket = null;
      this.orderedBuckets = null;
    }
  }

  /**
   * The exact flags as kept: {@code null} when no request is exact, and never for a COUNT, which counts rows whatever the column.
   */
  private static boolean[] normalizeExact(final boolean[] exact, final AggregationType[] types) {
    if (exact == null)
      return null;
    boolean any = false;
    final boolean[] kept = new boolean[types.length];
    for (int i = 0; i < types.length && i < exact.length; i++) {
      kept[i] = exact[i] && types[i] != AggregationType.COUNT;
      any |= kept[i];
    }
    return any ? kept : null;
  }

  /**
   * Whether request {@code requestIndex} is exact (see the class comment).
   */
  public boolean isExact(final int requestIndex) {
    return exact != null && exact[requestIndex];
  }

  /**
   * Refuses to fold a double into an exact request (code review on PR #9730): a double holds every integer only up to 2^53, so a
   * caller that reached an exact result through a double-only overload would lose exactly what the exact lane is for, silently.
   */
  private void requireLongs(final boolean longsGiven, final int requestIndex) {
    if (!longsGiven && isExact(requestIndex))
      throw new IllegalStateException(
          "Aggregation request " + requestIndex + " is exact: fold it through an accumulation overload that takes its long value");
  }

  private void requireLongs(final boolean longsGiven) {
    if (!longsGiven && exact != null)
      for (int i = 0; i < requestCount; i++)
        requireLongs(false, i);
  }

  /**
   * Returns whether this result uses flat array mode.
   */
  public boolean isFlatMode() {
    return flatMode;
  }

  // ---- Accumulation methods ----

  /**
   * Accumulates a value for request at the given index into the given bucket.
   */
  public void accumulate(final long bucketTs, final int requestIndex, final double value) {
    requireLongs(false, requestIndex);
    resolve(bucketTs);
    accumulateInPlace(curValues, curCounts, curExact, requestIndex, value, 0L);
  }

  /**
   * Batch accumulate for all requests in a single row.
   */
  public void accumulateRow(final long bucketTs, final double[] values) {
    accumulateRow(bucketTs, values, null);
  }

  /**
   * Batch accumulate for all requests in a single row, with the exact value of every exact request.
   *
   * @param values      per request, the sample as a double: {@link TimeSeriesNaN#ABSENT} when the row carries no value for it
   * @param exactValues per request, the sample as a {@code long}, read for an exact request whose double is not absent; may be
   *                    {@code null} when no request is exact
   */
  public void accumulateRow(final long bucketTs, final double[] values, final long[] exactValues) {
    requireLongs(exactValues != null);
    resolve(bucketTs);
    for (int i = 0; i < requestCount; i++)
      accumulateInPlace(curValues, curCounts, curExact, i, values[i], exactValues != null ? exactValues[i] : 0L);
  }

  /**
   * Accumulates block-level statistics for all requests in a single call.
   *
   * @param sampleCounts per request, the number of samples that contributed to {@code values[i]}: the block's
   *                     rows for a COUNT request, its real (non-NaN) samples for every other one
   */
  public void accumulateBlockStats(final long bucketTs, final double[] values, final long[] sampleCounts) {
    accumulateBlockStats(bucketTs, values, sampleCounts, null, null);
  }

  /**
   * Same as above with the exact statistics of every exact request (see the class comment).
   *
   * @param exactValues per request, the exact MIN/MAX, or the low word of the exact SUM; {@code null} when no request is exact
   * @param exactHigh   per request, the high word of the exact SUM; {@code null} when no request is exact
   */
  public void accumulateBlockStats(final long bucketTs, final double[] values, final long[] sampleCounts, final long[] exactValues,
      final long[] exactHigh) {
    requireLongs(exactValues != null && exactHigh != null);
    resolve(bucketTs);
    for (int i = 0; i < requestCount; i++)
      accumulateStatInPlace(curValues, curCounts, curExact, i, values[i], sampleCounts[i], exactValues != null ? exactValues[i] : 0L,
          exactHigh != null ? exactHigh[i] : 0L);
  }

  /**
   * Accumulates a single statistic result for one request at the given bucket.
   * Used by vectorized (SIMD) segment accumulation where each aggregation type
   * is computed separately per segment.
   *
   * @param bucketTs     aligned bucket timestamp
   * @param requestIndex which aggregation request this applies to
   * @param value        the aggregated value (sum, min, max, or count)
   * @param count        number of samples that produced this value
   */
  public void accumulateSingleStat(final long bucketTs, final int requestIndex,
      final double value, final long count) {
    requireLongs(false, requestIndex);
    resolve(bucketTs);
    accumulateStatInPlace(curValues, curCounts, curExact, requestIndex, value, count, 0L, 0L);
  }

  /**
   * Same as above for an exact request: {@code exactValue} is the MIN/MAX, or the low word of the SUM, and {@code exactHigh} the
   * high word of the SUM.
   */
  public void accumulateSingleStat(final long bucketTs, final int requestIndex, final double value, final long count,
      final long exactValue, final long exactHigh) {
    resolve(bucketTs);
    accumulateStatInPlace(curValues, curCounts, curExact, requestIndex, value, count, exactValue, exactHigh);
  }

  /**
   * Points {@link #curValues}/{@link #curCounts}/{@link #curExact} at the bucket {@code bucketTs} lands in, creating it on first
   * use: a flat slot, an overflow entry for a flat result's out-of-window bucket (issue #6937), or a map entry.
   */
  private void resolve(final long bucketTs) {
    if (curValues != null && bucketTs == curBucketTs)
      return;
    resolveBucket(bucketTs);
    curBucketTs = bucketTs;
  }

  /** The lookup {@link #resolve} skips while the cursor still stands for {@code bucketTs}. */
  private void resolveBucket(final long bucketTs) {
    if (flatMode) {
      final int idx = flatIndex(bucketTs);
      if (ensureFlatBucket(idx)) {
        curValues = flatValues[idx];
        curCounts = flatCounts[idx];
        curExact = flatExact != null ? flatExact[idx] : null;
      } else {
        curValues = overflowValuesFor(bucketTs);
        curCounts = overflowCounts.get(bucketTs);
        curExact = overflowExact != null ? overflowExact.get(bucketTs) : null;
      }
      return;
    }
    double[] vals = valuesByBucket.get(bucketTs);
    if (vals == null) {
      vals = newInitializedValues();
      valuesByBucket.put(bucketTs, vals);
      countsByBucket.put(bucketTs, new long[requestCount]);
      if (exactByBucket != null)
        exactByBucket.put(bucketTs, newExactValues());
      orderedBuckets.add(bucketTs);
    }
    curValues = vals;
    curCounts = countsByBucket.get(bucketTs);
    curExact = exactByBucket != null ? exactByBucket.get(bucketTs) : null;
  }

  /**
   * The value array of an existing bucket, {@code null} when the bucket does not exist. A lookup only: the read methods use it, so
   * that reading a result never moves the cursor {@link #resolve} keeps for the accumulation.
   */
  private double[] valuesOf(final long bucketTs) {
    if (flatMode) {
      final int idx = flatIndex(bucketTs);
      if (idx >= 0 && idx < maxBuckets && bucketUsed[idx])
        return flatValues[idx];
      return overflowValues != null ? overflowValues.get(bucketTs) : null;
    }
    return valuesByBucket.get(bucketTs);
  }

  /** The count array of an existing bucket, see {@link #valuesOf}. */
  private long[] countsOf(final long bucketTs) {
    if (flatMode) {
      final int idx = flatIndex(bucketTs);
      if (idx >= 0 && idx < maxBuckets && bucketUsed[idx])
        return flatCounts[idx];
      return overflowCounts != null ? overflowCounts.get(bucketTs) : null;
    }
    return countsByBucket.get(bucketTs);
  }

  /** The exact array of an existing bucket, see {@link #valuesOf}; {@code null} too when no request is exact. */
  private long[] exactOf(final long bucketTs) {
    if (exact == null)
      return null;
    if (flatMode) {
      final int idx = flatIndex(bucketTs);
      if (idx >= 0 && idx < maxBuckets && bucketUsed[idx])
        return flatExact[idx];
      return overflowExact != null ? overflowExact.get(bucketTs) : null;
    }
    return exactByBucket.get(bucketTs);
  }

  // ---- Finalize & query ----

  /**
   * Finalizes AVG accumulators by dividing accumulated sums by their counts - the counts of REAL samples, so a
   * bucket whose every sample was NaN keeps the {@link TimeSeriesNaN#ABSENT} its sum already is rather than
   * dividing by zero (issue #7089).
   */
  public void finalizeAvg() {
    if (flatMode) {
      for (int i = 0; i < requestCount; i++) {
        // an exact AVG is divided when it is read: its total is the exact lane's, and the double lane holds nothing for it
        if (types[i] == AggregationType.AVG && !isExact(i)) {
          for (int b = 0; b < maxBuckets; b++) {
            if (bucketUsed[b] && flatCounts[b][i] > 0)
              flatValues[b][i] = flatValues[b][i] / flatCounts[b][i];
          }
          if (overflowValues != null)
            for (final Map.Entry<Long, double[]> entry : overflowValues.entrySet()) {
              final long[] counts = overflowCounts.get(entry.getKey());
              if (counts[i] > 0)
                entry.getValue()[i] = entry.getValue()[i] / counts[i];
            }
        }
      }
    } else {
      for (int i = 0; i < requestCount; i++) {
        if (types[i] == AggregationType.AVG && !isExact(i)) {
          for (final Map.Entry<Long, double[]> entry : valuesByBucket.entrySet()) {
            final long[] counts = countsByBucket.get(entry.getKey());
            if (counts[i] > 0)
              entry.getValue()[i] = entry.getValue()[i] / counts[i];
          }
        }
      }
    }
  }

  /**
   * Returns bucket timestamps in order.
   */
  public List<Long> getBucketTimestamps() {
    if (flatMode) {
      if (cachedBucketTimestamps == null) {
        final List<Long> result = new ArrayList<>();
        for (int b = 0; b < maxBuckets; b++)
          if (bucketUsed[b])
            result.add(firstBucketTs + (long) b * bucketIntervalMs);
        if (overflowValues != null && !overflowValues.isEmpty()) {
          // Overflow buckets sit outside the pre-allocated window on either side, so the merged list
          // has to be re-sorted to keep the ascending-timestamp contract callers rely on.
          result.addAll(overflowValues.keySet());
          Collections.sort(result);
        }
        cachedBucketTimestamps = result;
      }
      return cachedBucketTimestamps;
    }
    return orderedBuckets;
  }

  /**
   * The MIN/MAX accumulator is returned as it stands: {@link TimeSeriesNaN#ABSENT} when no non-NaN sample ever
   * reached this request in this bucket, and the real extreme otherwise. There is deliberately no
   * "was the accumulator ever touched" guard here any more - the seed carries that answer itself, which is what
   * issues #4596 and #7043 needed and what a count-based guard could not give (a NaN sample increments the count
   * without contributing a value).
   * <p>
   * SUM and AVG answer the same way (issue #7089), and answer it whether the request was offered only absent
   * samples or offered none at all: the two are one question here, decided in issue #7694.
   * <p>
   * A bucket that does not EXIST answers the same way too (issue #7698), rather than the {@code 0.0} this used to
   * hand back from both modes. "There is no measurement here" is one question, and it had two answers: a bucket
   * that exists with nothing real in it said absent, a bucket nothing ever landed in said zero - for SUM and MIN
   * alike, the arm predating the NaN-as-absent policy of #4596/#7043/#7089 entirely. No reader in {@code src/main}
   * could reach it, because all four iterate {@link #getBucketTimestamps()} and ask only for what it returned; the
   * fifth reader is the one this is for - a caller that computes bucket timestamps from the REQUEST range instead,
   * which is the natural way to fill the gaps of a fixed-step series, and which could not tell a missing bucket
   * from a real total of zero. COUNT keeps answering {@code 0}, the way {@code COUNT(*)} does over no rows, and
   * for the same reason {@link #newInitializedValues()} seeds it with a number.
   */
  public double getValue(final long bucketTs, final int requestIndex) {
    if (!isExact(requestIndex)) {
      final double[] vals = valuesOf(bucketTs);
      return vals != null ? vals[requestIndex] : noBucketValue(requestIndex);
    }
    // An exact request, as the nearest double of the exact answer
    final Number value = getExactValue(bucketTs, requestIndex);
    return value != null ? value.doubleValue() : TimeSeriesNaN.ABSENT;
  }

  /**
   * The exact answer of an exact request in a bucket (issues #9718, #9720), or {@code null} when no sample contributed to it
   * (or the bucket does not exist): MIN/MAX as the {@link Long} sample, SUM as a {@link Long} when the total fits one and an exact
   * {@link java.math.BigDecimal} otherwise, AVG as the {@link Double} the generic {@code avg()} computes from that total.
   *
   * @throws IllegalStateException when the request is not exact
   */
  public Number getExactValue(final long bucketTs, final int requestIndex) {
    if (!isExact(requestIndex))
      throw new IllegalStateException("Aggregation request " + requestIndex + " is not exact");
    final long[] counts = countsOf(bucketTs);
    if (counts == null || counts[requestIndex] == 0)
      return null;
    final long[] ex = exactOf(bucketTs);
    final long value = ex[requestIndex * 2];
    return switch (types[requestIndex]) {
      case MIN, MAX -> value;
      case SUM -> ExactLongSum.toNumber(value, ex[requestIndex * 2 + 1]);
      case AVG -> ExactLongSum.average(value, ex[requestIndex * 2 + 1], counts[requestIndex]);
      case COUNT -> counts[requestIndex];
    };
  }

  /**
   * What a request answers for a bucket that does not exist: the same thing an untouched accumulator in a bucket
   * that DOES exist answers, so the two cannot be told apart by a reader that has no business telling them apart
   * (issue #7698). One expression of the rule, read by both modes, so the map arm and the two flat arms cannot
   * drift.
   */
  private double noBucketValue(final int requestIndex) {
    return types[requestIndex] == AggregationType.COUNT ? 0.0 : TimeSeriesNaN.ABSENT;
  }

  /**
   * The number of samples that CONTRIBUTED to this request in this bucket: the rows for a COUNT request, and the
   * REAL (non-NaN) samples for every other one - what AVG divides by (issue #7089).
   * <p>
   * It used to count NaN samples too, which is precisely what made the old {@code counts[i] == 0} guard miss an
   * all-NaN bucket and leak the MIN/MAX sentinel (issue #7043). The value still carries its own "no data" answer:
   * {@code TimeSeriesNaN.isAbsent(getValue(bucketTs, requestIndex))} is the test for MIN/MAX/SUM/AVG alike.
   */
  public long getCount(final long bucketTs, final int requestIndex) {
    if (flatMode) {
      final int idx = flatIndex(bucketTs);
      if (idx >= 0 && idx < maxBuckets && bucketUsed[idx])
        return flatCounts[idx][requestIndex];
      final long[] overflow = overflowCounts != null ? overflowCounts.get(bucketTs) : null;
      return overflow != null ? overflow[requestIndex] : 0;
    }
    final long[] counts = countsByBucket.get(bucketTs);
    return counts != null ? counts[requestIndex] : 0;
  }

  public int size() {
    return getUsedBucketCount();
  }

  /**
   * The number of buckets something has landed in, which is the number of rows
   * {@link #getBucketTimestamps()} will hand back. O(1) in both modes.
   */
  public int getUsedBucketCount() {
    if (flatMode)
      return flatUsedBuckets + (overflowValues != null ? overflowValues.size() : 0);
    return orderedBuckets.size();
  }

  /**
   * Declares the largest number of buckets the caller can accept, {@code <= 0} for no ceiling (issue #7724).
   * <p>
   * A bound on the WORK, not a truncation: a result that trips the ceiling is incomplete by construction and
   * exists only to be refused. Every surface that sets one already refuses a bucket count above it after the
   * fact - {@code /ts/query}, the Grafana query route, the gRPC bucket stream - and that refusal is what still
   * answers the caller, unchanged in wording. What changes is its price: the scan stops as soon as the count
   * passes the ceiling instead of visiting the whole range first. Because the scan stops only once the count is
   * ALREADY above the ceiling, the after-the-fact check necessarily fires, so no caller can receive a partial
   * result.
   */
  public void setBucketCeiling(final int bucketCeiling) {
    this.bucketCeiling = bucketCeiling;
  }

  /**
   * Whether this result has passed the ceiling {@link #setBucketCeiling(int)} declared, i.e. whether the scan
   * filling it is now doing work that can only end in a refusal. Always false when no ceiling was declared.
   */
  public boolean isOverBucketCeiling() {
    return bucketCeiling > 0 && getUsedBucketCount() > bucketCeiling;
  }

  /**
   * Merges another result into this one. Both must use flat mode with
   * the same firstBucketTs, bucketIntervalMs, and maxBuckets.
   */
  public void mergeFrom(final MultiColumnAggregationResult other) {
    if (flatMode && other.flatMode) {
      if (firstBucketTs != other.firstBucketTs || bucketIntervalMs != other.bucketIntervalMs || maxBuckets != other.maxBuckets)
        throw new IllegalArgumentException(
            "Cannot merge incompatible flat-mode results: firstBucketTs=" + firstBucketTs + "/" + other.firstBucketTs
                + " bucketIntervalMs=" + bucketIntervalMs + "/" + other.bucketIntervalMs
                + " maxBuckets=" + maxBuckets + "/" + other.maxBuckets);
      for (int b = 0; b < other.maxBuckets; b++) {
        if (!other.bucketUsed[b])
          continue;
        ensureFlatBucket(b);
        final double[] oVals = other.flatValues[b];
        final long[] oCounts = other.flatCounts[b];
        final double[] tVals = flatValues[b];
        final long[] tCounts = flatCounts[b];
        final long[] oExact = other.flatExact != null ? other.flatExact[b] : null;
        final long[] tExact = flatExact != null ? flatExact[b] : null;
        for (int i = 0; i < requestCount; i++) {
          if (isExact(i)) {
            mergeExact(tExact, tCounts, oExact, oCounts[i], i);
            tCounts[i] += oCounts[i];
            continue;
          }
          switch (types[i]) {
          case MIN:
            tVals[i] = TimeSeriesNaN.min(tVals[i], oVals[i]);
            break;
          case MAX:
            tVals[i] = TimeSeriesNaN.max(tVals[i], oVals[i]);
            break;
          case SUM:
          case AVG:
            tVals[i] = TimeSeriesNaN.mergeSum(tVals[i], tCounts[i], oVals[i], oCounts[i]);
            break;
          case COUNT:
            tVals[i] += oVals[i];
            break;
          }
          tCounts[i] += oCounts[i];
        }
      }
      // Buckets the other result had to park in its overflow map (issue #6937) still belong in the
      // merged total. Both sides share firstBucketTs/bucketIntervalMs/maxBuckets (asserted above), so
      // flatIndex() gives the same answer here and they necessarily re-overflow into this result's own
      // map; going through the by-timestamp accumulator is what puts them there and merges duplicates.
      if (other.overflowValues != null)
        for (final Map.Entry<Long, double[]> entry : other.overflowValues.entrySet())
          mergeBucket(entry.getKey(), entry.getValue(), other.overflowCounts.get(entry.getKey()),
              other.overflowExact != null ? other.overflowExact.get(entry.getKey()) : null);
    } else {
      // Fallback: merge map-mode results, bucket by bucket, from the other result's own arrays
      for (final long bucketTs : other.getBucketTimestamps())
        mergeBucket(bucketTs, other.valuesOf(bucketTs), other.countsOf(bucketTs), other.exactOf(bucketTs));
    }
  }

  /**
   * Folds one bucket of another result into this one: its statistics for every request, exact ones through the exact lane.
   */
  private void mergeBucket(final long bucketTs, final double[] oVals, final long[] oCounts, final long[] oExact) {
    resolve(bucketTs);
    for (int i = 0; i < requestCount; i++) {
      if (isExact(i)) {
        mergeExact(curExact, curCounts, oExact, oCounts[i], i);
        curCounts[i] += oCounts[i];
      } else
        accumulateStatInPlace(curValues, curCounts, curExact, i, oVals[i], oCounts[i], 0L, 0L);
    }
  }

  /**
   * Folds the exact accumulator of request {@code i} of another bucket, which {@code otherCount} samples contributed to, into
   * {@code target}, whose count has not been updated yet.
   */
  private void mergeExact(final long[] target, final long[] targetCounts, final long[] other, final long otherCount, final int i) {
    if (otherCount == 0 || other == null)
      return;
    foldExact(target, targetCounts[i], i, other[i * 2], other[i * 2 + 1]);
  }

  /**
   * Folds a statistic into the exact accumulator of request {@code i}, which {@code countSoFar} samples contributed to until now:
   * the extreme for a MIN/MAX, the 128-bit total {@code (value, high)} for a SUM/AVG.
   */
  private void foldExact(final long[] target, final long countSoFar, final int i, final long value, final long high) {
    final int at = i * 2;
    switch (types[i]) {
    case MIN -> target[at] = countSoFar == 0 ? value : Math.min(target[at], value);
    case MAX -> target[at] = countSoFar == 0 ? value : Math.max(target[at], value);
    case SUM, AVG -> ExactLongSum.add(target, at, value, high);
    default -> {
    }
    }
  }

  /**
   * Number of buckets that had to be parked in the flat-mode overflow map because they fell outside
   * the pre-allocated window (issue #6937). The results are correct either way; a non-zero value means
   * the caller's range estimate came up short, which is what used to lose samples silently. Read by
   * {@code TimeSeriesEngine.aggregateMulti()} into {@link AggregationMetrics#addOverflowBuckets(int)},
   * and by the tests that assert the sizing itself is right rather than merely rescued.
   *
   * @return the overflow bucket count, always 0 in map mode
   */
  int getOverflowBucketCount() {
    return overflowValues != null ? overflowValues.size() : 0;
  }

  // ---- Internal helpers ----

  int getRequestCount() {
    return requestCount;
  }

  AggregationType[] getTypes() {
    return types;
  }

  /**
   * Returns (creating on first use) the overflow value array for a bucket that falls outside the
   * pre-allocated flat window. The matching {@link #overflowCounts} entry is created alongside it,
   * so callers can read it straight after this call.
   */
  private double[] overflowValuesFor(final long bucketTs) {
    if (overflowValues == null) {
      overflowValues = new HashMap<>();
      overflowCounts = new HashMap<>();
      if (exact != null)
        overflowExact = new HashMap<>();
    }
    double[] vals = overflowValues.get(bucketTs);
    if (vals == null) {
      vals = newInitializedValues();
      overflowValues.put(bucketTs, vals);
      overflowCounts.put(bucketTs, new long[requestCount]);
      if (overflowExact != null)
        overflowExact.put(bucketTs, newExactValues());
      cachedBucketTimestamps = null; // invalidate cache
    }
    return vals;
  }

  private int flatIndex(final long bucketTs) {
    final long idx = Math.floorDiv(bucketTs - firstBucketTs, bucketIntervalMs);
    if (idx < 0 || idx >= maxBuckets)
      return -1;
    return (int) idx; // safe: idx < maxBuckets which is an int
  }

  private boolean ensureFlatBucket(final int idx) {
    if (idx < 0 || idx >= maxBuckets)
      return false;
    if (!bucketUsed[idx]) {
      bucketUsed[idx] = true;
      flatUsedBuckets++;
      flatValues[idx] = newInitializedValues();
      flatCounts[idx] = new long[requestCount];
      if (flatExact != null)
        flatExact[idx] = newExactValues();
      cachedBucketTimestamps = null; // invalidate cache
    }
    return true;
  }

  /**
   * MIN/MAX/SUM/AVG accumulators start at {@link TimeSeriesNaN#ABSENT}, not at a {@code ±Double.MAX_VALUE}
   * sentinel or at zero.
   * <p>
   * The sentinel was the whole defect of issue #7043: it is a finite double, so nothing downstream could tell an
   * untouched accumulator from a real extreme, and the guard that tried to (a {@code counts[i] == 0} test) keyed
   * off the raw sample count - which a NaN sample increments. An all-NaN bucket therefore looked touched and
   * handed {@code Double.MAX_VALUE} back as data. With the absent marker as the seed and
   * {@link TimeSeriesNaN#min}/{@link TimeSeriesNaN#max} as the fold, "nothing real arrived" IS the value, and no
   * side-channel is needed to recover it. SUM/AVG follow since issue #7089: a zero seed cannot tell "nothing real
   * arrived" from "it all added up to zero", and the {@code +=} it fed turned one NaN sample into a NaN total.
   * <p>
   * SUM keeps the absent seed too, rather than the additive identity issue #7506 gave it (issue #7694). That seed
   * existed so a request nothing was ever offered in a bucket could answer the empty sum instead of an absence -
   * a state no QUERY produces: every call site in {@code src/main} goes through {@link #accumulateRow} or
   * {@link #accumulateBlockStats}, which take one value per request, or loops {@link #accumulateSingleStat} over
   * all of them, so a bucket that exists has been offered to every request. The single-request
   * {@link #accumulate(long, int, double)} can still construct it, and the tests do; what it cannot do is come
   * from a query. The distinction also disagreed with SQL's {@code SUM}, which is NULL over an empty group and
   * over an all-NULL one alike. COUNT is the one accumulator seeded with a number, counting rows as
   * {@code COUNT(*)} does.
   */
  private double[] newInitializedValues() {
    final double[] vals = new double[requestCount];
    for (int i = 0; i < requestCount; i++)
      vals[i] = types[i] == AggregationType.COUNT ? 0.0 : TimeSeriesNaN.ABSENT;
    return vals;
  }

  private long[] newExactValues() {
    return new long[requestCount * 2];
  }

  /**
   * Folds one sample of request {@code idx}: {@code value} as a double, {@link TimeSeriesNaN#ABSENT} when the row has none, and
   * {@code exactValue} as the {@code long} an exact request folds instead.
   */
  private void accumulateInPlace(final double[] vals, final long[] counts, final long[] ex, final int idx, final double value,
      final long exactValue) {
    if (types[idx] == AggregationType.COUNT) {
      vals[idx] += 1;
      counts[idx]++;
      return;
    }
    if (isExact(idx)) {
      if (TimeSeriesNaN.isAbsent(value))
        return;
      foldExact(ex, counts[idx], idx, exactValue, exactValue >> 63);
      counts[idx]++;
      return;
    }
    switch (types[idx]) {
    case SUM:
    case AVG:
      vals[idx] = TimeSeriesNaN.sum(vals[idx], counts[idx], value);
      break;
    case MIN:
      vals[idx] = TimeSeriesNaN.min(vals[idx], value);
      break;
    case MAX:
      vals[idx] = TimeSeriesNaN.max(vals[idx], value);
      break;
    default:
      break;
    }
    counts[idx] = TimeSeriesNaN.countIfPresent(counts[idx], value);
  }

  /**
   * Folds a statistic of {@code count} samples into request {@code requestIndex}: {@code value} for the double lane, and the exact
   * {@code (exactValue, exactHigh)} for an exact request, which a statistic of no sample leaves untouched.
   */
  private void accumulateStatInPlace(final double[] vals, final long[] counts, final long[] ex,
      final int requestIndex, final double value, final long count, final long exactValue, final long exactHigh) {
    if (isExact(requestIndex)) {
      if (count > 0)
        foldExact(ex, counts[requestIndex], requestIndex, exactValue, exactHigh);
      counts[requestIndex] += count;
      return;
    }
    switch (types[requestIndex]) {
    case MIN:
      vals[requestIndex] = TimeSeriesNaN.min(vals[requestIndex], value);
      break;
    case MAX:
      vals[requestIndex] = TimeSeriesNaN.max(vals[requestIndex], value);
      break;
    case SUM:
    case AVG:
      vals[requestIndex] = TimeSeriesNaN.mergeSum(vals[requestIndex], counts[requestIndex], value, count);
      break;
    case COUNT:
      vals[requestIndex] += value;
      break;
    }
    counts[requestIndex] += count;
  }
}
