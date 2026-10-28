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

/**
 * How one facet was computed, for EXPLAIN and PROFILE: the segment views it read or skipped, how it counted, the counting memory it
 * reserved and the source of its result. Filled by the executor and its collectors on the query thread; read once the facet is done.
 */
public final class FacetExecutionStats {
  public enum CountingMode {
    /** An array indexed by merged ordinal. */
    DENSE,
    /** A hash map of the values seen (always for LONG and DOUBLE terms). */
    HASH
  }

  private long         segmentsScanned;
  private long         segmentsSkipped;
  private CountingMode countingMode;
  private long         budgetBytes;
  private int          cardinality;
  private long         counterBytes;
  private String       source;

  /** Segment views (segments and overlays) whose matched live rows were counted. */
  public long segmentsScanned() {
    return segmentsScanned;
  }

  /**
   * Segment views no value was decoded from: those holding no matched live row, and for a range or histogram facet those whose
   * {@code [minLong, maxLong]} lies in one interval or bucket (counted by presence alone).
   */
  public long segmentsSkipped() {
    return segmentsSkipped;
  }

  /** Null until a counter was allocated (no matched bucket, or the budget refused every representation). */
  public CountingMode countingMode() {
    return countingMode;
  }

  /** The counting memory reserved from {@link FacetMemoryBudget}, summed over the declaring types of the facet. */
  public long budgetBytes() {
    return budgetBytes;
  }

  /** The merged ordinals the counter was sized for (0 for LONG and DOUBLE terms), summed over the declaring types. */
  public int cardinality() {
    return cardinality;
  }

  /** The heap the counters held when the facet was ranked, never above {@link #budgetBytes()}. */
  public long counterBytes() {
    return counterBytes;
  }

  /** The {@link FacetResult#source()} of the facet. */
  public String source() {
    return source;
  }

  void segmentScanned() {
    segmentsScanned++;
  }

  void segmentSkipped() {
    segmentsSkipped++;
  }

  void counterBytes(final long bytes) {
    counterBytes += bytes;
  }

  /** More counting memory reserved for a counter that grows (a hash map over more values than it was presized for). */
  void toppedUp(final long bytes) {
    budgetBytes += bytes;
  }

  void counter(final CountingMode mode, final long bytes, final int cardinality) {
    // a facet over several declaring types reports the mode of the first counter it allocated
    if (countingMode == null)
      countingMode = mode;
    budgetBytes += bytes;
    this.cardinality += cardinality;
  }

  /** Counting memory reserved by a collector that has no {@link CountingMode} (the counters of a range or histogram facet). */
  void reserved(final long bytes) {
    budgetBytes += bytes;
  }

  /** Folds the statistics of the collector of another declaring type of the same facet into these. */
  void add(final FacetExecutionStats other) {
    segmentsScanned += other.segmentsScanned;
    segmentsSkipped += other.segmentsSkipped;
    if (countingMode == null)
      countingMode = other.countingMode;
    budgetBytes += other.budgetBytes;
    cardinality += other.cardinality;
    counterBytes += other.counterBytes;
  }

  void source(final String source) {
    this.source = source;
  }

  @Override
  public String toString() {
    return "segmentsScanned=" + segmentsScanned + ", segmentsSkipped=" + segmentsSkipped + ", countingMode=" + countingMode + ", budgetBytes="
        + budgetBytes + ", counterBytes=" + counterBytes + ", cardinality=" + cardinality + ", source=" + source;
  }
}
