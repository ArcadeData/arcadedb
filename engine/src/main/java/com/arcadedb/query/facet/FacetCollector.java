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

import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.PositionBitmap;

/**
 * Counts the values of one facet for one declaring type (the type of {@code getPolymorphicColumnarIndex(property)}, one per
 * hierarchy). The executor drives it bucket by bucket: {@link #beginBucket}, then {@link #collect} for each segment of the snapshot
 * and {@link #collectPatched} for each record the projection cannot answer; {@link #merge} folds the collectors of one facet across
 * declaring types and {@link #finish} ranks the counts and releases the memory the collector reserved.
 * <p>
 * The {@code matches} given to {@link #collect} is the facet's match bitmap AND NOT the patched positions, not ANDed with the live
 * rows: an ORDINAL or ORDINAL_LIST collector hands it to {@code ColumnReader.forEachOrdinal}, which visits only live rows, and counts
 * as missing {@code matches.andCardinality(segment.live())} minus the distinct positions visited; a LONG or DOUBLE collector ANDs
 * {@code segment.live()} itself. Only the collector of a {@link HistogramFacetSpec} may keep the segment and its {@code matches} to
 * count them at {@link #finish} (its bucket boundaries depend on every matched value): the executor keeps those bitmaps, and their
 * memory charge, until it returns, and closes the snapshots only after every facet finished. Any other collector must be done with
 * {@code matches} when {@link #collect} returns: the executor drops a derived bitmap (and gives its memory back) right after.
 */
public interface FacetCollector {
  /**
   * Begins a bucket. A bucket the fallback scan reads ({@link FacetFallbackScanner}) is begun with a null {@code snapshot} and
   * {@code map}: none of its segments is collected, every value of it arrives through {@link #collectPatched}, a dictionary value by
   * value ({@link PatchedValue#unknown}).
   */
  void beginBucket(int bucketId, ColumnarSnapshot snapshot, CrossBucketOrdinalMap map);

  void collect(ColumnarSegmentView segment, PositionBitmap matches);

  void collectPatched(long position, PatchedValue value);

  void merge(FacetCollector other);

  FacetResult finish();

  /**
   * Releases the memory the collector reserved without ranking anything: the executor calls it on every collector of a facet that
   * failed before {@link #finish}. Idempotent, and a no-op after {@link #finish}.
   */
  default void release() {
  }

  /** Whether the collector counts nothing more: the memory budget refused it (or, for a hierarchy, its view is not available). */
  default boolean isUnavailable() {
    return false;
  }

  /**
   * Adds the counts of {@code partial}, a collector of the same facet, column and ordinal map that counted other segments of the same
   * buckets on another thread (parallel counting, {@code arcadedb.columnar.parallelCountTasks}), so the facet is ranked as if this
   * collector had counted them. The caller releases {@code partial} afterwards.
   *
   * @return false when this kind does not add counts that way: the caller then {@link #merge merges} {@code partial} with the facet's
   * other collectors instead
   */
  default boolean absorb(final FacetCollector partial) {
    return false;
  }

  /**
   * How the facet was computed (the merged collectors included), for EXPLAIN and PROFILE; complete once {@link #finish} returned.
   * Null for a collector that keeps none.
   */
  default FacetExecutionStats stats() {
    return null;
  }

  /** The grants of the memory budget that were not immediate, this collector's and the merged ones'. */
  default int budgetWaits() {
    return 0;
  }
}
