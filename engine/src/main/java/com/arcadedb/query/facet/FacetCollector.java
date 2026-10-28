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
 * {@code segment.live()} itself.
 */
public interface FacetCollector {
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
}
