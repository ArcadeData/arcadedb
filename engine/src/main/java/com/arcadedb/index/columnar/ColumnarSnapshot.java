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
package com.arcadedb.index.columnar;

import java.util.List;

/**
 * A frozen, consistent view of one bucket's projection, acquired by a query: the segments and overlays it lists, their dead sets and
 * the dictionary generation they are encoded with never change while it is held, whatever the apply thread, a flush, a merge or a
 * compaction publishes meanwhile. Ref-counted: no file it reads is deleted before {@link #close()}.
 */
public interface ColumnarSnapshot extends AutoCloseable {
  int bucketId();

  /** The segments, then the overlay views (the frozen overlay being flushed first, then the live overlay) as the last ones. */
  List<ColumnarSegmentView> segments();

  /** Positions on dirty, not yet applied pages: the input of patching. */
  PositionBitmap pendingPositions();

  /** Start time of the oldest pending epoch, 0 when caught up. */
  long oldestPendingEpochStartMs();

  /** False while the projection is missing or rebuilding. */
  boolean isUsable();

  /**
   * The dictionary the snapshot's segments and overlays of the property are encoded with: decode with this generation, never with
   * {@code ColumnarProjection.dictionary}, which answers the current one. Null when the property has no dictionary (a LONG or DOUBLE
   * column, or no such column).
   */
  GlobalDictionary dictionary(String property);

  /**
   * True when the snapshot is usable and the manifest of its state publishes the property's column with the definition the projection
   * had for it when the snapshot was acquired ({@code ColumnarReconciler.sameData}, the property name aside). False for a column added or
   * retyped after the last publish (its {@code create} or {@code schema} rebuild window) and for a property the projection has no
   * column of: a reader answers such a property from the records instead.
   */
  boolean publishes(String property);

  /**
   * The definition the snapshot's segments of the property were written with, when it {@link #publishes} the property; null otherwise.
   * Captured at acquire: a column redefined afterwards (retyped, or dropped and created again) keeps answering the old definition here,
   * which a reader compares with the schema's before decoding anything.
   */
  ColumnDefinition publishedColumn(String property);

  /**
   * The conversion counts {@code {failures, coerced}} of the column with that index file id, as the state the snapshot holds was
   * published with them (see {@code ColumnarManifest}): what its values were extracted with. Null when unknown. A copy.
   */
  default long[] conversionCounts(final int indexFileId) {
    return null;
  }

  /** Releases the snapshot; calling it again does nothing. */
  @Override
  void close();
}
