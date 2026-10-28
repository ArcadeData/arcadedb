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

/**
 * Read access to one column of a {@link ColumnarSegmentView}. Point reads take a position of the view's bucket and answer the stored
 * row whatever the view's dead set says; {@link #forEachOrdinal} is the bulk path and honours the dead set.
 */
public interface ColumnReader {
  ColumnKind kind();

  /** False when the view holds no row at the position or the row has no value (missing). */
  boolean isPresent(long position);

  /** {@link ColumnKind#ORDINAL}: the ordinal of the value, -1 when not {@link #isPresent present}. */
  int ordinal(long position);

  /**
   * {@link ColumnKind#ORDINAL_LIST}: copies the sorted, deduplicated ordinals of the value into {@code out} (at most
   * {@code out.length} of them) and returns their count, 0 when not {@link #isPresent present}. A count above {@code out.length}
   * means {@code out} was too short.
   */
  int ordinals(long position, int[] out);

  /** {@link ColumnKind#LONG} (epoch counts for dates): the value, 0 when not {@link #isPresent present}. */
  long longValue(long position);

  /** {@link ColumnKind#DOUBLE}: the value, 0.0 when not {@link #isPresent present}. */
  double doubleValue(long position);

  /**
   * Visits exactly the positions of {@code matches AND live()} of this column's view that have a value, in ascending position order,
   * each once; for {@link ColumnKind#ORDINAL_LIST} once per ordinal, ordinals ascending within a position. Dead rows are never visited,
   * so a caller never ANDs {@code live()} itself. Presence and the dead set are consulted only for {@code matches.pages()}, and
   * {@code live()} is never materialized. Dense or point reads are chosen inside, per row block.
   *
   * @throws IllegalStateException when the column is not an ordinal kind
   */
  void forEachOrdinal(PositionBitmap matches, OrdinalConsumer consumer);
}
