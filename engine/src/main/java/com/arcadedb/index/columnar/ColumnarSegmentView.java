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
 * One immutable set of rows of a bucket's projection (a segment file, or the in-memory overlay), seen through one dead set.
 */
public interface ColumnarSegmentView {
  /**
   * Positions of the view's rows that are not dead. Computed once per view and cached (a view's dead set never changes): the
   * returned bitmap is shared, so a caller {@link PositionBitmap#copy() copies} it before writing.
   */
  PositionBitmap live();

  /** {@link #live()} restricted to the pages {@code [fromPage, toPageExclusive)}, computed on each call; owned by the caller. */
  PositionBitmap live(int fromPage, int toPageExclusive);

  /** The reader of the property's column, null when the view has no such column. */
  ColumnReader column(String property);

  /**
   * Smallest value of the column for skipping: the value for LONG, the ordinal for ORDINAL and ORDINAL_LIST, the
   * {@code Double.doubleToLongBits} of the value for DOUBLE. {@code Long.MAX_VALUE} when the column holds no value.
   *
   * @throws IllegalArgumentException when the view has no such column
   */
  long minLong(String property);

  /** Largest value, as {@link #minLong}; {@code Long.MIN_VALUE} when the column holds no value. */
  long maxLong(String property);
}
