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

/**
 * Read-only, primitive view over a block of samples to be ingested.
 * <p>
 * The mutable bucket stores every fixed-width column as raw bits, so the ingest path never needs the
 * value as an object: it needs the bits and the column width. This interface is exactly that
 * contract, which lets a caller holding primitive data (or off-heap data) feed the engine without
 * materialising a {@code Double}/{@code Long} per sample - the boxing reported in issue #5474.
 * <p>
 * {@code columnIndex} is the ordinal among the <b>non-timestamp</b> columns of the type, i.e. the
 * same index used by the {@code Object[][] columnValues} form of
 * {@link TimeSeriesEngine#appendSamples(long[], Object[][])}.
 * <p>
 * <b>Raw encoding.</b> {@link #getRawValue(int, int)} returns, per the column's declared
 * {@link com.arcadedb.schema.Type}:
 * <ul>
 *   <li>{@code DOUBLE} - {@link Double#doubleToRawLongBits(double)}</li>
 *   <li>{@code FLOAT} - {@link Float#floatToRawIntBits(float)}, widened to {@code long}</li>
 *   <li>{@code LONG}, {@code DATETIME}, {@code INTEGER}, {@code SHORT}, {@code BYTE} - the numeric
 *       value itself; only the low bytes of the column's fixed width are stored</li>
 *   <li>{@code BOOLEAN} - {@code 1} for true, {@code 0} for false</li>
 * </ul>
 * {@link #getStringValue(int, int)} is consulted only for {@code STRING} columns, and
 * {@link #getRawValue(int, int)} only for the others; an implementation may throw from the method
 * that does not apply to a given column.
 * <p>
 * Implementations are read during a single append call on a single thread and are never retained by
 * the engine.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see TimeSeriesBatch
 */
public interface TimeSeriesRowSource {

  /**
   * Number of samples exposed by this source.
   */
  int size();

  /**
   * Timestamp of the given sample, in the type's precision (millisecond epoch by default).
   */
  long getTimestamp(int row);

  /**
   * Raw bits of a fixed-width column value, encoded as documented on this interface.
   */
  long getRawValue(int row, int columnIndex);

  /**
   * Value of a {@code STRING} column. {@code null} is stored as an empty string.
   */
  String getStringValue(int row, int columnIndex);
}
