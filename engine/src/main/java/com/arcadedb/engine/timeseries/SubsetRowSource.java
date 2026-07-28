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
 * The rows of another {@link TimeSeriesRowSource} selected by an index array, used to hand each
 * shard its slice of a batch.
 * <p>
 * The slice is a view over an {@code int[]} of row numbers: splitting a batch across shards costs
 * one small index array per shard, not a copy of the sample data and not a boxed index per sample.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SubsetRowSource implements TimeSeriesRowSource {

  private final TimeSeriesRowSource delegate;
  private final int[]               rows;
  private final int                 size;

  SubsetRowSource(final TimeSeriesRowSource delegate, final int[] rows, final int size) {
    this.delegate = delegate;
    this.rows = rows;
    this.size = size;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public long getTimestamp(final int row) {
    return delegate.getTimestamp(rows[row]);
  }

  @Override
  public long getRawValue(final int row, final int columnIndex) {
    return delegate.getRawValue(rows[row], columnIndex);
  }

  @Override
  public String getStringValue(final int row, final int columnIndex) {
    return delegate.getStringValue(rows[row], columnIndex);
  }
}
