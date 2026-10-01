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
 * A contiguous run of the rows of another {@link TimeSeriesRowSource}, used to hand each shard its slice of a
 * batch.
 * <p>
 * The slice is a view, an offset and a length: splitting a batch across shards copies no sample data and
 * allocates no index array (issue #8574; the previous row-striped split needed one {@code int[]} per shard,
 * issue #5474).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class RangeRowSource implements TimeSeriesRowSource {

  private final TimeSeriesRowSource delegate;
  private final int                 from;
  private final int                 size;

  RangeRowSource(final TimeSeriesRowSource delegate, final int from, final int size) {
    this.delegate = delegate;
    this.from = from;
    this.size = size;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public long getTimestamp(final int row) {
    return delegate.getTimestamp(from + row);
  }

  @Override
  public long getRawValue(final int row, final int columnIndex) {
    return delegate.getRawValue(from + row, columnIndex);
  }

  @Override
  public String getStringValue(final int row, final int columnIndex) {
    return delegate.getStringValue(from + row, columnIndex);
  }
}
