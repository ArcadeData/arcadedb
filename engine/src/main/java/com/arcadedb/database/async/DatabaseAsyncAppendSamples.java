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
package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.log.LogManager;

import java.util.logging.Level;

public class DatabaseAsyncAppendSamples implements DatabaseAsyncTask {
  private final TimeSeriesEngine engine;
  private final int              shardIndex;
  private final long[]           timestamps;
  private final Object[][]       columnValues;

  public DatabaseAsyncAppendSamples(final TimeSeriesEngine engine, final int shardIndex, final long[] timestamps,
      final Object[][] columnValues) {
    this.engine = engine;
    this.shardIndex = shardIndex;
    this.timestamps = timestamps.clone();
    this.columnValues = new Object[columnValues.length][];
    for (int i = 0; i < columnValues.length; i++)
      this.columnValues[i] = columnValues[i] != null ? columnValues[i].clone() : null;
  }

  @Override
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    try {
      engine.getShard(shardIndex).appendSamples(timestamps, columnValues);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE,
          "Error appending timeseries samples to shard %d of type '%s' (%d points)",
          e, shardIndex, engine.getTypeName(), timestamps.length);
      throw new DatabaseOperationException("Error appending timeseries samples to shard " + shardIndex, e);
    }
  }

  @Override
  public String toString() {
    return "AppendSamples(type=" + engine.getTypeName() + " shard=" + shardIndex + " points=" + timestamps.length + ")";
  }
}
