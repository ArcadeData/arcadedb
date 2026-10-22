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
import com.arcadedb.engine.timeseries.TimeSeriesRowSource;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.log.LogManager;

import java.util.logging.Level;

public class DatabaseAsyncAppendSamples implements DatabaseAsyncTask {
  private final TimeSeriesEngine    engine;
  private final int                 shardIndex;
  private final TimeSeriesRowSource source;

  /**
   * Takes the row source as-is. The primitive path exists precisely to avoid a per-sample copy, so
   * ownership passes to this task: see
   * {@link DatabaseAsyncExecutor#appendSamples(String, TimeSeriesRowSource)}.
   * <p>
   * The source must already have been converted and held to what the columns can store and to the constraints of the type by the
   * caller ({@code TimeSeriesStorableRows.requireStorable}, issues #10381, #10487): this task appends it
   * on an async thread, where a refusal reaches nobody. It takes no boxed arrays of its own for that reason - the constructor
   * that did was the way around that conversion.
   */
  public DatabaseAsyncAppendSamples(final TimeSeriesEngine engine, final int shardIndex, final TimeSeriesRowSource source) {
    this.engine = engine;
    this.shardIndex = shardIndex;
    this.source = source;
  }

  @Override
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    try {
      engine.getShard(shardIndex).appendSamples(source);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE,
          "Error appending timeseries samples to shard %d of type '%s' (%d points)",
          e, shardIndex, engine.getTypeName(), source.size());
      throw new DatabaseOperationException("Error appending timeseries samples to shard " + shardIndex, e);
    }
  }

  @Override
  public boolean writesToSharedBatch() {
    // #7615 review: writes to a TimeSeriesEngine shard, a storage path entirely separate from database's
    // transaction - never touches the shared batch, so must not disable a periodic-boundary retry-by-replay
    // just for sharing a worker with buffered commands.
    return false;
  }

  @Override
  public String toString() {
    return "AppendSamples(type=" + engine.getTypeName() + " shard=" + shardIndex + " points=" + source.size() + ")";
  }
}
