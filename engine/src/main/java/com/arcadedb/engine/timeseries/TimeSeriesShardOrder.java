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

import com.arcadedb.database.Database;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The ONE order in which every multi-shard lock acquisition in this package visits TimeSeries shards, and the
 * only reason {@link TimeSeriesCompactionPause} and {@link TimeSeriesSealedInstallLock} cannot deadlock against
 * each other.
 *
 * <h2>Why this exists rather than each acquirer walking the schema itself</h2>
 *
 * Both classes take several shard locks non-atomically, one takes read locks and the other write locks, and they
 * run concurrently by design - that is the whole point of #7337. Two such acquisitions are deadlock-free exactly
 * when they agree on a TOTAL ORDER over the resources, and they were originally written to agree by walking
 * {@code database.getSchema().getTypes()} in the order it happened to return.
 * <p>
 * That order is not a guarantee (code review on PR #7474). {@code LocalSchema.getTypes()} is built from a
 * {@code ConcurrentHashMap}'s {@code values()}, whose iteration order is bucket order; a table resize - which a
 * {@code CREATE ... TYPE} can trigger at any moment - moves an entry to either bucket {@code i} or {@code i + n},
 * so two types already present can come back in the OPPOSITE relative order afterwards. Two snapshots taken
 * either side of such a resize therefore disagree, one acquirer walks T1 then T3 while the other walks T3 then
 * T1, and the cycle the javadoc ruled out is open:
 * <ul>
 *   <li>the pause holds T1's read lock and waits for T3's;</li>
 *   <li>the install holds T3's write lock and waits for T1's.</li>
 * </ul>
 * Both sides acquire under a deadline, so it resolves as a timeout rather than a permanent hang - but the
 * install runs on the single Raft apply thread with a 120s budget, so the cost is the whole replication pipeline
 * stalled for two minutes, which is not meaningfully better than a hang.
 * <p>
 * Sorting by type name fixes it because the name is the identity the schema is keyed by: it is stable for the
 * life of a type, total, and identical in every snapshot whatever the map did in between. Keeping the sort HERE,
 * in one method both acquirers call, is the other half - two independent sorts would agree today and could drift
 * apart in exactly the way the two independent schema walks did.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class TimeSeriesShardOrder {
  // The order is: types by name; for each type its lifecycle lock (issue #7475) and only then its shards by index.

  /** One shard, with the type and index that name it, in global lock order. */
  record ShardSlot(String typeName, int shardIndex, TimeSeriesShard shard) {
  }

  private TimeSeriesShardOrder() {
  }

  /**
   * Every TimeSeries type of {@code database}, ordered by name: the order both acquirers visit types in.
   * <p>
   * A type whose engine never started (issue #6356) is INCLUDED. It has no shard, but it has the lock that
   * outlives the engine ({@link LocalTimeSeriesType#getEngineLifecycleLock()}), which is what the HA sealed-store
   * repair holds while it creates the engine (issue #7475) - so a walk that skipped such a type would let a copy
   * of the database run straight through the repair.
   */
  static List<LocalTimeSeriesType> typesOf(final Database database) {
    final List<LocalTimeSeriesType> types = new ArrayList<>();
    for (final DocumentType type : database.getSchema().getTypes())
      if (type instanceof final LocalTimeSeriesType tsType)
        types.add(tsType);

    // The total order the whole mechanism rests on. Sorted by NAME and not by anything derived from the schema's
    // iteration, because the latter is what could not be relied on in the first place.
    types.sort(Comparator.comparing(LocalTimeSeriesType::getName));
    return types;
  }

  /**
   * The shards of one type, by ascending shard index, as its engine is RIGHT NOW. Empty while the type has no
   * engine. Call it AFTER taking the type's lifecycle lock: the engine can only appear or disappear under that
   * lock's write half, so a read made while holding either half is the answer for as long as it is held.
   */
  static List<ShardSlot> shardsOf(final LocalTimeSeriesType tsType) {
    // The unchecked accessor on purpose: this is engine-internal housekeeping on behalf of a caller already
    // authorized for the whole database.
    final TimeSeriesEngine engine = tsType.getEngine();
    if (engine == null)
      return List.of();
    final List<ShardSlot> slots = new ArrayList<>(engine.getShardCount());
    for (int i = 0; i < engine.getShardCount(); i++)
      slots.add(new ShardSlot(tsType.getName(), i, engine.getShard(i)));
    return slots;
  }

  /**
   * Every shard of every TimeSeries type of {@code database} that has an engine right now, ordered by type name
   * and then by ascending shard index. A snapshot for diagnostics and tests: the acquirers walk
   * {@link #typesOf} and {@link #shardsOf} themselves, because they must take each type's lifecycle lock between
   * the two.
   */
  static List<ShardSlot> of(final Database database) {
    final List<ShardSlot> slots = new ArrayList<>();
    for (final LocalTimeSeriesType tsType : typesOf(database))
      slots.addAll(shardsOf(tsType));
    return slots;
  }
}
