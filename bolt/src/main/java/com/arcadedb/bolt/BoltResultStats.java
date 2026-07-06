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
package com.arcadedb.bolt;

import com.arcadedb.query.sql.executor.QueryStatistics;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps engine QueryStatistics to the Neo4j Bolt SUCCESS "stats" metadata map. Emits only non-zero
 * counters plus contains-updates, matching how Neo4j server reports write summaries.
 */
final class BoltResultStats {
  private BoltResultStats() {
  }

  static Map<String, Object> toStatsMap(final QueryStatistics s) {
    final Map<String, Object> stats = new LinkedHashMap<>();
    putIfNonZero(stats, "nodes-created", s.getNodesCreated());
    putIfNonZero(stats, "nodes-deleted", s.getNodesDeleted());
    putIfNonZero(stats, "relationships-created", s.getRelationshipsCreated());
    putIfNonZero(stats, "relationships-deleted", s.getRelationshipsDeleted());
    putIfNonZero(stats, "properties-set", s.getPropertiesSet());
    putIfNonZero(stats, "labels-added", s.getLabelsAdded());
    putIfNonZero(stats, "labels-removed", s.getLabelsRemoved());
    putIfNonZero(stats, "indexes-added", s.getIndexesAdded());
    putIfNonZero(stats, "indexes-removed", s.getIndexesRemoved());
    putIfNonZero(stats, "constraints-added", s.getConstraintsAdded());
    putIfNonZero(stats, "constraints-removed", s.getConstraintsRemoved());
    // contains-updates is emitted unconditionally as true, so callers must only invoke this when
    // the source statistics report containsUpdates().
    stats.put("contains-updates", true);
    return stats;
  }

  private static void putIfNonZero(final Map<String, Object> stats, final String key, final int value) {
    if (value != 0)
      stats.put(key, (long) value);
  }
}
