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
package com.arcadedb.server.ha.raft;

import com.arcadedb.schema.FullSchemaLoadReason;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.SchemaFullLoadStats;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Counts the full schema loads a replica's apply thread ran instead of the incremental refresh, by reason, and how long
 * they took (issue #10020). Cumulative since the process started and shared by every state machine the
 * {@link RaftHAServer} builds, so an in-place Raft restart does not reset it. Written from the single apply thread and
 * read by a metrics scrape from another, hence atomics; the three counters are read one after the other, not as one
 * snapshot, and each only grows, so a reader sees at worst a load that completed between two reads in one array only.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SchemaFullLoadRecorder {
  private final AtomicLongArray fallbacks  = new AtomicLongArray(FullSchemaLoadReason.values().length);
  private final AtomicLongArray completed  = new AtomicLongArray(FullSchemaLoadReason.values().length);
  private final AtomicLongArray totalNanos = new AtomicLongArray(FullSchemaLoadReason.values().length);

  /** Called before the load starts, so one that throws is still counted. */
  void started(final FullSchemaLoadReason reason) {
    fallbacks.incrementAndGet(reason.ordinal());
  }

  /** Called when the load finished. */
  void completed(final FullSchemaLoadReason reason, final long nanos) {
    completed.incrementAndGet(reason.ordinal());
    totalNanos.addAndGet(reason.ordinal(), nanos);
  }

  SchemaFullLoadStats snapshot() {
    final int n = fallbacks.length();
    final long[] f = new long[n];
    final long[] c = new long[n];
    final long[] t = new long[n];
    for (int i = 0; i < n; i++) {
      f[i] = fallbacks.get(i);
      c[i] = completed.get(i);
      t[i] = totalNanos.get(i);
    }
    return new SchemaFullLoadStats(f, c, t);
  }
}
