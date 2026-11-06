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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.utility.DedicatedThreadPool;

/**
 * JVM-wide dedicated executor for the long-running maintenance of the COLUMNAR indexes: flush, merge, dictionary
 * compaction, rank-view rebuild, background rebuild, and the background rebuild of a hierarchy view ({@link HierarchyManager},
 * queued by its coalesced file watcher, at most one queued per hierarchy). Kept apart from {@link ColumnarApplyPool} so a long merge never
 * delays an apply pass. Lazy singleton; daemon threads, sized by {@code arcadedb.columnar.maintenanceThreads}.
 * <p>
 * <b>Why the queue is UNBOUNDED with no caller-runs policy</b> (a deliberate deviation from the "bounded queue +
 * caller-runs" rule of the {@code engine-concurrency} skill): a submitter is an apply pass that finishes, the ticker,
 * or an open or reconcile thread asking for a rebuild (bounded by the coalescing of rebuild requests, one per
 * projection), never a commit thread, and caller-runs would run a merge on a commit or Raft apply thread. Back-pressure is
 * structural: at most one of flush, merge or dictionary compaction per projection (a maintenance flag in the
 * projection) and at most one hierarchy rebuild per hierarchy, so the queue cannot grow past the number of open
 * projections and hierarchies. {@code queueDepth} is the saturation
 * signal ({@code pool=columnar_maintenance}); {@code queueCapacityRemaining} reports -1 and the caller-runs count stays
 * at zero.
 *
 * @see ColumnarApplyPool
 */
public final class ColumnarMaintenancePool extends DedicatedThreadPool {

  private static final class Holder {
    static final ColumnarMaintenancePool INSTANCE = new ColumnarMaintenancePool();
  }

  private ColumnarMaintenancePool() {
    super("ArcadeDB-ColumnarMaintenance-", Math.max(1, GlobalConfiguration.COLUMNAR_MAINTENANCE_THREADS.getValueAsInteger()),
        UNBOUNDED_QUEUE, SaturationPolicy.NONE, DedicatedThreadPool::plainWorker, "Columnar maintenance pool", null);
  }

  public static ColumnarMaintenancePool getInstance() {
    return Holder.INSTANCE;
  }
}
