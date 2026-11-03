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
package com.arcadedb.function.sql.vector;

import com.arcadedb.database.Document;
import com.arcadedb.log.LogManager;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Reports a vector search row whose record turned out to be gone when it was first read (issue #11202): a vector index entry for a
 * record removed from its bucket behind the index's back (a crash, a restore). The row was made without reading its record (issue
 * #10973), so it is in the answer already, with no properties, and it took a place a real neighbour would have had in the top K.
 * Nothing on the search path can tell without reading the record's page, which is the cost #10973 removed, so the anomaly is
 * repaired rather than probed for, as a ghost edge is (#10463): this makes it visible, naming the repair.
 * <p>
 * Each row is counted once and logged at {@code FINE}; a {@code WARNING} goes out at most once a minute, as for ghost edges, so a
 * badly damaged index does not flood the log.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class StaleVectorEntryReporter {
  private static final long       WARNING_WINDOW_NANOS = 60_000_000_000L; // 60s
  private static final AtomicLong totalReported        = new AtomicLong();
  // One full window in the past, so the first stale entry warns: see GhostEdgeReporter for why not Long.MIN_VALUE
  private static final AtomicLong lastWarningNanos     = new AtomicLong(-WARNING_WINDOW_NANOS);

  private StaleVectorEntryReporter() {
  }

  /** Records that the row of {@code record} was answered although its record is gone. */
  static void reportMissingRecord(final Document record) {
    final long total = totalReported.incrementAndGet();

    if (LogManager.instance().isLoggable(StaleVectorEntryReporter.class, Level.FINE))
      LogManager.instance().log(StaleVectorEntryReporter.class, Level.FINE,
          "Vector search answered %s of type '%s', whose record is gone; total since startup=%d", record.getIdentity(),
          record.getTypeName(), total);

    final long now = System.nanoTime();
    final long last = lastWarningNanos.get();
    if (now - last >= WARNING_WINDOW_NANOS && lastWarningNanos.compareAndSet(last, now))
      LogManager.instance().log(StaleVectorEntryReporter.class, Level.WARNING,
          "A vector index on type '%s' holds an entry for %s, a record removed from its bucket behind the index's back (a crash, a "
              + "restore): the search answered it as a row with no properties, in the place of a real neighbour. Repair the index "
              + "with REBUILD INDEX (total stale rows answered since startup=%d)", record.getTypeName(), record.getIdentity(), total);
  }

  /** Total rows answered for a record that was gone, since JVM startup (monotonic). */
  static long getTotalReported() {
    return totalReported.get();
  }
}
