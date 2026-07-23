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
package performance;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

/**
 * Quantifies the steady-state, ZERO-CONTENTION write-path cost of the disjoint-slot merge (TX_PAGE_SLOT_MERGE): a
 * single thread doing same-size in-place updates and inserts into a reused page, where the merge tracks every
 * write but never rebases. Reports ops/s and heap allocated per op with the feature off vs on, using the JVM's
 * per-thread allocation counter. Tagged benchmark so it is excluded from CI; run explicitly to reproduce the
 * numbers cited in PR #5385.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class SlotMergeWriteOverheadBenchmark extends TestHelper {
  private static final int WARMUP = 20_000;
  private static final int OPS    = 200_000;

  @Test
  void inPlaceUpdateOverhead() {
    final boolean savedMerge = GlobalConfiguration.TX_PAGE_SLOT_MERGE.getValueAsBoolean();
    try {
      database.transaction(() -> database.getSchema().createDocumentType("Bench", 1).createProperty("tag", Type.STRING));
      final RID[] rid = new RID[1];
      database.transaction(() -> {
        final var d = database.newDocument("Bench");
        d.set("tag", String.format("%016d", 0));
        d.save();
        rid[0] = d.getIdentity();
      });

      System.out.println("=== in-place UPDATE (same size), single thread, no contention ===");
      runUpdate(rid[0], false); // off
      runUpdate(rid[0], true);  // on

      System.out.println("=== INSERT into reused page, single thread, no contention ===");
      runInsert(false);
      runInsert(true);
    } finally {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(savedMerge);
    }
  }

  private void runUpdate(final RID rid, final boolean merge) {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(merge);
    for (int i = 0; i < WARMUP; i++) {
      final String v = String.format("%016d", i);
      database.transaction(() -> rid.asDocument(true).modify().set("tag", v).save(), true, 1);
    }
    final ThreadMXBean tb = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final long id = Thread.currentThread().threadId();
    final long a0 = tb.getThreadAllocatedBytes(id);
    final long t0 = System.nanoTime();
    for (int i = 0; i < OPS; i++) {
      final String v = String.format("%016d", i);
      database.transaction(() -> rid.asDocument(true).modify().set("tag", v).save(), true, 1);
    }
    final long dt = System.nanoTime() - t0;
    final long da = tb.getThreadAllocatedBytes(id) - a0;
    report("update merge=" + merge, dt, da);
  }

  private void runInsert(final boolean merge) {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(merge);
    for (int i = 0; i < WARMUP; i++)
      database.transaction(() -> database.newDocument("Bench").set("tag", "x").save(), true, 1);
    final ThreadMXBean tb = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final long id = Thread.currentThread().threadId();
    final long a0 = tb.getThreadAllocatedBytes(id);
    final long t0 = System.nanoTime();
    for (int i = 0; i < OPS; i++)
      database.transaction(() -> database.newDocument("Bench").set("tag", "x").save(), true, 1);
    final long dt = System.nanoTime() - t0;
    final long da = tb.getThreadAllocatedBytes(id) - a0;
    report("insert merge=" + merge, dt, da);
  }

  private static void report(final String label, final long nanos, final long allocated) {
    final double opsPerSec = OPS / (nanos / 1_000_000_000.0);
    System.out.printf("  %-18s %,10.0f ops/s   %,6d bytes/op%n", label, opsPerSec, allocated / OPS);
  }
}
