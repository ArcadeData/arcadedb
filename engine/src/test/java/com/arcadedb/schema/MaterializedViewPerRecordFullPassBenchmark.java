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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.PageManager;
import com.arcadedb.utility.StallAwareStopwatch;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Profiles the full pass of a per-record materialized view ({@code MaterializedViewPerRecordRefresher.fullPass}) against the
 * whole-view {@code writeSnapshot} of a non-per-record view over the same query and source (issue #11551): time (stall
 * discounted), bytes allocated by the refreshing thread, and pages read plus page-cache hits. Tagged benchmark so it is excluded
 * from CI; the row count defaults to 1M and can be lowered with {@code -Dmv.bench.rows=N}.
 * <p>
 * It also times the delta pass ({@code MaterializedViewPerRecordRefresher.deltaPass}) over N source records spread across the
 * whole source, for N around the default {@code arcadedb.materializedViewPerRecordMaxDelta} (10000), to locate the crossover
 * between a delta pass of N records and one full pass. The sizes can be set with {@code -Dmv.bench.deltas=1000,10000,...}.
 */
@Tag("benchmark")
class MaterializedViewPerRecordFullPassBenchmark extends TestHelper {
  private static final int ROWS   = Integer.getInteger("mv.bench.rows", 1_000_000);
  private static final int ROUNDS = Integer.getInteger("mv.bench.rounds", 3);
  private static final int    BATCH  = 50_000;
  private static final String DELTAS = System.getProperty("mv.bench.deltas", "1000,10000,50000,100000,250000");

  @Test
  void fullPassAgainstWriteSnapshot() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Person");
      type.createProperty("name", Type.STRING);
      type.createProperty("age", Type.INTEGER);
    });
    for (int start = 0; start < ROWS; start += BATCH) {
      final int from = start;
      database.transaction(() -> {
        for (int i = from; i < Math.min(from + BATCH, ROWS); i++)
          database.newDocument("Person").set("name", "p" + i).set("age", i % 100).save();
      });
    }

    createView("SnapshotView", MaterializedViewRefreshMode.MANUAL);
    createView("PerRecordView", MaterializedViewRefreshMode.INCREMENTAL);
    final MaterializedViewImpl snapshot = (MaterializedViewImpl) database.getSchema().getMaterializedView("SnapshotView");
    final MaterializedViewImpl perRecord = (MaterializedViewImpl) database.getSchema().getMaterializedView("PerRecordView");
    assertThat(snapshot.isPerRecord()).isFalse();
    assertThat(perRecord.isPerRecord()).isTrue();

    System.out.printf("=== %,d rows, %d rounds ===%n", ROWS, ROUNDS);
    for (int round = 0; round < ROUNDS; round++) {
      measure("writeSnapshot", () -> MaterializedViewRefresher.refreshOnce(database, snapshot));
      measure("per-record fullPass", () -> MaterializedViewPerRecordRefresher.fullPass(database, perRecord));
    }

    // Delta passes over N records spread evenly across the source, against the full pass above: where they cross is where the
    // default cap (arcadedb.materializedViewPerRecordMaxDelta) belongs
    final RID[] sources = new RID[ROWS];
    int count = 0;
    for (final Iterator<Record> it = database.iterateType("Person", true); it.hasNext() && count < ROWS; )
      sources[count++] = it.next().getIdentity();
    for (final String size : DELTAS.split(",")) {
      final int n = Math.min(Integer.parseInt(size.trim()), ROWS);
      for (int round = 0; round < ROUNDS; round++) {
        final RecordIdentities rids = new RecordIdentities(n);
        final long stride = Math.max(1, ROWS / n);
        for (int k = 0; k < n; k++)
          rids.add(sources[(int) ((k * stride + round) % ROWS)]);
        measure("per-record deltaPass N=" + n, () -> MaterializedViewPerRecordRefresher.deltaPass(database, perRecord, rids));
      }
    }

    // A pass over a source that moved: 1% of the records deleted, 1% added, 1% updated
    final int step = 100;
    database.transaction(() -> database.command("sql", "DELETE FROM Person WHERE name LIKE 'p%00'"));
    database.transaction(() -> {
      for (int i = 0; i < ROWS / step; i++)
        database.newDocument("Person").set("name", "n" + i).set("age", 7).save();
    });
    database.transaction(() -> database.command("sql", "UPDATE Person SET age = age + 1 WHERE name LIKE 'p%01'"));
    measure("per-record fullPass (1% del/ins/upd)", () -> MaterializedViewPerRecordRefresher.fullPass(database, perRecord));

    assertThat(database.countType("PerRecordView", false)).isEqualTo(database.countType("Person", true));
  }

  private void measure(final String label, final Runnable pass) {
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final long threadId = Thread.currentThread().threadId();
    final PageManager pages = ((DatabaseInternal) database).getPageManager();

    final PageManager.PPageManagerStats before = pages.getStats();
    final long allocatedBefore = threads.getThreadAllocatedBytes(threadId);
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    pass.run();
    final long effectiveMs = stopwatch.effectiveMs();
    final long allocated = threads.getThreadAllocatedBytes(threadId) - allocatedBefore;
    final PageManager.PPageManagerStats after = pages.getStats();

    System.out.printf("%-40s %,8d ms (stall %,d ms)  %,10d MB allocated  %,10d pages read  %,12d cache hits%n", label,
        effectiveMs, stopwatch.stallMs(), allocated / (1024 * 1024), after.pagesRead - before.pagesRead,
        after.cacheHits - before.cacheHits);
  }

  private void createView(final String name, final MaterializedViewRefreshMode mode) {
    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName(name)
        .withQuery("SELECT name, age FROM Person")
        .withRefreshMode(mode)
        .create());
  }
}
