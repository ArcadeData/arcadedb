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

import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

class ColumnarStatsTest {
  @Test
  void countersAreKeyedByTypeAndReason() {
    final ColumnarStats stats = new ColumnarStats();
    stats.rebuild("Asset", "create");
    stats.rebuild("Asset", "create");
    stats.rebuild("Asset", "corrupt");

    assertThat(stats.rebuilds("Asset", "create")).isEqualTo(2);
    assertThat(stats.rebuilds("Asset", "corrupt")).isEqualTo(1);
    assertThat(stats.rebuilds("Asset", "missing")).isZero();
    assertThat(stats.rebuilds("Other", "create")).isZero();
    assertThat(stats.rebuildsTotal()).isEqualTo(3);

    stats.conversionFailure("Asset", "fileSize");
    assertThat(stats.conversionFailures("Asset", "fileSize")).isEqualTo(1);
    assertThat(stats.conversionFailures("Asset", "brand")).isZero();
    assertThat(stats.conversionFailures("Other", "fileSize")).isZero();
  }

  /** Final review A-M6: a rebuild adds a column's counts at once, one add per column, not one increment per value. */
  @Test
  void conversionCountsAreAddedInOneCall() {
    final ColumnarStats stats = new ColumnarStats();
    stats.addConversionFailures("Asset", "fileSize", 5);
    stats.addConversionFailures("Asset", "fileSize", 0);
    stats.addCoercedValues("Asset", "brand", 7);
    stats.addCoercedValues("Asset", "brand", 2);

    assertThat(stats.conversionFailures("Asset", "fileSize")).isEqualTo(5);
    assertThat(stats.coercedValues("Asset", "brand")).isEqualTo(9);
    assertThat(stats.coercedValues("Asset", "fileSize")).isZero();
    assertThat(stats.conversionFailures("Asset", "brand")).isZero();
  }

  @Test
  void openFilesAreTheSumOfTheDeltasOfTheBucketProjections() {
    final ColumnarStats stats = new ColumnarStats();
    stats.addOpenFiles("Asset", 3);
    stats.addOpenFiles("Asset", 2);
    stats.addOpenFiles("Asset", -4);

    assertThat(stats.openFiles("Asset")).isEqualTo(1);
    assertThat(stats.openFiles("Other")).isZero();
  }

  @Test
  void readersReturnWhatWritersAdded() {
    final ColumnarStats stats = new ColumnarStats();
    writeEveryCell(stats);

    assertThat(stats.applyPasses()).isEqualTo(1);
    assertThat(stats.applyPagesCount("Asset")).isEqualTo(5);
    assertThat(stats.placeholdersResolvedCount("Asset")).isEqualTo(2);
    assertThat(stats.flushCount("Asset")).isEqualTo(1);
    assertThat(stats.mergeCount("Asset")).isEqualTo(1);
    assertThat(stats.recoveryPagesCount()).isEqualTo(7);
    assertThat(stats.recordsRereadCount("Asset")).isEqualTo(11);
    assertThat(stats.unreadableCount("Asset")).isEqualTo(1);
    assertThat(stats.rebuildPagesReusedCount("Asset")).isEqualTo(13);
    assertThat(stats.flushDurationCount("Asset")).isEqualTo(1);
    assertThat(stats.flushDurationTotalNanos("Asset")).isEqualTo(1_000);
    assertThat(stats.mergeDurationCount("Asset")).isEqualTo(1);
    assertThat(stats.mergeDurationTotalNanos("Asset")).isEqualTo(2_000);
    assertThat(stats.dictionaryCompactions("Asset", "brand")).isEqualTo(1);
    assertThat(stats.dictionaryOrdinalsReclaimed("Asset", "brand")).isEqualTo(600);
    assertThat(stats.overlayRows("Asset")).isEqualTo(99);
    assertThat(stats.segments("Asset")).isEqualTo(3);
    assertThat(stats.openFiles("Asset")).isEqualTo(8);

    // a reader never invents a value for another type
    assertThat(stats.applyPagesCount("Other")).isZero();
  }

  @Test
  void queryCountersAreDatabaseWide() {
    final ColumnarStats stats = new ColumnarStats();
    assertThat(stats.queryPatchedCount()).isZero();
    assertThat(stats.queryBudgetWaitsCount()).isZero();
    assertThat(stats.queryPatchOverflowsCount()).isZero();
    assertThat(stats.queryFallbackScansCount()).isZero();

    stats.queryPatched(40);
    stats.queryPatched(2);
    stats.queryBudgetWaits(3);
    stats.queryPatchOverflow();
    stats.queryFallbackScan(ColumnarStats.FALLBACK_NO_INDEX);
    stats.queryFallbackScan(ColumnarStats.FALLBACK_NO_INDEX);
    stats.queryFallbackScan(ColumnarStats.FALLBACK_NOT_PUBLISHED);

    assertThat(stats.queryPatchedCount()).isEqualTo(42);
    assertThat(stats.queryBudgetWaitsCount()).isEqualTo(3);
    assertThat(stats.queryPatchOverflowsCount()).isEqualTo(1);
    assertThat(stats.queryFallbackScansCount()).as("the sum of every reason").isEqualTo(3);
    assertThat(stats.queryFallbackScansCount("no_index")).isEqualTo(2);
    assertThat(stats.queryFallbackScansCount("not_published")).isEqualTo(1);
    assertThat(stats.queryFallbackScansCount("read_events")).isZero();
    assertThat(stats.queryFallbackScansCount("unknown")).isZero();
    // database-wide counters, not cells keyed by type
    assertThat(stats.hasCells()).isFalse();
  }

  @Test
  void forEachKeyListsEveryRegisteredCell() {
    final ColumnarStats stats = new ColumnarStats();
    writeEveryCell(stats);

    final List<String> keys = new ArrayList<>();
    stats.forEachKey((kind, type, property, reason) -> keys.add(kind + "|" + type + "|" + property + "|" + reason));

    final Set<String> expected = new TreeSet<>(List.of("rebuilds|Asset|null|create", "dictionary.compactions|Asset|brand|null",
        "dictionary.ordinals.reclaimed|Asset|brand|null", "apply.pages|Asset|null|null", "apply.placeholders.resolved|Asset|null|null",
        "flush.count|Asset|null|null", "merge.count|Asset|null|null", "conversion.failures|Asset|fileSize|null",
        "recovery.pages|null|null|null", "apply.records.reread|Asset|null|null", "apply.unreadable|Asset|null|null",
        "rebuild.pages.reused|Asset|null|null", "flush.duration|Asset|null|null", "merge.duration|Asset|null|null",
        "overlay.rows|Asset|null|null", "segments|Asset|null|null", "files.open|Asset|null|null"));
    assertThat(new TreeSet<>(keys)).isEqualTo(expected);
    assertThat(keys).hasSameSizeAs(expected);
  }

  @Test
  void cellsAreAllocationFreeAfterFirstUse() {
    final ColumnarStats stats = new ColumnarStats();
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    assumeThat(threads.isThreadAllocatedMemorySupported() && threads.isThreadAllocatedMemoryEnabled()).isTrue();
    final long threadId = Thread.currentThread().threadId();

    // first use creates the cells, and warms the JIT so the measured window holds no compilation garbage
    for (int i = 0; i < 20_000; i++)
      writeHotCells(stats);

    // the least of 3 windows: a one-off allocation of the runtime on this thread (a LongAdder base CAS that failed and inflated its
    // cells, a deoptimization in a shared JVM) lands in one window, while a write path that allocates shows in all of them
    long allocated = Long.MAX_VALUE;
    for (int window = 0; window < 3; window++) {
      final long before = threads.getThreadAllocatedBytes(threadId);
      for (int i = 0; i < 10_000; i++)
        writeHotCells(stats);
      allocated = Math.min(allocated, threads.getThreadAllocatedBytes(threadId) - before);
    }

    assertThat(allocated).as("bytes allocated by 10_000 writes to existing cells, least of 3 windows").isZero();
  }

  private static void writeHotCells(final ColumnarStats stats) {
    stats.conversionFailure("Asset", "fileSize");
    stats.rebuild("Asset", "create");
    stats.applyPages("Asset", 1);
    stats.flushDuration("Asset", 10);
    stats.setOverlayRows("Asset", 5);
    stats.dictionaryCompaction("Asset", "brand", 3);
  }

  private static void writeEveryCell(final ColumnarStats stats) {
    stats.rebuild("Asset", "create");
    stats.conversionFailure("Asset", "fileSize");
    stats.applyPass();
    stats.applyPages("Asset", 5);
    stats.placeholdersResolved("Asset", 2);
    stats.flush("Asset");
    stats.merge("Asset");
    stats.recoveryPages(7);
    stats.recordsReread("Asset", 11);
    stats.flushDuration("Asset", 1_000);
    stats.mergeDuration("Asset", 2_000);
    stats.unreadable("Asset");
    stats.rebuildPagesReused("Asset", 13);
    stats.dictionaryCompaction("Asset", "brand", 600);
    stats.setOverlayRows("Asset", 99);
    stats.setSegments("Asset", 3);
    stats.setOpenFiles("Asset", 8);
  }
}
