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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the low-severity storage/WAL findings grouped in issue #4958 that are observable
 * through public engine behavior:
 * <ul>
 *   <li>{@code TransactionManager.getStats()} used to fold every active WAL file's CUMULATIVE counters
 *   into the long-lived accumulators on each call, so the reported numbers inflated with polling
 *   frequency (each poll re-added everything the active files ever wrote).</li>
 *   <li>{@code TransactionManager.getStats()} used to NPE on a read-only database, where no active WAL
 *   file pool is created.</li>
 *   <li>{@code PageManager.getCachedPage} returned before bumping the cache-miss counter, so
 *   {@code cacheMiss} stayed at ~0 forever and the hit/miss ratio was meaningless.</li>
 *   <li>{@code PageManager.suspendFlushAndExecute} silently SKIPPED the callback when the database was
 *   already suspended by an outer scope (nested backup/snapshot).</li>
 * </ul>
 */
class Issue4958StorageStatsTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("StatsType");
  }

  @Test
  void walStatsAreNotInflatedByPolling() {
    database.transaction(() -> database.newDocument("StatsType").set("k", 1).save());

    final TransactionManager txManager = ((DatabaseInternal) database).getTransactionManager();

    final Map<String, Object> first = txManager.getStats();
    final Map<String, Object> second = txManager.getStats();
    final Map<String, Object> third = txManager.getStats();

    // No writes happened between the calls: polling must not change the totals.
    assertThat(second.get("pagesWritten")).as("pagesWritten must not inflate with polling")
        .isEqualTo(first.get("pagesWritten"));
    assertThat(third.get("pagesWritten")).isEqualTo(first.get("pagesWritten"));
    assertThat(second.get("bytesWritten")).as("bytesWritten must not inflate with polling")
        .isEqualTo(first.get("bytesWritten"));
    assertThat(third.get("bytesWritten")).isEqualTo(first.get("bytesWritten"));

    // The counters must still reflect the write that DID happen.
    assertThat((Long) first.get("pagesWritten")).isGreaterThan(0L);
    assertThat((Long) first.get("bytesWritten")).isGreaterThan(0L);
  }

  @Test
  void walStatsOnReadOnlyDatabaseDoesNotThrow() {
    database.transaction(() -> database.newDocument("StatsType").set("k", 2).save());

    reopenDatabaseInReadOnlyMode();
    try {
      final TransactionManager txManager = ((DatabaseInternal) database).getTransactionManager();
      // Read-only databases have no active WAL file pool: getStats() must not NPE.
      final Map<String, Object> stats = txManager.getStats();
      assertThat(stats).containsKeys("logFiles", "pagesWritten", "bytesWritten");
    } finally {
      reopenDatabase();
    }
  }

  @Test
  void pageCacheMissIsCounted() {
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("StatsType").set("k", i).save();
    });

    // Reopening drops this database's pages from the shared read cache, so the scan below must
    // load at least one page from disk = at least one cache miss.
    final long missBefore = ((DatabaseInternal) database).getPageManager().getStats().cacheMiss;

    reopenDatabase();
    database.begin();
    database.iterateType("StatsType", false).forEachRemaining(r -> r.asDocument().get("k"));
    database.commit();

    final long missAfter = ((DatabaseInternal) database).getPageManager().getStats().cacheMiss;
    assertThat(missAfter)
        .as("loading uncached pages from disk must increment the cacheMiss counter")
        .isGreaterThan(missBefore);
  }

  @Test
  void nestedSuspendFlushStillExecutesTheCallback() throws Exception {
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final AtomicBoolean innerCallbackRan = new AtomicBoolean(false);

    pageManager.suspendFlushAndExecute(database, () ->
        // Nested suspension (e.g. a snapshot triggered during a backup): the inner callback must
        // still run under the outer scope's suspension instead of being silently skipped.
        pageManager.suspendFlushAndExecute(database, () -> innerCallbackRan.set(true)));

    assertThat(innerCallbackRan.get())
        .as("a nested suspendFlushAndExecute must execute its callback, not silently skip it")
        .isTrue();
    assertThat(pageManager.isPageFlushingSuspended(database))
        .as("the outer scope must have restored the flush on exit")
        .isFalse();
  }
}
