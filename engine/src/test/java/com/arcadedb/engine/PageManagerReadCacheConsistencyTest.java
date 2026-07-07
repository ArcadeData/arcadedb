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

import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the read-cache consistency fixes:
 * <ul>
 * <li>#4925: {@code putPageInReadCache} was an unconditional {@code put}, so a reader that started a disk
 * read of version N before a committer cached version N+1 would overwrite the newer committed page with the
 * stale image when its read completed. Subsequent readers (and the commit-time version probe) then saw vN:
 * a later transaction could pass its version check against the poisoned cache and silently overwrite the
 * lost committed update. The put is now version-monotonic: an older version never replaces a newer one.</li>
 * <li>#4933: the bulk removal loops ({@code removeAllReadPagesOfDatabase}, {@code deleteFile}) subtracted a
 * page's size from {@code totalReadCacheRAM} unconditionally BEFORE removing it, while the eviction path and
 * {@code removePageFromCache} subtract only when their {@code remove()} actually returned the entry. Racing
 * the two subtracted the same page twice, driving the counter negative and permanently disabling eviction
 * (the {@code totalRAM < maxRAM} check never fires again): unbounded cache growth. All accounting is now
 * driven by the value actually removed.</li>
 * </ul>
 * <p>
 * ISOLATION NOTE: {@code totalReadCacheRAM} lives on the JVM-wide {@code PageManager.INSTANCE} and is shared
 * by every open database in the process. The exact-equality assertions therefore assume the suite runs these
 * tests single-threaded (the project does not enable JUnit parallel execution) and that no other actor
 * touches the cache inside the measured window: the test database is idle during it (no commits, so no async
 * flush re-caches pages) and the synthetic page ids belong to no real component. If parallel execution is
 * ever enabled, scope the assertions to the page ids the test owns instead.
 */
class PageManagerReadCacheConsistencyTest extends TestHelper {

  private static final int PAGE_SIZE = 65_536;

  @Test
  void staleLoadCannotOverwriteNewerCachedVersion() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pm = db.getPageManager();


    // Use a page id no real component touches (file 0 of this db at a huge page number).
    final PageId pageId = new PageId(db, 0, 1_000_000);
    final CachedPage newer = new CachedPage(new MutablePage(pageId, PAGE_SIZE, new byte[PAGE_SIZE], 7, 0), false);
    final CachedPage stale = new CachedPage(new MutablePage(pageId, PAGE_SIZE, new byte[PAGE_SIZE], 5, 0), false);

    final long ramBefore = pm.totalReadCacheRAM.get();
    try {
      // Committer caches v7, then the slow reader completes its disk read of v5 and tries to cache it.
      pm.putPageInReadCache(newer);
      pm.putPageInReadCache(stale);

      assertThat(pm.readCache.get(pageId).getVersion())
          .as("a stale loaded page must never replace a newer committed version in the read cache (#4925)")
          .isEqualTo(7);
      assertThat(pm.totalReadCacheRAM.get() - ramBefore)
          .as("RAM accounting must reflect exactly one cached page whichever write wins")
          .isEqualTo(newer.getPhysicalSize());
    } finally {
      pm.removePageFromCache(pageId);
    }
    assertThat(pm.totalReadCacheRAM.get()).as("cleanup must restore the accounting baseline").isEqualTo(ramBefore);
  }

  @Test
  void concurrentBulkRemovalDoesNotDoubleSubtractRam() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pm = db.getPageManager();


    // Enough small pages that each thread's sweep takes milliseconds, so the two removal paths genuinely
    // overlap (the double-subtract needs the bulk loop and a successful per-page remove to hit the same entry).
    final int pageSize = 1024;
    final int pagesPerRound = 8192;
    final int rounds = 20;

    for (int round = 0; round < rounds; round++) {
      // Start each round from a clean slate for this database so the baseline is stable.
      pm.removeAllReadPagesOfDatabase(db);
      final long baseline = pm.totalReadCacheRAM.get();

      final PageId[] ids = new PageId[pagesPerRound];
      for (int i = 0; i < pagesPerRound; i++) {
        ids[i] = new PageId(db, 0, 2_000_000 + i);
        pm.putPageInReadCache(new CachedPage(new MutablePage(ids[i], pageSize, new byte[pageSize], 1, 0), false));
      }

      // Race the bulk removal against per-page removals of the same entries: each page's size must be
      // subtracted exactly once no matter which thread wins each entry.
      final CountDownLatch start = new CountDownLatch(1);
      final Thread bulk = new Thread(() -> {
        try {
          start.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        pm.removeAllReadPagesOfDatabase(db);
      }, "bulk-removal");
      final Thread perPage = new Thread(() -> {
        try {
          start.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        for (final PageId id : ids)
          pm.removePageFromCache(id);
      }, "per-page-removal");
      bulk.start();
      perPage.start();
      start.countDown();
      bulk.join(10_000);
      perPage.join(10_000);

      assertThat(pm.totalReadCacheRAM.get())
          .as("round %d: every removed page must be subtracted exactly once (#4933); negative drift disables eviction forever",
              round)
          .isEqualTo(baseline);
    }
  }

}
