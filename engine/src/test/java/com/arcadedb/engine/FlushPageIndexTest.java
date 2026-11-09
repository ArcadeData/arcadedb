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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The flush queue index ({@code PageManagerFlushThread.pageIndex} / {@link FlushPageIndex}): removal by reference identity
 * (issue #4544) and pending counters per database instance (issue #11315).
 */
class FlushPageIndexTest extends TestHelper {
  // Regression test for issue #4544.
  // <p>
  // The flush queue index ({@code PageManagerFlushThread.pageIndex}) keeps the most recent {@link MutablePage}
  // per {@link PageId} so reads can still find a page that has been polled from the queue but not yet flushed to
  // disk. When a page finishes flushing it must be removed from the index, but ONLY if a later transaction has
  // not meanwhile queued a NEWER page for the same {@link PageId}.
  // <p>
  // The removal used to be {@code pageIndex.remove(key, value)}, which resolves "is it still the same page?"
  // through {@link BasePage#equals}. Two pages for the same {@link PageId} compare equal there (equality keys on
  // pageId only, see #4722), so a value-based removal wrongly evicts a newer entry while flushing a stale page,
  // and subsequent reads miss the page and fall back to the older on-disk version (spurious MVCC conflicts).
  // The fix removes by reference identity instead.
  /**
   * Reproduces the fragility: a stale page whose {@code version} has shifted to match the value currently
   * indexed must NOT evict that (newer) entry. With the old value-based {@code remove(key, value)} the newer
   * entry would be removed; reference-identity removal keeps it.
   */
  @Test
  void staleFlushMustNotEvictNewerIndexedPage() {
    // Constructing the flush thread directly does NOT start the background thread (that happens in
    // PageManager.startup(), run on the first acquire), so the index can be exercised deterministically in isolation.
    final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, new ContextConfiguration());

    final PageId pageId = new PageId(database, 3, 7);

    final MutablePage stale = new MutablePage(pageId, 1024, new byte[1024], 5, 0);
    final MutablePage newer = new MutablePage(pageId, 1024, new byte[1024], 6, 0);

    // A later transaction has queued the newer page: it is the current value in the flush index.
    flush.pageIndex.put(newer);

    // Both pages share the same PageId, so they always compare equal under BasePage.equals (which keys on
    // pageId only, see #4722). A value-based remove(key, value) would therefore happily evict the newer entry
    // when flushing the stale one: removal must use reference identity instead.
    stale.incrementVersion(); // 5 -> 6
    assertThat(stale.equals(newer)).as("pages with the same pageId compare equal regardless of version").isTrue();

    // Flushing the stale page must leave the newer entry untouched.
    flush.removeFromFlushIndex(stale);

    assertThat(flush.pageIndex.get(pageId)).as("newer indexed page must survive a stale flush").isSameAs(newer);
  }

  /**
   * Sanity check of the normal path: flushing the exact page that is indexed removes it.
   */
  @Test
  void flushingTheIndexedPageRemovesIt() {
    final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, new ContextConfiguration());

    final PageId pageId = new PageId(database, 4, 2);
    final MutablePage page = new MutablePage(pageId, 1024, new byte[1024], 9, 0);

    flush.pageIndex.put(page);
    flush.removeFromFlushIndex(page);

    assertThat(flush.pageIndex.containsKey(pageId)).isFalse();
  }

  // Regression test for issue #11315: {@link FlushPageIndex#scanPendingOf}, the oracle the regression test of #6133
  // compares the O(1) pending counter against, enumerated the RETAINED MAP KEYS and matched them with the path-based
  // {@code LocalDatabase.equals()} - the two enumerations #6440 and #6931 removed from the rest of the class. With two
  // instances open at one path, {@code Map.put()} keeps the first instance's key and swaps in the second one's page, so
  // the oracle attributed the second instance's page to the first, disagreed with the counter that was right, and would
  // have agreed with a counter regressed to key-based accounting.
  // <p>
  // The fixed oracle found exactly such a regression one method over: {@link FlushPageIndex#remove(PageId)} released the
  // counter of the database its ARGUMENT names, not of the page it removed, so the same aliasing drove the stale
  // instance's count negative and left the live one's above zero on an empty index - the "close() hangs on a pipeline
  // that is in fact empty" failure the class javadoc names.
  private static final int PAGE_SIZE   = 1_024;
  private static final int FILE_ID     = 1;
  private static final int PAGE_NUMBER = 7;

  @Test
  void theOracleAttributesAPageToTheInstanceItsValueNames() {
    withSamePathInstances((stale, live) -> {
      final FlushPageIndex index = new FlushPageIndex();

      index.put(page(stale));
      assertCountsAgree(index, stale, 1);

      // The live instance indexes the same (fileId, pageNumber): the map keeps the stale instance's key object
      index.put(page(live));
      assertThat(index.get(new PageId(live, FILE_ID, PAGE_NUMBER)).getPageId().getDatabase()).isSameAs(live);

      assertCountsAgree(index, stale, 0);
      assertCountsAgree(index, live, 1);
    });
  }

  @Test
  void removingByPageIdReleasesTheCounterOfThePageItRemoved() {
    withSamePathInstances((stale, live) -> {
      final FlushPageIndex index = new FlushPageIndex();
      index.put(page(stale));
      index.put(page(live));

      // A detach by the STALE instance's PageId resolves to the slot holding the live instance's page (path equality)
      final MutablePage removed = index.remove(new PageId(stale, FILE_ID, PAGE_NUMBER));
      assertThat(removed.getPageId().getDatabase()).isSameAs(live);

      assertThat(index.isEmpty()).isTrue();
      assertCountsAgree(index, stale, 0);
      assertCountsAgree(index, live, 0);
    });
  }

  private interface SamePathBody {
    void run(DatabaseInternal stale, DatabaseInternal live);
  }

  /** Two {@code LocalDatabase} instances that are {@code equals()} (same path) but not the same instance. */
  private void withSamePathInstances(final SamePathBody body) {
    final String path = "target/databases/" + getClass().getSimpleName() + "-same-path";
    final Database stale = TestHelper.createDatabase(path);
    stale.drop();

    final Database live = TestHelper.createDatabase(path);
    try {
      assertThat(live).isEqualTo(stale);
      assertThat(live).isNotSameAs(stale);
      body.run((DatabaseInternal) stale, (DatabaseInternal) live);
    } finally {
      live.drop();
    }
  }

  private static MutablePage page(final DatabaseInternal database) {
    return new MutablePage(new PageId(database, FILE_ID, PAGE_NUMBER), PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
  }

  private static void assertCountsAgree(final FlushPageIndex index, final DatabaseInternal database, final int expected) {
    assertThat(index.pendingOf(database)).as("pending count of instance %s", System.identityHashCode(database))
        .isEqualTo(expected);
    assertThat(index.scanPendingOf(database)).as("scan of the index for instance %s", System.identityHashCode(database))
        .isEqualTo(expected);
  }
}
