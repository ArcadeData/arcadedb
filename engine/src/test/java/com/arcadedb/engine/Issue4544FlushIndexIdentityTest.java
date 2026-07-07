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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4544.
 * <p>
 * The flush queue index ({@code PageManagerFlushThread.pageIndex}) keeps the most recent {@link MutablePage}
 * per {@link PageId} so reads can still find a page that has been polled from the queue but not yet flushed to
 * disk. When a page finishes flushing it must be removed from the index, but ONLY if a later transaction has
 * not meanwhile queued a NEWER page for the same {@link PageId}.
 * <p>
 * The removal used to be {@code pageIndex.remove(key, value)}, which resolves "is it still the same page?"
 * through {@link BasePage#equals}. Two pages for the same {@link PageId} compare equal there (equality keys on
 * pageId only, see #4722), so a value-based removal wrongly evicts a newer entry while flushing a stale page,
 * and subsequent reads miss the page and fall back to the older on-disk version (spurious MVCC conflicts).
 * The fix removes by reference identity instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4544FlushIndexIdentityTest extends TestHelper {

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
    flush.pageIndex.put(pageId, newer);

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

    flush.pageIndex.put(pageId, page);
    flush.removeFromFlushIndex(page);

    assertThat(flush.pageIndex.containsKey(pageId)).isFalse();
  }
}
