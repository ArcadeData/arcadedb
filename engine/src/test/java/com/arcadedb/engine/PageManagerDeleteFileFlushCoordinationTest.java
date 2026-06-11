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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4545: {@code PageManager.deleteFile} evicted only the
 * {@code readCache} entries for the dropped fileId, while the asynchronous flush thread
 * still held {@link MutablePage}s for that same fileId inside its {@code pageIndex} and
 * flush queue.
 * <p>
 * That left a per-fileId memory leak in {@code pageIndex} and a window where a page for a
 * dropped file could still be flushed or served back to {@code loadPage}. The fix drains
 * the flush thread's queue and index for the fileId in the same call that drops the file.
 */
class PageManagerDeleteFileFlushCoordinationTest extends TestHelper {

  private static final int FILE_ID    = 9_999;
  private static final int PAGE_SIZE  = 4_096;
  private static final int NUM_PAGES  = 8;

  @Test
  void deleteFileDrainsFlushThreadIndexAndQueue() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();
    final PageManagerFlushThread flushThread = pageManager.getFlushThread();

    // Suspend asynchronous flushing for this database so the scheduled pages stay parked
    // in the flush thread's pageIndex/queue and are NOT drained to disk before we assert.
    flushThread.setSuspended(db, true);
    try {
      // Schedule a batch of mutable pages for a synthetic fileId that does not exist in the
      // FileManager. They land in the flush thread's pageIndex (and queue) but are never
      // written to disk because flushPage() skips non-existent files.
      final java.util.List<MutablePage> pages = new java.util.ArrayList<>(NUM_PAGES);
      for (int i = 0; i < NUM_PAGES; i++)
        pages.add(new MutablePage(new PageId(db, FILE_ID, i), PAGE_SIZE));

      flushThread.scheduleFlushOfPages(pages);

      // Sanity: at least one of the scheduled pages must be visible in the flush thread index
      // before the drop, otherwise the test would pass vacuously.
      assertThat(anyPageStillIndexed(flushThread))
          .as("Scheduled pages must be present in the flush thread index before deleteFile")
          .isTrue();

      // Drop the file: this must also drain the flush thread's index/queue for that fileId.
      pageManager.deleteFile(db, FILE_ID);

      assertThat(anyPageStillIndexed(flushThread))
          .as("After deleteFile no page for the dropped fileId may remain in the flush thread index")
          .isFalse();
    } finally {
      flushThread.setSuspended(db, false);
    }
  }

  private boolean anyPageStillIndexed(final PageManagerFlushThread flushThread) {
    for (int i = 0; i < NUM_PAGES; i++)
      if (flushThread.getCachedPageFromMutablePageInQueue(new PageId((DatabaseInternal) database, FILE_ID, i)) != null)
        return true;
    return false;
  }
}
