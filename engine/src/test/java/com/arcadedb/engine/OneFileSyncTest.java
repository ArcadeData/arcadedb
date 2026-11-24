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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link PageManagerFlushThread#waitPagesOfFileAreFlushed}, the wait of the one-file sync a vector graph build runs while it
 * holds the build lock the searches of its index wait on (issue #11805): it does not spin on a page the disk refused.
 */
class OneFileSyncTest extends TestHelper {

  /**
   * A copy of a page the disk refused stays in the pipeline but is not counted as pending, so the drain wait returns at
   * once for it: with only such copies left, the wait for the file's pages must still sleep between rechecks instead of
   * spinning on the CPU while the caller holds the graph-build lock.
   */
  @Test
  void waitingOnlyForARefusedPageDoesNotSpin() throws Exception {
    database.getSchema().createDocumentType("Refused", 1);
    database.transaction(() -> database.newDocument("Refused").set("i", 1).save());
    final DatabaseInternal db = (DatabaseInternal) database;
    assertThat(db.getPageManager().waitPagesPendingNowOfDatabaseAreFlushed(db, 30_000)).isTrue();
    final int fileId = db.getSchema().getType("Refused").getBuckets(false).getFirst().getFileId();
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();

    final Field threadField = PageManager.class.getDeclaredField("flushThread");
    threadField.setAccessible(true);
    final PageManagerFlushThread thread = (PageManagerFlushThread) threadField.get(db.getPageManager());
    final Field retryField = PageManagerFlushThread.class.getDeclaredField("lastFailedFlushRetryMs");
    retryField.setAccessible(true);
    final Field failedField = PageManagerFlushThread.class.getDeclaredField("failedFlushPages");
    failedField.setAccessible(true);
    final Map<?, ?> failed = (Map<?, ?>) failedField.get(thread);

    final PageId pageId = new PageId(db, fileId, 0);
    // No retry while the test runs: the copy stays refused
    retryField.setLong(thread, Long.MAX_VALUE);
    thread.registerFailedFlush(new MutablePage(pageId, pageSize));
    try {
      final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
      final long cpuBefore = threads.getCurrentThreadCpuTime();
      final int stillPending = thread.waitPagesOfFileAreFlushed(db, fileId, new int[] { 0 }, 1, 1_000);
      final long cpuMillis = (threads.getCurrentThreadCpuTime() - cpuBefore) / 1_000_000;

      assertThat(stillPending).isEqualTo(1);
      assertThat(cpuMillis).as("CPU spent waiting 1 s for a refused page").isLessThan(300);
    } finally {
      failed.remove(pageId);
      retryField.setLong(thread, 0L);
    }
  }
}
