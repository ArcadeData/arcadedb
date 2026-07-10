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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TransactionException;

import org.junit.jupiter.api.Test;

import java.io.InterruptedIOException;
import java.lang.reflect.Field;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the "interrupt must throw, never silently skip I/O" fixes.
 * <ul>
 *   <li>#4924 - {@code PageManager.concurrentPageAccess} used to exit its loop without performing the read/write
 *       when the thread was already interrupted, which let {@code loadPage} cache a zero-filled page and
 *       {@code flushPage} ack a flush that never reached disk.</li>
 *   <li>#4938 - {@code TransactionManager.writeTransactionToWAL} used to {@code break} out of its retry loop on
 *       interrupt and return as if the WAL write had happened, so the commit published pages with no WAL record.</li>
 * </ul>
 */
class InterruptedIOFailsLoudTest extends TestHelper {

  @Test
  void interruptedPageLoadThrowsInsteadOfCachingZeroPage() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.getSchema().createDocumentType("Doc");
    final MutableDocument[] holder = new MutableDocument[1];
    db.transaction(() -> holder[0] = db.newDocument("Doc").set("v", 42).save());

    final RID rid = holder[0].getIdentity();
    final int fileId = rid.getBucketId();
    final int pageSize = ((PaginatedComponent) db.getSchema().getBucketById(fileId)).getPageSize();
    final PageId pageId = new PageId(db, fileId, 0);

    final PageManager pm = db.getPageManager();
    // Push the page to disk and out of the flush queue, then drop it from the read cache, so the next read is a
    // genuine file read that goes through concurrentPageAccess (not a flush-queue or cache hit).
    pm.waitAllPagesOfDatabaseAreFlushed(db);
    pm.removeAllReadPagesOfDatabase(db);

    Thread.currentThread().interrupt();
    try {
      assertThatThrownBy(() -> pm.getImmutablePage(pageId, pageSize, false, false))
          .as("an interrupted page load must fail loud, not return a zero page")
          .isInstanceOf(InterruptedIOException.class);
    } finally {
      // Clear the interrupt flag so the rest of the test (and the framework) is unaffected.
      Thread.interrupted();
    }

    // The read cache must NOT have been poisoned with a zero-filled page: a normal read now succeeds and returns
    // the real committed page (version >= 1). A poisoned entry would have version 0.
    final ImmutablePage page = pm.getImmutablePage(pageId, pageSize, false, false);
    assertThat(page).isNotNull();
    assertThat(page.getVersion()).as("committed page must keep its real version, not a poisoned 0").isGreaterThanOrEqualTo(1);
    assertThat(db.query("sql", "SELECT v FROM Doc").next().<Integer>getProperty("v")).isEqualTo(42);
  }

  @Test
  void interruptedWalWriteThrowsInsteadOfCommittingWithoutWal() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final TransactionManager tm = db.getTransactionManager();

    // Force the WAL slot to be unavailable so writeTransactionToWAL enters its retry/sleep path, where an
    // interrupt used to break out silently. Restored in the finally block.
    final Field poolField = TransactionManager.class.getDeclaredField("activeWALFilePool");
    poolField.setAccessible(true);
    final WALFile[] original = (WALFile[]) poolField.get(tm);
    poolField.set(tm, new WALFile[] { null });

    Thread.currentThread().interrupt();
    try {
      assertThatThrownBy(() -> tm.writeTransactionToWAL(List.of(), WALFile.FlushType.NO, 1234L, new Binary()))
          .as("an interrupted WAL write must throw, not return as if it succeeded")
          .isInstanceOf(TransactionException.class);
    } finally {
      poolField.set(tm, original);
      Thread.interrupted();
    }
  }
}
