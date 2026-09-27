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
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A committed page whose asynchronous flush failed (a full disk, an I/O error) used to leave the flush pipeline and
 * survive only in the read cache. Once the cache evicted it, the next read loaded the older image from the file, and
 * so did the commit-time version probe: a transaction built on that image passed its MVCC check and silently
 * overwrote the committed update - on a bucket page, a record rewritten from stale content, or a chunk slot handed to
 * a second record while the first one still points at it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FailedFlushPageStaysReadableTest extends TestHelper {

  @Test
  void aCommittedPageWhoseFlushFailedIsNeverServedFromTheStaleFile() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID[] rids = new RID[2];
    db.transaction(() -> {
      db.getSchema().createDocumentType("Doc", 1);
      rids[0] = db.newDocument("Doc").set("v", 0).save().getIdentity();
      rids[1] = db.newDocument("Doc").set("v", 0).save().getIdentity();
    });
    final PageManager pageManager = db.getPageManager();
    pageManager.waitAllPagesOfDatabaseAreFlushed(db);

    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(rids[0].getBucketId());
    final PageId pageId = new PageId(db, bucket.getFileId(), (int) (rids[0].getPosition() / bucket.getMaxRecordsInPage()));
    assertThat(rids[1].getPosition() / bucket.getMaxRecordsInPage()).as("both records share one page").isEqualTo(pageId.getPageNumber());

    final AtomicInteger failedWrites = new AtomicInteger();
    PageManager.setPageWriteFaultInjector(id -> {
      if (id.getFileId() == pageId.getFileId()) {
        failedWrites.incrementAndGet();
        throw new IOException("injected write failure");
      }
    });
    try {
      db.transaction(() -> rids[0].asDocument(true).modify().set("v", 1).save());
      pageManager.waitAllPagesOfDatabaseAreFlushed(db);
      assertThat(failedWrites.get()).as("the flush of the updated page must have been attempted and failed").isPositive();

      // The read cache lets the page go, as it does under memory pressure
      pageManager.removePageFromCache(pageId);
      assertThat(readV(rids[0])).as("the committed update must not be read back from the stale file").isEqualTo(1);

      // A transaction on ANOTHER record of the same page must build on the committed image, not on the file
      pageManager.removePageFromCache(pageId);
      db.transaction(() -> rids[1].asDocument(true).modify().set("v", 2).save());
      pageManager.removePageFromCache(pageId);
      assertThat(readV(rids[0])).as("the first update must survive the second transaction").isEqualTo(1);
      assertThat(readV(rids[1])).isEqualTo(2);
    } finally {
      PageManager.setPageWriteFaultInjector(null);
    }

    // With the disk healthy again the failed page is retried and reaches the file
    final long deadline = System.currentTimeMillis() + 30_000;
    while (pageManager.getStats().pagesFailedToFlush > 0 && System.currentTimeMillis() < deadline)
      Thread.sleep(50);
    assertThat(pageManager.getStats().pagesFailedToFlush).as("the failed page must be retried once the disk recovers").isZero();

    pageManager.removePageFromCache(pageId);
    assertThat(readV(rids[0])).isEqualTo(1);
    assertThat(readV(rids[1])).isEqualTo(2);
  }

  private int readV(final RID rid) {
    final int[] v = new int[1];
    database.transaction(() -> v[0] = rid.asDocument(true).getInteger("v"));
    return v[0];
  }
}
