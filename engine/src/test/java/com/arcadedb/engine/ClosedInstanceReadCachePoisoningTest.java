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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12244: the JVM-wide read cache keys a page by its {@link PageId}, whose database part compares by PATH, so a page
 * cached for a CLOSED instance is a hit for the next instance opened on the same directory. The close purges the pages of
 * the instance, but a reader of the closing instance that loaded a page before that purge caches it only after
 * ({@link PageManager#putPageInReadCache} runs once the I/O lock is released). When the files under the path are then
 * replaced by a newer copy (the snapshot install of a Raft follower: close, swap the files, reopen) the stale image
 * answers for the page instead of the file, one or more versions behind it: the next replicated entry on the page is
 * refused with a WAL version gap, and every reader sees the older record.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class ClosedInstanceReadCachePoisoningTest extends TestHelper {

  @Test
  void aPageCachedForAClosedInstanceDoesNotAnswerForTheNextInstanceOnThePath() throws Exception {
    // Another database stays open for the whole test, as on a server: with none open the page manager shuts down and
    // drops its whole read cache, which would hide the stale page
    final DatabaseFactory otherFactory = new DatabaseFactory(getDatabasePath() + "-other");
    if (otherFactory.exists())
      otherFactory.open().drop();
    final Database other = otherFactory.create();
    try {
      reproduce();
    } finally {
      other.drop();
      otherFactory.close();
    }
  }

  private void reproduce() throws Exception {
    database.getSchema().createDocumentType("Doc");

    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc");
      doc.set("value", "old");
      doc.save();
      rid[0] = doc.getIdentity();
    });

    // The first instance: a reader of it loads the record's page
    final DatabaseInternal first = (DatabaseInternal) database;
    final PageManager pm = first.getPageManager();
    final LocalBucket bucket = (LocalBucket) first.getSchema().getBucketById(rid[0].getBucketId());
    final int pageNumber = (int) (rid[0].getPosition() / bucket.getMaxRecordsInPage());
    final int pageSize = bucket.getPageSize();
    final CachedPage loadedByLateReader = pm.getCachedPageForRead(new PageId(first, bucket.getFileId(), pageNumber), pageSize);
    assertThat(loadedByLateReader).isNotNull();
    final int staleVersion = loadedByLateReader.getVersion();

    // The files under the path move on without that instance: here a second instance updates the record and closes,
    // which is what a snapshot install leaves behind (a newer copy on disk, nothing of it in the read cache)
    reopenDatabase();
    database.transaction(() -> {
      final MutableDocument doc = database.lookupByRID(rid[0], true).asDocument().modify();
      doc.set("value", "new");
      doc.save();
    });
    database.close();
    assertThat(first.isOpen()).isFalse();

    // The late reader of the first instance caches what it loaded, after that instance's close purged its pages
    pm.putPageInReadCache(loadedByLateReader);

    // The next instance on the path must read the page from its files, not the closed instance's image
    database = factory.open();
    final DatabaseInternal reopened = (DatabaseInternal) database;
    final ImmutablePage page = pm.getImmutablePage(new PageId(reopened, bucket.getFileId(), pageNumber), pageSize, false, false);
    assertThat(page.getVersion())
        .as("the page must be read at the version of the files under the path, not at the version a closed instance cached")
        .isGreaterThan(staleVersion);
    assertThat(database.lookupByRID(rid[0], true).asDocument().getString("value"))
        .as("a record read through the reopened instance must show the last committed value")
        .isEqualTo("new");
  }
}
