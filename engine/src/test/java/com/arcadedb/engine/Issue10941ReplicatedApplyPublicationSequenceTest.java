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
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10941: the replicated apply wrote the pages of a transaction into the read cache without moving
 * {@link PageManager#getPublicationSequence()}, so the #9369 check could not tell a replica reader that the pages of two commits
 * were mixed. The apply now publishes under the same odd/even protocol as a local commit.
 */
class Issue10941ReplicatedApplyPublicationSequenceTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "INSERT INTO Doc SET name = 'a'");
    });
  }

  /** A WAL entry that raises page 0 of the type's bucket by one version, with the content the page already holds. */
  private WALFile.WALTransaction entryAdvancingTheBucketPage() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = db.getSchema().getType("Doc").getBuckets(false).getFirst().getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final PageManager pageManager = db.getPageManager();
    assertThat(pageManager.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();

    final ImmutablePage page = pageManager.getImmutablePage(new PageId(db, fileId, 0), file.getPageSize(), false, true);

    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = fileId;
    walPage.pageNumber = 0;
    walPage.currentPageVersion = (int) page.getVersion() + 1;
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
    walPage.currentPageSize = page.getContentSize();
    final byte[] content = new byte[11];
    System.arraycopy(page.getContent().array(), walPage.changesFrom, content, 0, content.length);
    walPage.currentContent = new Binary(content);

    final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
    walTx.txId = 10941;
    walTx.timestamp = System.currentTimeMillis();
    walTx.pages = new WALFile.WALPage[] { walPage };
    return walTx;
  }

  @Test
  void aReplicatedApplyMovesTheSequenceByTwoAndLeavesItEven() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();
    final WALFile.WALTransaction entry = entryAdvancingTheBucketPage();
    final long before = pageManager.getPublicationSequence();
    assertThat(before & 1).isZero();

    final boolean applied = db.getTransactionManager().applyChanges(entry, Collections.emptyMap(), null, false);

    assertThat(applied).isTrue();
    assertThat(pageManager.getPublicationSequence()).isEqualTo(before + 2);
  }

  @Test
  void aFailedReplicatedApplyStillLeavesTheSequenceEven() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();
    final WALFile.WALTransaction entry = entryAdvancingTheBucketPage();
    // A version gap the apply refuses with an exception
    entry.pages[0].currentPageVersion += 5;
    final long before = pageManager.getPublicationSequence();

    try {
      db.getTransactionManager().applyChanges(entry, Collections.emptyMap(), null, false);
    } catch (final Exception expected) {
      // THE REFUSAL IS THE POINT OF THE TEST
    }

    assertThat(pageManager.getPublicationSequence() & 1).as("a sequence left odd would stall every lookup that waits for it").isZero();
    assertThat(pageManager.getPublicationSequence()).isGreaterThan(before);
  }

  /** A reader sampling the sequence while entries are applied sees it odd, which is what makes the #9369 check retry. */
  @Test
  void aReaderSeesTheSequenceOddWhileAnEntryIsBeingApplied() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicInteger oddSamples = new AtomicInteger();
    final Thread reader = new Thread(() -> {
      while (!stop.get())
        if ((pageManager.getPublicationSequence() & 1) != 0)
          oddSamples.incrementAndGet();
    });
    reader.start();
    try {
      for (int i = 0; i < 300 && oddSamples.get() == 0; i++)
        db.getTransactionManager().applyChanges(entryAdvancingTheBucketPage(), Collections.emptyMap(), null, false);
    } finally {
      stop.set(true);
      reader.join();
    }
    assertThat(oddSamples.get()).isPositive();
    assertThat(pageManager.getPublicationSequence() & 1).isZero();
  }

  /** The apply does not hold the page-manager lock, so it can overlap a local publication: neither may turn the counter even early. */
  @Test
  void overlappingPublicationsKeepTheSequenceOddUntilTheLastOneEnds() {
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final long before = pageManager.getPublicationSequence();

    pageManager.beginPublication();
    pageManager.beginPublication();
    assertThat(pageManager.getPublicationSequence()).isEqualTo(before + 1);
    pageManager.endPublication();
    assertThat(pageManager.getPublicationSequence() & 1).as("one publication is still in flight").isOne();
    pageManager.endPublication();
    assertThat(pageManager.getPublicationSequence()).isEqualTo(before + 2);
  }
}
