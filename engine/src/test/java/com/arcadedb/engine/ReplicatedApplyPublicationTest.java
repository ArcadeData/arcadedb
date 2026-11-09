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
import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Replicated apply versus the flush pipeline and concurrent readers: pending flush copies (issue #5326), the publication
 * sequence (issue #10941) and chain reads while an apply is open (issue #11283). Each section keeps its own fixture in a nested class.
 */
class ReplicatedApplyPublicationTest {
  // White-box regression test for the interaction between {@link TransactionManager#applyChanges} and the asynchronous
  // flush pipeline.
  // <p>
  // A committed page is published to the read cache and to the flush thread's {@code pageIndex} before it reaches the
  // disk. {@code applyChanges} (replicated/recovery replay) writes the page straight to the file, bypassing the queue.
  // While the older copy stayed in the pipeline two things went wrong:
  // <ul>
  //   <li>{@code PageManager.loadPage} resolves a page from the flush queue BEFORE reading the file, so every read after
  //   the replicated write kept returning the superseded version.</li>
  //   <li>when the flush thread eventually wrote the queued copy, it overwrote the replicated page on disk, rolling the
  //   page version backwards and re-opening the version-gap cascade the replay is meant to close.</li>
  // </ul>
  // Both outcomes depend on whether the flush thread happens to drain the queue before the read, which is what made the
  // {@code applyChanges} regression tests fail intermittently on loaded CI machines (issue #5326).
  // <p>
  // {@code applyChanges} now pushes any pending copy of the page to disk and detaches it from the pipeline before
  // applying the WAL delta, so the outcome no longer depends on flush timing.
  @Nested
  class PendingFlush extends TestHelper {
    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType("TestType");
    }

    @Test
    void applyChangesOverridesAPageStillPendingInTheFlushPipeline() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          db.newDocument("TestType").set("name", "record-" + i).save();
      });

      final int fileId = db.getSchema().getType("TestType").getBuckets(false).getFirst().getFileId();
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
      final int pageSize = file.getPageSize();
      final PageId pageId = new PageId(db, fileId, 0);
      final PageManager pageManager = db.getPageManager();

      // Start from a quiet pipeline so the only pending entry is the one this test publishes below.
      assertThat(pageManager.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();

      final ImmutablePage page = pageManager.getImmutablePage(pageId, pageSize, false, true);
      final int baseVersion = (int) page.getVersion();

      // Reproduce the state a page is in between commit and flush: published in the flush thread's index (so reads
      // resolve it from there) while the disk still holds the same version.
      final MutablePage pending = page.modify();
      pageManager.getFlushThread().pageIndex.put(pending);

      // Replicated/recovery WAL entry bumping the page by one version.
      final WALFile.WALPage walPage = new WALFile.WALPage();
      walPage.fileId = fileId;
      walPage.pageNumber = 0;
      walPage.currentPageVersion = baseVersion + 1;
      walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
      walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
      walPage.currentPageSize = page.getContentSize();
      final byte[] content = new byte[11];
      System.arraycopy(page.getContent().array(), walPage.changesFrom, content, 0, content.length);
      walPage.currentContent = new Binary(content);

      final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
      walTx.txId = 5326;
      walTx.timestamp = System.currentTimeMillis();
      walTx.pages = new WALFile.WALPage[] { walPage };

      assertThat(db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false)).isTrue();

      // No stale copy is left behind that a later flush would write over the replicated page.
      assertThat(pageManager.getFlushThread().pageIndex.get(pageId)).isNull();

      // The replicated version is what every subsequent read observes, cache-hit or reload from disk.
      assertThat(pageManager.getImmutablePage(pageId, pageSize, false, true).getVersion()).isEqualTo(baseVersion + 1);

      pageManager.removePageFromCache(pageId);
      final ImmutablePage reloaded = pageManager.getImmutablePage(pageId, pageSize, false, true);
      assertThat(reloaded.getVersion()).isEqualTo(baseVersion + 1);
    }

    /**
     * Same defect, but driven through the real flush pipeline instead of a fabricated index entry: the page is scheduled
     * by an ordinary commit while flushing is suspended, so it sits in a genuine deferred batch. This covers the batch
     * removal and the deferred-RAM accounting (#4728) that the white-box case above cannot reach, and it verifies the
     * durability half of the fix: once flushing resumes, the deferred batch must no longer carry a copy able to write
     * the superseded version over the replicated one.
     */
    @Test
    void applyChangesTakesTheDeferredBatchCopyOutOfThePipeline() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          db.newDocument("TestType").set("name", "record-" + i).save();
      });

      final int fileId = db.getSchema().getType("TestType").getBuckets(false).getFirst().getFileId();
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
      final int pageSize = file.getPageSize();
      final PageId pageId = new PageId(db, fileId, 0);
      final PageManager pageManager = db.getPageManager();
      final PageManagerFlushThread flushThread = pageManager.getFlushThread();

      assertThat(pageManager.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();

      final int replicatedVersion;

      // Suspend flushing (what a backup or an HA snapshot ship does) so the next commit's pages are parked in a
      // deferred batch instead of reaching the disk.
      flushThread.setSuspended(db, true);
      try {
        db.transaction(() -> {
          for (int i = 0; i < 10; i++)
            db.newDocument("TestType").set("name", "deferred-" + i).save();
        });

        awaitEveryCommittedBatchDeferred(flushThread);

        final MutablePage pending = flushThread.pageIndex.get(pageId);
        assertThat(pending).as("the committed page must still be pending in the flush pipeline").isNotNull();
        assertThat(flushThread.deferredRAMBytes.get()).isPositive();

        final long deferredBefore = flushThread.deferredRAMBytes.get();
        final long pendingSize = pending.getPhysicalSize();
        final int baseVersion = (int) pending.getVersion();
        replicatedVersion = baseVersion + 1;

        final WALFile.WALPage walPage = new WALFile.WALPage();
        walPage.fileId = fileId;
        walPage.pageNumber = 0;
        walPage.currentPageVersion = baseVersion + 1;
        walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
        walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
        walPage.currentPageSize = pending.getContentSize();
        final byte[] content = new byte[11];
        System.arraycopy(pending.getContent().array(), walPage.changesFrom, content, 0, content.length);
        walPage.currentContent = new Binary(content);

        final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
        walTx.txId = 53260;
        walTx.timestamp = System.currentTimeMillis();
        walTx.pages = new WALFile.WALPage[] { walPage };

        assertThat(db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false)).isTrue();

        assertThat(flushThread.pageIndex.get(pageId)).isNull();
        assertThat(flushThread.deferredRAMBytes.get()).isEqualTo(deferredBefore - pendingSize);
      } finally {
        // Resuming writes every batch still deferred: none of them may carry the superseded copy of this page.
        flushThread.setSuspended(db, false);
      }

      assertThat(pageManager.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();
      pageManager.removePageFromCache(pageId);
      final ImmutablePage reloaded = pageManager.getImmutablePage(pageId, pageSize, false, true);
      assertThat(reloaded.getVersion()).isEqualTo(replicatedVersion);
    }

    /**
     * Two commits of the same page while flushing is suspended leave TWO pending copies: the newest in
     * {@code pageIndex} and the older one still in the first batch (the two-instance case
     * {@link PageManagerFlushThread#removeFromFlushIndex} exists for, issue #4544). Detaching only the indexed copy
     * would leave the older one free to write the superseded version over the replicated one once flushing resumes,
     * so every copy has to leave the pipeline.
     */
    @Test
    void applyChangesTakesEveryPendingCopyOfThePageOutOfThePipeline() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      db.transaction(() -> db.newDocument("TestType").set("name", "seed").save());

      final int fileId = db.getSchema().getType("TestType").getBuckets(false).getFirst().getFileId();
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
      final int pageSize = file.getPageSize();
      final PageId pageId = new PageId(db, fileId, 0);
      final PageManager pageManager = db.getPageManager();
      final PageManagerFlushThread flushThread = pageManager.getFlushThread();

      assertThat(pageManager.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();

      final int replicatedVersion;

      flushThread.setSuspended(db, true);
      try {
        // Two separate commits: two batches, each carrying its own MutablePage for page 0.
        for (int tx = 0; tx < 2; tx++) {
          final int round = tx;
          db.transaction(() -> {
            for (int i = 0; i < 5; i++)
              db.newDocument("TestType").set("name", "round-" + round + "-" + i).save();
          });
        }

        awaitEveryCommittedBatchDeferred(flushThread);

        final MutablePage newest = flushThread.pageIndex.get(pageId);
        assertThat(newest).as("the twice-committed page must still be pending in the flush pipeline").isNotNull();

        final int baseVersion = (int) newest.getVersion();
        assertThat(baseVersion).as("both commits must have touched page 0, leaving two pending copies").isGreaterThan(1);
        replicatedVersion = baseVersion + 1;

        final WALFile.WALPage walPage = new WALFile.WALPage();
        walPage.fileId = fileId;
        walPage.pageNumber = 0;
        walPage.currentPageVersion = replicatedVersion;
        walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
        walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
        walPage.currentPageSize = newest.getContentSize();
        final byte[] content = new byte[11];
        System.arraycopy(newest.getContent().array(), walPage.changesFrom, content, 0, content.length);
        walPage.currentContent = new Binary(content);

        final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
        walTx.txId = 53261;
        walTx.timestamp = System.currentTimeMillis();
        walTx.pages = new WALFile.WALPage[] { walPage };

        final long deferredBefore = flushThread.deferredRAMBytes.get();

        assertThat(db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false)).isTrue();
        assertThat(flushThread.pageIndex.get(pageId)).isNull();

        // Both copies left the deferred backlog, not just the indexed one.
        assertThat(deferredBefore - flushThread.deferredRAMBytes.get())
            .as("every pending copy of the page must be detached, not only the one in pageIndex")
            .isEqualTo(2L * newest.getPhysicalSize());
      } finally {
        flushThread.setSuspended(db, false);
      }

      // Resuming writes every batch still deferred. If the older copy had survived the detach it would land here and
      // roll the page back below the replicated version.
      assertThat(pageManager.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();
      pageManager.removePageFromCache(pageId);
      final ImmutablePage reloaded = pageManager.getImmutablePage(pageId, pageSize, false, true);
      assertThat(reloaded.getVersion()).isEqualTo(replicatedVersion);
    }

    /**
     * Waits until EVERY batch committed while suspended has been polled out of the flush queue and parked in the
     * deferred backlog.
     * <p>
     * The condition is the drained pipeline, not {@code deferredRAMBytes > 0}: with more than one commit, the first
     * deferral says nothing about the second batch, which can still be sitting in the queue - and a copy of the page
     * that is in the QUEUE rather than in the deferred map is detached without touching the deferred accounting, so
     * the "both copies left the backlog" assertion would see one page's worth and fail, blaming the detach for a race
     * in the wait. The deadline only bounds a hang: the flush thread polls on a 1 s timeout, so 60 s is orders of
     * magnitude above the worst-case deferral latency even on the loaded runners that made #5326 visible.
     */
    private static void awaitEveryCommittedBatchDeferred(final PageManagerFlushThread flushThread) throws InterruptedException {
      final long deadline = System.currentTimeMillis() + 60_000;
      while (System.currentTimeMillis() < deadline) {
        if (flushThread.deferredRAMBytes.get() > 0 && flushThread.queue.isEmpty() && flushThread.nextPagesToFlush.get() == null)
          return;
        Thread.sleep(10);
      }
    }
  }

  // Issue #10941: the replicated apply wrote the pages of a transaction into the read cache without moving
  // {@link PageManager#getPublicationSequence()}, so the #9369 check could not tell a replica reader that the pages of two commits
  // were mixed. The apply now publishes under the same odd/even protocol as a local commit.
  @Nested
  class PublicationSequence extends TestHelper {
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

  // Issue #11283: the "one commit" proofs of the multi-page record read redo their check under the publication lock when a
  // publication overlapped. On a replica that lock does not exclude the replicated apply, so a publication still open (or moved)
  // inside the lock voids the proof: a chain in motion is reported as changed, never accepted, and never condemned as broken.
  // The test holds a publication open with {@link PageManager#beginPublication()}, which is what the replicated apply does.
  @Nested
  class ChainReadUnderApply extends BucketPageLayoutTestSupport {
    @Test
    void aChainReadWhileAnApplyIsOpenIsNotAnsweredFromTheLock() {
      final RID[] rids = createChunkedRecords("Read");
      final LocalBucket bucket = bucketOf("Read");
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final byte[] expected = bucket.getRecordFromCommittedPage(rids[0]).toByteArray();

      pageManager.beginPublication();
      try {
        assertThatThrownBy(() -> bucket.getRecordFromCommittedPage(rids[0])).as("a chain in motion must not be accepted as one commit")
            .isInstanceOf(ConcurrentModificationException.class);
      } finally {
        pageManager.endPublication();
      }

      assertThat(bucket.getRecordFromCommittedPage(rids[0]).toByteArray()).as("settled again: read as before").isEqualTo(expected);
    }

    @Test
    void aPlaceholderReadWhileAnApplyIsOpenIsNotAnsweredFromTheLock() {
      database.transaction(() -> database.getSchema().createDocumentType("Placeholder", 1).createProperty("v", Type.STRING));
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Placeholder").set("v", "p").save().getIdentity());
      sealFirstPage("Placeholder");
      database.transaction(() -> rid[0].asDocument(true).modify().set("v", "b".repeat(20 * 1024)).save());
      assertThat((Long) bucketStats("Placeholder").get("totalPlaceholderRecords")).as("the fixture needs a placeholder").isEqualTo(1L);
      final LocalBucket bucket = bucketOf("Placeholder");
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final byte[] expected = bucket.getRecordFromCommittedPage(rid[0]).toByteArray();

      pageManager.beginPublication();
      try {
        assertThatThrownBy(() -> bucket.getRecordFromCommittedPage(rid[0])).isInstanceOf(ConcurrentModificationException.class);
      } finally {
        pageManager.endPublication();
      }

      assertThat(bucket.getRecordFromCommittedPage(rid[0]).toByteArray()).isEqualTo(expected);
    }

    @Test
    void aProofOfCorruptionIsVoidWhileAnApplyIsOpen() throws Exception {
      createChunkedRecords("Read");
      final LocalBucket bucket = bucketOf("Read");
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();

      assertThat(bucket.provenAgainstOneCommit(() -> "broken")).as("no publication: the proof stands").isEqualTo("broken");

      pageManager.beginPublication();
      try {
        assertThat(bucket.provenAgainstOneCommit(() -> "broken")).as("a publication open inside the lock voids the proof").isNull();
      } finally {
        pageManager.endPublication();
      }
    }

    @Test
    void aProofIsVoidWhenAPublicationMovesDuringIt() throws Exception {
      createChunkedRecords("Read");
      final LocalBucket bucket = bucketOf("Read");
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final AtomicInteger runs = new AtomicInteger();

      // The first run overlaps a publication (so the lock is taken), the second one, inside the lock, is overlapped again
      assertThat(bucket.provenAgainstOneCommit(() -> {
        pageManager.beginPublication();
        pageManager.endPublication();
        runs.incrementAndGet();
        return "broken";
      })).isNull();
      assertThat(runs.get()).isEqualTo(2);
    }
  }
}
