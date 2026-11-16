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
import com.arcadedb.exception.BrokenChunkChainException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

    /** #11455: a hold is granted only with no publication open, and a caller that cannot get one in time is told so. */
    @Test
    void aHoldIsRefusedWhileAPublicationStaysOpen() throws Exception {
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final AtomicBoolean ran = new AtomicBoolean();

      pageManager.beginPublication();
      try {
        assertThat(pageManager.callWithPublicationsHeld(TimeUnit.MILLISECONDS.toNanos(5), () -> ran.getAndSet(true))).isNull();
        assertThat(ran.get()).as("the callable never runs without the hold").isFalse();
      } finally {
        pageManager.endPublication();
      }

      assertThat(pageManager.callWithPublicationsHeld(TimeUnit.MILLISECONDS.toNanos(5), () -> "held")).isEqualTo("held");
    }

    /** #11455: no publication starts while a hold is granted, so what the holder reads is one committed state. */
    @Test
    void aPublicationWaitsForTheHoldToBeReleased() throws Exception {
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final CountDownLatch holding = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final AtomicBoolean published = new AtomicBoolean();

      final Thread holder = new Thread(() -> {
        try {
          pageManager.callWithPublicationsHeld(TimeUnit.SECONDS.toNanos(5), () -> {
            holding.countDown();
            return release.await(10, TimeUnit.SECONDS);
          });
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
      holder.start();
      assertThat(holding.await(5, TimeUnit.SECONDS)).isTrue();

      final long before = pageManager.getPublicationSequence();
      final Thread publisher = new Thread(() -> {
        pageManager.beginPublication();
        published.set(true);
        pageManager.endPublication();
      });
      publisher.start();
      try {
        publisher.join(50);
        assertThat(published.get()).as("the publication waits for the hold").isFalse();
        assertThat(pageManager.getPublicationSequence()).isEqualTo(before);
      } finally {
        release.countDown();
        holder.join();
        publisher.join();
      }
      assertThat(published.get()).isTrue();
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

    /**
     * #11455: a transactional read of a multi-page record that meets a publication (a replicated apply on a follower) used to burn
     * its whole retry budget in microseconds, because the validation did not wait for the publication to end. A follower under
     * write load always has one open, so the reads failed by the thousand. The publication here closes only once the reader has
     * retried, which the old budget could not wait for.
     */
    @Test
    void aTransactionalReadWaitsOutAnApplyInsteadOfSpendingItsRetriesOnIt() throws Exception {
      final RID[] rids = createChunkedRecords("Read");
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final byte[] expected = database.lookupByRID(rids[0], true).asDocument().getString("payload").getBytes();
      final AtomicBoolean released = new AtomicBoolean();

      pageManager.beginPublication();
      final long openedAt = System.nanoTime();
      final Thread closer = new Thread(() -> {
        // Held for longer than the old read took to spend its whole budget (microseconds), but for less than the read that
        // waits for the apply to end is prepared to wait (20 ms per attempt)
        while (System.nanoTime() - openedAt < TimeUnit.MILLISECONDS.toNanos(30))
          Thread.onSpinWait();
        if (!released.getAndSet(true))
          pageManager.endPublication();
      });
      closer.start();
      try {
        final byte[][] read = new byte[1][];
        database.transaction(() -> read[0] = database.lookupByRID(rids[0], true).asDocument().getString("payload").getBytes());
        assertThat(read[0]).as("the read served the committed record once the apply ended").isEqualTo(expected);
      } finally {
        if (!released.getAndSet(true))
          pageManager.endPublication();
        closer.join();
      }
    }

    /**
     * Nightly Load Tests after #11455: a follower applies its entries back to back, so the sequence is even only for the instants
     * between two of them, far shorter than a validation. The wait of #11455 found one of those instants, the next apply opened
     * during the validation, and the redo under the page-manager lock, which does not exclude an apply, found it open again: every
     * attempt failed and the read raised "modified during read after 3 retries" on a record nobody had touched. The redo now holds
     * the next publication off for the validation, so a read of an unchanged record is served however busy the apply is.
     */
    @Test
    void aTransactionalReadIsServedWhileEntriesAreAppliedBackToBack() throws Exception {
      final RID[] rids = createChunkedRecords("Read");
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final String expected = database.lookupByRID(rids[0], true).asDocument().getString("payload");
      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicInteger applied = new AtomicInteger();

      final Thread applier = new Thread(() -> {
        while (!stop.get()) {
          pageManager.beginPublication();
          try {
            final long until = System.nanoTime() + TimeUnit.MICROSECONDS.toNanos(200);
            while (System.nanoTime() < until)
              Thread.onSpinWait();
          } finally {
            pageManager.endPublication();
          }
          applied.incrementAndGet();
        }
      });
      applier.start();
      try {
        for (int i = 0; i < 50; i++) {
          final String[] read = new String[1];
          database.transaction(() -> read[0] = database.lookupByRID(rids[0], true).asDocument().getString("payload"));
          assertThat(read[0]).as("read %d served the committed record", i).isEqualTo(expected);
        }
      } finally {
        stop.set(true);
        applier.join();
      }
      assertThat(applied.get()).as("the applier kept publishing while the reads ran").isPositive();
      assertThat(pageManager.getPublicationSequence() & 1).isZero();
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

    /**
     * #11505: a proof that a publication overlapped is run again with the publications held off, so the second run reads one
     * committed state and its verdict stands. The redo used to run under the page-manager lock, which does not exclude the
     * replicated apply (#11283), and a publication overlapping it voided the proof as well. A publication cannot start while the
     * hold is granted, so the second run here is not overlapped: a proof run under the hold that starts one would wait for itself.
     */
    @Test
    void aProofOverlappedByAPublicationIsRunAgainWithThePublicationsHeld() throws Exception {
      createChunkedRecords("Read");
      final LocalBucket bucket = bucketOf("Read");
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final AtomicInteger runs = new AtomicInteger();

      assertThat(bucket.provenAgainstOneCommit(() -> {
        if (runs.incrementAndGet() == 1) {
          // only the first run is overlapped
          pageManager.beginPublication();
          pageManager.endPublication();
        }
        return "broken";
      })).as("the redo ran against one committed state, so its proof stands").isEqualTo("broken");
      assertThat(runs.get()).as("the overlapped proof was run again").isEqualTo(2);
      assertThat(pageManager.getPublicationSequence() & 1).isZero();
    }

    /**
     * #11505: the proof that a chunk chain is corrupt did not wait out an open publication, as its two siblings do since #11455:
     * on a replica, where one is nearly always open, it ran at once on the odd sequence, its redo under the page-manager lock found
     * the apply still open, and the corruption was never proven: the read retried to exhaustion and reported contention. The
     * publication here ends a few milliseconds after the proof starts, well inside the wait.
     */
    @Test
    void aProofOfCorruptionWaitsOutAnApplyThatEnds() throws Exception {
      createChunkedRecords("Read");
      final LocalBucket bucket = bucketOf("Read");
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final AtomicBoolean released = new AtomicBoolean();

      pageManager.beginPublication();
      final long openedAt = System.nanoTime();
      final Thread closer = new Thread(() -> {
        while (System.nanoTime() - openedAt < TimeUnit.MILLISECONDS.toNanos(5))
          Thread.onSpinWait();
        if (!released.getAndSet(true))
          pageManager.endPublication();
      });
      closer.start();
      try {
        assertThat(bucket.provenAgainstOneCommit(() -> "broken")).as("the proof waited for the apply to end and stands")
            .isEqualTo("broken");
      } finally {
        if (!released.getAndSet(true))
          pageManager.endPublication();
        closer.join();
      }
    }

    /**
     * #11505: a replica applies its entries back to back, so the sequence is even only between two of them. A proof of corruption
     * must still reach a verdict: it waits for one of those instants and, when the next apply overlaps it, runs again with the
     * publications held off rather than under the page-manager lock, which the apply does not take.
     */
    @Test
    void aProofOfCorruptionStandsWhileEntriesAreAppliedBackToBack() throws Exception {
      createChunkedRecords("Read");
      final LocalBucket bucket = bucketOf("Read");
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicInteger applied = new AtomicInteger();

      final Thread applier = applyBackToBack(pageManager, stop, applied);
      try {
        for (int i = 0; i < 50; i++)
          assertThat(bucket.provenAgainstOneCommit(() -> "broken")).as("proof %d reached a verdict", i).isEqualTo("broken");
      } finally {
        stop.set(true);
        applier.join();
      }
      assertThat(applied.get()).as("the applier kept publishing while the proofs ran").isPositive();
    }

    /**
     * #11505: the placeholder pointer and its content are read against one commit the same way, and did not wait for an open
     * publication either: a placeholder read on a replica that met an apply failed with a concurrent modification although the
     * apply ended a few milliseconds later.
     */
    @Test
    void aPlaceholderReadWaitsOutAnApplyThatEnds() throws Exception {
      final RID rid = createPlaceholder();
      final LocalBucket bucket = bucketOf("Placeholder");
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final byte[] expected = bucket.getRecordFromCommittedPage(rid).toByteArray();
      final AtomicBoolean released = new AtomicBoolean();

      pageManager.beginPublication();
      final long openedAt = System.nanoTime();
      final Thread closer = new Thread(() -> {
        while (System.nanoTime() - openedAt < TimeUnit.MILLISECONDS.toNanos(5))
          Thread.onSpinWait();
        if (!released.getAndSet(true))
          pageManager.endPublication();
      });
      closer.start();
      try {
        assertThat(bucket.getRecordFromCommittedPage(rid).toByteArray()).as("served once the apply ended").isEqualTo(expected);
      } finally {
        if (!released.getAndSet(true))
          pageManager.endPublication();
        closer.join();
      }
    }

    /** #11505: a placeholder read is served while entries are applied back to back, as a multi-page read is since #11455. */
    @Test
    void aPlaceholderReadIsServedWhileEntriesAreAppliedBackToBack() throws Exception {
      final RID rid = createPlaceholder();
      final LocalBucket bucket = bucketOf("Placeholder");
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final byte[] expected = bucket.getRecordFromCommittedPage(rid).toByteArray();
      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicInteger applied = new AtomicInteger();

      final Thread applier = applyBackToBack(pageManager, stop, applied);
      try {
        for (int i = 0; i < 50; i++)
          assertThat(bucket.getRecordFromCommittedPage(rid).toByteArray()).as("read %d served", i).isEqualTo(expected);
      } finally {
        stop.set(true);
        applier.join();
      }
      assertThat(applied.get()).as("the applier kept publishing while the reads ran").isPositive();
    }

    private RID createPlaceholder() {
      database.transaction(() -> database.getSchema().createDocumentType("Placeholder", 1).createProperty("v", Type.STRING));
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Placeholder").set("v", "p").save().getIdentity());
      sealFirstPage("Placeholder");
      database.transaction(() -> rid[0].asDocument(true).modify().set("v", "b".repeat(20 * 1024)).save());
      assertThat((Long) bucketStats("Placeholder").get("totalPlaceholderRecords")).as("the fixture needs a placeholder").isEqualTo(1L);
      return rid[0];
    }
  }

  // Issue #11505: a genuinely broken chunk chain met on a replica under write load must be reported as corruption, pointing at
  // CHECK DATABASE FIX, on the read path and on the delete path alike, instead of being retried as contention until the budget
  // runs out. The replicated apply is played by a thread opening and closing publications back to back.
  @Nested
  class BrokenChainUnderApply extends BucketPageLayoutTestSupport {
    private static final String TYPE               = "Broken";
    /** {@code LocalBucket.FIRST_CHUNK} (-2), as the single zigzag byte it is stored as. */
    private static final byte   FIRST_CHUNK_MARKER = 3;

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // every test here injects on-disk corruption, which the post-test integrity check would (correctly) flag
      return false;
    }

    @Test
    void aBrokenChainReadWhileEntriesAreAppliedBackToBackIsReportedAsCorruption() throws Exception {
      final RID broken = createBrokenRecord();
      final LocalBucket bucket = bucketOf(TYPE);
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicInteger applied = new AtomicInteger();

      final Thread applier = applyBackToBack(pageManager, stop, applied);
      try {
        for (int i = 0; i < 10; i++) {
          database.begin();
          try {
            assertThatThrownBy(() -> bucket.getRecordInternal(broken, false)).as("read %d", i)
                .isInstanceOf(BrokenChunkChainException.class)
                .hasMessageContaining("CHECK DATABASE FIX");
          } finally {
            database.rollback();
          }
        }
      } finally {
        stop.set(true);
        applier.join();
      }
      assertThat(applied.get()).as("the applier kept publishing while the reads ran").isPositive();
    }

    @Test
    void aBrokenChainDeleteWhileEntriesAreAppliedBackToBackIsReportedAsCorruption() throws Exception {
      final RID broken = createBrokenRecord();
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicInteger applied = new AtomicInteger();

      final Thread applier = applyBackToBack(pageManager, stop, applied);
      try {
        for (int i = 0; i < 10; i++) {
          database.begin();
          try {
            assertThatThrownBy(() -> TestHelper.deleteRecordAtLowLevel(database, broken)).as("delete %d", i)
                .isInstanceOf(BrokenChunkChainException.class);
          } finally {
            database.rollback();
          }
        }
      } finally {
        stop.set(true);
        applier.join();
      }
      assertThat(applied.get()).as("the applier kept publishing while the deletes ran").isPositive();
      assertThat(database.getSchema().getBucketById(broken.getBucketId()).existsRecord(broken)).isTrue();
    }

    /** A multi-page record whose head chunk points at a page the file does not have: broken at chunk 0. */
    private RID createBrokenRecord() {
      final RID[] rids = createChunkedRecords(TYPE);
      final RID broken = rids[0];
      final int maxRecordsInPage = bucketOf(TYPE).getMaxRecordsInPage();
      database.transaction(() -> onSlot(broken, page -> {
        final int recordOffset = recordOffsetOf(page, broken);
        assertThat(page.readByte(recordOffset)).as("the record must be a multi-page head").isEqualTo(FIRST_CHUNK_MARKER);
        // [marker][chunkSize:int][nextChunkPointer:long]
        page.writeLong(recordOffset + 1 + Binary.INT_SERIALIZED_SIZE, 1_000_000L * maxRecordsInPage);
        return 0L;
      }));
      reopenDatabase();
      return broken;
    }
  }

  /**
   * Plays the replicated apply of a busy replica: publications opened and closed back to back, each held for 200 microseconds,
   * until {@code stop} is set. The sequence is even only for the instants between two of them.
   */
  private static Thread applyBackToBack(final PageManager pageManager, final AtomicBoolean stop, final AtomicInteger applied) {
    final Thread applier = new Thread(() -> {
      while (!stop.get()) {
        pageManager.beginPublication();
        try {
          final long until = System.nanoTime() + TimeUnit.MICROSECONDS.toNanos(200);
          while (System.nanoTime() < until)
            Thread.onSpinWait();
        } finally {
          pageManager.endPublication();
        }
        applied.incrementAndGet();
      }
    });
    applier.start();
    return applier;
  }
}
