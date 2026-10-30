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
package com.arcadedb.index.columnar;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.IntHashSet;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Task 13: the dirty page marks of a local commit, a replicated apply and a WAL replay (spec 6.2), and the no-WAL marker (6.5). */
class ColumnarDirtyPageHooksTest extends TestHelper {
  private static final String BLOB = "x".repeat(25_000);

  @Override
  protected void endTest() {
    ColumnarIndexManager.onMarkForTesting = null;
    TransactionContext.afterPublishBeforeSecondMarkForTesting = null;
  }

  @Test
  void localCommitMarksEveryModifiedBucketPage() throws Exception {
    createAsset();
    final int bucketId = bucketId("Asset");
    final ColumnarProjection projection = manager().projection(bucketId);

    final List<RID> rids = new ArrayList<>();
    try (final AutoCloseable hold = manager().suspendApply()) {
      // three records of 25KB on 64KB pages: two on the first page, the third on the next one
      database.transaction(() -> {
        for (int i = 0; i < 3; i++)
          rids.add(database.newDocument("Asset").set("brand", "b" + i).set("blob", BLOB).save().getIdentity());
      });

      final int[] expected = pagesOf(rids, bucketId);
      assertThat(expected).hasSize(2);
      assertThat(pending(projection.tracker())).containsExactly(expected);
    }
  }

  @Test
  void nonColumnarBucketsAreNotTracked() throws Exception {
    createAsset();
    database.command("sql", "CREATE DOCUMENT TYPE Other BUCKETS 1");
    final int otherId = bucketId("Other");
    final ColumnarProjection projection = manager().projection(bucketId("Asset"));
    final IntHashSet marked = new IntHashSet();
    ColumnarIndexManager.onMarkForTesting = fileId -> {
      synchronized (marked) {
        marked.add(fileId);
      }
    };

    try (final AutoCloseable hold = manager().suspendApply()) {
      database.transaction(() -> database.newDocument("Other").set("brand", "x").save());

      // the commit did mark the page of Other: nothing tracks it
      synchronized (marked) {
        assertThat(marked.contains(otherId)).isTrue();
      }
      assertThat(manager().projection(otherId)).isNull();
      assertThat(manager().isProjected(otherId)).isFalse();
      assertThat(projection.tracker().isEmpty()).isTrue();
    }
  }

  @Test
  void externalBucketPagesGoToTheExternalTracker() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("Blob", 1);
    type.createProperty("brand", Type.STRING);
    type.createProperty("payload", Type.STRING).setExternal(true);
    database.command("sql", "CREATE INDEX ON Blob (payload) COLUMNAR");
    final int bucketId = bucketId("Blob");
    final int externalId = ((LocalDocumentType) type).getExternalBucketIdFor(bucketId);
    // three primary records of 25KB: the third one lives on primary page 1, every external value on external page 0
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        rids.add(database.newDocument("Blob").set("brand", "b" + i).set("blob", BLOB).set("payload", "x".repeat(300)).save()
            .getIdentity());
    });
    final RID rid = rids.getLast();
    assertThat(pagesOf(List.of(rid), bucketId)).containsExactly(1);
    final ColumnarProjection projection = manager().projection(bucketId);
    assertThat(projection.externalFileId()).isEqualTo(externalId);
    settle(projection);

    try (final AutoCloseable hold = manager().suspendApply()) {
      // the value only, same length: the external record is rewritten in place on external page 0
      database.transaction(() -> {
        final MutableDocument document = rid.asDocument().modify();
        document.set("payload", "y".repeat(300)).save();
      });

      // a mark of the external file never lands in the primary tracker as the same page number
      assertThat(pending(projection.externalTracker())).containsExactly(0);
      assertThat(pending(projection.tracker())).doesNotContain(0);
    }
  }

  @Test
  void watchedFileWithoutProjectionNotifiesOnCommit() throws Exception {
    database.command("sql", "CREATE VERTEX TYPE Tag BUCKETS 1");
    final int tagId = bucketId("Tag");
    final AtomicInteger runs = new AtomicInteger();
    final CountDownLatch ran = new CountDownLatch(1);
    final Runnable onChange = () -> {
      runs.incrementAndGet();
      ran.countDown();
    };
    manager().watchFiles(new int[] { tagId }, onChange);
    final long before = manager().watchGeneration(onChange);

    final List<RID> rids = new ArrayList<>();
    try (final AutoCloseable hold = manager().suspendApply()) {
      // one record per 64KB page: 100 pages in one commit
      database.transaction(() -> {
        for (int i = 0; i < 100; i++)
          rids.add(database.newVertex("Tag").set("blob", "x".repeat(40_000)).save().getIdentity());
      });
      assertThat(pagesOf(rids, tagId)).hasSize(100);

      // synchronous with the commit, before the watcher runs
      assertThat(manager().watchGeneration(onChange)).isGreaterThan(before);
      assertThat(runs).hasValue(0);
    }

    assertThat(ran.await(10, TimeUnit.SECONDS)).isTrue();
    // a short wait expected to time out: the marks of the 100 pages are coalesced into one run
    assertThat(awaitRuns(runs, 2, 300)).isFalse();
    assertThat(runs).hasValue(1);

    manager().unwatchFiles(onChange);
    database.transaction(() -> database.newVertex("Tag").set("blob", "y").save());
    assertThat(awaitRuns(runs, 2, 300)).isFalse();
    assertThat(runs).hasValue(1);
  }

  @Test
  void watchedFileNotifiesOnReplayAndRaftApply() throws Exception {
    // a follower's Raft apply and the WAL replay both reach the pages through TransactionManager.applyChanges
    database.command("sql", "CREATE DOCUMENT TYPE Tag BUCKETS 1");
    final int tagId = bucketId("Tag");
    database.transaction(() -> database.newDocument("Tag").set("name", "t").save());
    final CountDownLatch ran = new CountDownLatch(1);
    final Runnable onChange = ran::countDown;
    manager().watchFiles(new int[] { tagId }, onChange);
    final long before = manager().watchGeneration(onChange);

    final int version = (int) page(tagId, 0).getVersion();
    assertThat(applyChanges(walTransaction(tagId, new int[] { 0 }, new int[] { version + 1 }))).isTrue();

    assertThat(manager().watchGeneration(onChange)).isGreaterThan(before);
    assertThat(ran.await(10, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  void markHappensAfterPublication() throws Exception {
    createAsset();
    final int bucketId = bucketId("Asset");
    database.transaction(() -> database.newDocument("Asset").set("brand", "a").save());
    final List<Long> versionsSeenByMarks = Collections.synchronizedList(new ArrayList<>());
    ColumnarIndexManager.onMarkForTesting = fileId -> {
      if (fileId == bucketId)
        versionsSeenByMarks.add(page(bucketId, 0).getVersion());
    };

    try (final AutoCloseable hold = manager().suspendApply()) {
      database.transaction(() -> database.newDocument("Asset").set("brand", "b").save());
    }
    ColumnarIndexManager.onMarkForTesting = null;

    final long committed = page(bucketId, 0).getVersion();
    // the first mark precedes the publication, the second one sees the committed page
    assertThat(versionsSeenByMarks).containsExactly(committed - 1, committed);
  }

  @Test
  void testSeamRunsBetweenPublishAndSecondMark() throws Exception {
    createAsset();
    final int bucketId = bucketId("Asset");
    final ColumnarProjection projection = manager().projection(bucketId);
    final AtomicInteger marks = new AtomicInteger();
    ColumnarIndexManager.onMarkForTesting = fileId -> {
      if (fileId == bucketId)
        marks.incrementAndGet();
    };
    final int[][] pendingAtSeam = new int[1][];
    final int[] marksAtSeam = new int[1];
    final long[] versionAtSeam = new long[1];

    try (final AutoCloseable hold = manager().suspendApply()) {
      TransactionContext.afterPublishBeforeSecondMarkForTesting = () -> {
        pendingAtSeam[0] = pending(projection.tracker());
        marksAtSeam[0] = marks.get();
        versionAtSeam[0] = page(bucketId, 0).getVersion();
      };
      database.transaction(() -> database.newDocument("Asset").set("brand", "a").save());
      TransactionContext.afterPublishBeforeSecondMarkForTesting = null;
    }

    assertThat(pendingAtSeam[0]).containsExactly(0);
    assertThat(marksAtSeam[0]).isEqualTo(1);
    assertThat(marks).hasValue(2);
    // the pages are visible when the seam runs
    assertThat(versionAtSeam[0]).isEqualTo(page(bucketId, 0).getVersion());
  }

  @Test
  void replayedTransactionMarksAppliedAndSkippedPages() throws Exception {
    createAsset();
    final int bucketId = bucketId("Asset");
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        rids.add(database.newDocument("Asset").set("brand", "b" + i).set("blob", BLOB).save().getIdentity());
    });
    assertThat(pagesOf(rids, bucketId)).containsExactly(0, 1);
    // a second version of page 0, so an entry one version older than the disk exists
    database.transaction(() -> rids.getFirst().asDocument().modify().set("brand", "c").save());
    final int version0 = (int) page(bucketId, 0).getVersion();
    final int version1 = (int) page(bucketId, 1).getVersion();
    assertThat(version0).isPositive();
    final ColumnarProjection projection = manager().projection(bucketId);
    settle(projection);

    try (final AutoCloseable hold = manager().suspendApply()) {
      // page 0 is older than the disk (skipped), page 1 newer (applied)
      assertThat(applyChanges(walTransaction(bucketId, new int[] { 0, 1 }, new int[] { version0 - 1, version1 + 1 }))).isTrue();

      assertThat(page(bucketId, 0).getVersion()).isEqualTo(version0);
      assertThat(page(bucketId, 1).getVersion()).isEqualTo(version1 + 1);
      assertThat(pending(projection.tracker())).containsExactly(0, 1);
    }
  }

  @Test
  void noWalCommitWritesMarkerOncePerFlushEpoch() throws Exception {
    createAsset();
    final int bucketId = bucketId("Asset");
    final ColumnarProjection projection = manager().projection(bucketId);
    final Path marker = ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucketId).resolve(ColumnarStore.NOWAL_MARKER);

    database.transaction(() -> database.newDocument("Asset").set("brand", "wal").save());
    assertThat(marker).doesNotExist();

    final TransactionContext transaction = ((DatabaseInternal) database).getTransaction();
    try {
      transaction.setUseWAL(false);
      database.transaction(() -> database.newDocument("Asset").set("brand", "nowal-0").save());
      assertThat(marker).exists();
      final BasicFileAttributes first = Files.readAttributes(marker, BasicFileAttributes.class);
      // the first of the two marks wrote the marker
      assertThat(projection.noWalMarkerEpoch).isEqualTo(1);

      for (int i = 1; i < 50; i++) {
        final int n = i;
        database.transaction(() -> database.newDocument("Asset").set("brand", "nowal-" + n).save());
      }

      // written once: the same file, and the epoch it was written at did not move while the no-WAL epoch did
      final BasicFileAttributes last = Files.readAttributes(marker, BasicFileAttributes.class);
      assertThat(last.fileKey()).isEqualTo(first.fileKey());
      assertThat(last.lastModifiedTime()).isEqualTo(first.lastModifiedTime());
      assertThat(projection.noWalMarkerEpoch).isEqualTo(1);
      assertThat(projection.noWalEpoch()).isEqualTo(100);
    } finally {
      transaction.setUseWAL(true);
    }

    // a flush keeps the marker: the bucket pages of the no-WAL commits reach the disk asynchronously
    manager().flushNow();
    assertThat(marker).exists();

    // a clean close, once every page is flushed, deletes it
    reopenDatabase();
    assertThat(marker).doesNotExist();
  }

  @Test
  void markCostIsOneArrayReadForOtherBuckets() {
    createAsset();
    database.command("sql", "CREATE DOCUMENT TYPE Other BUCKETS 1");
    final int otherId = bucketId("Other");
    final ColumnarIndexManager manager = manager();
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final int[] pages = { 0, 1, 2 };

    for (int i = 0; i < 10_000; i++) {
      manager.markPages(otherId, pages, pages.length);
      manager.markPages(1_000_000, pages, pages.length);
    }
    final long before = threads.getCurrentThreadAllocatedBytes();
    for (int i = 0; i < 10_000; i++) {
      manager.markPages(otherId, pages, pages.length);
      manager.markPages(1_000_000, pages, pages.length);
    }
    final long allocated = threads.getCurrentThreadAllocatedBytes() - before;

    assertThat(allocated).isZero();
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private void createAsset() {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
  }

  private int bucketId(final String type) {
    return database.getSchema().getType(type).getBuckets(false).stream().mapToInt(Bucket::getFileId).findFirst().orElseThrow();
  }

  /** The distinct pages of the records, ascending. */
  private int[] pagesOf(final List<RID> rids, final int bucketId) {
    final int maxRecordsInPage = ((LocalBucket) database.getSchema().getBucketById(bucketId)).getMaxRecordsInPage();
    final TreeSet<Integer> pages = new TreeSet<>();
    for (final RID rid : rids)
      pages.add((int) (rid.getPosition() / maxRecordsInPage));
    return pages.stream().mapToInt(Integer::intValue).toArray();
  }

  /** Applies and flushes everything the earlier commits marked, so the trackers hold only what the test marks next. */
  private void settle(final ColumnarProjection projection) throws InterruptedException {
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();
    manager().flushNow();
    assertThat(projection.tracker().isEmpty()).isTrue();
    if (projection.externalTracker() != null)
      assertThat(projection.externalTracker().isEmpty()).isTrue();
  }

  /** The pages in an epoch, in flight or unflushed, ascending; reads only. */
  private static int[] pending(final DirtyPageTracker tracker) {
    final IntHashSet pages = new IntHashSet();
    tracker.collectRecoverySet(pages);
    final int[] sorted = pages.toArray();
    Arrays.sort(sorted);
    return sorted;
  }

  private ImmutablePage page(final int fileId, final int pageNumber) {
    try {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
      return db.getPageManager().getImmutablePage(new PageId(db, fileId, pageNumber), pageSize, false, true);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean applyChanges(final WALFile.WALTransaction tx) {
    return ((DatabaseInternal) database).getTransactionManager().applyChanges(tx, Map.of(), false);
  }

  /** An entry rewriting the first bytes of the content area of each page with their current value, at the given versions. */
  private WALFile.WALTransaction walTransaction(final int fileId, final int[] pageNumbers, final int[] versions) {
    final WALFile.WALPage[] walPages = new WALFile.WALPage[pageNumbers.length];
    for (int i = 0; i < pageNumbers.length; i++) {
      final ImmutablePage current = page(fileId, pageNumbers[i]);
      final WALFile.WALPage walPage = new WALFile.WALPage();
      walPage.fileId = fileId;
      walPage.pageNumber = pageNumbers[i];
      walPage.currentPageVersion = versions[i];
      walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
      walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
      walPage.currentPageSize = current.getContentSize();
      final byte[] content = new byte[11];
      System.arraycopy(current.getContent().array(), walPage.changesFrom, content, 0, content.length);
      walPage.currentContent = new Binary(content);
      walPages[i] = walPage;
    }
    final WALFile.WALTransaction tx = new WALFile.WALTransaction();
    tx.txId = ((DatabaseInternal) database).getTransactionManager().getNextTransactionId();
    tx.timestamp = System.currentTimeMillis();
    tx.pages = walPages;
    return tx;
  }

  /** Polls until {@code runs} reaches {@code expected}: false when it did not within {@code timeoutMs}. */
  private static boolean awaitRuns(final AtomicInteger runs, final int expected, final long timeoutMs) throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (runs.get() < expected) {
      if (System.nanoTime() - deadline >= 0)
        return false;
      Thread.sleep(5);
    }
    return true;
  }
}
