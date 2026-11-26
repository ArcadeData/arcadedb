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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13441: a page committed with asynchronous flush and then rewritten by a commit with synchronous flush (a
 * database whose async flush is turned off after {@code create()}) used to leave the older copy in the flush pipeline,
 * and that copy reached the file AFTER the synchronous write, reverting the page on disk. On the dictionary page that
 * re-registered every property name in another order on the next open, so every record decoded under shifted names.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class SyncCommitSupersedesPendingFlushTest {
  private static final List<String> PROPERTIES     = List.of("name", "email", "age", "city", "country");
  private static final String       DATABASE_PATH  = "target/databases/" + SyncCommitSupersedesPendingFlushTest.class.getSimpleName();
  private static final String       BLOCKER_PATH   = DATABASE_PATH + "-blocker";

  @AfterEach
  void tearDown() {
    PageManager.setPageWriteFaultInjector(null);
    for (final String path : new String[] { DATABASE_PATH, BLOCKER_PATH }) {
      final DatabaseFactory factory = new DatabaseFactory(path);
      if (factory.exists())
        factory.open().drop();
      FileUtils.deleteRecursively(new File(path));
    }
  }

  /**
   * The older copy is still QUEUED when the synchronous write happens: the flush thread is held on a batch of another
   * database, so the batch of {@code create()} waits behind it.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aQueuedCopyOfTheDictionaryPageDoesNotRevertTheSynchronousWrite() throws Exception {
    final Database blocker = new DatabaseFactory(BLOCKER_PATH).create();
    try {
      final CountDownLatch held = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final String blockerName = blocker.getName();
      PageManager.setPageWriteFaultInjector(pageId -> {
        if (isFlushThread() && blockerName.equals(pageId.getDatabase().getName())) {
          held.countDown();
          awaitQuietly(release, 30);
        }
      });

      blocker.transaction(() -> blocker.getSchema().createDocumentType("Blocker"));
      assertThat(held.await(30, TimeUnit.SECONDS)).as("the flush thread must be held on the blocker's batch").isTrue();

      try {
        final Map<String, Integer> before;
        try (final Database db = new DatabaseFactory(DATABASE_PATH).create()) {
          before = declareSchemaSynchronously(db);
          // The queued copy of create() is written now, after the synchronous rewrites of the dictionary page
          release.countDown();
        }
        assertSurvivesReopen(before);
      } finally {
        release.countDown();
      }
    } finally {
      PageManager.setPageWriteFaultInjector(null);
      blocker.drop();
    }
  }

  /**
   * The older copy is IN FLIGHT when the synchronous write starts: the flush thread is inside the write of dictionary
   * page 0 of {@code create()}, held there for a while. The synchronous write must land after it, never before.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void anInFlightCopyOfTheDictionaryPageDoesNotRevertTheSynchronousWrite() throws Exception {
    final CountDownLatch held = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    PageManager.setPageWriteFaultInjector(pageId -> {
      if (isFlushThread() && pageId.getFileId() == 0 && pageId.getPageNumber() == 0 && DATABASE_PATH.endsWith(
          pageId.getDatabase().getName())) {
        held.countDown();
        awaitQuietly(release, 30);
      }
    });

    final Thread releaser = new Thread(() -> {
      // Released by time, not by the test thread: with the fix the synchronous write waits for this very write. The
      // 500 ms only widen the window in which the synchronous write races the held one; the assertions hold either way
      awaitQuietly(held, 30);
      try {
        Thread.sleep(500);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      release.countDown();
    });
    releaser.start();
    try {
      final Map<String, Integer> before;
      try (final Database db = new DatabaseFactory(DATABASE_PATH).create()) {
        before = declareSchemaSynchronously(db);
        assertThat(held.getCount()).as("the flush thread must have been held on dictionary page 0").isZero();
        release.countDown();
      }
      assertSurvivesReopen(before);
    } finally {
      release.countDown();
      releaser.join();
    }
  }

  /**
   * The page written synchronously can be the very instance still queued for its asynchronous write: a compaction
   * writes each data page asynchronously when it fills, then writes the same pages again synchronously with the root of
   * the series. That instance is not a superseded copy: taken out of the pipeline as one, and marked, it was skipped by
   * the synchronous write too, and the page never reached the file.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void theSameInstanceQueuedAsynchronouslyAndThenWrittenSynchronouslyReachesTheFile() throws Exception {
    final Database blocker = new DatabaseFactory(BLOCKER_PATH).create();
    try {
      final CountDownLatch held = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final String blockerName = blocker.getName();
      PageManager.setPageWriteFaultInjector(pageId -> {
        if (isFlushThread() && blockerName.equals(pageId.getDatabase().getName())) {
          held.countDown();
          awaitQuietly(release, 30);
        }
      });

      blocker.transaction(() -> blocker.getSchema().createDocumentType("Blocker"));
      assertThat(held.await(30, TimeUnit.SECONDS)).as("the flush thread must be held on the blocker's batch").isTrue();

      final PageId pageId;
      final int pageSize;
      final int markerOffset;
      try (final DatabaseInternal db = (DatabaseInternal) new DatabaseFactory(DATABASE_PATH).create()) {
        final PaginatedComponent bucket = (PaginatedComponent) db.getSchema().createDocumentType("Item").getBuckets(false)
            .getFirst();
        pageSize = bucket.getPageSize();
        pageId = new PageId(db, bucket.getFileId(), bucket.getTotalPages());

        final MutablePage page = new MutablePage(pageId, pageSize);
        markerOffset = page.getMaxContentSize() - Integer.BYTES;
        page.writeInt(markerOffset, 13441);

        final PageManager pageManager = db.getPageManager();
        pageManager.updatePageVersion(page, true);
        // Queued behind the held batch of the blocker, then the same instance is written synchronously
        pageManager.writePages(List.of(page), true);
        pageManager.writePages(List.of(page), false);

        // The instance was the only copy the detach reached, but an older one could still sit in a batch the flush
        // thread polled and had not published: the write is recorded all the same
        assertThat(pageManager.getFlushThread().isOlderThanASyncWrite(new MutablePage(pageId, pageSize)))
            .as("a copy older than the synchronous write must be skipped").isTrue();

        release.countDown();
      } finally {
        release.countDown();
      }

      PageManager.setPageWriteFaultInjector(null);
      try (final Database db = new DatabaseFactory(DATABASE_PATH).open()) {
        final PageId reopened = new PageId(db, pageId.getFileId(), pageId.getPageNumber());
        final ImmutablePage page = ((DatabaseInternal) db).getPageManager().getImmutablePage(reopened, pageSize, false, false);
        assertThat(page).as("the page must have reached the file").isNotNull();
        assertThat(page.readInt(markerOffset)).as("the page on disk must hold the content written").isEqualTo(13441);
      }
    } finally {
      PageManager.setPageWriteFaultInjector(null);
      blocker.drop();
    }
  }

  /**
   * A pending batch keeps the very ArrayList its caller passed. Passed again to a synchronous write, that list was
   * changed under the write's loop by the detach, which removes the pages from the pending batches: the loop stopped
   * after the first page, and the second one was never written synchronously.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void theSameListQueuedAsynchronouslyAndThenWrittenSynchronouslyWritesEveryPage() throws Exception {
    final Database blocker = new DatabaseFactory(BLOCKER_PATH).create();
    try {
      final CountDownLatch held = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final String blockerName = blocker.getName();
      PageManager.setPageWriteFaultInjector(pageId -> {
        if (isFlushThread() && blockerName.equals(pageId.getDatabase().getName())) {
          held.countDown();
          awaitQuietly(release, 30);
        }
      });

      blocker.transaction(() -> blocker.getSchema().createDocumentType("Blocker"));
      assertThat(held.await(30, TimeUnit.SECONDS)).as("the flush thread must be held on the blocker's batch").isTrue();

      try (final DatabaseInternal db = (DatabaseInternal) new DatabaseFactory(DATABASE_PATH).create()) {
        final PaginatedComponent bucket = (PaginatedComponent) db.getSchema().createDocumentType("Item").getBuckets(false)
            .getFirst();
        final int pageSize = bucket.getPageSize();
        final PageManager pageManager = db.getPageManager();

        final List<MutablePage> pages = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
          final MutablePage page = new MutablePage(new PageId(db, bucket.getFileId(), bucket.getTotalPages() + i), pageSize);
          page.writeInt(page.getMaxContentSize() - Integer.BYTES, 13441 + i);
          pageManager.updatePageVersion(page, true);
          pages.add(page);
        }

        // The pipeline changes the shared list: keep the pages apart to check them
        final List<MutablePage> created = List.copyOf(pages);

        // Queued behind the held batch of the blocker, then the same list is written synchronously
        pageManager.writePages(pages, true);
        pageManager.writePages(pages, false);

        assertThat(created).hasSize(2);
        for (final MutablePage page : created)
          assertThat(pageManager.getFlushThread().pageIndex.get(page.getPageId()))
              .as("page %s must have been written synchronously, not left pending", page.getPageId()).isNull();
        release.countDown();
      } finally {
        release.countDown();
      }
    } finally {
      PageManager.setPageWriteFaultInjector(null);
      blocker.drop();
    }
  }

  /**
   * A copy the flush thread polled but had not yet published as in flight is out of reach of the detach, so the
   * synchronous write records its version under the page's I/O lock, and the flush thread skips any OLDER copy it
   * writes after that. Driven directly, since the poll-to-publication window is a few instructions wide.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aCopyOlderThanARecordedSynchronousWriteIsNeverWrittenAfterIt() throws Exception {
    final PageId pageId;
    final int pageSize;
    final int markerOffset;
    // The flush thread is held inside one flush cycle for the whole test: an entry lives until the cycle it was
    // recorded in ends, and an idle thread would end one every second
    final Database blocker = new DatabaseFactory(BLOCKER_PATH).create();
    try {
      final CountDownLatch held = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final String blockerName = blocker.getName();
      PageManager.setPageWriteFaultInjector(id -> {
        if (isFlushThread() && blockerName.equals(id.getDatabase().getName())) {
          held.countDown();
          awaitQuietly(release, 30);
        }
      });
      blocker.transaction(() -> blocker.getSchema().createDocumentType("Blocker"));
      assertThat(held.await(30, TimeUnit.SECONDS)).as("the flush thread must be held on the blocker's batch").isTrue();

      try (final DatabaseInternal db = (DatabaseInternal) new DatabaseFactory(DATABASE_PATH).create()) {
        db.setAsyncFlush(false);
        final PaginatedComponent bucket = (PaginatedComponent) db.getSchema().createDocumentType("Item").getBuckets(false)
            .getFirst();
        pageSize = bucket.getPageSize();
        pageId = new PageId(db, bucket.getFileId(), bucket.getTotalPages());
        final PageManager pageManager = db.getPageManager();

        final MutablePage older = new MutablePage(pageId, pageSize);
        markerOffset = older.getMaxContentSize() - Integer.BYTES;
        older.writeInt(markerOffset, 1);
        pageManager.updatePageVersion(older, true);
        pageManager.writePages(List.of(older), false);

        final MutablePage newer = pageManager.getImmutablePage(pageId, pageSize, false, false).modify();
        newer.writeInt(markerOffset, 2);
        pageManager.updatePageVersion(newer, false);
        assertThat(newer.getVersion()).isGreaterThan(older.getVersion());

        // The synchronous write of the newer image, as recorded when it found the page pending
        assertThat(pageManager.flushPage(newer)).isTrue();
        pageManager.getFlushThread().recordSyncWrite(newer);

        // The older copy reaches the flush thread only now: it must not land over the newer image
        assertThat(pageManager.flushPage(older)).as("the older copy must be skipped").isFalse();
        assertThat(older.isSupersededOnDisk()).isTrue();
        release.countDown();
      } finally {
        release.countDown();
      }
    } finally {
      PageManager.setPageWriteFaultInjector(null);
      blocker.drop();
    }

    try (final Database db = new DatabaseFactory(DATABASE_PATH).open()) {
      final PageId reopened = new PageId(db, pageId.getFileId(), pageId.getPageNumber());
      final ImmutablePage page = ((DatabaseInternal) db).getPageManager().getImmutablePage(reopened, pageSize, false, false);
      assertThat(page.readInt(markerOffset)).as("the file must hold the newer image").isEqualTo(2);
    }
  }

  /** The application's sequence of the issue: async flush turned off right after {@code create()}, then the schema. */
  private static Map<String, Integer> declareSchemaSynchronously(final Database db) {
    db.setAsyncFlush(false);
    final VertexType type = db.getSchema().createVertexType("Item");
    for (final String p : PROPERTIES)
      type.createProperty(p, Type.STRING);
    db.transaction(() -> {
      for (int v = 0; v < 3; v++) {
        final MutableVertex vertex = db.newVertex("Item");
        for (final String p : PROPERTIES)
          vertex.set(p, p + "-" + v);
        vertex.save();
      }
    });
    return new TreeMap<>(dictionary(db));
  }

  private void assertSurvivesReopen(final Map<String, Integer> before) {
    PageManager.setPageWriteFaultInjector(null);
    try (final Database db = new DatabaseFactory(DATABASE_PATH).open()) {
      assertThat(new TreeMap<>(dictionary(db))).as("the dictionary must be the one written before close").isEqualTo(before);
      final Vertex first = db.iterateType("Item", false).next().asVertex();
      for (final String p : PROPERTIES)
        assertThat(first.getString(p)).as("property '%s' must decode under its own name", p).startsWith(p + "-");
    }
  }

  private static Map<String, Integer> dictionary(final Database db) {
    return ((DatabaseInternal) db).getSchema().getEmbedded().getDictionary().getDictionaryMap();
  }

  private static boolean isFlushThread() {
    return Thread.currentThread() instanceof PageManagerFlushThread;
  }

  private static void awaitQuietly(final CountDownLatch latch, final int seconds) {
    try {
      latch.await(seconds, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
