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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.exception.BrokenChunkChainException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.facet.FacetPatcher;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Task 14 of #10662: the walk of a multi-page record over the newest committed pages ({@code LocalBucket.assembleFromNewestCommitted})
 * is validated against one committed state: the validation runs once no publication is open and counts only when the publication
 * sequence did not move under it. A walk that a local commit or a replicated apply changed is walked again, and what it returns is
 * always one commit's version of the record, never chunks of two.
 */
class LocalBucketAssembleRaceTest extends TestHelper {
  private static final int  PAYLOAD = 200 * 1024;
  private static final long WAIT_S  = 30;

  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  @Override
  protected void endTest() {
    LocalBucket.committedWalkHookForTesting = null;
    PageManager.publishPagesHookForTesting = null;
    TransactionManager.applyChangesHookForTesting = null;
    executor.shutdownNow();
  }

  /**
   * A local commit rewrites the chain while a walk is between its head chunk and the rest: the walk is redone once and returns the
   * commit's version. Then a commit held half published across the whole read: the optimistic walks are spent and the read is redone
   * once under the page-manager lock, which waits for the commit and returns its version.
   */
  @Test
  void walkOverlappingAPublicationIsRedone() throws Exception {
    final RID rid = insertRecord('x');
    final LocalBucket bucket = bucketOf(rid);
    final byte[] versionX = committedBytes(bucket, rid);

    // 1. the commit lands entirely inside the walk
    final AtomicBoolean walking = new AtomicBoolean();
    final AtomicBoolean overlapSeen = new AtomicBoolean();
    PageManager.publishPagesHookForTesting = () -> {
      if (walking.get())
        overlapSeen.set((pageManager().getPublicationSequence() & 1) == 1);
    };
    final AtomicBoolean armed = new AtomicBoolean(true);
    LocalBucket.committedWalkHookForTesting = () -> {
      if (!armed.compareAndSet(true, false))
        return;
      walking.set(true);
      try {
        executor.submit(() -> updateRecord(rid, 'y')).get(WAIT_S, TimeUnit.SECONDS);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      } finally {
        walking.set(false);
      }
    };
    final long redos = bucket.getCommittedWalkRedosForTesting();
    final long lockRedos = bucket.getCommittedWalkLockRedosForTesting();
    final byte[] read = bytes(bucket.getRecordFromCommittedPage(rid));
    LocalBucket.committedWalkHookForTesting = null;
    PageManager.publishPagesHookForTesting = null;

    assertThat(armed.get()).as("the walk ran the hook").isFalse();
    assertThat(overlapSeen.get()).as("the commit was published while the walk was between its chunks").isTrue();
    final byte[] versionY = committedBytes(bucket, rid);
    assertThat(versionY).isNotEqualTo(versionX);
    assertThat(read).as("the version of the commit, never a mix of two").isEqualTo(versionY);
    assertThat(bucket.getCommittedWalkRedosForTesting() - redos).as("walks redone").isEqualTo(1);
    assertThat(bucket.getCommittedWalkLockRedosForTesting() - lockRedos).as("walks redone under the lock").isZero();

    // 2. a commit held half published (its first pages visible, the others not) across the whole read
    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch resume = new CountDownLatch(1);
    final AtomicReference<Thread> writer = new AtomicReference<>();
    PageManager.publishPagesHookForTesting = () -> {
      if (Thread.currentThread() != writer.get())
        return;
      paused.countDown();
      await(resume);
    };
    final Future<?> commit = executor.submit(() -> {
      writer.set(Thread.currentThread());
      updateRecord(rid, 'z');
    });
    assertThat(paused.await(WAIT_S, TimeUnit.SECONDS)).isTrue();

    final long redos2 = bucket.getCommittedWalkRedosForTesting();
    final long lockRedos2 = bucket.getCommittedWalkLockRedosForTesting();
    final ExecutorService readerThread = Executors.newSingleThreadExecutor();
    try {
      final Future<byte[]> reader = readerThread.submit(() -> bytes(bucket.getRecordFromCommittedPage(rid)));
      // the optimistic walks all meet the open publication; the redo under the lock then waits for the commit to end
      final int optimistic = retries() + 1;
      for (int i = 0; i < 6_000 && bucket.getCommittedWalkRedosForTesting() - redos2 < optimistic; i++)
        Thread.sleep(5);
      final long optimisticRedos = bucket.getCommittedWalkRedosForTesting() - redos2;
      resume.countDown();
      commit.get(WAIT_S, TimeUnit.SECONDS);
      final byte[] heldRead = reader.get(WAIT_S, TimeUnit.SECONDS);

      assertThat(optimisticRedos).as("every optimistic walk overlapped the held publication").isEqualTo(optimistic);
      assertThat(heldRead).as("the version of the held commit, read under the lock once it ended").isEqualTo(committedBytes(bucket, rid));
      assertThat(heldRead).isNotEqualTo(versionY);
      assertThat(bucket.getCommittedWalkLockRedosForTesting() - lockRedos2).as("walks redone under the lock").isEqualTo(1);
    } finally {
      resume.countDown();
      readerThread.shutdownNow();
    }
  }

  /**
   * A publication is a reason to compare the chain again, not to throw the walk away: a commit that lands inside the walk but touches
   * none of the chain's pages leaves every chunk at the version the walk read, so the walk is served without a redo. On a busy replica
   * the sequence moves with every entry applied, whatever it writes, and a walk refused for any overlap leaves its records unreadable
   * for the apply pass (the G3 load run of Task 14 of #10662: 0 unreadable records on the checkpoint, 102 and 138 with a walk
   * bracketed as a whole).
   */
  @Test
  void walkOverlappingAnUnrelatedPublicationIsNotRedone() throws Exception {
    final RID rid = insertRecord('x');
    final LocalBucket bucket = bucketOf(rid);
    final byte[] versionX = committedBytes(bucket, rid);
    database.command("sql", "CREATE DOCUMENT TYPE Other BUCKETS 1");

    final AtomicBoolean walking = new AtomicBoolean();
    final AtomicBoolean overlapSeen = new AtomicBoolean();
    PageManager.publishPagesHookForTesting = () -> {
      if (walking.get())
        overlapSeen.set((pageManager().getPublicationSequence() & 1) == 1);
    };
    final AtomicBoolean armed = new AtomicBoolean(true);
    LocalBucket.committedWalkHookForTesting = () -> {
      if (!armed.compareAndSet(true, false))
        return;
      walking.set(true);
      try {
        executor.submit(() -> database.transaction(() -> database.newDocument("Other").set("v", "unrelated").save()))
            .get(WAIT_S, TimeUnit.SECONDS);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      } finally {
        walking.set(false);
      }
    };
    final long redos = bucket.getCommittedWalkRedosForTesting();
    final byte[] read = bytes(bucket.getRecordFromCommittedPage(rid));

    assertThat(armed.get()).as("the walk ran the hook").isFalse();
    assertThat(overlapSeen.get()).as("the unrelated commit was published while the walk was between its chunks").isTrue();
    assertThat(read).isEqualTo(versionX);
    assertThat(bucket.getCommittedWalkRedosForTesting() - redos).as("no chunk of the chain changed: nothing to redo").isZero();
  }

  /**
   * The replicated apply publishes without the page-manager lock (#10941), so only the publication sequence tells a walk that an entry
   * overlapped it. An entry applied inside a walk: the walk is redone once and returns the entry's version. An entry held after its first
   * page: no walk can read one state, the lock does not help, and the read is a retryable conflict, never a mix of the two versions.
   */
  @Test
  void walkOverlappingAReplicatedApplyIsRedone() throws Exception {
    final RID rid = insertRecord('x');
    final LocalBucket bucket = bucketOf(rid);
    final byte[] versionX = committedBytes(bucket, rid);
    final Map<Integer, Image> imagesX = chainImages(bucket, rid, null);
    updateRecord(rid, 'y');
    final byte[] versionY = committedBytes(bucket, rid);
    final Map<Integer, Image> imagesY = chainImages(bucket, rid, imagesX);
    imagesX.keySet().forEach(page -> imagesY.computeIfAbsent(page, p -> image(bucket, p)));
    assertThat(versionY).isNotEqualTo(versionX);

    // 1. an entry that restores version x lands entirely inside the walk
    final AtomicBoolean walking = new AtomicBoolean();
    final AtomicBoolean overlapSeen = new AtomicBoolean();
    TransactionManager.applyChangesHookForTesting = () -> {
      if (walking.get())
        overlapSeen.set((pageManager().getPublicationSequence() & 1) == 1);
    };
    final AtomicBoolean armed = new AtomicBoolean(true);
    final WALFile.WALTransaction restoreX = entry(bucket, imagesX);
    LocalBucket.committedWalkHookForTesting = () -> {
      if (!armed.compareAndSet(true, false))
        return;
      walking.set(true);
      try {
        assertThat(executor.submit(() -> apply(restoreX)).get(WAIT_S, TimeUnit.SECONDS)).isTrue();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      } finally {
        walking.set(false);
      }
    };
    final long redos = bucket.getCommittedWalkRedosForTesting();
    final long lockRedos = bucket.getCommittedWalkLockRedosForTesting();
    final byte[] read = bytes(bucket.getRecordFromCommittedPage(rid));
    LocalBucket.committedWalkHookForTesting = null;
    TransactionManager.applyChangesHookForTesting = null;

    assertThat(armed.get()).as("the walk ran the hook").isFalse();
    assertThat(overlapSeen.get()).as("the entry was applied while the walk was between its chunks").isTrue();
    assertThat(committedBytes(bucket, rid)).as("the entry restored version x").isEqualTo(versionX);
    assertThat(read).as("the version of the entry, never a mix of two").isEqualTo(versionX);
    assertThat(bucket.getCommittedWalkRedosForTesting() - redos).as("walks redone").isEqualTo(1);
    assertThat(bucket.getCommittedWalkLockRedosForTesting() - lockRedos).as("walks redone under the lock").isZero();

    // 2. an entry that restores version y, held once its first page (the head chunk's) is visible
    final WALFile.WALTransaction restoreY = entry(bucket, imagesY);
    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch resume = new CountDownLatch(1);
    TransactionManager.applyChangesHookForTesting = () -> {
      paused.countDown();
      await(resume);
    };
    final Future<Boolean> applied = executor.submit(() -> apply(restoreY));
    try {
      assertThat(paused.await(WAIT_S, TimeUnit.SECONDS)).isTrue();
      final long redos2 = bucket.getCommittedWalkRedosForTesting();
      final long lockRedos2 = bucket.getCommittedWalkLockRedosForTesting();
      byte[] heldRead = null;
      ConcurrentModificationException conflict = null;
      try {
        heldRead = bytes(bucket.getRecordFromCommittedPage(rid));
      } catch (final ConcurrentModificationException e) {
        conflict = e;
      }
      if (heldRead != null)
        assertThat(Arrays.equals(heldRead, versionX) || Arrays.equals(heldRead, versionY))
            .as("a read during the held entry is one version, never a mix of the two").isTrue();
      assertThat(conflict).as("no walk can read one state while the entry is half applied: a retryable conflict").isNotNull();
      assertThat(bucket.getCommittedWalkRedosForTesting() - redos2).as("every optimistic walk overlapped the entry")
          .isEqualTo(retries() + 1);
      assertThat(bucket.getCommittedWalkLockRedosForTesting() - lockRedos2).as("the lock redo did not exclude the apply either")
          .isEqualTo(1);
    } finally {
      resume.countDown();
    }
    assertThat(applied.get(WAIT_S, TimeUnit.SECONDS)).isTrue();
    assertThat(bytes(bucket.getRecordFromCommittedPage(rid))).as("once applied, the entry's version").isEqualTo(versionY);
  }

  /**
   * On a replica ({@code isLeader()} false on the wrapped instance) the walk is never redone under the page-manager lock: the replicated
   * apply does not take it, so the redo could not exclude it. An entry held half applied across the read: every optimistic attempt
   * overlaps it, no lock redo is made, and the read is a retryable conflict, never a mix. The wrapper is a JDK proxy delegating every
   * call to the real database but {@code isLeader()}, the way the engine tests stand in for the HA wrapper.
   */
  @Test
  void aReplicaNeverRedoesTheWalkUnderTheLock() throws Exception {
    final RID rid = insertRecord('x');
    final LocalBucket bucket = bucketOf(rid);
    final Map<Integer, Image> imagesX = chainImages(bucket, rid, null);
    updateRecord(rid, 'y');
    final byte[] versionY = committedBytes(bucket, rid);
    final WALFile.WALTransaction restoreX = entry(bucket, imagesX);

    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final DatabaseInternal previous = local.getWrappedDatabaseInstance();
    local.setWrappedDatabaseInstance((DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          if ("isLeader".equals(method.getName()))
            return false;
          try {
            return method.invoke(local, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        }));
    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch resume = new CountDownLatch(1);
    TransactionManager.applyChangesHookForTesting = () -> {
      paused.countDown();
      await(resume);
    };
    Future<Boolean> applied = null;
    try {
      applied = executor.submit(() -> apply(restoreX));
      assertThat(paused.await(WAIT_S, TimeUnit.SECONDS)).isTrue();
      final long redos = bucket.getCommittedWalkRedosForTesting();
      final long lockRedos = bucket.getCommittedWalkLockRedosForTesting();

      assertThatThrownBy(() -> bucket.getRecordFromCommittedPage(rid)).isInstanceOf(ConcurrentModificationException.class);
      assertThat(bucket.getCommittedWalkRedosForTesting() - redos).as("every optimistic walk overlapped the entry")
          .isEqualTo(retries() + 1);
      assertThat(bucket.getCommittedWalkLockRedosForTesting() - lockRedos).as("no redo under the lock on a replica").isZero();
    } finally {
      resume.countDown();
      local.setWrappedDatabaseInstance(previous);
    }
    assertThat(applied.get(WAIT_S, TimeUnit.SECONDS)).isTrue();
    assertThat(committedBytes(bucket, rid)).as("once applied, the entry's version").isNotEqualTo(versionY);
  }

  /**
   * A read through {@code FacetPatcher.recordAt} during a replicated apply (the mapping of a broken chain to a retryable conflict is gone, #11359). A
   * read through the transaction ({@code loadMultiPageRecord}) proves a chain broken against the newest committed images, under the
   * page-manager lock when a publication overlapped. On a replica that lock does not exclude the replicated apply, and since #11283 a
   * proof a publication overlapped even under the lock is void: the chain the apply is still writing is no longer proved broken, the
   * read ends in the plain retryable conflict, never in a broken chain.
   */
  @Test
  void readThroughTheTransactionDuringAReplicatedApplyStillMeetsABrokenChain() throws Exception {
    final RID rid = insertRecord('x');
    final LocalBucket bucket = bucketOf(rid);
    final byte[] version = committedBytes(bucket, rid);
    final Map<Integer, Image> images = chainImages(bucket, rid, null);
    database.transaction(() -> database.deleteRecord(rid.asDocument(true)));
    assertThat(bucket.getRecordFromCommittedPage(rid)).as("deleted").isNull();

    // an entry that re-creates the record, held once its head page is visible: the head points at chunks not written yet
    final WALFile.WALTransaction recreate = entry(bucket, images);
    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch resume = new CountDownLatch(1);
    TransactionManager.applyChangesHookForTesting = () -> {
      paused.countDown();
      await(resume);
    };
    final Future<Boolean> applied = executor.submit(
        () -> ((DatabaseInternal) database).getTransactionManager().applyChanges(recreate, Map.of(bucket.getFileId(), 1), false));
    try {
      assertThat(paused.await(WAIT_S, TimeUnit.SECONDS)).isTrue();
      final AtomicReference<Throwable> thrown = new AtomicReference<>();
      database.transaction(() -> {
        try {
          FacetPatcher.recordAt((DatabaseInternal) database, rid);
        } catch (final RuntimeException e) {
          thrown.set(e);
        }
      });
      assertThat(thrown.get()).as("the read meets the chain half applied").isInstanceOf(ConcurrentModificationException.class);
      assertThat(thrown.get().getCause() instanceof BrokenChunkChainException)
          .as("#11283: a proof the apply overlapped under the lock is void, not a broken chain").isFalse();
    } finally {
      resume.countDown();
    }
    assertThat(applied.get(WAIT_S, TimeUnit.SECONDS)).isTrue();
    assertThat(committedBytes(bucket, rid)).as("once applied, the record is whole").isEqualTo(version);
  }

  // ------------------------------------------------------------------------------------------------------------------ helpers

  private RID insertRecord(final char fill) {
    database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Doc").set("v", String.valueOf(fill).repeat(PAYLOAD)).save().getIdentity());
    return rid[0];
  }

  private void updateRecord(final RID rid, final char fill) {
    database.transaction(() -> rid.asDocument(true).modify().set("v", String.valueOf(fill).repeat(PAYLOAD)).save());
  }

  /**
   * An interrupted reader waiting for an open publication to end: {@code parkNanos} returns at once while the interrupt flag is set,
   * so the wait gives up with the odd sequence (a retryable outcome) instead of spinning out its whole deadline. The flag is kept.
   */
  @Test
  void anInterruptedReaderGivesUpWaitingForAPublication() {
    final PageManager pageManager = pageManager();
    pageManager.beginPublication();
    try {
      Thread.currentThread().interrupt();
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      final long sequence = pageManager.awaitPublicationEnd(TimeUnit.SECONDS.toNanos(WAIT_S));
      stopwatch.assertGaveUpWithin(5_000L, "an interrupted reader giving up from one waiting out a 30 s publication deadline");
      assertThat(sequence & 1).as("the publication is still open").isEqualTo(1L);
      assertThat(Thread.currentThread().isInterrupted()).as("the interrupt is kept").isTrue();
    } finally {
      Thread.interrupted();
      pageManager.endPublication();
    }
  }

  private LocalBucket bucketOf(final RID rid) {
    return (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
  }

  private PageManager pageManager() {
    return ((DatabaseInternal) database).getPageManager();
  }

  private int retries() {
    return database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES);
  }

  private static byte[] committedBytes(final LocalBucket bucket, final RID rid) {
    final byte[] bytes = bytes(bucket.getRecordFromCommittedPage(rid));
    assertThat(bytes).isNotNull();
    return bytes;
  }

  private static byte[] bytes(final Binary content) {
    if (content == null)
      return null;
    final byte[] bytes = new byte[content.size()];
    System.arraycopy(content.getContent(), content.getContentBeginOffset(), bytes, 0, bytes.length);
    return bytes;
  }

  /**
   * The committed images of the pages of the record's chain, head page first, plus the pages of {@code also} (a chain at another
   * version can sit on other pages).
   */
  private Map<Integer, Image> chainImages(final LocalBucket bucket, final RID rid, final Map<Integer, Image> also) {
    final int head = (int) (rid.getPosition() / bucket.getMaxRecordsInPage());
    final List<Integer> pages = new ArrayList<>();
    pages.add(head);
    bucket.visitCommittedRecord(rid.getPosition(), (position, kind, content, offPagePages, offPageCount) -> {
      assertThat(content).isNotNull();
      for (int i = 0; i < offPageCount; i++)
        if (!pages.contains(offPagePages[i]))
          pages.add(offPagePages[i]);
    });
    assertThat(pages.size()).as("a chain over several pages").isGreaterThanOrEqualTo(2);
    if (also != null)
      for (final int page : also.keySet())
        if (!pages.contains(page))
          pages.add(page);
    final Map<Integer, Image> images = new LinkedHashMap<>();
    for (final int page : pages)
      images.put(page, image(bucket, page));
    return images;
  }

  /** A committed page: its bytes (header included, copied), content size and version. */
  private record Image(byte[] bytes, int contentSize, long version) {
  }

  private Image image(final LocalBucket bucket, final int pageNumber) {
    try {
      final ImmutablePage page = pageManager().getImmutablePage(new PageId(database, bucket.getFileId(), pageNumber), bucket.getPageSize(),
          false, false);
      return new Image(Arrays.copyOf(page.getContent().array(), bucket.getPageSize()), page.getContentSize(), page.getVersion());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** A replicated entry that writes the images over the pages, whole, at the next version of each page, in the map's order. */
  private WALFile.WALTransaction entry(final LocalBucket bucket, final Map<Integer, Image> images) {
    final WALFile.WALPage[] walPages = new WALFile.WALPage[images.size()];
    int i = 0;
    for (final Map.Entry<Integer, Image> image : images.entrySet()) {
      final WALFile.WALPage walPage = new WALFile.WALPage();
      walPage.fileId = bucket.getFileId();
      walPage.pageNumber = image.getKey();
      walPage.currentPageVersion = (int) image(bucket, image.getKey()).version() + 1;
      walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
      walPage.changesTo = bucket.getPageSize() - 1;
      walPage.currentPageSize = image.getValue().contentSize();
      walPage.currentContent = new Binary(Arrays.copyOfRange(image.getValue().bytes(), BasePage.PAGE_HEADER_SIZE, bucket.getPageSize()));
      walPages[i++] = walPage;
    }
    final WALFile.WALTransaction tx = new WALFile.WALTransaction();
    tx.txId = ((DatabaseInternal) database).getTransactionManager().getNextTransactionId();
    tx.timestamp = System.currentTimeMillis();
    tx.pages = walPages;
    return tx;
  }

  private boolean apply(final WALFile.WALTransaction tx) {
    return ((DatabaseInternal) database).getTransactionManager().applyChanges(tx, Map.of(), false);
  }

  private static void await(final CountDownLatch latch) {
    try {
      if (!latch.await(WAIT_S, TimeUnit.SECONDS))
        throw new IllegalStateException("the test never released the held publication");
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
