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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.schema.Schema;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10654 (item 1): every page an LSM index lookup read through {@link TransactionContext#getPage} was wrapped in a new
 * {@link ImmutablePage}, its {@link Binary} and a {@code ByteBuffer}, around the array the {@link CachedPage} already holds.
 * A lookup only reads its pages at absolute positions, so it now reads them through
 * {@link TransactionContext#getPageForAbsoluteReads}, which hands back one view per cached page, shared by every thread
 * ({@link CachedPage#getSharedImmutableView()}). The shared view must read like a per-call one, must not let one reader's
 * position reach another, and must stay out of the transaction's own pages.
 */
class Issue10654SharedPageViewTest extends TestHelper {
  private static final String TYPE_NAME = "Doc";
  private static final int    PAGE_SIZE = 256 * 1024;
  private static final String KEY_PAD   = "x".repeat(64);
  private static final int    COMPACTED = 40_000;
  private static final int    MUTABLE   = 12_000;

  private static String key(final int i) {
    return "K" + KEY_PAD + String.format("%08d", i);
  }

  @Override
  protected void beginTest() {
    // The explicit compaction below must be the only one, not raced by a background round.
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(1L);
  }

  @Override
  protected void endTest() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.reset();
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.reset();
  }

  /** A unique index whose older keys sit in compacted series and whose newer keys sit in mutable pages. */
  private TypeIndex buildCompactedAndMutableIndex() throws Exception {
    database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create().createProperty("email", String.class);
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "email" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(PAGE_SIZE).create();

    database.transaction(() -> {
      for (int i = 0; i < COMPACTED; i++)
        database.newDocument(TYPE_NAME).set("email", key(i)).save();
    });

    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("email").getFirst();
    assertThat(((IndexInternal) typeIndex).scheduleCompaction()).isTrue();
    assertThat(((IndexInternal) typeIndex).compact()).isTrue();

    database.transaction(() -> {
      for (int i = COMPACTED; i < COMPACTED + MUTABLE; i++)
        database.newDocument(TYPE_NAME).set("email", key(i)).save();
    });

    assertThat(mutableOf(typeIndex).getSubIndex()).as("compacted sub-index").isNotNull();
    assertThat(mutableOf(typeIndex).getTotalPages()).as("mutable pages the lookup walks").isGreaterThanOrEqualTo(2);
    return typeIndex;
  }

  private static LSMTreeIndexMutable mutableOf(final TypeIndex typeIndex) {
    return ((LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0]).getMutableIndex();
  }

  private PageManager pageManager() {
    return ((DatabaseInternal) database).getPageManager();
  }

  private PageId pageId(final int fileId, final int pageNumber) {
    return new PageId((DatabaseInternal) database, fileId, pageNumber);
  }

  /** The shared view is built once per cached page and reads the same bytes, version and size as a per-call view. */
  @Test
  void sharedViewIsBuiltOnceAndReadsLikeAPerCallView() throws Exception {
    final TypeIndex typeIndex = buildCompactedAndMutableIndex();
    final LSMTreeIndexMutable mutable = mutableOf(typeIndex);

    for (int p = 0; p < mutable.getTotalPages(); p++) {
      final CachedPage cached = pageManager().getCachedPageForRead(pageId(mutable.getFileId(), p), PAGE_SIZE);
      final ImmutablePage shared = cached.getSharedImmutableView();
      final ImmutablePage perCall = cached.useAsImmutable();

      assertThat(cached.getSharedImmutableView()).as("page %d: built once", p).isSameAs(shared);
      assertThat(shared.getPageId()).isEqualTo(perCall.getPageId());
      assertThat(shared.getVersion()).isEqualTo(perCall.getVersion());
      assertThat(shared.getContentSize()).isEqualTo(perCall.getContentSize());
      assertThat(shared.getPhysicalSize()).isEqualTo(perCall.getPhysicalSize());
      for (int offset = 0; offset < 64; offset += 4)
        assertThat(shared.readInt(offset)).as("page %d, int at %d", p, offset).isEqualTo(perCall.readInt(offset));
      assertThat(shared.slice()).isEqualTo(perCall.slice());
      assertThat(shared.getImmutableView(0, 32).toByteArray()).isEqualTo(perCall.getImmutableView(0, 32).toByteArray());
    }
  }

  /** Position-moving reads on the shared view answer what a per-call view answers, and never move a position another reader sees. */
  @Test
  void positionMovingReadsOnTheSharedViewTouchNoSharedPosition() throws Exception {
    final int pageSize = 4096;
    final Binary writer = new Binary(new byte[pageSize], pageSize);
    writer.position(BasePage.PAGE_HEADER_SIZE);
    final int[] numberAt = new int[200];
    final long[] numbers = new long[200];
    for (int i = 0; i < numbers.length; i++) {
      numberAt[i] = writer.position() - BasePage.PAGE_HEADER_SIZE;
      numbers[i] = (i % 2 == 0 ? 1L : -1L) * i * 1_000_003L;
      writer.putNumber(numbers[i]);
    }
    final int bytesAt = writer.position() - BasePage.PAGE_HEADER_SIZE;
    writer.putBytes(new byte[] { 1, 2, 3, 4, 5 });
    final int contentSize = writer.position();

    final CachedPageFixture fixture = new CachedPageFixture((DatabaseInternal) database, pageSize, writer.getContent(), contentSize);
    final ImmutablePage shared = fixture.shared;
    final ImmutablePage perCall = fixture.perCall;

    for (int i = 0; i < numbers.length; i++) {
      assertThat(shared.readNumberAndSize(numberAt[i])).isEqualTo(perCall.readNumberAndSize(numberAt[i]));
      assertThat(shared.readNumberAndSize(numberAt[i])[0]).isEqualTo(numbers[i]);
    }
    assertThat(shared.readBytes(bytesAt)).containsExactly(1, 2, 3, 4, 5);
    final byte[] into = new byte[3];
    shared.readByteArray(bytesAt + 1, into);
    assertThat(into).containsExactly(1, 2, 3);
    final byte[] intoWithOffset = new byte[4];
    shared.readByteArray(bytesAt + 2, intoWithOffset, 1, 2);
    assertThat(intoWithOffset).containsExactly(0, 2, 3, 0);
    assertThat(shared.readBytes()).isEqualTo(fixture.cached.useAsImmutable().readBytes());

    // a buffer handed out is the caller's own: moving it moves nothing the next caller sees
    final ByteBuffer first = shared.getContent();
    first.position(100);
    // (0 is where a fresh per-call view starts; the per-call view of this test has been moved by the reads above)
    assertThat(shared.getContent().position()).isEqualTo(fixture.cached.useAsImmutable().getContent().position()).isZero();

    // a position that every caller would share cannot be offered, and the view's metadata is not reloaded under its readers
    assertThatThrownBy(() -> shared.setBufferPosition(10)).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(shared::getBufferPosition).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(shared::loadMetadata).isInstanceOf(UnsupportedOperationException.class);

    // and many threads reading variable-length numbers from the one view at once all read their own
    final ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      final List<Future<Integer>> results = new ArrayList<>();
      for (int t = 0; t < 8; t++) {
        final int start = t;
        results.add(pool.submit(() -> {
          int wrong = 0;
          for (int round = 0; round < 2_000; round++)
            for (int i = start; i < numbers.length; i += 8)
              if (shared.readNumberAndSize(numberAt[i])[0] != numbers[i])
                ++wrong;
          return wrong;
        }));
      }
      for (final Future<Integer> result : results)
        assertThat(result.get(60, TimeUnit.SECONDS)).isZero();
    } finally {
      pool.shutdownNow();
    }

    // a shared view is still modifiable the way any immutable page is: into a private copy
    final MutablePage modified = shared.modify();
    modified.writeInt(0, 42);
    assertThat(shared.readInt(0)).isEqualTo(perCall.readInt(0));
  }

  /** A READ_COMMITTED transaction hands back the shared view on a miss, and a page it has itself modified otherwise. */
  @Test
  void readCommittedHandsBackTheSharedViewOrTheTransactionsOwnPage() throws Exception {
    final TypeIndex typeIndex = buildCompactedAndMutableIndex();
    final LSMTreeIndexMutable mutable = mutableOf(typeIndex);
    final PageId first = pageId(mutable.getFileId(), 0);
    final PageId last = pageId(mutable.getFileId(), mutable.getTotalPages() - 1);

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    try {
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      final BasePage page = tx.getPageForAbsoluteReads(first, PAGE_SIZE);
      assertThat(page).isSameAs(pageManager().getCachedPageForRead(first, PAGE_SIZE).getSharedImmutableView());
      assertThat(tx.getPageForAbsoluteReads(first, PAGE_SIZE)).isSameAs(page);
      assertThat(tx.getPage(first, PAGE_SIZE)).as("getPage still builds its own view").isNotSameAs(page);

      final MutablePage own = tx.getPageToModify(last, PAGE_SIZE, false);
      assertThat(tx.getPageForAbsoluteReads(last, PAGE_SIZE)).as("the transaction's own page").isSameAs(own);
    } finally {
      database.rollback();
    }
  }

  /** A REPEATABLE_READ transaction keeps the page it read first, as {@code getPage} does: never the shared view. */
  @Test
  void repeatableReadKeepsTheTransactionsOwnSnapshot() throws Exception {
    final TypeIndex typeIndex = buildCompactedAndMutableIndex();
    final PageId first = pageId(mutableOf(typeIndex).getFileId(), 0);

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      final BasePage page = tx.getPageForAbsoluteReads(first, PAGE_SIZE);
      assertThat(page).isNotSameAs(pageManager().getCachedPageForRead(first, PAGE_SIZE).getSharedImmutableView());
      assertThat(tx.getPage(first, PAGE_SIZE)).as("the page the transaction keeps").isSameAs(page);
    } finally {
      database.rollback();
    }
  }

  /** A lookup reads both its mutable pages and the compacted series through the shared view: none of them had one before it. */
  @Test
  void aLookupReadsItsPagesThroughTheSharedView() throws Exception {
    buildCompactedAndMutableIndex();
    // the unique checks of the build already looked keys up: start from pages nothing has read yet
    database.async().waitCompletion();
    reopenDatabase();
    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("email").getFirst();
    final LSMTreeIndexMutable mutable = mutableOf(typeIndex);
    final int compactedFileId = mutable.getSubIndex().getFileId();

    final List<CachedPage> pages = new ArrayList<>();
    for (int p = 0; p < mutable.getTotalPages(); p++)
      pages.add(pageManager().getCachedPageForRead(pageId(mutable.getFileId(), p), PAGE_SIZE));
    pages.add(pageManager().getCachedPageForRead(pageId(compactedFileId, 0), PAGE_SIZE));
    for (final CachedPage page : pages)
      assertThat(page.peekSharedImmutableView()).as("%s before the lookup", page).isNull();

    // an absent key walks every mutable page and every compacted series
    try (final IndexCursor cursor = typeIndex.get(new Object[] { "absent" })) {
      assertThat(cursor.hasNext()).isFalse();
    }

    for (final CachedPage page : pages) {
      final ImmutablePage view = pageManager().getCachedPageForRead(page.getPageId(), PAGE_SIZE).peekSharedImmutableView();
      assertThat(view).as("%s after the lookup", page).isNotNull();
      // getContent() duplicates the shared buffer, position included: the lookup left it where every view starts
      assertThat(view.getContent().position()).as("%s: position of the shared buffer", page).isZero();
    }
  }

  /** Reading a cached page through the shared view allocates nothing per call: measured against {@code getPage} on this JVM. */
  @Test
  void aCachedPageReadAllocatesNoViewPerCall() throws Exception {
    final TypeIndex typeIndex = buildCompactedAndMutableIndex();
    final PageId first = pageId(mutableOf(typeIndex).getFileId(), 0);
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

    // the results are kept, so the JIT cannot drop the allocations it is asked to measure
    final BasePage[] kept = new BasePage[64];
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final long threadId = Thread.currentThread().threadId();
    final int calls = 20_000;

    for (int round = 0; round < 3; round++) {
      final long beforeOld = threads.getThreadAllocatedBytes(threadId);
      for (int i = 0; i < calls; i++)
        kept[i % kept.length] = tx.getPage(first, PAGE_SIZE);
      final long old = threads.getThreadAllocatedBytes(threadId) - beforeOld;

      final long beforeNew = threads.getThreadAllocatedBytes(threadId);
      for (int i = 0; i < calls; i++)
        kept[i % kept.length] = tx.getPageForAbsoluteReads(first, PAGE_SIZE);
      final long now = threads.getThreadAllocatedBytes(threadId) - beforeNew;

      if (round == 2)
        assertThat(now).as("bytes of %d shared reads, against %d for getPage", calls, old).isLessThan(old / 4);
    }
  }

  /** Lookups from many threads at once, all reading the same shared views, each get their own key's RID. */
  @Test
  void concurrentLookupsAnswerTheirOwnKeys() throws Exception {
    final TypeIndex typeIndex = buildCompactedAndMutableIndex();

    final Map<String, RID> ridOf = new HashMap<>();
    final Iterator<Document> docs = database.iterateType(TYPE_NAME, false);
    while (docs.hasNext()) {
      final Document doc = docs.next();
      ridOf.put(doc.getString("email"), doc.getIdentity());
    }

    final int threads = 8;
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      final List<Future<Integer>> results = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final int start = t;
        results.add(pool.submit(() -> {
          DatabaseContext.INSTANCE.init((DatabaseInternal) database);
          int wrong = 0;
          for (int i = start; i < COMPACTED + MUTABLE; i += threads)
            try (final IndexCursor cursor = typeIndex.get(new Object[] { key(i) })) {
              if (!cursor.hasNext() || !cursor.next().getIdentity().equals(ridOf.get(key(i))) || cursor.hasNext())
                ++wrong;
            }
          return wrong;
        }));
      }
      for (final Future<Integer> result : results)
        assertThat(result.get(120, TimeUnit.SECONDS)).isZero();
    } finally {
      pool.shutdownNow();
    }
  }

  /** A cached page over hand-written bytes, with its shared view and a per-call view to compare it with. */
  private static final class CachedPageFixture {
    final CachedPage    cached;
    final ImmutablePage shared;
    final ImmutablePage perCall;

    CachedPageFixture(final DatabaseInternal database, final int pageSize, final byte[] content, final int contentSize) {
      final MutablePage page = new MutablePage(new PageId(database, 1, 0), pageSize, content, 0, contentSize);
      this.cached = new CachedPage(page, true);
      this.shared = cached.getSharedImmutableView();
      this.perCall = cached.useAsImmutable();
    }
  }
}
