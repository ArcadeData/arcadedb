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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

class SharedPageViewTest {

  @Nested
  class PageIdHashCode extends TestHelper {

    private static final int[] VALUES = { 0, 1, 2, 31, 255, 256, 65_535, 65_536, 1_000_003, Integer.MAX_VALUE - 1, Integer.MAX_VALUE,
        -1, -2, -31, -65_536, Integer.MIN_VALUE + 1, Integer.MIN_VALUE };

    @Test
    void hashCodeIsTheValueObjectsHashReturned() {
      for (final int fileId : VALUES)
        for (final int pageNumber : VALUES) {
          final PageId pageId = new PageId(database, fileId, pageNumber);
          assertThat(pageId.hashCode()).as("fileId %d, pageNumber %d", fileId, pageNumber)
              .isEqualTo(Objects.hash(database, fileId, pageNumber));
          // a second call answers from the cached value and agrees
          assertThat(pageId.hashCode()).isEqualTo(Objects.hash(database, fileId, pageNumber));
        }
    }

    @Test
    void equalPageIdsHashAlikeAndKeepTheirKeyIdentityInAMap() {
      final Map<PageId, Integer> map = new HashMap<>();
      for (int fileId = 0; fileId < 20; fileId++)
        for (int pageNumber = 0; pageNumber < 50; pageNumber++)
          map.put(new PageId(database, fileId, pageNumber), fileId * 1000 + pageNumber);

      assertThat(map).hasSize(1000);
      for (int fileId = 0; fileId < 20; fileId++)
        for (int pageNumber = 0; pageNumber < 50; pageNumber++) {
          final PageId probe = new PageId(database, fileId, pageNumber);
          assertThat(probe.hashCode()).isEqualTo(new PageId(database, fileId, pageNumber).hashCode());
          assertThat(map.get(probe)).isEqualTo(fileId * 1000 + pageNumber);
        }
      assertThat(map.get(new PageId(database, 20, 0))).isNull();
    }
  }

  @Nested
  class BasePageEqualsHashCode extends TestHelper {

    /**
     * Two pages for the same {@link PageId} must be equal and share the same hash code regardless of their
     * (mutable) version, and equality must not change when the version is incremented.
     */
    @Test
    void equalsIsStableAcrossVersionChangesAndConsistentWithHashCode() {
      final PageId pageId = new PageId(database, 3, 7);

      final MutablePage a = new MutablePage(pageId, 1024, new byte[1024], 5, 0);
      final MutablePage b = new MutablePage(pageId, 1024, new byte[1024], 9, 0);

      // Same pageId, different version: equal and same hash code (version is not part of identity).
      assertThat(a).isEqualTo(b);
      assertThat(a.hashCode()).isEqualTo(b.hashCode());

      // Mutating the version must not change equality nor the hash code (it would, when version was part of equals).
      b.incrementVersion();
      assertThat(a).as("equality must stay stable across version changes").isEqualTo(b);
      assertThat(a.hashCode()).isEqualTo(b.hashCode());

      a.incrementVersion();
      a.incrementVersion();
      assertThat(a).as("equality is independent of how many times version changed").isEqualTo(b);
      assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    /**
     * Pages for different {@link PageId}s are never equal.
     */
    @Test
    void differentPageIdsAreNotEqual() {
      final MutablePage a = new MutablePage(new PageId(database, 3, 7), 1024, new byte[1024], 1, 0);
      final MutablePage differentPage = new MutablePage(new PageId(database, 3, 8), 1024, new byte[1024], 1, 0);
      final MutablePage differentFile = new MutablePage(new PageId(database, 4, 7), 1024, new byte[1024], 1, 0);

      assertThat(a).isNotEqualTo(differentPage);
      assertThat(a).isNotEqualTo(differentFile);
    }

    /**
     * A {@link MutablePage} and an {@link ImmutablePage} with the same {@link PageId} are different types and must
     * not be equal (class-sensitive equality is preserved).
     */
    @Test
    void differentPageTypesAreNotEqual() {
      final PageId pageId = new PageId(database, 3, 7);
      final MutablePage mutable = new MutablePage(pageId, 1024, new byte[1024], 1, 0);
      final ImmutablePage immutable = new ImmutablePage(pageId, 1024, new byte[1024], 1, 0);

      assertThat(mutable).isNotEqualTo(immutable);
    }

    /**
     * The general {@code equals}/{@code hashCode} contract: equal pages must share the same hash code.
     */
    @Test
    void equalPagesShareHashCode() {
      final PageId pageId = new PageId(database, 1, 2);
      final MutablePage a = new MutablePage(pageId, 1024, new byte[1024], 0, 0);
      final MutablePage b = new MutablePage(pageId, 1024, new byte[1024], 0, 0);

      if (a.equals(b))
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }
  }

  @Nested
  class SharedPageViewSemantics extends TestHelper {
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

  @Nested
  class SharedPageViewReaders extends TestHelper {
    private static final int DOCS = 3_000;

    @Test
    void aRecordReadAndAScanReadTheirPagesThroughTheSharedView() {
      final Map<RID, Integer> idOf = createDocuments("Doc", DOCS);
      reopenDatabase();

      final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Doc").getBuckets(false).getFirst();
      final RID middle = idOf.keySet().stream().sorted().skip(DOCS / 2).findFirst().orElseThrow();
      final CachedPage recordPage = cachedPage(bucket, (int) (middle.getPosition() / bucket.getMaxRecordsInPage()));
      assertThat(recordPage.peekSharedImmutableView()).as("before the read").isNull();

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      try {
        assertThat(load(middle).getInteger("id")).isEqualTo(idOf.get(middle));
      } finally {
        database.rollback();
      }
      assertThat(cachedPage(bucket, recordPage.getPageId().getPageNumber()).peekSharedImmutableView()).as("after the read").isNotNull();

      // A scan: every page of the bucket, each read through the shared view, every record answered as written
      final Map<RID, Integer> scanned = new HashMap<>();
      final Iterator<Record> records = bucket.iterator();
      while (records.hasNext()) {
        final Document doc = (Document) records.next();
        scanned.put(doc.getIdentity(), doc.getInteger("id"));
      }
      assertThat(scanned).isEqualTo(idOf);
      for (int p = 0; p < bucket.getTotalPages(); p++)
        assertThat(cachedPage(bucket, p).peekSharedImmutableView()).as("bucket page %d after the scan", p).isNotNull();
    }

    @Test
    void aRecordSpreadOverSeveralPagesIsReadWholeFromTheSharedViewOfItsHead() {
      database.getSchema().createDocumentType("Big");
      final String content = "0123456789".repeat(30_000);
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Big").set("text", content).save().getIdentity());
      reopenDatabase();

      final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Big").getBuckets(false).getFirst();
      assertThat(bucket.getTotalPages()).as("the record spans several pages").isGreaterThan(1);
      final int headPage = (int) (rid[0].getPosition() / bucket.getMaxRecordsInPage());

      assertThat(load(rid[0]).getString("text")).isEqualTo(content);
      assertThat(cachedPage(bucket, headPage).peekSharedImmutableView()).as("the head page").isNotNull();

      // And an update that grows it again reads it back whole
      database.transaction(() -> load(rid[0]).modify().set("text", content + content).save());
      assertThat(load(rid[0]).getString("text")).isEqualTo(content + content);
    }

    @Test
    void repeatableReadKeepsTheHeadPageItRead() {
      final Map<RID, Integer> idOf = createDocuments("Doc", 100);
      reopenDatabase();
      final RID first = idOf.keySet().iterator().next();
      final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Doc").getBuckets(false).getFirst();
      final PageId pageId = new PageId((DatabaseInternal) database, bucket.getFileId(),
          (int) (first.getPosition() / bucket.getMaxRecordsInPage()));

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        assertThat(load(first).getInteger("id")).isEqualTo(idOf.get(first));
        final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
        final BasePage pinned = tx.getPinnedPage(pageId);
        assertThat(pinned).as("the page the transaction keeps").isNotNull();
        assertThat(pinned).as("never the shared view").isNotInstanceOf(SharedImmutablePage.class);
      } finally {
        database.rollback();
      }
    }

    @Test
    void aHashIndexLookupReadsItsPagesThroughTheSharedView() {
      database.getSchema().buildDocumentType().withName("Hashed").withTotalBuckets(1).create();
      database.getSchema().getType("Hashed").createProperty("id", Integer.class);
      database.getSchema().getType("Hashed").createProperty("group", Integer.class);
      database.getSchema().buildTypeIndex("Hashed", new String[] { "id" }).withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();
      database.getSchema().buildTypeIndex("Hashed", new String[] { "group" }).withType(Schema.INDEX_TYPE.HASH).withUnique(false).create();

      // 3 groups of 1,000 RIDs each: a key whose RIDs no longer fit its entry keeps them in a RID list of its own pages
      final Map<Integer, RID> ridOfId = new HashMap<>();
      final Map<Integer, Set<RID>> ridsOfGroup = new HashMap<>();
      database.transaction(() -> {
        for (int i = 0; i < DOCS; i++) {
          final MutableDocument doc = database.newDocument("Hashed").set("id", i, "group", i % 3).save();
          ridOfId.put(i, doc.getIdentity());
          ridsOfGroup.computeIfAbsent(i % 3, k -> new HashSet<>()).add(doc.getIdentity());
        }
      });
      reopenDatabase();

      final TypeIndex unique = database.getSchema().getType("Hashed").getIndexesByProperties("id").getFirst();
      final TypeIndex grouped = database.getSchema().getType("Hashed").getIndexesByProperties("group").getFirst();
      final IndexInternal uniqueOnBucket = (IndexInternal) unique.getIndexesOnBuckets()[0];
      final CachedPage metaBefore = cachedPage(uniqueOnBucket.getFileId(), uniqueOnBucket.getPageSize(), 0);
      assertThat(metaBefore.peekSharedImmutableView()).as("metadata page before the lookups").isNull();

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      try {
        for (int i = 0; i < DOCS; i++)
          try (final IndexCursor cursor = unique.get(new Object[] { i })) {
            assertThat(cursor.hasNext()).as("key %d", i).isTrue();
            assertThat(cursor.next().getIdentity()).isEqualTo(ridOfId.get(i));
            assertThat(cursor.hasNext()).isFalse();
          }
        for (int g = 0; g < 3; g++) {
          final Set<RID> found = new HashSet<>();
          try (final IndexCursor cursor = grouped.get(new Object[] { g })) {
            while (cursor.hasNext())
              found.add(cursor.next().getIdentity());
          }
          assertThat(found).as("group %d", g).isEqualTo(ridsOfGroup.get(g));
        }
        try (final IndexCursor cursor = unique.get(new Object[] { DOCS + 1 })) {
          assertThat(cursor.hasNext()).as("an absent key").isFalse();
        }
      } finally {
        database.rollback();
      }

      assertThat(cachedPage(uniqueOnBucket.getFileId(), uniqueOnBucket.getPageSize(), 0).peekSharedImmutableView())
          .as("metadata page after the lookups").isNotNull();
      final IndexInternal groupedOnBucket = (IndexInternal) grouped.getIndexesOnBuckets()[0];
      final PaginatedComponent groupedFile = (PaginatedComponent) database.getSchema().getFileById(groupedOnBucket.getFileId());
      int shared = 0;
      for (int p = 0; p < groupedFile.getTotalPages(); p++)
        if (cachedPage(groupedOnBucket.getFileId(), groupedOnBucket.getPageSize(), p).peekSharedImmutableView() != null)
          ++shared;
      // The metadata, the directory, the buckets of 3 keys and their RID lists: more than one page, none read through a view of its own
      assertThat(shared).as("pages of the non-unique index read through the shared view").isGreaterThan(2);
    }

    @Test
    void anLsmRangeCursorReadsItsPagesThroughTheSharedView() {
      final Map<RID, Integer> idOf = createDocuments("Ranged", DOCS);
      database.getSchema().buildTypeIndex("Ranged", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
      reopenDatabase();

      final TypeIndex index = database.getSchema().getType("Ranged").getIndexesByProperties("id").getFirst();
      final IndexInternal onBucket = (IndexInternal) index.getIndexesOnBuckets()[0];
      final PaginatedComponent file = (PaginatedComponent) database.getSchema().getFileById(onBucket.getFileId());
      for (int p = 0; p < file.getTotalPages(); p++)
        assertThat(cachedPage(onBucket.getFileId(), onBucket.getPageSize(), p).peekSharedImmutableView()).as("page %d before", p).isNull();

      final List<Integer> ascending = new ArrayList<>();
      final List<Integer> descending = new ArrayList<>();
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      try {
        try (final IndexCursor cursor = index.range(true, new Object[] { 100 }, true, new Object[] { 2_000 }, false)) {
          while (cursor.hasNext())
            ascending.add(cursor.next().asDocument(true).getInteger("id"));
        }
        try (final IndexCursor cursor = index.iterator(false)) {
          while (cursor.hasNext())
            descending.add(cursor.next().asDocument(true).getInteger("id"));
        }
      } finally {
        database.rollback();
      }

      assertThat(ascending).hasSize(1_900).isSorted().startsWith(100).endsWith(1_999);
      assertThat(descending).hasSize(idOf.size()).isSortedAccordingTo((a, b) -> Integer.compare(b, a));
      for (int p = 0; p < file.getTotalPages(); p++)
        assertThat(cachedPage(onBucket.getFileId(), onBucket.getPageSize(), p).peekSharedImmutableView()).as("page %d after", p).isNotNull();
    }

    /** A fixed-length byte array read off the shared view is copied out of the backing array: no private buffer per call. */
    @Test
    void aByteArrayReadOffTheSharedViewAllocatesNoBufferPerCall() {
      final int pageSize = 4096;
      final byte[] bytes = new byte[pageSize];
      for (int i = BasePage.PAGE_HEADER_SIZE; i < pageSize; i++)
        bytes[i] = (byte) i;
      final MutablePage page = new MutablePage(new PageId((DatabaseInternal) database, 1, 0), pageSize, bytes, 0, pageSize);
      final CachedPage cached = new CachedPage(page, true);
      final ImmutablePage shared = cached.getSharedImmutableView();
      final ImmutablePage perCall = cached.useAsImmutable();

      final byte[] fromShared = new byte[16];
      final byte[] fromPerCall = new byte[16];
      for (int offset = 0; offset < 200; offset += 7) {
        shared.readByteArray(offset, fromShared);
        perCall.readByteArray(offset, fromPerCall);
        assertThat(fromShared).as("at %d", offset).isEqualTo(fromPerCall);
        shared.readByteArray(offset, fromShared, 3, 5);
        perCall.readByteArray(offset, fromPerCall, 3, 5);
        assertThat(fromShared).as("at %d into offset 3", offset).isEqualTo(fromPerCall);
      }

      final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
      final long threadId = Thread.currentThread().threadId();
      final int calls = 50_000;
      long allocated = 0;
      for (int round = 0; round < 3; round++) {
        final long before = threads.getThreadAllocatedBytes(threadId);
        for (int i = 0; i < calls; i++)
          shared.readByteArray(i % 1_000, fromShared);
        allocated = threads.getThreadAllocatedBytes(threadId) - before;
      }
      // a Binary and a ByteBuffer per call were about 100 bytes: what is left is the measurement's own noise
      assertThat(allocated).as("bytes allocated by %d reads", calls).isLessThan(calls);
    }

    // -------------------------------------------------------------------------------------------------------------------------

    private Map<RID, Integer> createDocuments(final String type, final int count) {
      database.getSchema().buildDocumentType().withName(type).withTotalBuckets(1).create().createProperty("id", Integer.class);
      final Map<RID, Integer> idOf = new HashMap<>();
      database.transaction(() -> {
        for (int i = 0; i < count; i++)
          idOf.put(database.newDocument(type).set("id", i, "pad", "x".repeat(40)).save().getIdentity(), i);
      });
      return idOf;
    }

    /** The record at {@code rid}, read through the database open now: a RID kept from before a reopen names the closed one. */
    private Document load(final RID rid) {
      return (Document) database.lookupByRID(rid, true);
    }

    private CachedPage cachedPage(final LocalBucket bucket, final int pageNumber) {
      return cachedPage(bucket.getFileId(), bucket.getPageSize(), pageNumber);
    }

    private CachedPage cachedPage(final int fileId, final int pageSize, final int pageNumber) {
      try {
        return ((DatabaseInternal) database).getPageManager()
            .getCachedPageForRead(new PageId((DatabaseInternal) database, fileId, pageNumber), pageSize);
      } catch (final IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Nested
  class SharedPageViewVarInt extends TestHelper {
    private static final int THREADS    = 4;
    private static final int ITERATIONS = 500_000;

    /**
     * Public reads of {@link BasePage} / {@link ImmutablePage} that a shared instance can answer as they are: each reads at an
     * absolute position, or builds a buffer of the caller's own, or reads only final/metadata fields. Anything else must be
     * overridden by {@link SharedImmutablePage}.
     */
    private static final Map<String, String> POSITION_FREE = Map.ofEntries(//
        Map.entry("getImmutableView(int,int)", "a slice over a duplicate of the buffer"),//
        Map.entry("modify()", "a copy of the array"),//
        Map.entry("getPhysicalSize()", "a final field"),//
        Map.entry("getMaxContentSize()", "a final field"),//
        Map.entry("getAvailableContentSize()", "the content size, metadata of the cached page"),//
        Map.entry("getContentSize()", "metadata of the cached page"),//
        Map.entry("getVersion()", "metadata of the cached page"),//
        Map.entry("readNumber(int)", "an absolute varint decode, #10930"),//
        Map.entry("readNumberSize(int)", "an absolute varint scan, #10930"),//
        Map.entry("readNumberAndSize(int)", "readNumber + readNumberSize"),//
        Map.entry("readUnsignedNumber(int)", "an absolute varint decode, #11298"),//
        Map.entry("readFloatArray(int,float[],int)", "an absolute slice, a buffer of the caller's own, #11298"),//
        Map.entry("readLong(int)", "an absolute read"),//
        Map.entry("readInt(int)", "an absolute read"),//
        Map.entry("readFloat(int)", "an absolute read"),//
        Map.entry("readUnsignedInt(int)", "an absolute read"),//
        Map.entry("readShort(int)", "an absolute read"),//
        Map.entry("readUnsignedShort(int)", "an absolute read"),//
        Map.entry("readByte(int)", "an absolute read"),//
        Map.entry("readUnsignedByte(int)", "an absolute read"),//
        Map.entry("isSameContentAs(int,Binary,int,int)", "an array comparison"),//
        Map.entry("readString()", "delegates to readBytes(), which the shared view overrides"),//
        Map.entry("readString(int)", "delegates to readBytes(int), which the shared view overrides"),//
        Map.entry("getPageId()", "a final field"),//
        Map.entry("slice()", "an absolute slice, a buffer of the caller's own"),//
        Map.entry("equals(Object)", "the page id"),//
        Map.entry("hashCode()", "the page id"),//
        Map.entry("toString()", "the page id and version"));

    /** The instance state of a page: none of it is written by a read. A new field is per-read state until shown otherwise. */
    private static final Set<String> PAGE_FIELDS = Set.of("pageId", "content", "size", "version");

    @Test
    void manyThreadsReadingVarIntsFromTheSharedViewReadTheirOwn() throws Exception {
      final int pageSize = 4096;
      final Binary writer = new Binary(new byte[pageSize], pageSize);
      // TWO NUMBERS OF DIFFERENT ENCODED LENGTH: A STOLEN OFFSET CHANGES BOTH THE VALUE AND THE SIZE
      final int[] at = { 100, 200 };
      final long[] values = { 5L, 123_456_789_012L };
      final int[] sizes = new int[2];
      for (int i = 0; i < at.length; i++)
        sizes[i] = writer.putNumber(BasePage.PAGE_HEADER_SIZE + at[i], values[i]);
      assertThat(sizes[0]).isEqualTo(1);
      assertThat(sizes[1]).isEqualTo(6);

      final MutablePage page = new MutablePage(new PageId((DatabaseInternal) database, 1, 0), pageSize, writer.getContent(), 0,
          pageSize);
      final ImmutablePage shared = new CachedPage(page, true).getSharedImmutableView();
      assertThat(shared).isInstanceOf(SharedImmutablePage.class);

      final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
      try {
        final List<Future<long[]>> results = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
          final int first = t % 2;
          results.add(pool.submit(() -> {
            long wrongValues = 0;
            long wrongSizes = 0;
            for (int n = 0; n < ITERATIONS; n++) {
              final int i = (first + n) % 2;
              if (shared.readNumber(at[i]) != values[i])
                ++wrongValues;
              if (shared.readNumberSize(at[i]) != sizes[i])
                ++wrongSizes;
              final long[] pair = shared.readNumberAndSize(at[i]);
              if (pair[0] != values[i] || pair[1] != sizes[i])
                ++wrongValues;
            }
            return new long[] { wrongValues, wrongSizes };
          }));
        }
        for (final Future<long[]> result : results)
          assertThat(result.get(120, TimeUnit.SECONDS)).as("wrong values, wrong sizes").containsExactly(0L, 0L);
      } finally {
        pool.shutdownNow();
      }
    }

    @Test
    void readNumberDoesNotMoveThePosition() {
      final MutablePage page = new MutablePage(new PageId((DatabaseInternal) database, 1, 0), 1024);
      page.writeNumber(40, 123_456_789L);
      page.setBufferPosition(7);
      assertThat(page.readNumber(40)).isEqualTo(123_456_789L);
      assertThat(page.readNumberSize(40)).isEqualTo(Binary.getNumberSpace(123_456_789L));
      assertThat(page.readNumberAndSize(40)).containsExactly(123_456_789L, Binary.getNumberSpace(123_456_789L));
      assertThat(page.getBufferPosition()).isEqualTo(7);
    }

    @Test
    void readStringAtAnIndexReadsTheStringWrittenThere() {
      // readString(int) added the page header twice, once itself and once through readBytes(int)
      final MutablePage page = new MutablePage(new PageId((DatabaseInternal) database, 1, 0), 1024);
      page.writeString(30, "arcade");
      assertThat(page.readString(30)).isEqualTo("arcade");
    }

    @Test
    void everyPageReadIsPositionFreeOrAnsweredByTheSharedView() {
      final Set<String> overridden = new TreeSet<>();
      for (final Method m : SharedImmutablePage.class.getDeclaredMethods())
        if (Modifier.isPublic(m.getModifiers()))
          overridden.add(signature(m));

      final Set<String> uncovered = new TreeSet<>();
      final Set<String> seen = new TreeSet<>();
      for (final Class<?> c : List.of(BasePage.class, ImmutablePage.class))
        for (final Method m : c.getDeclaredMethods()) {
          if (!Modifier.isPublic(m.getModifiers()) || Modifier.isStatic(m.getModifiers()) || m.isSynthetic())
            continue;
          final String signature = signature(m);
          seen.add(signature);
          if (!overridden.contains(signature) && !POSITION_FREE.containsKey(signature))
            uncovered.add(signature);
        }

      assertThat(uncovered).as(
          "public page methods neither overridden by SharedImmutablePage nor listed as position-free: a shared instance would answer "
              + "them on the one buffer every thread reads").isEmpty();
      // A STALE ENTRY OF THE ALLOW-LIST IS A METHOD THAT WAS REMOVED OR RENAMED: KEEP THE LIST HONEST
      assertThat(seen).as("allow-listed methods that no longer exist").containsAll(POSITION_FREE.keySet());

      final Set<String> fields = new TreeSet<>();
      for (final Class<?> c : List.of(BasePage.class, ImmutablePage.class, SharedImmutablePage.class))
        for (final Field f : c.getDeclaredFields())
          if (!Modifier.isStatic(f.getModifiers()) && !f.isSynthetic())
            fields.add(f.getName());
      assertThat(fields).as("instance fields of a page: a new one kept by a read is a channel threads of a shared view would share")
          .isEqualTo(new TreeSet<>(PAGE_FIELDS));
    }

    private static String signature(final Method m) {
      final StringBuilder b = new StringBuilder(m.getName()).append('(');
      final Class<?>[] params = m.getParameterTypes();
      for (int i = 0; i < params.length; i++) {
        if (i > 0)
          b.append(',');
        b.append(params[i].getSimpleName());
      }
      return b.append(')').toString();
    }
  }

  @Nested
  class PageVarIntReadAllocation extends TestHelper {

    // EVERY ENCODED LENGTH, BOTH SIGNS, AND THE RECORD MARKERS THE BUCKET STORES IN A RECORD HEADER
    private static final long[] VALUES = { 0L, 1L, -1L, 63L, -64L, 64L, -65L, 127L, 128L, 8191L, -8192L, 8192L, 1L << 20, -(1L << 20),
        1L << 27, 1L << 34, 1L << 41, 1L << 48, 1L << 55, -(1L << 55), 1L << 62, Long.MAX_VALUE, Long.MIN_VALUE,
        LocalBucket.FIRST_CHUNK, LocalBucket.NEXT_CHUNK, LocalBucket.RECORD_PLACEHOLDER_POINTER, -5L };

    @Test
    void readNumberAgreesWithReadNumberAndSizeOnEveryLength() {
      final MutablePage page = new MutablePage(new PageId(database, 1, 0), 1024);
      final int[] positions = new int[VALUES.length];
      final int[] written = new int[VALUES.length];
      int position = 0;
      for (int i = 0; i < VALUES.length; i++) {
        positions[i] = position;
        written[i] = page.writeNumber(position, VALUES[i]);
        position += written[i];
      }

      for (int i = 0; i < VALUES.length; i++) {
        final long value = page.readNumber(positions[i]);
        final int size = page.readNumberSize(positions[i]);
        assertThat(value).as("value at #%d", i).isEqualTo(VALUES[i]);
        assertThat(size).as("bytes read at #%d", i).isEqualTo(written[i]).isEqualTo(Binary.getNumberSpace(VALUES[i]));

        // THE ALLOCATING FORM IS NOW A WRAPPER OF THE SAME READ: IT MUST ANSWER THE SAME PAIR
        final long[] pair = page.readNumberAndSize(positions[i]);
        assertThat(pair[0]).isEqualTo(value);
        assertThat(pair[1]).isEqualTo(size);
      }

      // OUT OF ORDER: THE SIZE IS THE ONE OF THE NUMBER ASKED FOR, NOT OF THE LAST READ NOR OF A RUNNING POSITION (#10930)
      final int last = VALUES.length - 1;
      page.readNumber(positions[last]);
      assertThat(page.readNumberSize(positions[0])).isEqualTo(written[0]);
    }

    @Test
    void readNumberOnAnImmutablePageAgrees() {
      final MutablePage page = new MutablePage(new PageId(database, 1, 0), 1024);
      final int size = page.writeNumber(10, 300_000L);
      final ImmutablePage immutable = new ImmutablePage(page.getPageId(), 1024, page.getContent().array(), 0, 1024);

      assertThat(immutable.readNumber(10)).isEqualTo(300_000L);
      assertThat(immutable.readNumberSize(10)).isEqualTo(size);
    }

    @Test
    void readNumberPastThePageFailsLikeReadNumberAndSize() {
      final MutablePage page = new MutablePage(new PageId(database, 1, 0), 1024);
      assertThatThrownBy(() -> page.readNumberAndSize(1_000_000)).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> page.readNumber(1_000_000)).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> page.readNumberSize(1_000_000)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readNumberAllocatesNothing() {
      assumeThat(ManagementFactory.getThreadMXBean()).isInstanceOf(ThreadMXBean.class);
      final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
      assumeThat(threads.isThreadAllocatedMemorySupported()).isTrue();
      threads.setThreadAllocatedMemoryEnabled(true);

      final MutablePage page = new MutablePage(new PageId(database, 1, 0), 1024);
      int position = 0;
      for (final long v : VALUES)
        position += page.writeNumber(position, v);
      final int end = position;

      final int rounds = 100_000;
      long checksum = readAll(page, end, 20_000); // WARM UP: LET THE JIT COMPILE THE LOOP BEFORE MEASURING
      final long threadId = Thread.currentThread().threadId();
      final long before = threads.getThreadAllocatedBytes(threadId);
      checksum += readAll(page, end, rounds);
      final long allocated = threads.getThreadAllocatedBytes(threadId) - before;

      assertThat(checksum).isNotZero();
      // A long[2] IS 32 BYTES: ONE PER READ WOULD BE rounds * VALUES.length * 32, ABOUT 86 MB HERE. THE BOUND LEAVES ROOM
      // FOR THE JVM'S OWN INCIDENTAL ALLOCATIONS ON THIS THREAD, NOT FOR ONE ARRAY PER READ
      assertThat(allocated).as("bytes allocated by %d varint reads", rounds * VALUES.length).isLessThan(256 * 1024L);
    }

    /**
     * The read paths that moved to the allocation-free form, end to end: a scan, an iterator, a lookup and a count over
     * in-page records, records that grew after they were written, and records split in chunks.
     */
    @Test
    void bucketReadPathsStillAnswerEveryRecordShape() {
      final DocumentType type = database.getSchema().createDocumentType("VarIntRead", 1);
      final List<RID> rids = new ArrayList<>();
      final int pageSize = ((LocalBucket) type.getBuckets(false).getFirst()).getPageSize();

      database.transaction(() -> {
        for (int i = 0; i < 50; i++) {
          final MutableDocument doc = database.newDocument("VarIntRead");
          doc.set("i", i);
          doc.set("text", "x".repeat(i % 10 == 0 ? pageSize * 2 : 10)); // EVERY 10TH SPANS PAGES
          doc.save();
          rids.add(doc.getIdentity());
        }
      });

      // GROW SOME SMALL RECORDS PAST THEIR SLOT, SO THEY ARE SHIFTED IN THEIR PAGE OR SPILLED OFF IT
      database.transaction(() -> {
        for (int i = 1; i < 50; i += 7) {
          final MutableDocument doc = rids.get(i).asDocument(true).modify();
          doc.set("text", "y".repeat(2_000));
          doc.save();
        }
      });

      database.transaction(() -> {
        assertThat(database.countType("VarIntRead", false)).isEqualTo(50);

        final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
        assertThat(bucket.count()).isEqualTo(50);

        final boolean[] seenByIterator = new boolean[50];
        bucket.iterator().forEachRemaining(r -> seenByIterator[r.asDocument().getInteger("i")] = true);
        for (final boolean seen : seenByIterator)
          assertThat(seen).isTrue();

        final int[] scanned = { 0 };
        bucket.scan((rid, view) -> {
          scanned[0]++;
          return true;
        }, null);
        assertThat(scanned[0]).isEqualTo(50);

        for (int i = 0; i < 50; i++) {
          final String text = rids.get(i).asDocument(false).getString("text");
          if (i % 7 == 1)
            assertThat(text).isEqualTo("y".repeat(2_000));
          else
            assertThat(text).hasSize(i % 10 == 0 ? pageSize * 2 : 10);
        }
      });
    }

    /**
     * The write-side readers that moved to the allocation-free form: the slot marker read of the existence and layout
     * checks, the content end an insert appends at, the per-slot walk of the commit-time page compression, and the
     * re-read of a record taken for update.
     */
    @Test
    void writePathsStillMeasureEveryRecordShape() {
      final DocumentType type = database.getSchema().createDocumentType("VarIntWrite", 1);
      final List<RID> rids = new ArrayList<>();
      final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
      final int pageSize = bucket.getPageSize();

      database.transaction(() -> {
        for (int i = 0; i < 40; i++) {
          final MutableDocument doc = database.newDocument("VarIntWrite");
          doc.set("i", i);
          doc.set("text", "x".repeat(i % 10 == 0 ? pageSize * 2 : 10)); // EVERY 10TH SPANS PAGES
          doc.save();
          rids.add(doc.getIdentity());
        }
      });

      database.transaction(() -> {
        for (int i = 0; i < 40; i++) {
          assertThat(bucket.isRecordPresent(rids.get(i))).as("record %d present", i).isTrue();
          // AN IN-PAGE RECORD HAS A POSITIVE MARKER, A MULTI-PAGE ONE FIRST_CHUNK
          assertThat(bucket.isRecordStoredInSinglePage(rids.get(i))).as("record %d in one page", i).isEqualTo(i % 10 != 0);
        }
        // A SLOT NEVER WRITTEN, ON A PAGE THAT DOES NOT EXIST
        final RID missing = new RID(rids.getFirst().getBucketId(), 1_000_000_000L);
        assertThat(bucket.isRecordPresent(missing)).isFalse();
        assertThat(bucket.isRecordStoredInSinglePage(missing)).isFalse();
      });

      // DELETE SOME, GROW OTHERS (A RECORD TAKEN FOR UPDATE IS RE-READ AND COMPARED), THEN APPEND MORE: EVERY COMMIT
      // COMPRESSES THE PAGES IT TOUCHED AND EVERY INSERT MEASURES WHERE THE PAGE CONTENT ENDS
      database.transaction(() -> {
        for (int i = 3; i < 40; i += 9)
          rids.get(i).asDocument(true).delete();
        for (int i = 1; i < 40; i += 6) {
          if (i % 9 == 3)
            continue;
          final MutableDocument doc = rids.get(i).asDocument(true).modify();
          doc.set("text", "y".repeat(3_000));
          doc.save();
        }
      });
      database.transaction(() -> {
        for (int i = 40; i < 60; i++) {
          final MutableDocument doc = database.newDocument("VarIntWrite");
          doc.set("i", i);
          doc.set("text", "z".repeat(i % 5 == 0 ? 1_500 : 20));
          doc.save();
          rids.add(doc.getIdentity());
        }
      });

      database.transaction(() -> {
        int live = 0;
        for (int i = 0; i < 60; i++) {
          if (i < 40 && i % 9 == 3 && rids.subList(40, 60).contains(rids.get(i)))
            // A DELETED SLOT HANDED TO A LATER INSERT: THE RID IS LIVE AGAIN, CHECKED AND COUNTED UNDER THAT INSERT
            continue;
          final boolean deleted = i < 40 && i % 9 == 3;
          assertThat(bucket.isRecordPresent(rids.get(i))).as("record %d present", i).isEqualTo(!deleted);
          if (deleted)
            continue;
          live++;
          final String text = rids.get(i).asDocument(false).getString("text");
          if (i >= 40)
            assertThat(text).isEqualTo("z".repeat(i % 5 == 0 ? 1_500 : 20));
          else if (i % 6 == 1)
            assertThat(text).isEqualTo("y".repeat(3_000));
          else
            assertThat(text).hasSize(i % 10 == 0 ? pageSize * 2 : 10);
        }
        assertThat(bucket.count()).isEqualTo(live);
        assertThat(database.countType("VarIntWrite", false)).isEqualTo(live);
      });

      // THE PAGES THE WRITES LEFT BEHIND ARE CONSISTENT ON DISK
      assertThat(database.command("sql", "check database").next().<Number>getProperty("totalErrors").longValue()).isZero();
    }

    private static long readAll(final BasePage page, final int end, final int rounds) {
      long checksum = 0;
      for (int r = 0; r < rounds; r++) {
        int position = 0;
        while (position < end) {
          checksum += page.readNumber(position);
          position += page.readNumberSize(position);
        }
      }
      return checksum;
    }
  }
}
