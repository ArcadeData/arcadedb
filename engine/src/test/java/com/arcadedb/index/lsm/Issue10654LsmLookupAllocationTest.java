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
package com.arcadedb.index.lsm;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10654 (items 2 and 3): a lookup in the not-yet-compacted pages of an LSM index built a {@code PageId} and two buffers
 * per page it visited, and every bloom filter probe built a {@code PageId} and a view of the filter page. The ids now come from
 * a per-component {@link PageIdCache}, the page search builds one buffer instead of two, and the probe reads the shared cached
 * page. What a lookup answers must not change, so the answers are pinned over an index with compacted series AND mutable pages.
 */
class Issue10654LsmLookupAllocationTest extends TestHelper {
  private static final String TYPE_NAME     = "Doc";
  private static final int    PAGE_SIZE     = 256 * 1024;
  private static final String KEY_PAD       = "x".repeat(64);
  private static final int    COMPACTED     = 40_000;
  private static final int    MUTABLE       = 12_000;
  private static final int    ABSENT        = 2_000;
  private static final int    LOOKUPS       = 2_000;

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

  /** A unique index whose older keys sit in compacted series (with bloom filters) and whose newer keys sit in mutable pages. */
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

    assertThat(mutableOf(typeIndex).getSubIndex().getSeriesCount()).as("compacted series").isGreaterThanOrEqualTo(2);
    assertThat(mutableOf(typeIndex).getSubIndex().getBloomFilter()).as("the compaction wrote a filter file").isNotNull();
    assertThat(mutableOf(typeIndex).getTotalPages()).as("mutable pages the lookup walks").isGreaterThanOrEqualTo(3);
    return typeIndex;
  }

  private static LSMTreeIndexMutable mutableOf(final TypeIndex typeIndex) {
    return ((LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0]).getMutableIndex();
  }

  /** Every key comes back with its own RID, from the compacted series and from the mutable pages; absent keys come back empty. */
  @Test
  void lookupsAnswerTheSameOverCompactedAndMutablePages() throws Exception {
    final TypeIndex typeIndex = buildCompactedAndMutableIndex();

    final Map<String, RID> ridOf = new HashMap<>();
    final Iterator<Document> docs = database.iterateType(TYPE_NAME, false);
    while (docs.hasNext()) {
      final Document doc = docs.next();
      ridOf.put(doc.getString("email"), doc.getIdentity());
    }
    assertThat(ridOf).hasSize(COMPACTED + MUTABLE);

    // two passes: the second one is answered from the ids and buffers the first one built
    for (int pass = 0; pass < 2; pass++) {
      for (int i = 0; i < COMPACTED + MUTABLE; i++) {
        try (final IndexCursor cursor = typeIndex.get(new Object[] { key(i) })) {
          assertThat(cursor.hasNext()).as("key %d, pass %d", i, pass).isTrue();
          assertThat(cursor.next().getIdentity()).as("key %d, pass %d", i, pass).isEqualTo(ridOf.get(key(i)));
          assertThat(cursor.hasNext()).as("key %d is unique", i).isFalse();
        }
      }
      for (int i = 0; i < ABSENT; i++)
        try (final IndexCursor cursor = typeIndex.get(new Object[] { key(i) + "-absent" })) {
          assertThat(cursor.hasNext()).as("absent key %d, pass %d", i, pass).isFalse();
        }
    }
  }

  /** {@code slice()} builds one buffer where {@code duplicate().position().slice()} built two: measured against that, on this JVM. */
  @Test
  void sliceBuildsOneBufferWhereTwoWereBuilt() throws Exception {
    final TypeIndex typeIndex = buildCompactedAndMutableIndex();
    final BasePage page = ((DatabaseInternal) database).getPageManager()
        .getImmutablePage(new PageId((DatabaseInternal) database, mutableOf(typeIndex).getFileId(), 0), PAGE_SIZE, false, false);
    final ByteBuffer content = page.getContent();

    // the results are kept, so the JIT cannot drop the allocations it is asked to measure
    final ByteBuffer[] kept = new ByteBuffer[64];
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final long threadId = Thread.currentThread().threadId();
    final int calls = 20_000;

    for (int round = 0; round < 3; round++) {
      final long beforeOld = threads.getThreadAllocatedBytes(threadId);
      for (int i = 0; i < calls; i++) {
        final ByteBuffer duplicate = content.duplicate();
        duplicate.position(BasePage.PAGE_HEADER_SIZE);
        kept[i % kept.length] = duplicate.slice();
      }
      final long old = threads.getThreadAllocatedBytes(threadId) - beforeOld;

      final long beforeNew = threads.getThreadAllocatedBytes(threadId);
      for (int i = 0; i < calls; i++)
        kept[i % kept.length] = page.slice();
      final long now = threads.getThreadAllocatedBytes(threadId) - beforeNew;

      if (round == 2)
        assertThat(now).as("bytes of %d slice() calls, against %d for duplicate().position().slice()", calls, old)
            .isLessThan(old * 3 / 4);
    }
  }

  /** The ids are equal to the ones a lookup used to build, are handed out again, and a bad page number cannot grow the cache. */
  @Test
  void pageIdsAreTheOnesANewPageIdWouldBe() throws Exception {
    final TypeIndex typeIndex = buildCompactedAndMutableIndex();
    final LSMTreeIndexMutable mutable = mutableOf(typeIndex);
    final PageIdCache cache = new PageIdCache(mutable);
    final int total = mutable.getTotalPages();

    for (int p = 0; p < total; p++) {
      final PageId id = cache.get(p);
      assertThat(id).isEqualTo(new PageId((DatabaseInternal) database, mutable.getFileId(), p));
      assertThat(id.hashCode()).isEqualTo(new PageId((DatabaseInternal) database, mutable.getFileId(), p).hashCode());
      assertThat(cache.get(p)).as("page %d is handed out again", p).isSameAs(id);
    }
    for (final int bad : new int[] { -1, total, total + 7, Integer.MAX_VALUE, Integer.MIN_VALUE })
      assertThat(cache.get(bad)).as("page %d", bad).isEqualTo(new PageId((DatabaseInternal) database, mutable.getFileId(), bad));
  }

  /** {@code BasePage.slice()} is still a buffer over the page content with its own position, and one object, not two. */
  @Test
  void sliceIsTheContentBufferWithItsOwnPosition() throws Exception {
    final TypeIndex typeIndex = buildCompactedAndMutableIndex();
    final LSMTreeIndexMutable mutable = mutableOf(typeIndex);
    final BasePage page = ((DatabaseInternal) database).getPageManager()
        .getImmutablePage(new PageId((DatabaseInternal) database, mutable.getFileId(), 0), PAGE_SIZE, false, false);

    final ByteBuffer content = page.getContent();
    final ByteBuffer expected = content.duplicate();
    expected.position(BasePage.PAGE_HEADER_SIZE);
    final ByteBuffer expectedSlice = expected.slice();

    final ByteBuffer slice = page.slice();
    assertThat(slice.position()).isZero();
    assertThat(slice.limit()).isEqualTo(expectedSlice.limit());
    assertThat(slice.capacity()).isEqualTo(expectedSlice.capacity());
    assertThat(slice.order()).isEqualTo(ByteOrder.BIG_ENDIAN);
    assertThat(slice.hasArray()).isTrue();
    assertThat(slice.arrayOffset()).isEqualTo(expectedSlice.arrayOffset());
    assertThat(slice.array()).isSameAs(content.array());
    assertThat(slice.equals(expectedSlice)).as("same remaining bytes").isTrue();

    // its own position: moving it leaves the page's buffer and the next slice alone
    final int pageBufferPosition = content.position();
    slice.position(100);
    assertThat(content.position()).isEqualTo(pageBufferPosition);
    assertThat(page.slice().position()).isZero();
    assertThat(new Binary(page.slice()).getInt(0)).isEqualTo(page.readInt(0));
  }
}
