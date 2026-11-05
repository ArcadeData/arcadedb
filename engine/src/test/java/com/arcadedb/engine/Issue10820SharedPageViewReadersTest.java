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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10820, the follow-up of #10654: the readers of a page that read it only at absolute positions take the page cache's
 * one shared view ({@link CachedPage#getSharedImmutableView()}) on a {@code READ_COMMITTED} miss, instead of building an
 * {@link ImmutablePage}, a {@link Binary} and a buffer per page per call:
 * <ul>
 * <li>a record read ({@code LocalBucket.getRecordInternal}), plain or spread over several pages, and a scan ({@link BucketIterator}),
 * which keeps its page across calls;</li>
 * <li>a hash index lookup, its RID lists and its metadata;</li>
 * <li>the LSM range cursors, which keep the page across calls too.</li>
 * </ul>
 * Each reader was checked to read only at absolute positions, which is what the shared view answers. The test reads every page
 * from a reopened database, so no page has a shared view before the read under test, and asserts both that the view was taken
 * and that the answers are unchanged.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10820SharedPageViewReadersTest extends TestHelper {
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
