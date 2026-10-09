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
package com.arcadedb.index.hash;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #9228: an insert into a NOTUNIQUE_HASH key rewrote the whole entry of the key, and once the entry
 * outgrew a page every insert compacted the page and walked the overflow chain, so the cost of an insert grew with the
 * RIDs the key already had. From layout version 3 the RIDs of a key that outgrows a quarter of a page live in a RID list of
 * their own, and an insert writes its last page.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9228HashHotKeyRidListTest extends TestHelper {
  private static final String TYPE  = "E";
  private static final String INDEX = "E[k]";

  @AfterEach
  void restoreLayout() {
    HashIndex.HashIndexFactoryHandler.layoutVersion = HashIndexBucket.CURRENT_VERSION;
  }

  @Test
  void anInsertIntoAHotKeyWritesTheLastPageOfItsListOnly() throws IOException {
    createType(4_096);
    final List<RID> hot = load(20_000, 1, 2_000);

    final HashIndexBucket bucket = bucket();
    assertThat(bucket.getVersion()).isEqualTo(HashIndexBucket.CURRENT_VERSION);
    // 20,000 compressed RIDs take 3 to 4 bytes each: about 20 pages of 4 KB. Inline, as entries of one RID each spread
    // over the overflow chain, they took more than four times as many
    assertThat(bucket.getTotalPages()).as("pages of the index file").isLessThan(45);

    // Each insert writes the last page of the list and the entry count on the metadata page; the page of the entry only
    // when a new last page is chained
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<RID> added = new ArrayList<>();
    int maxPagesWritten = 0;
    int insertsChainingAPage = 0;
    for (int i = 0; i < 300; i++) {
      final RID rid = new RID(hot.getFirst().getBucketId(), 10_000_000L + i);
      database.begin();
      bucket.put(new Object[] { 1L }, rid);
      final int written = db.getTransaction().getModifiedPages();
      database.commit();
      added.add(rid);
      maxPagesWritten = Math.max(maxPagesWritten, written);
      if (written > 2)
        insertsChainingAPage++;
    }
    assertThat(maxPagesWritten).as("pages written by one insert").isLessThanOrEqualTo(4);
    assertThat(insertsChainingAPage).as("inserts that chained a new page").isLessThanOrEqualTo(2);

    final Set<RID> expected = new HashSet<>(hot);
    expected.addAll(added);
    assertThat(lookup(1L)).containsExactlyInAnyOrderElementsOf(expected);

    // the RIDs that have no record behind them go away through the same bucket API
    database.transaction(() -> {
      for (final RID rid : added)
        try {
          bucket.remove(new Object[] { 1L }, rid);
        } catch (final IOException e) {
          throw new DatabaseOperationException("Cannot remove " + rid, e);
        }
    });
    assertThat(lookup(1L)).containsExactlyInAnyOrderElementsOf(hot);
    assertThat(bucket.checkMetadataIntegrity()).isEmpty();
  }

  @Test
  void deletionsMergeThePagesOfAListUpToItsLastOne() throws IOException {
    createType(1_024);
    final HashIndexBucket bucket = bucket();
    // about 250 RIDs of 4 bytes per page: a list of 5 pages
    final List<RID> rids = new ArrayList<>();
    for (int i = 0; i < 1_200; i++)
      rids.add(new RID(1_000, 5_000 + i));
    putAll(bucket, 9L, rids);

    // removed in the order they were added: the first page keeps shrinking and takes in the next one, the last one included
    putOrRemoveAll(bucket, 9L, rids.subList(0, 1_000), false);
    assertThat(bucket.checkMetadataIntegrity()).isEmpty();
    assertThat(lookupInBucket(bucket, 9L)).containsExactlyInAnyOrderElementsOf(rids.subList(1_000, 1_200));

    // the last page the entry names is the right one: new RIDs land after the survivors
    final List<RID> more = new ArrayList<>();
    for (int i = 0; i < 600; i++)
      more.add(new RID(1_000, 20_000 + i));
    putAll(bucket, 9L, more);
    assertThat(bucket.checkMetadataIntegrity()).isEmpty();
    final Set<RID> expected = new HashSet<>(rids.subList(1_000, 1_200));
    expected.addAll(more);
    assertThat(lookupInBucket(bucket, 9L)).containsExactlyInAnyOrderElementsOf(expected);

    putOrRemoveAll(bucket, 9L, new ArrayList<>(expected), false);
    assertThat(lookupInBucket(bucket, 9L)).isEmpty();
    assertThat(bucket.checkMetadataIntegrity()).isEmpty();
  }

  @Test
  void anEntryOfFewRIDsThatCannotGrowOnAFullUnsplittablePageMovesToARidList() throws IOException {
    // 256-byte pages and keys whose hashes share the first bit: the bucket can never split, so it overflows
    createType(256);
    final HashIndexBucket bucket = bucket();
    final List<Long> keys = new ArrayList<>();
    for (long k = 0; keys.size() < 120; k++)
      if (bucket.hashKeys(new Object[] { k }) >>> 63 == 0)
        keys.add(k);

    database.transaction(() -> {
      try {
        for (final long key : keys)
          bucket.put(new Object[] { key }, new RID(1_000, key));
      } catch (final IOException e) {
        throw new DatabaseOperationException("Cannot load the keys", e);
      }
    });
    assertThat(bucket.getGlobalDepth()).as("the bucket never split").isZero();
    assertThat(countRidListPages()).isZero();

    // The first key sits on the full head page with one RID: its second RID cannot grow the entry there, the bucket cannot
    // split, and the value (one RID) is shorter than the pointer that replaces it, so the entry is written again elsewhere
    final long first = keys.getFirst();
    putAll(bucket, first, List.of(new RID(1_001, first)));
    assertThat(countRidListPages()).isEqualTo(1);
    assertThat(lookupInBucket(bucket, first)).containsExactlyInAnyOrder(new RID(1_000, first), new RID(1_001, first));
    for (final long key : keys.subList(1, keys.size()))
      assertThat(lookupInBucket(bucket, key)).containsExactly(new RID(1_000, key));
    assertThat(bucket.checkMetadataIntegrity()).isEmpty();

    database.transaction(() -> {
      try {
        bucket.remove(new Object[] { first }, new RID(1_001, first));
        for (final long key : keys)
          bucket.remove(new Object[] { key }, new RID(1_000, key));
      } catch (final IOException e) {
        throw new DatabaseOperationException("Cannot remove the keys", e);
      }
    });
    assertThat(subIndex().countEntries()).isZero();
  }

  @Test
  void lookupsDeletionsAndReinsertsStayConsistent() {
    // a small page: lists of many pages, merges and splits of the buckets around them
    createType(1_024);
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 30_000; i++)
        rids.add(newRecord(i % 3).getIdentity());
      for (int i = 0; i < 3_000; i++)
        rids.add(newRecord(1_000 + i).getIdentity());
    });
    verifyAgainstScan();

    final Random random = new Random(9228);
    Collections.shuffle(rids, random);
    deleteAll(rids.subList(0, rids.size() / 2));
    verifyAgainstScan();

    reopenDatabase();
    verifyAgainstScan();
    assertThat(bucket().checkMetadataIntegrity()).isEmpty();
  }

  @Test
  void pagesFreedByAKeyAreReusedByAnother() {
    createType(1_024);
    final List<RID> first = load(10_000, 7, 0);
    final int pagesAfterTheFirstKey = bucket().getTotalPages();

    // the key loses all its records, one by one: its pages go to the free list
    deleteAll(first);
    assertThat(lookup(7L)).isEmpty();

    // another key of the same size takes them back instead of growing the file
    load(10_000, 8, 0);
    assertThat(bucket().getTotalPages()).as("pages after the second key").isLessThanOrEqualTo(pagesAfterTheFirstKey + 1);
    verifyAgainstScan();
    assertThat(bucket().checkMetadataIntegrity()).isEmpty();
  }

  @Test
  void removingAKeyAtOnceFreesItsList() throws IOException {
    createType(1_024);
    load(5_000, 3, 0);
    final HashIndex index = subIndex();
    final long entriesBefore = index.countEntries();

    database.begin();
    bucket().remove(new Object[] { 3L });
    database.commit();
    assertThat(index.countEntries()).isEqualTo(entriesBefore - 5_000);
    assertThat(lookup(3L)).isEmpty();

    final int pagesAfterRemoval = bucket().getTotalPages();
    database.transaction(() -> database.command("sql", "DELETE FROM " + TYPE));
    load(5_000, 4, 0);
    assertThat(bucket().getTotalPages()).isLessThanOrEqualTo(pagesAfterRemoval + 1);
    verifyAgainstScan();
  }

  @Test
  void indexesOfTheInlineLayoutKeepWorkingAndMoveToRidListsOnRebuild() {
    HashIndex.HashIndexFactoryHandler.layoutVersion = HashIndexBucket.INLINE_RIDS_VERSION;
    createType(1_024);
    load(6_000, 5, 500);
    assertThat(bucket().getVersion()).isEqualTo(HashIndexBucket.INLINE_RIDS_VERSION);
    assertThat(countRidListPages()).isZero();
    verifyAgainstScan();

    HashIndex.HashIndexFactoryHandler.layoutVersion = HashIndexBucket.CURRENT_VERSION;
    database.command("sql", "REBUILD INDEX `" + INDEX + "`").close();
    assertThat(bucket().getVersion()).isEqualTo(HashIndexBucket.CURRENT_VERSION);
    assertThat(countRidListPages()).isGreaterThan(0);
    verifyAgainstScan();
  }

  @Test
  void aRidListReachingAPageOfAnotherKeyIsReportedAsCorruption() throws IOException {
    createType(1_024);
    load(3_000, 2, 0);
    final HashIndexBucket bucket = bucket();
    final int listPage = firstRidListPage();

    final long owner = writeOwner(listPage, 42L);
    try {
      assertThat(bucket.checkMetadataIntegrity()).anySatisfy(problem -> assertThat(problem).contains("not a RID list page of its key"));
      assertThatThrownBy(() -> lookup(2L)).isInstanceOf(IndexException.class).hasMessageContaining("RID list");
    } finally {
      writeOwner(listPage, owner);
    }
    assertThat(bucket.checkMetadataIntegrity()).isEmpty();
  }

  @Test
  void aLookupNeverReturnsTheRIDsOfAnotherKeyWhileListPagesChangeHands() throws Exception {
    createType(1_024);
    final HashIndexBucket bucket = bucket();
    final AtomicBoolean done = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    // Index entries only, no records: the RIDs of each key sit in a bucket id of their own, so a RID of the wrong key is
    // recognized without loading anything (a deleted record's position can be recycled for a record of the other key)
    final Thread reader = new Thread(() -> {
      try {
        while (!done.get())
          for (final RID rid : bucket.get(new Object[] { 0L }, -1))
            if (rid.getBucketId() != 1_000)
              throw new AssertionError("Lookup of key 0 returned " + rid + ", a RID of key " + (rid.getBucketId() - 1_000));
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    reader.start();
    try {
      // the two keys trade the same pages: each round frees the list of one key and builds the other one on its pages
      for (int round = 0; round < 6 && failure.get() == null; round++) {
        putOrRemove(bucket, 0, true);
        putOrRemove(bucket, 1, true);
        putOrRemove(bucket, 0, false);
        putOrRemove(bucket, 1, false);
      }
    } finally {
      done.set(true);
      reader.join();
    }
    assertThat(failure.get()).isNull();
    assertThat(bucket.get(new Object[] { 0L }, -1)).isEmpty();
    assertThat(bucket.get(new Object[] { 1L }, -1)).isEmpty();
    assertThat(bucket.checkMetadataIntegrity()).isEmpty();
  }

  private void putOrRemove(final HashIndexBucket bucket, final long key, final boolean put) {
    database.transaction(() -> {
      try {
        for (int i = 0; i < 2_000; i++) {
          final RID rid = new RID(1_000 + (int) key, i);
          if (put)
            bucket.put(new Object[] { key }, rid);
          else
            bucket.remove(new Object[] { key }, rid);
        }
      } catch (final IOException e) {
        throw new DatabaseOperationException("Cannot update key " + key, e);
      }
    });
  }

  // ─── HELPERS ───

  private void putAll(final HashIndexBucket bucket, final long key, final List<RID> rids) {
    putOrRemoveAll(bucket, key, rids, true);
  }

  private void putOrRemoveAll(final HashIndexBucket bucket, final long key, final List<RID> rids, final boolean put) {
    database.transaction(() -> {
      try {
        for (final RID rid : rids)
          if (put)
            bucket.put(new Object[] { key }, rid);
          else
            bucket.remove(new Object[] { key }, rid);
      } catch (final IOException e) {
        throw new DatabaseOperationException("Cannot update key " + key, e);
      }
    });
  }

  private static Set<RID> lookupInBucket(final HashIndexBucket bucket, final long key) throws IOException {
    final List<RID> rids = bucket.get(new Object[] { key }, -1);
    final Set<RID> result = new HashSet<>(rids);
    assertThat(result).as("RIDs of key " + key + " are distinct").hasSize(rids.size());
    return result;
  }

  private void createType(final int pageSize) {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(1).create().createProperty("k", Type.LONG);
      database.getSchema().buildTypeIndex(TYPE, new String[] { "k" }).withType(Schema.INDEX_TYPE.HASH).withUnique(false)
          .withPageSize(pageSize).create();
    });
  }

  private MutableDocument newRecord(final long key) {
    final MutableDocument doc = database.newDocument(TYPE).set("k", key);
    doc.save();
    return doc;
  }

  /** Inserts {@code records} records of key {@code key}, plus {@code others} of distinct other keys. */
  private List<RID> load(final int records, final long key, final int others) {
    final List<RID> rids = new ArrayList<>(records);
    database.transaction(() -> {
      for (int i = 0; i < records; i++) {
        rids.add(newRecord(key).getIdentity());
        if (i < others)
          newRecord(100_000 + i);
      }
    });
    return rids;
  }

  private void deleteAll(final List<RID> rids) {
    database.transaction(() -> {
      for (final RID rid : rids)
        rid.asDocument().delete();
    });
  }

  private Set<RID> lookup(final long key) {
    final Set<RID> result = new HashSet<>();
    final IndexCursor cursor = subIndex().get(new Object[] { key });
    while (cursor.hasNext())
      assertThat(result.add(cursor.next().getIdentity())).as("RIDs of key " + key + " are distinct").isTrue();
    return result;
  }

  /** Every key holds, through the index, exactly the RIDs a scan of the type finds for it. */
  private void verifyAgainstScan() {
    final Map<Long, Set<RID>> scanned = new HashMap<>();
    long total = 0;
    for (final Iterator<Record> it = database.iterateType(TYPE, false); it.hasNext(); total++) {
      final Document record = it.next().asDocument();
      scanned.computeIfAbsent(((Number) record.get("k")).longValue(), k -> new HashSet<>()).add(record.getIdentity());
    }
    for (final Map.Entry<Long, Set<RID>> entry : scanned.entrySet())
      assertThat(lookup(entry.getKey())).as("RIDs of key " + entry.getKey()).containsExactlyInAnyOrderElementsOf(entry.getValue());
    assertThat(subIndex().countEntries()).isEqualTo(total);
  }

  private HashIndex subIndex() {
    return (HashIndex) ((TypeIndex) database.getSchema().getIndexByName(INDEX)).getIndexesOnBuckets()[0];
  }

  private HashIndexBucket bucket() {
    return subIndex().bucket;
  }

  private int countRidListPages() {
    final HashIndexBucket bucket = bucket();
    final DatabaseInternal db = (DatabaseInternal) database;
    int count = 0;
    for (int p = 2; p < bucket.getTotalPages(); p++)
      if (HashIndexBucket.isRidListPage(readPage(db, bucket, p)))
        count++;
    return count;
  }

  private int firstRidListPage() {
    final HashIndexBucket bucket = bucket();
    final DatabaseInternal db = (DatabaseInternal) database;
    for (int p = 2; p < bucket.getTotalPages(); p++)
      if (HashIndexBucket.isRidListPage(readPage(db, bucket, p)))
        return p;
    throw new AssertionError("No RID list page");
  }

  private long writeOwner(final int pageNum, final long owner) {
    final HashIndexBucket bucket = bucket();
    final DatabaseInternal db = (DatabaseInternal) database;
    final long[] previous = new long[1];
    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction()
            .getPageToModify(new PageId(db, bucket.getFileId(), pageNum), bucket.getPageSize(), false);
        previous[0] = page.readLong(HashIndexBucket.RID_PAGE_OWNER);
        page.writeLong(HashIndexBucket.RID_PAGE_OWNER, owner);
      } catch (final IOException e) {
        throw new DatabaseOperationException("Cannot write page " + pageNum, e);
      }
    });
    return previous[0];
  }

  private static BasePage readPage(final DatabaseInternal db, final HashIndexBucket bucket, final int pageNum) {
    try {
      return db.getPageManager().getImmutablePage(new PageId(db, bucket.getFileId(), pageNum), bucket.getPageSize(), false, false);
    } catch (final IOException e) {
      throw new DatabaseOperationException("Cannot read page " + pageNum, e);
    }
  }
}
