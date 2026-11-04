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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexBuildTestHooks;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code REBUILD INDEX} of a HASH type index dropped the index, saved the schema and built it again under the database write lock: a
 * query on any other type waited for the whole build, a stop in between reopened without the index (and without the unique
 * constraint), and every bucket index came back as a new object under a new name. A hash index is now rebuilt in place: each bucket
 * index is filled into a new file in the current layout and swapped in, keeping its object and its name.
 * <p>
 * The pause is the build seam {@link IndexBuildTestHooks}, which both paths reach; the waits are hang detectors.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RebuildHashIndexInPlaceTest extends TestHelper {
  private static final String TYPE        = "Account";
  private static final String OTHER_TYPE  = "BatchRun";
  private static final String INDEX       = TYPE + "[uid]";
  private static final int    BUCKETS     = 8;
  private static final int    RECORDS     = 8_000;
  private static final long   HANG_DETECT = 60;

  private final ExecutorService executor = Executors.newCachedThreadPool();

  @AfterEach
  void cleanUp() {
    IndexBuildTestHooks.onRecordIndexed = null;
    HashIndex.HashIndexFactoryHandler.layoutVersion = HashIndexBucket.CURRENT_VERSION;
    executor.shutdownNow();
  }

  @Test
  void aHashIndexIsRebuiltInPlaceWithoutBlockingOtherTypes() throws Exception {
    createSchema(true, RECORDS);
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    final IndexInternal[] bucketIndexesBefore = index.getIndexesOnBuckets();
    final List<String> namesBefore = new ArrayList<>();
    final List<Integer> filesBefore = new ArrayList<>();
    for (final IndexInternal onBucket : bucketIndexesBefore) {
      namesBefore.add(onBucket.getName());
      filesBefore.add(onBucket.getFileId());
    }

    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean pausedOnce = new AtomicBoolean();
    IndexBuildTestHooks.onRecordIndexed = (indexBeingBuilt, rid) -> {
      if (pausedOnce.compareAndSet(false, true)) {
        paused.countDown();
        try {
          if (!release.await(HANG_DETECT, TimeUnit.SECONDS))
            throw new IllegalStateException("The test never released the rebuild");
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };

    final Future<?> rebuild = executor.submit(() -> database.command("sql", "REBUILD INDEX `" + INDEX + "`").close());
    final Future<?> writer;
    try {
      assertThat(paused.await(HANG_DETECT, TimeUnit.SECONDS)).as("the rebuild reached its first record").isTrue();

      final Future<Long> otherType = executor.submit(() -> {
        database.transaction(() -> database.newDocument(OTHER_TYPE).set("name", "second").save());
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + OTHER_TYPE)) {
          return rs.next().<Number>getProperty("c").longValue();
        }
      });
      assertThat(otherType.get(HANG_DETECT, TimeUnit.SECONDS)).as("a query on another type is served").isEqualTo(2L);

      // the index answers while it is rebuilt
      assertThat(executor.submit(() -> count(database.lookupByKey(TYPE, "uid", "key-77"))).get(HANG_DETECT, TimeUnit.SECONDS))
          .isEqualTo(1);

      // inserts, key changes and deletes spread over every bucket, the one being rebuilt included
      writer = executor.submit(() -> {
        for (int i = 0; i < 200; i++) {
          final int n = i;
          database.transaction(() -> database.newDocument(TYPE).set("uid", "new-" + n).save(), false, 10);
        }
        for (int i = 0; i < 100; i++) {
          final int n = i;
          database.transaction(() -> {
            final Document record = database.lookupByKey(TYPE, "uid", "key-" + n).next().asDocument();
            if (n % 2 == 0)
              record.modify().set("uid", "moved-" + n).save();
            else
              record.delete();
          }, false, 10);
        }
      });
    } finally {
      release.countDown();
    }
    rebuild.get(HANG_DETECT, TimeUnit.SECONDS);
    writer.get(HANG_DETECT, TimeUnit.SECONDS);
    IndexBuildTestHooks.onRecordIndexed = null;

    // the same index and the same bucket index objects, under the same names, on new files
    final TypeIndex after = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(after).isSameAs(index);
    assertThat(after.getIndexesOnBuckets()).containsExactly(bucketIndexesBefore);
    assertThat(after.isValid()).isTrue();
    for (int i = 0; i < bucketIndexesBefore.length; i++) {
      assertThat(bucketIndexesBefore[i].getName()).isEqualTo(namesBefore.get(i));
      assertThat(bucketIndexesBefore[i].getFileId()).isNotEqualTo(filesBefore.get(i));
      assertThat(database.getSchema().getIndexByName(namesBefore.get(i))).isSameAs(bucketIndexesBefore[i]);
    }

    assertIndexMatchesTheRecords();
    assertThat(count(database.lookupByKey(TYPE, "uid", "new-199"))).isEqualTo(1);
    assertThat(count(database.lookupByKey(TYPE, "uid", "moved-10"))).isEqualTo(1);
    assertThat(count(database.lookupByKey(TYPE, "uid", "key-10"))).isZero();
    assertThat(count(database.lookupByKey(TYPE, "uid", "key-11"))).isZero();
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE).set("uid", "key-500").save()))
        .isInstanceOf(DuplicatedKeyException.class);

    // and after a reopen the bucket indexes still answer to their names
    reopenDatabase();
    final TypeIndex reopened = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(reopened.isValid()).isTrue();
    final List<String> namesAfterReopen = new ArrayList<>();
    for (final IndexInternal onBucket : reopened.getIndexesOnBuckets())
      namesAfterReopen.add(onBucket.getName());
    assertThat(namesAfterReopen).containsExactlyInAnyOrderElementsOf(namesBefore);
    for (final String name : namesBefore)
      assertThat(database.getSchema().getIndexByName(name).getTypeName()).isEqualTo(TYPE);
    assertIndexMatchesTheRecords();
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE).set("uid", "key-501").save()))
        .isInstanceOf(DuplicatedKeyException.class);

    // a second rebuild in place of the same index, now named after a file it no longer has
    database.command("sql", "REBUILD INDEX `" + INDEX + "`").close();
    reopenDatabase();
    assertIndexMatchesTheRecords();
  }

  @Test
  void aLegacyHashIndexIsMovedToTheCurrentLayoutInPlace() {
    HashIndex.HashIndexFactoryHandler.layoutVersion = HashIndexBucket.LEGACY_SORTED_VERSION;
    createSchema(true, 3_000);
    HashIndex.HashIndexFactoryHandler.layoutVersion = HashIndexBucket.CURRENT_VERSION;
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    final IndexInternal[] bucketIndexesBefore = index.getIndexesOnBuckets();
    for (final IndexInternal onBucket : bucketIndexesBefore)
      assertThat(onBucket.getComponent().getVersion()).isEqualTo(HashIndexBucket.LEGACY_SORTED_VERSION);

    database.command("sql", "REBUILD INDEX `" + INDEX + "`").close();

    assertThat(database.getSchema().getIndexByName(INDEX)).isSameAs(index);
    assertThat(index.getIndexesOnBuckets()).containsExactly(bucketIndexesBefore);
    for (final IndexInternal onBucket : bucketIndexesBefore)
      assertThat(onBucket.getComponent().getVersion()).isEqualTo(HashIndexBucket.CURRENT_VERSION);
    assertIndexMatchesTheRecords();

    reopenDatabase();
    for (final IndexInternal onBucket : ((TypeIndex) database.getSchema().getIndexByName(INDEX)).getIndexesOnBuckets())
      assertThat(onBucket.getComponent().getVersion()).isEqualTo(HashIndexBucket.CURRENT_VERSION);
    assertIndexMatchesTheRecords();
  }

  @Test
  void aNotUniqueHashIndexIsRebuiltInPlace() {
    createSchema(false, 3_000);
    database.transaction(() -> {
      for (int i = 0; i < 500; i++)
        database.newDocument(TYPE).set("uid", "key-" + (i % 10)).save();
    });
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    final IndexInternal[] bucketIndexesBefore = index.getIndexesOnBuckets();

    database.command("sql", "REBUILD INDEX `" + INDEX + "`").close();

    assertThat(index.getIndexesOnBuckets()).containsExactly(bucketIndexesBefore);
    assertThat(count(database.lookupByKey(TYPE, "uid", "key-3"))).isEqualTo(51);
    assertIndexMatchesTheRecords();
  }

  @Test
  void aKeyTwoBucketsShareIsRefusedByTheRebuild() {
    createSchema(true, RECORDS);
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);

    // corrupt the index the way a lost entry does: the entry of key-3 is gone, so the duplicate commits
    final RID original = database.lookupByKey(TYPE, "uid", "key-3").next().getIdentity();
    final IndexInternal holder = bucketIndexOf(index, original.getBucketId());
    database.transaction(() -> holder.remove(new Object[] { "key-3" }, original));
    final RID[] duplicate = new RID[1];
    database.transaction(() -> {
      MutableDocument doc;
      do {
        doc = database.newDocument(TYPE).set("uid", "key-3");
        doc.save();
        if (doc.getIdentity().getBucketId() == original.getBucketId()) {
          doc.delete();
          doc = null;
        }
      } while (doc == null);
      duplicate[0] = doc.getIdentity();
    });

    assertThatThrownBy(() -> database.command("sql", "REBUILD INDEX `" + INDEX + "`")).hasMessageContaining("key-3");
    // rebuilt and complete, as the data is: both holders of the key are found
    assertThat(database.getSchema().getIndexByName(INDEX)).isSameAs(index);
    assertThat(count(database.lookupByKey(TYPE, "uid", "key-3"))).isGreaterThanOrEqualTo(1);

    // without the duplicate, the index is consistent again
    database.transaction(() -> database.deleteRecord(database.lookupByRID(duplicate[0], true)));
  }

  private void createSchema(final boolean unique, final int records) {
    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(BUCKETS).create();
    type.createProperty("uid", Type.STRING);
    database.getSchema().buildTypeIndex(TYPE, new String[] { "uid" }).withType(Schema.INDEX_TYPE.HASH).withUnique(unique).create();
    database.getSchema().buildDocumentType().withName(OTHER_TYPE).withTotalBuckets(1).create();

    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        database.newDocument(TYPE).set("uid", "key-" + i).save();
      database.newDocument(OTHER_TYPE).set("name", "first").save();
    });
  }

  /** Every bucket index holds exactly one entry per record of its bucket, and every record is found by its key. */
  private void assertIndexMatchesTheRecords() {
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    final DocumentType type = database.getSchema().getType(TYPE);
    assertThat(index.getIndexesOnBuckets()).hasSize(type.getBuckets(false).size());

    final Map<Integer, Long> recordsPerBucket = new HashMap<>();
    final List<RID> rids = new ArrayList<>();
    final List<String> keys = new ArrayList<>();
    database.iterateType(TYPE, false).forEachRemaining(record -> {
      recordsPerBucket.merge(record.getIdentity().getBucketId(), 1L, Long::sum);
      rids.add(record.getIdentity());
      keys.add(record.asDocument().getString("uid"));
    });

    for (final Bucket bucket : type.getBuckets(false))
      assertThat(bucketIndexOf(index, bucket.getFileId()).countEntries()).as("entries of bucket %s", bucket.getName())
          .isEqualTo(recordsPerBucket.getOrDefault(bucket.getFileId(), 0L));

    for (int i = 0; i < keys.size(); i++) {
      boolean found = false;
      final IndexCursor cursor = database.lookupByKey(TYPE, "uid", keys.get(i));
      while (cursor.hasNext())
        if (rids.get(i).equals(cursor.next().getIdentity()))
          found = true;
      assertThat(found).as("record %s found by key %s", rids.get(i), keys.get(i)).isTrue();
    }
  }

  private static IndexInternal bucketIndexOf(final TypeIndex index, final int bucketId) {
    for (final IndexInternal onBucket : index.getIndexesOnBuckets())
      if (onBucket.getAssociatedBucketId() == bucketId)
        return onBucket;
    throw new AssertionError("No bucket index for bucket " + bucketId);
  }

  private static int count(final IndexCursor cursor) {
    int count = 0;
    while (cursor.hasNext())
      if (cursor.next() != null)
        count++;
    return count;
  }
}
