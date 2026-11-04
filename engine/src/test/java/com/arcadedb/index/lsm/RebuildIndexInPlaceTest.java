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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code REBUILD INDEX} of an LSM_TREE type index runs in place, one bucket index at a time. It used to
 * hold the database write lock for the whole build, so a query on an UNRELATED type waited for it, and it dropped the index before
 * building the new one. Here the rebuild is paused halfway, after the sort of its first bucket, and the database is used meanwhile.
 * <p>
 * The pause is a test hook and the waits are hang detectors: on the drop-and-create rebuild the query on the other type never
 * completes while the rebuild is paused.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RebuildIndexInPlaceTest extends TestHelper {
  private static final String TYPE        = "Form";
  private static final String OTHER_TYPE  = "BatchRun";
  private static final String INDEX       = TYPE + "[uid]";
  private static final int    BUCKETS     = 8;
  private static final int    RECORDS     = 8_000;
  private static final long   HANG_DETECT = 60;

  private final ExecutorService executor = Executors.newCachedThreadPool();

  @AfterEach
  void stopExecutor() {
    executor.shutdownNow();
  }

  @Test
  void aQueryOnAnotherTypeIsServedWhileATypeIndexIsRebuilt() throws Exception {
    createSchema(RECORDS);

    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Future<?> rebuild = startPausedRebuild(paused, release);
    try {
      assertThat(paused.await(HANG_DETECT, TimeUnit.SECONDS)).as("the rebuild reached its first sorted bucket").isTrue();

      // a read and a write on another type
      final Future<Long> otherType = executor.submit(() -> {
        database.transaction(() -> database.newDocument(OTHER_TYPE).set("name", "second").save());
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + OTHER_TYPE)) {
          return rs.next().<Number>getProperty("c").longValue();
        }
      });
      assertThat(otherType.get(HANG_DETECT, TimeUnit.SECONDS)).isEqualTo(2L);

      // the type being rebuilt is still served by its index
      final Future<Integer> lookup = executor.submit(() -> count(database.lookupByKey(TYPE, "uid", "key-77")));
      assertThat(lookup.get(HANG_DETECT, TimeUnit.SECONDS)).isEqualTo(1);
    } finally {
      release.countDown();
    }
    rebuild.get(HANG_DETECT, TimeUnit.SECONDS);

    assertIndexMatchesTheRecords();
    assertThat(((TypeIndex) database.getSchema().getIndexByName(INDEX)).isValid()).isTrue();
  }

  @Test
  void writesToTheRebuiltTypeDuringTheRebuildEndUpIndexed() throws Exception {
    createSchema(RECORDS);

    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Future<?> rebuild = startPausedRebuild(paused, release);
    final Future<?> writer;
    try {
      assertThat(paused.await(HANG_DETECT, TimeUnit.SECONDS)).isTrue();
      // inserts, key changes and deletes spread over every bucket, the one being rebuilt included
      writer = executor.submit(() -> {
        for (int i = 0; i < 200; i++) {
          final int n = i;
          database.transaction(() -> database.newDocument(TYPE).set("uid", "new-" + n).save(), false, 10);
        }
        for (int i = 0; i < 100; i++) {
          final int n = i;
          database.transaction(() -> {
            final IndexCursor cursor = database.lookupByKey(TYPE, "uid", "key-" + n);
            final Document record = cursor.next().asDocument();
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

    assertIndexMatchesTheRecords();
    assertThat(count(database.lookupByKey(TYPE, "uid", "new-199"))).isEqualTo(1);
    assertThat(count(database.lookupByKey(TYPE, "uid", "moved-10"))).isEqualTo(1);
    assertThat(count(database.lookupByKey(TYPE, "uid", "key-10"))).isZero();
    assertThat(count(database.lookupByKey(TYPE, "uid", "key-11"))).isZero();
  }

  @Test
  void theRebuiltIndexIsTheSameIndexAndStillRefusesADuplicate() {
    createSchema(RECORDS);
    final TypeIndex before = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    final IndexInternal[] bucketIndexesBefore = before.getIndexesOnBuckets();

    final ResultSet result = database.command("sql", "REBUILD INDEX `" + INDEX + "`");
    assertThat(result.next().<Long>getProperty("totalIndexed")).isEqualTo(RECORDS);

    final TypeIndex after = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(after).isSameAs(before);
    assertThat(after.getIndexesOnBuckets()).containsExactly(bucketIndexesBefore);
    assertThat(after.isUnique()).isTrue();
    assertIndexMatchesTheRecords();

    assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE).set("uid", "key-5").save()))
        .isInstanceOf(DuplicatedKeyException.class);

    // and it survives a reopen
    reopenDatabase();
    assertIndexMatchesTheRecords();
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE).set("uid", "key-6").save()))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void aBucketWithNoRecordsGetsAnEmptyIndex() {
    createSchema(3);
    database.command("sql", "REBUILD INDEX `" + INDEX + "`");
    assertIndexMatchesTheRecords();
    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.newDocument(TYPE).set("uid", "later-" + i).save();
    });
    assertIndexMatchesTheRecords();
  }

  @Test
  void aKeyTwoBucketsShareIsRefusedByTheRebuild() {
    createSchema(RECORDS);
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
    assertThat(duplicate[0].getBucketId()).isNotEqualTo(original.getBucketId());

    assertThatThrownBy(() -> database.command("sql", "REBUILD INDEX `" + INDEX + "`"))
        .hasMessageContaining("was rebuilt")
        .hasMessageContaining("key-3");

    // rebuilt all the same, mirroring the data: both records are in the index, which stays valid
    final TypeIndex after = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(after).isSameAs(index);
    assertThat(after.isValid()).isTrue();
    assertThat(after.countEntries()).isEqualTo(RECORDS + 1L);
    assertThat(count(bucketIndexOf(after, original.getBucketId()).get(new Object[] { "key-3" }))).isEqualTo(1);
    assertThat(count(bucketIndexOf(after, duplicate[0].getBucketId()).get(new Object[] { "key-3" }))).isEqualTo(1);
  }

  /**
   * The bucket indexes of a SUB type's buckets belong to the type index its super type declares. One of them missing leaves the
   * super type's index short of that bucket's records: it is reported as not valid, and the REBUILD of it creates the bucket index
   * again, owned by the sub type and attached to the super type's index, as linking the sub type did.
   */
  @Test
  void aSubTypeBucketWithoutItsBucketIndexMakesTheSuperTypeIndexInvalidUntilRebuilt() {
    createSchema(RECORDS);
    database.command("sql", "CREATE DOCUMENT TYPE SubForm EXTENDS " + TYPE + " BUCKETS 2");
    database.transaction(() -> {
      for (int i = 0; i < 600; i++)
        database.newDocument("SubForm").set("uid", "sub-" + i).save();
    });
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(index.getIndexesOnBuckets()).hasSize(BUCKETS + 2);
    assertThat(index.isValid()).isTrue();

    final Bucket subBucket = database.getSchema().getType("SubForm").getBuckets(false).get(1);
    database.getSchema().dropIndex(bucketIndexOf(index, subBucket.getFileId()).getName());

    assertThat(index.isValid()).isFalse();
    assertThat(index.getBucketsWithoutIndex()).containsExactly(subBucket);
    reopenDatabase();
    assertThat(((TypeIndex) database.getSchema().getIndexByName(INDEX)).isValid()).isFalse();

    database.command("sql", "REBUILD INDEX `" + INDEX + "`");

    final TypeIndex repaired = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(repaired.isValid()).isTrue();
    final IndexInternal recreated = bucketIndexOf(repaired, subBucket.getFileId());
    assertThat(recreated.getTypeName()).isEqualTo("SubForm");
    assertThat(recreated.countEntries()).isEqualTo(database.getSchema().getBucketById(subBucket.getFileId()).count());
    for (int i = 0; i < 600; i += 7)
      assertThat(count(database.lookupByKey(TYPE, "uid", "sub-" + i))).isEqualTo(1);
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE).set("uid", "sub-7").save()))
        .isInstanceOf(DuplicatedKeyException.class);

    reopenDatabase();
    assertThat(((TypeIndex) database.getSchema().getIndexByName(INDEX)).isValid()).isTrue();
  }

  /**
   * Writes to the very bucket being rebuilt wait for its commit lock while it is scanned and swapped, and then end up indexed:
   * the inserted record is found by its key, the deleted one leaves no entry behind.
   */
  @Test
  void insertsAndDeletesOnTheBucketBeingRebuiltWaitAndEndUpIndexed() throws Exception {
    createSchema(RECORDS);
    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicInteger pausedOnBucket = new AtomicInteger(-1);
    final Future<?> rebuild = startRebuildPausedAtFirstAppend(paused, release, pausedOnBucket, "");
    final Future<?> writer;
    final String[] deletedKey = new String[1];
    try {
      assertThat(paused.await(HANG_DETECT, TimeUnit.SECONDS)).isTrue();
      final String bucketName = database.getSchema().getBucketById(pausedOnBucket.get()).getName();
      writer = executor.submit(() -> {
        database.transaction(() -> {
          database.newDocument(TYPE).set("uid", "same-bucket-new").save(bucketName);
          final Document victim = database.iterateBucket(bucketName).next().asDocument();
          deletedKey[0] = victim.getString("uid");
          victim.delete();
        }, false, 10);
        return null;
      });
      // a short wait expected to time out: the commit waits for the bucket being rebuilt
      assertThatThrownBy(() -> writer.get(500, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    } finally {
      release.countDown();
    }
    rebuild.get(HANG_DETECT, TimeUnit.SECONDS);
    writer.get(HANG_DETECT, TimeUnit.SECONDS);

    assertIndexMatchesTheRecords();
    assertThat(count(database.lookupByKey(TYPE, "uid", "same-bucket-new"))).isEqualTo(1);
    assertThat(count(database.lookupByKey(TYPE, "uid", deletedKey[0]))).isZero();
  }

  /** A second REBUILD of the same index while the first one runs waits for the bucket indexes the first one holds; both complete. */
  @Test
  void twoConcurrentRebuildsOfTheSameIndexLeaveItComplete() throws Exception {
    createSchema(RECORDS);
    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Future<?> first = startRebuildPausedAtFirstAppend(paused, release, new AtomicInteger(-1), "");
    final Future<?> second;
    try {
      assertThat(paused.await(HANG_DETECT, TimeUnit.SECONDS)).isTrue();
      second = executor.submit(() -> {
        database.command("sql", "REBUILD INDEX `" + INDEX + "` WITH maxAttempts = 100");
        return null;
      });
      assertThatThrownBy(() -> second.get(500, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    } finally {
      release.countDown();
    }
    first.get(HANG_DETECT, TimeUnit.SECONDS);
    second.get(HANG_DETECT, TimeUnit.SECONDS);

    assertThat(((TypeIndex) database.getSchema().getIndexByName(INDEX)).isValid()).isTrue();
    assertIndexMatchesTheRecords();
    reopenDatabase();
    assertIndexMatchesTheRecords();
  }

  private Future<?> startRebuildPausedAtFirstAppend(final CountDownLatch paused, final CountDownLatch release,
      final AtomicInteger pausedOnBucket, final String settings) {
    return executor.submit(() -> {
      LSMTreeIndexBulkLoader.setBuildTestHook((phase, bucketIndex, completedBuckets) -> {
        if (phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_BUCKET_APPEND && pausedOnBucket.compareAndSet(-1,
            bucketIndex.getAssociatedBucketId())) {
          paused.countDown();
          try {
            if (!release.await(HANG_DETECT, TimeUnit.SECONDS))
              throw new IllegalStateException("The test never released the rebuild");
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      });
      try {
        database.command("sql", "REBUILD INDEX `" + INDEX + "`" + settings);
      } finally {
        LSMTreeIndexBulkLoader.setBuildTestHook(null);
      }
      return null;
    });
  }

  @Test
  void aTypeIndexMissingABucketIndexIsNotValidUntilRebuilt() {
    createSchema(RECORDS);
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(index.isValid()).isTrue();

    // the state an interrupted rebuild of an earlier version left behind: some buckets without their bucket index
    final IndexInternal[] onBuckets = index.getIndexesOnBuckets();
    database.getSchema().dropIndex(onBuckets[1].getName());
    database.getSchema().dropIndex(onBuckets[4].getName());

    assertThat(index.isValid()).isFalse();
    assertThat(index.getBucketsWithoutIndex()).hasSize(2);
    assertThat(schemaIndexValid(INDEX)).isFalse();

    reopenDatabase();
    final TypeIndex reopened = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(reopened.isValid()).isFalse();
    assertThat(schemaIndexValid(INDEX)).isFalse();

    database.command("sql", "REBUILD INDEX `" + INDEX + "`");

    final TypeIndex repaired = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(repaired.isValid()).isTrue();
    assertThat(schemaIndexValid(INDEX)).isTrue();
    assertIndexMatchesTheRecords();
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE).set("uid", "key-1").save()))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  /**
   * The repair of a type index that lacks some bucket indexes (left by a rebuild of an earlier version stopped half way) creates the
   * missing ones EMPTY and fills them like the others, one bucket at a time: the fill of the first missing bucket is paused, and
   * meanwhile the database serves another type and the type index stays reported as not valid.
   */
  @Test
  void repairingAnIncompleteIndexDoesNotBlockOtherTypes() throws Exception {
    createSchema(RECORDS);
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    final IndexInternal[] onBuckets = index.getIndexesOnBuckets();
    final List<Integer> missingBuckets = List.of(onBuckets[1].getAssociatedBucketId(), onBuckets[4].getAssociatedBucketId());
    database.getSchema().dropIndex(onBuckets[1].getName());
    database.getSchema().dropIndex(onBuckets[4].getName());
    assertThat(index.isValid()).isFalse();

    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicInteger pausedOnBucket = new AtomicInteger(-1);
    final Future<?> rebuild = executor.submit(() -> {
      LSMTreeIndexBulkLoader.setBuildTestHook((phase, bucketIndex, completedBuckets) -> {
        if (phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_BUCKET_APPEND && pausedOnBucket.compareAndSet(-1,
            bucketIndex.getAssociatedBucketId())) {
          paused.countDown();
          try {
            if (!release.await(HANG_DETECT, TimeUnit.SECONDS))
              throw new IllegalStateException("The test never released the rebuild");
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      });
      try {
        database.command("sql", "REBUILD INDEX `" + INDEX + "`");
      } finally {
        LSMTreeIndexBulkLoader.setBuildTestHook(null);
      }
      return null;
    });
    try {
      assertThat(paused.await(HANG_DETECT, TimeUnit.SECONDS)).as("the rebuild reached the fill of a bucket").isTrue();
      // the records of a missing bucket are written by the in-place fill, not under the schema lock
      assertThat(missingBuckets).contains(pausedOnBucket.get());

      final Future<Long> otherType = executor.submit(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + OTHER_TYPE)) {
          return rs.next().<Number>getProperty("c").longValue();
        }
      });
      assertThat(otherType.get(HANG_DETECT, TimeUnit.SECONDS)).isEqualTo(1L);

      // not valid, so no planner uses it, until every missing bucket is filled
      assertThat(((TypeIndex) database.getSchema().getIndexByName(INDEX)).isValid()).isFalse();
      assertThat(executor.submit(() -> schemaIndexValid(INDEX)).get(HANG_DETECT, TimeUnit.SECONDS)).isFalse();
    } finally {
      release.countDown();
    }
    rebuild.get(HANG_DETECT, TimeUnit.SECONDS);

    assertThat(((TypeIndex) database.getSchema().getIndexByName(INDEX)).isValid()).isTrue();
    assertIndexMatchesTheRecords();
    reopenDatabase();
    assertThat(((TypeIndex) database.getSchema().getIndexByName(INDEX)).isValid()).isTrue();
    assertIndexMatchesTheRecords();
  }

  private Future<?> startPausedRebuild(final CountDownLatch paused, final CountDownLatch release) {
    return executor.submit(() -> {
      final AtomicInteger sorts = new AtomicInteger();
      LSMTreeIndexBulkLoader.setBuildTestHook((phase, index, completedBuckets) -> {
        if (phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_SORT && sorts.incrementAndGet() == 1) {
          paused.countDown();
          try {
            if (!release.await(HANG_DETECT, TimeUnit.SECONDS))
              throw new IllegalStateException("The test never released the rebuild");
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      });
      try {
        database.command("sql", "REBUILD INDEX `" + INDEX + "`");
      } finally {
        LSMTreeIndexBulkLoader.setBuildTestHook(null);
      }
      return null;
    });
  }

  private void createSchema(final int records) {
    // so the drop-and-create rebuild takes the sorted build as well, and pauses at the same hook
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1L);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(BUCKETS).create();
    type.createProperty("uid", Type.STRING);
    database.getSchema().buildTypeIndex(TYPE, new String[] { "uid" }).withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(true).create();
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
    final List<String> keys = new ArrayList<>();
    database.iterateType(TYPE, false).forEachRemaining(record -> {
      recordsPerBucket.merge(record.getIdentity().getBucketId(), 1L, Long::sum);
      keys.add(record.asDocument().getString("uid"));
    });

    for (final Bucket bucket : type.getBuckets(false))
      assertThat(bucketIndexOf(index, bucket.getFileId()).countEntries()).as("entries of bucket %s", bucket.getName())
          .isEqualTo(recordsPerBucket.getOrDefault(bucket.getFileId(), 0L));

    for (final String key : keys)
      assertThat(count(database.lookupByKey(TYPE, "uid", key))).as("records found by key %s", key).isEqualTo(1);
  }

  private static IndexInternal bucketIndexOf(final TypeIndex index, final int bucketId) {
    for (final IndexInternal onBucket : index.getIndexesOnBuckets())
      if (onBucket.getAssociatedBucketId() == bucketId)
        return onBucket;
    throw new AssertionError("No bucket index for bucket " + bucketId);
  }

  private boolean schemaIndexValid(final String indexName) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:indexes WHERE name = ?", indexName)) {
      final Result row = rs.next();
      return row.<Boolean>getProperty("valid");
    }
  }

  private static int count(final IndexCursor cursor) {
    int count = 0;
    while (cursor.hasNext())
      if (cursor.next() != null)
        count++;
    return count;
  }
}
