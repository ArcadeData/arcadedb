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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexBuildTestHooks;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code CHECK DATABASE FIX} rebuilt the bucket index of a bucket holding a corrupted record by dropping it and creating it again:
 * the bucket index got a new object and a new name, was missing for the time of the build (for a UNIQUE index, so was the
 * constraint on that bucket), and the build held the database write lock, so a query on any other type waited for it. It now
 * rebuilds such a bucket index in place, as REBUILD INDEX does, and repairs a type index that lacks some bucket indexes.
 * <p>
 * The pause is the build seam {@link IndexBuildTestHooks}, reached at the same point by both rebuild paths; the waits are hang
 * detectors.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseFixInPlaceTest extends TestHelper {
  private static final String TYPE        = "Form";
  private static final String OTHER_TYPE  = "BatchRun";
  private static final String INDEX       = TYPE + "[uid]";
  private static final int    BUCKETS     = 4;
  private static final int    RECORDS     = 2_000;
  private static final long   HANG_DETECT = 60;

  private final ExecutorService executor = Executors.newCachedThreadPool();

  @AfterEach
  void stopExecutor() {
    IndexBuildTestHooks.onRecordIndexed = null;
    executor.shutdownNow();
  }

  @Test
  void fixRebuildsTheBucketIndexInPlaceWithoutBlockingOtherTypes() throws Exception {
    createSchema();
    final RID victim = database.lookupByKey(TYPE, "uid", "key-17").next().getIdentity();
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    final IndexInternal bucketIndex = bucketIndexOf(index, victim.getBucketId());
    final String bucketIndexName = bucketIndex.getName();
    corruptRecordTypeByte((DatabaseInternal) database, victim);

    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean pausedOnce = new AtomicBoolean();
    IndexBuildTestHooks.onRecordIndexed = (indexBeingBuilt, rid) -> {
      if (rid.getBucketId() == victim.getBucketId() && pausedOnce.compareAndSet(false, true)) {
        paused.countDown();
        try {
          if (!release.await(HANG_DETECT, TimeUnit.SECONDS))
            throw new IllegalStateException("The test never released the rebuild");
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };

    final Future<Result> fix = executor.submit(() -> {
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX")) {
        return rs.next();
      }
    });
    try {
      assertThat(paused.await(HANG_DETECT, TimeUnit.SECONDS)).as("FIX reached the rebuild of the bucket index").isTrue();

      // a read and a write on another type are served while the bucket index is rebuilt
      final Future<Long> otherType = executor.submit(() -> {
        database.transaction(() -> database.newDocument(OTHER_TYPE).set("name", "second").save());
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + OTHER_TYPE)) {
          return rs.next().<Number>getProperty("c").longValue();
        }
      });
      assertThat(otherType.get(HANG_DETECT, TimeUnit.SECONDS)).isEqualTo(2L);

      // and the type index still answers, for the bucket being rebuilt too
      final Future<Integer> lookup = executor.submit(() -> count(database.lookupByKey(TYPE, "uid", "key-18")));
      assertThat(lookup.get(HANG_DETECT, TimeUnit.SECONDS)).isEqualTo(1);
    } finally {
      release.countDown();
    }
    final Result row = fix.get(HANG_DETECT, TimeUnit.SECONDS);
    IndexBuildTestHooks.onRecordIndexed = null;

    assertThat(row.<Set<String>>getProperty("rebuiltIndexes")).contains(bucketIndexName);

    // the same bucket index, under the same name, in the same type index
    final TypeIndex after = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(after).isSameAs(index);
    assertThat(bucketIndexOf(after, victim.getBucketId())).isSameAs(bucketIndex);
    assertThat(database.getSchema().getIndexByName(bucketIndexName)).isSameAs(bucketIndex);
    assertThat(after.isValid()).isTrue();
    assertIndexMatchesTheRecords();

    // the corrupted record was removed, and its key is free again; any other key is still refused
    assertThat(count(database.lookupByKey(TYPE, "uid", "key-17"))).isZero();
    database.transaction(() -> database.newDocument(TYPE).set("uid", "key-17").save());
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE).set("uid", "key-5").save()))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void checkReportsAnIncompleteTypeIndexAndFixRepairsIt() {
    createSchema();
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    final IndexInternal[] onBuckets = index.getIndexesOnBuckets();
    final IndexInternal kept = onBuckets[0];
    // the state a rebuild of an earlier version stopped half way left behind
    database.getSchema().dropIndex(onBuckets[1].getName());
    database.getSchema().dropIndex(onBuckets[3].getName());
    assertThat(index.isValid()).isFalse();

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result row = rs.next();
      assertThat(row.<Set<String>>getProperty("incompleteIndexes")).containsExactly(INDEX);
      assertThat(row.<Set<String>>getProperty("warnings")).anyMatch(w -> w.contains(INDEX) && w.contains("no bucket index"));
    }
    assertThat(index.isValid()).as("a check without FIX changes nothing").isFalse();

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX")) {
      final Result row = rs.next();
      assertThat(row.<Set<String>>getProperty("rebuiltIndexes")).contains(INDEX);
    }

    final TypeIndex repaired = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(repaired).isSameAs(index);
    assertThat(repaired.isValid()).isTrue();
    assertThat(bucketIndexOf(repaired, kept.getAssociatedBucketId())).as("a bucket index that was there is left as it is")
        .isSameAs(kept);
    assertIndexMatchesTheRecords();
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE).set("uid", "key-1").save()))
        .isInstanceOf(DuplicatedKeyException.class);

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      assertThat(rs.next().<Set<String>>getProperty("incompleteIndexes")).isEmpty();
    }

    reopenDatabase();
    assertThat(((TypeIndex) database.getSchema().getIndexByName(INDEX)).isValid()).isTrue();
    assertIndexMatchesTheRecords();
  }

  private void createSchema() {
    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(BUCKETS).create();
    type.createProperty("uid", Type.STRING);
    database.getSchema().buildTypeIndex(TYPE, new String[] { "uid" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
        .create();
    database.getSchema().buildDocumentType().withName(OTHER_TYPE).withTotalBuckets(1).create();

    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set("uid", "key-" + i).save();
      database.newDocument(OTHER_TYPE).set("name", "first").save();
    });
  }

  /** Every bucket index holds exactly one entry per record of its bucket. */
  private void assertIndexMatchesTheRecords() {
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    final DocumentType type = database.getSchema().getType(TYPE);
    assertThat(index.getIndexesOnBuckets()).hasSize(type.getBuckets(false).size());

    final Map<Integer, Long> recordsPerBucket = new HashMap<>();
    database.iterateType(TYPE, false).forEachRemaining(r -> recordsPerBucket.merge(r.getIdentity().getBucketId(), 1L, Long::sum));
    for (final Bucket bucket : type.getBuckets(false))
      assertThat(bucketIndexOf(index, bucket.getFileId()).countEntries()).as("entries of bucket %s", bucket.getName())
          .isEqualTo(recordsPerBucket.getOrDefault(bucket.getFileId(), 0L));
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
