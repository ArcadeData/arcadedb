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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexBuildTestHooks;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code REBUILD INDEX} of an LSM_VECTOR index dropped the index and built it again under the database write lock: every query of
 * the database waited for the scan of the records, and every search of the index for the graph built afterwards. It is now rebuilt in
 * place, one bucket index at a time: the index is made to hold exactly what a build from the records would, and its file is then
 * compacted, which builds a fresh graph while the previous one keeps answering. The waits are hang detectors.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class RebuildVectorIndexInPlaceTest extends TestHelper {
  private static final String TYPE        = "Doc";
  private static final String OTHER_TYPE  = "BatchRun";
  private static final String INDEX       = TYPE + "[embedding]";
  private static final int    BUCKETS     = 4;
  private static final int    RECORDS     = 2_000;
  private static final int    DIMENSIONS  = 16;
  private static final long   HANG_DETECT = 120;

  private final ExecutorService executor = Executors.newCachedThreadPool();

  @AfterEach
  void cleanUp() {
    IndexBuildTestHooks.onRecordIndexed = null;
    executor.shutdownNow();
  }

  @Test
  void aVectorIndexIsRebuiltInPlaceWithoutBlockingTheDatabase() throws Exception {
    final Map<RID, float[]> vectors = createSchema(RECORDS);
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    final IndexInternal[] bucketIndexesBefore = index.getIndexesOnBuckets();
    final List<Integer> filesBefore = new ArrayList<>();
    for (final IndexInternal onBucket : bucketIndexesBefore)
      filesBefore.add(onBucket.getFileId());
    // a graph is resident before the rebuild
    assertFindsItself(vectors);

    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean pausedOnce = new AtomicBoolean();
    IndexBuildTestHooks.onRecordIndexed = (onBucket, rid) -> {
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
    final List<RID> written = new ArrayList<>();
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

      // the index itself keeps answering its searches
      final Map.Entry<RID, float[]> sample = vectors.entrySet().iterator().next();
      final Future<RID> search = executor.submit(() -> nearest(sample.getKey(), sample.getValue()));
      assertThat(search.get(HANG_DETECT, TimeUnit.SECONDS)).as("a search of the index is served").isEqualTo(sample.getKey());

      // writes to every bucket of the type, the one being rebuilt included: that one waits for its bucket index
      writer = executor.submit(() -> {
        final Random random = new Random(7);
        for (int i = 0; i < 100; i++) {
          final float[] vector = randomVector(random);
          database.transaction(() -> {
            final MutableDocument doc = database.newDocument(TYPE).set("name", "late", "embedding", vector);
            doc.save();
            synchronized (written) {
              written.add(doc.getIdentity());
              vectors.put(doc.getIdentity(), vector);
            }
          }, false, 10);
        }
      });
    } finally {
      release.countDown();
    }
    rebuild.get(HANG_DETECT, TimeUnit.SECONDS);
    writer.get(HANG_DETECT, TimeUnit.SECONDS);

    final TypeIndex after = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(after).isSameAs(index);
    assertThat(after.getIndexesOnBuckets()).containsExactly(bucketIndexesBefore);
    assertThat(after.isValid()).isTrue();
    assertThat(after.countEntries()).isEqualTo(RECORDS + 100);
    // every bucket index was compacted into a file of its own afterwards
    for (int i = 0; i < bucketIndexesBefore.length; i++)
      assertThat(bucketIndexesBefore[i].getFileId()).as("file of bucket index %d", i).isNotEqualTo(filesBefore.get(i));
    assertFindsItself(vectors);

    reopenDatabase();
    final TypeIndex reopened = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(reopened.isValid()).isTrue();
    assertThat(reopened.countEntries()).isEqualTo(RECORDS + 100);
    assertFindsItself(vectors);
  }

  /**
   * The index is out of sync with the records: some records are missing from it, and it holds a vector of a record that no longer
   * exists. The rebuild in place makes it hold exactly the records again.
   */
  @Test
  void aVectorIndexOutOfSyncWithItsRecordsIsRepaired() {
    final Map<RID, float[]> vectors = createSchema(RECORDS);
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);

    // entries lost: the records stay, their vectors are taken out of the index only
    final List<RID> lost = new ArrayList<>();
    for (final Map.Entry<RID, float[]> entry : vectors.entrySet()) {
      if (lost.size() == 50)
        break;
      lost.add(entry.getKey());
      final IndexInternal onBucket = bucketIndexOf(index, entry.getKey().getBucketId());
      database.transaction(() -> onBucket.remove(new Object[] { entry.getValue() }, entry.getKey()));
    }
    // an entry left behind by a record that is gone
    final float[] ghostVector = randomVector(new Random(11));
    final RID[] ghost = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument(TYPE).set("name", "ghost", "embedding", ghostVector);
      doc.save();
      ghost[0] = doc.getIdentity();
    });
    database.transaction(() -> database.deleteRecord(database.lookupByRID(ghost[0], true)));
    final IndexInternal ghostBucket = bucketIndexOf(index, ghost[0].getBucketId());
    database.transaction(() -> ghostBucket.put(new Object[] { ghostVector }, new RID[] { ghost[0] }));

    assertThat(index.countEntries()).isEqualTo(RECORDS - 50 + 1);

    database.command("sql", "REBUILD INDEX `" + INDEX + "`");

    final TypeIndex after = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(after).isSameAs(index);
    assertThat(after.countEntries()).isEqualTo(RECORDS);
    for (final RID rid : lost)
      assertThat(nearest(rid, vectors.get(rid))).as("lost record %s is found again", rid).isEqualTo(rid);
    assertThat(((LSMVectorIndex) ghostBucket).hasLiveVector(ghost[0])).as("the entry of the deleted record is gone").isFalse();

    reopenDatabase();
    assertThat(database.getSchema().getIndexByName(INDEX).countEntries()).isEqualTo(RECORDS);
  }

  /**
   * A record the index already holds as a write of it would leave it is not written again, so it stays in the resident graph:
   * an index in sync with its records changes nothing, whatever the quantization of its pages, and one missing some records
   * writes those only.
   */
  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8", "BINARY" })
  void onlyTheRecordsTheIndexDoesNotHoldRightAreWrittenAgain(final String quantization) {
    final Map<RID, float[]> vectors = createSchema(400, ", \"quantization\": \"" + quantization + "\"");
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);

    for (final IndexInternal onBucket : index.getIndexesOnBuckets())
      assertThat(((LSMVectorIndex) onBucket).reconcileWithBucket(100, null)).as("records written again in %s", onBucket.getName())
          .isZero();

    final Map.Entry<RID, float[]> lost = vectors.entrySet().iterator().next();
    final IndexInternal holder = bucketIndexOf(index, lost.getKey().getBucketId());
    database.transaction(() -> holder.remove(new Object[] { lost.getValue() }, lost.getKey()));
    assertThat(((LSMVectorIndex) holder).reconcileWithBucket(100, null)).isEqualTo(1);
    assertThat(((LSMVectorIndex) holder).hasLiveVector(lost.getKey())).isTrue();
    assertThat(((LSMVectorIndex) holder).reconcileWithBucket(100, null)).isZero();
    assertThat(index.countEntries()).isEqualTo(400);
  }

  private Map<RID, float[]> createSchema(final int records) {
    return createSchema(records, "");
  }

  private Map<RID, float[]> createSchema(final int records, final String extraMetadata) {
    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(BUCKETS).create();
    type.createProperty("name", Type.STRING);
    type.createProperty("embedding", Type.ARRAY_OF_FLOATS);
    database.getSchema().buildDocumentType().withName(OTHER_TYPE).withTotalBuckets(1).create();

    final Map<RID, float[]> vectors = new HashMap<>();
    final Random random = new Random(42);
    database.transaction(() -> {
      for (int i = 0; i < records; i++) {
        final float[] vector = randomVector(random);
        final MutableDocument doc = database.newDocument(TYPE).set("name", "doc-" + i, "embedding", vector);
        doc.save();
        vectors.put(doc.getIdentity(), vector);
      }
      database.newDocument(OTHER_TYPE).set("name", "first").save();
    });
    database.command("sql", "CREATE INDEX ON " + TYPE + " (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
        + ", \"similarity\": \"EUCLIDEAN\"" + extraMetadata + " }");
    return vectors;
  }

  private void assertFindsItself(final Map<RID, float[]> vectors) {
    int checked = 0;
    for (final Map.Entry<RID, float[]> entry : vectors.entrySet()) {
      if (checked++ % 20 != 0)
        continue;
      assertThat(nearest(entry.getKey(), entry.getValue())).as("record %s is found by its own vector", entry.getKey())
          .isEqualTo(entry.getKey());
    }
  }

  private RID nearest(final RID rid, final float[] vector) {
    final LSMVectorIndex onBucket = (LSMVectorIndex) bucketIndexOf((TypeIndex) database.getSchema().getIndexByName(INDEX),
        rid.getBucketId());
    final List<Pair<RID, Float>> found = onBucket.findNeighborsFromVector(vector, 1);
    return found.isEmpty() ? null : found.getFirst().getFirst();
  }

  private static IndexInternal bucketIndexOf(final TypeIndex index, final int bucketId) {
    for (final IndexInternal onBucket : index.getIndexesOnBuckets())
      if (onBucket.getAssociatedBucketId() == bucketId)
        return onBucket;
    throw new AssertionError("No bucket index for bucket " + bucketId);
  }

  private static float[] randomVector(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] = random.nextFloat() * 2 - 1;
    return vector;
  }
}
