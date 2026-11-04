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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.index.geospatial.LSMTreeGeoIndex;
import com.arcadedb.index.lsm.LSMTreeIndexBulkLoader;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.GeoIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code REBUILD INDEX} of a GEOSPATIAL, FULL_TEXT or HASH type index dropped the index, saved the schema and built it again under
 * the database write lock: a query on any other type waited for the whole build, and a stop in between reopened without the index.
 * These kinds are now rebuilt in place, one bucket index at a time, as an LSM_TREE index is: the rebuild is paused on its first
 * record through the build seam {@link IndexBuildTestHooks}, which both paths reach, and the database is used meanwhile. The waits
 * are hang detectors.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RebuildIndexInPlaceKindsTest extends TestHelper {
  private static final String OTHER_TYPE  = "BatchRun";
  private static final int    BUCKETS     = 8;
  private static final long   HANG_DETECT = 60;
  private static final String BOX         = "POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))";

  private final ExecutorService executor = Executors.newCachedThreadPool();

  @AfterEach
  void stopExecutor() {
    IndexBuildTestHooks.onRecordIndexed = null;
    executor.shutdownNow();
  }

  @Test
  void aGeospatialIndexIsRebuiltInPlace() throws Exception {
    final String typeName = "Place";
    final String indexName = typeName + "[coords]";
    final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(BUCKETS).create();
    type.createProperty("coords", Type.STRING);
    type.createProperty("copy", Type.STRING);
    createOtherType();
    // written before the index exists: a build skips it with a warning, and so must the rebuild in place
    database.transaction(() -> database.newDocument(typeName).set("coords", "POINT (not a point)").save());
    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++) {
        final String wkt = i % 10 == 0 ?
            "POLYGON ((%1$d.1 %2$d.1, %1$d.6 %2$d.1, %1$d.6 %2$d.6, %1$d.1 %2$d.6, %1$d.1 %2$d.1))".formatted(5 + i % 15, 35 + i % 12) :
            "POINT (%s %s)".formatted(5 + (i % 150) / 10.0, 35 + (i % 120) / 10.0);
        database.newDocument(typeName).set("coords", wkt, "copy", wkt).save();
      }
    });
    database.command("sql", "CREATE INDEX ON " + typeName + " (coords) GEOSPATIAL");

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
    final IndexInternal[] bucketIndexesBefore = index.getIndexesOnBuckets();
    final Set<RID> inTheBoxBefore = ridsOf(index.get(new Object[] { BOX }));
    final long namesInTheBoxBefore = countByQuery(typeName);
    assertThat(inTheBoxBefore).isNotEmpty();

    runPausedRebuild(indexName, () -> {
      for (int i = 0; i < 200; i++) {
        // outside the box, so what the box matches stays the same
        final String wkt = "POINT (%s %s)".formatted(100 + i / 10.0, -20 - i / 10.0);
        database.transaction(() -> database.newDocument(typeName).set("coords", wkt, "copy", wkt).save(), false, 10);
      }
    });

    final TypeIndex after = (TypeIndex) database.getSchema().getIndexByName(indexName);
    assertThat(after).isSameAs(index);
    assertThat(after.getIndexesOnBuckets()).containsExactly(bucketIndexesBefore);
    assertThat(after.isValid()).isTrue();
    for (final IndexInternal onBucket : bucketIndexesBefore)
      assertThat(database.getSchema().getIndexByName(onBucket.getName())).isSameAs(onBucket);

    assertThat(ridsOf(after.get(new Object[] { BOX }))).isEqualTo(inTheBoxBefore);
    assertThat(countByQuery(typeName)).isEqualTo(namesInTheBoxBefore);
    // the points written during the rebuild are found through it
    assertThat(ridsOf(after.get(new Object[] { "POLYGON ((99 -45, 125 -45, 125 -15, 99 -15, 99 -45))" }))).hasSize(200);

    // the same cells as an index built from scratch on a copy of the property, bucket by bucket
    database.command("sql", "CREATE INDEX ON " + typeName + " (copy) GEOSPATIAL");
    assertSameEntriesPerBucket(type, after, (TypeIndex) database.getSchema().getIndexByName(typeName + "[copy]"));

    reopenDatabase();
    final TypeIndex reopened = (TypeIndex) database.getSchema().getIndexByName(indexName);
    assertThat(reopened.isValid()).isTrue();
    assertThat(ridsOf(reopened.get(new Object[] { BOX }))).hasSize(inTheBoxBefore.size());
  }

  @Test
  void aLegacyGeospatialIndexIsStillUpgradedByARebuild() {
    final String typeName = "LegacyPlace";
    final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(2).create();
    type.createProperty("coords", Type.STRING);
    final GeoIndexMetadata legacy = new GeoIndexMetadata(typeName, new String[] { "coords" }, -1);
    legacy.setTokenization(GeoIndexMetadata.TOKENIZATION.FULL);
    database.getSchema().buildTypeIndex(typeName, new String[] { "coords" }).withType(Schema.INDEX_TYPE.GEOSPATIAL)
        .withMetadata(legacy).create();
    database.transaction(() -> database.newDocument(typeName).set("coords", "POINT (12.5 41.9)").save());
    final TypeIndex before = (TypeIndex) database.getSchema().getIndexByName(typeName + "[coords]");
    assertThat(((LSMTreeGeoIndex) before.getIndexesOnBuckets()[0]).isUpgradedByRebuild()).isTrue();

    database.command("sql", "REBUILD INDEX `" + typeName + "[coords]`");

    final TypeIndex after = (TypeIndex) database.getSchema().getIndexByName(typeName + "[coords]");
    for (final IndexInternal onBucket : after.getIndexesOnBuckets())
      assertThat(((LSMTreeGeoIndex) onBucket).isUpgradedByRebuild()).as("rebuilt into the current layout").isFalse();
    assertThat(ridsOf(after.get(new Object[] { BOX }))).hasSize(1);
  }

  @Test
  void aFullTextIndexIsRebuiltInPlace() throws Exception {
    final String typeName = "Article";
    final String indexName = typeName + "[title,body]";
    final DocumentType type = createArticles(typeName, 2_000);
    database.command("sql", "CREATE INDEX ON " + typeName + " (title, body) FULL_TEXT");

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
    final IndexInternal[] bucketIndexesBefore = index.getIndexesOnBuckets();
    final FullTextIndexMetadata metadata = LSMTreeFullTextIndex.metadataOf(index);
    final long totalDocsBefore = metadata.getTotalDocs();
    final long sumDocLengthBefore = metadata.getSumDocLength();

    runPausedRebuild(indexName, () -> {
      for (int i = 0; i < 200; i++) {
        final int n = i;
        database.transaction(() -> database.newDocument(typeName)
            .set("name", "late-" + n, "title", "zebra quokka", "body", "written during the rebuild",
                "titleCopy", "zebra quokka", "bodyCopy", "written during the rebuild").save(), false, 10);
      }
    });

    final TypeIndex after = (TypeIndex) database.getSchema().getIndexByName(indexName);
    assertThat(after).isSameAs(index);
    assertThat(after.getIndexesOnBuckets()).containsExactly(bucketIndexesBefore);
    assertThat(after.isValid()).isTrue();
    // the corpus counters were trustworthy and are kept, with the documents written during the rebuild counted by their writes
    assertThat(metadata.getTotalDocs()).isEqualTo(totalDocsBefore + 200);
    assertThat(metadata.getSumDocLength()).isGreaterThan(sumDocLengthBefore);

    assertThat(searchScores(typeName, indexName, "zebra")).hasSize(200);
    assertThat(searchScores(typeName, indexName, "title:quokka")).hasSize(200);
    assertThat(searchScores(typeName, indexName, "body:quokka")).isEmpty();

    // the same postings as an index built from scratch on a copy of the properties, bucket by bucket
    database.command("sql", "CREATE INDEX ON " + typeName + " (titleCopy, bodyCopy) FULL_TEXT");
    final TypeIndex fresh = (TypeIndex) database.getSchema().getIndexByName(typeName + "[titleCopy,bodyCopy]");
    assertSameEntriesPerBucket(type, after, fresh);
    assertThat(metadata.getTotalDocs()).isEqualTo(LSMTreeFullTextIndex.metadataOf(fresh).getTotalDocs());
    assertThat(metadata.getSumDocLength()).isEqualTo(LSMTreeFullTextIndex.metadataOf(fresh).getSumDocLength());
  }

  /**
   * The postings of a BM25 index carry the term frequency and the length of their document: the scores after a rebuild in place are
   * the scores before it, also when the sort spills its entries to disk, and the corpus counters are not touched.
   */
  @Test
  void bm25ScoresAreTheSameAfterARebuildInPlaceThatSpills() {
    final String typeName = "Article";
    final String indexName = typeName + "[title,body]";
    createArticles(typeName, 1_500);
    database.command("sql", "CREATE INDEX ON " + typeName + " (title, body) FULL_TEXT");
    database.command("sql", "CREATE INDEX ON " + typeName + " (name) FULL_TEXT METADATA {\"similarity\": \"CLASSIC\"}");

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
    final FullTextIndexMetadata metadata = LSMTreeFullTextIndex.metadataOf(index);
    final long totalDocsBefore = metadata.getTotalDocs();
    final long sumDocLengthBefore = metadata.getSumDocLength();
    final String[] queries = { "alpha", "beta gamma", "title:delta", "body:alpha epsilon", "omega" };
    final Map<String, Map<String, Float>> scoresBefore = new HashMap<>();
    for (final String query : queries)
      scoresBefore.put(query, searchScores(typeName, indexName, query));
    final Map<String, Float> classicBefore = searchScores(typeName, typeName + "[name]", "doc");

    // small runs, so the entries go through the spill files and their merge
    LSMTreeIndexBulkLoader.setEntriesPerRunTestLimit(500);
    try {
      database.command("sql", "REBUILD INDEX `" + indexName + "`");
      database.command("sql", "REBUILD INDEX `" + typeName + "[name]`");
    } finally {
      LSMTreeIndexBulkLoader.setEntriesPerRunTestLimit(0);
    }

    assertThat(database.getSchema().getIndexByName(indexName)).isSameAs(index);
    assertThat(metadata.getTotalDocs()).isEqualTo(totalDocsBefore);
    assertThat(metadata.getSumDocLength()).isEqualTo(sumDocLengthBefore);
    for (final String query : queries)
      assertThat(searchScores(typeName, indexName, query)).as("scores of '%s'", query).isEqualTo(scoresBefore.get(query));
    assertThat(searchScores(typeName, typeName + "[name]", "doc")).isEqualTo(classicBefore);

    reopenDatabase();
    for (final String query : queries)
      assertThat(searchScores(typeName, indexName, query)).as("scores of '%s' after a reopen", query)
          .isEqualTo(scoresBefore.get(query));
  }

  /** Counters that are not trustworthy are recomputed once, at the end of the rebuild, as REBUILD INDEX ... statsOnly does. */
  @Test
  void untrustworthyBm25CountersAreRecomputedByTheRebuild() {
    final String typeName = "Article";
    final String indexName = typeName + "[title,body]";
    createArticles(typeName, 300);
    database.command("sql", "CREATE INDEX ON " + typeName + " (title, body) FULL_TEXT");
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
    final FullTextIndexMetadata metadata = LSMTreeFullTextIndex.metadataOf(index);
    final long totalDocs = metadata.getTotalDocs();
    final long sumDocLength = metadata.getSumDocLength();

    metadata.setCounters(7L, 9L);
    metadata.setCountersValid(false);
    database.command("sql", "REBUILD INDEX `" + indexName + "`");

    assertThat(database.getSchema().getIndexByName(indexName)).isSameAs(index);
    assertThat(metadata.isCountersValid()).isTrue();
    assertThat(metadata.getTotalDocs()).isEqualTo(totalDocs);
    assertThat(metadata.getSumDocLength()).isEqualTo(sumDocLength);
  }

  private DocumentType createArticles(final String typeName, final int count) {
    final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(BUCKETS).create();
    for (final String property : new String[] { "name", "title", "body", "titleCopy", "bodyCopy" })
      type.createProperty(property, Type.STRING);
    createOtherType();
    final String[] words = { "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa", "omega" };
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final StringBuilder body = new StringBuilder();
        for (int w = 0; w < 3 + i % 9; w++)
          body.append(words[(i * 7 + w * 3) % words.length]).append(' ');
        final String title = words[i % words.length] + " " + words[(i / 3) % words.length];
        database.newDocument(typeName)
            .set("name", "doc " + i, "title", title, "body", body.toString(), "titleCopy", title, "bodyCopy", body.toString()).save();
      }
    });
    return type;
  }

  private Map<String, Float> searchScores(final String typeName, final String indexName, final String query) {
    final Map<String, Float> scores = new HashMap<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT name, $score FROM " + typeName + " WHERE SEARCH_INDEX('" + indexName + "', '" + query + "') = true")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        scores.put(row.getProperty("name"), ((Number) row.getProperty("$score")).floatValue());
      }
    }
    return scores;
  }

  /**
   * Pauses the rebuild of {@code indexName} on its first record and, meanwhile, writes to and reads another type, then runs
   * {@code writes} (to the type being rebuilt) and releases the rebuild.
   */
  private void runPausedRebuild(final String indexName, final Runnable writes) throws Exception {
    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean pausedOnce = new AtomicBoolean();
    IndexBuildTestHooks.onRecordIndexed = (index, rid) -> {
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

    final Future<?> rebuild = executor.submit(() -> database.command("sql", "REBUILD INDEX `" + indexName + "`").close());
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

      // writes to every bucket of the type, the one being rebuilt included: that one waits for its bucket index
      writer = executor.submit(writes);
    } finally {
      release.countDown();
    }
    rebuild.get(HANG_DETECT, TimeUnit.SECONDS);
    writer.get(HANG_DETECT, TimeUnit.SECONDS);
    IndexBuildTestHooks.onRecordIndexed = null;
  }

  private void createOtherType() {
    database.getSchema().buildDocumentType().withName(OTHER_TYPE).withTotalBuckets(1).create();
    database.transaction(() -> database.newDocument(OTHER_TYPE).set("name", "first").save());
  }

  private long countByQuery(final String typeName) {
    try (final ResultSet rs = database.query("sql",
        "SELECT count(*) AS c FROM " + typeName + " WHERE geo.within(coords, geo.geomFromText('" + BOX + "')) = true")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private static void assertSameEntriesPerBucket(final DocumentType type, final TypeIndex rebuilt, final TypeIndex fresh) {
    final Map<Integer, Long> expected = new HashMap<>();
    for (final IndexInternal onBucket : fresh.getIndexesOnBuckets())
      expected.put(onBucket.getAssociatedBucketId(), onBucket.countEntries());
    for (final Bucket bucket : type.getBuckets(false)) {
      IndexInternal onBucket = null;
      for (final IndexInternal candidate : rebuilt.getIndexesOnBuckets())
        if (candidate.getAssociatedBucketId() == bucket.getFileId())
          onBucket = candidate;
      assertThat(onBucket).as("bucket index of %s", bucket.getName()).isNotNull();
      assertThat(onBucket.countEntries()).as("entries of %s", bucket.getName()).isEqualTo(expected.get(bucket.getFileId()));
    }
  }

  private static Set<RID> ridsOf(final IndexCursor cursor) {
    final Set<RID> rids = new HashSet<>();
    while (cursor.hasNext()) {
      final Identifiable next = cursor.next();
      if (next != null)
        rids.add(next.getIdentity());
    }
    return rids;
  }
}
