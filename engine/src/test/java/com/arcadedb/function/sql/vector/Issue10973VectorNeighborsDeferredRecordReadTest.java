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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageManager;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10973: {@code vectorNeighbors} read the record of every neighbour to make its row, before the outer query had said
 * which properties it wants, so {@code SELECT @rid FROM (SELECT expand(vectorNeighbors(...)))} filled the page cache with
 * every result record, and a quantized index, whose walk reads no document, saved nothing through SQL. A row now reads its
 * record on the first property asked of it, and a projection of {@code @rid}, {@code @type} and the distance reads none.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10973VectorNeighborsDeferredRecordReadTest extends TestHelper {
  private static final int    DIMENSIONS = 16;
  private static final String RID_ONLY   =
      "SELECT @rid AS r, distance FROM (SELECT expand(vectorNeighbors('Doc[vector]', ?, 10)))";

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // one test leaves index entries pointing at records it removed from the bucket, on purpose
    return false;
  }

  @Test
  void aRowReadsItsRecordOnTheFirstPropertyAskedOfIt() {
    createDocs(50, "", 0);

    final List<ScoredRecordRow> rows = neighbours(query(new Random(1)));
    assertThat(rows).hasSize(10);
    for (final ScoredRecordRow row : rows) {
      assertThat(isUnread(row)).as("a row made by the search reads no record").isTrue();
      assertThat(row.get("@rid")).isEqualTo(row.getRecord().getIdentity());
      assertThat(row.get("@type")).isEqualTo("Doc");
      assertThat(row.get("distance")).isInstanceOf(Float.class);
      assertThat(isUnread(row)).as("@rid, @type and the distance read no record").isTrue();
    }

    final ScoredRecordRow first = rows.getFirst();
    assertThat(first.get("pid")).isInstanceOf(Integer.class);
    assertThat(isUnread(first)).as("a property reads the record").isFalse();
  }

  /** The reporter's measure: the page cache after the queries, against a projection that does read every record. */
  @Test
  void aRidOnlyQueryOnAQuantizedIndexLeavesTheDataPagesOutOfTheCache() {
    // ~16 KB of payload each, so the 400 records spread over ~100 pages, which the INT8 walk never reads
    createDocs(400, ", \"quantization\": \"INT8\"", 16_000);
    indexOf().buildVectorGraphNow();
    reopenDatabase();

    final List<float[]> queries = new ArrayList<>();
    final Random random = new Random(7);
    for (int i = 0; i < 30; i++)
      queries.add(query(random));

    // The walks first, through the Java API: the index pages they read are cached by the time the SQL queries run
    for (final float[] q : queries)
      assertThat(indexOf().findNeighborsFromVector(q, 10)).hasSize(10);

    final long beforeRidOnly = PageManager.INSTANCE.getReadCacheRAM();
    for (final float[] q : queries)
      assertThat(rows(RID_ONLY, q)).hasSize(10).allSatisfy(r -> assertThat(r.<RID>getProperty("r")).isNotNull());
    final long ridOnlyGrowth = PageManager.INSTANCE.getReadCacheRAM() - beforeRidOnly;

    final long beforeReading = PageManager.INSTANCE.getReadCacheRAM();
    for (final float[] q : queries)
      assertThat(rows("SELECT pid FROM (SELECT expand(vectorNeighbors('Doc[vector]', ?, 10)))", q)).hasSize(10)
          .allSatisfy(r -> assertThat(r.<Integer>getProperty("pid")).isNotNull());
    final long readingGrowth = PageManager.INSTANCE.getReadCacheRAM() - beforeReading;

    // Self-check: a projection that reads the records does fill the cache, so the pages were not cached already
    assertThat(readingGrowth).as("pages the records of the answers occupy").isGreaterThan(10L * 64 * 1024);
    assertThat(ridOnlyGrowth).as("pages the @rid-only queries brought into the cache").isLessThan(readingGrowth / 10);
  }

  /** The agreed trade: a row whose record is gone behind the index's back reads as a record with no properties. */
  @Test
  void aRecordRemovedFromItsBucketBehindTheIndexAnswersNoProperty() {
    createDocs(30, "", 0);
    indexOf().buildVectorGraphNow();
    final float[] q = new float[DIMENSIONS];

    final RID nearest = rows(RID_ONLY, q).getFirst().getProperty("r");
    database.transaction(() -> ((LocalBucket) database.getSchema().getBucketById(nearest.getBucketId())).deleteRecord(nearest));

    // The index still answers it: only reading the record could tell, and nothing reads it
    assertThat(rows(RID_ONLY, q)).extracting(r -> r.<RID>getProperty("r")).contains(nearest);
    // A projection that reads it is answered, not failed
    final List<Result> withPid = rows("SELECT @rid AS r, pid FROM (SELECT expand(vectorNeighbors('Doc[vector]', ?, 10)))", q);
    assertThat(withPid).hasSize(10);
    assertThat(withPid).filteredOn(r -> nearest.equals(r.getProperty("r"))).singleElement()
        .satisfies(r -> assertThat(r.<Object>getProperty("pid")).isNull());
    assertThat(withPid).filteredOn(r -> !nearest.equals(r.getProperty("r"))).allSatisfy(r -> assertThat(r.<Object>getProperty("pid")).isNotNull());
  }

  /**
   * Issue #11202: the row of a record gone behind the index's back takes one of the {@code limit} places, so the answer holds one
   * real neighbour fewer. It is reported once per row however many properties are read, and {@code REBUILD INDEX}, the repair the
   * report names, gives the real neighbour its place back.
   */
  @Test
  void aRecordGoneBehindTheIndexIsReportedAndRebuildIndexRepairsTheAnswer() {
    createDocs(30, "", 0);
    indexOf().buildVectorGraphNow();
    final float[] q = new float[DIMENSIONS];
    final String withPid = "SELECT @rid AS r, pid, payload, vector FROM (SELECT expand(vectorNeighbors('Doc[vector]', ?, 10)))";

    final List<RID> before = new ArrayList<>();
    rows(withPid, q).forEach(r -> before.add(r.getProperty("r")));
    final RID nearest = before.getFirst();
    // the neighbour that has the 11th place while every record is there
    final List<RID> eleven = new ArrayList<>();
    rows("SELECT @rid AS r FROM (SELECT expand(vectorNeighbors('Doc[vector]', ?, 11)))", q).forEach(r -> eleven.add(r.getProperty("r")));
    final RID eleventh = eleven.getLast();
    assertThat(before).doesNotContain(eleventh);

    database.transaction(() -> ((LocalBucket) database.getSchema().getBucketById(nearest.getBucketId())).deleteRecord(nearest));

    final long reportedBefore = StaleVectorEntryReporter.getTotalReported();
    final List<Result> damaged = rows(withPid, q);
    assertThat(damaged).hasSize(10).extracting(r -> r.<RID>getProperty("r")).contains(nearest).doesNotContain(eleventh);
    // three properties read off the row, one report
    assertThat(StaleVectorEntryReporter.getTotalReported() - reportedBefore).isEqualTo(1L);

    database.command("sql", "REBUILD INDEX `Doc[vector]`");
    indexOf().buildVectorGraphNow();

    final List<Result> repaired = rows(withPid, q);
    assertThat(repaired).hasSize(10).allSatisfy(r -> assertThat(r.<Object>getProperty("pid")).isNotNull());
    assertThat(repaired).extracting(r -> r.<RID>getProperty("r")).doesNotContain(nearest).contains(eleventh)
        .containsAll(before.subList(1, 10));
  }

  /** A delete through the API takes the vector out of the index in its own commit: the deferred row never sees it. */
  @Test
  void aCommittedDeleteCostsNoAnswerRow() {
    createDocs(30, "", 0);
    indexOf().buildVectorGraphNow();
    final float[] q = new float[DIMENSIONS];
    final List<RID> before = new ArrayList<>();
    rows(RID_ONLY, q).forEach(r -> before.add(r.getProperty("r")));

    database.transaction(() -> before.getFirst().asDocument().delete());

    assertThat(indexOf().hasLiveVector(before.getFirst())).isFalse();
    assertThat(indexOf().hasLiveVector(before.get(1))).isTrue();
    final List<RID> after = new ArrayList<>();
    rows(RID_ONLY, q).forEach(r -> after.add(r.getProperty("r")));
    assertThat(after).hasSize(10).doesNotContain(before.getFirst()).containsAll(before.subList(1, 10));
  }

  /** A read listener sees, and can veto, every record the search hands out: its records are read as before. */
  @Test
  void aReadListenerKeepsTheRecordsRead() {
    createDocs(30, "", 0);
    final AfterRecordReadListener listener = record -> record;
    database.getSchema().getType("Doc").getEvents().registerListener(listener);
    try {
      assertThat(neighbours(new float[DIMENSIONS])).hasSize(10).allSatisfy(row -> assertThat(isUnread(row)).isFalse());
    } finally {
      database.getSchema().getType("Doc").getEvents().unregisterListener(listener);
    }
  }

  /** The calling transaction answers its own rows from what it queued: they are read as before. */
  @Test
  void anIndexTheTransactionWroteToKeepsTheRecordsRead() {
    createDocs(30, "", 0);
    database.transaction(() -> {
      database.newDocument("Doc").set("pid", 1_000, "payload", "", "vector", new float[DIMENSIONS]).save();
      final List<ScoredRecordRow> rows = neighbours(new float[DIMENSIONS]);
      assertThat(rows).hasSize(10).allSatisfy(row -> assertThat(isUnread(row)).isFalse());
      assertThat(rows).extracting(row -> row.get("pid")).contains(1_000);
    });
  }

  private void createDocs(final int count, final String extraMetadata, final int payloadSize) {
    database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Doc.pid INTEGER");
    database.command("sql", "CREATE PROPERTY Doc.payload STRING");
    database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
        + ", \"similarity\": \"EUCLIDEAN\"" + extraMetadata + " }");
    final String payload = "x".repeat(payloadSize);
    final Random random = new Random(42);
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newDocument("Doc").set("pid", i, "payload", payload, "vector", query(random)).save();
    });
  }

  private List<ScoredRecordRow> neighbours(final float[] q) {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final Object result = new SQLFunctionVectorNeighbors().execute(null, null, null, new Object[] { "Doc[vector]", q, 10 }, context);
    final List<ScoredRecordRow> rows = new ArrayList<>();
    for (final Object row : (List<?>) result)
      rows.add((ScoredRecordRow) row);
    return rows;
  }

  private List<Result> rows(final String sql, final float[] q) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, q)) {
      rs.forEachRemaining(rows::add);
    }
    return rows;
  }

  private LSMVectorIndex indexOf() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]")).getIndexesOnBuckets()[0];
  }

  private static boolean isUnread(final ScoredRecordRow row) {
    return ((BaseRecord) row.getRecord()).getBuffer() == null;
  }

  private static float[] query(final Random random) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = random.nextFloat();
    return v;
  }
}
