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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9858, the native Select API twin of {@code db.index.vector.queryNodes}: {@code select().nearestTo()} loaded every
 * candidate the vector index answered with and let a {@code RecordNotFoundException} escape when a commit that overlapped
 * the search had deleted one of them. A stale candidate must be skipped and the answer topped up to the {@code k} nearest
 * LIVE records; a post-filter {@code where()} keeps applying to those {@code k}, as it does when nothing is stale.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9858SelectVectorStaleCandidateTest extends TestHelper {
  private static final int     COUNT  = 60;
  private static final float[] ORIGIN = { 0f, 0f };

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // the tests leave index entries pointing at records they removed from the bucket, on purpose
    return false;
  }

  @Test
  void staleCandidatesAreSkippedAndToppedUp() {
    createType("Doc", 1);
    removeFromBucketOnly("pid < 3");

    assertThat(pids(database.select().fromType("Doc").nearestTo("vector", ORIGIN, 5).vertices())).containsExactly(3, 4, 5,
        6, 7);
  }

  @Test
  void aWholeWindowOfStaleCandidatesIsToppedUpOverSeveralBuckets() {
    createType("Doc", 4);
    removeFromBucketOnly("pid < 10");

    assertThat(pids(database.select().fromType("Doc").nearestTo("vector", ORIGIN, 10).vertices())).containsExactly(10, 11,
        12, 13, 14, 15, 16, 17, 18, 19);
  }

  @Test
  void anIndexWithFewerLiveRecordsThanKAnswersWhatItHas() {
    createType("Doc", 1);
    removeFromBucketOnly("pid >= 4");

    assertThat(pids(database.select().fromType("Doc").nearestTo("vector", ORIGIN, 10).vertices())).containsExactly(0, 1, 2,
        3);
  }

  @Test
  void thePostFilterStillAppliesToTheKNearestLiveRecords() {
    createType("Doc", 1);
    removeFromBucketOnly("pid < 4");

    // The 10 nearest live records are 4..13: the filter keeps their even half, and never reaches past them.
    assertThat(pids(database.select().fromType("Doc").nearestTo("vector", ORIGIN, 10).where().property("even").eq().value(true)
        .vectorVertices())).containsExactly(4, 6, 8, 10, 12);
  }

  private void createType(final String typeName, final int buckets) {
    database.command("sql", "CREATE VERTEX TYPE " + typeName + " BUCKETS " + buckets);
    database.command("sql", "CREATE PROPERTY " + typeName + ".pid INTEGER");
    database.command("sql", "CREATE PROPERTY " + typeName + ".vector ARRAY_OF_FLOATS");
    database.command("sql",
        "CREATE INDEX ON " + typeName + " (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");
    database.transaction(() -> {
      for (int i = 0; i < COUNT; i++)
        database.newVertex(typeName).set("pid", i, "even", i % 2 == 0, "vector", new float[] { i, 0f }).save();
    });
  }

  /** Deletes the matching records from their bucket only, leaving their vector index entries behind. */
  private void removeFromBucketOnly(final String where) {
    final List<RID> stale = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Doc WHERE " + where)) {
      rs.forEachRemaining(r -> stale.add(r.getProperty("rid")));
    }
    assertThat(stale).isNotEmpty();

    // Build every graph first: a lazy build validates each record and would drop the ones removed below.
    final IndexInternal[] bucketIndexes = ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]")).getIndexesOnBuckets();
    for (final IndexInternal bucketIndex : bucketIndexes)
      ((LSMVectorIndex) bucketIndex).findNeighborsFromVector(ORIGIN, 10);

    database.transaction(() -> {
      for (final RID rid : stale)
        ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).deleteRecord(rid);
    });

    // Precondition: the indexes still answer with the removed records, otherwise these tests prove nothing.
    final List<RID> fromIndexes = new ArrayList<>();
    for (final IndexInternal bucketIndex : bucketIndexes)
      ((LSMVectorIndex) bucketIndex).findNeighborsFromVector(ORIGIN, COUNT).forEach(p -> fromIndexes.add(p.getFirst()));
    assertThat(fromIndexes).containsAll(stale);
  }

  private static List<Integer> pids(final List<SelectVectorResult<Vertex>> results) {
    final List<Integer> pids = new ArrayList<>(results.size());
    for (final SelectVectorResult<Vertex> result : results) {
      final Document document = result.getDocument();
      pids.add(document.getInteger("pid"));
    }
    return pids;
  }
}
