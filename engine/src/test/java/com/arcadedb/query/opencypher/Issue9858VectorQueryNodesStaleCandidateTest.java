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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9858: {@code db.index.vector.queryNodes} loaded every candidate the vector index answered with and let a
 * {@code RecordNotFoundException} escape, so a record deleted by a commit that overlapped the search failed the whole
 * query ({@code Neo.ClientError.Statement.EntityNotFound} over Bolt). A stale candidate must be skipped, and the answer
 * topped up to {@code k} live rows, exactly as {@code vectorNeighbors} does (#9368).
 * <p>
 * The overlapping delete is reproduced deterministically by removing records from their bucket only, which leaves their
 * index entries behind - the state a concurrent commit leaves between the index search and the record load.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9858VectorQueryNodesStaleCandidateTest extends TestHelper {
  private static final int     COUNT  = 60;
  private static final float[] ORIGIN = { 0f, 0f };

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // the tests leave index entries pointing at records they removed from the bucket, on purpose
    return false;
  }

  @Test
  void theNearestCandidatesBeingStaleCostsNoRow() {
    createType(1);
    removeFromBucketOnly("pid < 4");

    assertThat(queryNodes(10)).containsExactly(4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
  }

  @Test
  void aWholeWindowOfStaleCandidatesIsToppedUp() {
    createType(1);
    removeFromBucketOnly("pid < 10");

    assertThat(queryNodes(10)).containsExactly(10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
  }

  @Test
  void anIndexWithFewerLiveRecordsThanKAnswersWhatItHas() {
    createType(1);
    removeFromBucketOnly("pid >= 5");

    assertThat(queryNodes(10)).containsExactly(0, 1, 2, 3, 4);
  }

  @Test
  void staleCandidatesSpreadOverSeveralBucketIndexesAreToppedUp() {
    createType(4);
    removeFromBucketOnly("pid < 6");

    assertThat(queryNodes(10)).containsExactly(6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
  }

  @Test
  void theScoreOfEveryRowIsTheOneOfItsOwnRecord() {
    createType(1);
    removeFromBucketOnly("pid < 3");

    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.vector.queryNodes('Doc[vector]', 3, $v) YIELD node, score RETURN node.pid AS pid, score", Map.of("v", ORIGIN))) {
      final List<Integer> pids = new ArrayList<>();
      while (rs.hasNext()) {
        final var row = rs.next();
        final int pid = row.getProperty("pid");
        pids.add(pid);
        // EUCLIDEAN: the distance is the squared L2, mapped to 1 / (1 + d)
        assertThat(((Number) row.getProperty("score")).floatValue()).isEqualTo(1.0f / (1.0f + pid * pid));
      }
      assertThat(pids).containsExactly(3, 4, 5);
    }
  }

  private void createType(final int buckets) {
    database.command("sql", "CREATE VERTEX TYPE Doc BUCKETS " + buckets);
    database.command("sql", "CREATE PROPERTY Doc.pid INTEGER");
    database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");
    database.transaction(() -> {
      for (int i = 0; i < COUNT; i++)
        database.newVertex("Doc").set("pid", i, "vector", new float[] { i, 0f }).save();
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

  private List<Integer> queryNodes(final int k) {
    final List<Integer> pids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.vector.queryNodes('Doc[vector]', " + k + ", $v) YIELD node, score RETURN node.pid AS pid", Map.of("v", ORIGIN))) {
      rs.forEachRemaining(r -> pids.add(r.getProperty("pid")));
    }
    return pids;
  }
}
