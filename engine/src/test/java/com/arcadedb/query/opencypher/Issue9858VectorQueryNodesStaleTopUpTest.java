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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Issue #9858: queryNodes skips deleted candidates. */
class Issue9858VectorQueryNodesStaleTopUpTest extends TestHelper {
  private static final int     COUNT = 60;
  private static final float[] ORIGIN = { 0f, 0f };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.pid INTEGER");
    database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");
    database.transaction(() -> {
      for (int i = 0; i < COUNT; i++)
        database.newVertex("Doc").set("pid", i, "vector", new float[] { i, 0f }).save();
    });
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // Stale index entries are intentional.
    return false;
  }

  @Test
  void staleCandidatesAreSkippedAndToppedUp() {
    removeFromBucketOnly("pid < 4");

    assertThat(queryNodes(10)).containsExactly(4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
  }

  @Test
  void aWholeWindowOfStaleCandidatesIsToppedUp() {
    // The whole first window is stale.
    removeFromBucketOnly("pid < 10");

    assertThat(queryNodes(10)).containsExactly(10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
  }

  @Test
  void anIndexHoldingFewerThanKLiveRecordsAnswersWhatItHas() {
    // The loop must stop, not spin.
    removeFromBucketOnly("pid >= 5");

    assertThat(queryNodes(10)).containsExactly(0, 1, 2, 3, 4);
  }

  /** Deletes records but keeps their index entries. */
  private List<RID> removeFromBucketOnly(final String where) {
    final List<RID> stale = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Doc WHERE " + where)) {
      rs.forEachRemaining(r -> stale.add(r.getProperty("rid")));
    }
    assertThat(stale).isNotEmpty();

    // Build the graph before deleting.
    final LSMVectorIndex lsm = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]")).getIndexesOnBuckets()[0];
    assertThat(lsm.findNeighborsFromVector(ORIGIN, 10)).hasSize(10);

    database.transaction(() -> {
      for (final RID rid : stale)
        ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).deleteRecord(rid);
    });

    // Precondition: index still returns removed records.
    final List<RID> fromIndex = new ArrayList<>();
    lsm.findNeighborsFromVector(ORIGIN, 10).forEach(p -> fromIndex.add(p.getFirst()));
    assertThat(fromIndex).containsAnyElementsOf(stale);
    return stale;
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
