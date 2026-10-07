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

/**
 * Issue #9368: vectorNeighbors asked the index for exactly {@code limit} candidates and skipped the ones whose record no longer
 * exists, so each stale index entry (a record a concurrent commit has just deleted) cost the caller a row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9368VectorNeighborsStaleTopUpTest extends TestHelper {
  private static final int COUNT = 60;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.pid INTEGER");
    database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");
    database.transaction(() -> {
      for (int i = 0; i < COUNT; i++)
        database.newDocument("Doc").set("pid", i, "vector", new float[] { i, 0f }).save();
    });
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // the test leaves index entries pointing at records it removed from the bucket, on purpose
    return false;
  }

  @Test
  void staleIndexEntriesAreToppedUp() {
    // Remove the 4 nearest records from the bucket only, leaving their index entries behind, as a concurrent commit does.
    final List<RID> stale = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Doc WHERE pid < 4")) {
      rs.forEachRemaining(r -> stale.add(r.getProperty("rid")));
    }
    assertThat(stale).hasSize(4);

    // Build the graph first: a lazy build validates every record and would drop the ones removed below.
    final LSMVectorIndex lsm = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]")).getIndexesOnBuckets()[0];
    assertThat(lsm.findNeighborsFromVector(new float[] { 0f, 0f }, 10)).hasSize(10);

    database.transaction(() -> {
      for (final RID rid : stale)
        ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).deleteRecord(rid);
    });

    // Precondition: the index still answers with the removed records, otherwise this test proves nothing.
    final List<RID> fromIndex = new ArrayList<>();
    lsm.findNeighborsFromVector(new float[] { 0f, 0f }, 10).forEach(p -> fromIndex.add(p.getFirst()));
    assertThat(fromIndex).containsAll(stale);

    final List<Integer> pids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT pid FROM (SELECT expand(vectorNeighbors('Doc[vector]', :v, 10)))",
        Map.of("v", new float[] { 0f, 0f }))) {
      rs.forEachRemaining(r -> pids.add(r.getProperty("pid")));
    }
    assertThat(pids).hasSize(10).containsExactly(4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
  }
}
