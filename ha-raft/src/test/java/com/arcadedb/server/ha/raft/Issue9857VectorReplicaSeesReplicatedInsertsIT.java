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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Issue #9857: followers find later replicated vectors. */
@Tag("slow")
class Issue9857VectorReplicaSeesReplicatedInsertsIT extends BaseRaftHATest {
  private static final int DIMENSIONS = 16;
  private static final int FIRST      = 200;
  private static final int SECOND     = 100;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void followerFindsVectorsReplicatedAfterItsFirstSearch() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final DatabaseInternal leaderDb = (DatabaseInternal) getServerDatabase(leaderIndex, getDatabaseName());

    leaderDb.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
    leaderDb.command("sql", "CREATE PROPERTY Doc.id INTEGER");
    leaderDb.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    leaderDb.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
        + ", \"similarity\": \"COSINE\" }");

    insert(leaderDb, 0, FIRST);
    waitForAllServers();
    // First search builds each node's graph.
    assertEveryServerFinds(0, FIRST, leaderDb);

    insert(leaderDb, FIRST, SECOND);
    waitForAllServers();
    assertEveryServerFinds(FIRST, SECOND, leaderDb);

    assertClusterConsistency();
  }

  private void insert(final DatabaseInternal db, final int from, final int count) {
    db.transaction(() -> {
      for (int i = from; i < from + count; i++)
        db.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", i, embedding(i));
    });
  }

  private void assertEveryServerFinds(final int from, final int count, final DatabaseInternal leaderDb) {
    final RID[] expected = new RID[count];
    for (int k = 0; k < count; k++)
      expected[k] = ridOf(leaderDb, from + k);
    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      for (int k = 0; k < count; k++) {
        final int record = from + k;
        final RID found = withResyncRetry(server,
            db -> vectorIndex((DatabaseInternal) db).findNeighborsFromVector(embedding(record), 1, 64).getFirst()
                .getFirst());
        assertThat(found).as("server %d must find record %d by its own embedding", server, record)
            .isEqualTo(expected[k]);
      }
    }
  }

  private static RID ridOf(final DatabaseInternal db, final int id) {
    try (final ResultSet rs = db.query("sql", "SELECT @rid FROM Doc WHERE id = ?", id)) {
      return rs.next().getProperty("@rid");
    }
  }

  private static LSMVectorIndex vectorIndex(final DatabaseInternal db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("embedding")
        .getIndexesOnBuckets()[0];
  }

  private static float[] embedding(final int seed) {
    final Random random = new Random(0x9857L * 31 + seed);
    final float[] v = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] = (float) random.nextGaussian();
    return v;
  }
}
