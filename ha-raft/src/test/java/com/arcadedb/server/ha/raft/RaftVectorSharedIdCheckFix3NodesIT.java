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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code CHECK DATABASE FIX} on a replicated vector index whose pages carry two records on one vector id.
 * <p>
 * Such an index can never keep a persisted graph: the graph build counts both records, every load keeps one, and the
 * graph is rejected and rebuilt on each search, on every node that loads it. Nothing short of rebuilding the index
 * gives the losing records an id of their own, and a rebuild on a follower would write index pages outside the Raft
 * log. The repair therefore has to be computed on the leader and reach the followers as replicated WAL, which is the
 * path {@code CHECK DATABASE FIX} already takes for a corrupted index (see {@link RaftCheckDatabaseFix3NodesIT}).
 * <p>
 * The damage is written through the Raft wrapper on the leader, so all three nodes hold the same pages before the
 * repair starts - the shape an operator meets. The FIX is issued on a FOLLOWER, so the statement is forwarded.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class RaftVectorSharedIdCheckFix3NodesIT extends BaseRaftHATest {
  private static final int DIMENSIONS = 16;
  private static final int LIVE       = 200;
  private static final int COLLISIONS = 7;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void fixIssuedOnAFollowerGivesEveryRecordItsOwnVectorIdOnEveryNode() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final DatabaseInternal leaderDb = wrapped(leaderIndex);

    leaderDb.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
    leaderDb.command("sql", "CREATE PROPERTY Doc.id INTEGER");
    leaderDb.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    leaderDb.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
        + ", \"similarity\": \"COSINE\", \"quantization\": \"INT8\" }");
    leaderDb.transaction(() -> {
      for (int i = 0; i < LIVE; i++)
        leaderDb.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", i, embedding(i));
      for (int k = 0; k < COLLISIONS; k++)
        leaderDb.command("sql", "INSERT INTO Doc SET id = ?", 1000 + k);
    });
    waitForAllServers();

    // Record 1000+k is written onto vector id k, which belongs to one of the first records. The entries go in
    // through the wrapper, so they replicate as ordinary page writes.
    final RID[] owners = new RID[LIVE];
    for (int k = 0; k < LIVE; k++)
      owners[k] = ridOf(leaderDb, k);
    final RID[] winners = new RID[COLLISIONS];
    for (int k = 0; k < COLLISIONS; k++)
      winners[k] = ridOf(leaderDb, 1000 + k);
    leaderDb.transaction(() -> {
      final LSMVectorIndex index = vectorIndex(leaderDb);
      for (int k = 0; k < COLLISIONS; k++)
        index.persistEntryForTest(k, winners[k], embedding(1000 + k));
    });
    waitForAllServers();

    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      final List<String> before = withResyncRetry(server, db -> vectorIndex((DatabaseInternal) db).checkIntegrity());
      assertThat(before)
          .as("the damage must have replicated to server %d, otherwise the repair below proves nothing", server)
          .hasSize(1);
    }

    final Result plain = runCheck(followerIndex, "CHECK DATABASE");
    assertThat(plain.<Collection<String>>getProperty("corruptedIndexes"))
        .as("a plain check, forwarded to the leader, names the index and repairs nothing")
        .anyMatch(name -> name.startsWith("Doc_"));

    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      final List<String> afterPlainCheck = withResyncRetry(server,
          db -> vectorIndex((DatabaseInternal) db).checkIntegrity());
      assertThat(afterPlainCheck).as("the plain check repairs nothing: server %d still has the damage", server).hasSize(1);
    }

    runCheck(followerIndex, "CHECK DATABASE FIX");
    waitForAllServers();

    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      final List<String> after = withResyncRetry(server, db -> vectorIndex((DatabaseInternal) db).checkIntegrity());
      assertThat(after)
          .as("server %d must have no two records on one vector id: the repair replicates, it is not re-derived per node",
              server)
          .isEmpty();
      // Every record, not only the first few: vector ids are not guaranteed to follow insertion order, so which
      // records lost theirs is not something the fixture can name in advance.
      for (int k = 0; k < LIVE; k++) {
        final int record = k;
        final RID found = withResyncRetry(server,
            db -> vectorIndex((DatabaseInternal) db).findNeighborsFromVector(embedding(record), 1, 64).get(0).getFirst());
        assertThat(found)
            .as("server %d must find record %d by its own embedding, the ones that lost a vector id included", server, record)
            .isEqualTo(owners[k]);
      }
    }

    assertClusterConsistency();
  }

  private DatabaseInternal wrapped(final int serverIndex) {
    return ((DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName())).getWrappedDatabaseInstance();
  }

  private Result runCheck(final int serverIndex, final String statement) {
    try (final ResultSet rs = wrapped(serverIndex).command("sql", statement)) {
      assertThat(rs.hasNext()).as("'%s' must return a result", statement).isTrue();
      return rs.next();
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
    final Random random = new Random(0xC0FFEEL * 31 + seed);
    final float[] v = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] = (float) random.nextGaussian();
    return v;
  }
}
