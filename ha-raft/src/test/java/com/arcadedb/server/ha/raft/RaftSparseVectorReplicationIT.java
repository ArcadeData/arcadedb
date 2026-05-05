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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke test that the {@code LSM_SPARSE_VECTOR} index (page-component-backed v1 backend)
 * replicates from the Raft leader to all followers and that follower replicas can serve
 * top-K queries. Each segment is a {@code SparseSegmentComponent} and therefore replicates via
 * ArcadeDB's standard component-shipping pipeline; this test confirms that path end-to-end.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftSparseVectorReplicationIT extends BaseRaftHATest {

  private static final String TYPE_NAME  = "RaftSparseDoc";
  private static final String IDX_NAME   = "RaftSparseDoc[tokens,weights]";
  private static final int    DIMENSIONS = 100;
  private static final int    DOC_COUNT  = 200;
  private static final int    NNZ        = 8;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 30_000L);
  }

  @Override
  protected void populateDatabase() {
    // We want full control over schema + writes from the leader, so skip the default population.
  }

  /**
   * Smoke test for follower replication of {@code SparseSegmentComponent} files. The leader's
   * {@link com.arcadedb.index.sparsevector.PaginatedSparseVectorEngine#flush} runs inside
   * {@code DatabaseInternal.runWithCompactionReplication}, the same hook that LSM-Tree compaction
   * uses; the FileManager records the new component file plus the page contents, and a
   * {@code SCHEMA_ENTRY} ships the bundle to every follower. After replication completes each
   * follower's {@code LocalSchema.load} re-instantiates the {@code LSMSparseVectorIndex} wrapper
   * with a fresh engine that picks up the newly-arrived component and answers queries.
   */
  @Test
  void sparseVectorIndexReplicates() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Create the schema + index on the leader.
    leaderDb.transaction(() -> {
      final DocumentType type = leaderDb.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);
      leaderDb.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(DIMENSIONS)
          .create();
    });

    // Insert sparse vectors on the leader. Use a deterministic seed so we can reconstruct
    // expected results on followers without round-tripping the document RIDs. The dataset is
    // tiny (1600 postings, well below the 1M auto-flush threshold) so we explicitly flush the
    // index below to force segment-file creation; in production a workload that's permanently
    // below the threshold should call {@code Index.flush()} before relying on followers seeing
    // the data, which is the contract this test exercises.
    leaderDb.transaction(() -> {
      final java.util.Random rnd = new java.util.Random(0xCAFEL);
      for (int i = 0; i < DOC_COUNT; i++) {
        final int[] tokens = new int[NNZ];
        final float[] weights = new float[NNZ];
        final HashSet<Integer> picked = new HashSet<>();
        for (int j = 0; j < NNZ; j++) {
          int dim;
          do {
            dim = rnd.nextInt(DIMENSIONS);
          } while (!picked.add(dim));
          tokens[j] = dim;
          weights[j] = 0.1f + rnd.nextFloat();
        }
        final MutableDocument doc = leaderDb.newDocument(TYPE_NAME);
        doc.set("tokens", tokens);
        doc.set("weights", weights);
        doc.save();
      }
    });

    // Force every per-bucket sparse-vector index to flush its memtable into a sealed segment.
    // Replication ships sealed component pages, so until a flush happens the leader's writes are
    // memtable-resident only and don't propagate to followers.
    final com.arcadedb.index.TypeIndex typeIndex =
        (com.arcadedb.index.TypeIndex) leaderDb.getSchema().getIndexByName(IDX_NAME);
    for (final com.arcadedb.index.IndexInternal sub : typeIndex.getIndexesOnBuckets())
      sub.flush();

    // Wait for replication to complete on every server.
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Build a deterministic query that will hit a known set of dims.
    final int[] queryTokens = { 1, 7, 13, 21, 33 };
    final float[] queryWeights = { 0.5f, 0.7f, 0.9f, 0.6f, 0.4f };

    // Schema-level invariants: every server should know about the type and the index. This is
    // the bedrock that the rest of the smoke test depends on.
    testEachServer(serverIndex -> {
      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      assertThat(serverDb.countType(TYPE_NAME, false))
          .as("server %d document count", serverIndex)
          .isEqualTo((long) DOC_COUNT);
      assertThat(serverDb.getSchema().existsIndex(IDX_NAME))
          .as("server %d should know about index %s", serverIndex, IDX_NAME)
          .isTrue();
    });

    // Top-K query: every server (leader and every follower) must return the same non-empty
    // result set with byte-exact scores. Replication ships sealed SparseSegmentComponent files
    // via the standard component-shipping pipeline (SCHEMA_ENTRY with addFiles + synthetic page
    // WAL), so once waitForReplicationIsCompleted returns, every server is expected to be at
    // parity. A weaker "non-empty result set" check would mask a class of bugs where the WAL
    // replay corrupts a few weights silently - we capture the leader's full (rid, score) list
    // first, then assert each follower returns exactly the same sequence.
    final java.util.List<java.util.List<Object[]>> perServer = new java.util.ArrayList<>(getServerCount());
    for (int i = 0; i < getServerCount(); i++) {
      final Database serverDb = getServerDatabase(i, getDatabaseName());
      final ResultSet rs = serverDb.query("sql",
          "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
          IDX_NAME, queryTokens, queryWeights, 5);
      final java.util.List<Object[]> rows = new java.util.ArrayList<>();
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Object rid = row.getProperty("@rid");
        assertThat(rid).as("server %d result must carry @rid", i).isNotNull();
        rows.add(new Object[] { rid, row.getProperty("score") });
      }
      assertThat(rows.size())
          .as("server %d must serve top-K (leader=%s)", i, i == leaderIndex)
          .isGreaterThan(0);
      perServer.add(rows);
    }

    final java.util.List<Object[]> leaderRows = perServer.get(leaderIndex);
    for (int i = 0; i < getServerCount(); i++) {
      if (i == leaderIndex)
        continue;
      final java.util.List<Object[]> rows = perServer.get(i);
      assertThat(rows.size())
          .as("server %d row count must match leader (%d)", i, leaderRows.size())
          .isEqualTo(leaderRows.size());
      for (int r = 0; r < rows.size(); r++) {
        assertThat(rows.get(r)[0])
            .as("server %d row %d @rid must match leader", i, r)
            .isEqualTo(leaderRows.get(r)[0]);
        assertThat(rows.get(r)[1])
            .as("server %d row %d score must match leader (a divergent score implies WAL-replay weight corruption)", i, r)
            .isEqualTo(leaderRows.get(r)[1]);
      }
    }
  }
}
