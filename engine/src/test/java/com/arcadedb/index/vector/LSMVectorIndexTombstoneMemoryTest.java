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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5516: re-embedding the same vertices grew the in-memory
 * {@link VectorLocationIndex} without bound, because every update tombstoned the previous vector id but kept its
 * {@code VectorLocation} (plus its RID and its reverse-index slot) resident until a full graph rebuild happened to
 * clear the whole map. A superseded id is dead the moment its tombstone is persisted, so it must not stay in memory.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexTombstoneMemoryTest extends TestHelper {

  private static final int DIMENSIONS = 16;
  private static final int VERTICES   = 100;
  private static final int CYCLES     = 10;

  @Test
  void updateCyclesDoNotAccumulateStaleLocations() {
    createSchema();
    insertVertices();

    final LSMVectorIndex index = getVectorIndex();
    assertThat(index.getVectorIndex().size()).as("one location per vertex after the initial load").isEqualTo(VERTICES);

    for (int cycle = 1; cycle <= CYCLES; cycle++)
      updateAllVertices(cycle);

    final VectorLocationIndex locations = index.getVectorIndex();
    assertThat(locations.getActiveCount()).as("active vectors after %d re-embedding cycles", CYCLES).isEqualTo(VERTICES);
    assertThat(locations.size())
        .as("superseded vector locations must not stay resident (issue #5516)")
        .isEqualTo(VERTICES);
    assertThat(locations.getDeletedCount()).as("every superseded id is tracked as one bit").isEqualTo(VERTICES * CYCLES);

    // Each update must mint exactly ONE new vector id: the commit used to index the record twice (once while
    // serializing the updated record, once replaying the queued index operations), so every re-embedding cycle
    // burned two ids and wrote two entries plus two tombstones per vertex.
    assertThat(locations.getNextId())
        .as("vector ids handed out: one per vertex plus one per update")
        .isEqualTo(VERTICES * (CYCLES + 1));

    // The reverse index must shrink with it: one live id per RID, not one per update.
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE id = 'doc0'");
      final var rid = rs.next().getIdentity().get();
      assertThat(locations.getVectorIdsForRid(rid)).as("only the live vector id is mapped to the RID").hasSize(1);
    });

    assertSearchWorks(50);
  }

  @Test
  void reopenDoesNotLoadSupersededLocations() {
    createSchema();
    insertVertices();

    for (int cycle = 1; cycle <= CYCLES; cycle++)
      updateAllVertices(cycle);

    reopenDatabase();

    final LSMVectorIndex index = getVectorIndex();
    assertThat(index.getVectorIndex().size())
        .as("tombstoned page entries must not be loaded in memory at open (issue #5516)")
        .isEqualTo(VERTICES);
    assertThat(index.getVectorIndex().getActiveCount()).isEqualTo(VERTICES);
    assertThat(index.countEntries()).isEqualTo(VERTICES);

    assertSearchWorks(50);
  }

  @Test
  void deletedVerticesStayDeletedAcrossReopen() {
    createSchema();
    insertVertices();

    final int deleted = 30;
    database.transaction(() -> {
      for (int i = 0; i < deleted; i++)
        database.command("sql", "DELETE FROM Doc WHERE id = ?", "doc" + i);
    });

    final LSMVectorIndex index = getVectorIndex();
    assertThat(index.getVectorIndex().size()).isEqualTo(VERTICES - deleted);
    assertThat(index.countEntries()).isEqualTo(VERTICES - deleted);

    // The tombstones must reach the pages: the location they used to be read back from is released as soon as the
    // id is tombstoned, so a lost RID there would resurrect every deleted vector at the next open.
    reopenDatabase();

    final LSMVectorIndex reopened = getVectorIndex();
    assertThat(reopened.getVectorIndex().size()).isEqualTo(VERTICES - deleted);
    assertThat(reopened.countEntries()).isEqualTo(VERTICES - deleted);
    assertThat(reopened.getVectorIndex().getDeletedCount()).isEqualTo(deleted);

    // The vertices that survived are still searchable (the deleted ones are the first 30).
    assertSearchWorks(50);
  }

  /**
   * A transaction that does NOT replay its queued index operations - a replica, where the index pages arrive with
   * the leader's changes - must keep applying the vector update during the record-serialization step of its commit.
   * The queue-instead-of-apply optimization that removes the double indexing is gated on that flag precisely so
   * this path is left alone; dropping the guard would silently lose the update here, since nothing would replay it.
   */
  @Test
  void anUpdateStillLandsWhenTheTransactionDoesNotReplayItsIndexQueue() {
    createSchema();
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET id = 'r1', embedding = ?", embedding(1, 0)));

    final LSMVectorIndex index = getVectorIndex();
    final int idsBefore = index.getVectorIndex().getNextId();

    // Commit the update the way a replica does: 1st phase told it is not the leader, so TransactionIndexContext
    // never replays the operations queued during the transaction.
    final DatabaseInternal db = (DatabaseInternal) database;
    db.begin();
    final MutableDocument doc = database.query("sql", "SELECT FROM Doc WHERE id = 'r1'").next().getRecord().get().asVertex()
        .modify();
    doc.set("embedding", embedding(1, 1));
    doc.save();
    final TransactionContext tx = db.getTransaction();
    tx.commit2ndPhase(tx.commit1stPhase(false));

    assertThat(index.getVectorIndex().getNextId())
        .as("the replica applies the update itself: exactly one new vector id, and not zero")
        .isEqualTo(idsBefore + 1);
    assertThat(index.countEntries()).as("still one live vector for the RID").isEqualTo(1);
  }

  @Test
  void dropIndexReleasesInMemoryLocations() {
    createSchema();
    insertVertices();

    final LSMVectorIndex index = getVectorIndex();
    assertThat(index.getVectorIndex().size()).isEqualTo(VERTICES);

    database.command("sql", "DROP INDEX `Doc[embedding]`");

    assertThat(index.getVectorIndex().size()).as("DROP INDEX must release the in-memory locations").isEqualTo(0);
  }

  private void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

      // The graph rebuild is what used to (occasionally) clear the whole location map: disable both of its triggers
      // so the test measures the location index alone, the way a write-only workload between rebuilds behaves.
      database.command("sql", """
          CREATE INDEX ON Doc (embedding) LSM_VECTOR
          METADATA {
            "dimensions": %d,
            "similarity": "COSINE",
            "quantization": "INT8",
            "mutationsBeforeRebuild": 100000000,
            "inactivityRebuildTimeoutMs": 0
          }""".formatted(DIMENSIONS));
    });
  }

  private void insertVertices() {
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i, 0));
    });
  }

  private void updateAllVertices(final int cycle) {
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = ?", embedding(i, cycle), "doc" + i);
    });
  }

  private static float[] embedding(final int vertex, final int cycle) {
    final float[] v = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      v[j] = (float) Math.sin((vertex + 1) * 0.01 * (j + 1) + cycle * 0.001);
    return v;
  }

  private LSMVectorIndex getVectorIndex() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
    return (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  /** Search around a vertex that is still alive, and check the vector it finds first is that same vertex. */
  private void assertSearchWorks(final int liveVertex) {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT `vector.neighbors`('Doc[embedding]', ?, 5) AS neighbors FROM Doc LIMIT 1",
          (Object) embedding(liveVertex, CYCLES));
      assertThat(rs.hasNext()).isTrue();
      final List<Map<String, Object>> neighbors = rs.next().getProperty("neighbors");
      assertThat(neighbors).isNotEmpty();
      assertThat(neighbors.getFirst().get("id")).isEqualTo("doc" + liveVertex);
    });
  }
}
