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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that COMPACT INDEX on an LSM_VECTOR index actually reclaims the space the append-only write path
 * accumulates, and that the index survives it: same live vectors, same neighbors, same content after a reopen.
 * <p>
 * Automatic compaction of vector indexes is currently disabled ({@code LSMVectorIndex.onAfterCommit}), so an
 * LSM_VECTOR file grows with every write for the whole life of the database: one entry per insert, one entry plus
 * one tombstone per update. Manual {@code COMPACT INDEX} is the only reclaim path, and these tests are what says
 * whether it can be trusted with the job.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class LSMVectorIndexCompactionTest extends TestHelper {

  private static final int DIMENSIONS = 384;
  private static final int VERTICES   = 500;
  private static final int CYCLES     = 10;

  @Test
  void compactReclaimsSpaceAndKeepsTheIndexIntact() {
    createSchema();
    insertVertices();
    for (int cycle = 1; cycle <= CYCLES; cycle++)
      updateAllVertices(cycle);

    final long sizeBefore = indexBytes();
    final int pagesBefore = totalIndexPages();
    assertThat(pagesBefore).as("the workload must span several pages, otherwise nothing can be reclaimed")
        .isGreaterThan(4);
    assertThat(countLive()).isEqualTo(VERTICES);

    database.command("sql", "COMPACT INDEX `Doc[embedding]`");

    final long sizeAfter = indexBytes();

    assertThat(countLive()).as("compaction must not lose live vectors").isEqualTo(VERTICES);
    assertNeighborsAreExact("after compaction");
    assertThat(sizeAfter).as("compaction must reclaim the superseded entries (before=%d after=%d)", sizeBefore, sizeAfter)
        .isLessThan(sizeBefore);

    reopenDatabase();

    assertThat(countLive()).as("compaction must survive a reopen").isEqualTo(VERTICES);
    assertNeighborsAreExact("after reopen");
  }

  @Test
  void repeatedCompactionKeepsEveryVector() {
    createSchema();
    insertVertices();
    for (int cycle = 1; cycle <= CYCLES; cycle++)
      updateAllVertices(cycle);

    database.command("sql", "COMPACT INDEX `Doc[embedding]`");
    assertThat(countLive()).as("live vectors after the 1st compaction").isEqualTo(VERTICES);

    // More writes land in the new mutable file, then compact again: the 2nd run must merge with - not overwrite -
    // what the 1st one moved into the compacted component.
    for (int cycle = CYCLES + 1; cycle <= CYCLES + 5; cycle++)
      updateAllVertices(cycle);

    database.command("sql", "COMPACT INDEX `Doc[embedding]`");

    assertThat(countLive()).as("live vectors after the 2nd compaction").isEqualTo(VERTICES);
    assertNeighborsAreExact("after the 2nd compaction");

    reopenDatabase();
    assertThat(countLive()).as("live vectors after reopen").isEqualTo(VERTICES);
    assertNeighborsAreExact("after reopen");
  }

  @Test
  void deletedVectorsStayDeletedAcrossCompaction() {
    createSchema();
    insertVertices();
    for (int cycle = 1; cycle <= CYCLES; cycle++)
      updateAllVertices(cycle);

    final int deleted = 100;
    database.transaction(() -> {
      for (int i = 0; i < deleted; i++)
        database.command("sql", "DELETE FROM Doc WHERE id = ?", "doc" + i);
    });
    assertThat(countLive()).isEqualTo(VERTICES - deleted);

    database.command("sql", "COMPACT INDEX `Doc[embedding]`");
    assertThat(countLive()).as("deleted vectors must not come back through compaction").isEqualTo(VERTICES - deleted);

    reopenDatabase();
    assertThat(countLive()).as("deleted vectors must not come back after a reopen").isEqualTo(VERTICES - deleted);

    // A search around a surviving vertex must still find that vertex first.
    assertNeighborIsSelf(VERTICES - 1, "after compaction + reopen");
  }

  /**
   * A compaction swaps in a new data file, and the index must be named after it everywhere it is observable: its own
   * name, the name the schema keys its entry by, and the name it serializes. Every node names a vector index after
   * the file it holds - a follower rebuilds it from the file the leader shipped - so a leader that keeps answering
   * its creation name describes an index no other node has, and the cluster's schemas diverge. That failure only
   * reproduces across nodes (RaftIndexCompactionReplicationIT#lsmVectorCompactionReplication catches it); this is
   * the single-node guard for the same invariant, so a regression does not need a cluster to be noticed.
   */
  @Test
  void theIndexIdentityFollowsTheCompactedFile() {
    createSchema();
    insertVertices();
    for (int cycle = 1; cycle <= CYCLES; cycle++)
      updateAllVertices(cycle);

    final LSMVectorIndex index = vectorIndex();
    final String nameBefore = index.getName();

    database.command("sql", "COMPACT INDEX `Doc[embedding]`");

    final String component = index.getComponent().getName();
    assertThat(component).as("compaction must have swapped in a new data file").isNotEqualTo(nameBefore);
    assertThat(index.getName()).as("the index is named after its current component").isEqualTo(component);
    assertThat(index.getMostRecentFileName()).as("the schema keys its entry by this").isEqualTo(component);
    assertThat(index.toJSON().getString("indexName")).as("and serializes the same name").isEqualTo(component);

    // The file the name points at is the one that exists on disk.
    assertThat(new File(database.getDatabasePath()).listFiles((dir, n) -> n.startsWith(component + ".")))
        .as("the component named by the index must exist on disk").isNotEmpty();
  }

  // ------------------------------------------------------------------------------------------------- helpers

  private void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");
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

  /** Well-separated vectors: the nearest neighbor of vertex i must be vertex i itself. */
  private static float[] embedding(final int vertex, final int cycle) {
    final float[] v = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      v[j] = (float) Math.sin((vertex + 1) * 0.37 * (j + 1) + cycle * 0.0001);
    return v;
  }

  private LSMVectorIndex vectorIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }

  private long countLive() {
    return vectorIndex().countEntries();
  }

  private int totalIndexPages() {
    return vectorIndex().getTotalPages();
  }

  /** Bytes on disk of every component of the vector index (mutable + compacted + graph). */
  private long indexBytes() {
    long total = 0;
    final File[] files = new File(database.getDatabasePath()).listFiles();
    if (files != null)
      for (final File f : files)
        if (f.getName().endsWith("." + LSMVectorIndex.FILE_EXT) || f.getName().endsWith("." + LSMVectorIndexCompacted.FILE_EXT))
          total += f.length();
    return total;
  }

  private void assertNeighborsAreExact(final String phase) {
    for (final int vertex : new int[] { 0, 1, VERTICES / 2, VERTICES - 1 })
      assertNeighborIsSelf(vertex, phase);
  }

  private void assertNeighborIsSelf(final int vertex, final String phase) {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT `vector.neighbors`('Doc[embedding]', ?, 3) AS neighbors FROM Doc LIMIT 1",
          (Object) embedding(vertex, CYCLES));
      assertThat(rs.hasNext()).isTrue();
      final List<Map<String, Object>> neighbors = rs.next().getProperty("neighbors");
      assertThat(neighbors).as("neighbors of doc%d %s", vertex, phase).isNotEmpty();
      assertThat(neighbors.getFirst().get("id")).as("closest vector to doc%d %s", vertex, phase)
          .isEqualTo("doc" + vertex);
    });
  }
}
