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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An LSM_VECTOR data file only ever grows: an update appends a new vector plus a tombstone for the one it
 * supersedes, a delete appends a tombstone, and nothing reclaims either. Compaction is a rebuild that also rewrites
 * the file (issue #5516 follow-up), and this is the test that it happens on its own - an index left to run must not
 * need an operator typing COMPACT INDEX to stop growing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class LSMVectorIndexAutoCompactionTest extends TestHelper {

  private static final int DIMENSIONS = 384;
  private static final int VERTICES   = 400;
  private static final int CYCLES     = 12;
  // A small page fills after ~39 vectors, so 400 live vectors already occupy more pages than
  // indexCompactionMinPagesSchedule ignores. Every assertion below then turns on the garbage ratio, which is what
  // this test is about, instead of passing because the file was too small to bother with.
  private static final int PAGE_SIZE  = 16 * 1024;

  @Test
  void anUpdateHeavyIndexCompactsItselfWithoutAnExplicitCommand() {
    createSchema();
    insertVertices();

    final LSMVectorIndex index = vectorIndex();
    final int fileIdBefore = index.getFileId();

    // Re-embed everything a few times: each cycle appends a vector and a tombstone per vertex, so the file grows
    // well past what the live set needs and the automatic trigger has to fire.
    int pagesPeak = index.getTotalPages();
    for (int cycle = 1; cycle <= CYCLES && index.getFileId() == fileIdBefore; cycle++) {
      updateAllVertices(cycle);
      pagesPeak = Math.max(pagesPeak, index.getTotalPages());
      awaitCompactionIdle();
    }

    assertThat(pagesPeak).as("the workload must first grow the file well past the live set, or nothing is tested")
        .isGreaterThan(3 * index.estimatePagesForLiveSetForTest());
    assertThat(index.getFileId())
        .as("the index must have compacted itself: the data file should have been swapped (peak was %d pages)",
            pagesPeak)
        .isNotEqualTo(fileIdBefore);

    final LSMVectorIndex compacted = vectorIndex();
    assertThat(compacted.getTotalPages()).as("the compacted file holds only the live vectors").isLessThan(pagesPeak);
    assertThat(compacted.countEntries()).as("no vector lost to the automatic compaction").isEqualTo(VERTICES);
    assertNeighborIsSelf(VERTICES / 2);

    reopenDatabase();
    assertThat(vectorIndex().countEntries()).as("and it survives a reopen").isEqualTo(VERTICES);
  }

  @Test
  void anIndexThatIsNotBloatedIsLeftAlone() {
    createSchema();
    insertVertices();

    final LSMVectorIndex index = vectorIndex();
    final int fileIdBefore = index.getFileId();

    // Inserts alone leave no garbage behind: every entry is live, so there is nothing to reclaim and rewriting the
    // file would be pure cost.
    for (int i = VERTICES; i < VERTICES * 2; i++)
      insertVertex(i);
    awaitCompactionIdle();

    assertThat(index.getFileId()).as("an index with no garbage must not be rewritten").isEqualTo(fileIdBefore);
    assertThat(index.countEntries()).isEqualTo(VERTICES * 2L);
  }

  @Test
  void automaticCompactionCanBeTurnedOff() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    try {
      createSchema();
      insertVertices();

      final LSMVectorIndex index = vectorIndex();
      final int fileIdBefore = index.getFileId();

      for (int cycle = 1; cycle <= CYCLES; cycle++) {
        updateAllVertices(cycle);
        awaitCompactionIdle();
      }

      assertThat(index.getFileId()).as("with the scheduler disabled nothing may compact on its own")
          .isEqualTo(fileIdBefore);
      assertThat(index.countEntries()).isEqualTo(VERTICES);
    } finally {
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.reset();
    }
  }

  // ------------------------------------------------------------------------------------------------- helpers

  /** Compaction runs on the async executor: let anything already scheduled finish before looking at the file. */
  private void awaitCompactionIdle() {
    database.async().waitCompletion(30_000);
  }

  private void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

      final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType().withPageSize(PAGE_SIZE);
      builder.withDimensions(DIMENSIONS).withQuantization(VectorQuantizationType.INT8).create();
    });
  }

  private void insertVertices() {
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i, 0));
    });
  }

  private void insertVertex(final int i) {
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i,
        embedding(i, 0)));
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
      v[j] = (float) Math.sin((vertex + 1) * 0.37 * (j + 1) + cycle * 0.0001);
    return v;
  }

  private LSMVectorIndex vectorIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }

  private void assertNeighborIsSelf(final int vertex) {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT `vector.neighbors`('Doc[embedding]', ?, 3) AS neighbors FROM Doc LIMIT 1",
          (Object) embedding(vertex, 12));
      assertThat(rs.hasNext()).isTrue();
      final List<Map<String, Object>> neighbors = rs.next().getProperty("neighbors");
      assertThat(neighbors).isNotEmpty();
      assertThat(neighbors.getFirst().get("id")).isEqualTo("doc" + vertex);
    });
  }
}
