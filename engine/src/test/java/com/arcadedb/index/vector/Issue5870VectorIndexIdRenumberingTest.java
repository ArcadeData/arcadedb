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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5870: {@code LSMVectorIndex.rewriteDataFileWithLiveEntries} renumbers the live set densely from 0 during
 * a compaction instead of preserving each vector's pre-compaction id, so the id space is dense by construction
 * after every compaction rather than merely bounding the partially drained band trailing the live region (the
 * residual weakness issue #5588 left open).
 * <p>
 * Both files a compaction replaces - the previous data file and the legacy compacted component - are dropped
 * afterwards, and {@code publishLocationIndex} always rebuilds {@link VectorLocationIndex} fresh rather than
 * refilling it in place, so no on-disk tombstone entry and no in-memory {@code DeletedIds} bit for a discarded or
 * reissued id survives a compaction: that is what makes reissuing ids safe rather than merely convenient.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue5870VectorIndexIdRenumberingTest extends TestHelper {

  private static final int DIMENSIONS = 16;
  private static final int VERTICES   = 300;

  /**
   * Acceptance criterion 1: {@code getMaxVectorId() == size() - 1} after a compaction. Acceptance criterion 2: no
   * partially drained band survives a compaction - {@code chunkCount() == ceil(live / CHUNK_SIZE)}.
   * <p>
   * The "before" state is built the way a real re-embedding workload builds it (issue #5516/#5588): one vertex
   * ({@code doc0}) is updated hundreds of times, so its live id climbs far past the dense block the other
   * {@value #VERTICES}{@code - 1} vertices still occupy. Every chunk behind it drains and releases wholesale (the
   * existing #5516 mechanism already handles that), but the chunk holding its current id is a lone straggler - the
   * one thing only a renumbering compaction removes.
   */
  @Test
  void compactionMakesTheLiveIdSpaceDenseWithNoTrailingStraggler() {
    createSchema();
    insertVertices();

    final LSMVectorIndex index = vectorIndex();
    final int denseChunksForLiveSet =
        (VERTICES + VectorLocationIndex.CHUNK_SIZE - 1) / VectorLocationIndex.CHUNK_SIZE;

    // Re-embed doc0 enough times that its live id crosses at least one chunk boundary past the dense block the
    // other vertices still occupy (ids 1..VERTICES-1, i.e. chunks 0..denseChunksForLiveSet-1). One transaction per
    // update, like a real re-embedding workload issues it: each commit tombstones doc0's current id and mints
    // exactly one new one.
    final int updateCycles = VERTICES + 2 * VectorLocationIndex.CHUNK_SIZE;
    for (int cycle = 0; cycle < updateCycles; cycle++) {
      final int c = cycle;
      database.transaction(() -> database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = ?", embedding(0, c + 1), "doc0"));
    }

    assertThat(index.getVectorIndex().size()).as("re-embedding must not change the live count").isEqualTo(VERTICES);
    assertThat(index.getVectorIndex().chunkCount())
        .as("before compaction: the dense block plus one straggler chunk trailing far behind it")
        .isGreaterThan(denseChunksForLiveSet);

    database.command("sql", "COMPACT INDEX `Doc[embedding]`");

    assertThat(index.getVectorIndex().size()).as("compaction must not lose or gain live vectors").isEqualTo(VERTICES);
    assertThat(index.getVectorIndex().getMaxVectorId())
        .as("the live set occupies exactly [0, size()) after a renumbering compaction")
        .isEqualTo(index.getVectorIndex().size() - 1);
    assertThat(index.getVectorIndex().chunkCount())
        .as("no partially drained band survives a renumbering compaction")
        .isEqualTo(denseChunksForLiveSet);

    assertSearchWorks(0, updateCycles);
    assertSearchWorks(VERTICES / 2, 0);

    reopenDatabase();

    final LSMVectorIndex reopened = vectorIndex();
    assertThat(reopened.getVectorIndex().size()).as("density survives a reopen").isEqualTo(VERTICES);
    assertThat(reopened.getVectorIndex().getMaxVectorId()).isEqualTo(reopened.getVectorIndex().size() - 1);
    assertSearchWorks(0, updateCycles);
    assertSearchWorks(VERTICES / 2, 0);
  }

  /**
   * Acceptance criterion 4: a tombstone written before a renumbering cannot suppress a vector that inherits its id
   * afterwards.
   * <p>
   * Which document ends up holding vector id 0 after the initial insert is an implementation detail (a single
   * transaction's queued index operations are not guaranteed to replay in statement order), so this test does not
   * assume "doc0" got id 0. It reads the id assignment back from the location index instead, and deletes exactly
   * the documents holding the lowest half of the ids - guaranteeing id 0 itself is tombstoned before the
   * compaction. Renumbering then reissues id 0 to whichever survivor held the smallest surviving old id: without
   * the fix that survivor keeps its old (higher) id and id 0 stays absent; with the fix it inherits id 0, the exact
   * integer a tombstone referred to moments earlier.
   */
  @Test
  void aTombstoneWrittenBeforeRenumberingCannotSuppressAVectorThatInheritsItsId() {
    createSchema();
    insertVertices();

    final int deleted = VERTICES / 2;
    final int[] vertexOfLowestIds = resolveVertexIndicesOfLowestIds(deleted);
    database.transaction(() -> {
      for (final int vertex : vertexOfLowestIds)
        database.command("sql", "DELETE FROM Doc WHERE id = ?", "doc" + vertex);
    });
    assertThat(countLive()).isEqualTo(VERTICES - deleted);

    database.command("sql", "COMPACT INDEX `Doc[embedding]`");
    assertThat(countLive()).as("compaction must not resurrect the deleted half").isEqualTo(VERTICES - deleted);

    final LSMVectorIndex index = vectorIndex();
    final VectorLocationIndex locations = index.getVectorIndex();

    // Id 0 is reissued to a survivor by a renumbering compaction, and it must NOT read back as deleted even though
    // a tombstone for id 0 - a different vector entirely - was written moments before the compaction.
    assertThat(locations.isLive(0)).as("id 0 must be live: it was reissued to a surviving vector").isTrue();
    assertThat(locations.isDeleted(0)).as("no stale tombstone for the discarded owner of id 0 may survive").isFalse();

    final RID survivorRid = locations.getRid(0);
    assertThat(survivorRid).as("id 0 must resolve to a document").isNotNull();
    final int[] survivorVertexHolder = new int[1];
    database.transaction(() -> {
      final Document survivor = (Document) database.lookupByRID(survivorRid, true);
      final String survivorId = survivor.getString("id");
      survivorVertexHolder[0] = Integer.parseInt(survivorId.substring("doc".length()));
    });
    final int survivorVertex = survivorVertexHolder[0];
    boolean wasDeleted = false;
    for (final int vertex : vertexOfLowestIds)
      wasDeleted |= vertex == survivorVertex;
    assertThat(wasDeleted).as("id 0's owner must be one of the survivors, not one of the deleted vertices").isFalse();

    // And the survivor is not silently buried: it is still found as its own nearest neighbor.
    assertSearchWorks(survivorVertex, 0);

    reopenDatabase();

    assertThat(countLive()).as("the reissued id must not resurrect a deleted vector after a reopen")
        .isEqualTo(VERTICES - deleted);
    assertSearchWorks(survivorVertex, 0);
  }

  /**
   * Exercises the dense sequence from the insert side rather than only observing it through
   * {@code getMaxVectorId()}: a compaction renumbers the live set to exactly {@code [0, VERTICES)}, and the next
   * insert must mint id {@code VERTICES} - not collide with the vector that now legitimately holds
   * {@code VERTICES - 1} (an off-by-one in the reset would do exactly that), and not merely resume from whatever
   * the pre-compaction high-water mark used to be (which would still be correct but would defeat the point of
   * renumbering: the id space would grow unboundedly again on the very first write after every compaction).
   * <p>
   * The insert runs after a {@link #reopenDatabase()}, not in the same session the compaction ran in. That was
   * originally forced on this test: a write issued immediately after {@code COMPACT INDEX} in the same session did
   * not reach the vector index at all, independently of this fix, which was filed and fixed separately as issue
   * #6105 (the schema's index registry was left keyed by the name the compaction retired) and is pinned by
   * {@code Issue6105WriteAfterCompactionTest}. The reopen is kept because it is what this test is actually about:
   * it forces {@code loadVectorsFromPages} to recompute {@code nextId} from the (now dense) persisted file, which
   * is the code path a real "compact, then restart, then keep writing" operational sequence takes.
   */
  @Test
  void insertingAfterARenumberingCompactionDoesNotCollideWithAnExistingId() {
    createSchema();
    insertVertices();

    database.command("sql", "COMPACT INDEX `Doc[embedding]`");
    assertThat(vectorIndex().getVectorIndex().getMaxVectorId()).as("the live set is dense after compaction")
        .isEqualTo(VERTICES - 1);

    reopenDatabase();

    final RID ridHoldingTheHighestId = vectorIndex().getVectorIndex().getRid(VERTICES - 1);
    assertThat(ridHoldingTheHighestId).isNotNull();

    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + VERTICES,
        embedding(VERTICES, 0)));

    final RID[] newRidHolder = new RID[1];
    database.transaction(() -> newRidHolder[0] = database.query("sql", "SELECT FROM Doc WHERE id = ?", "doc" + VERTICES)
        .next().getIdentity().get());
    final int[] newIds = vectorIndex().getVectorIndex().getVectorIdsForRid(newRidHolder[0]);
    assertThat(newIds).as("exactly one id for the new vector").hasSize(1);
    assertThat(newIds[0]).as("the next insert after a renumbering compaction must continue the dense sequence")
        .isEqualTo(VERTICES);

    assertThat(vectorIndex().getVectorIndex().getRid(VERTICES - 1))
        .as("the new insert must not have overwritten the vector that already held id VERTICES-1")
        .isEqualTo(ridHoldingTheHighestId);
    assertThat(countLive()).as("both the pre-existing and the new vector must be live").isEqualTo(VERTICES + 1);

    assertSearchWorks(VERTICES, 0);
    assertSearchWorks(VERTICES / 2, 0);
  }

  // ------------------------------------------------------------------------------------------------- helpers

  private void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

      // Automatic compaction of vector indexes is disabled (LSMVectorIndex.onAfterCommit): only the explicit
      // COMPACT INDEX below must trigger the renumbering rewrite this test is about.
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

  /**
   * The vertex index (the {@code N} in {@code "docN"}) of the {@code count} documents currently holding the
   * lowest-numbered vector ids, resolved through the location index rather than assumed from insertion order.
   */
  private int[] resolveVertexIndicesOfLowestIds(final int count) {
    final VectorLocationIndex locations = vectorIndex().getVectorIndex();
    final int[] lowestIds = locations.getAllVectorIds().limit(count).toArray(); // already ascending
    final int[] vertices = new int[lowestIds.length];
    database.transaction(() -> {
      for (int i = 0; i < lowestIds.length; i++) {
        final Document doc = (Document) database.lookupByRID(locations.getRid(lowestIds[i]), true);
        vertices[i] = Integer.parseInt(doc.getString("id").substring("doc".length()));
      }
    });
    return vertices;
  }

  private void assertSearchWorks(final int vertex, final int cycle) {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT `vector.neighbors`('Doc[embedding]', ?, 3) AS neighbors FROM Doc LIMIT 1",
          (Object) embedding(vertex, cycle));
      assertThat(rs.hasNext()).isTrue();
      final List<Map<String, Object>> neighbors = rs.next().getProperty("neighbors");
      assertThat(neighbors).as("neighbors of doc%d", vertex).isNotEmpty();
      assertThat(neighbors.get(0).get("id")).as("closest vector to doc%d", vertex).isEqualTo("doc" + vertex);
    });
  }
}
