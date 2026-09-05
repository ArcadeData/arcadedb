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
package com.arcadedb.index.sparsevector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7140: {@code Index.countEntries()} promises the number of LIVE entries - "keys deleted but not yet purged by
 * a compaction (tombstones) are NOT counted, so on an LSM index the value drops as records are removed instead of
 * settling on a residual (#5601)". {@link LSMSparseVectorIndex} answered it with
 * {@code PaginatedSparseVectorEngine.totalPostings()}, whose own javadoc says it is the on-disk + in-memory entry
 * count INCLUDING tombstones - exactly the one thing the contract forbids.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7140SparseVectorLiveCountTest extends TestHelper {

  /** A tombstone still sitting in the memtable must not be counted, and must not count its masked posting either. */
  @Test
  void memtableTombstonesAreNotCounted() throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;
    try (final PaginatedSparseVectorEngine engine = nonCompactingEngine(db, "Issue7140Memtable")) {
      for (int i = 0; i < 10; i++)
        engine.put(1, new RID(0, i), 0.5f);

      assertThat(engine.livePostings()).isEqualTo(10L);

      for (int i = 0; i < 4; i++)
        engine.remove(1, new RID(0, i));

      assertThat(engine.totalPostings())
          .as("totalPostings stays the sizing metric: the tombstones are still stored")
          .isGreaterThan(6L);
      assertThat(engine.livePostings())
          .as("the live count must DROP as postings are removed, not settle on a residual")
          .isEqualTo(6L);
    }
  }

  /** The tombstone in a newer source has to mask the live posting an older sealed segment still holds. */
  @Test
  void tombstoneInNewerSegmentMasksAnOlderLivePosting() throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;
    try (final PaginatedSparseVectorEngine engine = nonCompactingEngine(db, "Issue7140Segments")) {
      for (int i = 0; i < 6; i++)
        engine.put(7, new RID(0, i), 0.5f);
      engine.flush();
      assertThat(engine.segmentCount()).isEqualTo(1);
      assertThat(engine.livePostings()).isEqualTo(6L);

      // A second sealed segment carrying only tombstones for RIDs the first one holds live
      for (int i = 0; i < 3; i++)
        engine.remove(7, new RID(0, i));
      engine.flush();
      assertThat(engine.segmentCount()).as("the tombstone-only memtable must be sealed, not dropped").isEqualTo(2);

      assertThat(engine.totalPostings())
          .as("both segments still hold their entries on disk")
          .isEqualTo(9L);
      assertThat(engine.livePostings())
          .as("newest-source-wins: the 3 tombstones mask the 3 live postings of the older segment")
          .isEqualTo(3L);
    }
  }

  /** A RID re-added after a delete counts once, not twice: the merge is per (dim, RID), not per stored entry. */
  @Test
  void aRidPresentInSeveralSourcesCountsOnce() throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;
    try (final PaginatedSparseVectorEngine engine = nonCompactingEngine(db, "Issue7140Shadow")) {
      engine.put(3, new RID(0, 1), 0.5f);
      engine.flush();
      engine.put(3, new RID(0, 1), 0.9f);
      engine.flush();

      assertThat(engine.totalPostings()).isEqualTo(2L);
      assertThat(engine.livePostings()).as("one RID under one dim is one live entry").isEqualTo(1L);
    }
  }

  /** End to end through the index: deleting records must make countEntries() go down. */
  @Test
  void countEntriesOnTheIndexDropsWhenRecordsAreDeleted() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Issue7140Sparse");
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);
      database.getSchema().buildTypeIndex("Issue7140Sparse", new String[] { "tokens", "weights" })
          .withSparseVectorType().withDimensions(16).create();
    });

    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("Issue7140Sparse")
            .set("tokens", new int[] { i % 16, (i + 1) % 16 })
            .set("weights", new float[] { 0.5f, 0.25f })
            .save();
    });

    final Index index = database.getSchema().getIndexByName("Issue7140Sparse[tokens,weights]");
    assertThat(index.countEntries()).as("2 postings per document").isEqualTo(20L);

    database.transaction(() -> database.command("sql", "delete from Issue7140Sparse limit 4"));

    assertThat(index.countEntries())
        .as("countEntries() must drop as records are deleted, not stay on the tombstone-inclusive total")
        .isEqualTo(12L);
  }

  /**
   * An engine whose size-tiered auto-compaction gate cannot fire under a unit-test workload, so the segment count -
   * and therefore which sources hold a tombstone - stays deterministic between assertions.
   */
  private static PaginatedSparseVectorEngine nonCompactingEngine(final DatabaseInternal db, final String name) {
    return new PaginatedSparseVectorEngine(db, name, SegmentParameters.defaults(),
        /* memtableFlushThreshold */ 1_000_000L,
        /* tierFanout */ 1_000_000,
        /* tierBasePostings */ 1L);
  }
}
