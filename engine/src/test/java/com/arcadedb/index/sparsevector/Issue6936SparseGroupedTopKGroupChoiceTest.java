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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;
import com.arcadedb.schema.LocalSchema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #6936: {@link BmwScorer#topKGrouped}'s group admission handed out its
 * {@code limit} distinct-group slots to whichever keys the ascending-RID DAAT traversal reached
 * first, and never revisited that choice once all slots were taken - the sparse index's own
 * separate collector repeating the defect #5761 fixed for the dense (HNSW) grouped search.
 * <p>
 * Reproduces the issue's own repro shape: low-RID documents from groups A, B and C score near
 * zero for the query, and a single much-later (higher-RID) document from group Z scores far above
 * everything else. Before the fix, groups A/B/C fill the 3 available slots first and group Z -
 * holding the single best-scoring document in the corpus - is rejected outright.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6936SparseGroupedTopKGroupChoiceTest extends TestHelper {

  private static final SegmentParameters EXACT_PARAMS = SegmentParameters.builder()
      .weightQuantization(WeightQuantization.FP32)
      .build();

  @Test
  void bestScoringGroupWinsEvenAtTheHighestRid() throws Exception {
    final Map<RID, Map<Integer, Float>> docs = new TreeMap<>();
    final Map<RID, String> groupOf = new HashMap<>();

    final int[]   queryDims    = { 0, 1, 2 };
    final float[] queryWeights = { 1.0f, 1.0f, 1.0f };

    // Groups A, B, C: two low-RID, near-zero-scoring documents each - they fill the 3 slots first.
    addDoc(docs, groupOf, new RID(0, 1L), "A", Map.of(0, 0.01f));
    addDoc(docs, groupOf, new RID(0, 2L), "A", Map.of(0, 0.01f));
    addDoc(docs, groupOf, new RID(0, 3L), "B", Map.of(1, 0.01f));
    addDoc(docs, groupOf, new RID(0, 4L), "B", Map.of(1, 0.01f));
    addDoc(docs, groupOf, new RID(0, 5L), "C", Map.of(2, 0.01f));
    addDoc(docs, groupOf, new RID(0, 6L), "C", Map.of(2, 0.01f));
    // Group Z: a single, much-later (higher-RID) document that matches every term - the single
    // best-scoring document in the corpus.
    addDoc(docs, groupOf, new RID(0, 1_000L), "Z", Map.of(0, 1.0f, 1, 1.0f, 2, 1.0f));

    final PaginatedSegmentReader[] readerHolder = new PaginatedSegmentReader[1];
    inTx(() -> readerHolder[0] = buildSegment("seg-6936", 1L, docs));

    inTx(() -> {
      final PaginatedSegmentReader reader = readerHolder[0];
      final DimCursor[] cursors = new DimCursor[queryDims.length];
      try {
        for (int i = 0; i < queryDims.length; i++)
          cursors[i] = new DimCursor(queryDims[i], List.of(reader.openCursor(queryDims[i])));

        final List<RidScore> got = BmwScorer.topKGrouped(queryDims, queryWeights, cursors, 3, 2,
            rid -> groupOf.get(rid), null);

        final List<String> groups = got.stream().map(rs -> groupOf.get(rs.rid())).distinct().toList();
        assertThat(groups).as("groups returned for %s", got).contains("Z");
        assertThat(got.get(0).rid()).isEqualTo(new RID(0, 1_000L));
      } finally {
        for (final DimCursor c : cursors)
          if (c != null)
            c.close();
      }
    });
  }

  // ---------- helpers ----------

  private static void addDoc(final Map<RID, Map<Integer, Float>> docs, final Map<RID, String> groupOf, final RID rid,
      final String group, final Map<Integer, Float> weights) {
    docs.put(rid, weights);
    groupOf.put(rid, group);
  }

  @FunctionalInterface
  private interface CheckedRunnable {
    void run() throws Exception;
  }

  private void inTx(final CheckedRunnable r) {
    database.transaction(() -> {
      try {
        r.run();
      } catch (final RuntimeException e) {
        throw e;
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private SparseSegmentComponent newComponent(final String name) {
    final DatabaseInternal db = (DatabaseInternal) database;
    try {
      final SparseSegmentComponent c = new SparseSegmentComponent(db, name, db.getDatabasePath() + "/" + name,
          ComponentFile.MODE.READ_WRITE, SparseSegmentComponent.DEFAULT_PAGE_SIZE);
      ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);
      return c;
    } catch (final IOException e) {
      throw new RuntimeException("failed to create sparse segment component '" + name + "'", e);
    }
  }

  private PaginatedSegmentReader buildSegment(final String name, final long segmentId,
      final Map<RID, Map<Integer, Float>> docs) throws IOException {
    final TreeMap<Integer, TreeMap<RID, Float>> byDim = new TreeMap<>();
    for (final var doc : docs.entrySet()) {
      for (final var dw : doc.getValue().entrySet())
        byDim.computeIfAbsent(dw.getKey(), k -> new TreeMap<>()).put(doc.getKey(), dw.getValue());
    }
    final SparseSegmentComponent c = newComponent(name);
    try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, EXACT_PARAMS)) {
      b.setSegmentId(segmentId);
      for (final var dim : byDim.entrySet()) {
        b.startDim(dim.getKey());
        for (final var p : dim.getValue().entrySet())
          b.appendPosting(p.getKey(), p.getValue());
        b.endDim();
      }
      b.finish();
    }
    return new PaginatedSegmentReader(c);
  }
}
