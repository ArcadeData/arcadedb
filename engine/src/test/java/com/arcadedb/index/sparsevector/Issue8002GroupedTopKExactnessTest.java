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
import com.arcadedb.index.vector.GroupAdmissionState;
import com.arcadedb.schema.LocalSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #8002 at the scorer level: {@link BmwScorer#topKGrouped} must return exactly what the
 * {@code groupBy} / {@code groupSize} rule gives when applied in score order - the {@code limit} highest-peaking
 * groups, each with its {@code groupSize} best members - whatever order the ascending-RID traversal meets the rows in,
 * and however far the pruning threshold has risen by then. {@link BmwScorer#topKForGroups} must return every group's
 * best members above the floor.
 * <p>
 * Randomised against a brute-force reference over corpora large enough to span many posting blocks, so the block-max
 * skip and the essential/non-essential split both engage. Each corpus plants groups whose single best row sits far
 * above their others and late in RID order - the shape the issue reports.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8002GroupedTopKExactnessTest extends TestHelper {
  private static final SegmentParameters EXACT_PARAMS = SegmentParameters.builder()
      .weightQuantization(WeightQuantization.FP32)
      .build();
  private static final int               DIMS         = 4;
  private static final int               DOCS         = 1_500;
  private static final int               TRIALS       = 40;

  @Test
  void groupedTopKMatchesTheScoreOrderedAdmissionRule() {
    final Random rnd = new Random(8002L);
    for (int trial = 0; trial < TRIALS; trial++) {
      final Corpus corpus = corpus(rnd, "seg-8002-g-" + trial);
      final int limit = 1 + rnd.nextInt(5);
      final int groupSize = 1 + rnd.nextInt(4);

      final List<RidScore> expected = new ArrayList<>();
      final GroupAdmissionState admission = new GroupAdmissionState(limit, groupSize);
      for (final RidScore row : corpus.ranked) {
        if (admission.isFull())
          break;
        if (admission.admit(corpus.groupOf.get(row.rid())))
          expected.add(row);
      }

      final int t = trial;
      inTx(() -> {
        final List<RidScore> got = BmwScorer.topKGrouped(corpus.queryDims, corpus.queryWeights, corpus::open, limit, groupSize,
            corpus.groupOf::get, null);
        assertThat(rids(got)).as("trial %d, limit %d, groupSize %d", t, limit, groupSize).isEqualTo(rids(expected));
      });
    }
  }

  @Test
  void topKForGroupsReturnsEachGroupsBestMembersAboveTheFloor() {
    final Random rnd = new Random(80021L);
    for (int trial = 0; trial < TRIALS; trial++) {
      final Corpus corpus = corpus(rnd, "seg-8002-f-" + trial);
      final int groupSize = 1 + rnd.nextInt(4);
      final Set<Object> keys = new HashSet<>();
      final int wanted = 1 + rnd.nextInt(4);
      for (int i = 0; i < wanted; i++)
        keys.add("g" + rnd.nextInt(corpus.groups));
      // Half the trials with no floor, half with one somewhere inside the score range.
      final float floor = rnd.nextBoolean() ? Float.NEGATIVE_INFINITY : corpus.ranked.get(rnd.nextInt(corpus.ranked.size())).score();

      final List<RidScore> expected = new ArrayList<>();
      final Map<Object, Integer> perGroup = new HashMap<>();
      for (final RidScore row : corpus.ranked) {
        final Object key = corpus.groupOf.get(row.rid());
        if (row.score() > floor && keys.contains(key) && perGroup.merge(key, 1, Integer::sum) <= groupSize)
          expected.add(row);
      }

      final int t = trial;
      inTx(() -> {
        final List<RidScore> got = BmwScorer.topKForGroups(corpus.queryDims, corpus.queryWeights, corpus::open, keys,
            groupSize, floor, corpus.groupOf::get, null, null);
        assertThat(rids(got)).as("trial %d, keys %s, groupSize %d, floor %s", t, keys, groupSize, floor)
            .isEqualTo(rids(expected));
      });
    }
  }

  // ---------- corpus ----------

  private final class Corpus {
    int[]                  queryDims;
    float[]                queryWeights;
    int                    groups;
    Map<RID, String>       groupOf = new HashMap<>();
    /** Every scored row, best first, ties by RID: the order the admission rule walks. */
    List<RidScore>         ranked  = new ArrayList<>();
    PaginatedSegmentReader reader;

    DimCursor[] open() throws IOException {
      final DimCursor[] cursors = new DimCursor[queryDims.length];
      for (int i = 0; i < queryDims.length; i++)
        cursors[i] = new DimCursor(queryDims[i], List.of(reader.openCursor(queryDims[i])));
      return cursors;
    }
  }

  private Corpus corpus(final Random rnd, final String segmentName) {
    final Corpus c = new Corpus();
    c.groups = 3 + rnd.nextInt(30);
    c.queryDims = new int[DIMS];
    c.queryWeights = new float[DIMS];
    for (int d = 0; d < DIMS; d++) {
      c.queryDims[d] = d;
      c.queryWeights[d] = 0.1f + rnd.nextFloat();
    }

    final TreeMap<RID, Map<Integer, Float>> docs = new TreeMap<>();
    for (int i = 0; i < DOCS; i++) {
      final RID rid = new RID(0, i);
      final Map<Integer, Float> weights = new HashMap<>();
      // Every dim present at least once, so each query dim has a posting list to open.
      if (i < DIMS)
        weights.put(i, rnd.nextFloat());
      final int nnz = 1 + rnd.nextInt(3);
      for (int z = 0; z < nnz; z++)
        weights.put(rnd.nextInt(DIMS), rnd.nextFloat() * 0.5f);
      // A late row far above everything else: its group's peak, met only after its other rows.
      if (i > DOCS / 2 && rnd.nextInt(100) == 0)
        weights.put(rnd.nextInt(DIMS), 5.0f + rnd.nextFloat());
      docs.put(rid, weights);
      c.groupOf.put(rid, "g" + rnd.nextInt(c.groups));
    }

    for (final Map.Entry<RID, Map<Integer, Float>> doc : docs.entrySet()) {
      float score = 0.0f;
      for (final Map.Entry<Integer, Float> w : doc.getValue().entrySet())
        score += c.queryWeights[w.getKey()] * w.getValue();
      c.ranked.add(new RidScore(doc.getKey(), score));
    }
    c.ranked.sort(BmwScorer.BY_SCORE_DESC);

    inTx(() -> c.reader = buildSegment(segmentName, docs));
    return c;
  }

  private static List<RID> rids(final List<RidScore> rows) {
    final List<RID> out = new ArrayList<>(rows.size());
    for (final RidScore r : rows)
      out.add(r.rid());
    return out;
  }

  // ---------- helpers ----------

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

  private PaginatedSegmentReader buildSegment(final String name, final Map<RID, Map<Integer, Float>> docs) throws IOException {
    final TreeMap<Integer, TreeMap<RID, Float>> byDim = new TreeMap<>();
    for (final var doc : docs.entrySet())
      for (final var dw : doc.getValue().entrySet())
        byDim.computeIfAbsent(dw.getKey(), k -> new TreeMap<>()).put(doc.getKey(), dw.getValue());

    final DatabaseInternal db = (DatabaseInternal) database;
    final SparseSegmentComponent c = new SparseSegmentComponent(db, name, db.getDatabasePath() + "/" + name,
        ComponentFile.MODE.READ_WRITE, SparseSegmentComponent.DEFAULT_PAGE_SIZE);
    ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);
    try (final SparseSegmentBuilder b = new SparseSegmentBuilder(c, EXACT_PARAMS)) {
      b.setSegmentId(1L);
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
