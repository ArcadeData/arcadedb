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
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

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
 * Issue #8553: the Block-Max MaxScore traversal orders its posting cursors by one packed {@code long} per RID instead of
 * comparing {@code (bucketId, position)} pairs. That only holds if the packing preserves {@link SparseSegmentBuilder#compareRid}
 * exactly - including at the edges of the packed ranges, where a wrong shift or a sign bit would reorder postings
 * silently - and if a RID the packing cannot hold is neither refused nor misordered: file ids only grow, so a long-lived
 * database can legitimately own bucket ids of any size. This pins the order itself, then the answers of traversals on
 * the packed path, relative to a large base bucket id, and forced onto the RID-comparison fallback.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8553PackedRidKeyTest extends TestHelper {

  private static final long MAX_POSITION   = SparseSegmentBuilder.MAX_PACKED_POSITION;
  private static final int  MAX_OFFSET     = SparseSegmentBuilder.MAX_PACKED_BUCKET_OFFSET;
  private static final int  BIG_BUCKET_ID  = 5_000_000;

  @Test
  void packedKeysOrderExactlyLikeCompareRid() {
    final Random rnd = new Random(8553L);
    for (final int base : new int[] { 0, 7, BIG_BUCKET_ID }) {
      final int[] offsets = { 0, 1, 2, 17, MAX_OFFSET - 1, MAX_OFFSET };
      final long[] positions = { 0L, 1L, 2L, 1L << 31, (1L << 41) + 7, MAX_POSITION - 1, MAX_POSITION };
      final List<Integer> bucketSamples = new ArrayList<>();
      final List<Long> positionSamples = new ArrayList<>();
      for (final int o : offsets)
        for (final long p : positions) {
          bucketSamples.add(base + o);
          positionSamples.add(p);
        }
      for (int i = 0; i < 1_000; i++) {
        bucketSamples.add(base + rnd.nextInt(MAX_OFFSET + 1));
        positionSamples.add(rnd.nextLong() & MAX_POSITION);
      }

      for (int i = 0; i < bucketSamples.size(); i++)
        for (int j = 0; j < bucketSamples.size(); j += 1 + (i % 7)) {
          final int b1 = bucketSamples.get(i);
          final long p1 = positionSamples.get(i);
          final int b2 = bucketSamples.get(j);
          final long p2 = positionSamples.get(j);
          final long k1 = SparseSegmentBuilder.packRid(b1, p1, base);
          final long k2 = SparseSegmentBuilder.packRid(b2, p2, base);
          assertThat(Integer.signum(Long.compare(k1, k2)))
              .as("(%d,%d) vs (%d,%d) on base %d", b1, p1, b2, p2, base)
              .isEqualTo(Integer.signum(SparseSegmentBuilder.compareRid(b1, p1, b2, p2)));
          assertThat(k1).isGreaterThanOrEqualTo(0L).isLessThan(SparseSegmentBuilder.UNPACKABLE);
          assertThat(SparseSegmentBuilder.unpackBucketId(k1, base)).isEqualTo(b1);
          assertThat(SparseSegmentBuilder.unpackPosition(k1)).isEqualTo(p1);
        }
    }

    // An exhausted cursor sorts first, as compareRid puts bucket -1 first.
    assertThat(SparseSegmentBuilder.packRid(-1, -1L, 0)).isEqualTo(-1L);
    // What does not fit is marked, never truncated into a wrong order.
    assertThat(SparseSegmentBuilder.packRid(MAX_OFFSET + 1, 0L, 0)).isEqualTo(SparseSegmentBuilder.UNPACKABLE);
    assertThat(SparseSegmentBuilder.packRid(3, MAX_POSITION + 1, 0)).isEqualTo(SparseSegmentBuilder.UNPACKABLE);
    assertThat(SparseSegmentBuilder.packRid(Integer.MAX_VALUE, 0L, 0)).isEqualTo(SparseSegmentBuilder.UNPACKABLE);
  }

  @Test
  void aBoundBeyondThePackedRangeMapsToTheFirstKeyNotBeforeIt() {
    assertThat(SparseSegmentBuilder.packRidCeiling(5, MAX_POSITION + 1, 0)).isEqualTo(SparseSegmentBuilder.packRid(6, 0L, 0));
    assertThat(SparseSegmentBuilder.packRidCeiling(5, Long.MAX_VALUE, 0)).isEqualTo(SparseSegmentBuilder.packRid(6, 0L, 0));
    assertThat(SparseSegmentBuilder.packRidCeiling(MAX_OFFSET + 1, 0L, 0)).isEqualTo(SparseSegmentBuilder.UNPACKABLE);
    assertThat(SparseSegmentBuilder.packRidCeiling(MAX_OFFSET, MAX_POSITION + 1, 0))
        .isGreaterThan(SparseSegmentBuilder.packRid(MAX_OFFSET, MAX_POSITION, 0))
        .isLessThan(SparseSegmentBuilder.UNPACKABLE);
    // A bound below the base is before every key the traversal can see.
    assertThat(SparseSegmentBuilder.packRidCeiling(3, 99L, 10)).isEqualTo(0L);
    assertThat(SparseSegmentBuilder.packRidCeiling(9, 12L, 0)).isEqualTo(SparseSegmentBuilder.packRid(9, 12L, 0));
  }

  /** Several buckets, positions at the very top of the range: the packed path, checked against an exact scan. */
  @Test
  void topKAcrossBucketsAndHighPositionsMatchesAnExactScan() throws Exception {
    final long fallbacksBefore = BmwScorer.WIDE_FALLBACKS.get();
    checkAgainstExactScan("Issue8553Packed", new int[] { 0, 3, 11, MAX_OFFSET }, false);
    assertThat(BmwScorer.WIDE_FALLBACKS.get())
        .as("RIDs inside the packed range must not leave the packed path")
        .isEqualTo(fallbacksBefore);
  }

  /** A bucket id far above 2^20 is not special: keys are packed relative to the traversal's smallest bucket id. */
  @Test
  void aLargeBucketIdIsAcceptedAndStaysOnThePackedPath() throws Exception {
    final long fallbacksBefore = BmwScorer.WIDE_FALLBACKS.get();
    checkAgainstExactScan("Issue8553BigBucket", new int[] { BIG_BUCKET_ID, BIG_BUCKET_ID + 2 }, false);
    assertThat(BmwScorer.WIDE_FALLBACKS.get()).isEqualTo(fallbacksBefore);
  }

  /**
   * Buckets further apart than the packed offset can span, and positions past 2^42: nothing is refused, and the
   * traversal finishes on RID comparisons with the same answers.
   */
  @Test
  void ridsThePackingCannotHoldFallBackToAnExactTraversal() throws Exception {
    final long fallbacksBefore = BmwScorer.WIDE_FALLBACKS.get();
    checkAgainstExactScan("Issue8553Wide", new int[] { 0, 2, MAX_OFFSET + 1, BIG_BUCKET_ID }, true);
    assertThat(BmwScorer.WIDE_FALLBACKS.get())
        .as("the corpus must actually force the fallback, or this test proves nothing about it")
        .isGreaterThan(fallbacksBefore);
  }

  private void checkAgainstExactScan(final String indexName, final int[] buckets, final boolean hugePositions)
      throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final SegmentParameters exact = SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build();
    final Random rnd = new Random(0x8553L);
    final int dims = 24;
    final Map<RID, Map<Integer, Float>> docs = new HashMap<>();
    for (final int bucket : buckets)
      for (int i = 0; i < 150; i++) {
        // Half the documents crowd the top of the position range (or pass it), half sit low: both edges of the packing
        // are exercised in the same traversal, and the bucket boundaries between them too.
        final long high = hugePositions && i % 4 == 0 ? Long.MAX_VALUE / 2 - i : MAX_POSITION - i;
        final long position = i % 2 == 0 ? high : (long) i * 1_000_003L;
        final Map<Integer, Float> doc = new TreeMap<>();
        while (doc.size() < 6)
          doc.put(rnd.nextInt(dims), 0.05f + rnd.nextFloat());
        docs.put(new RID(bucket, position), doc);
      }

    try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine(db, indexName, exact, 100_000L)) {
      int written = 0;
      for (final Map.Entry<RID, Map<Integer, Float>> doc : docs.entrySet()) {
        for (final Map.Entry<Integer, Float> dw : doc.getValue().entrySet())
          engine.put(dw.getKey(), doc.getKey(), dw.getValue());
        // Flush halfway, so half the corpus is answered from a sealed segment and half from the memtable.
        if (++written == docs.size() / 2)
          engine.flush();
      }

      for (int q = 0; q < 40; q++) {
        final int qDims = 2 + rnd.nextInt(8);
        final int[] queryDims = new int[qDims];
        final float[] queryWeights = new float[qDims];
        final Set<Integer> seen = new HashSet<>();
        for (int filled = 0; filled < qDims; ) {
          final int d = rnd.nextInt(dims);
          if (seen.add(d)) {
            queryDims[filled] = d;
            queryWeights[filled++] = 0.1f + rnd.nextFloat();
          }
        }
        final int k = 1 + rnd.nextInt(25);

        final List<RidScore> got = engine.topK(queryDims, queryWeights, k);
        final List<RidScore> expected = exactTopK(docs, queryDims, queryWeights, k);
        assertThat(got).hasSize(expected.size());
        for (int i = 0; i < expected.size(); i++) {
          assertThat(got.get(i).rid()).as("query %d rank %d", q, i).isEqualTo(expected.get(i).rid());
          assertThat(got.get(i).score()).isCloseTo(expected.get(i).score(), Offset.offset(1e-4f));
        }
      }
    }
  }

  private static List<RidScore> exactTopK(final Map<RID, Map<Integer, Float>> docs, final int[] queryDims,
      final float[] queryWeights, final int k) {
    final List<RidScore> all = new ArrayList<>();
    for (final Map.Entry<RID, Map<Integer, Float>> doc : docs.entrySet()) {
      float score = 0f;
      boolean matched = false;
      for (int i = 0; i < queryDims.length; i++) {
        final Float w = doc.getValue().get(queryDims[i]);
        if (w != null) {
          score += queryWeights[i] * w;
          matched = true;
        }
      }
      if (matched)
        all.add(new RidScore(doc.getKey(), score));
    }
    all.sort(BmwScorer.BY_SCORE_DESC);
    return all.size() <= k ? all : new ArrayList<>(all.subList(0, k));
  }
}
