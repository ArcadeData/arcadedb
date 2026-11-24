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

import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionIndexContext.IndexKey;
import com.arcadedb.database.TransactionIndexContext.IndexKey.IndexKeyOperation;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The pending side of a sparse search ranks equal scores by RID, the order the committed side ({@link BmwScorer}) gives: the
 * order must not be the iteration order of a map keyed by RID, which is whatever {@code RID.hashCode()} makes it, and at the
 * boundary of the top-k it decides WHICH of the tied records is returned at all.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class SparseTransactionOverlayTieBreakTest {
  private static final int DIM = 3;

  @Test
  void equalScoresAreRankedByRidAscending() {
    final List<RID> rids = tiedRids();

    final SparseTransactionOverlay overlay = overlayOf(rids, 0.5f);
    final List<RidScore> top = overlay.topK(new int[] { DIM }, new float[] { 1f }, null, rids.size());

    final List<RID> expected = new ArrayList<>(rids);
    Collections.sort(expected);
    assertThat(top).extracting(RidScore::rid).containsExactlyElementsOf(expected);
  }

  @Test
  void theTopKBoundaryKeepsTheLowestRidsOfATie() {
    final List<RID> rids = tiedRids();

    final SparseTransactionOverlay overlay = overlayOf(rids, 0.5f);
    final List<RidScore> top = overlay.topK(new int[] { DIM }, new float[] { 1f }, null, 5);

    final List<RID> expected = new ArrayList<>(rids);
    Collections.sort(expected);
    assertThat(top).extracting(RidScore::rid).containsExactlyElementsOf(expected.subList(0, 5));
  }

  @Test
  void aBetterScoreStillWinsOverALowerRid() {
    final List<RID> rids = tiedRids();
    final List<IndexKey> lane = lane(rids, 0.5f);
    // the highest RID of all scores better than every other
    final RID best = Collections.max(rids);
    lane.add(posting(IndexKeyOperation.ADD, best, 0.9f));

    final SparseTransactionOverlay overlay = SparseTransactionOverlay.of(lane);
    final List<RidScore> top = overlay.topK(new int[] { DIM }, new float[] { 1f }, null, 3);

    assertThat(top).hasSize(3);
    assertThat(top.getFirst().rid()).isEqualTo(best);
    final List<RID> expected = new ArrayList<>(rids);
    expected.remove(best);
    Collections.sort(expected);
    assertThat(top.get(1).rid()).isEqualTo(expected.get(0));
    assertThat(top.get(2).rid()).isEqualTo(expected.get(1));
  }

  /** 40 RIDs over 4 buckets in a scrambled order, so neither the insertion order nor the bucket order is the RID order. */
  private static List<RID> tiedRids() {
    final List<RID> rids = new ArrayList<>();
    for (int bucket = 10; bucket < 14; bucket++)
      for (int i = 0; i < 10; i++)
        rids.add(new RID(bucket, i * 2048L + 3 + i));
    Collections.shuffle(rids, new Random(42));
    return rids;
  }

  private static SparseTransactionOverlay overlayOf(final List<RID> rids, final float weight) {
    return SparseTransactionOverlay.of(lane(rids, weight));
  }

  private static List<IndexKey> lane(final List<RID> rids, final float weight) {
    final List<IndexKey> lane = new ArrayList<>(rids.size());
    for (final RID rid : rids)
      lane.add(posting(IndexKeyOperation.ADD, rid, weight));
    return lane;
  }

  private static IndexKey posting(final IndexKeyOperation operation, final RID rid, final float weight) {
    return new IndexKey(false, operation, new Object[] { new SparsePostingReplayKey(DIM, rid, weight) }, rid);
  }
}
