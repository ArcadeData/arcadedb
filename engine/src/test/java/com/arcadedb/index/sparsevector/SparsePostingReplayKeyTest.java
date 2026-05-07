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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link SparsePostingReplayKey} (issue #4073). The marker replaces the previous
 * shape-sniffing pattern in {@code LSMSparseVectorIndex.put}/{@code remove}; it must be
 * distinguishable from the original {@code (int[], float[])} call shape via {@code instanceof},
 * and its {@code equals}/{@code hashCode}/{@code compareTo} must preserve the dedup-map
 * semantics that {@code TransactionIndexContext} relies on (two puts on the same {@code (dim,
 * rid)} with different weights stay distinct).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SparsePostingReplayKeyTest {

  private static final RID R1 = new RID(12, 100);
  private static final RID R2 = new RID(12, 200);

  @Test
  void recordEqualsAndHashCodeAreFieldwise() {
    final SparsePostingReplayKey a = new SparsePostingReplayKey(5, R1, 0.3f);
    final SparsePostingReplayKey b = new SparsePostingReplayKey(5, R1, 0.3f);
    final SparsePostingReplayKey differentWeight = new SparsePostingReplayKey(5, R1, 0.5f);
    final SparsePostingReplayKey differentRid = new SparsePostingReplayKey(5, R2, 0.3f);
    final SparsePostingReplayKey differentDim = new SparsePostingReplayKey(6, R1, 0.3f);

    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
    assertThat(a).isNotEqualTo(differentWeight);
    assertThat(a).isNotEqualTo(differentRid);
    assertThat(a).isNotEqualTo(differentDim);
  }

  @Test
  void compareToOrdersByDimThenRidThenWeight() {
    final SparsePostingReplayKey a = new SparsePostingReplayKey(5, R1, 0.3f);
    final SparsePostingReplayKey sameDimSameRidLowerWeight = new SparsePostingReplayKey(5, R1, 0.1f);
    final SparsePostingReplayKey sameDimHigherRid = new SparsePostingReplayKey(5, R2, 0.1f);
    final SparsePostingReplayKey higherDim = new SparsePostingReplayKey(6, R1, 0.1f);

    assertThat(a.compareTo(a)).isZero();
    assertThat(a.compareTo(sameDimSameRidLowerWeight)).isPositive();
    assertThat(sameDimSameRidLowerWeight.compareTo(a)).isNegative();
    assertThat(a.compareTo(sameDimHigherRid)).isNegative();
    assertThat(a.compareTo(higherDim)).isNegative();
    assertThat(higherDim.compareTo(a)).isPositive();
  }

  @Test
  void instanceOfDistinguishesMarkerFromOriginalCallShape() {
    // Replay frame: a single-element Object[] holding the typed marker.
    final Object[] replayKeys = new Object[] { new SparsePostingReplayKey(7, R1, 0.42f) };
    // Original frame: parallel arrays of indices and weights, as DocumentIndexer supplies.
    final Object[] originalKeys = new Object[] { new int[] { 7 }, new float[] { 0.42f } };

    assertThat(replayKeys[0]).isInstanceOf(SparsePostingReplayKey.class);
    assertThat(originalKeys[0]).isNotInstanceOf(SparsePostingReplayKey.class);
  }

  /**
   * Pins the {@code TransactionIndexContext} dedup contract for the put-then-remove
   * same-(dim, rid)-same-weight case: the marker collides with itself in the dedup map, so
   * {@code HashMap.put} overwrite leaves whichever IndexKey was put last. The user's
   * last-write-wins intent (delete after insert in the same tx → doc deleted) survives.
   * <p>
   * This is the reviewer-flagged scenario where keying dedup on operation kind would split the
   * two ops into distinct entries and the two-phase commit (REMOVEs first, ADDs second) would
   * REVERSE the user intent: the post-remove ADD would resurrect the doc. The current
   * weight-keyed design relies on this collision to preserve correctness.
   */
  @Test
  void samePutAndRemoveMarkerCollideForLastWriteWins() {
    final SparsePostingReplayKey put = new SparsePostingReplayKey(5, R1, 0.5f);
    final SparsePostingReplayKey rem = new SparsePostingReplayKey(5, R1, 0.5f);
    assertThat(put).isEqualTo(rem);
    assertThat(put.hashCode()).isEqualTo(rem.hashCode());
    // The collision is the dedup-map mechanism that makes "put then remove with same weight"
    // produce a deletion (HashMap.put overwrite of the put's IndexKey by the remove's IndexKey).
    // If a future change adds a tombstone field to the marker, this assertion will break and the
    // change author MUST revisit the put-then-remove preservation in a regression test before
    // shipping.
  }

  /**
   * Companion contract: zero-weight put and remove paths are both filtered before reaching
   * queueOrApply (the wrapper's remove() path skips {@code values[i] == 0.0f}, same for put).
   * This means the put(d, r, 0.0f)-vs-remove(d, r) collision the reviewer flagged is unreachable
   * via the public API today. If a future direct caller (e.g. a rebuild path) bypasses that
   * filter, the dedup-map collision behaviour above still produces last-write-wins.
   */
  @Test
  void zeroWeightMarkerIsLegalAndDedupsLikeNonZero() {
    final SparsePostingReplayKey a = new SparsePostingReplayKey(5, R1, 0.0f);
    final SparsePostingReplayKey b = new SparsePostingReplayKey(5, R1, 0.0f);
    assertThat(a).isEqualTo(b);
    assertThat(a.compareTo(b)).isZero();
  }
}
