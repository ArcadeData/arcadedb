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
package com.arcadedb.graph.olap;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #7884: the ADD side of the post-compaction delta re-application.
 * <p>
 * Issue #7042 gave the DELETION side a multiplicity comparison - it spends an exclusion budget only for as many
 * occurrences of a pair as the freshly scanned base still carries. The add side kept dedupping by PRESENCE: it
 * probed {@code hasForwardEdge} and skipped the buffered add whenever the fresh base held ANY occurrence of the
 * pair. With parallel edges that drops a real edge: one occurrence of {@code s -> t} in the fresh run and a buffered
 * add of a SECOND one, and the second exists in neither the fresh base nor the overlay until the next compaction.
 * <p>
 * The symmetry with the deletion side is exact. There, the scan SWALLOWING a change led to an over-spent budget and
 * the fix was to subtract {@code forwardEdgeCount} from the pre-compaction count; here the scan MISSING a change
 * leads to an under-count and the same subtraction, read the other way round, is the fix.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DeltaOverlayCompactionAddDedupTest {
  private static final String EDGE_TYPE = "Knows";

  /** Builds a single-bucket mapping where global ids match positions 0..count-1. */
  private NodeIdMapping baseMappingWith(final int count) {
    final NodeIdMapping mapping = new NodeIdMapping(1);
    final int bucketIdx = mapping.registerBucket(1, "V", count);
    for (int i = 0; i < count; i++)
      mapping.addNode(bucketIdx, i);
    mapping.compact();
    return mapping;
  }

  private RID rid(final int position) {
    return new RID(1, position);
  }

  /** Builds a base CSR over {@code nodeCount} nodes holding {@code count} parallel forward edges {@code src -> tgt}. */
  private Map<String, CSRAdjacencyIndex> freshBaseWith(final int nodeCount, final int src, final int tgt,
      final int count) {
    final int[] fwdOffsets = new int[nodeCount + 1];
    final int[] bwdOffsets = new int[nodeCount + 1];
    for (int i = 0; i <= nodeCount; i++) {
      fwdOffsets[i] = i > src ? count : 0;
      bwdOffsets[i] = i > tgt ? count : 0;
    }
    final int[] fwdNeighbors = new int[count];
    final int[] bwdNeighbors = new int[count];
    Arrays.fill(fwdNeighbors, tgt);
    Arrays.fill(bwdNeighbors, src);
    return Map.of(EDGE_TYPE, new CSRAdjacencyIndex(fwdOffsets, fwdNeighbors, bwdOffsets, bwdNeighbors, nodeCount, count));
  }

  /** The pre-compaction multiplicity of the one pair these fixtures use. */
  private DeltaOverlay.PreCompactionPairCount preCompactionOccurrences(final int occurrences) {
    return (edgeType, source, target) -> occurrences;
  }

  private TxDelta additionOf(final RID edgeRid) {
    final TxDelta delta = new TxDelta();
    delta.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid));
    return delta;
  }

  /**
   * The issue's own repro. E1 (0-&gt;1) is in the base at compaction start; a second parallel edge E2 is added during
   * the rebuild and the scan crosses node 0's bucket BEFORE that commit, so the fresh base carries only E1. The
   * presence probe sees the pair and drops E2 on the floor.
   */
  @Test
  void aBufferedSecondParallelEdgeTheFreshScanMissedIsKept() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final Map<String, CSRAdjacencyIndex> freshBase = freshBaseWith(2, 0, 1, 1);

    // Without the pre-compaction reference nothing can be shown captured, so the presence probe stands - the
    // pre-fix behaviour, and the exact undercount that loses E2.
    final DeltaOverlay blind = new DeltaOverlay(mapping.size()).merge(additionOf(rid(11)), mapping, freshBase);
    assertThat(blind.getAddedOutNeighbors(0, EDGE_TYPE))
        .as("dedup by presence drops an addition the fresh run never captured").isEmpty();

    // With it, the pair's multiplicity has NOT risen, so nothing was captured and the addition belongs in the overlay.
    final DeltaOverlay merged = new DeltaOverlay(mapping.size())
        .merge(additionOf(rid(11)), mapping, freshBase, preCompactionOccurrences(1));
    assertThat(merged.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(1);
    assertThat(merged.getAddedInNeighbors(1, EDGE_TYPE)).containsExactly(0);
    assertThat(merged.countAbsorbedAdditions(EDGE_TYPE, 0, 1)).isZero();
    assertThat(merged.getDeltaEdgeCount()).isEqualTo(1);
  }

  /**
   * The other half of the same race, and the case #4588 is about: the scan crossed the bucket AFTER the add
   * committed, so the fresh base holds both edges and the buffered add must be skipped. Same fresh multiplicity as
   * the pair in the test above ends at, which is why the pre-compaction count is the discriminator.
   */
  @Test
  void aBufferedAdditionTheFreshScanCapturedIsStillSkipped() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final Map<String, CSRAdjacencyIndex> freshBase = freshBaseWith(2, 0, 1, 2);

    final DeltaOverlay merged = new DeltaOverlay(mapping.size())
        .merge(additionOf(rid(11)), mapping, freshBase, preCompactionOccurrences(1));

    assertThat(merged.getAddedOutNeighbors(0, EDGE_TYPE)).isEmpty();
    assertThat(merged.countAbsorbedAdditions(EDGE_TYPE, 0, 1)).isEqualTo(1);
    assertThat(merged.getDeltaEdgeCount()).isZero();
  }

  /**
   * Only the additions the scan actually captured are skipped, and they are the PREFIX of the pair's buffered
   * additions: one edge at compaction start, three more added during the rebuild, two of them captured by the scan
   * (the fresh run holds three). The third committed behind the scan and has to reach the overlay.
   */
  @Test
  void onlyTheAdditionsTheScanCapturedAreSkippedAndTheRestReachTheOverlay() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final Map<String, CSRAdjacencyIndex> freshBase = freshBaseWith(2, 0, 1, 3);
    final DeltaOverlay.PreCompactionPairCount preCount = preCompactionOccurrences(1);

    DeltaOverlay overlay = new DeltaOverlay(mapping.size());
    for (final int edgePosition : new int[] { 11, 12, 13 })
      overlay = overlay.merge(additionOf(rid(edgePosition)), mapping, freshBase, preCount);

    assertThat(overlay.countAbsorbedAdditions(EDGE_TYPE, 0, 1)).isEqualTo(2);
    assertThat(overlay.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(1);
    assertThat(overlay.getDeltaEdgeCount()).isEqualTo(1);
  }

  /**
   * A first edge for a pair the fresh base did capture is still skipped: the multiplicity rose from zero to one,
   * which is exactly the #4588 case the probe was written for, and it must not regress into a duplicate neighbour.
   */
  @Test
  void theFirstEdgeOfAPairTheFreshScanCapturedIsStillSkipped() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final Map<String, CSRAdjacencyIndex> freshBase = freshBaseWith(2, 0, 1, 1);

    final DeltaOverlay merged = new DeltaOverlay(mapping.size())
        .merge(additionOf(rid(10)), mapping, freshBase, preCompactionOccurrences(0));

    assertThat(merged.getAddedOutNeighbors(0, EDGE_TYPE)).isEmpty();
    assertThat(merged.countAbsorbedAdditions(EDGE_TYPE, 0, 1)).isEqualTo(1);
    assertThat(merged.getDeltaEdgeCount()).isZero();
  }

  /**
   * Delete then re-add of the pair across two buffered deltas, with the pre-compaction reference in hand: the re-add
   * is masked as deleted, so it is reinstated whatever the multiplicities say and spends no captured slot. That is
   * the behaviour #4588 put the masked branch there for, and the multiplicity comparison must not take it away.
   */
  @Test
  void anAdditionMaskedAsDeletedIsStillReinstated() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final Map<String, CSRAdjacencyIndex> freshBase = freshBaseWith(2, 0, 1, 1);
    final DeltaOverlay.PreCompactionPairCount preCount = preCompactionOccurrences(1);

    final TxDelta deletion = new TxDelta();
    deletion.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), rid(10)));
    final DeltaOverlay afterDelete = new DeltaOverlay(mapping.size()).merge(deletion, mapping, freshBase, preCount);
    assertThat(afterDelete.isEdgeDeleted(EDGE_TYPE, 0, 1)).isTrue();

    final DeltaOverlay afterReAdd = afterDelete.merge(additionOf(rid(10)), mapping, freshBase, preCount);

    assertThat(afterReAdd.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(1);
    assertThat(afterReAdd.countAbsorbedAdditions(EDGE_TYPE, 0, 1)).isZero();
  }

  /**
   * Issue #6777's RID reuse on the pair the capture budget is being counted for. E1 (0-&gt;1) is deleted during the
   * rebuild and its slot reused by E2 on the SAME pair; the scan crosses the bucket after both, so the fresh run
   * holds exactly one occurrence - E2's. Neither the deletion nor the addition may be treated as already reflected:
   * the pair's multiplicity is unchanged, so the deletion budgets and the addition reaches the overlay, and the pair
   * is reported exactly once.
   */
  @Test
  void aReusedRidOnTheSamePairIsCountedAsTheDistinctEdgeItIs() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final Map<String, CSRAdjacencyIndex> freshBase = freshBaseWith(2, 0, 1, 1);
    final DeltaOverlay.PreCompactionPairCount preCount = preCompactionOccurrences(1);

    final TxDelta deletion = new TxDelta();
    deletion.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), rid(10)));
    DeltaOverlay overlay = new DeltaOverlay(mapping.size()).merge(deletion, mapping, freshBase, preCount);
    overlay = overlay.merge(additionOf(rid(10)), mapping, freshBase, preCount);

    assertThat(overlay.countDeletedEdges(EDGE_TYPE, 0, 1)).isEqualTo(1);
    assertThat(overlay.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(1);
    assertThat(overlay.countAbsorbedAdditions(EDGE_TYPE, 0, 1)).isZero();
  }
}
