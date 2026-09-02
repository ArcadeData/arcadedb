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
 * Regression tests for issue #7042: the DELETION side of the post-compaction delta re-application.
 * <p>
 * Issue #4588 gave the ADD side a probe against the freshly built base CSR, so a buffered add the
 * read-committed scan had already captured is not appended a second time. The deletion side kept re-applying
 * buffered deltas blindly, which is worse than a skewed counter: the per-pair deleted count is an exclusion
 * BUDGET spent against the fresh base run for the pair ({@code copyBaseExcludingDeleted}), so budgeting for an
 * edge that run no longer holds takes the slot of whichever parallel edge DID survive the scan - and that live
 * edge disappears from {@code getNeighborIds}/{@code isConnectedTo} until the next compaction.
 * <p>
 * The discriminator the deletion side needs is the pair's multiplicity at compaction start against its
 * multiplicity in the fresh base: the drop between the two is exactly how many of that pair's buffered
 * deletions the scan already swallowed. The fresh multiplicity alone cannot say - "one occurrence left"
 * describes both "two edges, one deleted behind the scan" and "one edge, nothing deleted yet".
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class DeltaOverlayCompactionDeleteDedupTest {
  private static final String EDGE_TYPE = "Knows";

  /**
   * Builds a single-bucket mapping where global ids match positions 0..count-1.
   */
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

  /**
   * Builds a base CSR over {@code nodeCount} nodes holding {@code count} parallel forward edges
   * {@code src -> tgt} and nothing else.
   */
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

  /**
   * The pre-compaction multiplicity of the one pair these fixtures use.
   */
  private DeltaOverlay.PreCompactionPairCount preCompactionOccurrences(final int occurrences) {
    return (edgeType, source, target) -> occurrences;
  }

  private TxDelta deletionOf(final RID edgeRid) {
    final TxDelta delta = new TxDelta();
    delta.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), edgeRid));
    return delta;
  }

  /**
   * The issue's own repro. Two parallel edges E1 and E2 (0-&gt;1) in the base at compaction start; E1 is
   * deleted during the rebuild and the scan crosses node 0's bucket after that commit, so the fresh base
   * carries only E2. Re-applying E1's deletion spends a budget of 1 against a run of exactly one - and that
   * one is E2, which nobody deleted.
   */
  @Test
  void aDeletionTheFreshScanAlreadySwallowedDoesNotMaskTheSurvivingParallelEdge() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final Map<String, CSRAdjacencyIndex> freshBase = freshBaseWith(2, 0, 1, 1);

    // Without the pre-compaction reference nothing can be shown absorbed, so the deletion is budgeted - the
    // pre-fix behaviour, and the exact over-spend that hides E2.
    final DeltaOverlay blind = new DeltaOverlay(mapping.size()).merge(deletionOf(rid(10)), mapping, freshBase);
    assertThat(blind.countDeletedEdges(EDGE_TYPE, 0, 1))
        .as("blind re-application budgets against a fresh run that no longer holds the deleted edge").isEqualTo(1);

    // With it, the drop from two occurrences to one identifies the deletion as already reflected in the
    // fresh base: no budget, so E2 stays discoverable.
    final DeltaOverlay deduped = new DeltaOverlay(mapping.size())
        .merge(deletionOf(rid(10)), mapping, freshBase, preCompactionOccurrences(2));
    assertThat(deduped.countDeletedEdges(EDGE_TYPE, 0, 1))
        .as("the surviving parallel edge must keep its slot in the fresh base run").isZero();
    assertThat(deduped.countAbsorbedDeletions(EDGE_TYPE, 0, 1)).isEqualTo(1);
    assertThat(deduped.countDeletedOutEdges(0, EDGE_TYPE)).isZero();
    assertThat(deduped.countDeletedInEdges(1, EDGE_TYPE)).isZero();
    assertThat(deduped.getDeltaEdgeCount())
        .as("the overlay is in step with the fresh base for this edge, exactly as for a skipped add").isZero();
  }

  /**
   * The other half of the same race: the scan crossed the bucket BEFORE the delete committed, so the fresh
   * base still holds both edges and the deletion must be budgeted as before. Same fresh multiplicity question
   * as the test above answers the other way, which is why the pre-compaction count is the discriminator.
   */
  @Test
  void aDeletionThatCommittedBehindTheScanStillSpendsItsBudget() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final Map<String, CSRAdjacencyIndex> freshBase = freshBaseWith(2, 0, 1, 2);

    final DeltaOverlay merged = new DeltaOverlay(mapping.size())
        .merge(deletionOf(rid(10)), mapping, freshBase, preCompactionOccurrences(2));

    assertThat(merged.countDeletedEdges(EDGE_TYPE, 0, 1)).isEqualTo(1);
    assertThat(merged.countAbsorbedDeletions(EDGE_TYPE, 0, 1)).isZero();
    assertThat(merged.countDeletedOutEdges(0, EDGE_TYPE)).isEqualTo(1);
    assertThat(merged.getDeltaEdgeCount()).isEqualTo(-1);
  }

  /**
   * The single-edge shape. The over-spend masks nothing in the neighbour list here - there is no surviving
   * occurrence of the pair to take the slot from - but the per-node deleted counts it feeds are what
   * {@code getDegree} subtracts from the fresh base degree, so the node's degree comes out one short.
   */
  @Test
  void aDeletionOfTheOnlyEdgeAlreadyGoneFromTheFreshScanDoesNotUnderCountTheDegree() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final Map<String, CSRAdjacencyIndex> freshBase = freshBaseWith(2, 0, 1, 0);

    final DeltaOverlay merged = new DeltaOverlay(mapping.size())
        .merge(deletionOf(rid(10)), mapping, freshBase, preCompactionOccurrences(1));

    assertThat(merged.countDeletedEdges(EDGE_TYPE, 0, 1)).isZero();
    assertThat(merged.countAbsorbedDeletions(EDGE_TYPE, 0, 1)).isEqualTo(1);
    assertThat(merged.countDeletedOutEdges(0, EDGE_TYPE))
        .as("the fresh base already excludes this edge, so nothing may be subtracted from node 0's degree").isZero();
    assertThat(merged.countDeletedInEdges(1, EDGE_TYPE)).isZero();
    assertThat(merged.getDeltaEdgeCount()).isZero();
  }

  /**
   * Only the deletions the scan actually swallowed are absorbed, and they are the PREFIX of the pair's
   * buffered deletions: three parallel edges at compaction start, one occurrence left in the fresh base, all
   * three deleted. Two were swallowed by the scan, the third committed behind it and still has to spend the
   * one budget the surviving run can pay.
   */
  @Test
  void onlyTheDeletionsTheScanSwallowedAreAbsorbedAndTheRestStillBudget() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final Map<String, CSRAdjacencyIndex> freshBase = freshBaseWith(2, 0, 1, 1);
    final DeltaOverlay.PreCompactionPairCount preCount = preCompactionOccurrences(3);

    DeltaOverlay overlay = new DeltaOverlay(mapping.size());
    for (final int edgePosition : new int[] { 10, 11, 12 })
      overlay = overlay.merge(deletionOf(rid(edgePosition)), mapping, freshBase, preCount);

    assertThat(overlay.countAbsorbedDeletions(EDGE_TYPE, 0, 1)).isEqualTo(2);
    assertThat(overlay.countDeletedEdges(EDGE_TYPE, 0, 1)).isEqualTo(1);
    assertThat(overlay.getDeltaEdgeCount()).isEqualTo(-1);
  }

  /**
   * A deletion replayed under the same identity - across merges, or emitted twice within one TxDelta - is
   * absorbed exactly once, the way the identity dedup #6769 added already absorbs a replayed budget. Counting
   * it twice would eat the absorbable prefix and let the genuine deletion behind it fall through unbudgeted.
   */
  @Test
  void aReplayedDeletionIsAbsorbedOnlyOnce() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final Map<String, CSRAdjacencyIndex> freshBase = freshBaseWith(2, 0, 1, 1);
    final DeltaOverlay.PreCompactionPairCount preCount = preCompactionOccurrences(2);

    final DeltaOverlay once = new DeltaOverlay(mapping.size()).merge(deletionOf(rid(10)), mapping, freshBase, preCount);
    final DeltaOverlay twice = once.merge(deletionOf(rid(10)), mapping, freshBase, preCount);

    assertThat(twice.countAbsorbedDeletions(EDGE_TYPE, 0, 1)).isEqualTo(1);
    assertThat(twice.countDeletedEdges(EDGE_TYPE, 0, 1)).isZero();
    assertThat(twice.getDeltaEdgeCount()).isZero();
  }

  /**
   * The fresh base can also be WIDER than the pre-compaction view - the scan captured a buffered add - and a
   * deletion arriving after it must then be budgeted normally. Guards the arithmetic against reading a
   * negative drop as something absorbable.
   */
  @Test
  void aDeletionIsBudgetedNormallyWhenTheScanCapturedAnAddInstead() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final Map<String, CSRAdjacencyIndex> freshBase = freshBaseWith(2, 0, 1, 2);

    final DeltaOverlay merged = new DeltaOverlay(mapping.size())
        .merge(deletionOf(rid(10)), mapping, freshBase, preCompactionOccurrences(1));

    assertThat(merged.countAbsorbedDeletions(EDGE_TYPE, 0, 1)).isZero();
    assertThat(merged.countDeletedEdges(EDGE_TYPE, 0, 1)).isEqualTo(1);
    assertThat(merged.getDeltaEdgeCount()).isEqualTo(-1);
  }

  /**
   * An edge the overlay window added itself is still withdrawn rather than masked (issue #6775): that branch
   * runs before the absorbed check and must keep doing so, or the add would be cancelled AND a budget spent.
   */
  @Test
  void anAddWithdrawnWithinTheWindowIsStillNotBudgetedNorAbsorbed() {
    final NodeIdMapping mapping = baseMappingWith(2);
    final Map<String, CSRAdjacencyIndex> freshBase = freshBaseWith(2, 0, 1, 0);
    final DeltaOverlay.PreCompactionPairCount preCount = preCompactionOccurrences(1);

    final TxDelta add = new TxDelta();
    add.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, rid(0), rid(1), rid(20)));
    final DeltaOverlay afterAdd = new DeltaOverlay(mapping.size()).merge(add, mapping, freshBase, preCount);
    assertThat(afterAdd.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(1);

    final DeltaOverlay afterDelete = afterAdd.merge(deletionOf(rid(20)), mapping, freshBase, preCount);
    assertThat(afterDelete.getAddedOutNeighbors(0, EDGE_TYPE)).isEmpty();
    assertThat(afterDelete.countDeletedEdges(EDGE_TYPE, 0, 1)).isZero();
    assertThat(afterDelete.countAbsorbedDeletions(EDGE_TYPE, 0, 1)).isZero();
    assertThat(afterDelete.getDeltaEdgeCount()).isZero();
  }
}
