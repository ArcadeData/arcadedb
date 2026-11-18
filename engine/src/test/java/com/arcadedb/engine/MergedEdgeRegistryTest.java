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
package com.arcadedb.engine;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The protocol of {@link MergedEdgeRegistry} (issue #11482), deterministically: what a key is, and how a commit is refused by
 * another one that committed after its begin or is committing now. The concurrent end-to-end behaviour is covered by
 * {@code CypherConcurrentWriteTest.Issue11482ConcurrentMergeRelationship} and {@code ConcurrentMergeBatchesTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MergedEdgeRegistryTest {
  private static final RID A = new RID(3, 10);
  private static final RID B = new RID(3, 11);

  @Test
  void theKeyIsTheUnorderedPairTheTypeAndTheMatchedProperties() {
    final long key = MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e1"));
    assertThat(MergedEdgeRegistry.keyOf(B, A, "R", Map.of("uuid", "e1"))).isEqualTo(key);
    // 0 marks a free way of a set, so it is never a key
    assertThat(key).isNotZero();

    // Entry order does not matter, a whole number is the same key whatever its class
    final Map<String, Object> ordered = new LinkedHashMap<>();
    ordered.put("a", 1);
    ordered.put("b", "x");
    final Map<String, Object> reversed = new LinkedHashMap<>();
    reversed.put("b", "x");
    reversed.put("a", 1L);
    assertThat(MergedEdgeRegistry.keyOf(A, B, "R", ordered)).isEqualTo(MergedEdgeRegistry.keyOf(A, B, "R", reversed));
    assertThat(MergedEdgeRegistry.keyOf(A, B, "R", Map.of("n", 2.0))).isEqualTo(MergedEdgeRegistry.keyOf(A, B, "R", Map.of("n", 2)));

    // No properties and an empty map are the type-only key
    assertThat(MergedEdgeRegistry.keyOf(A, B, "R", null)).isEqualTo(MergedEdgeRegistry.keyOf(A, B, "R", Map.of()));
  }

  @Test
  void aKeyCommittedAfterTheBeginIsRefused() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final long[] keys = { MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e1")) };

    final long firstBegin = registry.getSequence();
    final long secondBegin = registry.getSequence();

    assertThat(registry.reserve(keys, 1, keys, 1, firstBegin)).isTrue();
    registry.committed(keys, 1);

    // Began before that commit: its read could not see the relationship
    assertThat(registry.reserve(keys, 1, keys, 1, secondBegin)).isFalse();
    // Began after it: its read saw the relationship, so a creation of the same key is not refused
    assertThat(registry.reserve(keys, 1, keys, 1, registry.getSequence())).isTrue();
    registry.release(keys, 1);
  }

  @Test
  void aKeyBeingCommittedRefusesTheSecondCommitAndNotTheFirst() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final long[] keys = { MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e1")) };
    final long begin = registry.getSequence();

    assertThat(registry.reserve(keys, 1, keys, 1, begin)).isTrue();
    assertThat(registry.reserve(keys, 1, keys, 1, begin)).isFalse();

    // The first one rolls back: the key is free again, and nothing was committed under it
    registry.release(keys, 1);
    assertThat(registry.reserve(keys, 1, keys, 1, begin)).isTrue();
    registry.release(keys, 1);
  }

  @Test
  void everyKeyIsFreeAgainOnceItsCommitEndsWhicheverWayItEnds() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final long own = MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e1"));
    final long typeOnly = MergedEdgeRegistry.keyOf(A, B, "R", null);
    final long[] marked = { own, typeOnly };

    assertThat(registry.reserve(new long[] { own }, 1, marked, 2, registry.getSequence())).isTrue();
    assertThat(registry.inFlight(own)).isEqualTo(1);
    registry.committed(marked, 2);
    assertThat(registry.inFlight(own)).isZero();
    assertThat(registry.inFlight(typeOnly)).isZero();

    assertThat(registry.reserve(new long[] { own }, 1, marked, 2, registry.getSequence())).isTrue();
    registry.release(marked, 2);
    // A second release of the same keys cannot drive a counter below zero, which would leave the key refusing for good
    registry.release(marked, 2);
    assertThat(registry.inFlight(own)).isZero();
    assertThat(registry.reserve(new long[] { own }, 1, marked, 2, registry.getSequence())).isTrue();
  }

  @Test
  void twoRelationshipsOfOneTransactionSharingAKeyAreNotItsOwnConflict() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final long typeOnly = MergedEdgeRegistry.keyOf(A, B, "R", null);
    final long[] checked = { MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e1")),
        MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e2")) };
    // Both mark the type-only key of the pair
    final long[] marked = { checked[0], typeOnly, checked[1], typeOnly };

    assertThat(registry.reserve(checked, 2, marked, 4, registry.getSequence())).isTrue();
    assertThat(registry.inFlight(typeOnly)).isEqualTo(2);
    registry.committed(marked, 4);
    // Marked twice, committed once per mark: nothing is left in flight
    assertThat(registry.inFlight(typeOnly)).isZero();
  }

  @Test
  void aMatchByTypeAloneIsRefusedByAConcurrentMatchByProperties() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final long typeOnly = MergedEdgeRegistry.keyOf(A, B, "R", null);
    final long withProperties = MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e1"));
    final long begin = registry.getSequence();

    // MERGE (a)-[:R {uuid: 'e1'}]->(b) commits: it checks its own key and marks the type-only one too
    assertThat(registry.reserve(new long[] { withProperties }, 1, new long[] { withProperties, typeOnly }, 2, begin)).isTrue();
    registry.committed(new long[] { withProperties, typeOnly }, 2);

    // MERGE (a)-[:R]->(b), begun before, would have matched it
    assertThat(registry.reserve(new long[] { typeOnly }, 1, new long[] { typeOnly }, 1, begin)).isFalse();
  }

  @Test
  void aMatchByAStrictSubsetOfAnotherMatchsPropertiesIsTheDocumentedLimit() {
    // {uuid: 'e1'} would have matched a relationship created by {uuid: 'e1', kind: 'a'}, but its key is a different one and the
    // larger map marks only its own and the type-only one: the registry does not see it (a unique constraint does)
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final long larger = MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e1", "kind", "a"));
    final long smaller = MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e1"));
    final long typeOnly = MergedEdgeRegistry.keyOf(A, B, "R", null);
    final long begin = registry.getSequence();

    registry.reserve(new long[] { larger }, 1, new long[] { larger, typeOnly }, 2, begin);
    registry.committed(new long[] { larger, typeOnly }, 2);

    assertThat(registry.reserve(new long[] { smaller }, 1, new long[] { smaller, typeOnly }, 2, begin)).isTrue();
  }

  /** Keys whose hashes fall in the same set, found by trying relationships until a set holds {@code count} of them. */
  private static List<Long> keysOfOneSet(final int count) {
    final Map<Integer, List<Long>> bySet = new HashMap<>();
    for (int i = 0; i < 2_000_000; i++) {
      final long key = MergedEdgeRegistry.keyOf(A, new RID(4, i), "R", null);
      final List<Long> inSet = bySet.computeIfAbsent((int) (key & (MergedEdgeRegistry.SETS - 1)), s -> new ArrayList<>());
      inSet.add(key);
      if (inSet.size() == count)
        return inSet;
    }
    throw new AssertionError("no set holds " + count + " of the first two million keys");
  }

  /**
   * The case that refused every pair of concurrent bulk loads: two relationships that merely hash alike. The earlier table kept
   * one counter per hash bucket, so a commit of one refused a transaction that began before and checked the other.
   */
  @Test
  void keysThatShareASetDoNotRefuseEachOther() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final List<Long> sameSet = keysOfOneSet(2);
    final long[] committing = { sameSet.get(0) };
    final long[] checking = { sameSet.get(1) };
    final long begin = registry.getSequence();

    assertThat(registry.reserve(committing, 1, committing, 1, begin)).isTrue();
    // Being written right now, and then committed: neither refuses the other key of the set
    assertThat(registry.reserve(checking, 1, checking, 1, begin)).isTrue();
    registry.release(checking, 1);
    registry.committed(committing, 1);
    assertThat(registry.reserve(checking, 1, checking, 1, begin)).isTrue();
    registry.release(checking, 1);
  }

  /** The cost of a bounded table: a transaction that outlived the entry a full set dropped cannot tell it was not its key. */
  @Test
  void aSetThatDroppedAnEntryRefusesATransactionThatBeganBeforeIt() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final List<Long> sameSet = keysOfOneSet(MergedEdgeRegistry.WAYS + 2);
    final long beganBefore = registry.getSequence();

    // WAYS + 1 keys committed one by one into a set of WAYS entries: the first one is dropped for the last
    for (int i = 0; i <= MergedEdgeRegistry.WAYS; i++) {
      final long[] one = { sameSet.get(i) };
      assertThat(registry.reserve(one, 1, one, 1, registry.getSequence())).isTrue();
      registry.committed(one, 1);
    }

    final long[] unrelated = { sameSet.get(MergedEdgeRegistry.WAYS + 1) };
    assertThat(registry.reserve(unrelated, 1, unrelated, 1, beganBefore)).isFalse();
    // One that began after the entry was dropped reads the committed state, so nothing is in doubt
    assertThat(registry.reserve(unrelated, 1, unrelated, 1, registry.getSequence())).isTrue();
    registry.release(unrelated, 1);
  }

  @Test
  void aKeyBeingWrittenIsNeverDroppedAndAFailedReservationLeavesNothingMarked() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final List<Long> sameSet = keysOfOneSet(MergedEdgeRegistry.WAYS + 1);
    final long[] filling = new long[MergedEdgeRegistry.WAYS];
    for (int i = 0; i < filling.length; i++)
      filling[i] = sameSet.get(i);
    assertThat(registry.reserve(new long[0], 0, filling, filling.length, registry.getSequence())).isTrue();

    // Every way of the set is being written: no room for one more, and the key this call marked first is taken back
    long elsewhere = 0;
    for (int i = 0; elsewhere == 0; i++) {
      final long candidate = MergedEdgeRegistry.keyOf(A, new RID(5, i), "ELSEWHERE", null);
      if ((candidate & (MergedEdgeRegistry.SETS - 1)) != (sameSet.get(0) & (MergedEdgeRegistry.SETS - 1)))
        elsewhere = candidate;
    }
    final long[] marked = { elsewhere, sameSet.get(MergedEdgeRegistry.WAYS) };
    assertThat(registry.reserve(new long[0], 0, marked, 2, registry.getSequence())).isFalse();
    assertThat(registry.inFlight(elsewhere)).isZero();
    assertThat(registry.inFlight(marked[1])).isZero();
    for (final long key : filling)
      assertThat(registry.inFlight(key)).isEqualTo(1);

    registry.committed(filling, filling.length);
    assertThat(registry.reserve(new long[0], 0, marked, 2, registry.getSequence())).isTrue();
    registry.release(marked, 2);
  }
}
