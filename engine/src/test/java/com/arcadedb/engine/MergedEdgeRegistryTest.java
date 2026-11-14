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

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The protocol of {@link MergedEdgeRegistry} (issue #11482), deterministically: what a key is, and how a commit is refused by
 * another one that committed after its begin or is committing now. The concurrent end-to-end behaviour is covered by
 * {@code CypherConcurrentWriteTest.Issue11482ConcurrentMergeRelationship}.
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
    assertThat(MergedEdgeRegistry.stripeOf(A, B, "R", Map.of("uuid", "e1"))).isBetween(0, MergedEdgeRegistry.STRIPES - 1);

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
  void aStripeCommittedAfterTheBeginIsRefused() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final long[] stripes = { MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e1")) };

    final long firstBegin = registry.getSequence();
    final long secondBegin = registry.getSequence();

    assertThat(registry.reserve(stripes, 1, stripes, 1, firstBegin)).isTrue();
    registry.committed(stripes, 1);

    // Began before that commit: its read could not see the relationship
    assertThat(registry.reserve(stripes, 1, stripes, 1, secondBegin)).isFalse();
    // Began after it: its read saw the relationship, so a creation of the same key is not refused
    assertThat(registry.reserve(stripes, 1, stripes, 1, registry.getSequence())).isTrue();
    registry.release(stripes, 1);
  }

  @Test
  void aStripeBeingCommittedRefusesTheSecondCommitAndNotTheFirst() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final long[] stripes = { MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e1")) };
    final long begin = registry.getSequence();

    assertThat(registry.reserve(stripes, 1, stripes, 1, begin)).isTrue();
    assertThat(registry.reserve(stripes, 1, stripes, 1, begin)).isFalse();

    // The first one rolls back: the stripe is free again, and nothing was committed in it
    registry.release(stripes, 1);
    assertThat(registry.reserve(stripes, 1, stripes, 1, begin)).isTrue();
    registry.release(stripes, 1);
  }

  @Test
  void everyStripeIsFreeAgainOnceItsCommitEndsWhicheverWayItEnds() {
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
    // A second release of the same stripes cannot drive a counter below zero, which would leave the stripe refusing for good
    registry.release(marked, 2);
    assertThat(registry.inFlight(own)).isZero();
    assertThat(registry.reserve(new long[] { own }, 1, marked, 2, registry.getSequence())).isTrue();
  }

  @Test
  void twoRelationshipsOfOneTransactionSharingAStripeAreNotItsOwnConflict() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final long typeOnly = MergedEdgeRegistry.keyOf(A, B, "R", null);
    final long[] checked = { MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e1")),
        MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e2")) };
    // Both mark the type-only stripe of the pair
    final long[] marked = { checked[0], typeOnly, checked[1], typeOnly };

    assertThat(registry.reserve(checked, 2, marked, 4, registry.getSequence())).isTrue();
    registry.committed(marked, 4);
  }

  @Test
  void aMatchByTypeAloneIsRefusedByAConcurrentMatchByProperties() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final long typeOnly = MergedEdgeRegistry.keyOf(A, B, "R", null);
    final long withProperties = MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e1"));
    final long begin = registry.getSequence();

    // MERGE (a)-[:R {uuid: 'e1'}]->(b) commits: it checks its own stripe and marks the type-only one too
    assertThat(registry.reserve(new long[] { withProperties }, 1, new long[] { withProperties, typeOnly }, 2, begin)).isTrue();
    registry.committed(new long[] { withProperties, typeOnly }, 2);

    // MERGE (a)-[:R]->(b), begun before, would have matched it
    assertThat(registry.reserve(new long[] { typeOnly }, 1, new long[] { typeOnly }, 1, begin)).isFalse();
  }

  @Test
  void aMatchByAStrictSubsetOfAnotherMatchsPropertiesIsTheDocumentedLimit() {
    // {uuid: 'e1'} would have matched a relationship created by {uuid: 'e1', kind: 'a'}, but its key is a different stripe and the
    // larger map marks only its own and the type-only one: the registry does not see it (a unique constraint does)
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final long larger = MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e1", "kind", "a"));
    final long smaller = MergedEdgeRegistry.keyOf(A, B, "R", Map.of("uuid", "e1"));
    final long typeOnly = MergedEdgeRegistry.keyOf(A, B, "R", null);
    final long begin = registry.getSequence();

    registry.reserve(new long[] { larger }, 1, new long[] { larger, typeOnly }, 2, begin);
    registry.committed(new long[] { larger, typeOnly }, 2);

    if (smaller != larger && smaller != typeOnly)
      assertThat(registry.reserve(new long[] { smaller }, 1, new long[] { smaller, typeOnly }, 2, begin)).isTrue();
  }

  /** Issue #11782: a key committed after the begin refuses only a creation under the same key, not one sharing its stripe. */
  @Test
  void anotherKeyCommittedInTheSameStripeDoesNotRefuse() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final long committed = MergedEdgeRegistry.keyOf(A, B, "R", null);
    final long sharingItsStripe = MergedEdgeRegistry.keyOf(A, ridSharingTheStripe(), "R", null);
    final long begin = registry.getSequence();

    assertThat(registry.reserve(new long[] { committed }, 1, new long[] { committed }, 1, begin)).isTrue();
    registry.committed(new long[] { committed }, 1);

    assertThat(registry.reserve(new long[] { sharingItsStripe }, 1, new long[] { sharingItsStripe }, 1, begin)).isTrue();
    registry.committed(new long[] { sharingItsStripe }, 1);
    assertThat(registry.reserve(new long[] { committed }, 1, new long[] { committed }, 1, begin)).isFalse();
  }

  /** Issue #11782: a commit in flight refuses only a creation under one of its own keys, not one sharing a stripe with them. */
  @Test
  void anotherKeyBeingCommittedInTheSameStripeDoesNotRefuse() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final long inFlight = MergedEdgeRegistry.keyOf(A, B, "R", null);
    final long sharingItsStripe = MergedEdgeRegistry.keyOf(A, ridSharingTheStripe(), "R", null);
    final long begin = registry.getSequence();

    assertThat(registry.reserve(new long[] { inFlight }, 1, new long[] { inFlight }, 1, begin)).isTrue();
    assertThat(registry.reserve(new long[] { sharingItsStripe }, 1, new long[] { sharingItsStripe }, 1, begin)).isTrue();
    assertThat(registry.reserve(new long[] { inFlight }, 1, new long[] { inFlight }, 1, begin)).isFalse();

    registry.release(new long[] { inFlight }, 1);
    assertThat(registry.inFlight(inFlight)).isZero();
    assertThat(registry.inFlight(sharingItsStripe)).isEqualTo(1);
    registry.committed(new long[] { sharingItsStripe }, 1);
    assertThat(registry.inFlight(sharingItsStripe)).isZero();
  }

  /**
   * Issue #11782: the table of committed keys is bounded and starts over once full. A transaction that began before it last started
   * over cannot tell a key from its stripe any more, and is refused by a moved stripe as before; one that began after is not.
   */
  @Test
  void aTransactionOlderThanTheTableOfCommittedKeysFallsBackToTheStripe() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final long first = MergedEdgeRegistry.keyOf(A, B, "R", null);
    final long sharingItsStripe = MergedEdgeRegistry.keyOf(A, ridSharingTheStripe(), "R", null);
    final long beforeTheRestart = registry.getSequence();

    assertThat(registry.reserve(new long[] { first }, 1, new long[] { first }, 1, beforeTheRestart)).isTrue();
    registry.committed(new long[] { first }, 1);
    // Enough other keys to fill the table, in commits of 1,000
    final long[] filler = new long[1_000];
    for (int commit = 0; commit <= MergedEdgeRegistry.EXACT_KEYS / filler.length; commit++) {
      for (int i = 0; i < filler.length; i++)
        filler[i] = MergedEdgeRegistry.keyOf(new RID(7, (long) commit * filler.length + i), A, "S", null);
      assertThat(registry.reserve(filler, filler.length, filler, filler.length, registry.getSequence())).isTrue();
      registry.committed(filler, filler.length);
    }
    final long afterTheRestart = registry.getSequence();
    registry.reserve(new long[] { first }, 1, new long[] { first }, 1, afterTheRestart);
    registry.committed(new long[] { first }, 1);

    assertThat(registry.reserve(new long[] { sharingItsStripe }, 1, new long[] { sharingItsStripe }, 1, beforeTheRestart)).isFalse();
    assertThat(registry.reserve(new long[] { sharingItsStripe }, 1, new long[] { sharingItsStripe }, 1, afterTheRestart)).isTrue();
  }

  /** A vertex whose relationship of type R with {@link #A} has a key other than the one of A and {@link #B}, in the same stripe. */
  private static RID ridSharingTheStripe() {
    final int stripe = MergedEdgeRegistry.stripeOf(A, B, "R", null);
    for (long position = 0; ; position++) {
      final RID candidate = new RID(5, position);
      if (MergedEdgeRegistry.stripeOf(A, candidate, "R", null) == stripe)
        return candidate;
    }
  }
}
