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
import java.util.List;
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
    final int key = MergedEdgeRegistry.stripeOf(A, B, "R", Map.of("uuid", "e1"));
    assertThat(MergedEdgeRegistry.stripeOf(B, A, "R", Map.of("uuid", "e1"))).isEqualTo(key);
    assertThat(key).isBetween(0, MergedEdgeRegistry.STRIPES - 1);

    // Entry order does not matter, a whole number is the same key whatever its class
    final Map<String, Object> ordered = new LinkedHashMap<>();
    ordered.put("a", 1);
    ordered.put("b", "x");
    final Map<String, Object> reversed = new LinkedHashMap<>();
    reversed.put("b", "x");
    reversed.put("a", 1L);
    assertThat(MergedEdgeRegistry.stripeOf(A, B, "R", ordered)).isEqualTo(MergedEdgeRegistry.stripeOf(A, B, "R", reversed));
    assertThat(MergedEdgeRegistry.stripeOf(A, B, "R", Map.of("n", 2.0))).isEqualTo(MergedEdgeRegistry.stripeOf(A, B, "R", Map.of("n", 2)));

    // No properties and an empty map are the type-only key
    assertThat(MergedEdgeRegistry.stripeOf(A, B, "R", null)).isEqualTo(MergedEdgeRegistry.stripeOf(A, B, "R", Map.of()));
  }

  @Test
  void aStripeCommittedAfterTheBeginIsRefused() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final int[] stripes = { MergedEdgeRegistry.stripeOf(A, B, "R", Map.of("uuid", "e1")) };

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
    final int[] stripes = { MergedEdgeRegistry.stripeOf(A, B, "R", Map.of("uuid", "e1")) };
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
    final int own = MergedEdgeRegistry.stripeOf(A, B, "R", Map.of("uuid", "e1"));
    final int typeOnly = MergedEdgeRegistry.stripeOf(A, B, "R", null);
    final int[] marked = { own, typeOnly };

    assertThat(registry.reserve(new int[] { own }, 1, marked, 2, registry.getSequence())).isTrue();
    assertThat(registry.inFlight(own)).isEqualTo(1);
    registry.committed(marked, 2);
    assertThat(registry.inFlight(own)).isZero();
    assertThat(registry.inFlight(typeOnly)).isZero();

    assertThat(registry.reserve(new int[] { own }, 1, marked, 2, registry.getSequence())).isTrue();
    registry.release(marked, 2);
    // A second release of the same stripes cannot drive a counter below zero, which would leave the stripe refusing for good
    registry.release(marked, 2);
    assertThat(registry.inFlight(own)).isZero();
    assertThat(registry.reserve(new int[] { own }, 1, marked, 2, registry.getSequence())).isTrue();
  }

  @Test
  void twoRelationshipsOfOneTransactionSharingAStripeAreNotItsOwnConflict() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final int typeOnly = MergedEdgeRegistry.stripeOf(A, B, "R", null);
    final int[] checked = { MergedEdgeRegistry.stripeOf(A, B, "R", Map.of("uuid", "e1")),
        MergedEdgeRegistry.stripeOf(A, B, "R", Map.of("uuid", "e2")) };
    // Both mark the type-only stripe of the pair
    final int[] marked = { checked[0], typeOnly, checked[1], typeOnly };

    assertThat(registry.reserve(checked, 2, marked, 4, registry.getSequence())).isTrue();
    registry.committed(marked, 4);
  }

  @Test
  void aMatchByTypeAloneIsRefusedByAConcurrentMatchByProperties() {
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final int typeOnly = MergedEdgeRegistry.stripeOf(A, B, "R", null);
    final int withProperties = MergedEdgeRegistry.stripeOf(A, B, "R", Map.of("uuid", "e1"));
    final long begin = registry.getSequence();

    // MERGE (a)-[:R {uuid: 'e1'}]->(b) commits: it checks its own stripe and marks the type-only one too
    assertThat(registry.reserve(new int[] { withProperties }, 1, new int[] { withProperties, typeOnly }, 2, begin)).isTrue();
    registry.committed(new int[] { withProperties, typeOnly }, 2);

    // MERGE (a)-[:R]->(b), begun before, would have matched it
    assertThat(registry.reserve(new int[] { typeOnly }, 1, new int[] { typeOnly }, 1, begin)).isFalse();
  }

  @Test
  void aMatchByAStrictSubsetOfAnotherMatchsPropertiesIsTheDocumentedLimit() {
    // {uuid: 'e1'} would have matched a relationship created by {uuid: 'e1', kind: 'a'}, but its key is a different stripe and the
    // larger map marks only its own and the type-only one: the registry does not see it (a unique constraint does)
    final MergedEdgeRegistry registry = new MergedEdgeRegistry();
    final int larger = MergedEdgeRegistry.stripeOf(A, B, "R", Map.of("uuid", "e1", "kind", "a"));
    final int smaller = MergedEdgeRegistry.stripeOf(A, B, "R", Map.of("uuid", "e1"));
    final int typeOnly = MergedEdgeRegistry.stripeOf(A, B, "R", null);
    final long begin = registry.getSequence();

    registry.reserve(new int[] { larger }, 1, new int[] { larger, typeOnly }, 2, begin);
    registry.committed(new int[] { larger, typeOnly }, 2);

    if (!List.of(larger, typeOnly).contains(smaller))
      assertThat(registry.reserve(new int[] { smaller }, 1, new int[] { smaller, typeOnly }, 2, begin)).isTrue();
  }
}
