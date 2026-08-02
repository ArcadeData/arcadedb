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
package com.arcadedb.index.vector;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5318: the RID -> vector-id reverse index in {@link VectorLocationIndex} must resolve
 * a RID's vector ids in O(k) and stay perfectly consistent with the primary {@code id -> location} map across
 * inserts, updates (new id + tombstone of the old one) and clear().
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class VectorLocationIndexRidReverseTest {

  @Test
  void reverseIndexResolvesRids() {
    final VectorLocationIndex index = new VectorLocationIndex();

    final RID rid0 = new RID(3, 10);
    final RID rid1 = new RID(3, 11);

    final int id0 = index.addVector(false, 100, rid0);
    final int id1 = index.addVector(false, 200, rid1);

    assertThat(index.getVectorIdsForRid(rid0)).containsExactly(id0);
    assertThat(index.getVectorIdsForRid(rid1)).containsExactly(id1);
    assertThat(index.getVectorIdsForRid(new RID(9, 9))).isEmpty();

    // Simulate an embedding update on rid0: a new id is added and the old one is tombstoned. Since issue #5516 the
    // tombstoned id keeps no location and no reverse-index slot: only the live id is mapped to the RID.
    final int id0b = index.addVector(false, 300, rid0);
    index.markDeleted(id0);

    assertThat(index.getVectorIdsForRid(rid0)).containsExactly(id0b);
    assertThat(index.getLocation(id0)).isNull();
    assertThat(index.isDeleted(id0)).isTrue();
    assertThat(index.getDeletedCount()).isEqualTo(1);
    assertThat(index.size()).as("only the live vectors stay resident").isEqualTo(2);

    // The reverse index still mirrors the primary map exactly.
    assertReverseMirrorsPrimary(index);

    index.clear();
    assertThat(index.getVectorIdsForRid(rid0)).isEmpty();
    assertThat(index.getVectorIdsForRid(rid1)).isEmpty();
    assertThat(index.getDeletedCount()).isEqualTo(0);
    assertThat(index.isDeleted(id0)).isFalse();
  }

  /**
   * Issue #5559: nothing evicts. The map used to FIFO-drop the oldest entry past a {@code maxSize} handed to the
   * constructor, which silently unmapped a live vector from its record. The capacity hint that replaced that
   * parameter must size the map, not bound it, so every entry added past it stays resolvable both ways.
   */
  @Test
  void nothingIsEvictedPastTheInitialCapacity() {
    final int initialCapacity = 8;
    final VectorLocationIndex index = new VectorLocationIndex(initialCapacity);

    final RID[] rids = new RID[initialCapacity * 4];
    final int[] ids = new int[rids.length];
    for (int i = 0; i < rids.length; i++) {
      rids[i] = new RID(3, i);
      ids[i] = index.addVector(false, i * 10L, rids[i]);
    }

    assertThat(index.size()).as("every location stays resident").isEqualTo(rids.length);
    for (int i = 0; i < rids.length; i++) {
      assertThat(index.getVectorIdsForRid(rids[i])).as("rid %d", i).containsExactly(ids[i]);
      assertThat(index.getLocation(ids[i])).as("location of id %d", ids[i]).isNotNull();
      assertThat(index.isDeleted(ids[i])).as("id %d must not read as deleted", ids[i]).isFalse();
    }

    assertReverseMirrorsPrimary(index);
  }

  /** Assert that the reverse RID index contains exactly the RIDs of the resident locations, and nothing else. */
  private static void assertReverseMirrorsPrimary(final VectorLocationIndex index) {
    index.getAllVectorIds().forEach(id -> {
      final VectorLocationIndex.VectorLocation loc = index.getLocation(id);
      assertThat(index.getVectorIdsForRid(loc.rid)).as("id %d present for its rid", id).contains(id);
    });
  }
}
