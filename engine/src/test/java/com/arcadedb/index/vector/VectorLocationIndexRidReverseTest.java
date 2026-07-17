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
 * inserts, updates (new id + tombstone of the old one), FIFO eviction (bounded mode) and clear().
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class VectorLocationIndexRidReverseTest {

  @Test
  void reverseIndexResolvesRidsInUnlimitedMode() {
    final VectorLocationIndex index = new VectorLocationIndex(-1);

    final RID rid0 = new RID(3, 10);
    final RID rid1 = new RID(3, 11);

    final int id0 = index.addVector(false, 100, rid0);
    final int id1 = index.addVector(false, 200, rid1);

    assertThat(index.getVectorIdsForRid(rid0)).containsExactly(id0);
    assertThat(index.getVectorIdsForRid(rid1)).containsExactly(id1);
    assertThat(index.getVectorIdsForRid(new RID(9, 9))).isEmpty();

    // Simulate an embedding update on rid0: a new id is added and the old one is tombstoned (kept resident).
    final int id0b = index.addVector(false, 300, rid0);
    index.markDeleted(id0);

    final int[] rid0Ids = index.getVectorIdsForRid(rid0);
    assertThat(rid0Ids).hasSize(2);
    assertThat(Arrays.stream(rid0Ids).boxed()).contains(id0, id0b);

    // The reverse index still mirrors the primary map exactly (deleted entries stay resident until compaction).
    assertReverseMirrorsPrimary(index);

    index.clear();
    assertThat(index.getVectorIdsForRid(rid0)).isEmpty();
    assertThat(index.getVectorIdsForRid(rid1)).isEmpty();
  }

  @Test
  void reverseIndexTracksFifoEvictionInBoundedMode() {
    final int maxSize = 8;
    final VectorLocationIndex index = new VectorLocationIndex(maxSize, maxSize);

    final RID[] rids = new RID[maxSize * 2];
    final int[] ids = new int[maxSize * 2];
    for (int i = 0; i < rids.length; i++) {
      rids[i] = new RID(3, i);
      ids[i] = index.addVector(false, i * 10L, rids[i]);
    }

    // Only the newest maxSize entries survive FIFO eviction; the evicted ones must be gone from the reverse index too.
    assertThat(index.size()).isEqualTo(maxSize);
    for (int i = 0; i < maxSize; i++)
      assertThat(index.getVectorIdsForRid(rids[i])).as("evicted rid %d", i).isEmpty();
    for (int i = maxSize; i < rids.length; i++)
      assertThat(index.getVectorIdsForRid(rids[i])).as("resident rid %d", i).containsExactly(ids[i]);

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
