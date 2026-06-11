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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for a stale cached key in {@code LSMTreeIndexCursor}: when a key-wide tombstone is shared
 * across pages, the constructor advanced a page cursor past the removed key without refreshing
 * {@code cursorKeys[i]}, dropping the valid entry that follows it from a range scan.
 */
class LSMTreeIndexCursorStaleKeyTest extends TestHelper {
  private static final String TYPE_NAME = "Item";
  private static final int    PAGE_SIZE = 4096;

  /**
   * The minimum key (0) is removed but never re-inserted, so an ascending scan must omit it while still
   * returning every following key. Before the fix the stale cached key suppressed key 1 (the entry right
   * after the removed minimum) from the scan; both an inclusive and an exclusive lower bound must return
   * the full {@code 1..N} tail exactly once.
   */
  @Test
  void scanAfterKeyWideTombstoneOnMinimumKeyDoesNotDropFollowingEntry() {
    final int firstBatch = 300;
    final int secondBatch = 300;
    final int totalKeys = firstBatch + secondBatch;
    final int removedKey = 0;

    createSchema();
    final LSMTreeIndexMutable mutable = bucketMutableIndex();
    final int bucketId = database.getSchema().getType(TYPE_NAME).getFirstBucketId();

    // Live copies of keys 0..firstBatch-1 on the older pages (the minimum key 0 included).
    database.transaction(() -> {
      for (int i = 0; i < firstBatch; ++i)
        mutable.put(new Object[] { i }, new RID[] { new RID(bucketId, i) });
    });

    // A second, newer live copy of the minimum key.
    database.transaction(() -> mutable.put(new Object[] { removedKey }, new RID[] { new RID(bucketId, 100000L) }));

    // Append the rest of the keys so the page that will hold the tombstone is fresh, keeping the tombstone
    // isolated (no live copy of the minimum key sharing its page).
    database.transaction(() -> {
      for (int i = firstBatch; i < totalKeys; ++i)
        mutable.put(new Object[] { i }, new RID[] { new RID(bucketId, i) });
    });

    // Key-wide removal of the minimum key. With no RID it writes the REMOVED_ENTRY_RID full-key tombstone
    // collected into removedKeys; being the minimum key it sorts first on its page, so the page cursor
    // surfaces it as a pure tombstone and seeds removedKeys before the older live pages are validated.
    database.transaction(() -> mutable.remove(new Object[] { removedKey }));

    database.transaction(() -> {
      assertThat(mutable.getTotalPages()).as("index must span multiple pages to open multiple cursors").isGreaterThan(1);

      final List<Integer> expectedTail = new ArrayList<>();
      for (int i = removedKey + 1; i < totalKeys; ++i)
        expectedTail.add(i);

      // The stale-key bug dropped the entry (key 1) immediately following the removed minimum, so the scan
      // is asserted on membership: the removed key must be gone and every following key must survive.
      final List<Integer> inclusive = collectKeys(mutable, removedKey, true, totalKeys);
      assertThat(inclusive).as("removed minimum key must be absent").doesNotContain(removedKey);
      assertThat(inclusive).as("inclusive lower bound must not drop the entry right after the removed minimum")
          .containsExactlyInAnyOrderElementsOf(expectedTail);

      final List<Integer> exclusive = collectKeys(mutable, removedKey, false, totalKeys);
      assertThat(exclusive).as("removed minimum key must be absent").doesNotContain(removedKey);
      assertThat(exclusive).as("exclusive lower bound must not drop the entry right after the removed minimum")
          .containsExactlyInAnyOrderElementsOf(expectedTail);
    });
  }

  /**
   * Same topology, but the removed minimum key is re-inserted with a fresh RID on the newest page. The
   * delete-then-reinsert sequence from the issue must yield the complete {@code 0..N} set with an inclusive
   * lower bound (boundary present once) and the {@code 1..N} tail with an exclusive lower bound.
   */
  @Test
  void scanAfterDeleteThenReinsertOfMinimumKeyReturnsCompleteSet() {
    final int firstBatch = 300;
    final int secondBatch = 300;
    final int totalKeys = firstBatch + secondBatch;
    final int boundaryKey = 0;

    createSchema();
    final LSMTreeIndexMutable mutable = bucketMutableIndex();
    final int bucketId = database.getSchema().getType(TYPE_NAME).getFirstBucketId();

    database.transaction(() -> {
      for (int i = 0; i < firstBatch; ++i)
        mutable.put(new Object[] { i }, new RID[] { new RID(bucketId, i) });
    });
    database.transaction(() -> mutable.put(new Object[] { boundaryKey }, new RID[] { new RID(bucketId, 100000L) }));
    database.transaction(() -> {
      for (int i = firstBatch; i < totalKeys; ++i)
        mutable.put(new Object[] { i }, new RID[] { new RID(bucketId, i) });
    });
    database.transaction(() -> mutable.remove(new Object[] { boundaryKey }));
    // Re-insert the boundary key on the newest page (delete-then-reinsert).
    database.transaction(() -> mutable.put(new Object[] { boundaryKey }, new RID[] { new RID(bucketId, 200000L) }));

    database.transaction(() -> {
      final List<Integer> inclusive = collectKeys(mutable, boundaryKey, true, totalKeys);
      final List<Integer> expectedFull = new ArrayList<>();
      for (int i = boundaryKey; i < totalKeys; ++i)
        expectedFull.add(i);
      assertThat(inclusive).containsExactlyInAnyOrderElementsOf(expectedFull);

      final List<Integer> exclusive = collectKeys(mutable, boundaryKey, false, totalKeys);
      final List<Integer> expectedTail = new ArrayList<>();
      for (int i = boundaryKey + 1; i < totalKeys; ++i)
        expectedTail.add(i);
      assertThat(exclusive).containsExactlyInAnyOrderElementsOf(expectedTail);
    });
  }

  private void createSchema() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("id", Integer.class);
      // NON-unique index: a key-wide remove() writes the REMOVED_ENTRY_RID full-key tombstone that the
      // cursor collects into removedKeys, which is the precondition for the buggy branch.
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(PAGE_SIZE).create();
    });
  }

  private static List<Integer> collectKeys(final LSMTreeIndexMutable mutable, final int fromKey, final boolean fromInclusive,
      final int toKeyExclusiveUpper) {
    try {
      final List<Integer> seen = new ArrayList<>();
      final IndexCursor cursor = mutable.range(true, new Object[] { fromKey }, fromInclusive, new Object[] { toKeyExclusiveUpper },
          false);
      while (cursor.hasNext()) {
        cursor.next();
        seen.add((Integer) cursor.getKeys()[0]);
      }
      return seen;
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private LSMTreeIndexMutable bucketMutableIndex() {
    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("id").getFirst();
    for (final Index bucketIndex : typeIndex.getIndexesOnBuckets())
      if (bucketIndex instanceof LSMTreeIndex lsmIndex)
        return lsmIndex.getMutableIndex();
    throw new IllegalStateException("No LSM bucket index found for type " + TYPE_NAME);
  }
}
