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
package com.arcadedb.index.lsm;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #5683 item 1: a latent minor-key-grouping defect in {@link LSMTreeIndexCursor#fetchNext()},
 * found while working on #5635 / #5662 / #5673 and re-confirmed there, but never reachable through the public API
 * because the constructor's own invariants keep a live {@code pageCursors[p]} paired with a non-null
 * {@code cursorKeys[p]}.
 * <p>
 * In the "FIND THE MINOR KEY" loop, the first live cursor whose cached key is {@code null} is added to
 * {@code minorKeyIndexes} while {@code minorKey} stays {@code null}. Before the fix, a second live cursor that also
 * reached the {@code minorKey == null} branch was appended to that SAME list instead of starting a fresh one, so the
 * merge treated two cursors positioned at two genuinely different keys as though they shared one: the first cursor's
 * RIDs were folded into a group labelled with the second cursor's key.
 * <p>
 * The test forces the otherwise-unreachable state directly via reflection on {@link LSMTreeIndexCursor}'s private
 * {@code cursorKeys} cache - the only way to exercise code that is defensive-by-construction today - and checks only
 * the FIRST merge round: once a slot's cached key is wiped, nothing in the class ever refreshes it for a cursor that
 * is not chosen into a group, so a full drain would run into the separate (and, for the same reason, equally
 * unreachable) question of what happens to a cursor whose key can never again compare equal to anything. That is out
 * of scope for the grouping defect under test here.
 */
class Issue5683LSMTreeIndexCursorTest extends TestHelper {
  private static final String TYPE_NAME = "Widget";
  private static final int    PAGE_SIZE = 4096;

  @Test
  void nullCachedCursorKeyDoesNotMergeUnrelatedKeysTogether() throws Exception {
    createSchema();
    final LSMTreeIndexMutable mutable = bucketMutableIndex();
    final int bucketId = database.getSchema().getType(TYPE_NAME).getFirstBucketId();

    final int firstBatch = 300;
    final int secondBatch = 300;
    final int totalKeys = firstBatch + secondBatch;

    // Two batches, each committed in its own transaction, so they land on separate (older/newer) mutable pages.
    database.transaction(() -> {
      for (int i = 0; i < firstBatch; ++i)
        mutable.put(new Object[] { i }, new RID[] { new RID(bucketId, i) });
    });
    database.transaction(() -> {
      for (int i = firstBatch; i < totalKeys; ++i)
        mutable.put(new Object[] { i }, new RID[] { new RID(bucketId, i) });
    });

    database.transaction(() -> {
      try {
        assertThat(mutable.getTotalPages()).as("precondition: must span multiple pages to open multiple cursors")
            .isGreaterThan(1);

        final LSMTreeIndexCursor cursor = (LSMTreeIndexCursor) mutable.range(true, null, true, null, true);
        try {
          final Object[] pageCursors = readArrayField(cursor, "pageCursors");
          final Object[][] cursorKeys = readArrayField(cursor, "cursorKeys");
          assertThat(pageCursors[0]).as("precondition: slot 0 (the newest page) must be a live cursor").isNotNull();
          assertThat(pageCursors[1]).as("precondition: slot 1 (an older page) must be a live cursor").isNotNull();

          // Capture the REAL keys the two live cursors are positioned at before corrupting the cache, so the
          // assertions below can tell "correctly processed p1's key" apart from "wrongly labelled with p0's key".
          final int p0RealKey = (Integer) cursorKeys[0][0];
          final int p1RealKey = (Integer) cursorKeys[1][0];
          assertThat(p0RealKey).as("precondition: the two live cursors must start on different keys").isNotEqualTo(p1RealKey);

          // Corrupt ONLY the cache: pageCursors[0] stays live and pointed at its real, correct data - it is the
          // cached key the merge loop reads for grouping decisions that goes missing, exactly the "live cursor,
          // unknown key" shape the issue describes as unreachable through the constructor alone.
          cursorKeys[0] = null;

          assertThat(cursor.hasNext()).as("a live cursor must still produce a first entry").isTrue();
          final RID firstRid = cursor.next();
          final int firstKey = (Integer) cursor.getKeys()[0];

          // The fix: this round's group must be exactly p1's real key with p1's single RID - p0's null-keyed
          // cursor must NOT have been folded in under either label.
          assertThat(firstKey).as("the emitted key must be the cursor genuinely positioned there (p1), not p0's")
              .isEqualTo(p1RealKey);
          assertThat(firstRid).as("the RID must be p1's own, not merged with p0's unrelated entry")
              .isEqualTo(new RID(bucketId, p1RealKey));
        } finally {
          cursor.close();
        }
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void createSchema() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("id", Integer.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(PAGE_SIZE).create();
    });
  }

  private LSMTreeIndexMutable bucketMutableIndex() {
    final com.arcadedb.index.TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("id").getFirst();
    for (final com.arcadedb.index.Index bucketIndex : typeIndex.getIndexesOnBuckets())
      if (bucketIndex instanceof LSMTreeIndex lsmIndex)
        return lsmIndex.getMutableIndex();
    throw new IllegalStateException("No LSM bucket index found for type " + TYPE_NAME);
  }

  @SuppressWarnings("unchecked")
  private static <T> T readArrayField(final LSMTreeIndexCursor cursor, final String fieldName) throws Exception {
    final Field field = LSMTreeIndexCursor.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (T) field.get(cursor);
  }
}
