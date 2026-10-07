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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #9461: {@link LSMTreeIndexCursor} reuses its per-key-group scratch ({@code ridState},
 * {@code mergedRIDs}) across groups (#6944). A {@code HashMap} never shrinks its table, and {@code clear()}, iteration
 * and {@code toArray()} all cost O(capacity), so once the scan passed ONE key holding N RIDs every later key paid O(N)
 * as well: a scan over K keys cost O(K x N). A FULL geospatial index, where every record shares the top GeoHash cells,
 * took ~4 hours to count 11M entries in the benchmark lane.
 */
class Issue9461HotKeyScanTest extends TestHelper {
  private static final int HOT_KEY_RIDS  = 50_000;
  private static final int DISTINCT_KEYS = 50_000;

  @Test
  void scanAfterAHotKeyStaysLinear() {
    final TypeIndex index = createIndexWithHotKey("Doc9461a");

    // Pre-fix this count takes tens of seconds (50k later keys x a 128k-slot table, three passes each); fixed, it is a
    // few hundred milliseconds. The bound is the complexity claim itself, so do not loosen it to silence a red run.
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    final long entries = index.countEntries();
    stopwatch.assertStayedUnder(5_000, "a full scan past one hot key must stay linear in the number of keys (#9461)");

    assertThat(entries).isEqualTo(HOT_KEY_RIDS + DISTINCT_KEYS);
  }

  @Test
  void scratchIsReplacedAfterAHotKeyGroup() throws Exception {
    final TypeIndex index = createIndexWithHotKey("Doc9461b");

    final LSMTreeIndexCursor cursor = (LSMTreeIndexCursor) lsmOf(index).getMutableIndex().range(true, null, true, null, true);
    try {
      final Field ridStateField = LSMTreeIndexCursor.class.getDeclaredField("ridState");
      final Field mergedRIDsField = LSMTreeIndexCursor.class.getDeclaredField("mergedRIDs");
      ridStateField.setAccessible(true);
      mergedRIDsField.setAccessible(true);

      // key 0 is the hot key: drain its whole group, then step onto the first ordinary key
      for (int i = 0; i < HOT_KEY_RIDS; i++)
        cursor.next();
      final Object ridStateAfterHotKey = ridStateField.get(cursor);
      final Object mergedRIDsAfterHotKey = mergedRIDsField.get(cursor);

      final RID firstOrdinary = cursor.next();
      assertThat(cursor.getKeys()[0]).isEqualTo(1);
      assertThat(firstOrdinary).isNotNull();

      assertThat(ridStateField.get(cursor)).as("a ridState grown by the hot key must not be reused").isNotSameAs(ridStateAfterHotKey);
      assertThat(mergedRIDsField.get(cursor)).as("a mergedRIDs grown by the hot key must not be reused")
          .isNotSameAs(mergedRIDsAfterHotKey);

      // ordinary groups keep reusing the (fresh, small) containers, as #6944 intended
      final Object ridStateSmall = ridStateField.get(cursor);
      cursor.next();
      assertThat(ridStateField.get(cursor)).isSameAs(ridStateSmall);
    } finally {
      cursor.close();
    }
  }

  private TypeIndex createIndexWithHotKey(final String typeName) {
    final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    final TypeIndex index = database.getSchema().buildTypeIndex(typeName, new String[] { "a" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> {
      // the hot key sorts first, so every ordinary key is scanned after it
      for (int i = 0; i < HOT_KEY_RIDS; i++)
        database.newDocument(typeName).set("a", 0).save();
      for (int i = 1; i <= DISTINCT_KEYS; i++)
        database.newDocument(typeName).set("a", i).save();
    });
    return index;
  }

  private static LSMTreeIndex lsmOf(final TypeIndex index) {
    return (LSMTreeIndex) index.getIndexesOnBuckets()[0];
  }
}
