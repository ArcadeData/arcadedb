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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An LSM lookup of a LONG or INTEGER key bisects every page of the index comparing the key with the page's entries. That
 * comparison reads the stored number as a primitive: pin that it still orders negative, extreme and sparse keys exactly, over
 * several small pages (a bisection is the only code that looks at more than the first and last entry).
 */
class LSMTreeIndexIntegralKeyLookupTest extends TestHelper {
  private static final int PAGE_SIZE = 4096;
  private static final int KEYS      = 3000;

  @Test
  void longKeysAreFoundAndAbsentNeighboursAreNot() {
    final Random random = new Random(42);
    final List<Long> keys = new ArrayList<>();
    keys.add(Long.MIN_VALUE);
    keys.add(Long.MAX_VALUE);
    keys.add(0L);
    keys.add(-1L);
    keys.add(1L);
    while (keys.size() < KEYS)
      keys.add(random.nextLong() >> random.nextInt(40));

    verify("Wide", Type.LONG, keys.stream().distinct().map(k -> (Object) k).toList(), 2L);
  }

  @Test
  void integerKeysAreFoundAndAbsentNeighboursAreNot() {
    final Random random = new Random(7);
    final List<Integer> keys = new ArrayList<>();
    keys.add(Integer.MIN_VALUE);
    keys.add(Integer.MAX_VALUE);
    keys.add(0);
    keys.add(-1);
    keys.add(1);
    while (keys.size() < KEYS)
      keys.add(random.nextInt() >> random.nextInt(20));

    verify("Narrow", Type.INTEGER, keys.stream().distinct().map(k -> (Object) k).toList(), 2L);
  }

  private void verify(final String typeName, final Type keyType, final List<Object> keys, final long absentOffset) {
    database.transaction(() -> {
      database.getSchema().createDocumentType(typeName, 1).createProperty("id", keyType);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, typeName, new String[] { "id" }, PAGE_SIZE);
    });

    final List<RID> rids = new ArrayList<>();
    // several transactions: the index grows pages in each, so a lookup has more than one page to look into
    for (int from = 0; from < keys.size(); from += 500) {
      final int start = from;
      database.transaction(() -> {
        for (int i = start; i < Math.min(keys.size(), start + 500); i++) {
          final MutableDocument doc = database.newDocument(typeName);
          doc.set("id", keys.get(i));
          doc.save();
          rids.add(doc.getIdentity());
        }
      });
    }

    final Set<Object> present = new HashSet<>(keys);
    for (int i = 0; i < keys.size(); i++) {
      try (final IndexCursor cursor = database.lookupByKey(typeName, "id", keys.get(i))) {
        assertThat(cursor.hasNext()).as("key %s must be found", keys.get(i)).isTrue();
        final Identifiable found = cursor.next();
        assertThat(found.getIdentity()).isEqualTo(rids.get(i));
        assertThat(cursor.hasNext()).isFalse();
      }

      final Object neighbour = keyType == Type.LONG ? (Object) (((Long) keys.get(i)) + absentOffset) : (Object) (((Integer) keys.get(i)) + (int) absentOffset);
      if (!present.contains(neighbour) && !wrapped(keys.get(i), neighbour))
        try (final IndexCursor cursor = database.lookupByKey(typeName, "id", neighbour)) {
          assertThat(cursor.hasNext()).as("key %s is not in the index", neighbour).isFalse();
        }
    }
  }

  private static boolean wrapped(final Object key, final Object neighbour) {
    // key + offset overflowed past the type's maximum: that is not a neighbour above it
    return key instanceof Long k ? (Long) neighbour < k : (Integer) neighbour < (Integer) key;
  }
}
