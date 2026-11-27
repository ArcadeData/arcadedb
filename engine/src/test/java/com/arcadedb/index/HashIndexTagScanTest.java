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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A hash bucket page files each entry under a one-byte tag of its key's hash, and a lookup scans the tags of the page and
 * compares the key only where the tag matches. With thousands of keys in a bucket many different keys share a tag: the scan has
 * to skip every one of them and find the right one, for a key present and for a key absent, in a unique and in a non-unique index.
 */
class HashIndexTagScanTest extends TestHelper {
  private static final int KEYS = 20_000;

  @Test
  void aUniqueIndexFindsEveryKeyAmongTagCollisionsAndRefusesTheAbsentOnes() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("U", 1).createProperty("id", Type.LONG);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.HASH, true, "U", "id");
    });

    final Random random = new Random(11);
    final Set<Long> present = new HashSet<>();
    while (present.size() < KEYS)
      present.add(random.nextLong());

    database.transaction(() -> {
      for (final Long key : present) {
        final MutableDocument doc = database.newDocument("U");
        doc.set("id", key);
        doc.save();
      }
    });

    for (final Long key : present)
      try (final IndexCursor cursor = database.lookupByKey("U", "id", key)) {
        assertThat(cursor.hasNext()).as("key %s must be found", key).isTrue();
        assertThat(cursor.next().asDocument().getLong("id")).isEqualTo(key);
        assertThat(cursor.hasNext()).isFalse();
      }

    int absent = 0;
    while (absent < KEYS) {
      final long key = random.nextLong();
      if (present.contains(key))
        continue;
      try (final IndexCursor cursor = database.lookupByKey("U", "id", key)) {
        assertThat(cursor.hasNext()).as("key %s is not in the index", key).isFalse();
      }
      absent++;
    }
  }

  @Test
  void aNonUniqueIndexListsEveryRecordOfAKeyAmongTagCollisions() {
    final int distinct = 2_000;
    final int perKey = 5;
    database.transaction(() -> {
      database.getSchema().createDocumentType("N", 1).createProperty("k", Type.LONG);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.HASH, false, "N", "k");
    });

    database.transaction(() -> {
      for (int round = 0; round < perKey; round++)
        for (long k = 0; k < distinct; k++) {
          final MutableDocument doc = database.newDocument("N");
          doc.set("k", k * 7919L);
          doc.save();
        }
    });

    for (long k = 0; k < distinct; k++)
      try (final IndexCursor cursor = database.lookupByKey("N", "k", k * 7919L)) {
        int count = 0;
        while (cursor.hasNext()) {
          assertThat(cursor.next().asDocument().getLong("k")).isEqualTo(k * 7919L);
          count++;
        }
        assertThat(count).as("records of key %s", k * 7919L).isEqualTo(perKey);
      }

    try (final IndexCursor cursor = database.lookupByKey("N", "k", -1L)) {
      assertThat(cursor.hasNext()).isFalse();
    }
  }

  /**
   * Removing an entry moves the LAST slot of the page, offset and tag, into the place of the removed one. The scan must keep
   * finding the moved entries and must not find the removed ones, inside the deleting transaction and after its commit.
   */
  @Test
  void aUniqueIndexKeepsFindingTheMovedSlotsAfterRemovals() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("D", 1).createProperty("id", Type.LONG);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.HASH, true, "D", "id");
    });

    final Random random = new Random(13);
    final long[] keys = new long[KEYS];
    final Set<Long> unique = new HashSet<>();
    for (int i = 0; i < KEYS; ) {
      final long key = random.nextLong();
      if (unique.add(key))
        keys[i++] = key;
    }

    database.transaction(() -> {
      for (final long key : keys) {
        final MutableDocument doc = database.newDocument("D");
        doc.set("id", key);
        doc.save();
      }
    });

    // REMOVE EVERY OTHER KEY, AND LOOK UP A SURVIVOR AFTER EACH REMOVAL WHILE THE PAGES ARE STILL THE TRANSACTION'S OWN
    database.transaction(() -> {
      for (int i = 0; i < KEYS; i += 2) {
        try (final IndexCursor cursor = database.lookupByKey("D", "id", keys[i])) {
          cursor.next().asDocument().delete();
        }
        try (final IndexCursor cursor = database.lookupByKey("D", "id", keys[i + 1])) {
          assertThat(cursor.hasNext()).as("key %s must survive the removal of %s", keys[i + 1], keys[i]).isTrue();
        }
      }
    });

    for (int i = 0; i < KEYS; i++)
      try (final IndexCursor cursor = database.lookupByKey("D", "id", keys[i])) {
        if (i % 2 == 0)
          assertThat(cursor.hasNext()).as("key %s was removed", keys[i]).isFalse();
        else {
          assertThat(cursor.hasNext()).as("key %s must be found", keys[i]).isTrue();
          assertThat(cursor.next().asDocument().getLong("id")).isEqualTo(keys[i]);
          assertThat(cursor.hasNext()).isFalse();
        }
      }

    // THE FREED KEYS CAN BE INSERTED AGAIN: THE UNIQUE CHECK SCANS THE TAGS TOO
    database.transaction(() -> {
      for (int i = 0; i < KEYS; i += 2) {
        final MutableDocument doc = database.newDocument("D");
        doc.set("id", keys[i]);
        doc.save();
      }
    });

    for (final long key : keys)
      try (final IndexCursor cursor = database.lookupByKey("D", "id", key)) {
        assertThat(cursor.hasNext()).as("key %s must be found", key).isTrue();
        assertThat(cursor.next().asDocument().getLong("id")).isEqualTo(key);
        assertThat(cursor.hasNext()).isFalse();
      }
  }
}
