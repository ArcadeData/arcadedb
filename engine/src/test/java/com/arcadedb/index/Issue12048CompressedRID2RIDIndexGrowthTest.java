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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12048: the hash table of {@link CompressedRID2RIDIndex} kept the slot count it was built with, so a map built small
 * (the JSONL importer builds it with 1,000 slots) and filled with millions of RIDs walked chains of entries / 1,000 on every
 * put() and get(), and a JSONL restore took time quadratic in the size of the database. The table must now grow with its
 * entries.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12048CompressedRID2RIDIndexGrowthTest extends TestHelper {
  private static final int ENTRIES = 200_000;

  @Test
  void slotsGrowWithTheEntriesSoChainsStayShort() throws Exception {
    final CompressedRID2RIDIndex index = new CompressedRID2RIDIndex(database, 1000, 1000);

    for (int i = 0; i < ENTRIES; i++)
      index.put(new RID(3, i), new RID(4, i * 2L));

    assertThat(index.size()).isEqualTo(ENTRIES);
    assertThat(index.getKeys())
        .as("the slots must keep up with the entries: with 1,000 fixed slots every chain held 200 entries and every put() and "
            + "get() walked one, which is what made a JSONL import quadratic (#12048)")
        .isGreaterThanOrEqualTo(ENTRIES);

    for (int i = 0; i < ENTRIES; i++)
      assertThat(index.get(new RID(3, i))).isEqualTo(new RID(4, i * 2L));
    assertThat(index.get(new RID(3, ENTRIES))).isNull();
    assertThat(index.get(new RID(5, 0))).isNull();

    int found = 0;
    for (final CompressedRID2RIDIndex.EntryIterator it = index.entryIterator(); it.hasNext(); it.moveNext()) {
      assertThat(it.getValueRID()).isEqualTo(new RID(4, it.getKeyRID().getPosition() * 2));
      ++found;
    }
    assertThat(found).isEqualTo(ENTRIES);
  }

  /** Keys with one hash value stay one chain at any table size: the rebuild must keep every one of them reachable. */
  @Test
  void growthKeepsEveryEntryOfACollidingChain() throws Exception {
    final CompressedRID2RIDIndex index = new CompressedRID2RIDIndex(database, 1, 64);

    for (int bucket = 0; bucket < 50; bucket++)
      index.put(collidingKey(bucket), new RID(7, bucket));

    for (int bucket = 0; bucket < 50; bucket++)
      assertThat(index.get(collidingKey(bucket))).isEqualTo(new RID(7, bucket));
    assertThat(index.getKeys()).isGreaterThanOrEqualTo(50);
    assertThat(index.getTotalUsedSlots()).as("one hash value: the rebuilt table has a single chain, so a single used slot").isEqualTo(1);
  }

  /** RIDs of many buckets (bucket ids and positions that take one to several bytes to serialize) survive the rebuilds. */
  @Test
  void growthKeepsEveryEntryOfRidsSpreadOverManyBuckets() throws Exception {
    final int buckets = 300;
    final int positionsPerBucket = 700;
    final int entries = buckets * positionsPerBucket;
    final CompressedRID2RIDIndex index = new CompressedRID2RIDIndex(database, 1000, 1000);

    for (int position = 0; position < positionsPerBucket; position++)
      for (int bucket = 0; bucket < buckets; bucket++)
        index.put(new RID(bucket, position), new RID(bucket + 1, position * 3L + 1));

    assertThat(index.size()).isEqualTo(entries);
    assertThat(index.getKeys()).isGreaterThanOrEqualTo(entries);

    for (int position = 0; position < positionsPerBucket; position++)
      for (int bucket = 0; bucket < buckets; bucket++)
        assertThat(index.get(new RID(bucket, position))).isEqualTo(new RID(bucket + 1, position * 3L + 1));
    assertThat(index.get(new RID(buckets, 0))).isNull();
    assertThat(index.get(new RID(0, positionsPerBucket))).isNull();

    int found = 0;
    for (final CompressedRID2RIDIndex.EntryIterator it = index.entryIterator(); it.hasNext(); it.moveNext()) {
      final RID key = it.getKeyRID();
      assertThat(it.getValueRID()).isEqualTo(new RID(key.getBucketId() + 1, key.getPosition() * 3L + 1));
      ++found;
    }
    assertThat(found).isEqualTo(entries);
  }

  /** put() walks the whole chain to refuse a duplicate: a key stored before a doubling must still be found after it. */
  @Test
  void duplicateKeysAreStillRefusedAfterTheTableGrew() throws Exception {
    final int entries = 5_000;
    final CompressedRID2RIDIndex index = new CompressedRID2RIDIndex(database, 16, 16);

    for (int i = 0; i < entries; i++)
      index.put(new RID(3, i), new RID(4, i));
    assertThat(index.getKeys()).isGreaterThanOrEqualTo(entries);

    for (int i = 0; i < entries; i += 7) {
      final RID key = new RID(3, i);
      assertThatThrownBy(() -> index.put(key, new RID(5, 5))).isInstanceOf(IllegalArgumentException.class);
    }

    assertThat(index.size()).isEqualTo(entries);
    for (int i = 0; i < entries; i++)
      assertThat(index.get(new RID(3, i))).isEqualTo(new RID(4, i));
  }

  /** RID.hashCode() is bucketId * 31 + position for a small position: every key built here hashes to 1,550. */
  static RID collidingKey(final int bucket) {
    return new RID(bucket, 1550 - 31L * bucket);
  }
}
