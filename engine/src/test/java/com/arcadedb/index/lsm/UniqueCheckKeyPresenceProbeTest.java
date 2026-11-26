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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The uniqueness check of a commit looks every new key up in each bucket index of the type, and nearly every key is new. It asks the
 * pages first ({@link TypeIndex.KeyProbe}) and builds an answer with {@code get()} only for a key some page has an entry for,
 * a deletion marker included. The work avoided is counted ({@link LSMTreeIndexMutable#getKeyPresentProbesForTesting()}), never timed:
 * inserting distinct keys must never find a page entry, and the keys that are held (a duplicate, a deleted record's key) must.
 * The perturbation this catches is a probe that answers "none" for a key a page holds, which lets a duplicate in, or one that
 * answers "maybe" for a key no page holds, which sends every insert through the full lookup again.
 */
class UniqueCheckKeyPresenceProbeTest extends TestHelper {
  private static final int BUCKETS = 4;
  private static final int BATCHES = 5;
  private static final int PER_BATCH = 300;
  private static final int KEYS    = BATCHES * PER_BATCH;
  // small pages, so each bucket index spans several and a compaction has sealed pages to move into a compacted series
  private static final int INDEX_PAGE_SIZE = 512;

  @BeforeEach
  @Override
  public void beforeTest() {
    // keep compaction manual: the counters belong to the mutable indexes the test captured
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  private TypeIndex createTypeWithUniqueIndex(final Type keyType) {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc", BUCKETS).createProperty("id", keyType);
      database.getSchema().buildTypeIndex("Doc", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
          .withPageSize(INDEX_PAGE_SIZE).create();
    });
    return (TypeIndex) database.getSchema().getIndexByName("Doc[id]");
  }

  private static Object keyOf(final Type keyType, final int i) {
    return keyType == Type.STRING ? "key-" + i : (Object) (long) i;
  }

  private void insertBatches(final Type keyType, final int firstBatch, final int lastBatch) {
    for (int b = firstBatch; b < lastBatch; b++) {
      final int batch = b;
      database.transaction(() -> {
        for (int i = 0; i < PER_BATCH; i++)
          database.newDocument("Doc").set("id", keyOf(keyType, batch * PER_BATCH + i)).save();
      });
    }
  }

  private static long keyPresentProbes(final TypeIndex typeIndex) {
    long probes = 0;
    for (final Index bucketIndex : typeIndex.getIndexesOnBuckets())
      probes += ((LSMTreeIndex) bucketIndex).getMutableIndex().getKeyPresentProbesForTesting();
    return probes;
  }

  private static void compactEveryBucketIndex(final TypeIndex typeIndex) throws Exception {
    for (final Index bucketIndex : typeIndex.getIndexesOnBuckets()) {
      ((LSMTreeIndex) bucketIndex).scheduleCompaction();
      assertThat(((LSMTreeIndex) bucketIndex).compact()).as("compaction of %s", bucketIndex.getName()).isTrue();
    }
  }

  private static boolean getFindsNothing(final TypeIndex typeIndex, final Object key) {
    try (final IndexCursor cursor = typeIndex.get(new Object[] { key }, 2)) {
      return !cursor.hasNext();
    }
  }

  @Test
  void insertingDistinctKeysNeverFindsAnEntryInAPage() throws Exception {
    for (final Type keyType : new Type[] { Type.LONG, Type.STRING }) {
      database.transaction(() -> {
        if (database.getSchema().existsType("Doc"))
          database.getSchema().dropType("Doc");
      });
      final TypeIndex typeIndex = createTypeWithUniqueIndex(keyType);

      insertBatches(keyType, 0, 3);
      compactEveryBucketIndex(typeIndex);
      // the rest sits in mutable pages above a compacted series, so both kinds of page are read for each new key
      insertBatches(keyType, 3, BATCHES);

      assertThat(database.countType("Doc", true)).as("%s keys", keyType).isEqualTo(KEYS);
      assertThat(keyPresentProbes(typeIndex)).as("%s keys: probes that found an entry of a new key", keyType).isZero();
    }
  }

  @Test
  void aDuplicateKeyIsStillRejectedInEveryKindOfPage() throws Exception {
    final TypeIndex typeIndex = createTypeWithUniqueIndex(Type.LONG);
    insertBatches(Type.LONG, 0, 3);
    compactEveryBucketIndex(typeIndex);
    insertBatches(Type.LONG, 3, BATCHES);

    final Random random = new Random(7);
    int attempts = 0;
    for (int n = 0; n < 40; n++) {
      final long duplicate = random.nextInt(KEYS);
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Doc").set("id", duplicate).save())).isInstanceOf(
          DuplicatedKeyException.class);
      ++attempts;
    }

    assertThat(database.countType("Doc", true)).isEqualTo(KEYS);
    assertThat(keyPresentProbes(typeIndex)).as("the %d rejected duplicates were each found in a page", attempts)
        .isGreaterThanOrEqualTo(attempts);
  }

  @Test
  void theKeyOfADeletedRecordCanBeInsertedAgain() throws Exception {
    final TypeIndex typeIndex = createTypeWithUniqueIndex(Type.LONG);
    insertBatches(Type.LONG, 0, BATCHES);
    compactEveryBucketIndex(typeIndex);

    database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE id < 50"));
    assertThat(database.countType("Doc", true)).isEqualTo(KEYS - 50);

    final long before = keyPresentProbes(typeIndex);
    database.transaction(() -> {
      for (long id = 0; id < 50; id++)
        database.newDocument("Doc").set("id", id).save();
    });

    assertThat(database.countType("Doc", true)).isEqualTo(KEYS);
    // the deletion markers are entries of the key: the probe sends those 50 keys to get(), which finds them deleted
    assertThat(keyPresentProbes(typeIndex) - before).isGreaterThanOrEqualTo(50);
    for (long id = 0; id < KEYS; id += 37)
      try (final IndexCursor cursor = typeIndex.get(new Object[] { id }, 2)) {
        assertThat(cursor.hasNext()).as("key %d", id).isTrue();
        cursor.next();
        assertThat(cursor.hasNext()).as("a second holder of key %d", id).isFalse();
      }
  }

  @Test
  void theProbeAnswersNoEntryOnlyWhereGetAnswersNone() throws Exception {
    for (final Type keyType : new Type[] { Type.LONG, Type.STRING }) {
      database.transaction(() -> {
        if (database.getSchema().existsType("Doc"))
          database.getSchema().dropType("Doc");
      });
      final TypeIndex typeIndex = createTypeWithUniqueIndex(keyType);
      insertBatches(keyType, 0, 3);
      compactEveryBucketIndex(typeIndex);
      insertBatches(keyType, 3, BATCHES);
      database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE id % 10 = 0"));

      // ONE PROBE, so one snapshot and one hint per page, for every order of keys: its starting point only changes how far it gallops
      final TypeIndex.KeyProbe probe = typeIndex.newKeyProbe();
      assertThat(probe).isNotNull();
      final List<Integer> order = new ArrayList<>();
      for (int i = -20; i < KEYS + 20; i++)
        order.add(i);
      final List<List<Integer>> orders = new ArrayList<>();
      orders.add(order);
      final List<Integer> descending = new ArrayList<>(order);
      Collections.reverse(descending);
      orders.add(descending);
      final List<Integer> shuffled = new ArrayList<>(order);
      Collections.shuffle(shuffled, new Random(11));
      orders.add(shuffled);

      for (final List<Integer> keys : orders) {
        int none = 0;
        for (final int i : keys) {
          final Object key = keyOf(keyType, i);
          final boolean noEntry = probe.holdsNoEntryFor(new Object[] { key });
          if (noEntry) {
            ++none;
            assertThat(getFindsNothing(typeIndex, key)).as("%s key %s: the probe promised nothing", keyType, key).isTrue();
          }
          if (i >= 0 && i < KEYS && i % 10 != 0)
            assertThat(noEntry).as("%s key %s is held by a live record", keyType, key).isFalse();
          if (i < 0 || i >= KEYS)
            assertThat(noEntry).as("%s key %s was never inserted", keyType, key).isTrue();
        }
        assertThat(none).isGreaterThanOrEqualTo(40);
      }

      // with a transaction open the pending changes of the key are merged by get(): the probe leaves the answer to it
      database.begin();
      try {
        assertThat(typeIndex.newKeyProbe().holdsNoEntryFor(new Object[] { keyOf(keyType, KEYS + 1000) })).isFalse();
      } finally {
        database.rollback();
      }
    }
  }

  @Test
  void aCompositeKeySharingOnlyOneComponentIsNotADuplicate() throws Exception {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Pair", BUCKETS);
      type.createProperty("a", Type.LONG);
      type.createProperty("b", Type.STRING);
      database.getSchema().buildTypeIndex("Pair", new String[] { "a", "b" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
          .withPageSize(INDEX_PAGE_SIZE).create();
    });
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Pair[a,b]");
    database.transaction(() -> {
      for (long a = 0; a < 200; a++)
        database.newDocument("Pair").set("a", a).set("b", "x").save();
    });
    compactEveryBucketIndex(typeIndex);
    database.transaction(() -> {
      for (long a = 200; a < 400; a++)
        database.newDocument("Pair").set("a", a).set("b", "x").save();
    });

    // the same first component with another second one, and the other way round: new keys, whatever the order they come in
    database.transaction(() -> {
      for (long a = 399; a >= 0; a--)
        database.newDocument("Pair").set("a", a).set("b", "y").save();
    });
    assertThat(database.countType("Pair", true)).isEqualTo(800);
    assertThat(keyPresentProbes(typeIndex)).as("probes that found an entry of a new composite key").isZero();

    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Pair").set("a", 7L).set("b", "y").save())).isInstanceOf(
        DuplicatedKeyException.class);
    assertThat(keyPresentProbes(typeIndex)).isGreaterThanOrEqualTo(1);

    final TypeIndex.KeyProbe probe = typeIndex.newKeyProbe();
    assertThat(probe.holdsNoEntryFor(new Object[] { 7L, "z" })).isTrue();
    assertThat(probe.holdsNoEntryFor(new Object[] { 7L, "y" })).isFalse();
    assertThat(probe.holdsNoEntryFor(new Object[] { 401L, "x" })).isTrue();
    assertThat(probe.holdsNoEntryFor(new Object[] { 399L, "x" })).isFalse();
  }

  @Test
  void aCompactionThatSwapsTheIndexEndsThePromisesOfAProbe() throws Exception {
    final TypeIndex typeIndex = createTypeWithUniqueIndex(Type.LONG);
    insertBatches(Type.LONG, 0, 2);

    final Object[] absent = { (long) KEYS + 1000 };
    final TypeIndex.KeyProbe probe = typeIndex.newKeyProbe();
    // a key no bucket index holds reaches every bucket index, so each takes its snapshot here
    assertThat(probe.holdsNoEntryFor(absent)).isTrue();

    // the compaction swaps every bucket index to a new mutable: the snapshots now describe pages that no longer answer get()
    compactEveryBucketIndex(typeIndex);
    assertThat(probe.holdsNoEntryFor(absent)).as("a probe whose snapshot a compaction replaced must not promise").isFalse();
    assertThat(probe.holdsNoEntryFor(new Object[] { 7L })).isFalse();

    // and the check of a commit made after the swap still rejects a duplicate in the compacted pages
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Doc").set("id", 7L).save())).isInstanceOf(
        DuplicatedKeyException.class);
    assertThat(database.countType("Doc", true)).isEqualTo(2 * PER_BATCH);
  }

  /**
   * The check repairs a dangling entry (its record is gone) by removing it, which writes a deletion marker into the mutable page of the
   * bucket index in place, while the commit is running. A snapshot of that page taken after a first repair holds the entry count of
   * the moment, and a second repair on the same page shifts the entries above its key: the highest one fell outside the snapshot and
   * a duplicate of it got through. Bucket index 0 holds a deletion marker of the first key, so the probe stops there and takes the
   * snapshot of bucket index 1 only at the second key, after the first repair has written into its page.
   */
  @Test
  void aRepairOfADanglingEntryDuringTheCheckDoesNotLetADuplicateThrough() {
    final TypeIndex typeIndex = createTypeWithUniqueIndex(Type.LONG);
    // the probe asks the bucket indexes in this order
    final Index[] bucketIndexes = typeIndex.getIndexesOnBuckets();
    final String firstBucket = database.getSchema().getBucketById(bucketIndexes[0].getAssociatedBucketId()).getName();
    final String secondBucket = database.getSchema().getBucketById(bucketIndexes[1].getAssociatedBucketId()).getName();

    // key 1 in the first bucket index: an entry and its deletion marker
    database.transaction(() -> database.newDocument("Doc").set("id", 1L).save(firstBucket));
    database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE id = 1"));

    // keys 1, 2 and 100 in the second; the records of 1 and 2 then vanish behind the index's back
    final RID[] dangling = new RID[2];
    database.transaction(() -> {
      dangling[0] = database.newDocument("Doc").set("id", 1L).save(secondBucket).getIdentity();
      dangling[1] = database.newDocument("Doc").set("id", 2L).save(secondBucket).getIdentity();
      database.newDocument("Doc").set("id", 100L).save(secondBucket);
    });
    database.transaction(() -> {
      for (final RID rid : dangling)
        database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid);
    });

    // 1 and 2 are free again (their entries dangle and are repaired), 100 is a real duplicate
    assertThatThrownBy(() -> database.transaction(() -> {
      for (final long id : new long[] { 1L, 2L, 100L })
        database.newDocument("Doc").set("id", id).save(firstBucket);
    })).isInstanceOf(DuplicatedKeyException.class);

    assertThat(database.query("sql", "SELECT FROM Doc WHERE id = 100").stream().count()).isEqualTo(1);

    // without the duplicate the same commit repairs both entries and goes through
    database.transaction(() -> {
      for (final long id : new long[] { 1L, 2L })
        database.newDocument("Doc").set("id", id).save(firstBucket);
    });
    assertThat(database.query("sql", "SELECT FROM Doc WHERE id <= 2").stream().count()).isEqualTo(2);
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Doc").set("id", 2L).save(secondBucket))).isInstanceOf(
        DuplicatedKeyException.class);

    // the records were removed behind the bucket's back: recompute its cached count before the integrity check of the test ends
    database.command("sql", "CHECK DATABASE FIX");
  }

  @Test
  void aTransactionThatInsertsAndDeletesTheSameKeyStillCommits() {
    createTypeWithUniqueIndex(Type.LONG);
    insertBatches(Type.LONG, 0, 1);

    // the key of a record this very transaction deletes is free for a new record: the deletion has to be known to the check
    database.transaction(() -> {
      database.command("sql", "DELETE FROM Doc WHERE id = 5");
      database.newDocument("Doc").set("id", 5L).save();
    });

    assertThat(database.countType("Doc", true)).isEqualTo(PER_BATCH);
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Doc").set("id", 5L).save())).isInstanceOf(
        DuplicatedKeyException.class);
  }
}
