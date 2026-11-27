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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.database.TransactionIndexContext.ComparableKey;
import com.arcadedb.database.TransactionIndexContext.IndexKey;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.IndexKeyEquality;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bookkeeping of the index entries a transaction queues: {@code IndexKey.hashCode} keeps the values {@code Objects.hash}
 * gave (the order a commit replays the entries of one key in follows from it), {@code ComparableKey} orders two LONG keys as the
 * generic comparison does, and a transaction that writes the same key twice and different keys in any order still commits them.
 */
class TransactionIndexContextKeyBookkeepingTest extends TestHelper {

  @Test
  void anIndexKeyHashesAsObjectsHashDid() {
    final Object[][] tuples = { { 1L }, { -5L, "a" }, { "x" }, { 2.5d, 3 }, { Long.MIN_VALUE } };
    final RID rid = new RID(3, 77L);
    for (final Object[] tuple : tuples) {
      final int tupleHash = IndexKeyEquality.hashTuple(tuple);
      assertThat(new IndexKey(true, IndexKey.IndexKeyOperation.ADD, tuple, rid).hashCode())
          .as("unique key %s", Arrays.toString(tuple)).isEqualTo(Objects.hash(tupleHash));
      assertThat(new IndexKey(false, IndexKey.IndexKeyOperation.ADD, tuple, rid).hashCode())
          .as("non unique key %s", Arrays.toString(tuple)).isEqualTo(Objects.hash(rid, tupleHash));
    }
    // an all-null key is exempt from uniqueness: its entries are told apart by the RID, as for a non unique index
    final Object[] allNull = { null };
    assertThat(new IndexKey(true, IndexKey.IndexKeyOperation.ADD, allNull, rid).hashCode())
        .isEqualTo(Objects.hash(rid, IndexKeyEquality.hashTuple(allNull)));
    assertThat(new IndexKey(false, IndexKey.IndexKeyOperation.ADD, new Object[] { 4L }, null).hashCode())
        .isEqualTo(Objects.hash(null, IndexKeyEquality.hashTuple(new Object[] { 4L })));
  }

  @Test
  void twoLongKeysAreOrderedAsTheGenericComparisonOrdersThem() {
    final Random random = new Random(3);
    final long[] edges = { Long.MIN_VALUE, Long.MIN_VALUE + 1, -1L, 0L, 1L, Long.MAX_VALUE - 1, Long.MAX_VALUE };
    for (final long a : edges)
      for (final long b : edges)
        assertThat(Integer.signum(new ComparableKey(new Object[] { a }).compareTo(new ComparableKey(new Object[] { b }))))
            .isEqualTo(Integer.signum(BinaryComparator.compareTo(a, b)));
    for (int i = 0; i < 1000; i++) {
      final long a = random.nextLong();
      final long b = random.nextLong();
      assertThat(Integer.signum(new ComparableKey(new Object[] { a, "t" }).compareTo(new ComparableKey(new Object[] { b, "t" }))))
          .isEqualTo(Integer.signum(Long.compare(a, b)));
    }
    // a LONG against another width still widens as before
    assertThat(new ComparableKey(new Object[] { 5L }).compareTo(new ComparableKey(new Object[] { 5 }))).isZero();
    assertThat(new ComparableKey(new Object[] { 5L }).compareTo(new ComparableKey(new Object[] { 6 }))).isNegative();
  }

  /**
   * Regression for #13022: queuing a key the transaction has not seen yet walks the per-index key tree ONCE. The old
   * {@code get()} + {@code put()} walked the same path twice, paying two key comparisons per level of the tree for every
   * indexed record a transaction saves. The comparisons are counted on the key values themselves and checked against the
   * path length of an identical tree, so the bound is exact and does not depend on how the tree happens to be balanced.
   */
  @Test
  void aNewKeyWalksTheKeyTreeOnce() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE CountedDoc");
      database.command("sql", "CREATE PROPERTY CountedDoc.id LONG");
      database.command("sql", "CREATE INDEX ON CountedDoc (id) NOTUNIQUE");
    });
    final IndexInternal index = database.getSchema().getType("CountedDoc").getPolymorphicIndexByProperties("id")
        .getIndexesOnBuckets()[0];

    final int[] comparisons = new int[1];
    final int total = 1023;
    final long[] values = new long[total];
    for (int i = 0; i < total; i++)
      values[i] = 2L * i;
    final Random random = new Random(11);
    for (int i = total - 1; i > 0; i--) {
      final int j = random.nextInt(i + 1);
      final long t = values[i];
      values[i] = values[j];
      values[j] = t;
    }

    // the same keys in the same order, in the transaction's bookkeeping and in a plain tree of the same shape
    final TransactionIndexContext changes = new TransactionIndexContext(null);
    final TreeMap<ComparableKey, Object> reference = new TreeMap<>();
    for (int i = 0; i < total; i++) {
      final Object[] key = { new CountingKey(values[i], comparisons) };
      changes.addIndexKeyLock(index, IndexKey.IndexKeyOperation.ADD, key, new RID(3, i));
      reference.put(new ComparableKey(key), Boolean.TRUE);
    }

    // odd values are not in the tree: each probe is a new key that lands at the bottom of a full path
    for (int probe = -1; probe <= 2 * total + 1; probe += 64) {
      final Object[] key = { new CountingKey(probe, comparisons) };

      comparisons[0] = 0;
      assertThat(reference.get(new ComparableKey(key))).isNull();
      final int pathLength = comparisons[0];
      assertThat(pathLength).as("precondition: the probe %s walks a path", probe).isPositive();

      comparisons[0] = 0;
      changes.addIndexKeyLock(index, IndexKey.IndexKeyOperation.ADD, key, new RID(4, probe + 1));
      assertThat(comparisons[0]).as("comparisons to queue the new key %s, path length %s", probe, pathLength).isEqualTo(pathLength);

      reference.put(new ComparableKey(key), Boolean.TRUE);
    }

    // a key already queued, by another record of the non unique index: one key in the tree, both entries kept
    final Object[] repeated = { new CountingKey(values[0], comparisons) };
    changes.addIndexKeyLock(index, IndexKey.IndexKeyOperation.ADD, repeated, new RID(5, 1));
    final List<TreeMap<ComparableKey, Map<IndexKey, IndexKey>>> lanes = changes.getIndexKeyLanes(index);
    assertThat(lanes).hasSize(1);
    assertThat(lanes.getFirst()).hasSize(reference.size());
    assertThat(lanes.getFirst().get(new ComparableKey(repeated))).hasSize(2);
  }

  /** A key value whose comparisons are counted: the generic comparison ends in {@link Comparable#compareTo} for one class. */
  private static final class CountingKey implements Comparable<CountingKey> {
    private final long  value;
    private final int[] comparisons;

    private CountingKey(final long value, final int[] comparisons) {
      this.value = value;
      this.comparisons = comparisons;
    }

    @Override
    public int compareTo(final CountingKey other) {
      ++comparisons[0];
      return Long.compare(value, other.value);
    }

    @Override
    public boolean equals(final Object o) {
      return o instanceof CountingKey other && other.value == value;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(value);
    }
  }

  @Test
  void aTransactionQueuingSeveralKeysInAnyOrderCommitsAllOfThem() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id LONG");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");
    });

    final int total = 500;
    final long[] ids = new long[total];
    for (int i = 0; i < total; i++)
      ids[i] = (i % 2 == 0 ? 1 : -1) * (long) i * 1_000_003L;
    final Random random = new Random(5);
    for (int i = total - 1; i > 0; i--) {
      final int j = random.nextInt(i + 1);
      final long t = ids[i];
      ids[i] = ids[j];
      ids[j] = t;
    }

    database.transaction(() -> {
      for (final long id : ids) {
        final MutableDocument doc = database.newDocument("Doc");
        doc.set("id", id);
        doc.save();
      }
    });

    for (final long id : ids)
      try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE id = ?", id)) {
        assertThat(rs.hasNext()).as("id %s", id).isTrue();
        rs.next();
        assertThat(rs.hasNext()).isFalse();
      }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Doc")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo((long) total);
    }
  }
}
