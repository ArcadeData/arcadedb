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
package com.arcadedb.query.facet;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.index.columnar.GlobalDictionary;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class CrossBucketOrdinalMapTest extends TestHelper {
  private static final String TYPE = ColumnarTestData.TYPE;

  private ColumnarTestData data(final int buckets) {
    final ColumnarTestData data = new ColumnarTestData(7);
    data.tagVocabulary = 500;
    data.buckets = buckets;
    return data;
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private List<Bucket> buckets(final String type) {
    return database.getSchema().getType(type).getBuckets(true);
  }

  /** One snapshot per polymorphic bucket of the type, in bucket order. */
  private ColumnarSnapshot[] acquire(final String type) {
    final List<Bucket> buckets = buckets(type);
    final ColumnarSnapshot[] snapshots = new ColumnarSnapshot[buckets.size()];
    for (int i = 0; i < snapshots.length; i++)
      snapshots[i] = manager().projection(buckets.get(i).getFileId()).acquire();
    return snapshots;
  }

  private static void close(final ColumnarSnapshot[] snapshots) {
    for (final ColumnarSnapshot snapshot : snapshots)
      snapshot.close();
  }

  private CrossBucketOrdinalMap of(final ColumnarSnapshot[] snapshots) {
    return CrossBucketOrdinalMap.of((DatabaseInternal) database, TYPE, "brand", snapshots);
  }

  /** The merged ordinal of every value of every snapshot's brand dictionary, checked against the dictionaries on the way. */
  private static Map<Object, Integer> mergedByValue(final CrossBucketOrdinalMap map, final ColumnarSnapshot[] snapshots) {
    final Map<Object, Integer> byValue = new HashMap<>();
    for (final ColumnarSnapshot snapshot : snapshots) {
      final GlobalDictionary dictionary = snapshot.dictionary("brand");
      if (dictionary == null)
        continue; // a bucket that never held a record
      for (int ordinal = 0; ordinal < dictionary.size(); ordinal++) {
        final Object value = dictionary.valueOf(ordinal);
        final int merged = map.merged(snapshot.bucketId(), ordinal);
        assertThat(map.value(merged)).as("value of the merged ordinal of " + value).isEqualTo(value);
        final Integer previous = byValue.put(value, merged);
        if (previous != null)
          assertThat(merged).as("merged ordinal of " + value + " in bucket " + snapshot.bucketId()).isEqualTo(previous);
      }
    }
    return byValue;
  }

  private void insertIntoBucket(final Bucket bucket, final String brand, final int times) {
    database.transaction(() -> {
      for (int i = 0; i < times; i++)
        database.newDocument(TYPE).set("brand", brand).set("tags", List.of("tag-1")).save(bucket.getName());
    });
  }

  @Test
  @Timeout(120)
  void singleBucketTypeBypassesTheMap() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 1_000);
    FacetOracle.awaitCaughtUp(database);

    final ColumnarSnapshot[] snapshots = acquire(TYPE);
    try (final CrossBucketOrdinalMap map = of(snapshots)) {
      assertThat(snapshots).hasSize(1);
      assertThat(map).isInstanceOf(CrossBucketOrdinalMap.IdentityOrdinalMap.class);
      final GlobalDictionary dictionary = snapshots[0].dictionary("brand");
      assertThat(map.size()).isEqualTo(dictionary.size());
      for (int ordinal = 0; ordinal < dictionary.size(); ordinal++) {
        assertThat(map.merged(snapshots[0].bucketId(), ordinal)).isEqualTo(ordinal);
        assertThat(map.value(ordinal)).isEqualTo(dictionary.valueOf(ordinal));
      }
      assertThat(map.mergedOf("brand-3")).isEqualTo(dictionary.ordinalOf("brand-3")).isNotNegative();
      assertThat(map.mergedOf("Nobody")).isEqualTo(-1);
    } finally {
      close(snapshots);
    }
  }

  @Test
  @Timeout(300)
  void sameValueInThreeBucketsIsOneMergedOrdinal() {
    final ColumnarTestData data = data(3);
    data.createSchema(database);
    data.insertAssets(database, 3_000);
    FacetOracle.awaitCaughtUp(database);

    final ColumnarSnapshot[] snapshots = acquire(TYPE);
    try (final CrossBucketOrdinalMap map = of(snapshots)) {
      assertThat(snapshots).hasSize(3);
      assertThat(map).isInstanceOf(CrossBucketOrdinalMap.MergedOrdinalMap.class);
      final int[] ordinals = new int[3];
      for (int i = 0; i < 3; i++) {
        ordinals[i] = snapshots[i].dictionary("brand").ordinalOf("brand-7");
        assertThat(ordinals[i]).as("brand-7 in bucket " + snapshots[i].bucketId()).isNotNegative();
      }
      final int merged = map.merged(snapshots[0].bucketId(), ordinals[0]);
      assertThat(map.merged(snapshots[1].bucketId(), ordinals[1])).isEqualTo(merged);
      assertThat(map.merged(snapshots[2].bucketId(), ordinals[2])).isEqualTo(merged);
      assertThat(map.mergedOf("brand-7")).isEqualTo(merged);
      assertThat(map.mergedOf("Nobody")).isEqualTo(-1);

      final Set<Object> distinct = new HashSet<>(FacetOracle.expectedCounts(database, TYPE, "brand", null).keySet());
      distinct.remove(FacetOracle.MISSING);
      assertThat(map.size()).isEqualTo(distinct.size());
      assertThat(mergedByValue(map, snapshots).keySet()).isEqualTo(distinct);
    } finally {
      close(snapshots);
    }
  }

  @Test
  @Timeout(300)
  void mapIsExtendedAfterNewValues() {
    final ColumnarTestData data = data(3);
    data.createSchema(database);
    data.insertAssets(database, 3_000);
    FacetOracle.awaitCaughtUp(database);

    final ColumnarSnapshot[] first = acquire(TYPE);
    final CrossBucketOrdinalMap m1;
    final Map<Object, Integer> before;
    try {
      m1 = of(first);
      before = mergedByValue(m1, first);
      m1.close();
    } finally {
      close(first);
    }

    final Bucket third = database.getSchema().getType(TYPE).getBuckets(false).get(2);
    for (int i = 0; i < 20; i++)
      insertIntoBucket(third, "brand-" + (1_000 + i), 2);
    FacetOracle.awaitCaughtUp(database);

    final ColumnarSnapshot[] second = acquire(TYPE);
    try (final CrossBucketOrdinalMap m2 = of(second)) {
      assertThat(((CrossBucketOrdinalMap.MergedOrdinalMap) m2).entry()).isSameAs(((CrossBucketOrdinalMap.MergedOrdinalMap) m1).entry());
      assertThat(m2.size()).isEqualTo(m1.size() + 20);
      assertThat(m2.version()).isEqualTo(m1.version());
      final Map<Object, Integer> after = mergedByValue(m2, second);
      for (final Map.Entry<Object, Integer> entry : before.entrySet())
        assertThat(after.get(entry.getKey())).as("merged ordinal of " + entry.getKey()).isEqualTo(entry.getValue());
      for (int i = 0; i < 20; i++)
        assertThat(m2.mergedOf("brand-" + (1_000 + i))).isGreaterThanOrEqualTo(m1.size());
    } finally {
      close(second);
    }
  }

  /** 1,200 brands in the first bucket, then every record of 600 of them deleted: half its dictionary is reclaimable. */
  private Bucket compactableFirstBucket() {
    final ColumnarTestData data = data(3);
    data.createSchema(database);
    data.insertAssets(database, 1_500);
    final Bucket first = database.getSchema().getType(TYPE).getBuckets(false).getFirst();
    database.transaction(() -> {
      for (int i = 0; i < 1_200; i++)
        database.newDocument(TYPE).set("brand", "brand-" + (2_000 + i)).save(first.getName());
    });
    FacetOracle.awaitCaughtUp(database);
    return first;
  }

  private void deleteBrandsOfBucket(final Bucket bucket, final int fromInclusive, final int toExclusive) {
    final List<RID> doomed = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid, brand FROM bucket:" + bucket.getName())) {
      while (rs.hasNext()) {
        final var row = rs.next();
        final String brand = row.getProperty("brand");
        if (brand != null) {
          final int n = Integer.parseInt(brand.substring("brand-".length()));
          if (n >= fromInclusive && n < toExclusive)
            doomed.add(row.<Identifiable>getProperty("rid").getIdentity());
        }
      }
    }
    assertThat(doomed).isNotEmpty();
    database.transaction(() -> {
      for (final RID rid : doomed)
        database.deleteRecord(database.lookupByRID(rid, true));
    });
    FacetOracle.awaitCaughtUp(database);
  }

  @Test
  @Timeout(300)
  void mapIsRebuiltForACompactedBucket() throws Exception {
    final Bucket first = compactableFirstBucket();

    final ColumnarSnapshot[] before = acquire(TYPE);
    final long version;
    final Map<Object, Integer> mergedBefore;
    try (final CrossBucketOrdinalMap m1 = of(before)) {
      version = m1.version();
      mergedBefore = mergedByValue(m1, before);
    } finally {
      close(before);
    }

    deleteBrandsOfBucket(first, 2_000, 2_600);
    final GlobalDictionary old;
    try (final ColumnarSnapshot snapshot = manager().projection(first.getFileId()).acquire()) {
      old = snapshot.dictionary("brand");
    }
    assertThat(manager().compactDictionaryNow(first.getFileId(), "brand")).isTrue();

    final ColumnarSnapshot[] after = acquire(TYPE);
    try (final CrossBucketOrdinalMap m2 = of(after)) {
      assertThat(after[0].dictionary("brand")).isNotSameAs(old);
      assertThat(m2.version()).isGreaterThan(version);
      final Map<Object, Integer> mergedAfter = mergedByValue(m2, after);
      for (final Map.Entry<Object, Integer> entry : mergedAfter.entrySet())
        assertThat(entry.getValue()).as("merged ordinal of " + entry.getKey()).isEqualTo(mergedBefore.get(entry.getKey()));
      // a value no bucket references any more keeps its merged ordinal
      assertThat(m2.size()).isEqualTo(mergedBefore.size());
    } finally {
      close(after);
    }

    try (final MatchSet matches = MatchSet.fromProjection((DatabaseInternal) database, bucketIds(TYPE), 0)) {
      matches.setFacetFilterIndex(new int[] { -1 });
      final FacetResults results = FacetExecutor.execute((DatabaseInternal) database, matches,
          List.of(new TermsFacetSpec("brand", "brand", null, null, 1_000, 1, FacetOrder.COUNT_DESC, null, false, null)),
          new BasicCommandContext());
      final Map<Object, Long> counts = new HashMap<>();
      for (final FacetBucket bucket : results.get("brand").buckets())
        counts.put(bucket.value(), bucket.count());
      if (results.get("brand").missing() > 0)
        counts.put(FacetOracle.MISSING, results.get("brand").missing());
      assertThat(results.get("brand").otherCount()).isZero();
      assertThat(counts).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", null));
    }
  }

  private int[] bucketIds(final String type) {
    final List<Bucket> buckets = buckets(type);
    final int[] ids = new int[buckets.size()];
    for (int i = 0; i < ids.length; i++)
      ids[i] = buckets.get(i).getFileId();
    return ids;
  }

  @Test
  @Timeout(300)
  void polymorphicSubtypeSharesTheMap() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    database.command("sql", "CREATE DOCUMENT TYPE Image EXTENDS " + TYPE);
    FacetOracle.awaitCaughtUp(database);
    data.insertAssets(database, 1_000);
    database.transaction(() -> {
      for (int i = 0; i < 200; i++)
        database.newDocument("Image").set("brand", "brand-" + (i % 30)).save();
    });
    FacetOracle.awaitCaughtUp(database);

    final ColumnarSnapshot[] snapshots = acquire(TYPE);
    try (final CrossBucketOrdinalMap map = of(snapshots)) {
      assertThat(snapshots.length).isGreaterThan(1);
      assertThat(map).isInstanceOf(CrossBucketOrdinalMap.MergedOrdinalMap.class);
      mergedByValue(map, snapshots);
      final int assetBucket = database.getSchema().getType(TYPE).getBuckets(false).getFirst().getFileId();
      final ColumnarSnapshot asset = snapshots[indexOf(snapshots, assetBucket)];
      final int expected = map.merged(assetBucket, asset.dictionary("brand").ordinalOf("brand-5"));
      int imageBuckets = 0;
      for (final Bucket bucket : database.getSchema().getType("Image").getBuckets(false)) {
        final GlobalDictionary dictionary = snapshots[indexOf(snapshots, bucket.getFileId())].dictionary("brand");
        final int ordinal = dictionary == null ? -1 : dictionary.ordinalOf("brand-5");
        if (ordinal >= 0) {
          assertThat(map.merged(bucket.getFileId(), ordinal)).isEqualTo(expected);
          imageBuckets++;
        }
      }
      assertThat(imageBuckets).as("Image buckets holding brand-5").isPositive();
    } finally {
      close(snapshots);
    }

    try (final MatchSet matches = MatchSet.fromProjection((DatabaseInternal) database, bucketIds(TYPE), 0)) {
      matches.setFacetFilterIndex(new int[] { -1 });
      final FacetResult brand = FacetExecutor.execute((DatabaseInternal) database, matches,
          List.of(new TermsFacetSpec("brand", "brand", null, null, 1_000, 1, FacetOrder.COUNT_DESC, null, false, null)),
          new BasicCommandContext()).get("brand");
      final Map<Object, Long> counts = new HashMap<>();
      for (final FacetBucket bucket : brand.buckets())
        counts.put(bucket.value(), bucket.count());
      assertThat(brand.missing()).isZero();
      assertThat(counts).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", null));
    }
  }

  private static int indexOf(final ColumnarSnapshot[] snapshots, final int bucketId) {
    for (int i = 0; i < snapshots.length; i++)
      if (snapshots[i].bucketId() == bucketId)
        return i;
    throw new AssertionError("No snapshot of bucket " + bucketId);
  }

  @Test
  @Timeout(300)
  void olderSnapshotGenerationGetsAPrivateArray() throws Exception {
    final Bucket first = compactableFirstBucket();
    deleteBrandsOfBucket(first, 2_000, 2_600);

    final ColumnarSnapshot[] s1 = acquire(TYPE);
    try {
      assertThat(manager().compactDictionaryNow(first.getFileId(), "brand")).isTrue();
      final ColumnarSnapshot[] s2 = acquire(TYPE);
      try {
        assertThat(s2[0].dictionary("brand")).isNotSameAs(s1[0].dictionary("brand"));
        final long version;
        try (final CrossBucketOrdinalMap current = of(s2)) {
          version = current.version();
          assertThat(((CrossBucketOrdinalMap.MergedOrdinalMap) current).privateBytes()).isZero();
        }

        try (final CrossBucketOrdinalMap older = of(s1)) {
          // every ordinal of the older generation resolves to its own value
          mergedByValue(older, s1);
          assertThat(((CrossBucketOrdinalMap.MergedOrdinalMap) older).privateBytes()).isPositive();
          assertThat(older.mergedOf("brand-2000")).isNotNegative();
        }

        // the cached entry stayed on s2's dictionary: taking s2 again rebuilds nothing
        try (final CrossBucketOrdinalMap again = of(s2)) {
          assertThat(again.version()).isEqualTo(version);
          assertThat(((CrossBucketOrdinalMap.MergedOrdinalMap) again).privateBytes()).isZero();
          mergedByValue(again, s2);
        }
      } finally {
        close(s2);
      }
    } finally {
      close(s1);
    }
  }
}
