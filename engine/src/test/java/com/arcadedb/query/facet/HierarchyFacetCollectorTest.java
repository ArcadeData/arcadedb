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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.index.columnar.ColumnReader;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.index.columnar.GlobalDictionary;
import com.arcadedb.index.columnar.HierarchyManager;
import com.arcadedb.index.columnar.HierarchyView;
import com.arcadedb.index.columnar.OrdinalConsumer;
import com.arcadedb.index.columnar.PositionBitmap;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The HIERARCHY collector: each matched document counts once in every node of the closure of its values over the parents (two values
 * in one subtree count the shared ancestors once), a value with no node is an orphan root bucket, ranking applies after the expansion.
 * The first tests drive one collector by hand over a hand-built {@link HierarchyView}, ordinal map and segment views; the last ones go
 * through the SQL query over a real tag graph, where the ordinal to node mapping is the collector's per-query memo.
 */
class HierarchyFacetCollectorTest extends TestHelper {
  private static final String           PROPERTY    = "tags";
  private static final int              MAX_RECORDS = 128;
  private static final ColumnDefinition COLUMN      = new ColumnDefinition(-1, PROPERTY, Type.STRING, ColumnKind.ORDINAL_LIST, true, null);
  private static final long             WAIT_MS     = 1_000L;
  private static final String           TYPE        = ColumnarTestData.TYPE;

  // ------------------------------------------------------------------------------------------------------- hand-built inputs

  /**
   * {@code a} (root), {@code a/b} under a, {@code a/b/c} and {@code a/b/d} under a/b, {@code a/d} under a, {@code x} (root): node ids in
   * that order.
   */
  private static HierarchyView tree() {
    return HierarchyView.of(new int[] { 0, 0, 1, 2, 3, 4, 4 }, new int[] { 0, 1, 1, 0 }, new Object[] { "a", "a/b", "a/b/c", "a/b/d", "a/d", "x" },
        new int[] { 0, 1, 2, 2, 1, 0 }, null);
  }

  private static HierarchyFacetSpec spec(final int top, final int minCount) {
    return new HierarchyFacetSpec("t", PROPERTY, null, top, minCount);
  }

  private static HierarchyFacetCollector collector(final HierarchyView view, final int top, final int minCount) {
    return HierarchyFacetCollector.forView(spec(top, minCount), COLUMN, 1_000, view, WAIT_MS);
  }

  private static GlobalDictionary dictionary(final String... values) {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    for (final String value : values)
      dictionary.getOrAdd(value);
    return dictionary;
  }

  private static PositionBitmap matches(final long... positions) {
    final PositionBitmap matches = new PositionBitmap(MAX_RECORDS);
    for (final long position : positions)
      matches.set(position);
    return matches;
  }

  private static Map<Object, Long> counts(final FacetResult result) {
    final Map<Object, Long> counts = new HashMap<>();
    for (final FacetBucket bucket : result.buckets())
      assertThat(counts.put(bucket.value(), bucket.count())).as("value " + bucket.value() + " returned twice").isNull();
    return counts;
  }

  private static FacetBucket bucket(final FacetResult result, final Object value) {
    for (final FacetBucket bucket : result.buckets())
      if (value.equals(bucket.value()))
        return bucket;
    throw new AssertionError("No bucket " + value + " in " + result.buckets());
  }

  /** A segment view of hand-written rows of the ORDINAL_LIST column {@code tags}: the live rows are those added by {@link #row}. */
  private static final class Rows implements ColumnarSegmentView, ColumnReader {
    private final TreeMap<Long, int[]> rows = new TreeMap<>();
    private final PositionBitmap       live = new PositionBitmap(MAX_RECORDS);

    Rows row(final long position, final int... ordinals) {
      dead(position, ordinals);
      live.set(position);
      return this;
    }

    /** A row the view still holds but that is dead (deleted, or superseded by an update in a later view). */
    Rows dead(final long position, final int... ordinals) {
      final int[] sorted = ordinals.clone();
      Arrays.sort(sorted);
      rows.put(position, sorted);
      return this;
    }

    @Override
    public PositionBitmap live() {
      return live;
    }

    @Override
    public PositionBitmap live(final int fromPage, final int toPageExclusive) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ColumnReader column(final String property) {
      return PROPERTY.equals(property) ? this : null;
    }

    @Override
    public long minLong(final String property) {
      return 0;
    }

    @Override
    public long maxLong(final String property) {
      return 0;
    }

    @Override
    public ColumnKind kind() {
      return ColumnKind.ORDINAL_LIST;
    }

    @Override
    public boolean isPresent(final long position) {
      return live.get(position) && rows.containsKey(position) && rows.get(position).length > 0;
    }

    @Override
    public int ordinal(final long position) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int ordinals(final long position, final int[] out) {
      final int[] ordinals = isPresent(position) ? rows.get(position) : new int[0];
      System.arraycopy(ordinals, 0, out, 0, Math.min(out.length, ordinals.length));
      return ordinals.length;
    }

    @Override
    public long longValue(final long position) {
      throw new UnsupportedOperationException();
    }

    @Override
    public double doubleValue(final long position) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void forEachOrdinal(final PositionBitmap matches, final OrdinalConsumer consumer) {
      for (final Map.Entry<Long, int[]> row : rows.entrySet())
        if (matches.get(row.getKey()) && live.get(row.getKey()))
          for (final int ordinal : row.getValue())
            consumer.accept(row.getKey(), ordinal);
    }
  }

  /** A snapshot that only answers its bucket and its dictionary (the collector reads nothing else of it). */
  private record Snapshot(int bucketId, GlobalDictionary dictionary) implements ColumnarSnapshot {
    @Override
    public List<ColumnarSegmentView> segments() {
      return List.of();
    }

    @Override
    public PositionBitmap pendingPositions() {
      return new PositionBitmap(MAX_RECORDS);
    }

    @Override
    public long oldestPendingEpochStartMs() {
      return 0;
    }

    @Override
    public boolean isUsable() {
      return true;
    }

    @Override
    public GlobalDictionary dictionary(final String property) {
      return dictionary;
    }

    @Override
    public boolean publishes(final String property) {
      return true;
    }

    @Override
    public ColumnDefinition publishedColumn(final String property) {
      return COLUMN;
    }

    @Override
    public void close() {
    }
  }

  /** The merged ordinals of hand-built bucket dictionaries (bucket i is {@code dictionaries[i]}), given by value in first-seen order. */
  private static final class Merged extends CrossBucketOrdinalMap {
    private final int[][]              toMerged;
    private final List<Object>         values  = new ArrayList<>();
    private final Map<Object, Integer> byValue = new HashMap<>();

    Merged(final GlobalDictionary... dictionaries) {
      toMerged = new int[dictionaries.length][];
      for (int bucket = 0; bucket < dictionaries.length; bucket++) {
        toMerged[bucket] = new int[dictionaries[bucket].size()];
        for (int ordinal = 0; ordinal < toMerged[bucket].length; ordinal++) {
          final Object value = dictionaries[bucket].valueOf(ordinal);
          toMerged[bucket][ordinal] = byValue.computeIfAbsent(value, v -> {
            values.add(v);
            return values.size() - 1;
          });
        }
      }
    }

    @Override
    public int merged(final int bucketId, final int bucketOrdinal) {
      return toMerged[bucketId][bucketOrdinal];
    }

    @Override
    public Object value(final int mergedOrdinal) {
      return values.get(mergedOrdinal);
    }

    @Override
    public int size() {
      return values.size();
    }

    @Override
    public int mergedOf(final Object value) {
      return byValue.getOrDefault(value, -1);
    }

    @Override
    public long version() {
      return 1;
    }

    @Override
    int[] bucketArray(final int bucketId) {
      return toMerged[bucketId];
    }
  }

  // ------------------------------------------------------------------------------------------------------- hand-built tests

  @Test
  void documentWithTwoTagsInOneSubtreeCountsOnce() {
    final GlobalDictionary dictionary = dictionary("a/b/c", "a/b/d");
    final HierarchyFacetCollector collector = collector(tree(), 10, 1);
    collector.beginBucket(0, new Snapshot(0, dictionary), new Merged(dictionary));
    collector.collect(new Rows().row(0, 0, 1), matches(0));
    final FacetResult result = collector.finish();

    assertThat(result.kind()).isEqualTo("hierarchy");
    assertThat(result.source()).isEqualTo("index");
    assertThat(counts(result)).isEqualTo(Map.of("a", 1L, "a/b", 1L, "a/b/c", 1L, "a/b/d", 1L));
    assertThat(result.missing()).isZero();
    assertThat(result.otherCount()).isZero();
    final JSONObject leaf = bucket(result, "a/b/c").attributes();
    assertThat(leaf.getString("parent")).isEqualTo("a/b");
    assertThat(leaf.getInt("depth")).isEqualTo(2);
    assertThat(leaf.has("orphan")).isFalse();
    final JSONObject root = bucket(result, "a").attributes();
    assertThat(root.has("parent")).isTrue();
    assertThat(root.isNull("parent")).isTrue();
    assertThat(root.getInt("depth")).isZero();
  }

  @Test
  void topAndMinCountApplyAfterExpansion() {
    final GlobalDictionary dictionary = dictionary("a/b", "a/d", "x");
    // a/b in 3 documents, a/d in 2, x in 3: a counts 5 after the expansion
    final Rows rows = new Rows().row(0, 0).row(1, 0).row(2, 0).row(3, 1).row(4, 1).row(5, 2).row(6, 2).row(7, 2);
    final PositionBitmap matches = matches(0, 1, 2, 3, 4, 5, 6, 7);

    final HierarchyFacetCollector top2 = collector(tree(), 2, 1);
    top2.beginBucket(0, new Snapshot(0, dictionary), new Merged(dictionary));
    top2.collect(rows, matches);
    final FacetResult result = top2.finish();
    // a/b and x tie at 3: the value order breaks the tie
    assertThat(result.buckets()).extracting(FacetBucket::value).containsExactly("a", "a/b");
    assertThat(result.buckets()).extracting(FacetBucket::count).containsExactly(5L, 3L);
    // the counts of the nodes not returned: x 3 and a/d 2
    assertThat(result.otherCount()).isEqualTo(5L);

    final HierarchyFacetCollector min4 = collector(tree(), 10, 4);
    min4.beginBucket(0, new Snapshot(0, dictionary), new Merged(dictionary));
    min4.collect(rows, matches);
    assertThat(counts(min4.finish())).isEqualTo(Map.of("a", 5L));
  }

  @Test
  void patchedValueWithoutOrdinalUsesNodeOfValue() {
    final GlobalDictionary dictionary = dictionary("a/b/c");
    final HierarchyFacetCollector collector = collector(tree(), 10, 1);
    collector.beginBucket(0, new Snapshot(0, dictionary), new Merged(dictionary));
    final PatchedValue value = PatchedValue.ofValues(dictionary, "a/d");
    assertThat(value.unknownCount()).isEqualTo(1);
    collector.collectPatched(3, value);
    final FacetResult result = collector.finish();
    assertThat(counts(result)).isEqualTo(Map.of("a/d", 1L, "a", 1L));
    assertThat(bucket(result, "a/d").attributes().getString("parent")).isEqualTo("a");
  }

  @Test
  void patchedUnknownValueIsOrphanBucket() {
    final GlobalDictionary dictionary = dictionary("a/b/c");
    final HierarchyFacetCollector collector = collector(tree(), 10, 1);
    collector.beginBucket(0, new Snapshot(0, dictionary), new Merged(dictionary));
    collector.collectPatched(3, PatchedValue.ofValues(dictionary, "nope"));
    collector.collectPatched(4, PatchedValue.ofValues(dictionary));
    final FacetResult result = collector.finish();
    assertThat(counts(result)).isEqualTo(Map.of("nope", 1L));
    final JSONObject orphan = bucket(result, "nope").attributes();
    assertThat(orphan.getBoolean("orphan")).isTrue();
    assertThat(orphan.isNull("parent")).isTrue();
    assertThat(orphan.getInt("depth")).isZero();
    assertThat(result.missing()).isEqualTo(1L);
  }

  @Test
  void twoBucketsShareMergedOrdinals() {
    // the same tags with other ordinals in each bucket
    final GlobalDictionary first = dictionary("a/b/c", "x");
    final GlobalDictionary second = dictionary("x", "zzz", "a/b/c");
    final Merged map = new Merged(first, second);
    final HierarchyFacetCollector collector = collector(tree(), 10, 1);
    collector.beginBucket(0, new Snapshot(0, first), map);
    collector.collect(new Rows().row(0, 0), matches(0));
    collector.beginBucket(1, new Snapshot(1, second), map);
    collector.collect(new Rows().row(0, 2).row(1, 0, 1), matches(0, 1));
    final FacetResult result = collector.finish();
    assertThat(counts(result)).isEqualTo(Map.of("a/b/c", 2L, "a/b", 2L, "a", 2L, "x", 1L, "zzz", 1L));
    assertThat(bucket(result, "zzz").attributes().getBoolean("orphan")).isTrue();
  }

  @Test
  void mergeOfTwoTasksSumsCounts() {
    final GlobalDictionary dictionary = dictionary("a/b/c", "a/d");
    final Merged map = new Merged(dictionary);
    final HierarchyFacetCollector first = collector(tree(), 10, 1);
    first.beginBucket(0, new Snapshot(0, dictionary), map);
    first.collect(new Rows().row(0, 0).row(1, 1), matches(0, 1));
    final HierarchyFacetCollector second = collector(tree(), 10, 1);
    second.beginBucket(0, new Snapshot(0, dictionary), map);
    second.collect(new Rows().row(5, 1).row(6), matches(5, 6));
    first.merge(second);
    final FacetResult result = first.finish();
    assertThat(counts(result)).isEqualTo(Map.of("a", 3L, "a/b", 1L, "a/b/c", 1L, "a/d", 2L));
    assertThat(result.missing()).isEqualTo(1L);
    assertThat(first.stats().segmentsScanned()).isEqualTo(2);
  }

  @Test
  void countsArrayIsChargedToTheBudget() {
    final int nodes = 50_000;
    final Object[] values = new Object[nodes];
    for (int i = 0; i < nodes; i++)
      values[i] = "n" + i;
    final HierarchyView flat = HierarchyView.of(new int[nodes + 1], new int[0], values, new int[nodes], null);
    final GlobalDictionary dictionary = dictionary("n7");
    final long before = FacetMemoryBudget.instance().available();
    final HierarchyFacetCollector collector = HierarchyFacetCollector.forView(spec(10, 1), COLUMN, nodes, flat, WAIT_MS);
    collector.beginBucket(0, new Snapshot(0, dictionary), new Merged(dictionary));
    collector.collect(new Rows().row(0, 0), matches(0));
    assertThat(before - FacetMemoryBudget.instance().available()).isGreaterThanOrEqualTo(4L * nodes);
    assertThat(collector.stats().budgetBytes()).isGreaterThanOrEqualTo(4L * nodes);
    assertThat(counts(collector.finish())).isEqualTo(Map.of("n7", 1L));
    assertThat(FacetMemoryBudget.instance().available()).isEqualTo(before);
  }

  /**
   * Review I1: a document whose node value is buffered when the reservation of an orphan of the same document is refused. The facet is
   * unavailable ({@code memory budget}) and gives everything back; the buffered node is dropped, never expanded over released counters.
   */
  @Test
  void refusalAfterANodeOfTheSameDocumentIsUnavailable() {
    final GlobalDictionary dictionary = dictionary("a/b/c", "nope");
    final long before = FacetMemoryBudget.instance().available();

    // from the segments: ordinal 0 (a node) then ordinal 1 (an orphan by merged ordinal) in one row
    final HierarchyFacetCollector segments = HierarchyFacetCollector.forView(spec(10, 1), COLUMN, 1_000, tree(), 10L);
    segments.beginBucket(0, new Snapshot(0, dictionary), new Merged(dictionary));
    assertRefusedAfterANode(segments, () -> segments.collect(new Rows().row(0, 0, 1), matches(0)), before);

    // from a patched record: its ordinal is a node, its new value an orphan by value
    final HierarchyFacetCollector patched = HierarchyFacetCollector.forView(spec(10, 1), COLUMN, 1_000, tree(), 10L);
    patched.beginBucket(0, new Snapshot(0, dictionary), new Merged(dictionary));
    assertRefusedAfterANode(patched, () -> patched.collectPatched(3, PatchedValue.ofValues(dictionary, "a/b/c", "brand-new")), before);
  }

  /** Runs {@code counting} with the budget exhausted after the collector's counters were granted. */
  private static void assertRefusedAfterANode(final HierarchyFacetCollector collector, final Runnable counting, final long before) {
    final FacetMemoryBudget.Reservation everything = FacetMemoryBudget.instance().reserve(FacetMemoryBudget.instance().available(), 0);
    assertThat(everything).isNotNull();
    try {
      counting.run();
    } finally {
      everything.close();
    }
    final FacetResult result = collector.finish();
    assertThat(result.source()).isEqualTo(FacetResult.UNAVAILABLE);
    assertThat(result.reason()).isEqualTo("memory budget");
    assertThat(FacetMemoryBudget.instance().available()).isEqualTo(before);
  }

  @Test
  void validationOnlyCollectorFinishesEmpty() {
    final FacetCollector collector = FacetExecutor.collectorFor(spec(10, 1), null, null, 0);
    assertThat(collector).isInstanceOf(HierarchyFacetCollector.class);
    collector.release();
    final FacetResult result = collector.finish();
    assertThat(result.kind()).isEqualTo("hierarchy");
    assertThat(result.source()).isEqualTo("index");
    assertThat(result.buckets()).isEmpty();
    assertThat(result.missing()).isZero();
    assertThat(result.otherCount()).isZero();
  }

  @Test
  void deadRowsOfAViewAreNotCounted() {
    final GlobalDictionary dictionary = dictionary("a/b/c", "x");
    final HierarchyFacetCollector collector = collector(tree(), 10, 1);
    collector.beginBucket(0, new Snapshot(0, dictionary), new Merged(dictionary));
    // the matches are not ANDed with live by the caller: dead rows 1 (deleted) and 2 (superseded by the overlay below) are matched
    collector.collect(new Rows().row(0, 0).dead(1, 0).dead(2, 1), matches(0, 1, 2));
    collector.collect(new Rows().row(2, 0), matches(0, 1, 2));
    final FacetResult result = collector.finish();
    assertThat(counts(result)).isEqualTo(Map.of("a/b/c", 2L, "a/b", 2L, "a", 2L));
    // a dead row is no missing value either
    assertThat(result.missing()).isZero();
  }

  @Test
  void mergeAcrossDeclaringTypesByValue() {
    // two declaring types, each with its own hierarchy: a/c is a node of the second only, a/b of the first only
    final HierarchyView firstView = HierarchyView.of(new int[] { 0, 0, 1 }, new int[] { 0 }, new Object[] { "a", "a/b" }, new int[] { 0, 1 },
        null);
    final HierarchyView secondView = HierarchyView.of(new int[] { 0, 0, 1 }, new int[] { 0 }, new Object[] { "a", "a/c" }, new int[] { 0, 1 },
        null);
    final GlobalDictionary firstDictionary = dictionary("a/b", "a/c");
    final GlobalDictionary secondDictionary = dictionary("a/c");
    final HierarchyFacetCollector first = collector(firstView, 10, 1);
    first.beginBucket(0, new Snapshot(0, firstDictionary), new Merged(firstDictionary));
    first.collect(new Rows().row(0, 0).row(1, 1), matches(0, 1));
    final HierarchyFacetCollector second = collector(secondView, 10, 1);
    second.beginBucket(0, new Snapshot(0, secondDictionary), new Merged(secondDictionary));
    second.collect(new Rows().row(0, 0), matches(0));
    first.merge(second);
    final FacetResult result = first.finish();
    // a/c is an orphan in the first type and a node in the second: summed by value
    assertThat(counts(result)).isEqualTo(Map.of("a", 2L, "a/b", 1L, "a/c", 2L));
    final JSONObject merged = bucket(result, "a/c").attributes();
    assertThat(merged.getString("parent")).isEqualTo("a");
    assertThat(merged.getInt("depth")).isEqualTo(1);
    assertThat(merged.has("orphan")).isFalse();
  }

  // ------------------------------------------------------------------------------------------------------- over a real tag graph

  private static JSONObject hierarchyJSON() {
    return new JSONObject().put("vertexType", ColumnarTestData.TAG_TYPE).put("key", "path").put("parentEdge", ColumnarTestData.PARENT_EDGE);
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  /** Tags a, a/b under a, a/b/c under a/b, a/d under a, x; assets drawn from a/b/c, a/d and zzz (an orphan). */
  private ColumnarTestData load(final int buckets, final int assets) {
    final ColumnarTestData data = new ColumnarTestData(4242L);
    data.tagHierarchy = hierarchyJSON();
    data.tagValues = List.of("a/b/c", "a/d", "zzz");
    data.buckets = buckets;
    data.createSchema(database);
    database.transaction(() -> {
      for (final String path : List.of("a", "a/b", "a/b/c", "a/d", "x"))
        database.newVertex(ColumnarTestData.TAG_TYPE).set("path", path).save();
    });
    database.transaction(() -> {
      edge("a", "a/b");
      edge("a/b", "a/b/c");
      edge("a", "a/d");
    });
    data.insertAssets(database, assets);
    FacetOracle.awaitCaughtUp(database);
    return data;
  }

  private void edge(final String from, final String to) {
    try (final ResultSet parent = database.query("sql", "SELECT FROM Tag WHERE path = ?", from);
        final ResultSet child = database.query("sql", "SELECT FROM Tag WHERE path = ?", to)) {
      parent.next().getVertex().orElseThrow().newEdge(ColumnarTestData.PARENT_EDGE, child.next().getVertex().orElseThrow());
    }
  }

  private FacetResult facet() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " FACETS t: tags HIERARCHY TOP 1000")) {
      return rs.getFacets().get("t");
    }
  }

  private Map<Object, Long> expected() {
    return FacetOracle.hierarchicalCounts(database, TYPE, PROPERTY, null, ColumnarTestData.TAG_TYPE, "path", ColumnarTestData.PARENT_EDGE);
  }

  private static Map<Object, Long> withMissing(final FacetResult result) {
    final Map<Object, Long> counts = counts(result);
    if (result.missing() > 0)
      counts.put(FacetOracle.MISSING, result.missing());
    return counts;
  }

  private HierarchyManager hierarchy() {
    return manager().hierarchy(TYPE, PROPERTY);
  }

  private void insertTags(final String bucketName, final List<String> tags, final int times) {
    database.transaction(() -> {
      for (int i = 0; i < times; i++)
        database.newDocument(TYPE).set("tags", tags).save(bucketName);
    });
  }

  @Test
  @Timeout(120)
  void ordinalsAddedAfterBuildMapByValue() {
    load(1, 200);
    assertThat(withMissing(facet())).isEqualTo(expected());
    final HierarchyView built = hierarchy().view();
    // a/b is in no asset yet: its ordinal is added to the dictionary after the view was built
    database.transaction(() -> database.newDocument(TYPE).set("tags", List.of("a/b")).save());
    FacetOracle.awaitCaughtUp(database);

    final FacetResult result = facet();
    assertThat(withMissing(result)).isEqualTo(expected());
    assertThat(bucket(result, "a/b").count()).isEqualTo(expected().get("a/b"));
    assertThat(bucket(result, "a").count()).isEqualTo(expected().get("a"));
    // nothing the hierarchy watches changed: the same view mapped the new ordinal by value
    assertThat(hierarchy().view()).isSameAs(built);
  }

  @Test
  @Timeout(120)
  void newValueIsOrphanBucket() {
    load(1, 200);
    facet();
    final HierarchyView built = hierarchy().view();
    database.transaction(() -> database.newDocument(TYPE).set("tags", List.of("new")).save());
    FacetOracle.awaitCaughtUp(database);

    final FacetResult result = facet();
    assertThat(withMissing(result)).isEqualTo(expected());
    final FacetBucket orphan = bucket(result, "new");
    assertThat(orphan.count()).isEqualTo(1L);
    assertThat(orphan.attributes().getBoolean("orphan")).isTrue();
    assertThat(orphan.attributes().isNull("parent")).isTrue();
    assertThat(bucket(result, "zzz").attributes().getBoolean("orphan")).isTrue();
    assertThat(hierarchy().view()).isSameAs(built);
    assertThat(built.nodeOfValue("new")).isEqualTo(-1);
  }

  @Test
  @Timeout(120)
  void multiBucketMergedOrdinalsShareANode() {
    load(3, 0);
    final List<Bucket> buckets = database.getSchema().getType(TYPE).getBuckets(false);
    assertThat(buckets).hasSize(3);
    insertTags(buckets.get(0).getName(), List.of("x", "a/d"), 3);
    insertTags(buckets.get(1).getName(), List.of("zzz"), 2);
    insertTags(buckets.get(2).getName(), List.of("a/d"), 4);
    FacetOracle.awaitCaughtUp(database);
    // a/d has its own ordinal in bucket 0 and in bucket 2
    try (final ColumnarSnapshot first = manager().projection(buckets.get(0).getFileId()).acquire();
        final ColumnarSnapshot third = manager().projection(buckets.get(2).getFileId()).acquire()) {
      assertThat(first.dictionary(PROPERTY).ordinalOf("a/d")).isNotNegative();
      assertThat(third.dictionary(PROPERTY).ordinalOf("a/d")).isNotNegative();
    }

    final FacetResult result = facet();
    assertThat(result.buckets().stream().filter(b -> "a/d".equals(b.value())).count()).isEqualTo(1);
    assertThat(counts(result)).isEqualTo(Map.of("a/d", 7L, "a", 7L, "x", 3L, "zzz", 2L));
    assertThat(withMissing(result)).isEqualTo(expected());
  }

  @Test
  @Timeout(300)
  void compactedBucketStillMapsByValue() throws Exception {
    load(1, 300);
    final String bucketName = database.getSchema().getType(TYPE).getBuckets(false).getFirst().getName();
    final int bucketId = database.getSchema().getType(TYPE).getBuckets(false).getFirst().getFileId();
    // enough distinct values for a compaction, then most of them dead
    database.transaction(() -> {
      for (int i = 0; i < 1_500; i++)
        database.newDocument(TYPE).set("tags", List.of("gone-" + i)).set("author", "gone").save(bucketName);
    });
    FacetOracle.awaitCaughtUp(database);
    database.transaction(() -> database.command("sql", "DELETE FROM " + TYPE + " WHERE author = 'gone'"));
    FacetOracle.awaitCaughtUp(database);
    final GlobalDictionary before;
    try (final ColumnarSnapshot snapshot = manager().projection(bucketId).acquire()) {
      before = snapshot.dictionary(PROPERTY);
    }
    assertThat(manager().compactDictionaryNow(bucketId, PROPERTY)).isTrue();
    try (final ColumnarSnapshot snapshot = manager().projection(bucketId).acquire()) {
      // a new generation: other ordinals, the dead values gone
      assertThat(snapshot.dictionary(PROPERTY)).isNotSameAs(before);
      assertThat(snapshot.dictionary(PROPERTY).ordinalOf("gone-7")).isNegative();
      assertThat(snapshot.dictionary(PROPERTY).ordinalOf("a/d")).isNotNegative();
    }

    final FacetResult result = facet();
    assertThat(withMissing(result)).isEqualTo(expected());
    assertThat(bucket(result, "a/b").attributes().getString("parent")).isEqualTo("a");
    assertThat(bucket(result, "zzz").attributes().getBoolean("orphan")).isTrue();
  }

  @Test
  @Timeout(120)
  void deletedAndUpdatedRowsAreNotCounted() {
    load(1, 300);
    database.transaction(() -> {
      database.command("sql", "DELETE FROM " + TYPE + " WHERE tags CONTAINS 'a/d' LIMIT 20");
      database.command("sql", "UPDATE " + TYPE + " SET tags = ['x'] WHERE tags CONTAINS 'zzz' LIMIT 20");
    });
    FacetOracle.awaitCaughtUp(database);
    final FacetResult result = facet();
    assertThat(result.source()).isEqualTo("index");
    assertThat(withMissing(result)).isEqualTo(expected());
  }
}
