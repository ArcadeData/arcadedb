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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestAccess;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FacetExecutorTermsTest extends TestHelper {
  private static final String       TYPE   = ColumnarTestData.TYPE;
  private static final int          ALL    = 1_000;
  private static final List<String> WHERES = Arrays.asList(null, "fileSize < 1048576", "author = 'author-7'", "tags CONTAINS 'tag-5'",
      "fileSize >= 2097152 AND author IS NOT NULL");

  private ColumnarTestData data(final int buckets) {
    final ColumnarTestData data = new ColumnarTestData(42);
    data.tagVocabulary = 500;
    data.buckets = buckets;
    return data;
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private MatchSet matches(final String where, final int facetCount) {
    final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0);
    try (builder; final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE + (where == null ? "" : " WHERE " + where))) {
      while (rs.hasNext())
        assertThat(builder.add(rs.next().<Identifiable>getProperty("rid").getIdentity(), 0)).isTrue();
      final MatchSet matches = builder.build();
      final int[] noFilter = new int[facetCount];
      Arrays.fill(noFilter, -1);
      matches.setFacetFilterIndex(noFilter);
      return matches;
    }
  }

  private FacetResults execute(final String where, final FacetSpec... facets) {
    try (final MatchSet matches = matches(where, facets.length)) {
      assertThat(matches.size()).as("records matching " + where).isPositive();
      return executeOn(matches, facets);
    }
  }

  private FacetResults executeOn(final MatchSet matches, final FacetSpec... facets) {
    return FacetExecutor.execute((DatabaseInternal) database, matches, List.of(facets), new BasicCommandContext());
  }

  private static TermsFacetSpec terms(final String property, final int top) {
    return new TermsFacetSpec(property, property, null, null, top, 1, FacetOrder.COUNT_DESC, null, false, null);
  }

  /** The value buckets as a map, the records without a value under {@link FacetOracle#MISSING} (as the oracle keys them). */
  /**
   * The counts keyed as the oracle keys them: the facets return the values a row holds (a Double for a DOUBLE property), the oracle
   * keys them normalized ({@code Type.normalizeNumberForKey}), so the facet values go through the same normalization first.
   */
  private static Map<Object, Long> counts(final FacetResult result) {
    final Map<Object, Long> counts = new HashMap<>();
    for (final FacetBucket bucket : result.buckets())
      if (!bucket.isMissing())
        assertThat(counts.put(FacetValueComparator.normalize(bucket.value()), bucket.count())).as("value " + bucket.value() + " returned twice")
            .isNull();
    if (result.missing() > 0)
      counts.put(FacetOracle.MISSING, result.missing());
    return counts;
  }

  /** A bucket value as the oracle keys it: a date as its column's epoch count, through the extractor's own conversion. */
  private Object oracleKey(final String property, final Object value) {
    if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean)
      return value;
    return manager().extractor(TYPE).toColumnLong(property, value);
  }

  private static List<Object> values(final FacetResult result) {
    final List<Object> values = new ArrayList<>();
    for (final FacetBucket bucket : result.buckets())
      values.add(bucket.isMissing() ? FacetOracle.MISSING : bucket.value());
    return values;
  }

  /** The references held on each projected bucket's current state; comparable between two reads while caught up. */
  private Map<Integer, Integer> stateReferences() {
    final Map<Integer, Integer> references = new HashMap<>();
    for (final Bucket bucket : database.getSchema().getType(TYPE).getBuckets(true))
      if (manager().projection(bucket.getFileId()) != null)
        references.put(bucket.getFileId(), ColumnarTestAccess.stateReferences(manager(), bucket.getFileId()));
    return references;
  }

  /** Runs the facets twice, the second time checking that the executor released every snapshot it acquired. */
  private void assertSnapshotsReleased(final String where, final FacetSpec... facets) {
    final Map<Integer, Integer> before = stateReferences();
    assertThat(before).isNotEmpty();
    try {
      execute(where, facets);
    } catch (final RuntimeException expected) {
      // a failing facet must release them too
    }
    assertThat(stateReferences()).as("references on the bucket states after the facets").isEqualTo(before);
  }

  private void insert(final String brand, final List<String> tags, final int times) {
    database.transaction(() -> {
      for (int i = 0; i < times; i++) {
        final var document = database.newDocument(TYPE);
        if (brand != null)
          document.set("brand", brand);
        if (tags != null)
          document.set("tags", tags);
        document.save();
      }
    });
  }

  @Test
  @Timeout(300)
  void termsEqualGroupByAtLagZero() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 5_000);
    FacetOracle.awaitCaughtUp(database);

    for (final String where : WHERES) {
      final FacetResults results = execute(where, terms("brand", ALL), terms("tags", ALL), terms("fileSize", ALL), terms("publishDate", ALL));
      for (final String property : List.of("brand", "tags", "fileSize", "publishDate")) {
        final FacetResult result = results.get(property);
        assertThat(result.kind()).isEqualTo("terms");
        assertThat(result.source()).isEqualTo("index");
        final Map<Object, Long> expected = FacetOracle.expectedCounts(database, TYPE, property, where);
        if (expected.size() - (expected.containsKey(FacetOracle.MISSING) ? 1 : 0) <= ALL) {
          final Map<Object, Long> keyed = new HashMap<>();
          counts(result).forEach((value, count) -> keyed.put(value == FacetOracle.MISSING ? value : oracleKey(property, value), count));
          assertThat(keyed).as(property + " WHERE " + where).isEqualTo(expected);
          assertThat(result.otherCount()).isZero();
        } else {
          // more distinct values than TOP: every returned count is exact and the rest is otherCount
          long returned = 0;
          for (final FacetBucket bucket : result.buckets()) {
            assertThat(bucket.count()).as(property + "=" + bucket.value()).isEqualTo(expected.get(oracleKey(property, bucket.value())));
            returned += bucket.count();
          }
          final long total = expected.entrySet().stream().filter(e -> e.getKey() != FacetOracle.MISSING).mapToLong(Map.Entry::getValue).sum();
          assertThat(result.otherCount()).isEqualTo(total - returned);
          assertThat(result.missing()).isEqualTo(expected.getOrDefault(FacetOracle.MISSING, 0L));
        }
      }
    }
    assertSnapshotsReleased("author = 'author-7'", terms("brand", ALL), terms("tags", ALL), terms("fileSize", ALL));
  }

  @Test
  @Timeout(120)
  void multiValuedCountsDocuments() {
    data(1).createSchema(database);
    insert("brand-1", List.of("tag-1", "tag-1", "tag-2"), 1);
    insert("brand-2", List.of("tag-2"), 1);
    FacetOracle.awaitCaughtUp(database);

    final FacetResult tags = execute(null, terms("tags", 10)).get("tags");
    assertThat(counts(tags)).isEqualTo(Map.of("tag-1", 1L, "tag-2", 2L));
    assertThat(counts(tags)).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "tags", null));
    assertThat(tags.otherCount()).isZero();
  }

  @Test
  @Timeout(120)
  void missingAndOtherCount() {
    final ColumnarTestData data = data(1);
    data.brands = 5;
    data.createSchema(database);
    data.insertAssets(database, 500);
    insert(null, List.of("tag-1"), 7);
    FacetOracle.awaitCaughtUp(database);

    final FacetResult brand = execute(null, terms("brand", 2)).get("brand");
    final Map<Object, Long> expected = FacetOracle.expectedCounts(database, TYPE, "brand", null);
    assertThat(expected.get(FacetOracle.MISSING)).isEqualTo(7L);
    assertThat(expected).hasSize(6);
    assertThat(brand.missing()).isEqualTo(7);
    assertThat(brand.buckets()).hasSize(2);

    final List<Long> sorted = expected.entrySet().stream().filter(e -> e.getKey() != FacetOracle.MISSING).map(Map.Entry::getValue)
        .sorted((a, b) -> Long.compare(b, a)).toList();
    assertThat(brand.buckets().get(0).count()).isEqualTo(sorted.get(0));
    assertThat(brand.buckets().get(1).count()).isEqualTo(sorted.get(1));
    for (final FacetBucket bucket : brand.buckets())
      assertThat(bucket.count()).isEqualTo(expected.get(bucket.value()));
    assertThat(brand.otherCount()).isEqualTo(sorted.get(2) + sorted.get(3) + sorted.get(4));
  }

  @Test
  @Timeout(120)
  void topNIsDeterministicOnTies() {
    data(1).createSchema(database);
    // inserted in descending value order, so the dictionary's ordinals run against the value order
    for (int b = 4; b >= 1; b--)
      insert("brand-" + b, null, 3);
    insert("brand-0", null, 5);
    FacetOracle.awaitCaughtUp(database);

    final List<List<Object>> runs = new ArrayList<>();
    for (int run = 0; run < 3; run++)
      runs.add(values(execute(null, terms("brand", 3)).get("brand")));
    assertThat(runs.get(0)).containsExactly("brand-0", "brand-1", "brand-2");
    assertThat(runs.get(1)).isEqualTo(runs.get(0));
    assertThat(runs.get(2)).isEqualTo(runs.get(0));

    final TermsFacetSpec byValueDesc = new TermsFacetSpec("brand", "brand", null, null, 2, 1, FacetOrder.VALUE_DESC, null, false, null);
    assertThat(values(execute(null, byValueDesc).get("brand"))).containsExactly("brand-4", "brand-3");
    final TermsFacetSpec byCountAsc = new TermsFacetSpec("brand", "brand", null, null, 2, 1, FacetOrder.COUNT_ASC, null, false, null);
    assertThat(values(execute(null, byCountAsc).get("brand"))).containsExactly("brand-1", "brand-2");
  }

  @Test
  @Timeout(300)
  void includeReturnsZeroCountValues() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 5_000);
    FacetOracle.awaitCaughtUp(database);

    final TermsFacetSpec spec = new TermsFacetSpec("brand", "brand", null, null, 10, 1, FacetOrder.COUNT_DESC, List.of("brand-1", "Nobody"),
        false, null);
    final FacetResult brand = execute(null, spec).get("brand");
    final Map<Object, Long> expected = FacetOracle.expectedCounts(database, TYPE, "brand", null);
    assertThat(values(brand)).containsExactly("brand-1", "Nobody");
    assertThat(brand.buckets().get(0).count()).isEqualTo(expected.get("brand-1")).isPositive();
    assertThat(brand.buckets().get(1).count()).isZero();
    final long total = expected.entrySet().stream().filter(e -> e.getKey() != FacetOracle.MISSING).mapToLong(Map.Entry::getValue).sum();
    assertThat(brand.otherCount()).isEqualTo(total - expected.get("brand-1"));
  }

  @Test
  @Timeout(120)
  void missingBucketRanks() {
    data(1).createSchema(database);
    insert("brand-0", null, 6);
    insert("brand-1", null, 4);
    insert("brand-2", null, 2);
    insert(null, List.of("tag-1"), 3);
    FacetOracle.awaitCaughtUp(database);

    final TermsFacetSpec all = new TermsFacetSpec("brand", "brand", null, null, 10, 1, FacetOrder.COUNT_DESC, null, true, null);
    final FacetResult brand = execute(null, all).get("brand");
    assertThat(values(brand)).containsExactly("brand-0", "brand-1", FacetOracle.MISSING, "brand-2");
    final FacetBucket missing = brand.buckets().get(2);
    assertThat(missing.isMissing()).isTrue();
    assertThat(missing.value()).isNull();
    assertThat(missing.count()).isEqualTo(3);
    assertThat(brand.missing()).isEqualTo(3);
    assertThat(brand.otherCount()).isZero();

    // the missing bucket takes a TOP slot: the value it displaced goes to otherCount
    insert(null, List.of("tag-1"), 2);
    FacetOracle.awaitCaughtUp(database);
    final TermsFacetSpec top2 = new TermsFacetSpec("brand", "brand", null, null, 2, 1, FacetOrder.COUNT_DESC, null, true, null);
    final FacetResult two = execute(null, top2).get("brand");
    assertThat(values(two)).containsExactly("brand-0", FacetOracle.MISSING);
    assertThat(two.buckets().get(1).count()).isEqualTo(5);
    assertThat(two.otherCount()).isEqualTo(4 + 2);
  }

  @Test
  @Timeout(300)
  void polymorphicTypesMergeByValue() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    database.command("sql", "CREATE DOCUMENT TYPE Image EXTENDS " + TYPE);
    FacetOracle.awaitCaughtUp(database);
    data.insertAssets(database, 2_000);
    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.newDocument("Image").set("brand", "brand-" + (i % 20)).set("tags", List.of("tag-" + (i % 7))).save();
    });
    FacetOracle.awaitCaughtUp(database);
    for (final Bucket bucket : database.getSchema().getType("Image").getBuckets(false))
      assertThat(manager().projection(bucket.getFileId())).as("projection of Image bucket " + bucket.getName()).isNotNull();

    for (final String where : WHERES) {
      final FacetResults results = execute(where, terms("brand", ALL), terms("tags", ALL));
      assertThat(counts(results.get("brand"))).as("brand WHERE " + where).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", where));
      assertThat(counts(results.get("tags"))).as("tags WHERE " + where).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "tags", where));
    }
    assertSnapshotsReleased(null, terms("brand", ALL));
  }

  @Test
  @Timeout(300)
  void multiBucketTypeEqualsGroupBy() {
    final ColumnarTestData data = data(3);
    data.createSchema(database);
    data.insertAssets(database, 5_000);
    FacetOracle.awaitCaughtUp(database);
    assertThat(database.getSchema().getType(TYPE).getBuckets(false)).hasSize(3);
    // the same brand is held by the dictionary of every bucket
    for (final Bucket bucket : database.getSchema().getType(TYPE).getBuckets(false))
      try (final var snapshot = manager().projection(bucket.getFileId()).acquire()) {
        assertThat(snapshot.dictionary("brand").ordinalOf("brand-7")).isNotNegative();
      }

    for (final String where : WHERES) {
      final FacetResults results = execute(where, terms("brand", ALL), terms("tags", ALL));
      final Map<Object, Long> brand = FacetOracle.expectedCounts(database, TYPE, "brand", where);
      assertThat(counts(results.get("brand"))).as("brand WHERE " + where).isEqualTo(brand);
      assertThat(counts(results.get("tags"))).as("tags WHERE " + where).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "tags", where));
      // once per document: the brand counts add up to the matched records
      final long documents = database.query("sql", "SELECT count(*) AS c FROM " + TYPE + (where == null ? "" : " WHERE " + where)).next()
          .<Number>getProperty("c").longValue();
      assertThat(counts(results.get("brand")).values().stream().mapToLong(Long::longValue).sum()).isEqualTo(documents);
    }
    assertSnapshotsReleased(null, terms("brand", ALL), terms("tags", ALL));
  }

  @Test
  @Timeout(300)
  void denseAndHashModesGiveSameCounts() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 5_000);
    FacetOracle.awaitCaughtUp(database);

    // ~25 matches against 500 tags: matchCount * 8 < cardinality
    final String selective = "brand = 'brand-3'";
    final FacetResults hash = execute(selective, terms("tags", ALL));
    assertThat(counts(hash.get("tags"))).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "tags", selective));
    final FacetExecutionStats hashStats = hash.executionStats().get("tags");
    assertThat(hashStats.countingMode()).isEqualTo(FacetExecutionStats.CountingMode.HASH);
    // ~25 records hold ~100 distinct tags: the list counter outgrew its presizing, and every byte it grew was reserved first
    assertThat(hashStats.counterBytes()).isGreaterThan(HashFacetCounter.bytesFor(25));
    assertThat(hashStats.budgetBytes()).isGreaterThanOrEqualTo(hashStats.counterBytes());

    final FacetResults dense = execute(null, terms("tags", ALL));
    assertThat(counts(dense.get("tags"))).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "tags", null));
    final FacetExecutionStats stats = dense.executionStats().get("tags");
    assertThat(stats.countingMode()).isEqualTo(FacetExecutionStats.CountingMode.DENSE);
    assertThat(stats.budgetBytes()).isEqualTo(DenseFacetCounter.bytesFor(stats.cardinality()));
    assertThat(stats.segmentsScanned()).isPositive();
    assertThat(stats.source()).isEqualTo("index");
  }

  @Test
  @Timeout(120)
  void dateTermsReturnTheValueTypeGroupByReturns() {
    final ColumnarTestData data = data(1);
    data.dateColumn = true;
    data.createSchema(database);
    data.insertAssets(database, 500);
    FacetOracle.awaitCaughtUp(database);

    for (final String property : List.of("publishDate", "day")) {
      final Map<Object, Long> groupBy = new HashMap<>();
      Class<?> groupByClass = null;
      try (final ResultSet rs = database.query("sql", "SELECT " + property + " AS v, count(*) AS c FROM " + TYPE + " GROUP BY " + property)) {
        while (rs.hasNext()) {
          final var row = rs.next();
          final Object value = row.getProperty("v");
          if (value != null) {
            groupBy.put(value, row.<Number>getProperty("c").longValue());
            groupByClass = value.getClass();
          }
        }
      }
      assertThat(groupByClass).as(property + " values").isNotNull();

      final FacetResult result = execute(null, terms(property, ALL)).get(property);
      final Map<Object, Long> counts = counts(result);
      counts.remove(FacetOracle.MISSING);
      assertThat(counts).as(property).isEqualTo(groupBy);
      for (final FacetBucket bucket : result.buckets())
        assertThat(bucket.value()).as(property + " bucket value").isExactlyInstanceOf(groupByClass);
      assertThat(counts(result).keySet().stream().map(v -> v == FacetOracle.MISSING ? v : oracleKey(property, v)).toList())
          .containsExactlyInAnyOrderElementsOf(FacetOracle.expectedCounts(database, TYPE, property, null).keySet());
    }
  }

  /**
   * Supertype Media, sibling subtypes Clip ({@code ts DATE}), Shot ({@code ts DATETIME}) and Note ({@code ts LONG}), each with its own
   * COLUMNAR index. 2020-01-01 is held by 2 Clips (as a day) and 3 Shots (at midnight UTC); the Notes hold the same numbers as that
   * epoch day and epoch millis.
   */
  private void createSiblingSubtypes() {
    database.command("sql", "CREATE DOCUMENT TYPE Media");
    for (final String[] sibling : new String[][] { { "Clip", "DATE" }, { "Shot", "DATETIME" }, { "Note", "LONG" } }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + sibling[0] + " EXTENDS Media");
      database.command("sql", "CREATE PROPERTY " + sibling[0] + ".ts " + sibling[1]);
      database.command("sql", "CREATE INDEX ON " + sibling[0] + " (ts) COLUMNAR");
    }
    FacetOracle.awaitCaughtUp(database);
    final long day2020 = LocalDate.of(2020, 1, 1).toEpochDay();
    final long millis2020 = day2020 * 86_400_000L;
    database.transaction(() -> {
      for (int i = 0; i < 6; i++)
        database.newDocument("Clip").set("ts", LocalDate.ofEpochDay(day2020 + i % 3)).save();
      for (int i = 0; i < 5; i++)
        database.newDocument("Shot").set("ts", new Date(millis2020 + (i % 2) * 3_600_000L)).save();
      // the same numbers as a DATE's epoch day and a DATETIME's epoch millis: merging raw epochs would fold them into the dates
      for (final long value : new long[] { day2020, day2020, millis2020, 42L })
        database.newDocument("Note").set("ts", value).save();
    });
    FacetOracle.awaitCaughtUp(database);
  }

  /** The facet over every record of the supertype Media. */
  private FacetResult mediaFacet(final TermsFacetSpec spec) {
    final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0);
    try (builder; final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Media")) {
      while (rs.hasNext())
        assertThat(builder.add(rs.next().<Identifiable>getProperty("rid").getIdentity(), 0)).isTrue();
      try (final MatchSet matches = builder.build()) {
        matches.setFacetFilterIndex(new int[] { -1 });
        return executeOn(matches, spec).get(spec.alias());
      }
    }
  }

  @Test
  @Timeout(120)
  void siblingSubtypesWithDifferentPropertyTypesMergeOnTheirValues() {
    createSiblingSubtypes();
    final Map<Object, Long> groupBy = new HashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT ts, count(*) AS c FROM Media GROUP BY ts")) {
      while (rs.hasNext()) {
        final var row = rs.next();
        groupBy.put(row.getProperty("ts"), row.<Number>getProperty("c").longValue());
      }
    }

    final FacetResult ts = mediaFacet(terms("ts", ALL));
    final Map<Object, Long> counts = counts(ts);
    assertThat(counts).as("facet over the supertype against polymorphic GROUP BY").isEqualTo(groupBy);
    for (final FacetBucket bucket : ts.buckets()) {
      final Object same = groupBy.keySet().stream().filter(bucket.value()::equals).findFirst().orElseThrow();
      assertThat(bucket.value()).isExactlyInstanceOf(same.getClass());
    }
  }

  @Test
  @Timeout(120)
  void includeLiteralsAreConvertedPerSubtypeOnTheMergedPath() {
    createSiblingSubtypes();
    final long day2020 = LocalDate.of(2020, 1, 1).toEpochDay();
    // each literal, on each subtype alone (the single-type path), matches what a WHERE on the same literal matches there (the schema
    // conversion of the literal to the property's type); the merged path over the three differently typed columns matches their sum
    final Object[] literals = { "2020-01-01", day2020, 42L };
    final long[] merged = new long[literals.length];
    for (int l = 0; l < literals.length; l++) {
      long alone = 0;
      for (final String subtype : List.of("Clip", "Shot", "Note")) {
        final long included = includedCount(subtype, literals[l]);
        assertThat(included).as(subtype + " INCLUDE " + literals[l]).isEqualTo(whereCount(subtype, literals[l]));
        alone += included;
      }
      merged[l] = alone;
      assertThat(mediaFacet(include(literals[l])).buckets().getFirst().count()).as("merged matches of " + literals[l]).isEqualTo(alone);
    }
    assertThat(merged[0]).as("the date string matches the DATE day and the DATETIME midnight").isEqualTo(2 + 3);
    assertThat(merged[2]).as("42 matches the LONG value only").isEqualTo(1);

    final FacetResult both = mediaFacet(new TermsFacetSpec("ts", "ts", null, null, 10, 1, FacetOrder.COUNT_DESC,
        List.of("2020-01-01", 42L, "Nobody"), false, null));
    assertThat(both.buckets().stream().map(FacetBucket::count).toList()).containsExactly(merged[0], merged[2], 0L);
    assertThat(both.otherCount()).isEqualTo(6 + 5 + 4 - merged[0] - merged[2]);
  }

  /** {@code count()} of a WHERE comparing the property with the literal, the conversion an INCLUDE must match. */
  private long whereCount(final String type, final Object literal) {
    final String sql = "SELECT count() AS c FROM " + type + " WHERE ts = " + (literal instanceof String ? "'" + literal + "'" : literal);
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.hasNext() ? rs.next().<Number>getProperty("c").longValue() : 0L;
    }
  }

  @Test
  @Timeout(120)
  void includeLiteralIsConvertedAsWhereConvertsIt() {
    final ColumnarTestData data = data(1);
    data.dateColumn = true;
    data.createSchema(database);
    data.insertAssets(database, 500);
    FacetOracle.awaitCaughtUp(database);

    final String day;
    try (final ResultSet rs = database.query("sql", "SELECT day FROM " + TYPE + " WHERE day IS NOT NULL LIMIT 1")) {
      day = rs.next().<Object>getProperty("day").toString();
    }
    final long matching;
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + TYPE + " WHERE day = '" + day + "'")) {
      matching = rs.next().<Number>getProperty("c").longValue();
    }
    assertThat(matching).as("records WHERE day = '" + day + "'").isPositive();

    // a date string on a DATE column, and a number given as a string on a LONG column
    final TermsFacetSpec byDay = new TermsFacetSpec("day", "day", null, null, 10, 1, FacetOrder.COUNT_DESC, List.of(day), false, null);
    assertThat(execute(null, byDay).get("day").buckets().getFirst().count()).isEqualTo(matching);

    final long size;
    try (final ResultSet rs = database.query("sql", "SELECT fileSize FROM " + TYPE + " WHERE fileSize IS NOT NULL LIMIT 1")) {
      size = rs.next().<Number>getProperty("fileSize").longValue();
    }
    final long sizeMatching;
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + TYPE + " WHERE fileSize = '" + size + "'")) {
      sizeMatching = rs.next().<Number>getProperty("c").longValue();
    }
    assertThat(sizeMatching).isPositive();
    final TermsFacetSpec bySize = new TermsFacetSpec("fileSize", "fileSize", null, null, 10, 1, FacetOrder.COUNT_DESC, List.of(String.valueOf(size)),
        false, null);
    assertThat(execute(null, bySize).get("fileSize").buckets().getFirst().count()).isEqualTo(sizeMatching);
  }

  private static TermsFacetSpec include(final Object literal) {
    return new TermsFacetSpec("ts", "ts", null, null, 10, 1, FacetOrder.COUNT_DESC, List.of(literal), false, null);
  }

  /** The INCLUDE count of the literal over one subtype alone. */
  private long includedCount(final String subtype, final Object literal) {
    final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0);
    try (builder; final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + subtype)) {
      while (rs.hasNext())
        builder.add(rs.next().<Identifiable>getProperty("rid").getIdentity(), 0);
      try (final MatchSet matches = builder.build()) {
        matches.setFacetFilterIndex(new int[] { -1 });
        return executeOn(matches, include(literal)).get("ts").buckets().getFirst().count();
      }
    }
  }

  @Test
  @Timeout(120)
  void negativeZeroAndZeroAreOneTerm() {
    final ColumnarTestData data = data(1);
    data.doubleColumn = true;
    data.createSchema(database);
    database.transaction(() -> {
      for (final double score : new double[] { -0.0d, 0.0d, 0.0d, 1.5d, -0.0d })
        database.newDocument(TYPE).set("brand", "brand-1").set("score", score).save();
    });
    FacetOracle.awaitCaughtUp(database);

    final FacetResult score = execute(null, terms("score", ALL)).get("score");
    assertThat(counts(score)).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "score", null));
    assertThat(score.buckets()).hasSize(2);
    assertThat(score.buckets().getFirst().count()).isEqualTo(4);
    long groups = 0;
    try (final ResultSet rs = database.query("sql", "SELECT score, count(*) AS c FROM " + TYPE + " GROUP BY score")) {
      while (rs.hasNext()) {
        rs.next();
        groups++;
      }
    }
    assertThat(groups).as("GROUP BY keys -0.0 and 0.0 as one group").isEqualTo(2);
  }

  @Test
  @Timeout(300)
  void numericTermsGrowthIsChargedToTheBudget() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 5_000);
    FacetOracle.awaitCaughtUp(database);
    final long distinct = FacetOracle.expectedCounts(database, TYPE, "fileSize", null).size();
    assertThat(distinct).as("distinct file sizes, more than the counter is presized for").isGreaterThan(TermsCollector.NUMERIC_EXPECTED_TERMS);

    final long saved = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValueAsLong();
    final long headroom = 48 * 1024;
    try (final MatchSet matches = matches(null, 1)) {
      // what the match set holds, plus room for the presized counter but not for its growth to every distinct size
      final long held = saved - FacetMemoryBudget.instance().available();
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(held + headroom);
      assertThat(FacetMemoryBudget.instance().available()).isEqualTo(headroom);
      assertThat(LongTermsCounter.bytesFor(TermsCollector.NUMERIC_EXPECTED_TERMS)).isLessThan(headroom);

      final FacetResult fileSize = executeOn(matches, terms("fileSize", ALL)).get("fileSize");
      assertThat(fileSize.source()).isEqualTo(FacetResult.UNAVAILABLE);
      assertThat(fileSize.reason()).isEqualTo("memory budget");
      assertThat(FacetMemoryBudget.instance().available()).as("everything the facet reserved is released").isEqualTo(headroom);

      // with room for the growth the same facet is counted, and never holds more than it reserved
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(saved);
      final FacetResults counted = executeOn(matches, terms("fileSize", ALL));
      assertThat(counted.get("fileSize").source()).isEqualTo("index");
      final FacetExecutionStats stats = counted.executionStats().get("fileSize");
      assertThat(stats.budgetBytes()).isGreaterThanOrEqualTo(stats.counterBytes());
      assertThat(stats.budgetBytes()).isGreaterThan(LongTermsCounter.bytesFor(TermsCollector.NUMERIC_EXPECTED_TERMS));
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(saved);
    }
  }

  @Test
  @Timeout(300)
  void aRefusedListTopUpMidSegmentMakesTheFacetUnavailable() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 5_000);
    FacetOracle.awaitCaughtUp(database);

    final long saved = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValueAsLong();
    final long savedWait = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong();
    final long headroom = 1024;
    // ~25 records against 500 tags: a hash counter presized for 25 ordinals, which the ~100 distinct tags outgrow inside the segment
    try (final MatchSet matches = matches("brand = 'brand-3'", 1)) {
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(0L);
      final long held = saved - FacetMemoryBudget.instance().available();
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(held + headroom);
      assertThat(HashFacetCounter.bytesFor((int) matches.size())).isLessThanOrEqualTo(headroom);

      final FacetResults results = executeOn(matches, terms("tags", ALL));
      assertThat(results.get("tags").source()).isEqualTo(FacetResult.UNAVAILABLE);
      assertThat(results.get("tags").reason()).isEqualTo("memory budget");
      assertThat(results.executionStats().get("tags").countingMode()).isEqualTo(FacetExecutionStats.CountingMode.HASH);
      assertThat(FacetMemoryBudget.instance().available()).as("everything the facet reserved is released").isEqualTo(headroom);
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(saved);
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(savedWait);
    }
  }

  @Test
  @Timeout(120)
  void everyKindHasACollectorAndReleasesTheSnapshots() {
    // plan 5 Task 9: STATS, the last kind without a collector, is computed like the others
    data(1).createSchema(database);
    insert("brand-1", null, 1);
    FacetOracle.awaitCaughtUp(database);
    final StatsFacetSpec stats = new StatsFacetSpec("s", "fileSize", null);
    assertThat(execute(null, stats).get("s").stats()).isEqualTo(FacetOracle.stats(database, TYPE, "fileSize", null));
    assertSnapshotsReleased(null, terms("brand", ALL), stats);
  }
}
