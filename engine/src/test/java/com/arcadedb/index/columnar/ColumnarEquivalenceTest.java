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
package com.arcadedb.index.columnar;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Task 20: the columnar projection answers the same facet counts as polymorphic GROUP BY under static loads, randomized mutations,
 * an apply lag, a WHERE clause, subtype buckets, schema events, many flushes and merges, and several buckets per type.
 */
class ColumnarEquivalenceTest extends TestHelper {
  private static final String       TYPE            = ColumnarTestData.TYPE;
  private static final List<String> ALL_PROPERTIES  = List.of("brand", "tags", "fileSize", "publishDate", "author");
  private static final List<String> ORDINAL_FACETS  = List.of("brand", "tags", "author");
  private static final List<String> LONG_FACETS     = List.of("fileSize", "publishDate");
  private static final int          ROUNDS          = 20;
  private static final int          OPS_PER_ROUND   = 2_000;
  private static final int          LAG_CHECK_EVERY = 200;
  private static final long         MERGE_WAIT_MS   = 60_000;

  @Test
  void staticLoadAtLagZero() {
    final ColumnarTestData data = new ColumnarTestData(20);
    data.createSchema(database);
    data.insertAssets(database, 20_000);

    for (final String property : ALL_PROPERTIES)
      FacetOracle.assertEquivalent(database, TYPE, property, null);
    assertThat(count("SELECT count(*) AS c FROM " + TYPE)).isEqualTo(20_000L);
  }

  static Stream<Arguments> randomizedSeeds() {
    // vertices exercise edges in the record pages; documents exercise the R21 placeholders (the fixture seals document pages only)
    return Stream.of(Arguments.of(1L, true), Arguments.of(7L, true), Arguments.of(42L, false), Arguments.of(1234L, false));
  }

  @ParameterizedTest(name = "seed {0}, asVertices {1}")
  @MethodSource("randomizedSeeds")
  @Tag("slow")
  void randomizedOperationsAtLagZero(final long seed, final boolean asVertices) {
    final ColumnarTestData data = new ColumnarTestData(seed);
    data.bigRecordRatio = 0.05;
    data.deleteRatio = 0.1;
    data.reuseRatio = 0.05;
    data.externalBody = true;
    data.asVertices = asVertices;
    if (asVertices)
      data.edgesPerVertex = 3;
    else
      data.placeholderRatio = 0.02;
    data.createSchema(database);
    data.insertAssets(database, OPS_PER_ROUND);
    if (!asVertices)
      assertThat(layoutCount("totalPlaceholderRecords")).isPositive();

    for (int round = 0; round < ROUNDS; round++) {
      data.mutate(database, OPS_PER_ROUND);
      assertRound(round);
    }
  }

  @Test
  @Tag("slow")
  void randomizedWhileLagging() throws Exception {
    final ColumnarTestData data = new ColumnarTestData(99);
    data.deleteRatio = 0.1;
    data.reuseRatio = 0.05;
    data.createSchema(database);
    data.insertAssets(database, 5_000);
    FacetOracle.assertEquivalent(database, TYPE, "brand", null);

    for (int round = 0; round < 5; round++) {
      data.mutate(database, OPS_PER_ROUND / 2);
      try (final AutoCloseable hold = manager().suspendApply()) {
        // read once the hold is in place: suspendApply() waits for a running pass, which may count itself until then
        final long passes = manager().getStats().applyPasses();
        for (int done = 0; done < OPS_PER_ROUND / 2; done += LAG_CHECK_EVERY) {
          data.mutate(database, LAG_CHECK_EVERY);
          for (final String property : ALL_PROPERTIES)
            FacetOracle.assertEquivalentWhileLagging(database, TYPE, property, null);
        }
        // the hold really held: the mutations of the half round wait as pending pages, none was applied
        assertThat(manager().getStats().applyPasses()).isEqualTo(passes);
        assertThat(pendingPositions()).isPositive();
      }
      assertRound(round);
    }
  }

  @Test
  void withWhereClause() {
    final ColumnarTestData data = new ColumnarTestData(11);
    data.deleteRatio = 0.1;
    data.createSchema(database);
    data.insertAssets(database, 10_000);
    data.mutate(database, 2_000);

    // the brief's literal condition ('b7' is no brand of the fixture), then the same with a brand that exists
    for (final String where : List.of("fileSize > 1000000 AND brand <> 'b7'", "fileSize > 1000000 AND brand <> 'brand-7'")) {
      final long matched = count("SELECT count(*) AS c FROM " + TYPE + " WHERE " + where);
      assertThat(matched).isPositive().isLessThan(count("SELECT count(*) AS c FROM " + TYPE));
      for (final String property : ALL_PROPERTIES)
        FacetOracle.assertEquivalent(database, TYPE, property, where);
    }
    assertThat(FacetOracle.expectedCounts(database, TYPE, "brand", "fileSize > 1000000 AND brand <> 'brand-7'")).doesNotContainKey(
        "brand-7");
    FacetOracle.assertEquivalent(database, TYPE, "brand", null);
  }

  @Test
  void polymorphicSubtypeBucketsAreProjected() {
    final ColumnarTestData data = new ColumnarTestData(5);
    data.asVertices = true;
    data.createSchema(database);
    data.insertAssets(database, 3_000);
    database.command("sql", "CREATE VERTEX TYPE Image EXTENDS " + TYPE);

    database.transaction(() -> {
      for (int i = 0; i < 1_500; i++) {
        final MutableVertex image = database.newVertex("Image");
        image.set("brand", "image-brand-" + (i % 7));
        image.set("tags", List.of("tag-" + (i % 13), "image"));
        image.set("fileSize", 2_048L * (i % 100));
        image.set("publishDate", new Date(1_600_000_000_000L + i * 86_400_000L));
        if (i % 4 != 0)
          image.set("author", "author-" + (i % 50));
        image.save();
      }
    });
    data.mutate(database, 1_000);

    final List<Integer> imageBuckets = new ArrayList<>();
    for (final Bucket bucket : database.getSchema().getType("Image").getBuckets(false))
      imageBuckets.add(bucket.getFileId());
    FacetOracle.awaitCaughtUp(database);
    for (final int bucketId : imageBuckets)
      assertThat(manager().projection(bucketId)).as("projection of Image bucket %s", bucketId).isNotNull();

    for (final String property : ALL_PROPERTIES)
      FacetOracle.assertEquivalent(database, TYPE, property, null);
    // the subtype's values are in the supertype's facets, exactly as polymorphic GROUP BY counts them
    final Map<Object, Long> tags = FacetOracle.projectionCounts(database, TYPE, "tags", null, false);
    assertThat(tags.get("image")).isEqualTo(1_500L);
    assertThat(FacetOracle.projectionCounts(database, TYPE, "brand", null, false).get("image-brand-0")).isEqualTo(215L);
    FacetOracle.assertEquivalent(database, TYPE, "brand", "@type = 'Image'");
  }

  @Test
  void schemaEventsInTheMiddleOfOperations() throws Exception {
    final ColumnarTestData data = new ColumnarTestData(17);
    data.deleteRatio = 0.1;
    data.reuseRatio = 0.05;
    data.createSchema(database);
    data.insertAssets(database, 5_000);
    data.mutate(database, 1_000);
    assertAllEquivalent(TYPE);

    // a type rename (DDL refuses renaming an indexed property): the projections follow the type, and the index is resolved, dropped
    // and recreated by its NEW name with no reload in between (the registry is re-keyed by the rename)
    database.command("sql", "ALTER TYPE " + TYPE + " NAME Item");
    assertAllEquivalent("Item");
    // the renamed index keeps the COLUMNAR suffix, so it cannot take the name of a key index on the same property
    assertThat(columnarIndexName("Item", "brand")).isEqualTo("Item[brand]_columnar");
    assertThat(database.getSchema().existsIndex("Asset[brand]_columnar")).isFalse();
    database.command("sql", "DROP INDEX `Item[brand]_columnar`");
    assertThat(database.getSchema().existsIndex("Item[brand]_columnar")).isFalse();
    database.transaction(() -> database.command("sql", "UPDATE Item SET brand = 'renamed-brand' WHERE fileSize < 400000"));
    database.command("sql", "CREATE INDEX ON Item (brand) COLUMNAR");
    assertAllEquivalent("Item");
    assertThat(FacetOracle.projectionCounts(database, "Item", "brand", null, false)).containsKey("renamed-brand");
    // renamed back only because the fixture writes type Asset; the drop below then uses the name of the double rename
    database.command("sql", "ALTER TYPE Item NAME " + TYPE);
    assertThat(columnarIndexName(TYPE, "brand")).isEqualTo("Asset[brand]_columnar");
    data.mutate(database, 1_000);
    assertAllEquivalent(TYPE);

    // a new bucket: the round-robin inserts of the next round land in it too
    database.command("sql", "CREATE BUCKET Asset_extra");
    database.command("sql", "ALTER TYPE " + TYPE + " BUCKET +Asset_extra");
    final int extraId = database.getSchema().getBucketByName("Asset_extra").getFileId();
    data.mutate(database, 2_000);
    assertAllEquivalent(TYPE);
    assertThat(count("SELECT count(*) AS c FROM bucket:Asset_extra")).isPositive();
    assertThat(manager().projection(extraId)).isNotNull();

    // drop an index and recreate it between rounds: the build converges on records mutated meanwhile
    database.command("sql", "DROP INDEX `Asset[brand]_columnar`");
    data.mutate(database, 1_000);
    for (final String property : List.of("tags", "fileSize", "publishDate", "author"))
      FacetOracle.assertEquivalent(database, TYPE, property, null);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) COLUMNAR");
    data.mutate(database, 1_000);
    assertAllEquivalent(TYPE);
  }

  @Test
  void multipleFlushesAndMergesKeepEquivalence() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_FLUSH_MAX_ROWS, 1_000);
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_MERGE_MAX_SEGMENTS, 3);
    final ColumnarTestData data = new ColumnarTestData(23);
    data.deleteRatio = 0.1;
    data.reuseRatio = 0.05;
    data.createSchema(database);
    data.insertAssets(database, 10_000);

    for (int round = 0; round < 5; round++) {
      data.mutate(database, OPS_PER_ROUND);
      assertRound(round);
    }
    final ColumnarStats stats = manager().getStats();
    // V-M22: the background maintenance flushed by row count and merged the segments beyond the configured maximum
    final long deadline = System.currentTimeMillis() + MERGE_WAIT_MS;
    while (stats.mergeCount(TYPE) == 0 && System.currentTimeMillis() < deadline)
      Thread.sleep(10);
    assertThat(stats.flushCount(TYPE)).isGreaterThan(3);
    assertThat(stats.mergeCount(TYPE)).isPositive();
    assertAllEquivalent(TYPE);
  }

  @Test
  void multiBucketTypeAtLagZero() {
    final ColumnarTestData data = new ColumnarTestData(31);
    data.buckets = 3;
    data.deleteRatio = 0.1;
    data.createSchema(database);
    data.insertAssets(database, 9_000);
    data.mutate(database, 3_000);

    final List<Bucket> buckets = database.getSchema().getType(TYPE).getBuckets(false);
    assertThat(buckets).hasSize(3);
    FacetOracle.awaitCaughtUp(database);
    final List<GlobalDictionary> dictionaries = new ArrayList<>();
    for (final Bucket bucket : buckets) {
      assertThat(count("SELECT count(*) AS c FROM bucket:" + bucket.getName())).isPositive();
      final ColumnarProjection projection = manager().projection(bucket.getFileId());
      assertThat(projection).isNotNull();
      try (final ColumnarSnapshot snapshot = projection.acquire()) {
        final GlobalDictionary brands = snapshot.dictionary("brand");
        assertThat(brands).isNotNull();
        assertThat(dictionaries).noneMatch(d -> d == brands);
        dictionaries.add(brands);
      }
    }
    for (final String property : List.of("brand", "tags", "fileSize"))
      FacetOracle.assertEquivalent(database, TYPE, property, null);
  }

  // ------------------------------------------------------------------------------------------------------------------ helpers

  /** One round's checks: no-WHERE equivalence (with the oracle's live-set check) for the ordinal facets, histograms for the LONG ones. */
  private void assertRound(final int round) {
    FacetOracle.awaitCaughtUp(database);
    for (final String property : ORDINAL_FACETS)
      FacetOracle.assertEquivalent(database, TYPE, property, null);
    for (final String property : LONG_FACETS)
      assertThat(FacetOracle.projectionHistogram(database, TYPE, property, null)).as("round %s, %s", round, property)
          .isEqualTo(FacetOracle.expectedHistogram(database, TYPE, property, null));
  }

  private void assertAllEquivalent(final String type) {
    for (final String property : ALL_PROPERTIES)
      FacetOracle.assertEquivalent(database, type, property, null);
  }

  private String columnarIndexName(final String type, final String property) {
    for (final TypeIndex index : database.getSchema().getType(type).getAllIndexes(false))
      if (index.getType() == Schema.INDEX_TYPE.COLUMNAR && index.getPropertyNames().equals(List.of(property)))
        return index.getName();
    throw new IllegalArgumentException("No COLUMNAR index on " + type + "." + property);
  }

  private long pendingPositions() {
    long pending = 0;
    for (final Bucket bucket : database.getSchema().getType(TYPE).getBuckets(true))
      try (final ColumnarSnapshot snapshot = manager().projection(bucket.getFileId()).acquire()) {
        pending += snapshot.pendingPositions().cardinality();
      }
    return pending;
  }

  private long layoutCount(final String name) {
    return (Long) BucketPageLayoutTestSupport.bucketStats(database, TYPE).get(name);
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }
}
