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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Task 19: the shared fixtures {@link FacetOracle} and {@link ColumnarTestData} check out against themselves and can fail. */
class FacetOracleSelfTest extends TestHelper {

  @Test
  void oracleAgreesWithItselfOnStaticData() {
    final ColumnarTestData data = new ColumnarTestData(42);
    data.createSchema(database);
    data.insertAssets(database, 5_000);

    for (final String property : List.of("brand", "tags", "fileSize", "publishDate", "author"))
      FacetOracle.assertEquivalent(database, "Asset", property, null);
    FacetOracle.assertEquivalent(database, "Asset", "brand", "fileSize < 300000");
    FacetOracle.assertEquivalent(database, "Asset", "tags", "brand = 'brand-7'");

    // BY ITEM is inferred: documents per tag, computed here without SQL, records with no tag counted as missing
    final Map<Object, Long> perTag = new HashMap<>();
    for (final Document asset : allAssets()) {
      final List<Object> tags = asset.getList("tags");
      if (tags == null || tags.isEmpty())
        perTag.merge(FacetOracle.MISSING, 1L, Long::sum);
      else
        for (final Object tag : new HashSet<>(tags))
          perTag.merge(tag, 1L, Long::sum);
    }
    assertThat(FacetOracle.expectedCounts(database, "Asset", "tags", null)).isEqualTo(perTag);

    // author is null on some records: they are the missing key on both sides
    final Map<Object, Long> authors = FacetOracle.expectedCounts(database, "Asset", "author", null);
    assertThat(authors.get(FacetOracle.MISSING)).isPositive();
    assertThat(authors.values().stream().mapToLong(Long::longValue).sum()).isEqualTo(5_000L);

    // a LONG column: the histogram is the counts without the missing key
    final Map<Long, Long> histogram = FacetOracle.expectedHistogram(database, "Asset", "fileSize", null);
    assertThat(histogram.values().stream().mapToLong(Long::longValue).sum()).isEqualTo(5_000L);
    assertThat(FacetOracle.projectionHistogram(database, "Asset", "fileSize", null)).isEqualTo(histogram);

    // a DATETIME column is keyed by epoch millis: anchored to Date.getTime() of the records, computed here without SQL
    final Map<Long, Long> perMillis = new HashMap<>();
    for (final Document asset : allAssets())
      perMillis.merge(asset.getDate("publishDate").getTime(), 1L, Long::sum);
    assertThat(FacetOracle.expectedHistogram(database, "Asset", "publishDate", null)).isEqualTo(perMillis);
    assertThat(FacetOracle.projectionHistogram(database, "Asset", "publishDate", null)).isEqualTo(perMillis);
  }

  @Test
  void staleLiveRowIsDetected() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, TimeUnit.HOURS.toMillis(1));
    final ColumnarTestData data = new ColumnarTestData(4);
    data.createSchema(database);
    final List<RID> rids = data.insertAssets(database, 200);
    FacetOracle.assertEquivalent(database, "Asset", "brand", null);

    // a row the projection holds live for a slot with no record (a delete it never applied): no value differs for any record
    final RID last = rids.getLast();
    final long stale = last.getPosition() + 1;
    assertThat(database.existsRecord(new RID(last.getBucketId(), stale))).isFalse();
    try (final AutoCloseable hold = manager().suspendApply()) {
      injectOverlayRow(last.getBucketId(), stale, "brand-0");
      assertThatThrownBy(() -> FacetOracle.assertEquivalent(database, "Asset", "brand", null)).isInstanceOf(AssertionError.class)
          .hasMessageContaining("live rows with no record").hasMessageContaining(String.valueOf(stale));
      // with a WHERE the live set is not checked, the counts are: the stale row matches nothing
      FacetOracle.assertEquivalent(database, "Asset", "brand", "fileSize >= 0");
    }
  }

  @Test
  void liveRowOfAnEmptiedBucketIsDetected() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, TimeUnit.HOURS.toMillis(1));
    final ColumnarTestData data = new ColumnarTestData(6);
    data.buckets = 2;
    data.createSchema(database);
    data.insertAssets(database, 200);
    final Bucket emptied = database.getSchema().getType("Asset").getBuckets(false).getLast();
    database.transaction(() -> database.command("sql", "DELETE FROM bucket:" + emptied.getName()));
    FacetOracle.assertEquivalent(database, "Asset", "brand", null);
    assertThat(count("SELECT count(@rid) AS c FROM bucket:" + emptied.getName())).isZero();

    try (final AutoCloseable hold = manager().suspendApply()) {
      injectOverlayRow(emptied.getFileId(), 0, "brand-0");
      assertThatThrownBy(() -> FacetOracle.assertEquivalent(database, "Asset", "brand", null)).isInstanceOf(AssertionError.class)
          .hasMessageContaining("bucket " + emptied.getFileId());
    }
  }

  @Test
  void oracleDetectsADeliberateDivergence() throws Exception {
    // no background flush: the corrupted row stays in the overlay
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, TimeUnit.HOURS.toMillis(1));
    final ColumnarTestData data = new ColumnarTestData(3);
    data.createSchema(database);
    final List<RID> rids = data.insertAssets(database, 300);
    FacetOracle.assertEquivalent(database, "Asset", "brand", null);

    final RID victim = rids.get(17);
    final String brand = victim.asDocument().getString("brand");
    final String otherBrand = brand.equals("brand-0") ? "brand-1" : "brand-0";
    try (final AutoCloseable hold = manager().suspendApply()) {
      corruptOverlayBrand(victim, otherBrand);

      final Map<Object, Long> expected = FacetOracle.expectedCounts(database, "Asset", "brand", null);
      final Map<Object, Long> projected = FacetOracle.projectionCounts(database, "Asset", "brand", null, false);
      assertThat(projected.getOrDefault(brand, 0L)).isEqualTo(expected.get(brand) - 1);
      assertThat(projected.getOrDefault(otherBrand, 0L)).isEqualTo(expected.getOrDefault(otherBrand, 0L) + 1);
      assertThatThrownBy(() -> FacetOracle.assertEquivalent(database, "Asset", "brand", null)).isInstanceOf(AssertionError.class)
          .hasMessageContaining("brand");
    }
  }

  @Test
  void seededDataIsReproducible() {
    final ColumnarTestData first = new ColumnarTestData(11);
    first.createSchema(database);
    first.insertAssets(database, 1_000);
    final Map<Object, Long> counts = FacetOracle.expectedCounts(database, "Asset", "brand", null);

    final Database second = TestHelper.createDatabase(getDatabasePath() + "-second");
    try {
      final ColumnarTestData same = new ColumnarTestData(11);
      same.createSchema(second);
      same.insertAssets(second, 1_000);
      assertThat(FacetOracle.expectedCounts(second, "Asset", "brand", null)).isEqualTo(counts);
    } finally {
      second.drop();
    }

    // the seed is what makes it reproducible: another seed draws other brands
    final Database third = TestHelper.createDatabase(getDatabasePath() + "-third");
    try {
      final ColumnarTestData other = new ColumnarTestData(12);
      other.createSchema(third);
      other.insertAssets(third, 1_000);
      assertThat(FacetOracle.expectedCounts(third, "Asset", "brand", null)).isNotEqualTo(counts);
    } finally {
      third.drop();
    }
  }

  @Test
  void awaitCaughtUpWaitsForPendingPages() throws Exception {
    final ColumnarTestData data = new ColumnarTestData(5);
    data.createSchema(database);
    data.insertAssets(database, 100);
    FacetOracle.awaitCaughtUp(database);
    final ColumnarIndexManager manager = manager();
    final ColumnarProjection projection = manager.projection(bucketId());

    final AutoCloseable hold = manager.suspendApply();
    final AtomicBoolean releasing = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread releaser = new Thread(() -> {
      try {
        // a hold duration, not a bound: the assertion below is the ordering
        Thread.sleep(200);
        releasing.set(true);
        hold.close();
      } catch (final Throwable e) {
        failure.set(e);
      }
    }, "facet-oracle-releaser");
    try {
      data.insertAssets(database, 50);
      assertThat(projection.tracker().oldestPendingEpochStartMs()).isPositive();
      releaser.start();

      FacetOracle.awaitCaughtUp(database);

      assertThat(releasing.get()).as("awaitCaughtUp returned while apply was still suspended").isTrue();
      assertThat(projection.tracker().oldestPendingEpochStartMs()).as("pages still pending after awaitCaughtUp").isZero();
    } finally {
      // idempotent: never leave apply suspended into the teardown
      hold.close();
      if (releaser.isAlive() || releasing.get())
        releaser.join();
    }
    assertThat(failure.get()).isNull();
    FacetOracle.assertEquivalent(database, "Asset", "brand", null);
  }

  @Test
  void lagIsPatchedFromThePendingRecords() throws Exception {
    final ColumnarTestData data = new ColumnarTestData(9);
    data.createSchema(database);
    data.insertAssets(database, 200);
    FacetOracle.awaitCaughtUp(database);

    try (final AutoCloseable hold = manager().suspendApply()) {
      data.insertAssets(database, 40);
      database.transaction(() -> database.newDocument("Asset").set("brand", "never-seen").set("tags", List.of("new-tag")).save());
      final Map<Object, Long> expected = FacetOracle.expectedCounts(database, "Asset", "brand", null);
      assertThat(FacetOracle.projectionCounts(database, "Asset", "brand", null, false)).isNotEqualTo(expected);
      assertThat(FacetOracle.projectionCounts(database, "Asset", "brand", null, true)).isEqualTo(expected);
      for (final String property : List.of("brand", "tags", "fileSize", "publishDate"))
        FacetOracle.assertEquivalentWhileLagging(database, "Asset", property, null);
    }
    FacetOracle.assertEquivalent(database, "Asset", "tags", null);
  }

  @Test
  void duplicateTagInOneRecordCountsOnce() {
    new ColumnarTestData(1).createSchema(database);
    database.transaction(() -> database.newDocument("Asset").set("brand", "x").set("tags", List.of("a", "a", "b")).save());
    FacetOracle.awaitCaughtUp(database);

    final Map<Object, Long> once = Map.of("a", 1L, "b", 1L);
    assertThat(FacetOracle.expectedCounts(database, "Asset", "tags", null)).isEqualTo(once);
    assertThat(FacetOracle.projectionCounts(database, "Asset", "tags", null, false)).isEqualTo(once);
  }

  @Test
  void seededDataContainsInRecordDuplicates() {
    final ColumnarTestData data = new ColumnarTestData(21);
    data.createSchema(database);
    data.insertAssets(database, 1_000);

    int withDuplicates = 0;
    for (final Document asset : allAssets()) {
      final List<Object> tags = asset.getList("tags");
      if (tags != null && new HashSet<>(tags).size() < tags.size())
        withDuplicates++;
    }
    assertThat(withDuplicates).isPositive();
  }

  @Test
  void placeholderRatioProducesPlaceholders() {
    final ColumnarTestData data = new ColumnarTestData(17);
    data.placeholderRatio = 0.05;
    data.createSchema(database);
    data.insertAssets(database, 200);

    // one placeholder per 1 / 0.05 = 20 drawn records
    assertThat((Long) BucketPageLayoutTestSupport.bucketStats(database, "Asset").get("totalPlaceholderRecords")).isEqualTo(200L / 20);
    FacetOracle.assertEquivalent(database, "Asset", "brand", null);
    FacetOracle.assertEquivalent(database, "Asset", "tags", null);
  }

  @Test
  void mutationsKeepTheProjectionEquivalent() {
    final ColumnarTestData data = new ColumnarTestData(31);
    data.deleteRatio = 0.1;
    data.reuseRatio = 0.1;
    data.bigRecordRatio = 0.05;
    data.createSchema(database);
    final List<RID> inserted = data.insertAssets(database, 500);
    data.mutate(database, 600);

    assertThat(count("SELECT count(@rid) AS c FROM Asset")).isNotEqualTo(inserted.size());
    for (final String property : List.of("brand", "tags", "fileSize", "publishDate", "author"))
      FacetOracle.assertEquivalent(database, "Asset", property, null);
  }

  @Test
  void verticesWithEdgesAndExternalBodies() {
    final ColumnarTestData data = new ColumnarTestData(37);
    data.asVertices = true;
    data.edgesPerVertex = 2;
    data.externalBody = true;
    data.deleteRatio = 0.05;
    data.createSchema(database);
    data.insertAssets(database, 300);
    data.mutate(database, 300);

    assertThat(count("SELECT count(@rid) AS c FROM " + ColumnarTestData.EDGE_TYPE)).isPositive();
    for (final String property : List.of("brand", "tags", "fileSize"))
      FacetOracle.assertEquivalent(database, "Asset", property, null);
  }

  @Test
  void multipleBucketsAreAllCounted() {
    final ColumnarTestData data = new ColumnarTestData(41);
    data.buckets = 3;
    data.createSchema(database);
    data.insertAssets(database, 600);

    assertThat(database.getSchema().getType("Asset").getBuckets(false)).hasSize(3);
    for (final String property : List.of("brand", "tags"))
      FacetOracle.assertEquivalent(database, "Asset", property, null);
  }

  /**
   * Stages a live overlay row at {@code position} with only a brand, as a buggy apply pass would, keeping the other rows of its page,
   * and publishes it.
   */
  private void injectOverlayRow(final int bucketId, final long position, final String brand) {
    final ColumnarProjection projection = manager().projection(bucketId);
    final int max = projection.maxRecordsInPage();
    final int page = (int) (position / max);
    final ColumnDefinition[] columns = projection.columns();
    final RowValues values = new RowValues(columns.length);
    for (int k = 0; k < columns.length; k++)
      if (columns[k].property().equals("brand")) {
        values.present[k] = true;
        values.ordinal[k] = projection.dictionary("brand").getOrAdd(brand);
      }
    projection.applyLock().lock();
    try {
      final OverlayPageBlock block = projection.overlay().page(page);
      final RowBuffer rows = new RowBuffer(max, columns);
      if (block != null)
        rows.seed(block);
      rows.put((int) (position % max), values);
      projection.overlay().replacePage(OverlayPageBlock.build(page, max, columns, block != null ? block.pageVersion() : 1, rows));
      projection.publishOverlay();
    } finally {
      projection.applyLock().unlock();
    }
  }

  /** Rewrites the brand of the record's overlay row to {@code brand}, as a buggy apply pass would, and publishes it. */
  private void corruptOverlayBrand(final RID rid, final String brand) {
    final ColumnarProjection projection = manager().projection(rid.getBucketId());
    final int max = projection.maxRecordsInPage();
    final int page = (int) (rid.getPosition() / max);
    final int slot = (int) (rid.getPosition() % max);
    final ColumnDefinition[] columns = projection.columns();
    int brandColumn = -1;
    for (int k = 0; k < columns.length; k++)
      if (columns[k].property().equals("brand"))
        brandColumn = k;
    final int ordinal = projection.dictionary("brand").ordinalOf(brand);
    assertThat(ordinal).isNotNegative();

    projection.applyLock().lock();
    try {
      final OverlayPageBlock block = projection.overlay().page(page);
      assertThat(block).as("the record's row must be in the overlay").isNotNull();
      final int row = block.row(slot);
      assertThat(row).isNotNegative();
      final RowValues values = new RowValues(columns.length);
      block.readRow(row, values);
      values.ordinal[brandColumn] = ordinal;
      final RowBuffer rows = new RowBuffer(max, columns);
      rows.seed(block);
      rows.put(slot, values);
      projection.overlay().replacePage(OverlayPageBlock.build(page, max, columns, block.pageVersion(), rows));
      projection.publishOverlay();
    } finally {
      projection.applyLock().unlock();
    }
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private int bucketId() {
    return database.getSchema().getType("Asset").getBuckets(false).getFirst().getFileId();
  }

  private Set<Document> allAssets() {
    final Set<Document> assets = new HashSet<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM Asset")) {
      while (rs.hasNext())
        assets.add(rs.next().getRecord().orElseThrow().asDocument());
    }
    return assets;
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final Result row = rs.next();
      return row.<Number>getProperty("c").longValue();
    }
  }
}
