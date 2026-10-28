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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.ColumnarIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Schema events and the reconcile of the projections with the COLUMNAR indexes of the schema (spec 6.4). */
class ColumnarReconcilerTest extends TestHelper {
  private static final long WAIT_MS = 60_000;

  @Override
  protected void endTest() {
    ColumnarTestHooks.killPoint = null;
  }

  // ------------------------------------------------------------------------------------------------------------------ pure diff

  @Test
  void dropLastColumnDropsProjection() {
    final ColumnDefinition brand = column(100, "brand", Type.STRING);
    final ColumnDefinition color = column(101, "color", Type.STRING);

    assertThat(ColumnarReconciler.diff(Map.of(10, new ColumnDefinition[] { brand }), Map.of(), b -> false))
        .containsExactly(new ColumnarSchemaEvent(ColumnarSchemaEvent.Kind.DROP_PROJECTION, 10, -1, null, null));
    // not the last one: the column only
    assertThat(ColumnarReconciler.diff(Map.of(10, new ColumnDefinition[] { brand, color }), Map.of(10, new ColumnDefinition[] { color }),
        b -> false)).containsExactly(new ColumnarSchemaEvent(ColumnarSchemaEvent.Kind.DROP_COLUMN, 10, 100, null, null));
    // an empty column array is no column at all
    assertThat(ColumnarReconciler.diff(Map.of(10, new ColumnDefinition[] { brand }), Map.of(10, new ColumnDefinition[0]), b -> false))
        .containsExactly(new ColumnarSchemaEvent(ColumnarSchemaEvent.Kind.DROP_PROJECTION, 10, -1, null, null));
  }

  @Test
  void renameKeepsData() {
    final Map<Integer, ColumnDefinition[]> before = Map.of(10, new ColumnDefinition[] { column(100, "brand", Type.STRING) });
    final Map<Integer, ColumnDefinition[]> after = Map.of(10, new ColumnDefinition[] { column(100, "maker", Type.STRING) });

    assertThat(ColumnarReconciler.diff(before, after, b -> false))
        .containsExactly(new ColumnarSchemaEvent(ColumnarSchemaEvent.Kind.RENAME, 10, 100, null, null));
    // nothing changed: nothing reported
    assertThat(ColumnarReconciler.diff(before, before, b -> false)).isEmpty();
  }

  @Test
  void retypeRebuildsWithSchemaReason() {
    final Map<Integer, ColumnDefinition[]> before = Map.of(10, new ColumnDefinition[] { column(100, "size", Type.INTEGER) });
    final Map<Integer, ColumnDefinition[]> after = Map.of(10, new ColumnDefinition[] { column(100, "size", Type.LONG) });
    // renamed AND retyped: the rebuild covers the rename
    final Map<Integer, ColumnDefinition[]> both = Map.of(10, new ColumnDefinition[] { column(100, "weight", Type.STRING) });

    final ColumnarSchemaEvent retype = new ColumnarSchemaEvent(ColumnarSchemaEvent.Kind.RETYPE_COLUMN, 10, 100, ColumnarRebuilder.SCHEMA,
        null);
    assertThat(ColumnarReconciler.diff(before, after, b -> false)).containsExactly(retype);
    assertThat(ColumnarReconciler.diff(before, both, b -> false)).containsExactly(retype);
  }

  @Test
  void newEmptyBucketCreatesWithoutRebuild() {
    final ColumnDefinition brand = column(100, "brand", Type.STRING);
    final Map<Integer, ColumnDefinition[]> before = Map.of(10, new ColumnDefinition[] { brand });
    final Map<Integer, ColumnDefinition[]> after = Map.of(10, new ColumnDefinition[] { brand }, 11,
        new ColumnDefinition[] { column(102, "brand", Type.STRING) });

    assertThat(ColumnarReconciler.diff(before, after, b -> b == 11))
        .containsExactly(new ColumnarSchemaEvent(ColumnarSchemaEvent.Kind.CREATE_EMPTY, 11, -1, null, null));
  }

  @Test
  void newIndexOnDataRebuildsWithCreateReason() {
    final ColumnDefinition brand = column(100, "brand", Type.STRING);
    final ColumnDefinition color = column(101, "color", Type.STRING);
    final Map<Integer, ColumnDefinition[]> before = Map.of(10, new ColumnDefinition[] { brand });
    // a new column of a projected bucket, and a new bucket that has records
    final Map<Integer, ColumnDefinition[]> after = Map.of(10, new ColumnDefinition[] { brand, color }, 12,
        new ColumnDefinition[] { column(103, "brand", Type.STRING) });

    assertThat(ColumnarReconciler.diff(before, after, b -> false)).containsExactly(
        new ColumnarSchemaEvent(ColumnarSchemaEvent.Kind.ADD_COLUMN, 10, 101, ColumnarRebuilder.CREATE, null),
        new ColumnarSchemaEvent(ColumnarSchemaEvent.Kind.ADD_COLUMN, 12, 103, ColumnarRebuilder.CREATE, null));
  }

  @Test
  void hierarchyDiffFlagsChangedTypesOnly() {
    final Map<String, String> before = Map.of("Tag", "a", "ParentOf", "b");

    assertThat(ColumnarReconciler.hierarchyDiff(before, Map.of("Tag", "a2", "ParentOf", "b"))).containsExactly(
        new ColumnarSchemaEvent(ColumnarSchemaEvent.Kind.HIERARCHY_SOURCE_CHANGED, -1, -1, null, "Tag"));
    assertThat(ColumnarReconciler.hierarchyDiff(before, Map.of("ParentOf", "b"))).containsExactly(
        new ColumnarSchemaEvent(ColumnarSchemaEvent.Kind.HIERARCHY_SOURCE_CHANGED, -1, -1, null, "Tag"));
    // a type named for the first time
    assertThat(ColumnarReconciler.hierarchyDiff(before, Map.of("Tag", "a", "ParentOf", "b", "Category", "c"))).containsExactly(
        new ColumnarSchemaEvent(ColumnarSchemaEvent.Kind.HIERARCHY_SOURCE_CHANGED, -1, -1, null, "Category"));
    assertThat(ColumnarReconciler.hierarchyDiff(before, before)).isEmpty();
  }

  @Test
  void hierarchyFingerprintsDescribeTheSourceTypes() {
    database.command("sql", "CREATE VERTEX TYPE Tag BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Tag.path STRING");
    database.command("sql", "CREATE EDGE TYPE ParentOf");
    createAssetType(1);
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA "
        + "{\"hierarchy\":{\"vertexType\":\"Tag\",\"key\":\"path\",\"parentEdge\":\"ParentOf\"}}");

    final Map<String, String> fingerprints = ColumnarReconciler.hierarchyFingerprints(database.getSchema(), hierarchies());
    assertThat(fingerprints).containsOnlyKeys("Tag", "ParentOf");
    final String tagBuckets = String.valueOf(database.getSchema().getType("Tag").getBuckets(false).getFirst().getFileId());
    assertThat(fingerprints.get("Tag")).isEqualTo("exists|" + tagBuckets + "|path:STRING");

    database.command("sql", "DROP PROPERTY Tag.path");
    assertThat(ColumnarReconciler.hierarchyFingerprints(database.getSchema(), hierarchies()).get("Tag"))
        .isEqualTo("exists|" + tagBuckets + "|path:absent");
    database.command("sql", "DROP TYPE ParentOf");
    assertThat(ColumnarReconciler.hierarchyFingerprints(database.getSchema(), hierarchies()).get("ParentOf")).isEqualTo("absent");
  }

  // ---------------------------------------------------------------------------------------------------------------- integration

  @Test
  void hierarchySourceTypeChangeIsForwarded() throws Exception {
    database.command("sql", "CREATE VERTEX TYPE Tag BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Tag.path STRING");
    database.command("sql", "CREATE EDGE TYPE ParentOf");
    createAssetType(1);
    insertAssets(500);
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA "
        + "{\"hierarchy\":{\"vertexType\":\"Tag\",\"key\":\"path\",\"parentEdge\":\"ParentOf\"}}");
    awaitCaughtUp();
    manager().flushNow();
    final ColumnarProjection projection = manager().projection(bucketIds()[0]);
    final long generation = projection.manifest().generation();

    final List<ColumnarSchemaEvent> events = new CopyOnWriteArrayList<>();
    manager().addSchemaEventListener(events::add);

    database.command("sql", "CREATE BUCKET Tag_2");
    database.command("sql", "ALTER TYPE Tag BUCKET +Tag_2");
    awaitHierarchyEvents(events, 1);
    assertThat(hierarchyEvents(events)).containsExactly(
        new ColumnarSchemaEvent(ColumnarSchemaEvent.Kind.HIERARCHY_SOURCE_CHANGED, -1, -1, null, "Tag"));

    database.command("sql", "DROP PROPERTY Tag.path");
    awaitHierarchyEvents(events, 2);
    assertThat(hierarchyEvents(events)).hasSize(2).allMatch(e -> e.typeName().equals("Tag"));

    awaitCaughtUp();
    assertThat(manager().projection(bucketIds()[0])).isSameAs(projection);
    assertThat(projection.manifest().generation()).isEqualTo(generation);
    assertThat(stats().rebuildsTotal()).isEqualTo(1);
  }

  @Test
  void dropIndexDropsTheProjectionDirectory() throws InterruptedException {
    createIndexedAssets(1, 2_000);
    final int bucketId = bucketIds()[0];
    assertThat(Files.isDirectory(bucketDir(bucketId))).isTrue();

    database.command("sql", "DROP INDEX `" + columnarTypeIndex().getName() + "`");

    assertThat(Files.exists(bucketDir(bucketId))).isFalse();
    assertThat(manager().projection(bucketId)).isNull();
    awaitCaughtUp();
  }

  @Test
  void dropTypeDropsEveryBucketProjection() throws InterruptedException {
    createIndexedAssets(3, 3_000);
    final int[] bucketIds = bucketIds();
    assertThat(bucketIds).hasSize(3);
    for (final int bucketId : bucketIds)
      assertThat(Files.isDirectory(bucketDir(bucketId))).isTrue();

    database.command("sql", "DROP TYPE Asset");

    for (final int bucketId : bucketIds) {
      assertThat(Files.exists(bucketDir(bucketId))).isFalse();
      assertThat(manager().projection(bucketId)).isNull();
    }
    awaitCaughtUp();
  }

  @Test
  void dropOneOfTwoIndexesKeepsTheOtherColumn() throws Exception {
    createAssetType(1);
    insertAssets(2_000);
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Asset (color) COLUMNAR");
    awaitCaughtUp();
    final ColumnarProjection projection = manager().projection(bucketIds()[0]);
    final int colorFileId = columnarIndex("color").getFileId();

    database.command("sql", "DROP INDEX `Asset[color]_columnar`");
    awaitCaughtUp();

    assertThat(manager().projection(bucketIds()[0])).isSameAs(projection);
    assertThat(projection.propertyOf(colorFileId)).isNull();
    assertThat(projection.dictionary("color")).isNull();
    assertMatchesGroupBy("Asset", "brand", "brand");
    // the next publish drops the column from the manifest
    insertAssets(10);
    awaitCaughtUp();
    manager().flushNow();
    final ColumnarManifest manifest = ColumnarManifest.load(bucketDir(bucketIds()[0]));
    assertThat(manifest.columns()).extracting(ColumnDefinition::property).containsExactly("brand");
    assertThat(manifest.dictionaries()).extracting(ColumnarManifest.DictionaryEntry::indexFileId).doesNotContain(colorFileId);
  }

  @Test
  void renamePropertyKeepsCounts() throws Exception {
    createIndexedAssets(1, 3_000);
    manager().flushNow();
    insertAssets(500); // rows in the overlay too
    awaitCaughtUp();
    final int bucketId = bucketIds()[0];
    final ColumnarProjection projection = manager().projection(bucketId);
    final int brandFileId = columnarIndex("brand").getFileId();

    // the schema refuses to rename an indexed property (an existing rule, re-proven here only to document why the rename below is
    // driven through the reconcile): the DDL changes nothing
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY Asset.brand NAME maker")).isInstanceOf(SchemaException.class);
    awaitCaughtUp();
    assertMatchesGroupBy("Asset", "brand", "brand");

    // the reconcile of a definition that names the column 'maker' (a follower schema entry, a future rename): data kept
    final Map<Integer, ColumnDefinition[]> renamed = new HashMap<>();
    renamed.put(bucketId, new ColumnDefinition[] { column(brandFileId, "maker", Type.STRING) });
    try (final AutoCloseable ignored = manager().suspendApply()) {
      manager().reconcile(renamed);
    }
    assertThat(manager().projection(bucketId)).isSameAs(projection);
    assertThat(projection.propertyOf(brandFileId)).isEqualTo("maker");
    assertMatchesGroupBy("Asset", "brand", "maker");
    assertThat(projectedCounts("maker")).isNotEmpty();
    assertThat(stats().rebuildsTotal()).isEqualTo(1);

    // the next publish (the overlay rows re-keyed by the rename included) names the column 'maker' in the manifest
    manager().flushNow();
    assertMatchesGroupBy("Asset", "brand", "maker");
    assertThat(ColumnarManifest.load(bucketDir(bucketId)).columns()).extracting(ColumnDefinition::property).containsExactly("maker");
    assertThat(stats().rebuildsTotal()).isEqualTo(1);

    // reopened, the schema's name wins again over the segments' name, still without a rebuild
    reopenDatabase();
    awaitCaughtUp();
    assertMatchesGroupBy("Asset", "brand", "brand");
    assertThat(stats().rebuildsTotal()).isZero();
  }

  @Test
  void renameTypeKeepsTheProjection() throws Exception {
    createIndexedAssets(1, 3_000);
    manager().flushNow();
    final int bucketId = bucketIds()[0];
    final Path dir = bucketDir(bucketId);
    final Map<Object, Long> before = projectedCounts("brand");

    database.command("sql", "ALTER TYPE Asset NAME Item");
    awaitCaughtUp();

    final String bucketName = database.getSchema().getBucketById(bucketId).getName();
    final ColumnarProjection projection = manager().projection(bucketId);
    assertThat(projection.bucketName()).isEqualTo(bucketName);
    assertThat(projection.typeName()).isEqualTo("Item");
    assertThat(projectedCounts("Item", "brand")).isEqualTo(before);
    assertMatchesGroupBy("Item", "brand", "brand");
    insertItems(10);
    awaitCaughtUp();
    manager().flushNow();
    assertThat(ColumnarManifest.load(dir).bucketName()).isEqualTo(bucketName);
    assertThat(stats().rebuildsTotal()).isEqualTo(1);

    reopenDatabase();
    awaitCaughtUp();
    assertThat(bucketDir(database.getSchema().getType("Item").getBuckets(false).getFirst().getFileId())).isEqualTo(dir);
    assertThat(Files.isDirectory(dir)).isTrue();
    assertThat(stats().rebuildsTotal()).isZero();
    assertMatchesGroupBy("Item", "brand", "brand");
  }

  @Test
  void alterTypeAddBucketCreatesEmptyProjection() throws Exception {
    createIndexedAssets(1, 1_000);
    final long rebuilds = stats().rebuildsTotal();

    database.command("sql", "CREATE BUCKET Asset_extra");
    database.command("sql", "ALTER TYPE Asset BUCKET +Asset_extra");
    final int extraId = database.getSchema().getBucketByName("Asset_extra").getFileId();
    awaitCaughtUp();

    final ColumnarProjection projection = manager().projection(extraId);
    assertThat(projection).isNotNull();
    assertThat(projection.isUsable()).isTrue();
    assertThat(stats().rebuildsTotal()).isEqualTo(rebuilds);
    assertThat(columnarTypeIndex().isUsable()).isTrue();
  }

  @Test
  void truncateBucketEmptiesByDeletes() throws InterruptedException {
    createIndexedAssets(1, 3_000);
    final int bucketId = bucketIds()[0];
    final ColumnarProjection projection = manager().projection(bucketId);
    assertThat(projectedCounts("brand")).isNotEmpty();

    database.command("sql", "TRUNCATE BUCKET " + database.getSchema().getBucketById(bucketId).getName() + " UNSAFE");
    awaitCaughtUp();

    assertThat(database.countType("Asset", false)).isZero();
    assertThat(projectedCounts("brand")).isEmpty();
    assertThat(manager().projection(bucketId)).isSameAs(projection);
    assertThat(stats().rebuildsTotal()).isEqualTo(1);
  }

  @Test
  void checkFixRebuildsAffectedBuckets() throws Exception {
    createIndexedAssets(2, 2_000);
    final int indexFileId = columnarTypeIndex().getIndexesOnBuckets()[0].getFileId();
    final String indexName = columnarTypeIndex().getName();
    final RID victim;
    try (final ResultSet result = database.query("sql", "SELECT @rid AS rid FROM Asset LIMIT 1")) {
      victim = result.next().getProperty("rid");
    }
    corruptRecordTypeByte((DatabaseInternal) database, victim);

    final Result fix;
    try (final ResultSet result = database.command("sql", "CHECK DATABASE FIX")) {
      fix = result.next();
    }
    final Collection<String> rebuilt = fix.getProperty("rebuiltIndexes");
    assertThat(rebuilt).doesNotContain(indexName);

    // only the bucket the FIX repaired: the other one is not rebuilt
    awaitRebuild(manager().projection(victim.getBucketId()), ColumnarRebuilder.CHECK_FIX, 1);
    awaitCaughtUp();
    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.CHECK_FIX)).isEqualTo(1);
    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.CREATE)).isEqualTo(bucketIds().length);
    // the shell index was not dropped and re-created by the checker (R19)
    assertThat(columnarTypeIndex().getIndexesOnBuckets()[0].getFileId()).isEqualTo(indexFileId);
    assertMatchesGroupBy("Asset", "brand", "brand");
  }

  @Test
  void addColumnAfterLeaderBuildDoesNotRebuildTwice() throws InterruptedException {
    createIndexedAssets(1, 3_000);
    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.CREATE)).isEqualTo(1);

    manager().requestReconcile();
    awaitCaughtUp();
    database.command("sql", "CREATE PROPERTY Asset.weight INTEGER");
    awaitCaughtUp();

    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.CREATE)).isEqualTo(1);
    assertThat(stats().rebuildsTotal()).isEqualTo(1);
    assertMatchesGroupBy("Asset", "brand", "brand");
  }

  @Test
  void addColumnWithoutABuildSchedulesTheCreateRebuild() throws Exception {
    createIndexedAssets(1, 2_000);
    final int bucketId = bucketIds()[0];
    final ColumnarProjection projection = manager().projection(bucketId);
    final int brandFileId = columnarIndex("brand").getFileId();

    // as on a follower: a column the schema has and no build() covered, on a projection with no valid state
    final Map<Integer, ColumnDefinition[]> withNewColumn = new HashMap<>();
    withNewColumn.put(bucketId, new ColumnDefinition[] { column(brandFileId, "brand", Type.STRING), column(999_999, "color", Type.STRING) });
    try (final AutoCloseable ignored = manager().suspendApply()) {
      projection.markUnusableWithoutRequest(ColumnarProjection.MISSING);
      manager().reconcile(withNewColumn);
    }
    awaitRebuild(projection, ColumnarRebuilder.CREATE, 2);
    assertThat(projection.isUsable()).isTrue();
  }

  @Test
  void dropIndexWhileReadingDefersDeletion() throws InterruptedException {
    createIndexedAssets(1, 2_000);
    final int bucketId = bucketIds()[0];
    final Path dir = bucketDir(bucketId);
    final ColumnarSnapshot snapshot = manager().projection(bucketId).acquire();
    try {
      database.command("sql", "DROP INDEX `" + columnarTypeIndex().getName() + "`");
      assertThat(Files.isDirectory(dir)).isTrue();
      assertThat(snapshot.segments()).isNotEmpty();
    } finally {
      snapshot.close();
    }
    assertThat(Files.exists(dir)).isFalse();
  }

  @Test
  void retypeIsUnpublishedUntilItsSchemaRebuildPublishes() throws Exception {
    // a kind change: LONG to DOUBLE
    retypeSizeAndRebuild(Type.DOUBLE);
  }

  @Test
  void aSameKindRetypeCopiesNoPageOfTheOldType() throws Exception {
    // INTEGER to LONG keeps the LONG kind: only the definition check keeps the rebuild from copying the old segments' rows
    retypeSizeAndRebuild(Type.LONG);
  }

  private void retypeSizeAndRebuild(final Type target) throws Exception {
    createSizedAssets(3_000);
    final int bucketId = bucketIds()[0];
    final ColumnarProjection projection = manager().projection(bucketId);
    final ColumnarIndex size = columnarIndex("size");
    final ColumnarIndex brand = columnarIndex("brand");

    final Map<Integer, ColumnDefinition[]> retyped = new HashMap<>();
    retyped.put(bucketId, new ColumnDefinition[] { brand.columnDefinition(), column(size.getFileId(), "size", target) });
    final int permits = ColumnarRebuilder.REBUILD_SEMAPHORE.drainPermits();
    try {
      try (final AutoCloseable ignored = manager().suspendApply()) {
        manager().reconcile(retyped);
      }
      // the schema rebuild cannot run: the retyped column is not served, the other one is, and nothing read throws
      assertThat(manager().isUsable(size)).isFalse();
      assertThat(manager().isUsable(brand)).isTrue();
      readEveryColumn(projection);
      assertMatchesGroupBy("Asset", "brand", "brand");
      assertThat(stats().rebuilds("Asset", ColumnarRebuilder.SCHEMA)).isZero();
    } finally {
      ColumnarRebuilder.REBUILD_SEMAPHORE.release(permits);
    }

    awaitRebuild(projection, ColumnarRebuilder.SCHEMA, 1);
    awaitCaughtUp();
    assertThat(manager().isUsable(size)).isTrue();
    // no page copied from the segments of the old type
    assertThat(stats().rebuildPagesReusedCount("Asset")).isZero();
    assertThat(numberCounts(projection, "size")).isEqualTo(groupByAsDouble("size"));
    assertThat(ColumnarManifest.load(bucketDir(bucketId)).columns()).filteredOn(c -> c.property().equals("size"))
        .extracting(ColumnDefinition::propertyType).containsExactly(target);
  }

  @Test
  void retypeLeftByACloseIsRebuiltAtOpen() throws Exception {
    createSizedAssets(2_000);
    final int bucketId = bucketIds()[0];
    final Path dir = bucketDir(bucketId);

    // what a close leaves when a RETYPE's schema rebuild did not publish: a manifest whose definition differs from the schema's
    database.close();
    final ColumnarManifest published = ColumnarManifest.load(dir);
    final ColumnDefinition[] columns = published.columns().clone();
    for (int k = 0; k < columns.length; k++)
      if (columns[k].property().equals("size"))
        columns[k] = column(columns[k].indexFileId(), "size", Type.LONG);
    new ColumnarManifest(published.generation() + 1, published.bucketFileId(), published.bucketName(), published.maxRecordsInPage(),
        columns, published.segments(), published.dictionaries(), published.ownerMapFile(), published.externalOwnerMapFile(),
        published.lastTransactionId()).publish(dir);
    database = factory.open();

    final ColumnarProjection projection = manager().projection(bucketId);
    awaitRebuild(projection, ColumnarRebuilder.SCHEMA, 1);
    awaitCaughtUp();
    assertThat(manager().isUsable(columnarIndex("size"))).isTrue();
    assertThat(numberCounts(projection, "size")).isEqualTo(groupByAsDouble("size"));
  }

  @Test
  void columnAddedDuringARunningRebuildIsBuiltAfterIt() throws Exception {
    createIndexedAssets(1, 2_000);
    final int bucketId = bucketIds()[0];
    final ColumnarProjection projection = manager().projection(bucketId);
    final int brandFileId = columnarIndex("brand").getFileId();
    final CountDownLatch scanned = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean first = new AtomicBoolean(true);
    ColumnarTestHooks.killPoint = point -> {
      if (point.equals(ColumnarRebuilder.SCANNED) && first.compareAndSet(true, false)) {
        scanned.countDown();
        try {
          release.await(WAIT_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
    try {
      manager().onSchemaEvent(ColumnarSchemaEvent.rebuild(bucketId, ColumnarRebuilder.CHECK_FIX));
      assertThat(scanned.await(WAIT_MS, TimeUnit.MILLISECONDS)).isTrue();
      // a follower's new column, while the check_fix rebuild that captured the old columns runs
      final Map<Integer, ColumnDefinition[]> withColor = new HashMap<>();
      withColor.put(bucketId, new ColumnDefinition[] { column(brandFileId, "brand", Type.STRING), column(999_998, "color", Type.STRING) });
      try (final AutoCloseable ignored = manager().suspendApply()) {
        manager().reconcile(withColor);
      }
      assertThat(stats().rebuilds("Asset", ColumnarRebuilder.CREATE)).isEqualTo(1);
    } finally {
      release.countDown();
    }
    awaitRebuild(projection, ColumnarRebuilder.CREATE, 2);
    assertThat(projection.isPublishedWith(999_998)).isTrue();
    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.CHECK_FIX)).isEqualTo(1);
    assertMatchesGroupBy("Asset", "color", "color");
  }

  @Test
  void aReconcileThatThrowsIsRetried() throws Exception {
    createAssetType(1);
    insertAssets(1_000);
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Asset (color) COLUMNAR");
    awaitCaughtUp();
    final ColumnarProjection projection = manager().projection(bucketIds()[0]);
    final int colorFileId = columnarIndex("color").getFileId();
    final AtomicBoolean thrown = new AtomicBoolean();
    ColumnarTestHooks.killPoint = point -> {
      if (point.equals(ColumnarIndexManager.RECONCILE) && thrown.compareAndSet(false, true))
        throw new IllegalStateException("injected reconcile failure");
    };

    database.command("sql", "DROP INDEX `Asset[color]_columnar`");
    awaitCaughtUp();

    assertThat(thrown.get()).isTrue();
    assertThat(projection.propertyOf(colorFileId)).isNull();
  }

  // ---------------------------------------------------------------------------------------------------------------- helpers

  /** Asset with brand and an INTEGER size, both COLUMNAR, flushed to a segment. */
  private void createSizedAssets(final int count) throws Exception {
    createAssetType(1);
    database.command("sql", "CREATE PROPERTY Asset.size INTEGER");
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newDocument("Asset").set("brand", "b" + (i % 13)).set("size", i % 4).save();
    });
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Asset (size) COLUMNAR");
    awaitCaughtUp();
    manager().flushNow();
  }

  /** Reads every value of every column of every view the projection serves, as its kind says: nothing may throw. */
  private static void readEveryColumn(final ColumnarProjection projection) {
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      for (final ColumnDefinition definition : projection.columns())
        for (final ColumnarSegmentView view : snapshot.segments()) {
          final ColumnReader reader = view.column(definition.property());
          if (reader == null)
            continue;
          final GlobalDictionary dictionary = snapshot.dictionary(definition.property());
          final PositionBitmap live = view.live();
          for (final int page : live.pages()) {
            final long[] stripe = live.stripe(page);
            for (int w = 0; w < stripe.length; w++)
              for (long word = stripe[w]; word != 0; word &= word - 1) {
                final long position = (long) page * live.maxRecordsInPage() + ((long) w << 6) + Long.numberOfTrailingZeros(word);
                if (!reader.isPresent(position))
                  continue;
                switch (reader.kind()) {
                case ORDINAL -> assertThat(dictionary.valueOf(reader.ordinal(position))).isNotNull();
                case LONG -> reader.longValue(position);
                case DOUBLE -> reader.doubleValue(position);
                case ORDINAL_LIST -> reader.ordinals(position, new int[64]);
                }
              }
          }
        }
    }
  }

  /** Counts of a LONG or DOUBLE column over the live rows of every view, by value as a double. */
  private static Map<Double, Long> numberCounts(final ColumnarProjection projection, final String property) {
    final Map<Double, Long> counts = new HashMap<>();
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      assertThat(snapshot.isUsable()).isTrue();
      for (final ColumnarSegmentView view : snapshot.segments()) {
        final ColumnReader reader = view.column(property);
        if (reader == null)
          continue;
        final PositionBitmap live = view.live();
        for (final int page : live.pages()) {
          final long[] stripe = live.stripe(page);
          for (int w = 0; w < stripe.length; w++)
            for (long word = stripe[w]; word != 0; word &= word - 1) {
              final long position = (long) page * live.maxRecordsInPage() + ((long) w << 6) + Long.numberOfTrailingZeros(word);
              if (reader.isPresent(position))
                counts.merge(reader.kind() == ColumnKind.LONG ? (double) reader.longValue(position) : reader.doubleValue(position), 1L,
                    Long::sum);
            }
        }
      }
    }
    return counts;
  }

  private Map<Double, Long> groupByAsDouble(final String property) {
    final Map<Double, Long> expected = new HashMap<>();
    try (final ResultSet result = database.query("sql", "SELECT " + property + " AS v, count(*) AS n FROM Asset GROUP BY " + property)) {
      while (result.hasNext()) {
        final Result row = result.next();
        final Object value = row.getProperty("v");
        if (value != null)
          expected.put(((Number) value).doubleValue(), ((Number) row.getProperty("n")).longValue());
      }
    }
    assertThat(expected).isNotEmpty();
    return expected;
  }

  private static ColumnDefinition column(final int indexFileId, final String property, final Type type) {
    return ColumnarIndex.defineColumn(indexFileId, property, type, false);
  }

  private List<ColumnarIndexMetadata> hierarchies() {
    final List<ColumnarIndexMetadata> result = new ArrayList<>();
    for (final Index index : database.getSchema().getIndexes())
      if (index instanceof ColumnarIndex columnar && columnar.getColumnarMetadata().getHierarchy() != null)
        result.add(columnar.getColumnarMetadata());
    return result;
  }

  private static List<ColumnarSchemaEvent> hierarchyEvents(final List<ColumnarSchemaEvent> events) {
    return events.stream().filter(e -> e.kind() == ColumnarSchemaEvent.Kind.HIERARCHY_SOURCE_CHANGED).toList();
  }

  private static void awaitHierarchyEvents(final List<ColumnarSchemaEvent> events, final int count) throws InterruptedException {
    for (int i = 0; i < WAIT_MS && hierarchyEvents(events).size() < count; i++)
      Thread.sleep(1);
    assertThat(hierarchyEvents(events)).hasSize(count);
  }

  private void awaitRebuild(final ColumnarProjection projection, final String reason, final long expected) throws InterruptedException {
    for (int i = 0; i < WAIT_MS && (stats().rebuilds(projection.typeName(), reason) < expected || projection.rebuildRequest.get() != null); i++)
      Thread.sleep(1);
    assertThat(stats().rebuilds(projection.typeName(), reason)).isEqualTo(expected);
    assertThat(projection.rebuildRequest.get()).isNull();
  }

  private void createAssetType(final int buckets) {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS " + buckets);
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE PROPERTY Asset.color STRING");
  }

  private void createIndexedAssets(final int buckets, final int count) throws InterruptedException {
    createAssetType(buckets);
    insertAssets(count);
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    awaitCaughtUp();
    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.CREATE)).isEqualTo(buckets);
  }

  private void insertAssets(final int count) {
    insert("Asset", count);
  }

  private void insertItems(final int count) {
    insert("Item", count);
  }

  private void insert(final String type, final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final MutableDocument asset = database.newDocument(type).set("brand", "b" + (i % 13));
        if (i % 7 != 0)
          asset.set("color", "c" + (i % 5));
        asset.save();
      }
    });
  }

  private void awaitCaughtUp() throws InterruptedException {
    assertThat(manager().awaitCaughtUp(WAIT_MS)).isTrue();
  }

  /** The GROUP BY of the type's property equals what the projections answer for the column named {@code column}. */
  private void assertMatchesGroupBy(final String type, final String property, final String column) {
    final Map<Object, Long> expected = new HashMap<>();
    try (final ResultSet result = database.query("sql", "SELECT " + property + " AS v, count(*) AS n FROM " + type + " GROUP BY " + property)) {
      while (result.hasNext()) {
        final Result row = result.next();
        final Object value = row.getProperty("v");
        if (value != null)
          expected.put(value, ((Number) row.getProperty("n")).longValue());
      }
    }
    assertThat(expected).isNotEmpty();
    assertThat(projectedCounts(type, column)).isEqualTo(expected);
  }

  private Map<Object, Long> projectedCounts(final String column) {
    return projectedCounts("Asset", column);
  }

  private Map<Object, Long> projectedCounts(final String type, final String column) {
    final Map<Object, Long> counts = new LinkedHashMap<>();
    for (final Bucket bucket : database.getSchema().getType(type).getBuckets(false))
      try (final ColumnarSnapshot snapshot = manager().projection(bucket.getFileId()).acquire()) {
        assertThat(snapshot.isUsable()).isTrue();
        assertThat(snapshot.pendingPositions().isEmpty()).isTrue();
        final GlobalDictionary dictionary = snapshot.dictionary(column);
        for (final ColumnarSegmentView view : snapshot.segments()) {
          final ColumnReader reader = view.column(column);
          if (reader != null)
            reader.forEachOrdinal(view.live(), (position, ordinal) -> counts.merge(dictionary.valueOf(ordinal), 1L, Long::sum));
        }
      }
    return counts;
  }

  private TypeIndex columnarTypeIndex() {
    return database.getSchema().getType(database.getSchema().existsType("Asset") ? "Asset" : "Item").getAllIndexes(false).stream()
        .filter(i -> i.getType() == Schema.INDEX_TYPE.COLUMNAR).findFirst().orElseThrow();
  }

  private ColumnarIndex columnarIndex(final String property) {
    return (ColumnarIndex) database.getSchema().getType("Asset").getAllIndexes(false).stream()
        .filter(i -> i.getType() == Schema.INDEX_TYPE.COLUMNAR && i.getPropertyNames().contains(property)).findFirst().orElseThrow()
        .getIndexesOnBuckets()[0];
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private ColumnarStats stats() {
    return manager().getStats();
  }

  private int[] bucketIds() {
    return database.getSchema().getType("Asset").getBuckets(false).stream().mapToInt(Bucket::getFileId).toArray();
  }

  private Path bucketDir(final int bucketId) {
    return ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucketId);
  }
}
