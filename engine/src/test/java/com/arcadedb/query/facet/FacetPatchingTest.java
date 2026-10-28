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
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.index.columnar.PositionBitmap;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Patching (D10, D11): while the projection lags, the matches it cannot answer are read from the records, so facets stay equal to
 * GROUP BY; the caller's own open transaction is always read from the records; above the patch budget the answer is flagged inexact.
 */
class FacetPatchingTest extends TestHelper {
  private static final String       TYPE       = ColumnarTestData.TYPE;
  private static final int          ALL        = 1_000;
  private static final List<String> PROPERTIES = List.of("brand", "tags", "fileSize", "publishDate");
  private static final List<String> WHERES     = Arrays.asList(null, "fileSize < 1048576", "author = 'author-7'", "tags CONTAINS 'tag-5'");

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private ColumnarTestData data(final int buckets) {
    final ColumnarTestData data = new ColumnarTestData(7);
    data.tagVocabulary = 300;
    data.buckets = buckets;
    return data;
  }

  private MatchSet matches(final String where) {
    final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0);
    try (builder; final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE + (where == null ? "" : " WHERE " + where))) {
      while (rs.hasNext())
        assertThat(builder.add(rs.next().<Identifiable>getProperty("rid").getIdentity(), 0)).isTrue();
      final MatchSet matches = builder.build();
      noFilters(matches);
      return matches;
    }
  }

  private static void noFilters(final MatchSet matches) {
    final int[] noFilter = new int[PROPERTIES.size()];
    Arrays.fill(noFilter, -1);
    matches.setFacetFilterIndex(noFilter);
  }

  private static List<FacetSpec> facets() {
    final List<FacetSpec> facets = new ArrayList<>();
    for (final String property : PROPERTIES)
      facets.add(new TermsFacetSpec(property, property, null, null, ALL, 1, FacetOrder.COUNT_DESC, null, false, null));
    return facets;
  }

  private FacetResults executeOn(final MatchSet matches) {
    return FacetExecutor.execute((DatabaseInternal) database, matches, facets(), new BasicCommandContext());
  }

  private FacetResults run(final String where) {
    try (final MatchSet matches = matches(where)) {
      assertThat(matches.size()).as("records matching " + where).isPositive();
      return executeOn(matches);
    }
  }

  private AutoCloseable suspendApply() {
    return manager().suspendApply();
  }

  private int[] bucketIds() {
    final List<Bucket> buckets = database.getSchema().getType(TYPE).getBuckets(true);
    final int[] ids = new int[buckets.size()];
    for (int i = 0; i < ids.length; i++)
      ids[i] = buckets.get(i).getFileId();
    return ids;
  }

  /** A bucket value as the oracle keys it: a date as its column's epoch count. */
  private Object oracleKey(final String property, final Object value) {
    if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean)
      return value;
    return manager().extractor(TYPE).toColumnLong(property, value);
  }

  /** Every facet of the results equals GROUP BY over the same WHERE, run now (in the caller's transaction, if any). */
  private void assertEqualsOracle(final FacetResults results, final String where) {
    for (final String property : PROPERTIES) {
      final FacetResult result = results.get(property);
      assertThat(result.source()).as(property).isNotEqualTo(FacetResult.UNAVAILABLE);
      final Map<Object, Long> expected = FacetOracle.expectedCounts(database, TYPE, property, where);
      final long expectedMissing = expected.getOrDefault(FacetOracle.MISSING, 0L);
      final long total = expected.entrySet().stream().filter(e -> e.getKey() != FacetOracle.MISSING).mapToLong(Map.Entry::getValue).sum();
      long returned = 0;
      final Map<Object, Long> counts = new HashMap<>();
      for (final FacetBucket bucket : result.buckets()) {
        if (bucket.isMissing())
          continue;
        counts.put(oracleKey(property, bucket.value()), bucket.count());
        returned += bucket.count();
      }
      if (expected.size() - (expected.containsKey(FacetOracle.MISSING) ? 1 : 0) <= ALL) {
        final Map<Object, Long> withoutMissing = new HashMap<>(expected);
        withoutMissing.remove(FacetOracle.MISSING);
        assertThat(counts).as(property + " WHERE " + where).isEqualTo(withoutMissing);
      } else
        for (final Map.Entry<Object, Long> entry : counts.entrySet())
          assertThat(entry.getValue()).as(property + "=" + entry.getKey() + " WHERE " + where).isEqualTo(expected.get(entry.getKey()));
      assertThat(result.otherCount()).as(property + " otherCount WHERE " + where).isEqualTo(total - returned);
      assertThat(result.missing()).as(property + " missing WHERE " + where).isEqualTo(expectedMissing);
    }
  }

  private static long count(final FacetResult result, final Object value) {
    for (final FacetBucket bucket : result.buckets())
      if (!bucket.isMissing() && value.equals(bucket.value()))
        return bucket.count();
    return 0;
  }

  private List<RID> firstMatches(final String where, final int limit) {
    final List<RID> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE + " WHERE " + where + " LIMIT " + limit)) {
      while (rs.hasNext())
        rids.add(rs.next().<Identifiable>getProperty("rid").getIdentity());
    }
    assertThat(rids).as("records matching " + where).hasSize(limit);
    return rids;
  }

  private void setBrand(final List<RID> rids, final String brand) {
    for (final RID rid : rids)
      database.lookupByRID(rid, true).asDocument().modify().set("brand", brand).save();
  }

  @Test
  @Timeout(300)
  void lagZeroPatchesNothing() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    data.mutate(database, 200);
    FacetOracle.awaitCaughtUp(database);

    final long patchedBefore = manager().getStats().queryPatchedCount();
    for (final String where : WHERES) {
      // caught up, every match is live in a view on a page that is not pending: the safety net of D11 finds nothing
      final FacetResults results = run(where);
      assertThat(results.patched()).as("patched WHERE " + where).isZero();
      assertThat(results.exact()).isTrue();
      assertThat(results.lagMs()).isZero();
      assertEqualsOracle(results, where);
    }
    assertThat(manager().getStats().queryPatchedCount()).isEqualTo(patchedBefore);
  }

  @Test
  @Timeout(300)
  void whileLaggingFacetsEqualGroupBy() throws Exception {
    final ColumnarTestData data = data(1);
    data.deleteRatio = 0.2;
    data.reuseRatio = 0.1;
    data.bigRecordRatio = 0.02;
    data.createSchema(database);
    data.insertAssets(database, 3_000);
    FacetOracle.awaitCaughtUp(database);

    final long patchedBefore = manager().getStats().queryPatchedCount();
    try (final AutoCloseable hold = suspendApply()) {
      data.mutate(database, 500);
      long patched = 0;
      for (final String where : WHERES) {
        final FacetResults results = run(where);
        assertThat(results.exact()).as("exact WHERE " + where).isTrue();
        assertThat(results.lagMs()).as("lag WHERE " + where).isPositive();
        assertEqualsOracle(results, where);
        patched += results.patched();
      }
      assertThat(patched).isPositive();
      assertThat(manager().getStats().queryPatchedCount() - patchedBefore).isEqualTo(patched);
    }
  }

  @Test
  @Timeout(300)
  void overBudgetIsInexactAndFlagged() throws Exception {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    final List<RID> rids = data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);

    final Object budget = GlobalConfiguration.COLUMNAR_PATCH_BUDGET.getValue();
    final Object ratio = GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.getValue();
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(5);
    // the absolute budget alone: the share of the matches would let this patch set through
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(0d);
    try (final AutoCloseable hold = suspendApply()) {
      database.transaction(() -> {
        for (int i = 0; i < 50; i++)
          database.lookupByRID(rids.get(i * 37), true).asDocument().modify().set("fileSize", 4_000L + i).save();
      });
      final long overflowsBefore = manager().getStats().queryPatchOverflowsCount();
      final FacetResults results = run(null);
      assertThat(results.exact()).isFalse();
      assertThat(results.patched()).isZero();
      assertThat(results.lagMs()).isPositive();
      assertThat(manager().getStats().queryPatchOverflowsCount()).isEqualTo(overflowsBefore + 1);
      // counted from the projection alone: none of the updated values is there yet (and the fixture's sizes never are this small)
      for (long size = 4_000L; size < 4_050L; size++)
        assertThat(count(results.get("fileSize"), size)).as("fileSize " + size).isZero();
      assertThat(FacetOracle.expectedCounts(database, TYPE, "fileSize", null).get(4_000L)).isEqualTo(1L);
    } finally {
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(budget);
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(ratio);
    }
  }

  /**
   * Final review ruling (relative patch budget): above the absolute budget a query stays exact while what it patches is at most
   * {@code patchBudgetRatio} of its matches; a ratio of 0 keeps the absolute budget alone.
   */
  @Test
  @Timeout(300)
  void aPatchSetWithinTheRatioOfTheMatchesStaysExact() throws Exception {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    final List<RID> rids = data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);

    final Object budget = GlobalConfiguration.COLUMNAR_PATCH_BUDGET.getValue();
    final Object ratio = GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.getValue();
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(5);
    try (final AutoCloseable hold = suspendApply()) {
      // neighbors: a few dirty pages, so the patch set stays a small share of the 2,000 matches
      database.transaction(() -> {
        for (int i = 0; i < 50; i++)
          database.lookupByRID(rids.get(i), true).asDocument().modify().set("fileSize", 4_000L + i).save();
      });
      final long overflowsBefore = manager().getStats().queryPatchOverflowsCount();
      final FacetResults exact = run(null);
      assertThat(exact.exact()).isTrue();
      assertThat(exact.patched()).isGreaterThan(5).isLessThanOrEqualTo(1_000);
      assertThat(manager().getStats().queryPatchOverflowsCount()).isEqualTo(overflowsBefore);
      assertEqualsOracle(exact, null);

      GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(0d);
      final FacetResults absolute = run(null);
      assertThat(absolute.exact()).isFalse();
      assertThat(absolute.patched()).isZero();
      assertThat(manager().getStats().queryPatchOverflowsCount()).isEqualTo(overflowsBefore + 1);
    } finally {
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(budget);
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(ratio);
    }
  }

  @Test
  @Timeout(300)
  void ownTransactionWritesAreCounted() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);

    final List<RID> mine = firstMatches("author = 'author-7'", 3);
    database.begin();
    try {
      setBrand(mine, "Mine");
      for (final String where : List.of("author = 'author-7'", "fileSize < 1048576")) {
        final FacetResults results = run(where);
        assertThat(results.exact()).isTrue();
        assertEqualsOracle(results, where);
      }
      final FacetResults all = run("author = 'author-7'");
      assertThat(count(all.get("brand"), "Mine")).isEqualTo(3);
      assertThat(all.get("brand").source()).isEqualTo("index");
    } finally {
      database.rollback();
    }
  }

  @Test
  @Timeout(300)
  void ownTransactionOverBudgetScans() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);

    final List<RID> mine = firstMatches("author = 'author-7'", 3);
    final Object budget = GlobalConfiguration.COLUMNAR_PATCH_BUDGET.getValue();
    final Object ratio = GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.getValue();
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(1);
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(0d);
    final long scansBefore = manager().getStats().queryFallbackScansCount();
    database.begin();
    try {
      setBrand(mine, "Mine");
      final FacetResults results = run("author = 'author-7'");
      assertThat(results.exact()).isTrue();
      for (final String property : PROPERTIES)
        assertThat(results.get(property).source()).as(property).isEqualTo("scan");
      assertThat(count(results.get("brand"), "Mine")).isEqualTo(3);
      assertEqualsOracle(results, "author = 'author-7'");
      assertThat(manager().getStats().queryFallbackScansCount()).isEqualTo(scansBefore + 1);
    } finally {
      database.rollback();
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(budget);
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(ratio);
    }
    assertThat(manager().fallbackScans().availablePermits())
        .isEqualTo(database.getConfiguration().getValueAsInteger(GlobalConfiguration.COLUMNAR_MAX_FALLBACK_SCANS));
  }

  @Test
  @Timeout(300)
  void newRecordsInOpenTransaction() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);

    database.begin();
    try {
      for (int i = 0; i < 5; i++)
        database.newDocument(TYPE).set("brand", "Fresh").set("tags", List.of("tag-fresh")).set("fileSize", 7L).save();
      final FacetResults results = run(null);
      assertThat(count(results.get("brand"), "Fresh")).isEqualTo(5);
      assertThat(count(results.get("tags"), "tag-fresh")).isEqualTo(5);
      assertEqualsOracle(results, null);
    } finally {
      database.rollback();
    }
  }

  @Test
  @Timeout(300)
  void deletedBetweenMatchAndPatch() throws Exception {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);

    final String where = "author = 'author-7'";
    try (final AutoCloseable hold = suspendApply(); final MatchSet matches = matches(where)) {
      final List<RID> gone = firstMatches(where, 2);
      database.transaction(() -> {
        for (final RID rid : gone)
          database.deleteRecord(database.lookupByRID(rid, true));
      });
      final FacetResults results = executeOn(matches);
      assertThat(results.exact()).isTrue();
      assertEqualsOracle(results, where);
    }
  }

  @Test
  @Timeout(300)
  void deletedPendingPositionIsNotAMatch() throws Exception {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    final List<RID> rids = data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);

    try (final AutoCloseable hold = suspendApply()) {
      database.transaction(() -> {
        for (int i = 0; i < 5; i++)
          database.deleteRecord(database.lookupByRID(rids.get(i * 101), true));
      });
      try (final MatchSet matches = MatchSet.fromProjection((DatabaseInternal) database, bucketIds(), 0)) {
        noFilters(matches);
        final FacetResults results = executeOn(matches);
        assertThat(results.exact()).isTrue();
        assertThat(results.patched()).isPositive();
        assertEqualsOracle(results, null);
      }
    }
  }

  /** The pages of the bucket's pending positions, read from a snapshot taken now. */
  private int pendingPages(final int bucketId) {
    try (final ColumnarSnapshot snapshot = manager().projection(bucketId).acquire()) {
      return snapshot.pendingPositions().pages().length;
    }
  }

  @Test
  @Timeout(300)
  void noWhereFacetsOverManyPendingPagesStayExact() throws Exception {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 1_000);
    FacetOracle.awaitCaughtUp(database);
    final int bucketId = bucketIds()[0];

    try (final AutoCloseable hold = suspendApply()) {
      // inserts until 8 pages are pending: 8 x 2048 slots is beyond the default budget, the records written are far below it
      int inserted = 0;
      while (pendingPages(bucketId) < 8) {
        data.insertAssets(database, 100);
        inserted += 100;
      }
      assertThat(inserted).as("records inserted while the apply is held").isLessThan(GlobalConfiguration.COLUMNAR_PATCH_BUDGET.getValueAsInteger());

      try (final MatchSet matches = MatchSet.fromProjection((DatabaseInternal) database, bucketIds(), 0)) {
        noFilters(matches);
        assertThat(matches.size()).as("the match set holds records, not the slots of pending pages").isEqualTo(1_000L + inserted);
        final FacetResults results = executeOn(matches);
        assertThat(results.exact()).isTrue();
        assertThat(results.patched()).isPositive();
        assertEqualsOracle(results, null);
      }
    }
  }

  /** Records which positions reach {@link #collectPatched}. */
  private static final class RecordingCollector implements FacetCollector {
    private final List<Long> patched = new ArrayList<>();
    private final List<Long> missing = new ArrayList<>();

    @Override
    public void beginBucket(final int bucketId, final ColumnarSnapshot snapshot, final CrossBucketOrdinalMap map) {
    }

    @Override
    public void collect(final ColumnarSegmentView segment, final PositionBitmap matches) {
    }

    @Override
    public void collectPatched(final long position, final PatchedValue value) {
      patched.add(position);
      if (value.isMissing())
        missing.add(position);
    }

    @Override
    public void merge(final FacetCollector other) {
    }

    @Override
    public FacetResult finish() {
      return null;
    }
  }

  @Test
  @Timeout(300)
  void patchedRecordsReachOnlyTheFacetsWhoseMatchesHoldThem() throws Exception {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 500);
    FacetOracle.awaitCaughtUp(database);
    final int bucketId = bucketIds()[0];

    try (final AutoCloseable hold = suspendApply(); final MatchSet matches = MatchSet.fromProjection((DatabaseInternal) database,
        bucketIds(), 0)) {
      matches.setFacetFilterIndex(new int[] { -1, -1 });
      // a record committed after the match set was made: not one of its matches, though the bucket's records are the matches
      final RID later = data.insertAssets(database, 1).getFirst();
      final RID matched = firstMatches("brand IS NOT NULL", 1).getFirst();
      assertThat(matches.forFacet(0, bucketId).get(later.getPosition())).isFalse();
      assertThat(matches.forFacet(0, bucketId).get(matched.getPosition())).isTrue();

      final PositionBitmap patch = new PositionBitmap(matches.forBucket(bucketId).maxRecordsInPage());
      patch.set(later.getPosition());
      patch.set(matched.getPosition());
      final RecordingCollector brand = new RecordingCollector();
      final RecordingCollector tags = new RecordingCollector();
      try (final ColumnarSnapshot snapshot = manager().projection(bucketId).acquire()) {
        final long read = FacetPatcher.countPatched((DatabaseInternal) database, matches, snapshot, patch,
            List.of(facets().get(0), facets().get(1)), Arrays.asList(brand, tags), WorkGuard.forCommandDeadline(null));
        assertThat(read).isEqualTo(2);
      }
      assertThat(brand.patched).containsExactly(matched.getPosition());
      assertThat(tags.patched).containsExactly(matched.getPosition());
    }
  }

  /**
   * Final review A-M4: a patched record whose type lost the facet's column meanwhile (its COLUMNAR index dropped while the query counts)
   * is still a match of the facet: it reaches the collector as missing, never silently dropped.
   */
  @Test
  @Timeout(300)
  void aPatchedRecordWithoutTheColumnAnyMoreIsCountedAsMissing() throws Exception {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 500);
    FacetOracle.awaitCaughtUp(database);
    final int bucketId = bucketIds()[0];

    try (final MatchSet matches = MatchSet.fromProjection((DatabaseInternal) database, bucketIds(), 0)) {
      matches.setFacetFilterIndex(new int[] { -1 });
      final RID matched = firstMatches("brand IS NOT NULL", 1).getFirst();
      final PositionBitmap patch = new PositionBitmap(matches.forBucket(bucketId).maxRecordsInPage());
      patch.set(matched.getPosition());
      final RecordingCollector brand = new RecordingCollector();
      try (final ColumnarSnapshot snapshot = manager().projection(bucketId).acquire()) {
        // the type keeps its other COLUMNAR indexes (it still has an extractor), without the brand column
        database.command("sql", "DROP INDEX `" + database.getSchema().getType(TYPE).getPolymorphicColumnarIndex("brand").getName() + "`");
        final long read = FacetPatcher.countPatched((DatabaseInternal) database, matches, snapshot, patch, List.of(facets().get(0)),
            Arrays.asList(brand), WorkGuard.forCommandDeadline(null));
        assertThat(read).isEqualTo(1);
      }
      assertThat(brand.patched).containsExactly(matched.getPosition());
      assertThat(brand.missing).containsExactly(matched.getPosition());
    }
  }

  @Test
  @Timeout(300)
  void cappedFallbackScansMakeTheFacetsUnavailable() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 1_000);
    FacetOracle.awaitCaughtUp(database);

    final List<RID> mine = firstMatches("author = 'author-7'", 3);
    final Object budget = GlobalConfiguration.COLUMNAR_PATCH_BUDGET.getValue();
    final Object ratio = GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.getValue();
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(1);
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(0d);
    final Semaphore permits = manager().fallbackScans();
    final int held = permits.drainPermits();
    final long scansBefore = manager().getStats().queryFallbackScansCount();
    database.begin();
    try {
      setBrand(mine, "Mine");
      final FacetResults results = run("author = 'author-7'");
      for (final String property : PROPERTIES) {
        assertThat(results.get(property).source()).as(property).isEqualTo(FacetResult.UNAVAILABLE);
        assertThat(results.get(property).reason()).isEqualTo("fallback scans capped");
        assertThat(results.executionStats().get(property).source()).isEqualTo(FacetResult.UNAVAILABLE);
      }
      assertThat(manager().getStats().queryFallbackScansCount()).isEqualTo(scansBefore);
    } finally {
      database.rollback();
      permits.release(held);
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(budget);
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(ratio);
    }
  }

  @Test
  @Timeout(300)
  void overBudgetScansTheTransactionsBucketAndDropsTheOthers() throws Exception {
    final ColumnarTestData data = data(3);
    data.createSchema(database);
    data.insertAssets(database, 3_000);
    FacetOracle.awaitCaughtUp(database);

    final Object budget = GlobalConfiguration.COLUMNAR_PATCH_BUDGET.getValue();
    final Object ratio = GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.getValue();
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(1);
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(0d);
    try (final AutoCloseable hold = suspendApply()) {
      data.mutate(database, 300);
      final RID mine = firstMatches("brand IS NOT NULL", 1).getFirst();
      database.begin();
      try {
        setBrand(List.of(mine), "Mine");
        final FacetResults results = run(null);
        assertThat(results.exact()).as("the other buckets lag and are counted from the projection alone").isFalse();
        assertThat(results.patched()).isZero();
        assertThat(results.get("brand").source()).isEqualTo("scan");
        assertThat(count(results.get("brand"), "Mine")).isEqualTo(1);
      } finally {
        database.rollback();
      }
    } finally {
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(budget);
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(ratio);
    }
  }

  @Test
  @Timeout(300)
  void multiBucketWhileLagging() throws Exception {
    final ColumnarTestData data = data(3);
    data.deleteRatio = 0.1;
    data.createSchema(database);
    data.insertAssets(database, 3_000);
    FacetOracle.awaitCaughtUp(database);

    try (final AutoCloseable hold = suspendApply()) {
      data.mutate(database, 300);
      long patched = 0;
      for (final String where : WHERES) {
        final FacetResults results = run(where);
        assertThat(results.exact()).isTrue();
        assertEqualsOracle(results, where);
        patched += results.patched();
      }
      assertThat(patched).isPositive();
    }
  }
}
