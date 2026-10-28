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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarManifest;
import com.arcadedb.index.columnar.ColumnarProjection;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.ColumnarStats;
import com.arcadedb.index.columnar.ColumnarStore;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.ColumnarTestHooks;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.query.facet.FacetMemoryBudget;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * GROUP BY answered from the COLUMNAR projection ({@link ColumnarGroupByStep}): every accelerated execution returns what the same
 * statement returns with {@code arcadedb.columnar.groupByAcceleration} off (rows, order under ORDER BY, column names, value types), and
 * every state the projection cannot answer for runs the normal aggregation chain instead.
 */
class ColumnarGroupByAccelerationTest extends TestHelper {
  private static final String TYPE    = ColumnarTestData.TYPE;
  /** A hang detector, never a latency bound. */
  private static final long   HANG_MS = 60_000L;

  /** The eligible shapes of the planner, plus the INTEGER key, the FLOAT min/max, the null group and the unprojected key. No WHERE. */
  static final List<String> QUERIES = List.of(//
      "SELECT brand, count(*) FROM Asset GROUP BY brand",
      "SELECT brand AS b, count(*) AS c, sum(fileSize), avg(fileSize), min(score), max(score), count(fileSize) FROM Asset GROUP BY brand",
      "SELECT author, count(DISTINCT brand) FROM Asset GROUP BY author",
      "SELECT fileSize, count(*) FROM Asset GROUP BY fileSize",
      // a total order: ties at the LIMIT are in no defined order on either path (a parallel normal plan emits partition order)
      "SELECT brand, count(*) AS c FROM Asset GROUP BY brand ORDER BY c DESC, brand LIMIT 5",
      "SELECT `count`, min(ratio), max(ratio), sum(`count`), avg(`count`), count(ratio) FROM Asset GROUP BY `count`",
      "SELECT author, count(*), min(fileSize), max(fileSize), min(`count`) FROM Asset GROUP BY author",
      "SELECT count(*) AS c FROM Asset GROUP BY brand");

  private static final String Q1 = QUERIES.getFirst();

  /** Ruling C1/I1: statements with a WHERE (an index answers the last one) keep the normal plan. */
  static final List<String> WHERE_QUERIES = List.of(//
      "SELECT fileSize, count(*) FROM Asset WHERE brand = 'brand-1' GROUP BY fileSize",
      "SELECT brand, count(*), sum(fileSize) FROM Asset WHERE fileSize > 1000000 GROUP BY brand",
      "SELECT brand, count(*), sum(fileSize) FROM Asset WHERE author = 'author-3' GROUP BY brand");

  /**
   * {@code Asset} as {@link ColumnarGroupByEligibilityTest} declares it, {@code records} assets, then {@code count}, {@code ratio} and
   * {@code rating} set on most of them, then {@code records / 5} mutations (updates, deletes, deletes whose slot is reused, inserts that
   * leave {@code count} and {@code ratio} missing), caught up.
   */
  private ColumnarTestData load(final long seed, final int buckets, final int records) {
    final ColumnarTestData data = new ColumnarTestData(seed);
    data.doubleColumn = true;
    data.brands = 40;
    data.maxTags = 2;
    data.buckets = buckets;
    data.deleteRatio = 0.1;
    data.reuseRatio = 0.1;
    data.createSchema(database);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".rating INTEGER");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".`count` INTEGER");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (`count`) COLUMNAR");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".ratio FLOAT");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (ratio) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    data.insertAssets(database, records);
    database.transaction(() -> database.command("sql",
        "UPDATE " + TYPE + " SET `count` = fileSize % 13, ratio = fileSize / 3072.0, rating = fileSize % 5 WHERE fileSize % 7 <> 0"));
    data.mutate(database, records / 5);
    FacetOracle.awaitCaughtUp(database);
    return data;
  }

  private List<Map<String, Object>> rows(final String sql) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rows.add(rs.next().toMap());
    }
    return rows;
  }

  /** The rows of the statement with the acceleration off: the normal plan. */
  private List<Map<String, Object>> normalRows(final String sql) {
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, false);
    try {
      return rows(sql);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, true);
    }
  }

  /** The {@link ColumnarGroupByStep} of the plan a PROFILE of the statement ran, null when the plan has none. */
  private ColumnarGroupByStep profiledStep(final String sql) {
    try (final ResultSet rs = database.query("sql", "PROFILE " + sql)) {
      while (rs.hasNext())
        rs.next();
      for (final ExecutionStep step : rs.getExecutionPlan().get().getSteps())
        if (step instanceof ColumnarGroupByStep columnar)
          return columnar;
    }
    return null;
  }

  private String lastPath(final String sql) {
    final ColumnarGroupByStep step = profiledStep(sql);
    assertThat(step).as("the plan of " + sql + " has a ColumnarGroupByStep").isNotNull();
    return step.lastPath();
  }

  /** The statement answers what the normal plan answers (in its order under ORDER BY), from the projection. */
  private void assertIdentical(final String sql) {
    final List<Map<String, Object>> accelerated = rows(sql);
    assertThat(lastPath(sql)).as(sql).isEqualTo("columnar");
    final List<Map<String, Object>> normal = normalRows(sql);
    assertThat(normal).as(sql).isNotEmpty();
    // Map.equals: an Integer where the normal plan has a Long, or a Double for a Float, is a difference
    if (sql.contains("ORDER BY"))
      assertThat(accelerated).as(sql).containsExactlyElementsOf(normal);
    else
      assertThat(accelerated).as(sql).containsExactlyInAnyOrderElementsOf(normal);
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private ColumnarStats stats() {
    return manager().getStats();
  }

  private String bucketName(final int index) {
    return database.getSchema().getType(TYPE).getBuckets(false).get(index).getName();
  }

  @ParameterizedTest
  @ValueSource(longs = { 1L, 7L, 42L })
  @Timeout(300)
  void identicalToForcedNormalPlan(final long seed) {
    load(seed, 1, 5000);
    for (final String sql : QUERIES)
      assertIdentical(sql);
  }

  @Test
  @Timeout(300)
  void orderedQueriesKeepOrder() {
    load(42L, 1, 5000);
    final String sql = "SELECT brand, count(*) AS c FROM Asset GROUP BY brand ORDER BY c DESC, brand ASC LIMIT 5";
    final List<Map<String, Object>> accelerated = rows(sql);
    assertThat(lastPath(sql)).isEqualTo("columnar");
    assertThat(accelerated).hasSize(5).containsExactlyElementsOf(normalRows(sql));
  }

  /** Pending pages are patched from their records: the answer stays the normal plan's while the projection lags. */
  @Test
  @Timeout(300)
  void identicalWhileLagging() throws Exception {
    final ColumnarTestData data = load(7L, 1, 5000);
    final long patched;
    try (final AutoCloseable hold = manager().suspendApply()) {
      data.insertAssets(database, 500);
      database.transaction(() -> database.command("sql", "UPDATE " + TYPE + " SET brand = 'brand-late', `count` = 99 LIMIT 200"));
      database.transaction(() -> database.command("sql", "DELETE FROM " + TYPE + " WHERE brand = 'brand-3' LIMIT 100"));
      for (final String sql : QUERIES)
        assertIdentical(sql);
      final ColumnarGroupByStep step = profiledStep(Q1);
      assertThat(step.lastPath()).isEqualTo("columnar");
      patched = step.patched();
      assertThat(patched).isGreaterThan(0);
      assertThat(rows(Q1)).contains(Map.of("brand", "brand-late", "count(*)", 200L));
    }
    FacetOracle.awaitCaughtUp(database);
    for (final String sql : QUERIES)
      assertIdentical(sql);
    // recorded for the tuning of page-granular pending marks (#10662)
    assertThat(patched).isGreaterThanOrEqualTo(700);
  }

  @Test
  @Timeout(300)
  void readYourOwnWritesFallsBack() {
    load(1L, 1, 1000);
    database.begin();
    try {
      database.newDocument(TYPE).set("brand", "Z").save();
      assertThat(rows(Q1)).contains(Map.of("brand", "Z", "count(*)", 1L));
      assertThat(lastPath(Q1)).isEqualTo("normal: the transaction modified bucket " + bucketName(0));
    } finally {
      database.rollback();
    }
    // the next execution, outside the transaction
    assertThat(lastPath(Q1)).isEqualTo("columnar");
    assertThat(rows(Q1)).noneMatch(row -> "Z".equals(row.get("brand")));
  }

  /** A type whose only COLUMNAR index is in its create rebuild has no usable projection: the normal path, the normal rows. */
  @Test
  @Timeout(300)
  void unusableProjectionFallsBack() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Clip");
    database.command("sql", "CREATE PROPERTY Clip.brand STRING");
    database.command("sql", "CREATE PROPERTY Clip.size LONG");
    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.newDocument("Clip").set("brand", "c" + (i % 7)).set("size", (long) i).save();
    });
    final String sql = "SELECT brand, count(*), sum(size) FROM Clip GROUP BY brand";
    final List<Map<String, Object>> normal = rows(sql);

    final CountDownLatch reached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    ColumnarTestHooks.killPoint = name -> {
      if ("rebuild.scanned".equals(name)) {
        reached.countDown();
        try {
          release.await(HANG_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
    try {
      database.command("sql", "CREATE INDEX ON Clip (brand) COLUMNAR");
      database.command("sql", "CREATE INDEX ON Clip (size) COLUMNAR");
      assertThat(reached.await(HANG_MS, TimeUnit.MILLISECONDS)).as("the create rebuild reached the end of its scan").isTrue();
      final String bucket = database.getSchema().getType("Clip").getBuckets(false).getFirst().getName();
      assertThat(lastPath(sql)).isEqualTo("normal: projection of bucket " + bucket + " is not usable");
      assertThat(rows(sql)).containsExactlyInAnyOrderElementsOf(normal);
    } finally {
      ColumnarTestHooks.killPoint = null;
      release.countDown();
    }
    FacetOracle.awaitCaughtUp(database);
    assertThat(lastPath(sql)).isEqualTo("columnar");
    assertThat(rows(sql)).containsExactlyInAnyOrderElementsOf(normal);
  }

  /** Review focus 2: records without the key and records with an explicit null form the groups the normal plan forms. */
  @Test
  @Timeout(300)
  void missingAndNullFormOneNullGroup() {
    load(42L, 1, 1000);
    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        database.newDocument(TYPE).set("brand", "nullish").set("fileSize", 10L).set("author", null).save();
        database.newDocument(TYPE).set("brand", "nullish").set("fileSize", 20L).save();
      }
    });
    FacetOracle.awaitCaughtUp(database);
    final String sql = "SELECT author, count(*), sum(fileSize) FROM Asset GROUP BY author";
    assertIdentical(sql);
    final long nullGroups = rows(sql).stream().filter(row -> row.get("author") == null).count();
    assertThat(nullGroups).isEqualTo(normalRows(sql).stream().filter(row -> row.get("author") == null).count());
    assertThat(nullGroups).isEqualTo(1);
    // and from patched records too
    try (final AutoCloseable hold = manager().suspendApply()) {
      database.transaction(() -> {
        database.newDocument(TYPE).set("brand", "nullish").set("fileSize", 30L).set("author", null).save();
        database.newDocument(TYPE).set("brand", "nullish").set("fileSize", 40L).save();
      });
      assertIdentical(sql);
    } catch (final Exception e) {
      throw new AssertionError(e);
    }
  }

  /** Review focus 3: an INTEGER sum past the int range is a Long, a LONG sum past the long range a BigDecimal, as sum() makes them. */
  @Test
  @Timeout(300)
  void sumOverflowMatchesNormalPlan() {
    load(1L, 1, 500);
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument(TYPE).set("brand", "big").set("count", 2_000_000_000).set("fileSize", Long.MAX_VALUE - 5).save();
    });
    FacetOracle.awaitCaughtUp(database);
    final String sql = "SELECT brand, sum(`count`) AS si, sum(fileSize) AS sl, avg(fileSize) AS al, avg(`count`) AS ai FROM Asset GROUP BY brand";
    assertIdentical(sql);
    final Map<String, Object> big = rows(sql).stream().filter(row -> "big".equals(row.get("brand"))).findFirst().orElseThrow();
    assertThat(big.get("si")).isEqualTo(6_000_000_000L);
    assertThat(big.get("sl")).isInstanceOf(BigDecimal.class);
    // a group whose INTEGER sum stays in range keeps the Integer type
    assertThat(rows(sql).stream().filter(row -> !"big".equals(row.get("brand")) && row.get("si") != null))
        .allMatch(row -> row.get("si") instanceof Integer);
  }

  @Test
  @Timeout(300)
  void groupKeyTypeFollowsSchema() {
    load(7L, 1, 1000);
    final String sql = "SELECT `count`, count(*) FROM Asset GROUP BY `count`";
    assertIdentical(sql);
    assertThat(rows(sql)).filteredOn(row -> row.get("count") != null).isNotEmpty().allMatch(row -> row.get("count") instanceof Integer);
  }

  /** A user who may not read the type's bucket gets what the normal plan gives that user: the same error, or the same rows. */
  @Test
  @Timeout(300)
  void permissionDeniedLikeNormalPlan() {
    load(1L, 1, 500);
    assertRestrictedUserGetsTheNormalAnswer(Set.of(database.getSchema().getType(TYPE).getBuckets(false).getFirst().getFileId()), Q1);
  }

  /** A user without READ on a subtype's bucket: never the remaining buckets counted from the projection. */
  @Test
  @Timeout(300)
  void restrictedOnSubtypeGetsTheNormalAnswer() {
    load(1L, 1, 500);
    database.command("sql", "CREATE DOCUMENT TYPE Image EXTENDS " + TYPE);
    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        database.newDocument("Image").set("brand", "brand-" + (i % 4)).set("fileSize", (long) i).save();
    });
    FacetOracle.awaitCaughtUp(database);
    final Set<Integer> hidden = new HashSet<>(database.getSchema().getType("Image").getBucketIds(false));
    assertRestrictedUserGetsTheNormalAnswer(hidden, Q1);
  }

  private void assertRestrictedUserGetsTheNormalAnswer(final Set<Integer> hidden, final String sql) {
    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContext(database.getDatabasePath());
    final long fallbacks = stats().groupByFallbacksCount();
    final long accelerated = stats().groupByAcceleratedCount();
    context.setCurrentUser(userWithoutReadOn(hidden));
    final String withStep;
    try {
      withStep = outcome(sql);
    } finally {
      context.setCurrentUser(null);
    }
    final long acceleratedAfter = stats().groupByAcceleratedCount();
    final long fallbacksAfter = stats().groupByFallbacksCount();
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, false);
    context.setCurrentUser(userWithoutReadOn(hidden));
    try {
      assertThat(withStep).isEqualTo(outcome(sql));
    } finally {
      context.setCurrentUser(null);
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, true);
    }
    // the normal chain answered, never a count of the readable buckets only. An embedded database has no security manager, so its
    // scan reads every bucket for this user and the two outcomes match either way: the path is what pins the rule
    assertThat(acceleratedAfter).isEqualTo(accelerated);
    assertThat(fallbacksAfter).isEqualTo(fallbacks + 1);
    final int firstHidden = hidden.stream().mapToInt(Integer::intValue).min().getAsInt();
    context.setCurrentUser(userWithoutReadOn(hidden));
    try {
      assertThat(lastPath(sql)).isEqualTo("normal: the user may not read bucket " + database.getSchema().getBucketById(firstHidden).getName());
    } finally {
      context.setCurrentUser(null);
    }
  }

  /** The rows of the statement in a canonical order, or the class and message of the exception it raised. */
  private String outcome(final String sql) {
    try {
      final List<String> rows = new ArrayList<>();
      for (final Map<String, Object> row : rows(sql))
        rows.add(row.toString());
      rows.sort(null);
      return "rows " + rows;
    } catch (final RuntimeException e) {
      return "error " + e.getClass().getName() + ": " + e.getMessage();
    }
  }

  /** A user who may do everything but read the records of {@code hidden}. */
  private static SecurityDatabaseUser userWithoutReadOn(final Set<Integer> hidden) {
    return new SecurityDatabaseUser() {
      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return access != ACCESS.READ_RECORD || !hidden.contains(fileId);
      }

      @Override
      public String getName() {
        return "restricted";
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    };
  }

  /** R24: a cached plan takes its path at each execution, from the switch the database holds then. */
  @Test
  @Timeout(300)
  void cachedPlanObeysRuntimeToggle() {
    load(1L, 1, 1000);
    final String sql = "SELECT brand FROM Asset GROUP BY brand";
    final long accelerated = stats().groupByAcceleratedCount();
    final long fallbacks = stats().groupByFallbacksCount();
    final List<Map<String, Object>> first = rows(sql);
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(sql)).isTrue();
    assertThat(stats().groupByAcceleratedCount()).isEqualTo(accelerated + 1);
    assertThat(stats().groupByFallbacksCount()).isEqualTo(fallbacks);

    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, false);
    try {
      assertThat(rows(sql)).containsExactlyInAnyOrderElementsOf(first);
      assertThat(stats().groupByAcceleratedCount()).isEqualTo(accelerated + 1);
      assertThat(stats().groupByFallbacksCount()).isEqualTo(fallbacks + 1);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, true);
    }
    assertThat(first).hasSize(40);
  }

  @Test
  @Timeout(300)
  void multiBucketTypeIdenticalToNormalPlan() {
    load(42L, 3, 5000);
    for (final String sql : QUERIES)
      assertIdentical(sql);
    // a brand every bucket holds is one group
    final List<Map<String, Object>> rows = rows(Q1);
    assertThat(rows.stream().map(row -> row.get("brand")).distinct().count()).isEqualTo(rows.size());
    long buckets = 0;
    for (final Bucket bucket : database.getSchema().getType(TYPE).getBuckets(false))
      if (database.query("sql", "SELECT count(*) AS c FROM bucket:" + bucket.getName() + " WHERE brand = 'brand-1'").next()
          .<Long>getProperty("c") > 0)
        buckets++;
    assertThat(buckets).isGreaterThan(1);
  }

  /** R21: the COLUMNAR index is declared on Asset only, and projects the Image buckets too. */
  @Test
  @Timeout(300)
  void polymorphicTargetIdenticalToNormalPlan() {
    load(7L, 1, 2000);
    database.command("sql", "CREATE DOCUMENT TYPE Image EXTENDS " + TYPE);
    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.newDocument("Image").set("brand", "brand-" + (i % 50)).set("fileSize", (long) i).set("count", i % 3).save();
    });
    FacetOracle.awaitCaughtUp(database);
    assertIdentical("SELECT brand, count(*) FROM Asset GROUP BY brand");
    assertIdentical("SELECT brand, count(*), sum(fileSize), max(`count`) FROM Asset GROUP BY brand");
    assertIdentical("SELECT brand, count(*) FROM Image GROUP BY brand");
  }

  /**
   * Review focus 12 and ruling C1/I1: a WHERE, an index-answered one included, is never answered from the projection: no step in the
   * plan, and the rows of the switch-off execution, grouping only the WHERE's rows.
   */
  @Test
  @Timeout(300)
  void indexedWhereIsNotWhereless() {
    load(1L, 1, 3000);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (author) NOTUNIQUE");
    for (final String sql : WHERE_QUERIES) {
      assertThat(profiledStep(sql)).as(sql).isNull();
      final List<Map<String, Object>> rows = rows(sql);
      assertThat(rows).as(sql).isNotEmpty().containsExactlyInAnyOrderElementsOf(normalRows(sql));
    }
    final String indexed = WHERE_QUERIES.getLast();
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + indexed)) {
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("FETCH FROM INDEX").doesNotContain("COLUMNAR PROJECTION");
    }
    final long total = rows(indexed).stream().mapToLong(row -> (Long) row.get("count(*)")).sum();
    assertThat(total).isEqualTo(database.query("sql", "SELECT count(*) AS c FROM Asset WHERE author = 'author-3'").next()
        .<Long>getProperty("c"));
  }

  /** Review focus 8 (R23): a column in its create rebuild window is not published, so the step runs the normal chain. */
  @Test
  @Timeout(300)
  void unpublishedColumnFallsBack() throws Exception {
    load(1L, 1, 1000);
    final String sql = "SELECT brand, count(*), sum(fileSize) FROM Asset GROUP BY brand";
    final List<Map<String, Object>> normal = normalRows(sql);
    database.command("sql", "DROP INDEX `" + TYPE + "[brand]_columnar`");
    FacetOracle.awaitCaughtUp(database);

    final CountDownLatch reached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    ColumnarTestHooks.killPoint = name -> {
      if ("rebuild.scanned".equals(name)) {
        reached.countDown();
        try {
          release.await(HANG_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
    try {
      database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) COLUMNAR");
      assertThat(reached.await(HANG_MS, TimeUnit.MILLISECONDS)).as("the create rebuild reached the end of its scan").isTrue();
      assertThat(lastPath(sql)).isEqualTo("normal: projection of bucket " + bucketName(0) + " is not usable");
      assertThat(rows(sql)).containsExactlyInAnyOrderElementsOf(normal);
    } finally {
      ColumnarTestHooks.killPoint = null;
      release.countDown();
    }
    FacetOracle.awaitCaughtUp(database);
    assertIdentical(sql);
  }

  @Test
  @Timeout(300)
  void floatMinMaxKeepFloat() {
    load(42L, 1, 2000);
    final String sql = "SELECT brand, min(ratio) AS lo, max(ratio) AS hi FROM Asset GROUP BY brand";
    assertIdentical(sql);
    assertThat(rows(sql)).filteredOn(row -> row.get("lo") != null).isNotEmpty()
        .allMatch(row -> row.get("lo") instanceof Float && row.get("hi") instanceof Float);
  }

  /** A memory budget refused before anything is counted: the normal path, the normal rows. */
  @Test
  @Timeout(300)
  void budgetRefusedFallsBack() {
    load(7L, 1, 2000);
    final List<String> queries = List.of(QUERIES.get(1), QUERIES.get(3));
    final List<List<Map<String, Object>>> normal = new ArrayList<>();
    for (final String sql : queries)
      normal.add(normalRows(sql));
    final FacetMemoryBudget budget = FacetMemoryBudget.instance();
    final FacetMemoryBudget.Reservation hold = budget.reserve(budget.available() - 8 * 1024, 0);
    assertThat(hold).isNotNull();
    try {
      for (int i = 0; i < queries.size(); i++) {
        assertThat(rows(queries.get(i))).as(queries.get(i)).containsExactlyInAnyOrderElementsOf(normal.get(i));
        assertThat(lastPath(queries.get(i))).as(queries.get(i)).isEqualTo("normal: query memory budget not granted");
      }
    } finally {
      hold.close();
    }
    for (final String sql : queries)
      assertIdentical(sql);
  }

  /**
   * The budget refused while groups are counted (an integral key outgrows the first 64 KiB step): the counted state is dropped and the
   * normal chain answers, upstream never having been read.
   */
  @Test
  @Timeout(300)
  void budgetRefusedWhileCountingFallsBack() {
    load(7L, 1, 3000);
    final String sql = "SELECT fileSize, count(*), sum(`count`) FROM Asset GROUP BY fileSize";
    final List<Map<String, Object>> normal = normalRows(sql);
    final FacetMemoryBudget budget = FacetMemoryBudget.instance();
    // the match set takes its 1 MiB step and the initial groups a 64 KiB one; the growth of the groups finds nothing left
    final FacetMemoryBudget.Reservation hold = budget.reserve(budget.available() - (1024 * 1024 + 64 * 1024 + 1024), 0);
    assertThat(hold).isNotNull();
    try {
      assertThat(rows(sql)).containsExactlyInAnyOrderElementsOf(normal);
      assertThat(lastPath(sql)).isEqualTo("normal: query memory budget not granted");
    } finally {
      hold.close();
    }
    assertIdentical(sql);
  }

  /**
   * I2: inside a REPEATABLE_READ transaction the normal plan reads the pages the transaction pinned, the projection the latest committed
   * state: the step answers through the normal chain, so a commit made meanwhile stays invisible.
   */
  @Test
  @Timeout(300)
  void repeatableReadTransactionFallsBack() throws Exception {
    load(1L, 1, 1000);
    final List<Map<String, Object>> before = normalRows(Q1);
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      // pins every page of the bucket
      assertThat(rows("SELECT brand FROM " + TYPE)).isNotEmpty();
      final Thread writer = new Thread(() -> database.transaction(() -> database.command("sql", "UPDATE " + TYPE + " SET brand = 'brand-moved' LIMIT 50")));
      writer.start();
      writer.join(HANG_MS);
      FacetOracle.awaitCaughtUp(database);
      assertThat(rows(Q1)).containsExactlyInAnyOrderElementsOf(before);
      assertThat(lastPath(Q1)).isEqualTo("normal: the transaction reads at REPEATABLE_READ");
    } finally {
      database.rollback();
    }
    assertThat(rows(Q1)).contains(Map.of("brand", "brand-moved", "count(*)", 50L));
    assertThat(lastPath(Q1)).isEqualTo("columnar");
  }

  /**
   * I3: a value stored before its property was typed (a number in a STRING key, an Integer in a LONG aggregate) is held in its column as
   * the declared type, where the normal plan returns it as stored: the step declines.
   */
  @Test
  @Timeout(300)
  void valuesStoredAsAnotherTypeFallBack() {
    database.command("sql", "CREATE DOCUMENT TYPE Clip");
    database.transaction(() -> {
      for (int i = 0; i < 60; i++)
        database.newDocument("Clip").set("brand", i % 4 == 0 ? (Object) (i % 3) : "c" + (i % 3)).set("label", "l" + (i % 2))
            .set("size", i % 5 == 0 ? (Object) i : (Object) (long) i).save();
    });
    // the schema API declares the properties over the existing values (CREATE PROPERTY in SQL refuses them, the API does not)
    final DocumentType clip = database.getSchema().getType("Clip");
    clip.createProperty("brand", Type.STRING);
    clip.createProperty("label", Type.STRING);
    clip.createProperty("size", Type.LONG);
    database.command("sql", "CREATE INDEX ON Clip (brand) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Clip (label) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Clip (size) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    final String key = "SELECT brand, count(*) FROM Clip GROUP BY brand";
    final String value = "SELECT label, min(size), max(size), sum(size) FROM Clip GROUP BY label";
    for (final String sql : List.of(key, value)) {
      final List<Map<String, Object>> accelerated = rows(sql);
      assertThat(accelerated).as(sql).isNotEmpty().containsExactlyInAnyOrderElementsOf(normalRows(sql));
    }
    assertThat(lastPath(key)).isEqualTo("normal: Clip.brand has values stored as another type");
    assertThat(lastPath(value)).isEqualTo("normal: Clip.size has values stored as another type");
    assertThat(manager().coercedValues("Clip", "label")).isZero();
    // the key a record stores as a number is returned as that number
    assertThat(rows(key)).anyMatch(row -> row.get("brand") instanceof Integer);
  }

  /**
   * {@code Clip(label STRING, size LONG)} with a COLUMNAR index on each, declared through the schema API over values stored before:
   * {@code size} holds Integers when {@code coerced}, and a string no LONG column can hold when {@code failing}. Caught up.
   */
  private void createClip(final boolean coerced, final boolean failing) {
    database.command("sql", "CREATE DOCUMENT TYPE Clip");
    database.transaction(() -> {
      for (int i = 0; i < 60; i++)
        database.newDocument("Clip").set("label", "l" + (i % 3))
            .set("size", coerced && i % 5 == 0 ? (Object) i : failing && i % 7 == 0 ? (Object) ("big-" + i) : (Object) (long) i).save();
    });
    final DocumentType clip = database.getSchema().getType("Clip");
    clip.createProperty("label", Type.STRING);
    clip.createProperty("size", Type.LONG);
    database.command("sql", "CREATE INDEX ON Clip (label) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Clip (size) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
  }

  private static final String CLIP_SUM = "SELECT label, count(*), sum(size) FROM Clip GROUP BY label";

  private String clipBucket() {
    return database.getSchema().getType("Clip").getBuckets(false).getFirst().getName();
  }

  private void reopen() {
    reopenDatabase();
    FacetOracle.awaitCaughtUp(database);
  }

  /** Fix round 2: the coerced values a projection holds are persisted with it, so the step still declines after a reopen. */
  @Test
  @Timeout(300)
  void coercedValueStillDeclinesAfterReopen() {
    createClip(true, false);
    assertThat(lastPath(CLIP_SUM)).isEqualTo("normal: Clip.size has values stored as another type");
    reopen();
    assertThat(lastPath(CLIP_SUM)).isEqualTo("normal: Clip.size has values stored as another type");
    assertThat(rows(CLIP_SUM)).containsExactlyInAnyOrderElementsOf(normalRows(CLIP_SUM));
  }

  /** Fix round 2: the conversion failures a projection holds (values counted as missing) are persisted too. */
  @Test
  @Timeout(300)
  void conversionFailureStillDeclinesAfterReopen() {
    createClip(false, true);
    final String count = "SELECT label, count(size) FROM Clip GROUP BY label";
    // the planner declines while the failures are in this run's stats; after a reopen only the persisted counts know them
    assertThat(profiledStep(count)).isNull();
    reopen();
    assertThat(lastPath(count)).isEqualTo("normal: Clip.size has values that failed to convert");
    assertThat(rows(count)).containsExactlyInAnyOrderElementsOf(normalRows(count));
  }

  /** Fix round 2: a projection persisted with no conversion of note is answered after a reopen as before it. */
  @Test
  @Timeout(300)
  void cleanProjectionIsAcceleratedAfterReopen() {
    createClip(false, false);
    assertIdentical(CLIP_SUM);
    reopen();
    assertIdentical(CLIP_SUM);
  }

  /**
   * Fix round 2: a manifest written without the conversion counts (an earlier format) leaves them unknown: the step declines for the
   * bucket until a rebuild extracts the column again.
   */
  @Test
  @Timeout(300)
  void manifestWithoutConversionCountsDeclinesUntilRebuild() throws Exception {
    createClip(false, false);
    assertIdentical(CLIP_SUM);
    final int bucketId = database.getSchema().getType("Clip").getBuckets(false).getFirst().getFileId();
    final Path manifest = ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucketId).resolve(ColumnarManifest.FILE_NAME);
    database.close();
    final JSONObject json = new JSONObject(Files.readString(manifest));
    assertThat(json.has("conversions")).as("the manifest records the conversion counts").isTrue();
    json.remove("conversions");
    Files.writeString(manifest, json.toString());
    database = factory.open();
    FacetOracle.awaitCaughtUp(database);

    assertThat(lastPath(CLIP_SUM)).isEqualTo("normal: the conversions of bucket " + clipBucket() + " are unknown until its next rebuild");
    assertThat(rows(CLIP_SUM)).containsExactlyInAnyOrderElementsOf(normalRows(CLIP_SUM));
    // a rebuild extracts every value again
    database.command("sql", "REBUILD INDEX `Clip[label]_columnar`");
    database.command("sql", "REBUILD INDEX `Clip[size]_columnar`");
    FacetOracle.awaitCaughtUp(database);
    assertIdentical(CLIP_SUM);
  }

  /**
   * Fix round 3 (review N2, N3): an apply pass that stages a record holding a value of another type (a record written before its property
   * was typed, updated since on another property) adds to the column's counts before it publishes; a snapshot keeps the counts its state
   * was published with; a flush persists the grown counts, which a reopen restores.
   */
  @Test
  @Timeout(300)
  void applyPassCountsCoercedValuesItStages() {
    createClip(true, false);
    final int bucketId = database.getSchema().getType("Clip").getBuckets(false).getFirst().getFileId();
    final ColumnarProjection projection = manager().projection(bucketId);
    final long[] before;
    final int sizeId;
    try (final ColumnarSnapshot held = projection.acquire()) {
      sizeId = held.publishedColumn("size").indexFileId();
      before = held.conversionCounts(sizeId);
      assertThat(before).as("counted by the create rebuild").isNotNull();
      assertThat(before[1]).isGreaterThan(0);

      // the apply pass re-extracts the updated records, whose size is still the Integer they were written with
      database.transaction(() -> database.command("sql", "UPDATE Clip SET label = 'moved' WHERE label = 'l0'"));
      FacetOracle.awaitCaughtUp(database);
      // the held snapshot answers with its own state's counts
      assertThat(held.conversionCounts(sizeId)).containsExactly(before);
    }
    long staged = 0;
    try (final ResultSet rs = database.query("sql", "SELECT size FROM Clip WHERE label = 'moved'")) {
      while (rs.hasNext())
        if (rs.next().getProperty("size") instanceof Integer)
          staged++;
    }
    assertThat(staged).isGreaterThan(0);
    final long expected = before[1] + staged;
    try (final ColumnarSnapshot now = projection.acquire()) {
      assertThat(now.conversionCounts(sizeId)[1]).isEqualTo(expected);
    }
    // a flush persists the grown counts
    try {
      manager().flushNow();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
    final Path dir = ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucketId);
    try {
      assertThat(ColumnarManifest.load(dir).conversions()).anyMatch(e -> e.indexFileId() == sizeId && e.coerced() == expected);
    } catch (final IOException e) {
      throw new AssertionError(e);
    }
    assertThat(lastPath(CLIP_SUM)).isEqualTo("normal: Clip.size has values stored as another type");
    reopen();
    assertThat(lastPath(CLIP_SUM)).isEqualTo("normal: Clip.size has values stored as another type");
  }

  /** I47 (M6): the statement's own TIMEOUT clause stops the accelerated loops, as it stops the normal aggregation. */
  @Test
  @Timeout(300)
  void timeoutClauseStops() {
    load(1L, 1, 5000);
    assertThat(lastPath(Q1)).isEqualTo("columnar");
    final Thread query = Thread.currentThread();
    // the projection is acquired once the statement's deadline is set: holding the acquire past it leaves the deadline to the loops
    ColumnarTestHooks.killPoint = name -> {
      if ("acquire.pendingRead".equals(name) && Thread.currentThread() == query)
        try {
          Thread.sleep(100);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
    };
    final long accelerated = stats().groupByAcceleratedCount();
    final long fallbacks = stats().groupByFallbacksCount();
    Throwable failure = null;
    try {
      rows(Q1 + " TIMEOUT 20 EXCEPTION");
    } catch (final Throwable e) {
      failure = e;
    } finally {
      ColumnarTestHooks.killPoint = null;
    }
    assertThat(failure).as("the statement must time out").isNotNull();
    Throwable cause = failure;
    while (cause != null && !(cause instanceof TimeoutException))
      cause = cause.getCause();
    assertThat(cause).as("caused by the timeout: " + failure).isInstanceOf(TimeoutException.class);
    // stopped inside the counting loops: the step never finished a path (the TIMEOUT step alone would raise only after the rows)
    assertThat(stats().groupByAcceleratedCount()).isEqualTo(accelerated);
    assertThat(stats().groupByFallbacksCount()).isEqualTo(fallbacks);
  }

  /** I47: the accelerated loops stop a statement that was terminated, as the normal aggregation does. */
  @Test
  @Timeout(300)
  void terminatedStatementStops() {
    load(1L, 1, 5000);
    assertThat(lastPath(Q1)).isEqualTo("columnar");
    final RunningQueryRegistry registry = new RunningQueryRegistry();
    Throwable failure = null;
    try (final RunningQuery query = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
      query.terminate("root");
      try {
        rows(Q1);
      } catch (final Throwable e) {
        failure = e;
      }
    }
    assertThat(failure).as("the terminated statement must stop").isNotNull();
    Throwable cause = failure;
    while (cause != null && !(cause instanceof QueryTerminatedException))
      cause = cause.getCause();
    assertThat(cause).as("caused by the termination: " + failure).isInstanceOf(QueryTerminatedException.class);
  }
}
