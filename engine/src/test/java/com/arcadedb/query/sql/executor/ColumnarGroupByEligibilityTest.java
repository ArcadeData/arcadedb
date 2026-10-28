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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.index.columnar.ColumnarStats;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.ColumnarTestHooks;
import com.arcadedb.index.columnar.FacetOracle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which GROUP BY shapes the planner answers with a {@link ColumnarGroupByStep} (EXPLAIN shows it, with the normal aggregation chain as
 * its fallback), which keep the normal plan, and the fallback-only execution of the step.
 */
class ColumnarGroupByEligibilityTest extends TestHelper {
  private static final String TYPE    = ColumnarTestData.TYPE;
  /** A hang detector, never a latency bound. */
  private static final long   HANG_MS = 60_000L;

  /**
   * {@code Asset} with COLUMNAR on brand, author, tags (BY ITEM), fileSize, publishDate and score DOUBLE, plus {@code rating INTEGER}
   * without index, {@code count INTEGER} and {@code ratio FLOAT} with a COLUMNAR index each, and {@code records} assets.
   */
  private void load(final int records) {
    final ColumnarTestData data = new ColumnarTestData(42);
    data.doubleColumn = true;
    data.brands = 20;
    data.maxTags = 2;
    data.createSchema(database);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".rating INTEGER");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".`count` INTEGER");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (`count`) COLUMNAR");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".ratio FLOAT");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (ratio) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    data.insertAssets(database, records);
    FacetOracle.awaitCaughtUp(database);
  }

  private String explain(final String sql) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private List<Map<String, Object>> rows(final String sql) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rows.add(rs.next().toMap());
    }
    return rows;
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

  private ColumnarStats stats() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager().getStats();
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT brand, count(*) FROM Asset GROUP BY brand",
      "SELECT brand AS b, count(*) AS c, sum(fileSize), avg(fileSize), min(score), max(score), count(fileSize) FROM Asset GROUP BY brand",
      "SELECT author, count(DISTINCT brand) FROM Asset GROUP BY author",
      "SELECT fileSize, count(*) FROM Asset WHERE brand = 'b1' GROUP BY fileSize",
      "SELECT brand, count(*) AS c FROM Asset GROUP BY brand ORDER BY c DESC LIMIT 5",
      "SELECT `count`, min(ratio), max(ratio), sum(`count`) FROM Asset GROUP BY `count`",
      "SELECT count(*) FROM Asset GROUP BY brand" })                         // the key grouped by, not projected
  @Timeout(120)
  void eligibleShapesShowColumnarStep(final String sql) {
    load(500);
    assertThat(explain(sql)).contains("AGGREGATE FROM COLUMNAR PROJECTION").contains("fallback: CALCULATE AGGREGATE PROJECTIONS");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT tags, count(*) FROM Asset GROUP BY tags",                       // list
      "SELECT brand.toLowerCase(), count(*) FROM Asset GROUP BY brand.toLowerCase()",
      "SELECT brand, count(*) + 1 FROM Asset GROUP BY brand",
      "SELECT brand, sum(score) FROM Asset GROUP BY brand",                   // DOUBLE sum
      "SELECT brand, avg(score) FROM Asset GROUP BY brand",                   // DOUBLE avg
      "SELECT brand, sum(ratio) FROM Asset GROUP BY brand",                   // FLOAT sum
      "SELECT rating, count(*) FROM Asset GROUP BY rating",                   // no COLUMNAR
      "SELECT brand, max(rating) FROM Asset GROUP BY brand",                  // aggregate over a property without COLUMNAR
      "SELECT brand, count(brand) FROM Asset GROUP BY brand",                 // count(q) of a STRING
      "SELECT brand, count(DISTINCT fileSize) FROM Asset GROUP BY brand",     // count(DISTINCT q) of a LONG
      "SELECT brand, min(publishDate) FROM Asset GROUP BY brand",             // DATETIME min
      "SELECT publishDate, count(*) FROM Asset GROUP BY publishDate",         // DATETIME key
      "SELECT score, count(*) FROM Asset GROUP BY score",                     // DOUBLE key
      "SELECT brand, author, count(*) FROM Asset GROUP BY brand, author",
      "SELECT brand, author, count(*) FROM Asset GROUP BY brand",             // a projected property that is not the key
      "SELECT author AS brand, count(*) FROM Asset GROUP BY brand",           // the key names an alias of another property
      "SELECT brand, count(*) FROM Asset GROUP BY brand LIMIT 3",             // LIMIT without ORDER BY
      "SELECT brand, count(*) FROM Asset GROUP BY brand SKIP 3",              // SKIP without ORDER BY
      "SELECT brand, count(*) FROM Asset GROUP BY brand ORDER BY count(*) DESC", // ORDER BY an aggregate the projection does not name
      "SELECT brand, count(*) FROM Asset GROUP BY brand ORDER BY author",     // ORDER BY a property the projection does not name
      "SELECT brand, count(*) FROM Asset LET $x = 1 GROUP BY brand",
      "SELECT brand, count(*) FROM (SELECT FROM Asset) GROUP BY brand",
      "SELECT brand, count(*) FROM bucket:Asset_0 GROUP BY brand",
      "SELECT DISTINCT brand, count(*) FROM Asset GROUP BY brand",
      "SELECT brand, count(*) FROM Asset GROUP BY brand UNWIND brand",
      "SELECT brand, count(*) FROM Asset GROUP BY brand FACETS b: brand TERMS",
      "SELECT brand, count(*), sum(fileSize) * 2 FROM Asset GROUP BY brand",
      "SELECT brand, max(fileSize).asString() FROM Asset GROUP BY brand",
      "SELECT brand, count(*) AS brand FROM Asset GROUP BY brand" })
  @Timeout(120)
  void ineligibleShapesKeepTheNormalPlan(final String sql) {
    load(500);
    assertThat(explain(sql)).doesNotContain("COLUMNAR PROJECTION");
  }

  /** {@code SELECT DISTINCT brand} over a type above the rewrite threshold becomes a GROUP BY the planner did not review: declined. */
  @Test
  @Timeout(300)
  void distinctRewrittenAsGroupByKeepsTheNormalPlan() {
    load(10_000);
    final String plan = explain("SELECT DISTINCT brand FROM Asset");
    // the rewrite took place, so the decline is what keeps the step away
    assertThat(plan).contains("CALCULATE AGGREGATE PROJECTIONS");
    assertThat(plan).doesNotContain("COLUMNAR PROJECTION");
  }

  /** A supertype target whose index is declared on a subtype only: the supertype's own bucket has no column, so the plan is normal. */
  @Test
  @Timeout(120)
  void subtypeBucketWithoutIndexIsIneligible() {
    load(500);
    database.command("sql", "CREATE DOCUMENT TYPE Media");
    database.command("sql", "CREATE PROPERTY Media.brand STRING");
    database.command("sql", "ALTER TYPE " + TYPE + " SUPERTYPE +Media");
    database.transaction(() -> database.command("sql", "INSERT INTO Media SET brand = 'b1'"));
    FacetOracle.awaitCaughtUp(database);

    assertThat(explain("SELECT brand, count(*) FROM Media GROUP BY brand")).doesNotContain("COLUMNAR PROJECTION");
    // the subtype itself is still answered from its projection
    assertThat(explain("SELECT brand, count(*) FROM " + TYPE + " GROUP BY brand")).contains("AGGREGATE FROM COLUMNAR PROJECTION");
  }

  /**
   * A column in its create rebuild window is not published: the step is planned all the same (the run-time decision is the step's), the
   * execution takes the normal path with the rows of a plan without the step, and the plan is cached as any other.
   */
  @Test
  @Timeout(120)
  void unpublishedColumnStillPlansTheStep() throws Exception {
    load(500);
    final String sql = "SELECT brand FROM Asset GROUP BY brand";
    final String stepless = "SELECT brand FROM (SELECT FROM Asset) GROUP BY brand";
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
      assertThat(explain(sql)).contains("AGGREGATE FROM COLUMNAR PROJECTION");
      assertThat(rows(sql)).isNotEmpty().containsExactlyInAnyOrderElementsOf(rows(stepless));
      assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(sql)).isTrue();
      assertThat(profiledStep(sql).lastPath()).startsWith("normal");
    } finally {
      ColumnarTestHooks.killPoint = null;
      release.countDown();
    }
    FacetOracle.awaitCaughtUp(database);
    assertThat(rows(sql)).containsExactlyInAnyOrderElementsOf(rows(stepless));
  }

  /** A subtype target finds the COLUMNAR index its supertype declares, which projects the subtype's buckets too. */
  @Test
  @Timeout(120)
  void subtypeTargetInheritsTheSupertypeIndex() {
    load(500);
    database.command("sql", "CREATE DOCUMENT TYPE Image EXTENDS " + TYPE);
    database.transaction(() -> {
      for (int i = 0; i < 30; i++)
        database.newDocument("Image").set("brand", "brand-" + (i % 3)).set("fileSize", (long) i).save();
    });
    FacetOracle.awaitCaughtUp(database);
    final String sql = "SELECT brand, count(*) AS c, sum(fileSize) AS s FROM Image GROUP BY brand";
    assertThat(explain(sql)).contains("+ AGGREGATE FROM COLUMNAR PROJECTION (Image.brand)", "match set: all records");
    assertThat(rows(sql)).hasSize(3).containsExactlyInAnyOrderElementsOf(rows(sql.replace("FROM Image", "FROM (SELECT FROM Image)")));
  }

  /** The step prints what it aggregates, its match set and its fallback. */
  @Test
  @Timeout(120)
  void explainDescribesTheStep() {
    load(500);
    final String plan = explain("SELECT brand, count(*), sum(fileSize) FROM Asset GROUP BY brand");
    assertThat(plan).contains("+ AGGREGATE FROM COLUMNAR PROJECTION (Asset.brand)", "aggregates: count(*), sum(fileSize)",
        "match set: all records", "fallback: CALCULATE AGGREGATE PROJECTIONS");
    assertThat(explain("SELECT fileSize, count(*) FROM Asset WHERE brand = 'b1' GROUP BY fileSize")).contains("match set: from upstream");
  }

  /** R22: a WHERE an index search consumes is still a WHERE: the match set comes from upstream, never "all records". */
  @Test
  @Timeout(120)
  void whereConsumedByAnIndexIsFromUpstream() {
    load(500);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (author) NOTUNIQUE");
    final String plan = explain("SELECT brand, count(*) FROM Asset WHERE author = 'x' GROUP BY brand");
    assertThat(plan).contains("FETCH FROM INDEX");
    assertThat(plan).contains("AGGREGATE FROM COLUMNAR PROJECTION", "match set: from upstream");
    assertThat(plan).doesNotContain("match set: all records");
  }

  /** Until the columnar path exists, the step runs its fallback: the rows of the forced-normal plan, and a "normal" path. */
  @Test
  @Timeout(120)
  void fallbackOnlySkeletonReturnsNormalRows() {
    load(500);
    final List<String> queries = List.of("SELECT brand, count(*) AS c, sum(fileSize) AS s, avg(fileSize) AS a FROM Asset GROUP BY brand",
        "SELECT brand, count(*) AS c FROM Asset GROUP BY brand ORDER BY c DESC, brand LIMIT 5",
        "SELECT fileSize, count(*) FROM Asset WHERE brand = 'brand-1' GROUP BY fileSize",
        "SELECT author, count(DISTINCT brand) AS d FROM Asset GROUP BY author");
    for (final String sql : queries) {
      final List<Map<String, Object>> withStep = rows(sql);
      final ColumnarGroupByStep step = profiledStep(sql);
      assertThat(step).as(sql).isNotNull();
      assertThat(step.lastPath()).as(sql).startsWith("normal");

      // the oracle: the same statement over a subquery, which the planner declines, so its plan has no step at all
      final String stepless = sql.contains("WHERE") ?
          sql.replace("FROM Asset WHERE brand = 'brand-1'", "FROM (SELECT FROM Asset WHERE brand = 'brand-1')") :
          sql.replace("FROM Asset", "FROM (SELECT FROM Asset)");
      assertThat(explain(stepless)).as(stepless).doesNotContain("COLUMNAR PROJECTION");
      final List<Map<String, Object>> oracle = rows(stepless);
      assertThat(withStep).as(sql).isNotEmpty();
      if (sql.contains("ORDER BY"))
        assertThat(withStep).as(sql).containsExactlyElementsOf(oracle);
      else
        assertThat(withStep).as(sql).containsExactlyInAnyOrderElementsOf(oracle);

      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, false);
      try {
        final List<Map<String, Object>> normal = rows(sql);
        assertThat(withStep).as(sql).isNotEmpty();
        if (sql.contains("ORDER BY"))
          assertThat(withStep).as(sql).containsExactlyElementsOf(normal);
        else
          assertThat(withStep).as(sql).containsExactlyInAnyOrderElementsOf(normal);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, true);
      }
    }
  }

  /** The switch is read from the database configuration at each execution, and each execution counts its path. */
  @Test
  @Timeout(120)
  void killSwitchIsReadAtExecutionFromTheDatabase() {
    load(500);
    final String sql = "SELECT brand, count(*) FROM Asset GROUP BY brand";
    assertThat(profiledStep(sql).lastPath()).isNotEqualTo("normal: disabled by arcadedb.columnar.groupByAcceleration");

    final long fallbacks = stats().groupByFallbacksCount();
    final long accelerated = stats().groupByAcceleratedCount();
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, false);
    try {
      assertThat(profiledStep(sql).lastPath()).isEqualTo("normal: disabled by arcadedb.columnar.groupByAcceleration");
      assertThat(stats().groupByFallbacksCount()).isEqualTo(fallbacks + 1);
      assertThat(stats().groupByAcceleratedCount()).isEqualTo(accelerated);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, true);
    }
  }

  /** A PROFILE prints the path the execution took and, for the normal path, the fallback chain that ran. */
  @Test
  @Timeout(120)
  void profilePrintsThePathAndTheFallbackThatRan() {
    load(500);
    try (final ResultSet rs = database.query("sql", "PROFILE SELECT brand, count(*) FROM Asset GROUP BY brand")) {
      while (rs.hasNext())
        rs.next();
      final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(plan).contains("path: normal (reason: ");
      assertThat(plan).containsPattern("\\n +\\+ CALCULATE AGGREGATE PROJECTIONS");
    }
    // the fallback is the step's sub-steps: plan JSON and cost roll-ups see the aggregation
    assertThat(profiledStep("SELECT brand, count(*) FROM Asset GROUP BY brand").getSubSteps())
        .anyMatch(step -> step instanceof AggregateProjectionCalculationStep);
  }

  /**
   * A plan with the step is cached (an aggregate makes a statement uncacheable whatever its plan, so the shape has none), and the copy a
   * cache hit runs is wired to its own fallback: the second execution returns the rows of the first.
   */
  @Test
  @Timeout(120)
  void cachedPlanCopiesTheStep() {
    load(500);
    final String sql = "SELECT brand FROM Asset GROUP BY brand";
    assertThat(explain(sql)).contains("AGGREGATE FROM COLUMNAR PROJECTION");
    final List<Map<String, Object>> first = rows(sql);
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(sql)).isTrue();
    assertThat(first).hasSize(20);
    assertThat(rows(sql)).containsExactlyInAnyOrderElementsOf(first);
  }
}
