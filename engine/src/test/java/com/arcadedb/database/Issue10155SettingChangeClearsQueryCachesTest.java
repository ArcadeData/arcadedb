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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10155: a database setting changed on an open database re-derived two of its four query caches. The SQL and Cypher PLAN
 * caches were never cleared, and the plan cache is asked before anything is re-parsed, so a setting read at planning time -
 * {@code arcadedb.queryIndexMaxSelectivity}, baked into the plan as a scan-fallback step - kept the plan it was baked into until a
 * DDL statement or a reopen. And the three settings that SIZE the caches were read once, by the constructor.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10155SettingChangeClearsQueryCachesTest extends TestHelper {
  private static final String SQL    = "select from S";
  private static final String CYPHER = "MATCH (n:S) RETURN n";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("S").createProperty("v", Type.INTEGER);
      database.getSchema().getType("S").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "v");
      for (int i = 0; i < 60; i++)
        database.newVertex("S").set("v", i % 2).save();
    });
  }

  /**
   * The invariant, over every cache a setting change has to empty: a fifth cache added later is either on this list or visibly
   * absent from it.
   */
  @Test
  void aSettingChangeEmptiesEveryQueryCache() {
    final LocalDatabase db = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final Map<String, BooleanSupplier> caches = new LinkedHashMap<>();
    caches.put("SQL statement cache", () -> db.getStatementCache().contains(SQL));
    caches.put("SQL execution plan cache", () -> db.getExecutionPlanCache().contains(SQL));
    caches.put("Cypher statement cache", () -> db.getCypherStatementCache().size() > 0);
    caches.put("Cypher plan cache", () -> db.getCypherPlanCache().size() > 0);

    for (int i = 0; i < 2; i++) {
      database.query("sql", SQL).close();
      database.query("opencypher", CYPHER).close();
    }
    caches.forEach((name, holds) -> assertThat(holds.getAsBoolean()).as("%s holds the query before the change", name).isTrue());

    database.command("sql", "ALTER DATABASE `arcadedb.queryIndexMaxSelectivity` 0.5").close();

    caches.forEach((name, holds) -> assertThat(holds.getAsBoolean()).as("%s after a setting change", name).isFalse());
  }

  /** The repro of the issue: a cached plan carried the scan fallback of a selectivity the database no longer runs with. */
  @Test
  void aSettingReadAtPlanningTimeReachesTheNextExecutionOfACachedQuery() {
    final String query = "select count(*) as c from S where v = 1";

    database.command("sql", "ALTER DATABASE `arcadedb.queryIndexMaxSelectivity` 0.6").close();
    // twice: the second execution is served by the cached plan
    assertThat(planOf(query)).contains("full scan of S");
    assertThat(planOf(query)).contains("full scan of S");

    database.command("sql", "ALTER DATABASE `arcadedb.queryIndexMaxSelectivity` 0").close();
    assertThat(planOf(query)).as("the plan is rebuilt with the selectivity now in force").doesNotContain("full scan of S");
  }

  @Test
  void theCacheSizesFollowTheirSettingsOnAnOpenDatabase() {
    final LocalDatabase db = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();

    database.command("sql", "ALTER DATABASE `arcadedb.opencypher.planCache` 1").close();
    database.command("sql", "ALTER DATABASE `arcadedb.opencypher.statementCache` 1").close();
    database.command("sql", "ALTER DATABASE `arcadedb.sqlStatementCache` 1").close();

    for (final String label : new String[] { "a", "b", "c" }) {
      database.query("opencypher", "MATCH (n:S) RETURN n AS " + label).close();
      database.query("sql", "select v as " + label + " from S").close();
    }

    assertThat(db.getCypherPlanCache().size()).isEqualTo(1);
    assertThat(db.getCypherStatementCache().size()).isEqualTo(1);
    assertThat(db.getExecutionPlanCache().contains("select v as c from S")).isTrue();
    assertThat(db.getExecutionPlanCache().contains("select v as a from S")).as("evicted by a cache of one").isFalse();
    assertThat(db.getStatementCache().contains("select v as a from S")).as("evicted by a cache of one").isFalse();

    // a size of 0 caches nothing, and every query still answers
    database.command("sql", "ALTER DATABASE `arcadedb.sqlStatementCache` 0").close();
    database.query("sql", "select v as zero from S").close();
    assertThat(db.getExecutionPlanCache().contains("select v as zero from S")).isFalse();
    try (final ResultSet rs = database.query("sql", "select count(*) as c from S")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(60L);
    }
  }

  /**
   * The caches are resized under the same monitors their lookups take, so queries running while settings keep changing see a
   * cache either before or after a resize, never half of one: every query answers, and nothing escapes from inside a cache.
   */
  @Test
  void queriesRacingSettingChangesAlwaysAnswer() throws Exception {
    final int threads = 4;
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      for (int t = 0; t < threads; t++) {
        final int thread = t;
        pool.submit(() -> {
          int i = 0;
          while (!stop.get() && failure.get() == null)
            try {
              // distinct texts, so the lookups insert and evict while the caches are being resized
              final String label = "c" + thread + "_" + (i++ % 50);
              try (final ResultSet rs = database.query("sql", "select count(*) as " + label + " from S where v = 1")) {
                assertThat(rs.next().<Long>getProperty(label)).isEqualTo(30L);
              }
              try (final ResultSet rs = database.query("opencypher", "MATCH (n:S) WHERE n.v = 1 RETURN count(n) AS " + label)) {
                assertThat(rs.next().<Long>getProperty(label)).isEqualTo(30L);
              }
            } catch (final Throwable e) {
              failure.compareAndSet(null, e);
            }
        });
      }

      for (int i = 0; i < 200 && failure.get() == null; i++) {
        final int size = 1 + i % 4;
        database.command("sql", "ALTER DATABASE `arcadedb.sqlStatementCache` " + size).close();
        database.command("sql", "ALTER DATABASE `arcadedb.opencypher.planCache` " + size).close();
        database.command("sql", "ALTER DATABASE `arcadedb.opencypher.statementCache` " + size).close();
      }
    } finally {
      stop.set(true);
      pool.shutdown();
      assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).as("query threads ended").isTrue();
    }
    assertThat(failure.get()).as("a query racing the resizes").isNull();
  }

  private String planOf(final String query) {
    try (final ResultSet resultSet = database.query("sql", query)) {
      assertThat(resultSet.next().<Long>getProperty("c")).isEqualTo(30L);
      return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
