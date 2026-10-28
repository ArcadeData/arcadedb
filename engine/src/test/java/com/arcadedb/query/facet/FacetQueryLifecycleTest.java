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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestAccess;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * The life of a FACETS query around its rows: the application writing while it iterates them, a result set dropped half read, and the
 * query deadline reached while the facets read records.
 */
class FacetQueryLifecycleTest extends TestHelper {
  private static final String TYPE    = "Product";
  private static final int    RECORDS = 3_000;

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  /** {@code brand} (COLUMNAR), {@code color} (no index: always scanned) and {@code price}, in one bucket. */
  private void load() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".brand STRING");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".color STRING");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".price LONG");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) COLUMNAR");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set("brand", "brand-" + (i % 10)).set("color", "color-" + (i % 7)).set("price", (long) i).save();
    });
    FacetOracle.awaitCaughtUp(database);
  }

  private static Map<Object, Long> counts(final FacetResult result) {
    final Map<Object, Long> counts = new HashMap<>();
    for (final FacetBucket bucket : result.buckets())
      if (!bucket.isMissing())
        counts.put(bucket.value(), bucket.count());
    if (result.missing() > 0)
      counts.put(FacetOracle.MISSING, result.missing());
    return counts;
  }

  private Map<Object, Long> groupBy(final String property) {
    final Map<Object, Long> counts = new HashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT " + property + " AS v, count(*) AS c FROM " + TYPE + " GROUP BY " + property)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Object value = row.getProperty("v");
        counts.put(value == null ? FacetOracle.MISSING : value, row.<Number>getProperty("c").longValue());
      }
    }
    return counts;
  }

  /**
   * Final review A-I1: a query with no WHERE takes its matches from the projection. The application updates a row and inserts a record
   * while it iterates the rows (in its own transaction): the facets are decided when they are computed, after the rows, so they count
   * the transaction's writes and nothing throws.
   */
  @Test
  @Timeout(120)
  void writesWhileIteratingAProjectionSourceAreCountedWhenTheFacetsAreComputed() {
    load();
    final long fullBudget = FacetMemoryBudget.instance().available();
    final int bucketId = database.getSchema().getType(TYPE).getBuckets(true).getFirst().getFileId();
    final int references = ColumnarTestAccess.stateReferences(manager(), bucketId);
    database.begin();
    try {
      final FacetResults facets;
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " FACETS b: brand TERMS TOP 100")) {
        boolean first = true;
        long rows = 0;
        while (rs.hasNext()) {
          final Result row = rs.next();
          rows++;
          if (first) {
            first = false;
            // nothing is taken while the rows stream: the projection's match set (its charged bitmaps, its snapshots) is made after them
            assertThat(FacetMemoryBudget.instance().available()).isEqualTo(fullBudget);
            assertThat(ColumnarTestAccess.stateReferences(manager(), bucketId)).as("no snapshot held while the rows stream")
                .isEqualTo(references);
            row.getElement().orElseThrow().asDocument().modify().set("brand", "updated").save();
            database.newDocument(TYPE).set("brand", "inserted").set("color", "color-1").set("price", -1L).save();
          }
        }
        assertThat(rows).isGreaterThanOrEqualTo(RECORDS);
        facets = rs.getFacets();
      }
      final FacetResult brand = facets.get("b");
      assertThat(brand.source()).isNotEqualTo(FacetResult.UNAVAILABLE);
      assertThat(facets.exact()).isTrue();
      assertThat(counts(brand)).isEqualTo(groupBy("brand"));
      assertThat(counts(brand)).containsEntry("updated", 1L).containsEntry("inserted", 1L);
    } finally {
      database.rollback();
    }
  }

  /** Opens a FACETS query, reads one row and drops the result set without closing it. */
  private void abandonAfterOneRow() {
    abandonAfterOneRow("SELECT FROM " + TYPE + " WHERE price >= 0 FACETS brand TERMS LIMIT 20");
  }

  private void abandonAfterOneRow(final String sql) {
    final ResultSet rs = database.query("sql", sql);
    assertThat(rs.hasNext()).isTrue();
    rs.next();
  }

  /**
   * Final review C-I2: a result set read half way and never closed must not keep its memory reservations: once it is unreachable the
   * cleaner gives them back (a bounded GC loop, the only way to make an object unreachable on demand).
   */
  @Test
  @Timeout(120)
  void anAbandonedPartiallyReadResultSetGivesItsBudgetBack() {
    load();
    final FacetMemoryBudget budget = FacetMemoryBudget.instance();
    final long full = budget.available();
    abandonAfterOneRow();
    // the rows source reserved its first match bitmap step for the row read
    assertThat(budget.available()).isLessThan(full);
    await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(50)).until(() -> {
      System.gc();
      return budget.available() == full;
    });
  }

  /**
   * Final fix batch 2 (r1 M9): the same for what the FILTER evaluators hold. An INDEX evaluator charges the RID set it looked up when the
   * step opened; a query with no WHERE takes its matches from the projection only after the rows, so the evaluator is all it holds.
   */
  @Test
  @Timeout(120)
  void anAbandonedResultSetGivesTheBudgetOfItsFilterEvaluatorsBack() {
    load();
    database.command("sql", "CREATE INDEX ON " + TYPE + " (color) NOTUNIQUE");
    final FacetMemoryBudget budget = FacetMemoryBudget.instance();
    final long full = budget.available();
    final String sql = "SELECT FROM " + TYPE + " FACETS brand TERMS FILTER color = 'color-1', c: color TERMS LIMIT 20";
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).contains("[INDEX]");
    }
    abandonAfterOneRow(sql);
    assertThat(budget.available()).as("the INDEX evaluator's RID set").isLessThan(full);
    await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(50)).until(() -> {
      System.gc();
      return budget.available() == full;
    });
  }

  private MatchSet allMatches() {
    final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0);
    try (builder; final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE)) {
      while (rs.hasNext())
        assertThat(builder.add(rs.next().<Identifiable>getProperty("rid").getIdentity(), 0)).isTrue();
      final MatchSet matches = builder.build();
      matches.setFacetFilterIndex(new int[] { -1 });
      return matches;
    }
  }

  private static BasicCommandContext pastDeadline() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setCommandDeadline(System.currentTimeMillis() - 1, "test deadline", false);
    return context;
  }

  private static List<FacetSpec> terms(final String property) {
    return List.of(new TermsFacetSpec(property, property, null, null, 100, 1, FacetOrder.COUNT_DESC, null, false, null));
  }

  private void assertResourcesReturned(final long fullBudget) {
    assertThat(FacetMemoryBudget.instance().available()).isEqualTo(fullBudget);
    assertThat(manager().fallbackScans().availablePermits())
        .isEqualTo(database.getConfiguration().getValueAsInteger(GlobalConfiguration.COLUMNAR_MAX_FALLBACK_SCANS));
  }

  /** Final review C-I3: the fallback scan reads every match; it stops at the query deadline and gives its permit and budget back. */
  @Test
  @Timeout(120)
  void aFallbackScanStopsAtTheQueryDeadline() {
    load();
    final long full = FacetMemoryBudget.instance().available();
    try (final MatchSet matches = allMatches()) {
      // a property without a COLUMNAR index: every match is read from its record
      assertThatThrownBy(() -> FacetExecutor.execute((DatabaseInternal) database, matches, terms("color"), pastDeadline()))
          .isInstanceOf(TimeoutException.class);
    }
    assertResourcesReturned(full);
  }

  /** Final review C-I3: patching reads the records the projection lags behind on; it stops at the query deadline as well. */
  @Test
  @Timeout(120)
  void patchingStopsAtTheQueryDeadline() throws Exception {
    load();
    final long full = FacetMemoryBudget.instance().available();
    try (final AutoCloseable hold = manager().suspendApply()) {
      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE)) {
          while (rs.hasNext())
            rs.next().getElement().orElseThrow().asDocument().modify().set("brand", "lagging").save();
        }
      });
      try (final MatchSet matches = allMatches()) {
        assertThatThrownBy(() -> FacetExecutor.execute((DatabaseInternal) database, matches, terms("brand"), pastDeadline()))
            .isInstanceOf(TimeoutException.class);
        // the same match set with no deadline patches every record: what the deadline cut short was the patch loop
        final FacetResults results = FacetExecutor.execute((DatabaseInternal) database, matches, terms("brand"), new BasicCommandContext());
        assertThat(results.exact()).isTrue();
        assertThat(results.patched()).isEqualTo(RECORDS);
        assertThat(counts(results.get("brand"))).isEqualTo(Map.of("lagging", (long) RECORDS));
      }
    }
    assertResourcesReturned(full);
  }
}
