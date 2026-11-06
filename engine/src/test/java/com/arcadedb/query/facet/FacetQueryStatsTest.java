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
import com.arcadedb.index.columnar.ColumnarStats;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.WhereClause;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The query counters of {@link ColumnarStats} (D21): which events of a FACETS query move which counter, and the ones that must not.
 */
class FacetQueryStatsTest extends TestHelper {
  private static final String TYPE = ColumnarTestData.TYPE;

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  /** Asset with a COLUMNAR index on brand alone, so a facet on author needs the fallback scan. */
  private ColumnarTestData brandOnly(final int records) {
    final ColumnarTestData data = new ColumnarTestData(13);
    data.tagVocabulary = 300;
    data.columnarIndexes = false;
    data.createSchema(database);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) COLUMNAR");
    data.insertAssets(database, records);
    FacetOracle.awaitCaughtUp(database);
    return data;
  }

  private FacetResults facets(final String facet) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " FACETS " + facet)) {
      return rs.getFacets();
    }
  }

  @Test
  @Timeout(300)
  void countersMoveWithTheEvents() throws Exception {
    final ColumnarTestData data = brandOnly(2_000);
    final ColumnarStats stats = manager().getStats();

    // a fallback scan that runs
    long scans = stats.queryFallbackScansCount();
    assertThat(facets("author TERMS").get("author").source()).isEqualTo("scan");
    assertThat(stats.queryFallbackScansCount()).isEqualTo(scans + 1);

    // a capped one: the permit was not given, so the scan did not run and is not counted
    final Semaphore permits = manager().fallbackScans();
    final int held = permits.drainPermits();
    scans = stats.queryFallbackScansCount();
    try {
      assertThat(facets("author TERMS").get("author").source()).isEqualTo(FacetResult.UNAVAILABLE);
      assertThat(stats.queryFallbackScansCount()).isEqualTo(scans);
    } finally {
      permits.release(held);
    }

    // patched records and an overflow of the patch budget, while the projection lags
    try (final AutoCloseable hold = manager().suspendApply()) {
      data.mutate(database, 300);
      final long patchedBefore = stats.queryPatchedCount();
      final long overflowsBefore = stats.queryPatchOverflowsCount();
      final FacetResults patched = facets("brand TERMS TOP 1000");
      assertThat(patched.patched()).isPositive();
      assertThat(stats.queryPatchedCount() - patchedBefore).isEqualTo(patched.patched());
      assertThat(stats.queryPatchOverflowsCount()).as("within the patch budget").isEqualTo(overflowsBefore);

      final Object budget = GlobalConfiguration.COLUMNAR_PATCH_BUDGET.getValue();
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(1);
      try {
        assertThat(facets("brand TERMS TOP 1000").exact()).isFalse();
      } finally {
        GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(budget);
      }
      assertThat(stats.queryPatchOverflowsCount()).isEqualTo(overflowsBefore + 1);
    }
  }

  @Test
  @Timeout(300)
  void aBudgetWaitThatEndsRefusedIsCounted() {
    brandOnly(2_000);
    final ColumnarStats stats = manager().getStats();
    final long saved = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValueAsLong();
    try {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(4L << 20);
      final FacetMemoryBudget budget = FacetMemoryBudget.instance();
      try (final FacetMemoryBudget.Reservation all = budget.reserve(budget.available(), 0)) {
        assertThat(all).isNotNull();
        final long waitsBefore = stats.queryBudgetWaitsCount();
        final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 30);
        assertThat(builder.add(firstRid(), 0)).as("refused after the wait").isFalse();
        final MatchSet matches = builder.build();
        try {
          assertThat(matches.isOverBudget()).isTrue();
          assertThat(matches.budgetWaits()).isEqualTo(1);
          matches.setFacetFilterIndex(new int[] { -1 });
          FacetExecutor.execute((DatabaseInternal) database, matches, List.of(new TermsFacetSpec("brand", "brand", null, null, 10, 1,
              FacetOrder.COUNT_DESC, null, false, null)), new BasicCommandContext());
        } finally {
          matches.close();
        }
        assertThat(stats.queryBudgetWaitsCount()).isEqualTo(waitsBefore + 1);
      }
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(saved);
      FacetMemoryBudget.instance();
    }
  }

  private RID firstRid() {
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE + " LIMIT 1")) {
      return rs.next().<Identifiable>getProperty("rid").getIdentity();
    }
  }

  @Test
  @Timeout(300)
  void anIndexFilterThatFellBackToTheRowsIsCounted() {
    brandOnly(2_000);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (author) NOTUNIQUE");
    final ColumnarStats stats = manager().getStats();
    final WhereClause where = ((SelectStatement) new SQLAntlrParser(null).parse("SELECT FROM " + TYPE + " WHERE author = 'author-7'"))
        .getWhereClause();
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setDatabase(database);

    final FacetFilterEvaluator healthy = FacetFilterEvaluator.choose(where, database.getSchema().getType(TYPE), (DatabaseInternal) database);
    assertThat(healthy).isInstanceOf(FacetFilterEvaluator.Index.class);
    final long before = stats.queryFilterFallbacksCount();
    healthy.open(ctx);
    healthy.close();
    assertThat(((FacetFilterEvaluator.Index) healthy).fallback()).isNull();
    assertThat(stats.queryFilterFallbacksCount()).as("the index answered").isEqualTo(before);

    final long savedBudget = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValueAsLong();
    final long savedWait = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong();
    try {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(4L << 20);
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(30);
      final FacetMemoryBudget budget = FacetMemoryBudget.instance();
      final long waitsBefore = stats.queryBudgetWaitsCount();
      try (final FacetMemoryBudget.Reservation all = budget.reserve(budget.available(), 0)) {
        assertThat(all).isNotNull();
        final FacetFilterEvaluator starved = FacetFilterEvaluator.choose(where, database.getSchema().getType(TYPE),
            (DatabaseInternal) database);
        starved.open(ctx);
        starved.close();
        assertThat(((FacetFilterEvaluator.Index) starved).fallback()).isEqualTo("memory budget");
      }
      assertThat(stats.queryFilterFallbacksCount()).isEqualTo(before + 1);
      assertThat(stats.queryBudgetWaitsCount()).as("the refused step waited first").isEqualTo(waitsBefore + 1);
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(savedBudget);
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(savedWait);
      FacetMemoryBudget.instance();
    }
  }

  /** Runs {@code body} with a 4 MiB budget and a 30 ms wait window, all restored after. */
  private void withStarvedBudget(final Runnable body) {
    final long savedBudget = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValueAsLong();
    final long savedWait = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong();
    try {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(4L << 20);
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(30);
      body.run();
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(savedBudget);
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(savedWait);
      FacetMemoryBudget.instance();
    }
  }

  private FacetResults executeStarved(final MatchSet matches, final String property) {
    matches.setFacetFilterIndex(new int[] { -1 });
    final FacetMemoryBudget budget = FacetMemoryBudget.instance();
    try (final FacetMemoryBudget.Reservation all = budget.reserve(budget.available(), 0)) {
      assertThat(all).isNotNull();
      return FacetExecutor.execute((DatabaseInternal) database, matches,
          List.of(new TermsFacetSpec(property, property, null, null, 10, 1, FacetOrder.COUNT_DESC, null, false, null)),
          new BasicCommandContext());
    }
  }

  private MatchSet allRows() {
    final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0);
    try (builder; final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE)) {
      while (rs.hasNext())
        builder.add(rs.next().<Identifiable>getProperty("rid").getIdentity(), 0);
      return builder.build();
    }
  }

  @Test
  @Timeout(300)
  void aCollectorReservationThatWaitsThenIsRefusedIsCounted() {
    brandOnly(2_000);
    final ColumnarStats stats = manager().getStats();
    withStarvedBudget(() -> {
      try (final MatchSet matches = allRows()) {
        final long before = stats.queryBudgetWaitsCount();
        final FacetResults results = executeStarved(matches, "brand");
        assertThat(results.get("brand").source()).isEqualTo(FacetResult.UNAVAILABLE);
        assertThat(results.get("brand").reason()).isEqualTo("memory budget");
        assertThat(stats.queryBudgetWaitsCount() - before).as("the hash reservation waited, then was refused").isEqualTo(1);
      }
    });
  }

  @Test
  @Timeout(300)
  void aCappedFacetReservesNothingSoItWaitsForNothing() {
    brandOnly(2_000);
    final ColumnarStats stats = manager().getStats();
    final Semaphore permits = manager().fallbackScans();
    final int held = permits.drainPermits();
    try {
      withStarvedBudget(() -> {
        try (final MatchSet matches = allRows()) {
          final long before = stats.queryBudgetWaitsCount();
          final FacetResults results = executeStarved(matches, "author");
          assertThat(results.get("author").reason()).isEqualTo(FacetExecutor.FALLBACK_SCANS_CAPPED);
          // counting (and so every reservation) is skipped for a capped facet: no wait to count, none invented
          assertThat(stats.queryBudgetWaitsCount() - before).isZero();
        }
      });
    } finally {
      permits.release(held);
    }
  }

  @Test
  @Timeout(300)
  void theNoWhereMatchSetCountsAWaitThatEndsRefused() {
    brandOnly(2_000);
    withStarvedBudget(() -> {
      final List<Bucket> buckets = database.getSchema().getType(TYPE).getBuckets(true);
      final int[] ids = new int[buckets.size()];
      for (int i = 0; i < ids.length; i++)
        ids[i] = buckets.get(i).getFileId();
      final FacetMemoryBudget budget = FacetMemoryBudget.instance();
      try (final FacetMemoryBudget.Reservation all = budget.reserve(budget.available(), 0)) {
        assertThat(all).isNotNull();
        final MatchSet matches = MatchSet.fromProjection((DatabaseInternal) database, ids, 30);
        try {
          assertThat(matches.isOverBudget()).isTrue();
          assertThat(matches.budgetWaits()).isEqualTo(1);
        } finally {
          matches.close();
        }
      }
    });
  }
}
