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
package com.arcadedb.server;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.facet.FacetExecutionStats;
import com.arcadedb.query.facet.FacetMemoryBudget;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concurrent FACETS queries share one JVM-wide counting budget ({@code arcadedb.columnar.queryMemoryBudget}): 16 clients facet a
 * 100,000-value tag vocabulary over 200,000 records at once under a 4 MiB budget, which a handful of them fill (half the queries over
 * RemoteDatabase, half embedded, whose results carry the execution statistics). No facet counts in more memory than it reserved,
 * every reservation is given back, every answer is either the exact one or {@code unavailable / memory budget}, at least one is exact,
 * and the contention shows in {@code query.budget.waits}.
 */
@Tag("slow")
public class FacetsMemoryBudgetIT extends BaseGraphServerTest {
  private static final String TYPE      = ColumnarTestData.TYPE;
  private static final int    RECORDS   = 200_000;
  private static final int    TAGS      = 100_000;
  private static final long   BUDGET    = 4L << 20;
  private static final long   WAIT_MS   = 2_000;
  private static final int    THREADS   = 16;
  private static final int    QUERIES   = 10;
  private static final String QUERY     = "SELECT FROM " + TYPE + " FACETS tags TERMS TOP 10 LIMIT 1";
  private static final String MEMORY    = "memory budget";

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database db = getDatabase(0);
    final ColumnarTestData data = new ColumnarTestData(17);
    data.tagVocabulary = TAGS;
    data.createSchema(db);
    data.insertAssets(db, RECORDS);
    FacetOracle.awaitCaughtUp(db);
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.MINUTES)
  void concurrentQueriesStayWithinBudget() throws Exception {
    final DatabaseInternal database = (DatabaseInternal) getServerDatabase(0, getDatabaseName());
    FacetOracle.awaitCaughtUp(database);
    final ColumnarIndexManager manager = ((LocalDatabase) database.getEmbedded()).getColumnarIndexManager();

    final Object previousBudget = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValue();
    final Object previousWait = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValue();
    GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(BUDGET);
    GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(WAIT_MS);
    try {
      final FacetMemoryBudget budget = FacetMemoryBudget.instance();
      assertThat(budget.available()).isEqualTo(BUDGET);

      // alone, one query fits the budget: the exact answer every concurrent one is compared with
      final String expected;
      try (final ResultSet rs = database.query("sql", QUERY)) {
        final FacetResult tags = rs.getFacets().get("tags");
        assertThat(tags.source()).isNotEqualTo(FacetResult.UNAVAILABLE);
        assertThat(tags.buckets()).hasSize(10);
        expected = tags.toJSON().toString();
      }
      assertThat(budget.available()).isEqualTo(BUDGET);
      final long waitsBefore = manager.getStats().queryBudgetWaitsCount();

      final AtomicBoolean done = new AtomicBoolean();
      final AtomicLong maxUsed = new AtomicLong();
      final Thread sampler = new Thread(() -> {
        while (!done.get()) {
          maxUsed.accumulateAndGet(BUDGET - FacetMemoryBudget.instance().available(), Math::max);
          try {
            Thread.sleep(5);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }, "facets-budget-sampler");
      sampler.setDaemon(true);
      sampler.start();

      final Queue<FacetResult> answers = new ConcurrentLinkedQueue<>();
      final Queue<FacetExecutionStats> stats = new ConcurrentLinkedQueue<>();
      final Queue<Throwable> failures = new ConcurrentLinkedQueue<>();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> clients = new ArrayList<>(THREADS);
      for (int t = 0; t < THREADS; t++) {
        final Thread client = new Thread(() -> {
          try (final RemoteDatabase remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(0), getDatabaseName(), "root",
              DEFAULT_PASSWORD_FOR_TESTS)) {
            start.await();
            for (int q = 0; q < QUERIES; q++)
              if (q % 2 == 0)
                try (final ResultSet rs = remote.query("sql", QUERY)) {
                  rs.stream().count();
                  answers.add(rs.getFacets().get("tags"));
                }
              else
                // embedded, in the same contention: only the engine's results carry the execution statistics
                try (final ResultSet rs = database.query("sql", QUERY)) {
                  rs.stream().count();
                  final FacetResults facets = rs.getFacets();
                  answers.add(facets.get("tags"));
                  stats.addAll(facets.executionStats().values());
                }
          } catch (final Throwable e) {
            failures.add(e);
          }
        }, "facets-budget-client-" + t);
        clients.add(client);
        client.start();
      }
      start.countDown();
      for (final Thread client : clients)
        client.join();
      done.set(true);
      sampler.join();

      assertThat(failures).isEmpty();
      assertThat(answers).hasSize(THREADS * QUERIES);
      // a sanity check of the sampler (available() never goes below 0): the claim itself is the one on the execution statistics below
      assertThat(maxUsed.get()).isGreaterThan(0L).isLessThanOrEqualTo(BUDGET);
      // nothing was counted in memory the budget did not grant: per facet the counter bytes are within the bytes reserved for them
      // a query refused its match set before any count carries no statistics: at most one per embedded query
      assertThat(stats).isNotEmpty().hasSizeLessThanOrEqualTo(THREADS * QUERIES / 2);
      long counted = 0;
      for (final FacetExecutionStats facet : stats) {
        assertThat(facet.counterBytes()).as(facet.toString()).isLessThanOrEqualTo(facet.budgetBytes());
        if (facet.counterBytes() > 0)
          ++counted;
      }
      assertThat(counted).isGreaterThan(0);
      int exact = 0;
      for (final FacetResult answer : answers)
        if (FacetResult.UNAVAILABLE.equals(answer.source()))
          assertThat(answer.reason()).isEqualTo(MEMORY);
        else {
          assertThat(answer.toJSON().toString()).isEqualTo(expected);
          ++exact;
        }
      assertThat(exact).isGreaterThan(0);
      assertThat(manager.getStats().queryBudgetWaitsCount()).isGreaterThan(waitsBefore);
      // every reservation was given back
      assertThat(FacetMemoryBudget.instance().available()).isEqualTo(BUDGET);
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(previousBudget);
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(previousWait);
      FacetMemoryBudget.instance();
    }
  }
}
