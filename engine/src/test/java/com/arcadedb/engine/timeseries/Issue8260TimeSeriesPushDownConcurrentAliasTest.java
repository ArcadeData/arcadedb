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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for a gap found reviewing #8260's {@code ProjectionItem.cachedDefaultAlias} field:
 * {@code SelectExecutionPlanner.tryTimeSeriesAggregationPushDown()} reads {@code statement.getProjection()} - the
 * UNCOPIED projection {@code StatementCache.get()} hands out, the same instance for every caller of identical SQL
 * text - and calls {@code ProjectionItem.getProjectionAliasAsString()} on it directly, unlike every other planner
 * path, which works off a {@code copy()}. So an unaliased item in a push-down-eligible query is a
 * {@code ProjectionItem} genuinely shared and written by concurrent threads the very first time that SQL text is
 * parsed, which is exactly the scenario {@code cachedDefaultAlias} has to survive; it is now {@code volatile} for
 * this reason, matching {@code Projection.excludes}/{@code sourceColumns}.
 * <p>
 * A benign data race - the recomputed value is always the same immutable {@link com.arcadedb.query.sql.parser.Identifier}
 * either way - cannot be forced to fail deterministically by a unit test; what this pins down instead is that many
 * threads racing the FIRST-EVER parse and push-down of the same, never-before-seen statement text all get the
 * correct default-aliased column names and the correct aggregate values, with nothing thrown.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8260TimeSeriesPushDownConcurrentAliasTest extends TestHelper {

  private static final int THREADS = 16;

  @Test
  void concurrentFirstExecutionsOfTheSameUnaliasedPushDownQueryAllSeeTheCorrectAliases() throws Exception {
    final String type = "PushDownAliasRace";
    database.command("sql", "CREATE TIMESERIES TYPE " + type + " TIMESTAMP ts FIELDS (v DOUBLE) SHARDS 1");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(type);
    database.transaction(() -> {
      try {
        tsType.getEngine().appendSamples(new long[] { 1_000L, 2_000L, 3_000L }, new Object[] { 4.0, 1.0, 5.0 });
      } catch (final Exception e) {
        throw new IllegalStateException("cannot append the test samples", e);
      }
    });

    // No AS on the aggregate items: this is the shape whose default alias is computed through the UNCOPIED
    // projection in tryTimeSeriesAggregationPushDown(). The bucket alias stays explicit only because GROUP BY has
    // to name it; that item is unaliased too until then, exercising the same call.
    final String query = "SELECT ts.timeBucket('1h', ts) AS b, sum(v), count(*) FROM " + type + " GROUP BY b";

    final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    try {
      final CyclicBarrier barrier = new CyclicBarrier(THREADS);
      final List<Callable<List<String>>> tasks = new ArrayList<>();
      for (int i = 0; i < THREADS; i++) {
        tasks.add(() -> {
          barrier.await(30, TimeUnit.SECONDS);
          final List<String> columns = new ArrayList<>();
          try (ResultSet rs = database.query("sql", query)) {
            assertThat(rs.hasNext()).isTrue();
            final Result row = rs.next();
            columns.addAll(row.getPropertyNames());
            assertThat(row.<Number>getProperty("sum(v)").doubleValue()).isEqualTo(10.0);
            assertThat(row.<Number>getProperty("count(*)").longValue()).isEqualTo(3L);
          }
          return columns;
        });
      }

      final List<Future<List<String>>> futures = pool.invokeAll(tasks, 30, TimeUnit.SECONDS);
      for (final Future<List<String>> future : futures)
        assertThat(future.get()).containsExactlyInAnyOrder("b", "sum(v)", "count(*)");
    } finally {
      pool.shutdown();
      assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }
  }
}
