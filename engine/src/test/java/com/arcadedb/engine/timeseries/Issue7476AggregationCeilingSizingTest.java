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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7476: the ceiling of an aggregation bounded what was returned, not what was allocated. #7724 carried it into
 * the scan, but the result was still sized before the scan: a range spanning millions of buckets got a flat array of
 * that many slots up front whatever the ceiling, and the SQL push-down passed no ceiling at all, so its bucket map grew
 * with the data.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7476AggregationCeilingSizingTest extends TestHelper {
  private static final String TYPE = "Sized";

  @Test
  void aCeilingFarBelowTheSpanOfTheRangeDoesNotAllocateAFlatWindow() throws Exception {
    final TimeSeriesEngine engine = createType(1, 0L, 5_000_000L, 9_000_000L);

    final MultiColumnAggregationResult bounded = aggregate(engine, 100);
    assertThat(bounded.isFlatMode()).as("9 million slots for 3 samples, under a ceiling of 100").isFalse();
    assertThat(bounded.getBucketTimestamps()).containsExactly(0L, 5_000_000L, 9_000_000L);
    assertThat(bounded.isOverBucketCeiling()).isFalse();
  }

  @Test
  void aCeilingTheSpanFitsUnderKeepsTheFlatWindow() throws Exception {
    final TimeSeriesEngine engine = createType(1, 0L, 500L, 900L);

    final MultiColumnAggregationResult result = aggregate(engine, 5_000);
    assertThat(result.isFlatMode()).isTrue();
    assertThat(result.getBucketTimestamps()).containsExactly(0L, 500L, 900L);
  }

  @Test
  void theFlatWindowIsKeptUpToTwiceTheCeilingAndNotBeyond() throws Exception {
    // A WINDOW OF span + 2 BUCKETS: 199 IS UNDER TWICE A CEILING OF 100, 201 IS OVER IT
    final TimeSeriesEngine engine = createType(1, 0L, 197L);
    assertThat(aggregate(engine, 100).isFlatMode()).isTrue();
    database.command("sql", "INSERT INTO " + TYPE + " SET ts = 199, value = 1.0").close();
    assertThat(aggregate(engine, 100).isFlatMode()).isFalse();
  }

  @Test
  void noCeilingKeepsTheFlatWindow() throws Exception {
    final TimeSeriesEngine engine = createType(1, 0L, 5_000_000L, 9_000_000L);

    assertThat(aggregate(engine, 0).isFlatMode()).isTrue();
  }

  @Test
  void aSparseAnswerUnderTheCeilingIsStillCompleteOnSeveralShards() throws Exception {
    final TimeSeriesEngine engine = createType(4, 0L, 5_000_000L, 9_000_000L);

    final MultiColumnAggregationResult result = aggregate(engine, 100);
    assertThat(result.getBucketTimestamps()).containsExactly(0L, 5_000_000L, 9_000_000L);
  }

  @Test
  void aDenseAnswerOverTheCeilingStopsOverIt() throws Exception {
    final long[] timestamps = new long[300];
    for (int i = 0; i < timestamps.length; i++)
      timestamps[i] = i;
    final TimeSeriesEngine engine = createType(1, timestamps);

    final MultiColumnAggregationResult result = aggregate(engine, 50);
    assertThat(result.getBucketTimestamps().size()).isGreaterThan(50);
    assertThat(result.isOverBucketCeiling()).isTrue();
  }

  @Test
  void sqlPushDownRefusesMoreBucketsThanTheHeapCap() {
    final long[] timestamps = new long[300];
    for (int i = 0; i < timestamps.length; i++)
      timestamps[i] = i * 1_000L;
    createType(1, timestamps);

    final long cap = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(50L);
    try {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql",
            "SELECT ts.timeBucket('1s', ts) AS b, sum(value) AS s FROM " + TYPE + " GROUP BY b")) {
          while (rs.hasNext())
            rs.next();
        }
      }).isInstanceOf(CommandExecutionException.class).hasMessageContaining("exceeded");

      // A CAP THE ANSWER STAYS UNDER CHANGES NOTHING
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(1_000L);
      int rows = 0;
      try (final ResultSet rs = database.query("sql",
          "SELECT ts.timeBucket('1s', ts) AS b, sum(value) AS s FROM " + TYPE + " GROUP BY b")) {
        while (rs.hasNext()) {
          rs.next();
          ++rows;
        }
      }
      assertThat(rows).isEqualTo(300);
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(cap);
    }
  }

  private MultiColumnAggregationResult aggregate(final TimeSeriesEngine engine, final int ceiling) throws Exception {
    final List<MultiColumnAggregationRequest> requests =
        List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "s"));
    database.begin();
    try {
      return engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, 1L, null, null, ceiling);
    } finally {
      database.commit();
    }
  }

  private TimeSeriesEngine createType(final int shards, final long... timestamps) {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE
        + " TIMESTAMP ts FIELDS (value DOUBLE) SHARDS " + shards + " COMPACTION_INTERVAL 1 SECONDS").close();
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(TYPE)).getEngine();
    database.transaction(() -> {
      for (final long timestamp : timestamps)
        database.command("sql", "INSERT INTO " + TYPE + " SET ts = :ts, value = :v",
            Map.of("ts", timestamp, "v", 1.0)).close();
    });
    try {
      engine.compactAll();
    } catch (final Exception e) {
      throw new IllegalStateException(e);
    }
    return engine;
  }
}
