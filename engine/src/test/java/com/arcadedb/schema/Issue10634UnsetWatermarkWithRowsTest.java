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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10634: the watermark of a continuous aggregate is saved with a plain schema save after each refresh, so a node can hold
 * the rows of the backing type (replicated with the refresh transaction, or committed before a schema save that failed) beside a
 * schema whose watermark was never set: a follower promoted to leader before any schema session carried the watermark to it, an
 * aggregate created over a source that was empty at the time. A refresh from an unset watermark used to run the whole defining
 * query and delete nothing, so every bucket the backing type held was added a second time.
 * <p>
 * A refresh that finds the watermark unset and the backing type not empty now takes the newest bucket of the backing type as the
 * watermark first: that is the watermark the refresh that wrote those rows installed.
 */
class Issue10634UnsetWatermarkWithRowsTest extends TestHelper {
  private static final long HOUR = 3_600_000L;
  private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

  @Test
  void aRefreshFromAnUnsetWatermarkDoesNotDuplicateTheBucketsTheBackingTypeHolds() {
    createAggregateOverAnEmptySource();
    assertThat(aggregate().isWatermarkSet()).as("created over an empty source").isFalse();

    for (int h = 0; h < 3; h++)
      insert(H0 + h * HOUR, 1.0);
    refresh();
    assertThat(buckets()).isEqualTo(Map.of(H0, 1.0, H0 + HOUR, 1.0, H0 + 2 * HOUR, 1.0));

    // the node holds the rows but not the watermark the refresh that wrote them installed
    aggregate().restoreWatermark(0L, false);

    insert(H0 + 2 * HOUR + 1, 2.0);
    refresh();

    assertThat(rowCount()).as("every bucket once").isEqualTo(3L);
    assertThat(buckets()).isEqualTo(Map.of(H0, 1.0, H0 + HOUR, 1.0, H0 + 2 * HOUR, 3.0));
    assertThat(aggregate().isWatermarkSet()).isTrue();
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 2 * HOUR);
  }

  /** The same state read from the schema file, as a promoted follower or a restart after a failed schema save finds it. */
  @Test
  void aRefreshAfterAReopenWithAnUnsetWatermarkDoesNotDuplicateTheBuckets() {
    createAggregateOverAnEmptySource();
    for (int h = 0; h < 3; h++)
      insert(H0 + h * HOUR, 1.0);
    refresh();

    aggregate().restoreWatermark(0L, false);
    ((LocalSchema) database.getSchema()).saveConfiguration();
    reopenDatabase();
    assertThat(aggregate().isWatermarkSet()).as("the schema file holds no watermark").isFalse();

    refresh();

    assertThat(rowCount()).as("every bucket once").isEqualTo(3L);
    assertThat(buckets()).isEqualTo(Map.of(H0, 1.0, H0 + HOUR, 1.0, H0 + 2 * HOUR, 1.0));
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 2 * HOUR);

    // ... and the watermark it took is saved
    reopenDatabase();
    assertThat(aggregate().isWatermarkSet()).isTrue();
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 2 * HOUR);
  }

  /** An aggregate whose backing type is empty keeps reading every sample on its first refresh, as it always did. */
  @Test
  void anUnsetWatermarkWithAnEmptyBackingTypeStillReadsEverySample() {
    createAggregateOverAnEmptySource();
    refresh();
    assertThat(aggregate().isWatermarkSet()).isFalse();
    assertThat(rowCount()).isZero();

    insert(H0, 1.0);
    insert(H0 + HOUR, 2.0);
    refresh();

    assertThat(buckets()).isEqualTo(Map.of(H0, 1.0, H0 + HOUR, 2.0));
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + HOUR);
  }

  private void createAggregateOverAnEmptySource() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");
    database.command("sql", "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, sum(v) AS total FROM S GROUP BY hr");
  }

  private void insert(final long ts, final double v) {
    database.command("sql", "INSERT INTO S SET ts = ?, h = 'a', v = ?", ts, v).close();
  }

  private void refresh() {
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE A").close();
  }

  private ContinuousAggregateImpl aggregate() {
    return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("A");
  }

  private long rowCount() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM A")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private Map<Long, Double> buckets() {
    final Map<Long, Double> buckets = new HashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT hr, total FROM A")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        buckets.merge(DateUtils.toEpochMillis(row.getProperty("hr")), row.<Number>getProperty("total").doubleValue(), Double::sum);
      }
    }
    return buckets;
  }
}
