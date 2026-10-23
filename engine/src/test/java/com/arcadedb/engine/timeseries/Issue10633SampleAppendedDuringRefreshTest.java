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
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.ContinuousAggregateImpl;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10633: an append that commits while a refresh of a continuous aggregate runs judged its samples "behind the watermark"
 * against the watermark of before that refresh. A sample at or after it, which the refresh in progress had not read, was then not
 * recorded, and when the refresh installed a newer watermark past it no later refresh read it again: the sample never reached its
 * bucket.
 * <p>
 * Each test pauses a real refresh once it has read the source (it is about to write the first row of the backing type), appends a
 * sample, and lets the refresh end.
 */
class Issue10633SampleAppendedDuringRefreshTest extends TestHelper {
  private static final long HOUR = 3_600_000L;
  /** An hour boundary, so that the buckets of the default grid start where the test says they do. */
  private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

  private record Bucket(long start, String tag, double total) {
  }

  private void createSourceAndAggregate() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total FROM S GROUP BY hr, h");
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType("S")).getEngine();
  }

  private void append(final long ts, final double v) throws Exception {
    // the engine API does not refresh by itself, so the refreshes are the ones the test runs
    engine().appendSamples(new long[] { ts }, new Object[] { "a" }, new Object[] { v });
  }

  private ContinuousAggregateImpl aggregate() {
    return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("A");
  }

  private void refresh() {
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE A").close();
  }

  private List<Bucket> buckets() {
    final List<Bucket> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM A ORDER BY hr, h")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        result.add(new Bucket(DateUtils.toEpochMillis(row.getProperty("hr")), row.getProperty("h"),
            ((Number) row.getProperty("total")).doubleValue()));
      }
    }
    return result;
  }

  /**
   * Runs a refresh in another thread, holds it once it has read the source (when it writes its first row of the backing type: the
   * GROUP BY of the defining query has consumed every sample by then), runs {@code duringRefresh}, then lets the refresh end.
   */
  private void duringARefresh(final ThrowingRunnable duringRefresh) throws Exception {
    final CountDownLatch sourceRead = new CountDownLatch(1);
    final CountDownLatch resume = new CountDownLatch(1);
    final AtomicBoolean paused = new AtomicBoolean();
    final BeforeRecordCreateListener pause = record -> {
      if (paused.compareAndSet(false, true)) {
        sourceRead.countDown();
        try {
          resume.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return true;
    };
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    database.getSchema().getType("A").getEvents().registerListener(pause);
    try {
      final Thread refresher = new Thread(() -> {
        try {
          refresh();
        } catch (final Throwable t) {
          failure.set(t);
        }
      });
      refresher.start();
      assertThat(sourceRead.await(30, TimeUnit.SECONDS)).isTrue();
      try {
        duringRefresh.run();
      } finally {
        resume.countDown();
        refresher.join(30_000);
      }
      assertThat(refresher.isAlive()).isFalse();
      assertThat(failure.get()).isNull();
    } finally {
      database.getSchema().getType("A").getEvents().unregisterListener(pause);
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  /** The repro of the issue: the sample is at the old watermark, and the refresh moves the watermark one bucket past it. */
  @Test
  void aSampleAppendedDuringARefreshThatMovesTheWatermarkPastItReachesItsBucket() throws Exception {
    createSourceAndAggregate();
    append(H0 + 1_000, 10);
    append(H0 + 2 * HOUR + 1_000, 20);
    refresh();
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 2 * HOUR);
    // at the watermark: the next refresh reads it, and moves the watermark to its bucket
    append(H0 + 3 * HOUR + 1_000, 30);

    // not behind the watermark of before the refresh, behind the one the refresh installs
    duringARefresh(() -> append(H0 + 2 * HOUR + 2_000, 5));
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 3 * HOUR);
    assertThat(aggregate().getInvalidation()).isNotNull();

    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + 2 * HOUR, "a", 25),
        new Bucket(H0 + 3 * HOUR, "a", 30));
    assertThat(aggregate().getInvalidation()).isNull();
  }

  /** The first refresh has no watermark to judge against at all, and installs one. */
  @Test
  void aSampleAppendedDuringTheFirstRefreshReachesItsBucket() throws Exception {
    createSourceAndAggregate();
    assertThat(aggregate().isWatermarkSet()).isFalse();
    append(H0 + 1_000, 10);
    append(H0 + HOUR + 1_000, 20);

    duringARefresh(() -> append(H0 + 2_000, 5));
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + HOUR);

    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15), new Bucket(H0 + HOUR, "a", 20));
    assertThat(aggregate().getInvalidation()).isNull();
  }

  /**
   * An {@code INSERT} in a transaction refreshes the aggregate once the transaction commits. The refresh it asks for while another one runs is not skipped any
   * more: the sample is recorded, so the refresh in progress runs again when it ends and nothing else has to be asked.
   */
  @Test
  void anInsertDuringARefreshIsAggregatedWhenThatRefreshEnds() throws Exception {
    createSourceAndAggregate();
    append(H0 + 1_000, 10);
    append(H0 + 2 * HOUR + 1_000, 20);
    refresh();
    append(H0 + 3 * HOUR + 1_000, 30);

    // in a transaction: its commit is what runs the refresh of the INSERT
    duringARefresh(() -> database.transaction(
        () -> database.command("sql", "INSERT INTO S SET ts = ?, h = ?, v = ?", H0 + 2 * HOUR + 2_000, "a", 5.0).close()));

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + 2 * HOUR, "a", 25),
        new Bucket(H0 + 3 * HOUR, "a", 30));
    assertThat(aggregate().getInvalidation()).isNull();
  }

  /** Outside a refresh, a sample at or after the watermark is still recorded nowhere: the next refresh reads it. */
  @Test
  void aSampleAtTheWatermarkOutsideARefreshRecordsNothing() throws Exception {
    createSourceAndAggregate();
    append(H0 + 1_000, 10);
    refresh();

    append(H0 + 2_000, 5);
    assertThat(aggregate().getInvalidation()).isNull();
  }
}
